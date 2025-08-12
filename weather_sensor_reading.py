import os
import time
from venv import create
import requests
from typing import Dict, Any
import logging
import sys
import datetime

import asyncio
import aiohttp
import aiosqlite
from aiohttp import ClientError, ClientTimeout


API_URL = "https://api.synopticdata.com/v2/stations/timeseries"
TOKEN = os.getenv("SYNOPTIC_TOKEN", "7c76618b66c74aee913bdbae4b448bdd")

DEFAULT_PARAMS = {
    "showemptystations": 1,
    "units": "temp|F,speed|mph,english",
    "recent": 10800,
    "complete": 1,
    "obtimezone": "local",
    "token": TOKEN,
}

HEADERS = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "accept-language": "en-US,en;q=0.9",
    "dnt": "1",
    "origin": "https://www.weather.gov",
    "priority": "u=1, i",
    "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "macOS",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/138.0.0.0 Safari/537.36"
    ),
}
INSERT_ROW_SQL = """
    INSERT OR IGNORE INTO weather
    (inserted_at, station, observation_time,
    air_temp, relative_humidity, dew_point, wind_speed)
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS weather (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    inserted_at        TEXT NOT NULL,
    station            TEXT NOT NULL,
    observation_time   TEXT,
    air_temp           REAL,
    relative_humidity  REAL,
    dew_point          REAL,
    wind_speed         REAL,
    UNIQUE(station, observation_time)
);
"""


async def get_timeseries_async(
    create_table_sql, insert_row_sql, db_file
) -> Dict[str, Any]:
    params = {**DEFAULT_PARAMS, "STID": "KNYC,KMDW,KMIA,KAUS,KDEN,KPHL,KLAX"}
    async with aiosqlite.connect(db_file) as conn:
        await conn.execute(create_table_sql)
        await conn.commit()

    async with aiohttp.ClientSession() as session:
        async with session.get(
            API_URL, params=params, headers=HEADERS, timeout=15
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
            all_obs = [
                (
                    datetime.datetime.now(datetime.timezone.utc).isoformat(
                        timespec="microseconds"
                    ),
                    st["STID"],
                    dt,
                    t,
                    rh,
                    dp,
                    ws,
                )
                for st in data["STATION"]
                for dt, t, rh, dp, ws in zip(
                    st["OBSERVATIONS"]["date_time"],
                    st["OBSERVATIONS"]["air_temp_set_1"],
                    st["OBSERVATIONS"]["relative_humidity_set_1"],
                    st["OBSERVATIONS"]["dew_point_temperature_set_1d"],
                    st["OBSERVATIONS"]["wind_speed_set_1"],
                )
            ]

    async with aiosqlite.connect(db_file) as conn:
        await conn.executemany(insert_row_sql, all_obs)
        await conn.commit()

    return all_obs


class Producer:
    def __init__(self, pid: int, queue: asyncio.Queue):
        self.pid = pid
        self.q = queue

    async def run(self):
        while True:
            await asyncio.sleep(1)
            start = time.perf_counter()
            try:
                payload = await get_timeseries_async(
                    CREATE_TABLE_SQL, INSERT_ROW_SQL, "weather.db"
                )
            except (ClientError, asyncio.TimeoutError, ValueError) as exc:
                logging.exception("Error fetching timeseries %s", exc)
                continue  # do not push bad/None data to the queue

            await self.q.put(payload)
            end = time.perf_counter()
            logging.info("Producer took %.0f us", (end - start) * 1e6)


async def consumer(queue: asyncio.Queue):
    while True:
        item = await queue.get()


async def main():
    queue = asyncio.Queue(maxsize=10_000)
    producers = [Producer(0, queue)]
    producer_tasks = [asyncio.create_task(p.run()) for p in producers]
    consumer_task = asyncio.create_task(consumer(queue))

    await asyncio.gather(*producer_tasks)
    await queue.join()
    await consumer_task


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(message)s", stream=sys.stdout
    )
    asyncio.run(main())
