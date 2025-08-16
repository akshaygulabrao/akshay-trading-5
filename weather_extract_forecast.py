import asyncio
import aiosqlite
from pathlib import Path
import logging, os

import time

import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from collections import defaultdict

import httpx
import datetime as dt
import pytz
import sys


CREATE_TABLE_SQL = """
            CREATE TABLE IF NOT EXISTS forecast (
                inserted_at      TEXT NOT NULL,
                idx              INT,
                station          TEXT NOT NULL,
                observation_time TEXT NOT NULL,
                air_temp         REAL,
                relative_humidity REAL,
                dew_point        REAL,
                wind_speed       REAL,
                PRIMARY KEY (idx, station, observation_time)
            );
            """

INSERT_ROW_SQL = """
        INSERT OR IGNORE INTO forecast
        (inserted_at, idx, station, observation_time,
        air_temp, relative_humidity, dew_point, wind_speed)
        VALUES
        (:inserted_at, :idx, :station, :observation_time,
        :air_temp, :relative_humidity, :dew_point, :wind_speed)
        """

tz_map = {
    "KNYC": "US/Eastern",
    "KMDW": "US/Central",
    "KAUS": "US/Central",
    "KMIA": "US/Eastern",
    "KDEN": "US/Mountain",
    "KPHL": "US/Eastern",
    "KLAX": "US/Pacific",
}
nws_site2forecast = {
    "KNYC": "https://forecast.weather.gov/MapClick.php?lat=40.78&lon=-73.97&lg=english&&FcstType=digital",
    "KMDW": "https://forecast.weather.gov/MapClick.php?lat=41.78&lon=-87.76&lg=english&&FcstType=digital",
    "KAUS": "https://forecast.weather.gov/MapClick.php?lat=30.18&lon=-97.68&lg=english&&FcstType=digital",
    "KMIA": "https://forecast.weather.gov/MapClick.php?lat=25.7554&lon=-80.2262&lg=english&&FcstType=digital",
    "KDEN": "https://forecast.weather.gov/MapClick.php?lat=39.85&lon=-104.66&lg=english&&FcstType=digital",
    "KPHL": "https://forecast.weather.gov/MapClick.php?lat=40.08&lon=-75.01&lg=english&&FcstType=digital",
    "KLAX": "https://forecast.weather.gov/MapClick.php?lat=33.96&lon=-118.42&lg=english&&FcstType=digital",
}

site2mkt = {
    "KLAX": "KXHIGHLAX",
    "KNYC": "KXHIGHNY",
    "KMDW": "KXHIGHCHI",
    "KAUS": "KXHIGHAUS",
    "KMIA": "KXHIGHMIA",
    "KDEN": "KXHIGHDEN",
    "KPHL": "KXHIGHPHIL",
}


async def extract_forecast(nws_site):
    """Fetches forecast data for a given NWS site."""
    url = nws_site2forecast.get(nws_site)
    if not url:
        logging.warning("%s is down", nws_site)
        return []

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
        if response.status_code != 200:
            logging.warning("%s is down", nws_site)
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        try:
            table = soup.find_all("table")[4]
        except IndexError:
            logging.warning("%s is down", nws_site)
            return []

        forecast_dict = defaultdict(list)
        for row in table.find_all("tr"):
            row_data = [cell.get_text(strip=True) for cell in row.find_all(["td", "th"])]
            if row_data and row_data[0]:
                forecast_dict[row_data[0]].extend(row_data[1:])

        tz = pytz.timezone(tz_map[nws_site])
        df = pd.DataFrame.from_dict(forecast_dict)
        df["Date"] = df["Date"].replace("", np.nan).ffill()
        df["Date"] = df["Date"].apply(lambda x: x + "/2025")
        df["Date"] = pd.to_datetime(df["Date"], format="%m/%d/%Y")
        df.iloc[:, 1] = df.iloc[:, 1].astype(int)
        df["Date"] = df["Date"] + pd.to_timedelta(df.iloc[:, 1], unit="h")
        df["Date"] = df["Date"].dt.tz_localize(tz).dt.strftime("%Y-%m-%dT%H:%M:%S%z")

        df.insert(0, "idx", range(len(df)))
        df.insert(0, "station", nws_site)
        df.insert(
            0,
            "inserted_at",
            dt.datetime.now(dt.timezone.utc).isoformat(timespec="microseconds"),
        )
        rename_map = {
            "station": "station",
            "Date": "observation_time",
            "Temperature (°F)": "air_temp",
            "Relative Humidity (%)": "relative_humidity",
            "Dewpoint (°F)": "dew_point",
            "Surface Wind (mph)": "wind_speed",
        }
        df = df.rename(columns=rename_map)
        rows = df[
            [
                "inserted_at",
                "idx",
                "station",
                "observation_time",
                "air_temp",
                "dew_point",
                "wind_speed",
                "relative_humidity",
            ]
        ].to_dict(orient="records")
        return rows

    except Exception as e:
        logging.warning("%s is down", nws_site)
        return []


class ForecastPoll:
    def __init__(self, queue: asyncio.Queue, db_file: str):
        self.q = queue
        self.db_file = db_file
        logging.getLogger("httpx").setLevel(logging.CRITICAL)

    async def resubscribe(self):
        await asyncio.sleep(1)
        start = time.perf_counter()
        coros = [extract_forecast(i) for i in nws_site2forecast.keys()]
        for coro in asyncio.as_completed(coros):
            result = await coro
            async with aiosqlite.connect(self.db_file) as conn:
                await conn.executemany(INSERT_ROW_SQL, result)
                await conn.commit()
            filtered = [(i["observation_time"], i["air_temp"]) for i in result]
            packet = {
                "type": self.__class__.__name__,
                "site": site2mkt[result[0]["station"]],
                "payload": filtered,
            }

            await self.q.put(packet)
        end = time.perf_counter()
        logging.info("%s took %.0f us", self.__class__.__name__, (end - start) * 1e6)

    async def run(self):
        async with aiosqlite.connect(self.db_file) as conn:
            await conn.execute(CREATE_TABLE_SQL)
            await conn.commit()
        while True:
            await asyncio.sleep(5)
            start = time.perf_counter()
            coros = [extract_forecast(i) for i in nws_site2forecast.keys()]
            for coro in asyncio.as_completed(coros):
                result = await coro
                async with aiosqlite.connect(self.db_file) as conn:
                    await conn.executemany(INSERT_ROW_SQL, result)
                    await conn.commit()
                filtered = [(i["observation_time"], i["air_temp"]) for i in result]
                packet = {
                    "type": self.__class__.__name__,
                    "site": site2mkt[result[0]["station"]],
                    "payload": filtered,
                }

                await self.q.put(packet)
            end = time.perf_counter()
            logging.info("%s took %.0f us", self.__class__.__name__, (end - start) * 1e6)


async def consumer(queue: asyncio.Queue):
    while True:
        item = await queue.get()


async def main():

    def _require_envs(*names):
        for n in names:
            p = os.getenv(n)
            if p is None or not Path(p).exists():
                sys.exit(f"Missing or invalid env var {n}")

    _require_envs("FORECAST_DB_PATH")

    queue = asyncio.Queue(maxsize=10_000)
    producers = [ForecastPoll(queue, os.getenv("FORECAST_DB_PATH"))]
    producer_tasks = [asyncio.create_task(p.run()) for p in producers]
    consumer_task = asyncio.create_task(consumer(queue))

    await asyncio.gather(*producer_tasks)
    await queue.join()
    await consumer_task


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", stream=sys.stdout)
    asyncio.run(main())
