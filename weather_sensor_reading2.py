import asyncio
import logging
import time
import aiohttp
import aiosqlite
import datetime
import typing

from dataSource import DataSource


class FastPollSource(DataSource):
    def __init__(self, websocket_queue: typing.Optional[asyncio.Queue]):
        super().__init__("fast_poll", app_state)
        self.poll_interval = 1.0
        self.db_file = "data/weather2.db"

    async def start(self):
        self.db = await aiosqlite.connect(self.db_file)
        await self.db.execute(
            """
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
        )
        await self.db.commit()
        API_URL = "https://api.synopticdata.com/v2/stations/timeseries"
        TOKEN = "7c76618b66c74aee913bdbae4b448bdd"

        DEFAULT_PARAMS = {
            "showemptystations": 1,
            "units": "temp|F,speed|mph,english",
            "recent": 100,
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

        while not self.app_state.shutdown_event.is_set():
            start_time = time.monotonic()

            try:
                params = {
                    **DEFAULT_PARAMS,
                    "STID": "KNYC,KMDW,KMIA,KAUS,KDEN,KPHL,KLAX",
                }
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        API_URL, params=params, headers=HEADERS, timeout=1
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

                await self.queue.put(all_obs)

            except Exception as e:
                logging.error(f"Fast poll error: {e}")

            # Ensure consistent polling interval
            elapsed = time.monotonic() - start_time
            await asyncio.sleep(max(0, self.poll_interval - elapsed))

    async def process_queue(self) -> None:
        INSERT_SQL = """
        INSERT OR IGNORE INTO weather
        (inserted_at, station, observation_time,
        air_temp, relative_humidity, dew_point, wind_speed)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        while not self.app_state.shutdown_event.is_set():
            message = await self.queue.get()
            logging.info("rcvd sensor data")
            async with aiosqlite.connect(self.db_file) as conn:
                await conn.executemany(INSERT_SQL, message)
                await conn.commit()
            sensor_msg = {"type": "sensor", "data": message}
            await self.app_state.broadcast_queue.put(sensor_msg)

    async def stop(self) -> None:
        if hasattr(self, "db"):
            await self.db.close()
