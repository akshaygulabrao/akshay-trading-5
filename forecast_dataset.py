import asyncio
import datetime as dt
from zoneinfo import ZoneInfo
import os

from loguru import logger

from weather_info import nwsSite2fcast, nws_site2tz
from weather_extract_forecast import extract_forecast

logger.remove()
logger.add(
    "forecasts/forecast-{time:YYYY-MM-DD}.log",  # Filename pattern with date
    rotation="00:00",  # Rotate at midnight
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",  # Custom log format
    level="INFO",  # Minimum log level
    enqueue=True,  # Thread-safe logging
    retention="30 days",  # Optional: Keep logs for 30 days
)

global nextFore
nextFore = None


async def heartbeat():
    global nextFore
    try:
        while True:
            if nextFore is not None:
                ct = dt.datetime.now()
                ttRead = (nextFore - ct).total_seconds()
                logger.info(f"next forecast reading in {ttRead}")
            await asyncio.sleep(10)
    except Exception as e:
        logger.error(f"Exception: {e}")


async def getForecast(nwsSite):
    tz_name = nws_site2tz[nwsSite]  # Use the correct key
    tz = ZoneInfo(tz_name)

    forecast = await extract_forecast(nwsSite)
    datetime_stamp = dt.datetime.now(tz=tz)
    date = datetime_stamp.date()
    hour = datetime_stamp.hour
    minute = datetime_stamp.minute

    fdir = f"forecasts/{nwsSite}/{date}"
    os.makedirs(fdir, exist_ok=True)
    fpath = f"{fdir}/{hour:02}{minute:02}.csv"
    forecast.to_csv(fpath, index=None)
    logger.info(f"Fetched forecast for {nwsSite}")


async def forecasts():
    global nextFore
    while True:
        tasks = [getForecast(nwsSite) for nwsSite in nwsSite2fcast.keys()]
        forecasts = await asyncio.gather(*tasks, return_exceptions=True)
        n = dt.datetime.now()
        nextFore = n.replace(minute=1, second=0, microsecond=0)
        nextFore = nextFore + dt.timedelta(hours=1)
        tt_nextFore = nextFore - n
        await asyncio.sleep(tt_nextFore.total_seconds())


async def forecasts():
    global nextFore
    while True:
        tasks = [getForecast(nwsSite) for nwsSite in nwsSite2fcast.keys()]
        forecasts = await asyncio.gather(*tasks, return_exceptions=True)
        n = dt.datetime.now()
        nextFore = n.replace(minute=1, second=0, microsecond=0)
        nextFore = nextFore + dt.timedelta(hours=1)
        tt_nextFore = nextFore - n
        await asyncio.sleep(tt_nextFore.total_seconds())


async def main():
    tasks = [asyncio.create_task(forecasts()), asyncio.create_task(heartbeat())]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.error("SIGINT")
    except Exception as e:
        logger.error(f"Exception: {e}")
