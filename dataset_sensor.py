import asyncio
import datetime as dt
from zoneinfo import ZoneInfo
import os

import pandas as pd
from loguru import logger

from weather_info import nwsSite2fcast, nws_site2tz
from weather_sensor_reading import latest_sensor_reading_async, sensor_reading_history

logger.remove()
logger.add(
    "sensors/sensors-{time:YYYY-MM-DD}.log",  # Filename pattern with date
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
                logger.info(f"next sensor reading in {ttRead}")
            await asyncio.sleep(10)
    except Exception as e:
        logger.error(f"Exception: {e}")


async def getReading(nwsSite):
    tz_name = nws_site2tz[nwsSite]  # Use the correct key
    tz = ZoneInfo(tz_name)

    fdir = f"sensors/{nwsSite}"
    os.makedirs(fdir, exist_ok=True)
    fpath = f"{fdir}/sensorData.csv"
    
    # Get current readings first
    snsrReading = sensor_reading_history(nwsSite)
    if snsrReading.empty:
        logger.warning(f"No sensor readings available for {nwsSite}")
        return
    
    snsrReading.index = snsrReading.index.tz_convert(tz)
    
    if os.path.exists(fpath):
        snsrHist = pd.read_csv(fpath, index_col=0, parse_dates=True)
        snsrHist.index = pd.to_datetime(snsrHist.index, utc=True).tz_convert(tz)
        new_readings = snsrReading[~snsrReading.index.isin(snsrHist.index)]
        
        if not new_readings.empty:
            snsrReading = pd.concat([snsrHist, new_readings])
    
    snsrReading.index = snsrReading.index.tz_localize(None)
    snsrReading.to_csv(fpath)
    logger.info(f"Fetched sensor readings for {nwsSite}")


async def readings():
    global nextFore
    while True:
        tasks = [getReading(nwsSite) for nwsSite in nwsSite2fcast.keys()]
        readings = await asyncio.gather(*tasks, return_exceptions=True)
        n = dt.datetime.now()
        nextFore = n.replace(minute=1, second=0, microsecond=0)
        nextFore = nextFore + dt.timedelta(hours=1)
        tt_nextFore = nextFore - n
        await asyncio.sleep(tt_nextFore.total_seconds())


async def main():
    tasks = [asyncio.create_task(readings()), asyncio.create_task(heartbeat())]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.error("SIGINT")
    except Exception as e:
        logger.error(f"Exception: {e}")
