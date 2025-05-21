import asyncio
import datetime as dt

from loguru import logger

from weather_info import nwsSite2fcast


async def getForecast(nws_site):
    pass


async def main():
    while True:
        tasks = [getForecast(nwsSite) for nwsSite in nwsSite2fcast.keys()]
        await asyncio.gather(*tasks)

        n = dt.datetime.now()
        nextRead = n.replace(minute=1, second=0, microsecond=0)
        nextRead = nextRead + dt.timedelta(hours=1)
        nextRead = nextRead - n
        await asyncio.sleep(nextRead.total_seconds())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.error("SIGINT")
