import asyncio
import logging
from weather_extract_forecast import ForecastPoll
from weather_sensor_reading import SensorPoll
from orderbook import ObWebsocket
import sys

async def consumer(queue: asyncio.Queue):
    while True:
        message = await queue.get()


async def main() -> None:
    queue = asyncio.Queue(maxsize=10_000)

    producers = [
        ForecastPoll(queue, "forecast.db"),
        SensorPoll(queue, "weather.db"),
        ObWebsocket(queue, "data/data_orderbook.db"),
    ]

    producer_tasks = [asyncio.create_task(p.run()) for p in producers]
    consumer_task = asyncio.create_task(consumer(queue))

    all_tasks = producer_tasks + [consumer_task]

    await asyncio.gather(*all_tasks, return_exceptions=True)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(message)s", stream=sys.stdout
    )
    asyncio.run(main())
