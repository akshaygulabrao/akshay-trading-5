import asyncio
from loguru import logger


class User:
    def __init__(self):
        pass

    async def run(self):
        while True:
            await asyncio.sleep(1)


if __name__ == "__main__":
    u = User()
    try:
        asyncio.run(u.run())
    except KeyboardInterrupt:
        logger.error(f"Received Keyboard Interrupt")
