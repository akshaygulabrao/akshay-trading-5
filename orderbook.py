"""
Publishes orderbook data to ipc socket via zmq.
I used to use this ~5/12/25, but realized the orderbook
is pointless at my frequency.
"""
import asyncio
import json
import signal
from collections import defaultdict

import zmq
from zmq.asyncio import Context
import websockets
from loguru import logger

from kalshi_ref import KalshiWebSocketClient
import utils


class OrderbookWebSocketClient(KalshiWebSocketClient):
    def __init__(self, key_id, private_key, environment, pub):
        super().__init__(key_id, private_key, environment)
        self.order_books = {}
        self.pub = pub
        self.delta_count = 0
        self.shutdown_requested = False
        self.tasks = []

    async def connect(self, tickers):
        """Establishes a WebSocket connection and runs concurrent tasks."""
        host = self.WS_BASE_URL + self.url_suffix
        auth_headers = self.request_headers("GET", self.url_suffix)
        async with websockets.connect(
            host, additional_headers=auth_headers
        ) as websocket:
            self.ws = websocket
            await self.on_open(tickers)

            # Create tasks for handler and periodic publishing
            handler_task = asyncio.create_task(self.handler())
            periodic_task = asyncio.create_task(self.periodic_publish())
            self.tasks.extend([handler_task, periodic_task])

            # Run both tasks concurrently
            await asyncio.gather(*self.tasks, return_exceptions=True)

    async def periodic_publish(self):
        """Periodically publishes orderbook data every second."""
        while not self.shutdown_requested:
            try:
                await asyncio.sleep(10)
                if self.shutdown_requested:
                    break

                for market_id in list(self.order_books.keys()):
                    self.publish_orderbook(market_id)
                logger.info(
                    f"periodic published finished, processed {self.delta_count} updates"
                )
                self.delta_count = 0
            except asyncio.CancelledError:
                logger.info("Periodic publish task cancelled")
                break

    async def on_message(self, message_str):
        try:
            message = json.loads(message_str)
            if message["type"] == "orderbook_delta":
                self.handle_orderbook_delta(message)
            elif message["type"] == "orderbook_snapshot":
                self.handle_orderbook_snapshot(message)
            elif message["type"] == "subscribed":
                logger.info("Websocket connected and subscribed to orderbook.")
            else:
                logger.info("Unknown message type:", message["type"])
        except Exception as e:
            logger.error(f"err: {e}")

    def handle_orderbook_delta(self, delta):
        market_id = delta["msg"]["market_ticker"]
        if market_id not in self.order_books:
            logger.info(f"Warning: Received delta for unknown market {market_id}")
            return
        side = delta["msg"]["side"]
        price = delta["msg"]["price"]
        size = delta["msg"]["delta"]
        book = self.order_books[market_id][side]
        book[price] += size
        if book[price] <= 0:
            del book[price]
        self.publish_orderbook(market_id)
        self.delta_count += 1

    def handle_orderbook_snapshot(self, snapshot):
        market_id = snapshot["msg"]["market_ticker"]
        if market_id not in self.order_books:
            self.order_books[market_id] = {
                "yes": defaultdict(int),
                "no": defaultdict(int),
            }
        if "no" in snapshot["msg"]:
            for price, volume in snapshot["msg"]["no"]:
                self.order_books[market_id]["no"][price] = volume
        if "yes" in snapshot["msg"]:
            for price, volume in snapshot["msg"]["yes"]:
                self.order_books[market_id]["yes"][price] = volume
        self.publish_orderbook(market_id)
        logger.info("Received Orderbook Snapshot")

    def publish_orderbook(self, market_id):
        """Publishes both Yes and converted No orderbooks"""
        if self.shutdown_requested:
            return

        yes_book = self.order_books[market_id]["yes"]
        no_book = self.order_books[market_id]["no"]

        self.pub.send_json(
            {
                "market_ticker": market_id,
                "yes": dict(yes_book),
                "no": dict(no_book),
            }
        )

    async def graceful_shutdown(self):
        """Clean up resources"""
        self.shutdown_requested = True
        logger.info("Starting graceful shutdown...")

        # Cancel all running tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()

        # Wait for tasks to complete cancellation
        await asyncio.gather(*self.tasks, return_exceptions=True)

        # Close WebSocket connection if it exists
        if hasattr(self, "ws"):
            await self.ws.close()

        # Close ZMQ socket
        if hasattr(self, "pub"):
            self.pub.close()

        logger.info("Graceful shutdown complete")

    async def on_close(self):
        """Clean up resources"""
        await self.graceful_shutdown()
        await super().on_close()


def handle_signal(signal_name):
    """Signal handler factory"""

    def handler(signum, frame):
        logger.warning(f"Received {signal_name}, initiating graceful shutdown...")
        # Set shutdown flag on the client instance
        if "client" in globals():
            client.shutdown_requested = True
        # Get running event loop and create shutdown task
        loop = asyncio.get_running_loop()
        loop.create_task(client.graceful_shutdown())

    return handler


async def main():
    logger.add(
        "orderbook_logs/ob.log", rotation="24 hours", retention="3 days", enqueue=True
    )

    # Set up signal handlers
    signal.signal(signal.SIGINT, handle_signal("SIGINT"))
    signal.signal(signal.SIGTERM, handle_signal("SIGTERM"))

    KEYID, private_key, env = utils.setup_prod()

    context = Context.instance()
    pub = context.socket(zmq.PUB)
    pub.bind("ipc:///tmp/orderbook.ipc")
    mkts = utils.get_markets()

    global client
    client = OrderbookWebSocketClient(
        key_id=KEYID, private_key=private_key, environment=env, pub=pub
    )

    try:
        await client.connect(mkts)
    except asyncio.CancelledError:
        logger.info("Main task cancelled during shutdown")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        await client.graceful_shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, shutdown complete")
