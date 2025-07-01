"""
Logs orderbook data to command line.
I used to use this ~5/12/25, but realized the orderbook
is pointless at my frequency.
"""
import asyncio
import json
import signal
from collections import defaultdict
from datetime import datetime

import websockets
from loguru import logger

from trading import MarketTicker
from trading.kalshi_ref import KalshiWebSocketClient

import trading.utils as utils


class OrderbookWebSocketClient(KalshiWebSocketClient):
    def __init__(self, key_id, private_key, environment) -> None:
        super().__init__(key_id, private_key, environment)
        self.order_books = {}
        self.delta_count = 0
        self.shutdown_requested = False
        self.tasks = []

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file_path = f"orderbook_messages_{timestamp}.json"
        self.messages: list[dict] = []

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
            periodic_write_task = asyncio.create_task(self.periodic_write_messages())
            self.tasks.extend([handler_task, periodic_task, periodic_write_task])

            # Run all tasks concurrently
            await asyncio.gather(*self.tasks, return_exceptions=True)

    async def periodic_write_messages(self):
        """Periodically writes messages to JSON file"""
        while not self.shutdown_requested:
            try:
                await asyncio.sleep(30)  # Write every 30 seconds
                if self.messages:
                    self.write_messages_to_file()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic message writing: {e}")

    def write_messages_to_file(self):
        """Write accumulated messages to JSON file"""
        try:
            with open(self.log_file_path, "a") as f:
                for message in self.messages:
                    json.dump(message, f)
                    f.write("\n")  # Each message on a new line for easier parsing

            # Clear the messages after writing
            self.messages.clear()
            logger.info(f"Wrote {len(self.messages)} messages to {self.log_file_path}")
        except Exception as e:
            logger.error(f"Error writing messages to file: {e}")

    async def periodic_publish(self):
        """Periodically publishes orderbook data every second."""
        while not self.shutdown_requested:
            try:
                await asyncio.sleep(10)
                if self.shutdown_requested:
                    break

                for market_id in list(self.order_books.keys()):
                    self.log_orderbook(market_id)
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

            # Store the raw message
            self.messages.append(
                {"timestamp": datetime.now().isoformat(), "message": message}
            )

            if message["type"] == "orderbook_delta":
                self.handle_orderbook_delta(message)
            elif message["type"] == "orderbook_snapshot":
                self.handle_orderbook_snapshot(message)
            elif message["type"] == "subscribed":
                logger.info("Websocket connected and subscribed to orderbook.")
            else:
                logger.info("Unknown message type:", message)
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
        self.log_orderbook(market_id)
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
        self.log_orderbook(market_id)
        logger.info("Received Orderbook Snapshot")

    def log_orderbook(self, market_id):
        """Logs both Yes and converted No orderbooks"""
        if self.shutdown_requested:
            return

        yes_book = self.order_books[market_id]["yes"]
        no_book = self.order_books[market_id]["no"]

        logger.info(
            f"Orderbook for {market_id}: " f"Yes: {dict(yes_book)}, No: {dict(no_book)}"
        )

    async def graceful_shutdown(self):
        """Clean up resources and write any remaining messages"""
        self.shutdown_requested = True
        logger.info("Starting graceful shutdown...")

        # Write any remaining messages
        if self.messages:
            self.write_messages_to_file()

        # Cancel all running tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()

        # Wait for tasks to complete cancellation
        await asyncio.gather(*self.tasks, return_exceptions=True)

        # Close WebSocket connection if it exists
        if hasattr(self, "ws"):
            await self.ws.close()

        logger.info("Graceful shutdown complete")

    async def on_close(self):
        """Clean up resources"""
        await self.graceful_shutdown()
        await super().on_close(1000, "Closing websocket")


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
    # Set up signal handlers
    signal.signal(signal.SIGINT, handle_signal("SIGINT"))
    signal.signal(signal.SIGTERM, handle_signal("SIGTERM"))

    KEYID, private_key, env = utils.setup_prod()
    sites = utils.all_sites()
    mkts_dict = utils.get_markets_for_sites(sites)

    global client
    client = OrderbookWebSocketClient(
        key_id=KEYID, private_key=private_key, environment=env
    )

    # Flatten the dictionary values to get all market tickers
    all_markets: list[MarketTicker] = []
    for market_list in mkts_dict.values():
        all_markets.extend(market_list)

    try:
        await client.connect([market.name for market in all_markets])
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
