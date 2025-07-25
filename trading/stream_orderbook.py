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
from dataclasses import dataclass, field
from typing import Optional
from fastapi import FastAPI, WebSocket

import websockets
from loguru import logger
import draccus

from trading import MarketTicker
from trading.kalshi_ref import KalshiWebSocketClient
from orderbook_update import OrderBook

import trading.utils as utils


@dataclass
class OrderbookConfig:
    # whether to record the log later for replay
    record: bool = field(default=False)
    # display CLI output
    command_line_output: bool = field(default=True)


class OrderbookWebSocketClient(KalshiWebSocketClient):
    def __init__(
        self, key_id, private_key, environment, config: OrderbookConfig
    ) -> None:
        super().__init__(key_id, private_key, environment)
        # Initialize order_books as a dictionary of OrderBook instances
        self.config = config
        self.order_books: dict[str, OrderBook] = {}
        self.delta_count = 0
        self.shutdown_requested = False
        self.tasks = []
        self.clients: set[WebSocket] = set()

        # Only set up logging if record is True
        if self.config.record:
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
            # periodic_task = asyncio.create_task(self.periodic_publish())

            # Add periodic write task only if recording
            self.tasks = [handler_task]
            if self.config.record:
                periodic_write_task = asyncio.create_task(
                    self.periodic_write_messages()
                )
                self.tasks.append(periodic_write_task)

            # Run all tasks concurrently
            await asyncio.gather(*self.tasks, return_exceptions=True)

    async def periodic_write_messages(self):
        """Periodically writes messages to JSON file"""
        if not self.config.record:
            return

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
        if not self.config.record:
            return

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

    # async def periodic_publish(self):
    #     """Periodically publishes orderbook data every second."""
    #     while not self.shutdown_requested:
    #         try:
    #             await asyncio.sleep(10)
    #             if self.shutdown_requested:
    #                 break

    #             for market_id in list(self.order_books.keys()):
    #                 self.log_orderbook(market_id)
    #             logger.info(
    #                 f"periodic published finished, processed {self.delta_count} updates"
    #             )
    #             self.delta_count = 0
    #         except asyncio.CancelledError:
    #             logger.info("Periodic publish task cancelled")
    #             break

    async def on_message(self, message_str) -> None:
        try:
            message = json.loads(message_str)

            # Store the raw message only if recording
            if self.config.record:
                self.messages.append(
                    {"timestamp": datetime.now().isoformat(), "message": message}
                )

            if message["type"] == "orderbook_delta":
                self.handle_orderbook_delta(message["msg"])
            elif message["type"] == "orderbook_snapshot":
                self.handle_orderbook_snapshot(message["msg"])
            elif message["type"] == "subscribed":
                # If command_line_output is False, just log a minimal confirmation
                if self.config.command_line_output:
                    logger.info("Websocket connected and subscribed to orderbook.")
                else:
                    logger.info("Orderbook connected successfully.")
            else:
                if self.config.command_line_output:
                    logger.info("Unknown message type:", message)
        except Exception as e:
            logger.error(f"err: {e}")

    def handle_orderbook_delta(self, delta):
        try:
            market_ticker = delta["market_ticker"]

            # Ensure an OrderBook exists for this market ticker
            if market_ticker not in self.order_books:
                self.order_books[market_ticker] = OrderBook()

            self.order_books[market_ticker].process_delta(delta)
            self.log_orderbook(market_ticker)
            self.delta_count += 1
        except Exception as e:
            logger.error(f"Error processing delta: {e}")

    def handle_orderbook_snapshot(self, snapshot):
        try:
            market_ticker = snapshot["market_ticker"]

            # Create or update OrderBook for this market ticker
            self.order_books[market_ticker] = OrderBook()
            self.order_books[market_ticker].process_snapshot(snapshot)

            # Log the snapshot
            self.log_orderbook(market_ticker)

            if self.config.command_line_output:
                logger.info("Received Orderbook Snapshot")
        except Exception as e:
            logger.error(f"Error processing snapshot: {e}")

    def log_orderbook(self, market_ticker):
        """Logs top price level for Yes and No sides"""
        if self.shutdown_requested:
            return

        # Only log if command_line_output is True
        if not self.config.command_line_output:
            return

        try:
            orderbook = self.order_books[market_ticker]
            yes_book = orderbook.markets[market_ticker]["yes"]
            no_book = orderbook.markets[market_ticker]["no"]

            # Get top price levels (or None if empty)
            top_yes_price = next(iter(yes_book.keys()), None)
            top_yes_volume = yes_book[top_yes_price] if top_yes_price is not None else 0

            top_no_price = next(iter(no_book.keys()), None)
            top_no_volume = no_book[top_no_price] if top_no_price is not None else 0

            logger.info(
                f"{market_ticker} | Yes: {top_yes_price}@{top_yes_volume} | "
                f"No: {top_no_price}@{top_no_volume}"
            )
        except Exception as e:
            logger.error(f"Error logging orderbook for {market_ticker}: {e}")

    async def graceful_shutdown(self):
        """Clean up resources and write any remaining messages"""
        self.shutdown_requested = True
        logger.info("Starting graceful shutdown...")

        # Write any remaining messages
        if self.config.record:
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

    async def broadcast(self, msg: str):
        dead = set()
        for c in self.clients:
            try:
                await c.send_text(msg)
            except:
                dead.add(c)
        self.clients -= dead

    async def add_client(self, ws: WebSocket):
        await ws.accept()
        self.clients.add(ws)

    async def remove_client(self, ws: WebSocket):
        self.clients.discard(ws)


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
    cfg = draccus.parse(config_class=OrderbookConfig)
    signal.signal(signal.SIGINT, handle_signal("SIGINT"))
    signal.signal(signal.SIGTERM, handle_signal("SIGTERM"))

    KEYID, private_key, env = utils.setup_prod()
    sites = utils.all_sites()
    mkts_dict = utils.get_markets_for_sites(sites)

    global client
    client = OrderbookWebSocketClient(
        key_id=KEYID, private_key=private_key, environment=env, config=cfg
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
