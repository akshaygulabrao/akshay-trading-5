import asyncio
import json
import signal
from collections import defaultdict
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware

import websockets
from loguru import logger
import draccus

from kalshi_ref import KalshiWebSocketClient
from orderbook_update import OrderBook
import utils

# --- Configuration ---
RESTART_INTERVAL = 300  # 5 minutes in seconds
shutdown_event = asyncio.Event()

# --- Global Client Variable ---
# This will be shared between the FastAPI server and the supervisor.
# The supervisor will update this variable with the newest client instance.
client: Optional["OrderbookWebSocketClient"] = None

# --- FastAPI App Setup ---
fast_app = FastAPI()
fast_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ... (Your OrderbookWebSocketClient class remains unchanged) ...
class OrderbookWebSocketClient(KalshiWebSocketClient):
    def __init__(
        self, key_id, private_key, environment, config: "OrderbookConfig"
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
            publish_task = asyncio.create_task(self.periodic_publish())

            # Add periodic write task only if recording
            self.tasks = [handler_task, publish_task]
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
            num_messages = len(self.messages)
            self.messages.clear()
            logger.info(f"Wrote {num_messages} messages to {self.log_file_path}")
        except Exception as e:
            logger.error(f"Error writing messages to file: {e}")

    async def periodic_publish(self):
        """Periodically publishes orderbook data every second."""
        while not self.shutdown_requested:
            try:
                await asyncio.sleep(5)
                if self.shutdown_requested:
                    break

                for market_id in list(self.order_books.keys()):
                    await self.log_orderbook(market_id)
                logger.info(
                    f"Periodic publish finished, processed {self.delta_count} updates"
                )
                self.delta_count = 0
            except asyncio.CancelledError:
                logger.info("Periodic publish task cancelled")
                break

    async def on_message(self, message_str) -> None:
        try:
            message = json.loads(message_str)

            # Store the raw message only if recording
            if self.config.record:
                self.messages.append(
                    {"timestamp": datetime.now().isoformat(), "message": message}
                )

            if message["type"] == "orderbook_delta":
                await self.handle_orderbook_delta(message["msg"])
            elif message["type"] == "orderbook_snapshot":
                await self.handle_orderbook_snapshot(message["msg"])
            elif message["type"] == "subscribed":
                # If command_line_output is False, just log a minimal confirmation
                if self.config.command_line_output:
                    logger.info("Websocket connected and subscribed to orderbook.")
                else:
                    logger.info("Orderbook connected successfully.")
            else:
                if self.config.command_line_output:
                    logger.info(f"Unknown message type: {message}")
        except Exception as e:
            logger.error(f"err: {e}")

    async def handle_orderbook_delta(self, delta):
        try:
            market_ticker = delta["market_ticker"]

            # Ensure an OrderBook exists for this market ticker
            if market_ticker not in self.order_books:
                self.order_books[market_ticker] = OrderBook()

            self.order_books[market_ticker].process_delta(delta)
            await self.log_orderbook(market_ticker)
            self.delta_count += 1
        except Exception as e:
            logger.error(f"Error processing delta: {e}")

    async def handle_orderbook_snapshot(self, snapshot):
        try:
            market_ticker = snapshot["market_ticker"]

            # Create or update OrderBook for this market ticker
            self.order_books[market_ticker] = OrderBook()
            self.order_books[market_ticker].process_snapshot(snapshot)

            # Log the snapshot
            await self.log_orderbook(market_ticker)

            if self.config.command_line_output:
                logger.info("Received Orderbook Snapshot")
        except Exception as e:
            logger.error(f"Error processing snapshot: {e}")

    async def log_orderbook(self, market_ticker):
        """Logs top price level for Yes and No sides"""
        if self.shutdown_requested:
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

            mkt = {
                "ticker": market_ticker,
                "yes": f"{top_yes_price}@{top_yes_volume}"
                if top_yes_price is not None
                else "N/A",
                "no": f"{top_no_price}@{top_no_volume}"
                if top_no_price is not None
                else "N/A",
            }
            await self.broadcast(mkt)
        except Exception as e:
            logger.error(f"Error logging orderbook for {market_ticker}: {e}")

    async def graceful_shutdown(self):
        """Clean up resources and write any remaining messages"""
        if self.shutdown_requested:
            return  # Already shutting down
        self.shutdown_requested = True
        logger.info("Starting graceful shutdown of Kalshi client...")

        # Write any remaining messages
        if self.config.record:
            self.write_messages_to_file()

        # Cancel all running tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()

        # Wait for tasks to complete cancellation
        await asyncio.gather(*self.tasks, return_exceptions=True)

        if hasattr(self, "ws") and self.ws:
            try:
                # Simply attempt to close. This is safe even if it's already closed.
                await self.ws.close()
            except websockets.exceptions.ConnectionClosed:
                # This is expected if the connection was already closed by the other side
                pass
            except Exception as e:
                # Log any other unexpected errors during close
                logger.error(f"Unexpected error while closing websocket: {e}")

        logger.info("Graceful shutdown of Kalshi client complete")

    async def on_close(self):
        """Clean up resources"""
        await self.graceful_shutdown()
        await super().on_close(1000, "Closing websocket")

    async def broadcast(self, msg: dict):
        # Create a list of coroutines to send to all clients
        send_tasks = [c.send_json(msg) for c in self.clients]
        # Run them concurrently and get results
        results = await asyncio.gather(*send_tasks, return_exceptions=True)
        # Find clients that failed and remove them
        dead_clients = {
            client
            for client, result in zip(self.clients, results)
            if isinstance(result, Exception)
        }
        self.clients -= dead_clients

    async def add_client(self, ws: WebSocket):
        await ws.accept()
        self.clients.add(ws)

    async def remove_client(self, ws: WebSocket):
        self.clients.discard(ws)


@dataclass
class OrderbookConfig:
    # whether to record the log later for replay
    record: bool = field(default=False)
    # display CLI output
    command_line_output: bool = field(default=True)


async def run_fastapi_server():
    """
    Runs the Uvicorn server once and for all. It will not be restarted.
    """
    import uvicorn

    config = uvicorn.Config(app=fast_app, host="0.0.0.0", port=8000, log_level="info")
    server = uvicorn.Server(config)
    logger.info("Starting Uvicorn server on http://0.0.0.0:8000")
    await server.serve()


@fast_app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    """
    FastAPI endpoint for clients to connect to our server.
    It uses the global 'client' object to register itself.
    """
    if client is None:
        logger.warning("WebSocket connection attempt before client is ready.")
        await ws.close(code=1011, reason="Server is initializing, please try again.")
        return

    await client.add_client(ws)
    try:
        while True:
            # We just wait here. The client will be broadcasting to us.
            # A receive_text() keeps the connection alive and handles client-side closes.
            await ws.receive_text()
    except Exception:
        # This will trigger when the client disconnects
        logger.info("Client disconnected from /ws endpoint.")
    finally:
        if client:
            await client.remove_client(ws)


async def kalshi_connection_supervisor(cfg: OrderbookConfig, key_id, private_key, env):
    """
    Supervises the Kalshi client connection, restarting it periodically.
    THIS function is now the one that loops.
    """
    global client
    while not shutdown_event.is_set():
        try:
            # 1. Create a fresh client for this iteration
            client = OrderbookWebSocketClient(
                key_id=key_id, private_key=private_key, environment=env, config=cfg
            )
            logger.info("Created new Kalshi client instance.")

            # 2. Get market data
            sites = utils.all_sites()
            mkts_dict = utils.get_markets_for_sites(sites)
            all_markets = [
                m.name for market_list in mkts_dict.values() for m in market_list
            ]

            # 3. Run the client connection until the timer expires
            logger.info(
                f"Connecting to Kalshi... Will restart in {RESTART_INTERVAL} seconds."
            )
            await asyncio.wait_for(
                client.connect(all_markets), timeout=RESTART_INTERVAL
            )

        except asyncio.TimeoutError:
            logger.info(f"Restart interval of {RESTART_INTERVAL}s reached.")
        except asyncio.CancelledError:
            logger.info("Supervisor task cancelled. Shutting down.")
            break
        except Exception as e:
            logger.error(
                f"Kalshi client failed with an error: {e}. Retrying in 10 seconds."
            )
            await asyncio.sleep(10)  # Wait before retrying on failure
        finally:
            # 4. Gracefully shut down the *current* client before the next loop
            if client:
                await client.graceful_shutdown()

        logger.info("Kalshi client restart cycle complete. Reconnecting...")


async def main():
    """
    The main orchestrator. Sets up and runs the server and supervisor concurrently.
    """

    def signal_handler(sig):
        logger.warning(f"Received signal {sig}, initiating shutdown...")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, lambda: signal_handler("SIGINT"))
    loop.add_signal_handler(signal.SIGTERM, lambda: signal_handler("SIGTERM"))

    # --- Initial Setup ---
    cfg = draccus.parse(config_class=OrderbookConfig)
    KEYID, private_key, env = utils.setup_prod()

    # --- Create and run main tasks ---
    server_task = asyncio.create_task(run_fastapi_server())
    supervisor_task = asyncio.create_task(
        kalshi_connection_supervisor(cfg, KEYID, private_key, env)
    )

    # Wait for either task to finish (which they shouldn't unless there's an error or shutdown)
    done, pending = await asyncio.wait(
        [server_task, supervisor_task], return_when=asyncio.FIRST_COMPLETED
    )

    # If we get here, one of the main tasks has stopped. Time to shut down everything.
    shutdown_event.set()
    for task in pending:
        task.cancel()

    if client:
        await client.graceful_shutdown()

    await asyncio.gather(*pending, return_exceptions=True)
    logger.info("Application shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application stopped by KeyboardInterrupt.")
