import asyncio
import sqlite3
import aiosqlite
import time
import weakref
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import datetime as dt
from typing import Any, Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor
import logging.handlers as H, logging, pathlib
import logging
import pathlib
import sys
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from orderbook_update import OrderBook


# Base class for all data sources
class DataSource(ABC):
    def __init__(self, name: str, app_state: "AppState"):
        self.name = name
        self.app_state = app_state
        self.queue = asyncio.Queue(maxsize=10_000)
        self.db_pool = ThreadPoolExecutor(max_workers=4)

    @abstractmethod
    async def start(self) -> None:
        """Start collecting data from this source"""
        pass

    @abstractmethod
    async def process_queue(self) -> None:
        """Process items from this source's queue"""
        pass

    @abstractmethod
    async def insert_to_db(self, message: Dict[str, Any]) -> None:
        """Common database insertion method"""
        pass


# WebSocket data source
class WebSocketSource(DataSource):
    def __init__(self, app_state: "AppState"):
        super().__init__("ws", app_state)
        self.reconnect_delay = 1
        self.max_reconnect_delay = 60
        self.order_books: dict[str, OrderBook] = {}
        self.url = "wss://api.elections.kalshi.com/trade-api/ws/v2"
        self.DB = pathlib.Path(__file__).with_name("data_orderbook.db")
        self.order_books: dict[str, OrderBook] = {}

    async def stop(self) -> None:
        if hasattr(self, "db"):
            await self.db.close()

    async def start(self) -> None:
        await asyncio.sleep(0.5)  # wait a little before first attempt

        # --- load private key once at start-up ---
        import os, base64, time, json, requests, websockets
        from cryptography.hazmat.primitives import serialization, hashes
        from cryptography.hazmat.primitives.asymmetric import padding

        KEY_ID = os.getenv("PROD_KEYID")
        PRIVATE_KEY_PATH = os.getenv("PROD_KEYFILE")

        with open(PRIVATE_KEY_PATH, "rb") as f:
            private_key = serialization.load_pem_private_key(f.read(), password=None)

        def sign_pss_text(text: str) -> str:
            message = text.encode("utf-8")
            signature = private_key.sign(
                message,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.DIGEST_LENGTH,
                ),
                hashes.SHA256(),
            )
            return base64.b64encode(signature).decode("utf-8")

        def make_headers(method: str, path: str) -> dict:
            ts = str(int(time.time() * 1000))
            sig = sign_pss_text(ts + method + path.split("?")[0])
            return {
                "KALSHI-ACCESS-KEY": KEY_ID,
                "KALSHI-ACCESS-SIGNATURE": sig,
                "KALSHI-ACCESS-TIMESTAMP": ts,
            }

        self.db = await aiosqlite.connect("data_orderbook.db")
        await self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS orderbook_events (
                ts_micro        TEXT,
                exch_ts_micro   TEXT,
                seq_num         BIGINT,
                ticker          TEXT,
                side            SMALLINT,
                price           BIGINT,
                signed_qty      BIGINT,
                is_delta        BOOLEAN
            );
        """
        )
        await self.db.commit()
        # --- reconnect loop ---
        while not self.app_state.shutdown_event.is_set():
            r = requests.get(
                "https://api.elections.kalshi.com/trade-api/v2/markets",
                params={"series_ticker": "KXHIGHNY", "status": "open"},
            )

            tickers = [m["ticker"] for m in r.json()["markets"]]
            logging.info(f"fetched {len(tickers)=}")
            headers = make_headers("GET", "/trade-api/ws/v2")
            try:
                async with websockets.connect(
                    self.url, additional_headers=headers
                ) as ws:
                    self.reconnect_delay = 1  # reset backoff
                    await ws.send(
                        json.dumps(
                            {
                                "id": 1,
                                "cmd": "subscribe",
                                "params": {
                                    "channels": ["orderbook_delta"],
                                    "market_tickers": tickers,
                                },
                            }
                        )
                    )
                    await ws.send(
                        json.dumps(
                            {
                                "id": 2,
                                "cmd": "subscribe",
                                "params": {"channels": ["market_lifecycle_v2"]},
                            }
                        )
                    )
                    await ws.send(
                        json.dumps(
                            {
                                "id": 3,
                                "cmd": "subscribe",
                                "params": {"channels": ["market_positions"]},
                            }
                        )
                    )

                    async for raw in ws:
                        await self.queue.put(
                            {
                                "source": self.name,
                                "data": json.loads(raw),
                                "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(
                                    timespec="microseconds"
                                ),
                            }
                        )

            except Exception as e:
                logging.info(
                    f"WebSocket error: {e}, reconnecting in {self.reconnect_delay}s"
                )
                await asyncio.sleep(self.reconnect_delay)
                self.reconnect_delay = min(
                    self.reconnect_delay * 2, self.max_reconnect_delay
                )

    async def process_queue(self) -> None:
        while not self.app_state.shutdown_event.is_set():
            msg = await self.queue.get()
            logging.info(msg)
            await self.app_state.broadcast_queue.put(msg)
            await self.insert_to_db(msg)

    async def insert_to_db(self, message: dict) -> None:
        data = message["data"]
        payload = data.get("msg", {})
        ts_local = message["timestamp"]

        ts_exch = payload.get("ts", "")
        seq = data.get("seq", 0)
        # ---- snapshots ----
        if data["type"] == "orderbook_snapshot":
            ticker = payload["market_ticker"]
            self.order_books[ticker] = OrderBook()
            self.order_books[ticker].process_snapshot(payload)
            for side_key, levels in (
                ("yes", payload.get("yes", [])),
                ("no", payload.get("no", [])),
            ):
                side = 1 if side_key == "yes" else -1
                for price_ticks, qty in levels:
                    await self.db.execute(
                        "INSERT INTO orderbook_events VALUES (?,?,?,?,?,?,?,?)",
                        (ts_local, ts_exch, seq, ticker, side, price_ticks, qty, False),
                    )

        # ---- deltas ----
        elif data["type"] == "orderbook_delta":
            ticker = payload["market_ticker"]
            self.order_books[ticker].process_delta(payload)
            side_delta = 1 if payload["side"] == "yes" else -1
            await self.db.execute(
                "INSERT INTO orderbook_events VALUES (?,?,?,?,?,?,?,?)",
                (
                    ts_local,
                    ts_exch,
                    seq,
                    ticker,
                    side_delta,
                    payload["price"],
                    payload["delta"],
                    True,
                ),
            )

        await self.db.commit()


# Fast polling data source (1 second interval)
class FastPollSource(DataSource):
    def __init__(self, app_state: "AppState"):
        super().__init__("fast_poll", app_state)
        self.poll_interval = 1.0

    async def start(self) -> None:
        while not self.app_state.shutdown_event.is_set():
            start_time = time.monotonic()

            try:
                async with asyncio.timeout(0.9):
                    await asyncio.sleep(0.05)  # Simulate network request
                    message = {
                        "source": self.name,
                        "data": "sample",
                        "timestamp": time.time(),
                    }
                    await self.queue.put(message)

            except Exception as e:
                logging.error(f"Fast poll error: {e}")

            # Ensure consistent polling interval
            elapsed = time.monotonic() - start_time
            await asyncio.sleep(max(0, self.poll_interval - elapsed))

    async def process_queue(self) -> None:
        while not self.app_state.shutdown_event.is_set():
            message = await self.queue.get()

            # Broadcast the message
            await self.app_state.broadcast_queue.put(message.copy())

            # Insert into database
            await self.insert_to_db(message)


# Slow polling data source (1 hour interval)
class SlowPollSource(DataSource):
    def __init__(self, app_state: "AppState"):
        super().__init__("slow_poll", app_state)

    async def start(self) -> None:
        while not self.app_state.shutdown_event.is_set():
            # Align to top of the hour
            now = datetime.now()
            next_hour = (now + timedelta(hours=1)).replace(
                minute=0, second=0, microsecond=0
            )
            delay = (next_hour - now).total_seconds()
            await asyncio.sleep(delay)

            try:
                async with asyncio.timeout(3590):  # Give ourselves 10s buffer
                    await asyncio.sleep(5)  # Simulate network request
                    message = {
                        "source": self.name,
                        "data": "sample",
                        "timestamp": time.time(),
                    }
                    await self.queue.put(message)

            except Exception as e:
                logging.error(f"Slow poll error: {e}")

    async def process_queue(self) -> None:
        while not self.app_state.shutdown_event.is_set():
            message = await self.queue.get()

            # Broadcast the message
            await self.app_state.broadcast_queue.put(message.copy())

            # Insert into database
            await self.insert_to_db(message)


# Global application state
class AppState:
    def __init__(self):
        # Data sources will be initialized here
        self.sources: List[DataSource] = []

        # Broadcast queue
        self.broadcast_queue = asyncio.Queue(maxsize=10_000)

        # WebSocket listeners
        self.clients: set[WebSocket] = set()

        # Task management
        self.tasks: List[asyncio.Task] = []
        self.shutdown_event = asyncio.Event()

        self.fast_app = FastAPI()

        # CORS
        self.fast_app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # WebSocket endpoint
        @self.fast_app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()
            self.clients.add(websocket)
            try:
                while True:
                    await websocket.receive_text()  # keep-alive
            except Exception:
                pass
            finally:
                self.clients.discard(websocket)

    def initialize_sources(self):
        """Initialize all data sources"""
        self.sources = [
            WebSocketSource(self),
            # FastPollSource(self),
            # SlowPollSource(self),
        ]

    async def broadcast_message(self):
        """
        Endless coroutine:
          - waits for the next message on broadcast_queue
          - pushes it to every currently-connected WebSocket client
          - removes disconnected clients automatically
        """
        while not self.shutdown_event.is_set():
            try:
                # pop with a small timeout so we can check shutdown_event periodically
                msg = await asyncio.wait_for(self.broadcast_queue.get(), timeout=0.2)
            except asyncio.TimeoutError:
                continue  # just loop back and check shutdown_event

            # Fast-fail if no one is listening
            if not self.clients:
                continue

            # Try to send to every client; remove dead ones
            disconnected = set()
            for ws in self.clients:
                try:
                    await ws.send_json(msg)
                except Exception:
                    # Any send failure → mark for removal
                    disconnected.add(ws)

            if disconnected:
                self.clients -= disconnected


async def start_websocket_server(app_state: AppState):
    config = uvicorn.Config(
        app=app_state.fast_app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
    )
    server = uvicorn.Server(config)
    logging.info("Starting Uvicorn server on http://0.0.0.0:8000")
    await server.serve()


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )
    app_state = AppState()
    app_state.initialize_sources()

    try:
        async with asyncio.TaskGroup() as tg:
            for src in app_state.sources:
                tg.create_task(src.start())
                tg.create_task(src.process_queue())

            tg.create_task(start_websocket_server(app_state))
            tg.create_task(app_state.broadcast_message())

            await app_state.shutdown_event.wait()

    except* Exception as eg:
        logging.exception("Fatal error in TaskGroup – starting graceful shutdown ...")
    finally:
        await graceful_shutdown(app_state)


async def graceful_shutdown(app_state: AppState):
    logging.info("Starting graceful shutdown...")
    app_state.shutdown_event.set()

    # Cancel all tasks
    for task in app_state.tasks:
        task.cancel()

    # Wait for tasks to complete (they'll raise CancelledError)
    await asyncio.gather(*app_state.tasks, return_exceptions=True)

    # Wait for queues to drain
    try:
        await asyncio.wait_for(
            asyncio.gather(
                *[source.queue.join() for source in app_state.sources],
                app_state.broadcast_queue.join(),
            ),
            timeout=0.25,
        )
    except asyncio.TimeoutError:
        logging.warning("Warning: Timed out waiting for queues to drain")

    for source in app_state.sources:
        if hasattr(source, "stop"):
            await source.stop()
        source.db_pool.shutdown()

    logging.info("Shutdown complete")


if __name__ == "__main__":
    p = pathlib.Path("logs")
    p.mkdir(exist_ok=True)

    # Root logger – everything goes to the rotating file at DEBUG
    file_handler = H.RotatingFileHandler(
        p / "app.log", maxBytes=1_000_000, backupCount=3
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)  # <- console only sees INFO+
    console_handler.setFormatter(
        logging.Formatter("%(levelname)s:%(name)s:[line:%(lineno)d] %(message)s")
    )

    logging.basicConfig(
        level=logging.DEBUG, handlers=[file_handler, console_handler]  # root level
    )
    app_state = AppState()
    app_state.initialize_sources()

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        asyncio.run(graceful_shutdown(app_state))
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        asyncio.run(graceful_shutdown(app_state))
