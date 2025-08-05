# relay.py
"""
High-level asyncio relay.
    - Three data sources (websocket, 1 s HTTP poller, 1 h HTTP poller)
    - Every message is broadcast to all connected websocket listeners
    - Each message is also INSERT-ed into its own SQLite DB
"""

import asyncio
import json
import sqlite3
import time
from collections.abc import Awaitable
from datetime import datetime, timedelta
from typing import Any
from weakref import WeakSet

import websockets
from websockets.server import WebSocketServerProtocol

# ---------- configuration -------------------------------------------------

HTTP_POLL_URL_1S = "https://httpbin.org/uuid"  # returns {"uuid": "…"}
HTTP_POLL_URL_1H = "https://httpbin.org/uuid"  # identical for demo
WS_SOURCE_URL = "wss://echo.websocket.org/"  # public echo service
WS_LISTEN_HOST, WS_LISTEN_PORT = "localhost", 8765

DB_NAMES = ["one.db", "two.db", "three.db"]
QUEUE_MAX = 10_000

# ---------- global objects -------------------------------------------------

broadcast_q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=QUEUE_MAX)
source_queues = [asyncio.Queue(maxsize=QUEUE_MAX) for _ in range(3)]
db_pools = [sqlite3.connect(db, check_same_thread=False) for db in DB_NAMES]
active_listeners: WeakSet[WebSocketServerProtocol] = WeakSet()

# ---------- helper utilities ---------------------------------------------


async def sleep_until_next_hour() -> None:
    """Sleep until the top of the next wall-clock hour."""
    now = datetime.utcnow()
    next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    await asyncio.sleep((next_hour - now).total_seconds())


import logging

LOG_LEVEL = logging.INFO  # or DEBUG / WARNING / ERROR / CRITICAL
LOG_FORMAT = "[%(asctime)s] %(levelname)s %(name)s - %(message)s"
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT, datefmt="%Y-%m-%dT%H:%M:%S")
logger = logging.getLogger("relay")


def log(msg: str) -> None:
    logger.info(msg)


# ---------- SQLite helpers ------------------------------------------------


def create_tables() -> None:
    for db in db_pools:
        with db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY, ts REAL, payload TEXT)"
            )


async def insert_with_retry(pool: sqlite3.Connection, payload: str) -> None:
    backoff = 0.1
    while True:
        try:
            await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: pool.execute(
                    "INSERT INTO messages (ts, payload) VALUES (?, ?)",
                    (time.time(), payload),
                ),
            )
            pool.commit()
            return
        except sqlite3.OperationalError:  # busy / locked
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 5)


# ---------- data-source coroutines ----------------------------------------


async def ws_source(queue: asyncio.Queue[dict[str, Any]]) -> None:
    backoff = 1
    while True:
        try:
            async with websockets.connect(WS_SOURCE_URL) as ws:
                log("WS source connected")
                backoff = 1
                await ws.send(json.dumps({"subscribe": "demo"}))  # keeps echo busy
                async for msg in ws:
                    await queue.put({"source": 0, "data": msg})
        except Exception as exc:
            log(f"WS source error: {exc}, reconnecting in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


async def poll_every_1s(queue: asyncio.Queue[dict[str, Any]]) -> None:
    session = None
    while True:
        try:
            async with asyncio.timeout(1):
                if session is None:
                    reader, writer = await asyncio.open_connection(
                        "httpbin.org", 443, ssl=True
                    )
                    session = (reader, writer)
                writer = session[1]
                writer.write(
                    b"GET /uuid HTTP/1.1\r\nHost: httpbin.org\r\nUser-Agent: relay\r\n\r\n"
                )
                await writer.drain()
                line = await reader.readline()
                while line.strip():
                    line = await reader.readline()
                payload = (await reader.readline()).decode()
                data = json.loads(payload)
                await queue.put({"source": 1, "data": data})
        except Exception as exc:
            log(f"1s poller error: {exc}")
            if session:
                session[1].close()
                session = None
            await asyncio.sleep(1)


async def poll_every_1h(queue: asyncio.Queue[dict[str, Any]]) -> None:
    await sleep_until_next_hour()
    while True:
        try:
            reader, writer = await asyncio.open_connection("httpbin.org", 443, ssl=True)
            writer.write(
                b"GET /uuid HTTP/1.1\r\nHost: httpbin.org\r\nUser-Agent: relay\r\n\r\n"
            )
            await writer.drain()
            line = await reader.readline()
            while line.strip():
                line = await reader.readline()
            payload = (await reader.readline()).decode()
            data = json.loads(payload)
            await queue.put({"source": 2, "data": data})
            writer.close()
            await writer.wait_closed()
        except Exception as exc:
            log(f"1h poller error: {exc}")
        await sleep_until_next_hour()


# ---------- per-queue processor -------------------------------------------


async def processor(idx: int, in_q: asyncio.Queue[dict[str, Any]]) -> None:
    db = db_pools[idx]
    while True:
        msg = await in_q.get()
        try:
            log(msg.copy())
            await broadcast_q.put(msg.copy())
            await insert_with_retry(db, json.dumps(msg))
        except Exception as exc:
            log(f"processor {idx} failed to insert: {exc}")


# ---------- broadcaster ---------------------------------------------------


async def broadcaster() -> None:
    while True:
        msg = await broadcast_q.get()
        if not active_listeners:
            continue
        # fan-out to every connected listener concurrently
        coros: list[Awaitable[None]] = []
        for ws in list(active_listeners):
            coros.append(asyncio.create_task(ws.send(json.dumps(msg))))
        # wait for all sends; remove dead sockets
        results = await asyncio.gather(*coros, return_exceptions=True)
        for ws, res in zip(list(active_listeners), results):
            if isinstance(res, Exception):
                active_listeners.discard(ws)


# ---------- websocket listener server -------------------------------------


async def listener_handler(ws: WebSocketServerProtocol) -> None:
    active_listeners.add(ws)
    log(f"listener connected ({len(active_listeners)} total)")
    try:
        await ws.wait_closed()
    finally:
        active_listeners.discard(ws)
        log(f"listener disconnected ({len(active_listeners)} total)")


async def listener_server() -> None:
    async with websockets.serve(listener_handler, WS_LISTEN_HOST, WS_LISTEN_PORT):
        log(f"listening on ws://{WS_LISTEN_HOST}:{WS_LISTEN_PORT}")
        await asyncio.Future()  # run forever


# ---------- graceful shutdown ---------------------------------------------


class TerminateTaskGroup(Exception):
    """Exception raised to terminate a task group."""


async def force_terminate_task_group() -> None:
    raise TerminateTaskGroup()


async def graceful_shutdown(tg: asyncio.TaskGroup) -> None:
    log("shutting down…")
    # add the terminator task to the *same* TaskGroup
    tg.create_task(force_terminate_task_group())


# ---------- main ----------------------------------------------------------


async def main() -> None:
    create_tables()
    async with asyncio.TaskGroup() as tg:
        tg.create_task(ws_source(source_queues[0]))
        tg.create_task(poll_every_1s(source_queues[1]))
        tg.create_task(poll_every_1h(source_queues[2]))
        for i in range(3):
            tg.create_task(processor(i, source_queues[i]))
        tg.create_task(broadcaster())
        tg.create_task(listener_server())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
