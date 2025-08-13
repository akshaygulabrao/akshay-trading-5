import asyncio
import sqlite3
import aiosqlite
import time
import weakref
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor
import logging.handlers as H, logging, pathlib
import logging
import pathlib
import sys
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import aiohttp
import datetime

from orderbook_update import OrderBook
from weather_extract_forecast import ForecastPoll
from weather_sensor_reading import SensorPoll

CREATE_TABLE_SQL = """
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

INSERT_ROW_SQL = "INSERT INTO orderbook_events VALUES (?,?,?,?,?,?,?,?)"


class ObWebsocket:
    def __init__(self, queue: asyncio.Queue, db_file: str):
        self.reconnect_delay = 1
        self.max_reconnect_delay = 60
        self.order_books: dict[str, OrderBook] = {}
        self.url = "wss://api.elections.kalshi.com/trade-api/ws/v2"
        self.db_file = db_file
        self.queue = queue

    # ------------------------------------------------------------------
    #  single coroutine: connect, subscribe, receive, store & log
    # ------------------------------------------------------------------
    async def run(self) -> None:
        import os, base64, time, json, requests, websockets
        from cryptography.hazmat.primitives import serialization, hashes
        from cryptography.hazmat.primitives.asymmetric import padding

        KEY_ID = os.getenv("PROD_KEYID")
        PRIV_PATH = os.getenv("PROD_KEYFILE")

        with open(PRIV_PATH, "rb") as f:
            priv = serialization.load_pem_private_key(f.read(), password=None)

        def sign(text: str) -> str:
            sig = priv.sign(
                text.encode(),
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.DIGEST_LENGTH,
                ),
                hashes.SHA256(),
            )
            return base64.b64encode(sig).decode()

        def auth_headers(method: str, path: str) -> dict:
            ts = str(int(time.time() * 1000))
            sig = sign(ts + method + path.split("?")[0])
            return {
                "KALSHI-ACCESS-KEY": KEY_ID,
                "KALSHI-ACCESS-SIGNATURE": sig,
                "KALSHI-ACCESS-TIMESTAMP": ts,
            }

        # 1. build ticker list once
        tickers = []
        for city in ["NY", "CHI", "MIA", "AUS", "DEN", "PHIL", "LAX"]:
            r = requests.get(
                "https://api.elections.kalshi.com/trade-api/v2/markets",
                params={"series_ticker": f"KXHIGH{city}", "status": "open"},
                timeout=2,
            )
            tickers.extend([m["ticker"] for m in r.json()["markets"]])
        logging.info(f"fetched {len(tickers)=}")

        async with aiosqlite.connect(self.db_file) as db:
            await db.execute(CREATE_TABLE_SQL)
            await db.commit()

        headers = auth_headers("GET", "/trade-api/ws/v2")
        async with websockets.connect(self.url, additional_headers=headers) as ws:
            self.reconnect_delay = 1

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
                start = time.perf_counter()
                data = json.loads(raw)
                msg = data.get("msg", {})
                ts_l = datetime.datetime.now(datetime.timezone.utc).isoformat(
                    timespec="microseconds"
                )
                ts_e = msg.get("ts", "")
                seq = data.get("seq", 0)

                if data["type"] == "orderbook_snapshot":
                    ticker = msg["market_ticker"]
                    self.order_books[ticker] = OrderBook()
                    self.order_books[ticker].process_snapshot(msg)

                    for side_key, levels in (
                        ("yes", msg.get("yes", [])),
                        ("no", msg.get("no", [])),
                    ):
                        async with aiosqlite.connect(self.db_file) as db:
                            side = 1 if side_key == "yes" else -1
                            for px, qty in levels:
                                await db.execute(
                                    INSERT_ROW_SQL,
                                    (ts_l, ts_e, seq, ticker, side, px, qty, False),
                                )

                elif data["type"] == "orderbook_delta":
                    ticker = msg["market_ticker"]
                    if ticker in self.order_books:
                        self.order_books[ticker].process_delta(msg)
                    side = 1 if msg["side"] == "yes" else -1
                    async with aiosqlite.connect(self.db_file) as db:
                        await db.execute(
                            INSERT_ROW_SQL,
                            (
                                ts_l,
                                ts_e,
                                seq,
                                ticker,
                                side,
                                msg["price"],
                                msg["delta"],
                                True,
                            ),
                        )

                        await db.commit()

                if data["type"] in {"orderbook_snapshot", "orderbook_delta"}:
                    market_ticker = msg["market_ticker"]
                    ob = self.order_books[market_ticker]
                    yes_top = next(iter(ob.markets[market_ticker]["yes"]), None)
                    no_top = next(iter(ob.markets[market_ticker]["no"]), None)

                    mkt = {
                        "ticker": market_ticker,
                        "no": f"{100 - yes_top}@{ob.markets[market_ticker]['yes'][yes_top]}"
                        if yes_top
                        else "N/A",
                        "yes": f"{100 - no_top}@{ob.markets[market_ticker]['no'][no_top]}"
                        if no_top
                        else "N/A",
                    }
                    end = time.perf_counter()
                    await self.queue.put({"type": "orderbook", "data": mkt})
                    logging.info(
                        "%s took %.0f us",
                        self.__class__.__name__,
                        (end - start) * 1e6,
                    )

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
