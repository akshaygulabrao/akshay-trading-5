import asyncio
import aiosqlite
import logging
import datetime
import logging

from orderbook_update import OrderBook

import os, base64, time, json, requests, websockets
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding

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
        self.tickers = []
        self.orderbook_delta_id = -1

    async def resubscribe(self, ws):
            await ws.send(
                json.dumps(
                    {
                        "id": 1,
                        "cmd": "unsubscribe",
                        "params": {
                            "sids": [self.orderbook_delta_id],
                            "market_tickers": self.tickers,
                            "action": "remove"
                        },
                    }
                )
            )
            await asyncio.get_event_loop().run_in_executor(None,self.active_tickers)
            await ws.send(
                json.dumps(
                    {
                        "id": 1,
                        "cmd": "subscribe",
                        "params": {
                            "channels": ["orderbook_delta"],
                            "market_tickers": self.tickers,
                        },
                    }
                )
            )
        
    async def heartbeat(self,ws) -> None:
        while True:
            await asyncio.sleep(300)
            await self.resubscribe(ws)
    
    def active_tickers(self):
        self.tickers = []
        for city in ["NY", "CHI", "MIA", "AUS", "DEN", "PHIL", "LAX"]:
            r = requests.get(
                "https://api.elections.kalshi.com/trade-api/v2/markets",
                params={"series_ticker": f"KXHIGH{city}", "status": "open"},
                timeout=2,
            )
            self.tickers.extend([m["ticker"] for m in r.json()["markets"]])

    async def run(self) -> None:
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

        await asyncio.get_event_loop().run_in_executor(None, self.active_tickers)
        logging.info(f"fetched {len(self.tickers)=}")

        async with aiosqlite.connect(self.db_file) as db:
            await db.execute(CREATE_TABLE_SQL)
            await db.commit()

        headers = auth_headers("GET", "/trade-api/ws/v2")
        async with websockets.connect(self.url, additional_headers=headers) as ws:

            await ws.send(
                json.dumps(
                    {
                        "id": 1,
                        "cmd": "subscribe",
                        "params": {
                            "channels": ["orderbook_delta"],
                            "market_tickers": self.tickers,
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
            asyncio.create_task(self.heartbeat(ws))
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
                if data["type"] == "subscribed" and msg["channel"] == "orderbook_delta":
                    self.orderbook_delta_id = msg["sid"]

                elif data["type"] in {"orderbook_snapshot", "orderbook_delta"}:
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
                    await self.queue.put({"type": "orderbook", "data": mkt})

                end = time.perf_counter()
                logging.info(
                    "%s took %.0f us",
                    self.__class__.__name__,
                    (end - start) * 1e6,
                )