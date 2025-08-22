#!.venv/bin/python
import os,base64
import functools
import json
import time
import asyncio
import logging
import signal
from typing import Dict, Any, Set, Protocol, Callable

from websockets.asyncio.server import Server,ServerConnection,serve,broadcast
from websockets.exceptions import ConnectionClosed
import websockets

import requests
from sortedcontainers import SortedDict
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding


class KalshiOrderBook:

    def __init__(self, queue: asyncio.Queue, tickers: list[str]):
        self.queue = queue
        self.tickers = tickers

        self.url = "wss://api.elections.kalshi.com/trade-api/ws/v2"
        self.ws: websockets.WebSocketClientProtocol | None = None

        self.reconnect_delay = 1
        self.max_reconnect_delay = 60

        # Map: market_ticker -> {market_id, yes: SortedDict, no: SortedDict}
        self.books: Dict[str, Dict[str, Any]] = {}

        self.orderbook_delta_id: int | None = None

        self.unsubscribed_event = asyncio.Event()

    def _process_snapshot(self, msg: Dict[str, Any]) -> None:
        try:
            ticker = msg["market_ticker"]
            market_id = msg.get("market_id")

            if ticker not in self.books:
                self.books[ticker] = {
                    "market_id": market_id,
                    "yes": SortedDict(lambda x: -x),
                    "no": SortedDict(lambda x: -x),
                }

            self.books[ticker]["yes"].clear()
            self.books[ticker]["no"].clear()

            for side in ("yes", "no"):
                if side in msg:
                    for price, volume in msg[side]:
                        self.books[ticker][side][price] = volume
        except Exception:
            logging.exception("Error processing snapshot: %s", msg)

    def _process_delta(self, msg: Dict[str, Any]) -> None:
        try:
            ticker = msg["market_ticker"]
            side = msg["side"]  # "yes" or "no"
            price = msg["price"]
            delta = msg["delta"]

            if ticker not in self.books:
                logging.debug("Delta for unknown ticker %s, ignoring", ticker)
                return

            current = self.books[ticker][side].get(price, 0)
            new = current + delta
            if new > 0:
                self.books[ticker][side][price] = new
            else:
                self.books[ticker][side].pop(price, None)
        except Exception:
            logging.exception("Error processing delta: %s", msg)

    def _emit_top(self, ticker: str) -> None:
        if ticker not in self.books:
            return

        try:
            yes_top = next(iter(self.books[ticker]["yes"]), None)
            no_top = next(iter(self.books[ticker]["no"]), None)

            # Safely fetch volumes and build strings
            if yes_top is not None:
                yes_vol = self.books[ticker]["yes"].get(yes_top, 0)
                no_str = f"{100 - yes_top}@{yes_vol}"
            else:
                no_str = "N/A"

            if no_top is not None:
                no_vol = self.books[ticker]["no"].get(no_top, 0)
                yes_str = f"{100 - no_top}@{no_vol}"
            else:
                yes_str = "N/A"

            payload = {"ticker": ticker, "no": no_str, "yes": yes_str}

            try:
                self.queue.put_nowait({"type": "orderbook", "data": payload})
            except Exception:
                # put_nowait can raise if the queue is bounded and full
                logging.exception("Failed to enqueue orderbook payload for %s", ticker)
        except Exception:
            logging.exception("Error emitting top-of-book for %s", ticker)

    @staticmethod
    def _sign(priv_key, text: str) -> str:
        sig = priv_key.sign(
            text.encode(),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(sig).decode()

    def _auth_headers(self, priv_key) -> Dict[str, str]:
        ts = str(int(time.time() * 1000))
        sig = self._sign(priv_key, ts + "GET" + "/trade-api/ws/v2")
        try:
            access_key = os.environ["PROD_KEYID"]
        except KeyError:
            logging.error("Environment variable PROD_KEYID not set")
            raise
        return {
            "KALSHI-ACCESS-KEY": access_key,
            "KALSHI-ACCESS-SIGNATURE": sig,
            "KALSHI-ACCESS-TIMESTAMP": ts,
        }

    async def _resubscribe(self) -> None:
        if self.ws is None:
            logging.debug("No upstream websocket to resubscribe on")
            return
        try:
            # websockets.protocol.State may vary depending on version; guard with attribute checks
            if getattr(self.ws, "state", None) is not None and self.ws.state != websockets.protocol.State.OPEN:
                logging.debug("Upstream websocket not open; skipping resubscribe")
                return
            if self.orderbook_delta_id is not None:
                self.unsubscribed_event.clear()
                await self.ws.send(
                    json.dumps(
                        {
                            "id": 1,
                            "cmd": "unsubscribe",
                            "params": {
                                "sids": [self.orderbook_delta_id],
                                "market_tickers": self.tickers,
                                "action": "remove",
                            },
                        }
                    )
                )
                await asyncio.wait_for(self.unsubscribed_event.wait(),timeout=5)


            await self.ws.send(
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
        except (websockets.exceptions.ConnectionClosed, OSError) as e:
            logging.warning("Cannot resubscribe, upstream ws closed: %s", e)
        except Exception:
            logging.exception("Unexpected error while resubscribing")

    async def run(self) -> None:
        priv_path = os.environ.get("PROD_KEYFILE")
        if not priv_path:
            logging.error("PROD_KEYFILE env var not set; orderbook task exiting")
            return

        try:
            with open(priv_path, "rb") as f:
                priv_key = serialization.load_pem_private_key(f.read(), password=None)
        except Exception:
            logging.exception("Failed to load private key from %s; orderbook task exiting", priv_path)
            return

        delay = self.reconnect_delay
        while True:
            try:
                # compute headers at each connect attempt so signature/timestamp are fresh
                headers = self._auth_headers(priv_key)

                async with websockets.connect(self.url, additional_headers=headers) as ws:
                    self.ws = ws
                    logging.info("WebSocket connected")
                    delay = self.reconnect_delay  # reset backoff

                    await self._resubscribe()

                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                        except json.JSONDecodeError:
                            logging.exception("Failed to decode JSON from upstream: %s", raw)
                            continue

                        msg = data.get("msg", {})
                        typ = data.get("type")

                        if typ == "subscribed" and msg.get("channel") == "orderbook_delta":
                            self.orderbook_delta_id = msg.get("sid")
                            self.unsubscribed_event.clear()
                        elif typ == "unsubscribed":
                            self.unsubscribed_event.set()
                        elif typ == "orderbook_snapshot":
                            self._process_snapshot(msg)
                            self._emit_top(msg.get("market_ticker", "unknown"))
                        elif typ == "orderbook_delta":
                            self._process_delta(msg)
                            self._emit_top(msg.get("market_ticker", "unknown"))
                        else:
                            # unrecognized message type; log at debug level
                            logging.debug("Upstream message of unknown type: %s", data)

            except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as exc:
                logging.warning("WebSocket closed (%s); reconnecting in %ss", exc, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, self.max_reconnect_delay)
            except Exception as e:
                logging.error(e)
                raise

class Manager:
    def __init__(self, queue: asyncio.Queue):
        self.queue = queue
        self.server: Server | None = None
        self.connections: set[ServerConnection] = set()

    async def handler(self, websocket: ServerConnection) -> None:
        self.connections.add(websocket)
        if kalshi_orderbook := getattr(self, "_ob", None):
            asyncio.create_task(kalshi_orderbook._resubscribe())
        try:
            async for message in websocket:
                pass  # ignore inbound messages
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            self.connections.discard(websocket)

    async def broadcast(self,msg) -> None:
        for websocket in self.connections.copy():
            try:
                await websocket.send(json.dumps(msg))
            except ConnectionClosed:
                pass

    async def relay(self) -> None:
        try:
            while True:
                msg = await self.queue.get()
                logging.info(msg)
                await self.broadcast(msg)
        except asyncio.CancelledError:
            pass
        except Exception:
            logging.exception("relay task error")
            raise
        finally:
            if self.server:
                self.server.close()
                await self.server.wait_closed()

    async def start_server(self) -> None:
        try:
            async with serve(self.handler, "localhost", 8000, ping_interval=5) as srv:
                self.server = srv
                await self.relay()
        except asyncio.CancelledError:
            pass
        except Exception:
            logging.exception("server task error")
            raise
        finally:
            if self.server:
                self.server.close()
                await self.server.wait_closed()

async def main():
    logging.basicConfig(level=logging.INFO)

    tickers = []
    try:
        for series in ("KXWTAMATCH", "KXMLBGAME"):
            r = requests.get(
                "https://api.elections.kalshi.com/trade-api/v2/markets",
                params={"series_ticker": series, "status": "open"},
                timeout=5,
            )
            r.raise_for_status()
            tickers.extend([m["ticker"] for m in r.json().get("markets", [])])
    except Exception:
        logging.exception("Error fetching tickers; continuing with whatever we have")

    logging.info("Tickers: %s", tickers)

    q: asyncio.Queue = asyncio.Queue()
    m = Manager(q)
    kalshi_orderbook = KalshiOrderBook(q, tickers)
    m._ob = kalshi_orderbook
    ob_task = asyncio.create_task(kalshi_orderbook.run(), name="kalshi_orderbook")
    relay_task = asyncio.create_task(m.start_server(), name="relay")
    await asyncio.gather(relay_task,ob_task)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Program interrupted by user; shutting down")
    except Exception:
        logging.exception("Unexpected error in program")
