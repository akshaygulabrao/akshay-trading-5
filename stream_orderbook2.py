import asyncio, logging, sys, os,requests
from pathlib import Path
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.websockets import WebSocketState
from starlette.websockets import WebSocket, WebSocketState
import uvicorn

from weather_extract_forecast import ForecastPoll
from weather_sensor_reading import SensorPoll
from orderbook import ObWebsocket

from functools import partial
from orderbook_trader import OrderbookTrader

class ConnectionManager:
    """
    Keeps track of every active WebSocket so we can broadcast
    a single message to all of them.
    """

    def __init__(self, producers, callbacks):
        self.active: set[WebSocket] = set()
        self.producers = producers
        self.callbacks = callbacks or []

    async def connect(self, ws: WebSocket):
        await ws.accept()

        self.active.add(ws)
        for p in self.producers:
            if hasattr(p, "resubscribe"):
                asyncio.create_task(p.resubscribe())

    def disconnect(self, ws: WebSocket):
        self.active.discard(ws)

    async def broadcast(self, message: dict):
        """
        Try to send `message` to every connected client.
        Dead connections are removed automatically.
        """
        dead = set()
        for ws in self.active:
            try:
                if ws.client_state == WebSocketState.CONNECTED:
                    await ws.send_json(message)
            except Exception:  # ConnectionClosed, etc.
                dead.add(ws)
        # Clean-up in a second pass so we don’t mutate while iterating
        for ws in dead:
            self.active.discard(ws)

    async def relay(self, queue: asyncio.Queue):
        """
        Waits for messages from your producers and immediately
        pushes them to every open WebSocket via ConnectionManager.
        """
        try:
            while True:
                message = await queue.get()
                if message["type"] == "SensorPoll":
                    await self.broadcast(message)
                elif message["type"] == "ForecastPoll":
                    await self.broadcast(message)
                elif message["type"] == "orderbook":
                    await self.broadcast(message)
                elif message["type"] == "positionUpdate":
                    await self.broadcast(message)

                for cb in self.callbacks:
                    try:
                        await cb(message)
                    except Exception as e:
                        raise ValueError(e)
        except Exception as e:
            raise Exception(e)
        finally:
            sys.exit(1)


async def main() -> None:
    queue = asyncio.Queue(maxsize=10_000)

    app = FastAPI()

    @app.websocket("/ws")
    async def websocket_endpoint(ws: WebSocket):
        await manager.connect(ws)
        try:
            while ws.client_state == WebSocketState.CONNECTED:
                await ws.send_json({"type": "heartbeat"})
                await asyncio.sleep(25)
        except (WebSocketDisconnect, RuntimeError):
            manager.disconnect(ws)
            pass

    def _require_envs(*names):
        for n in names:
            p = os.getenv(n)
            if p is None or not Path(p).exists():
                sys.exit(f"Missing or invalid env var {n}")

    _require_envs("FORECAST_DB_PATH", "WEATHER_DB_PATH", "ORDERS_DB_PATH")
    tickers = []
    for city in ["NY", "CHI", "MIA", "AUS", "DEN", "PHIL", "LAX"]:
        r = requests.get(
            "https://api.elections.kalshi.com/trade-api/v2/markets",
            params={"series_ticker": f"KXHIGH{city}", "status": "open"},
            timeout=2,
            )
        tickers.extend([m["ticker"] for m in r.json()["markets"]])
    r = requests.get(
        "https://api.elections.kalshi.com/trade-api/v2/markets",
        params={"series_ticker": f"KXWTAMATCH", "status": "open"},
        timeout=2,
        )
    tickers.extend([m['ticker'] for m in r.json()['markets']])
    logging.info(tickers)
    producers = [
        #ForecastPoll(queue, os.getenv("FORECAST_DB_PATH")),
        #SensorPoll(queue, os.getenv("WEATHER_DB_PATH")),
        ObWebsocket(queue, os.getenv("ORDERBOOK_DB_PATH"),tickers),
    ]
    tickers = [t for t in tickers if "KXHIGHAUS-25AUG20" in t or "KXWTAMATCH-25AUG19ANNJOV" in t]
    trader = OrderbookTrader(queue, os.getenv("ORDERS_DB_PATH"), tickers)
    await trader.initialize_positions()
    manager = ConnectionManager(producers, [trader.on_message])

    producer_tasks = [asyncio.create_task(p.run()) for p in producers]

    consumer_tasks = [asyncio.create_task(manager.relay(queue))]
    position_task = asyncio.create_task(trader.update_positions())
    balance_task = asyncio.create_task(trader.update_balance())
    consumer_tasks.append(position_task)
    consumer_tasks.append(balance_task)

    server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=8000, log_level="info"))
    server_task = asyncio.create_task(server.serve())

    await asyncio.gather(*producer_tasks, *consumer_tasks, server_task, return_exceptions=True)


if __name__ == "__main__":

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    for h in root.handlers[:]:
        root.removeHandler(h)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    root.addHandler(handler)

    asyncio.run(main())
