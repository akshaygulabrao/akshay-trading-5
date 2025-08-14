import asyncio,logging,sys,os
from pathlib import Path
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.websockets import WebSocketState
import uvicorn

from weather_extract_forecast import ForecastPoll
from weather_sensor_reading import SensorPoll
from orderbook import ObWebsocket
from demo_graph_readings_forecast import graph_readings_forecast

from functools import partial
import concurrent.futures

class ConnectionManager:
    """
    Keeps track of every active WebSocket so we can broadcast
    a single message to all of them.
    """
    def __init__(self):
        self.active: set[WebSocket] = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.add(ws)

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
            except Exception:          # ConnectionClosed, etc.
                dead.add(ws)
        # Clean-up in a second pass so we don’t mutate while iterating
        for ws in dead:
            self.active.discard(ws)

manager = ConnectionManager()

app = FastAPI()

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            # Keep the socket open; we never expect inbound messages
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)

async def graph_readings_forecast_async(queue):
    airports = ["KNYC", "KMDW", "KAUS", "KMIA", "KDEN", "KPHL", "KLAX"]
    loop = asyncio.get_running_loop()

    async def worker():
        with concurrent.futures.ThreadPoolExecutor() as pool:
            while True:
                tasks = []
                for apt in airports:
                    def _run(a=apt):             # capture airport name for logging
                        try:
                            return graph_readings_forecast(a, 2)
                        except Exception as exc:
                            logging.exception("Failed for %s: %s", a, exc)
                            return None          # or sentinel object
                    tasks.append(loop.run_in_executor(pool, _run))

                # gather with return_exceptions=True so one bad airport
                # doesn’t cancel the rest
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Filter out None or Exception objects if you only want successes
                data = [r for r in results if not isinstance(r, Exception) and r is not None]
                msg = {"type": "graph", "data": data}
                await queue.put(msg)

                await asyncio.sleep(5)
    while True:
        try:
            await worker()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logging.exception("Unexpected error in forecast loop: %s", e)
            await asyncio.sleep(5) 
    
async def consumer(queue: asyncio.Queue):
    """
    Waits for messages from your producers and immediately
    pushes them to every open WebSocket via ConnectionManager.
    """
    while True:
        message = await queue.get()
        if message["type"] == "graph":
            logging.info("graph")
        if message["type"] == "graph" or message["type"] == "orderbook":
            await manager.broadcast(message)



async def main() -> None:
    queue = asyncio.Queue(maxsize=10_000)

    def _require_envs(*names):
        for n in names:
            p = os.getenv(n)
            if p is None or not Path(p).exists():
                sys.exit(f"Missing or invalid env var {n}")

    _require_envs("FORECAST_DB_PATH", "WEATHER_DB_PATH", "ORDERBOOK_DB_PATH")
    producers = [
        ForecastPoll(queue, os.getenv("FORECAST_DB_PATH")),
        SensorPoll(queue, os.getenv("WEATHER_DB_PATH")),
        ObWebsocket(queue, os.getenv("ORDERBOOK_DB_PATH")),
    ]

    producer_tasks = [asyncio.create_task(p.run()) for p in producers]
    producer_tasks.append(asyncio.create_task(graph_readings_forecast_async(queue)))

    consumer_task = asyncio.create_task(consumer(queue))

    server = uvicorn.Server(
        uvicorn.Config(app, host="0.0.0.0", port=8000, log_level="info")
    )
    server_task = asyncio.create_task(server.serve())

    await asyncio.gather(*producer_tasks, consumer_task, server_task,
                         return_exceptions=True)

if __name__ == "__main__":
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    for h in root.handlers[:]:
        root.removeHandler(h)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    root.addHandler(handler)

    asyncio.run(main())