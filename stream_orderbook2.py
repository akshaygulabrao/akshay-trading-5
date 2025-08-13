import asyncio,logging,sys

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.websockets import WebSocketState
import uvicorn

from weather_extract_forecast import ForecastPoll
from weather_sensor_reading import SensorPoll
from orderbook import ObWebsocket

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

async def consumer(queue: asyncio.Queue):
    """
    Waits for messages from your producers and immediately
    pushes them to every open WebSocket via ConnectionManager.
    """
    while True:
        message = await queue.get()

        await manager.broadcast(message)



async def main() -> None:
    queue = asyncio.Queue(maxsize=10_000)

    producers = [
        ForecastPoll(queue, "forecast.db"),
        SensorPoll(queue, "weather.db"),
        ObWebsocket(queue, "data/data_orderbook.db"),
    ]
    producer_tasks = [asyncio.create_task(p.run()) for p in producers]

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