"""
Publishes orderbook data to ipc socket via zmq.
"""
import asyncio
import json
from collections import defaultdict

import zmq
import websockets

from original.clients import KalshiWebSocketClient
import utils

# Load environment variables
KEYID, private_key, env = utils.setup_prod()

context = zmq.Context()
pub = context.socket(zmq.PUB)
pub.bind("ipc:///tmp/orderbook.ipc")

class OrderbookWebSocketClient(KalshiWebSocketClient):
    def __init__(self, key_id, private_key, environment, pub):
        super().__init__(key_id, private_key, environment)
        self.order_books = {}
        self.pub = pub
    
    async def connect(self, tickers):
        """Establishes a WebSocket connection and runs concurrent tasks."""
        host = self.WS_BASE_URL + self.url_suffix
        auth_headers = self.request_headers("GET", self.url_suffix)
        async with websockets.connect(host, additional_headers=auth_headers) as websocket:
            self.ws = websocket
            await self.on_open(tickers)
            
            # Create tasks for handler and periodic publishing
            handler_task = asyncio.create_task(self.handler())
            periodic_task = asyncio.create_task(self.periodic_publish())
            
            # Run both tasks concurrently
            await asyncio.gather(handler_task, periodic_task)


    async def periodic_publish(self):
        """Periodically publishes orderbook data every second."""
        while True:
            await asyncio.sleep(1)
            for market_id in list(self.order_books.keys()):  # Use a copy to avoid dict size change during iteration
                self.publish_orderbook(market_id)
            

    async def on_message(self, message_str):
        message = json.loads(message_str)
        if message["type"] == "orderbook_delta":
            self.handle_orderbook_delta(message)
        elif message["type"] == "orderbook_snapshot":
            self.handle_orderbook_snapshot(message)
        elif message["type"] == "subscribed":
            print("Websocket connected and subscribed to orderbook.")
        else:
            print("Unknown message type:", message["type"])
    
    def handle_orderbook_delta(self, delta):
        market_id = delta["msg"]["market_ticker"]
        if market_id not in self.order_books:
            print(f"Warning: Received delta for unknown market {market_id}")
            return
        side = delta["msg"]["side"]
        price = delta["msg"]["price"]
        size = delta["msg"]["delta"]
        book = self.order_books[market_id][side]
        book[price] += size
        if book[price] <= 0:
            del book[price]
        self.publish_orderbook(market_id)

    def handle_orderbook_snapshot(self, snapshot):
        market_id = snapshot["msg"]["market_ticker"]
        if market_id not in self.order_books:
            self.order_books[market_id] = {"yes": defaultdict(int), "no": defaultdict(int)}
        if "no" in snapshot["msg"]:
            for price, volume in snapshot["msg"]["no"]:
                self.order_books[market_id]["no"][price] = volume
        if "yes" in snapshot["msg"]:
            for price, volume in snapshot["msg"]["yes"]:
                self.order_books[market_id]["yes"][price] = volume
        self.publish_orderbook(market_id)

    def publish_orderbook(self, market_id):
        """Publishes both Yes and converted No orderbooks"""
        yes_book = self.order_books[market_id]["yes"]
        no_book = self.order_books[market_id]["no"]
        
        
        self.pub.send_json({
            "market_ticker": market_id,
            "yes": dict(yes_book),
            "no": dict(no_book),
        })
    async def on_close(self):
        """Clean up resources"""
        self.periodic_task.cancel()
        try:
            await self.periodic_task
        except asyncio.CancelledError:
            pass
        await super().on_close()

ny_mkts = [mkt for mkt in utils.get_markets() if mkt.startswith("KXHIGHNY")]

client = OrderbookWebSocketClient(
    key_id=KEYID,
    private_key=private_key,
    environment=env,
    pub=pub
)

asyncio.run(client.connect(ny_mkts))