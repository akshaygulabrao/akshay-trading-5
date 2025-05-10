"""
Publishes orderbook data to ipc socket via zmq.
"""
import asyncio
import json
from collections import defaultdict

import zmq

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
        if side == "yes":
            self.publish_yes_orderbook(market_id)
    
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
        self.publish_yes_orderbook(market_id)
    
    def publish_yes_orderbook(self, market_id):
        self.pub.send_json({
            "market_ticker": market_id,
            "yes": dict(self.order_books[market_id]["yes"])
        })


ny_mkts = [mkt for mkt in utils.get_markets() if mkt.startswith("KXHIGHNY")]

client = OrderbookWebSocketClient(
    key_id=KEYID,
    private_key=private_key,
    environment=env,
    pub=pub
)

asyncio.run(client.connect(ny_mkts))