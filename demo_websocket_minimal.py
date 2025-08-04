import asyncio
import base64
import json
import time
import websockets
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding

import os
import utils

# Configuration
KEY_ID = os.getenv("PROD_KEYID")
PRIVATE_KEY_PATH = os.getenv("PROD_KEYFILE")
sites = utils.all_sites()
mkts_dict = utils.get_markets_for_sites(sites)
all_markets = [m.name for market_list in mkts_dict.values() for m in market_list]
MARKET_TICKER = all_markets
WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"


def sign_pss_text(private_key, text: str) -> str:
    """Sign message using RSA-PSS"""
    message = text.encode("utf-8")
    signature = private_key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH
        ),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode("utf-8")


def create_headers(private_key, method: str, path: str) -> dict:
    """Create authentication headers"""
    timestamp = str(int(time.time() * 1000))
    msg_string = timestamp + method + path.split("?")[0]
    signature = sign_pss_text(private_key, msg_string)

    return {
        "Content-Type": "application/json",
        "KALSHI-ACCESS-KEY": KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": signature,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
    }


async def orderbook_websocket():
    """Connect to WebSocket and subscribe to orderbook"""
    # Load private key
    with open(PRIVATE_KEY_PATH, "rb") as f:
        private_key = serialization.load_pem_private_key(f.read(), password=None)

    # Create WebSocket headers
    ws_headers = create_headers(private_key, "GET", "/trade-api/ws/v2")

    async with websockets.connect(WS_URL, additional_headers=ws_headers) as websocket:
        print(f"Connected! Subscribing to orderbook for {MARKET_TICKER}")

        # Subscribe to orderbook
        ob_subscribe_msg = {
            "id": 1,
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta"],
                "market_tickers": MARKET_TICKER,
            },
        }
        await websocket.send(json.dumps(ob_subscribe_msg))

        lifecycle_subscribe_msg = {
            "id": 2,
            "cmd": "subscribe",
            "params": {
                "channels": ["market_lifecycle_v2"],
            },
        }
        await websocket.send(json.dumps(lifecycle_subscribe_msg))

        mkt_pos_subscribe_msg = {
            "id": 3,
            "cmd": "subscribe",
            "params": {
                "channels": ["market_positions"],
            },
        }
        await websocket.send(json.dumps(mkt_pos_subscribe_msg))

        # Process messages
        async for message in websocket:
            data = json.loads(message)
            msg_type = data.get("type")

            if msg_type == "subscribed":
                print(f"Subscribed: {data}")

            elif msg_type == "orderbook_snapshot":
                # print(f"Orderbook snapshot: {data}")
                a = None

            elif msg_type == "orderbook_delta":
                # print(f"Orderbook update: {data}")
                a = None

            elif msg_type == "fill":
                print(f"User Fills: {data}")

            elif msg_type == "market_position":
                print(f"Market position: {data}")

            elif msg_type == "market_lifecycle_v2":
                print(f"market_lifecycle: {data}")

            elif msg_type == "event_lifecycle":
                print(f"event_lifecycel: {data}")

            elif msg_type == "error":
                print(f"Error: {data}")

            else:
                print(f"{data}")


# Run the example
if __name__ == "__main__":
    asyncio.run(orderbook_websocket())
