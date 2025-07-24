from configparser import ParsingError
import json
from typing import LiteralString

import pytest

from trading.orderbook_update import OrderBook


@pytest.fixture
def filename() -> LiteralString:
    return "/Users/ox/workspace/akshay-trading-5/orderbook_small.json"


def test_process_messages(filename: LiteralString) -> None:
    orderbook = OrderBook()

    with open(filename, "r") as f:
        for line in f:
            message = json.loads(line)
            msg_type = message["message"]["type"]

            try:
                if msg_type == "orderbook_snapshot":
                    orderbook.process_snapshot(message["message"]["msg"])
                elif msg_type == "orderbook_delta":
                    orderbook.process_delta(message["message"]["msg"])

                # Select the market ticker to print (you can modify this)
                # if "market_ticker" in message["message"]["msg"]:
                #     orderbook.print_orderbook(
                #         message["message"]["msg"]["market_ticker"]
                #     )

            except Exception as e:
                raise ParsingError(f"Error processing message: {e}")
