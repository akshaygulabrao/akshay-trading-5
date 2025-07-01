import json
import time
from typing import Dict, List, Tuple


class OrderBook:
    def __init__(self):
        self.markets: Dict[str, Dict] = {}

    def process_snapshot(self, snapshot: Dict):
        market_ticker = snapshot["market_ticker"]
        market_id = snapshot["market_id"]

        # Initialize market if not exists
        if market_ticker not in self.markets:
            self.markets[market_ticker] = {"market_id": market_id, "yes": {}, "no": {}}

        # Process yes side if exists
        if "yes" in snapshot:
            for price, volume in snapshot["yes"]:
                self.markets[market_ticker]["yes"][price] = volume

        # Process no side if exists
        if "no" in snapshot:
            for price, volume in snapshot["no"]:
                self.markets[market_ticker]["no"][price] = volume

    def process_delta(self, delta: Dict):
        market_ticker = delta["market_ticker"]
        side = delta["side"]
        price = delta["price"]
        volume_delta = delta["delta"]

        # Update volume for the specific price and side
        if market_ticker in self.markets:
            current_volume = self.markets[market_ticker][side].get(price, 0)
            new_volume = current_volume + volume_delta

            if new_volume > 0:
                self.markets[market_ticker][side][price] = new_volume
            else:
                self.markets[market_ticker][side].pop(price, None)

    def print_orderbook(self, market_ticker: str):
        if market_ticker not in self.markets:
            print(f"No orderbook found for {market_ticker}")
            return

        market = self.markets[market_ticker]
        print(f"Orderbook for {market_ticker}:")
        print("Yes Side:")
        for price, volume in sorted(market["yes"].items())[-3:]:
            print(f"  {price}: {volume}")
        print("==================================")
        print("No Side:")
        for price, volume in sorted(market["no"].items(), reverse=True)[:3]:
            print(f"  {100-price}: {volume}")
        print("\n")


def process_messages(filename):
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
                if "market_ticker" in message["message"]["msg"]:
                    orderbook.print_orderbook(
                        message["message"]["msg"]["market_ticker"]
                    )

                # Sleep after processing each message

            except Exception as e:
                print(f"Error processing message: {e}")
                print(message)


def main():
    process_messages("orderbook_small.json")


if __name__ == "__main__":
    main()
