import json
import time
from typing import Dict, List, Tuple
from sortedcontainers import SortedDict


class OrderBook:
    def __init__(self):
        self.markets: Dict[str, Dict] = {}

    def process_snapshot(self, snapshot: Dict):
        market_ticker = snapshot["market_ticker"]
        market_id = snapshot["market_id"]

        # Initialize market if not exists
        if market_ticker not in self.markets:
            self.markets[market_ticker] = {
                "market_id": market_id,
                "yes": SortedDict(lambda x: -x),
                "no": SortedDict(lambda x: -x),
            }

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

    def print_orderbook(self, market_ticker: str) -> None:
        if market_ticker not in self.markets:
            print(f"No orderbook found for {market_ticker}")
            return

        market = self.markets[market_ticker]
        print(f"Orderbook for {market_ticker}:")

        # Yes Side: Directly use the sorted keys of SortedDict
        print("Yes Side:")
        yes_prices = market["yes"].keys()
        for price in yes_prices[:3][::-1]:  # First 3 prices in reverse order
            print(f"  {price}: {market['yes'][price]}")

        print("==================================")

        # No Side: Directly use the sorted keys of SortedDict in reverse
        print("No Side:")
        no_prices = market["no"].keys()
        for price in no_prices[:3]:  # First 3 prices (lowest)
            print(f"  {100-price}: {market['no'][price]}")

        print("\n")
