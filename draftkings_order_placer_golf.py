import asyncio
import json
import zmq
import os
from zmq.asyncio import Context
from collections import defaultdict
from utils import now

from sport_pricing import contract_price_odds
from draftkings_kalshi_sports import filter_mkts, get_all_mkts

# Global variables shared across all coroutines
name2odds = defaultdict(lambda: 0)
name2mkt = defaultdict(lambda: '')
mkts = []

async def get_kalshi_mkts():
    global mkts  # Declare mkts as global to modify the module-level variable
    while True:
        mkts = get_all_mkts()  # This now updates the global mkts list
        await asyncio.sleep(100)

async def match_mkts():
    while True:
        for name in name2odds:
            mkt_raw = filter_mkts(mkts,name)
            for mkt in mkt_raw:
                if 'PGA Championship' in mkt['rules_primary']:
                    name2mkt[name] = mkt['ticker']
        await asyncio.sleep(1)

async def odds():
    context = Context()
    socket = context.socket(zmq.SUB)
    socket.connect("ipc:///tmp/draftkings_golf.ipc")
    socket.setsockopt_string(zmq.SUBSCRIBE, "")
    
    print("ZMQ JSON listener started. Waiting for messages...")
    
    try:
        while True:
            message = await socket.recv()
            os.system('clear')
            try:
                data = json.loads(message.decode('utf-8'))
                i = 0
                for name, odds_val in data.items():
                    # Update the global name2odds
                    calculated_odds, _ = contract_price_odds(odds_val, 0)
                    name2odds[name] = calculated_odds  # Mutates the global defaultdict
                    if i < 10:
                        print(f"{name}: {calculated_odds:.02f}, {name2mkt[name]}")
                        i += 1
                print(now())
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                print(f"Error processing message: {e}")
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        socket.close()
        context.term()

async def main():
    await asyncio.gather(
        odds(),
        get_kalshi_mkts(),
        match_mkts(),
    )

if __name__ == "__main__":
    try: 
        asyncio.run(main())
    except Exception as e:
        print(e)