import asyncio
import json
import zmq
import os
from zmq.asyncio import Context
from collections import defaultdict
from utils import now

from sport_pricing import contract_price_odds
from draftkings_kalshi_sports import filter_mkts,get_all_mkts

name2odds = defaultdict(lambda: 0)
name2mkt = defaultdict(lambda: '')
mtks = []

async def get_kalshi_mkts():
    while True:
        mkts = get_all_mkts()
        await asyncio.sleep(100)

async def match_mkts():
    while True:
        print("Getting markets...")
        # for n in name2odds.keys():
        #     print(n)
        await asyncio.sleep(5)  # Example: run every 5 seconds

async def odds():
    context = Context()
    socket = context.socket(zmq.SUB)
    
    socket.connect("ipc:///tmp/draftkings_golf.ipc")
    socket.setsockopt_string(zmq.SUBSCRIBE, "")
    
    print("ZMQ JSON listener started. Waiting for messages...")
    
    try:
        while True:
            # Receive as bytes and decode
            message = await socket.recv()
            try:
                # Clear the terminal
                os.system('clear')
                
                # Parse JSON
                data = json.loads(message.decode('utf-8'))
                i = 0
                for name,odds in data.items():
                    odds,_ = contract_price_odds(odds,0)
                    name2odds[name] = odds
                    if i < 10:
                        print(name,f'{odds:.02f}')
                        i+=1
                print(now())


            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                print(f"Error processing message: {e}")
                print(f"Raw message: {message}")
                
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        socket.close()
        context.term()

async def main():
    # Run both tasks concurrently
    await asyncio.gather(
        odds(),
        get_kalshi_mkts(),
        match_mkts(),
    )

if __name__ == "__main__":
    name2odds = defaultdict(lambda: 0)
    name2mkt = defaultdict(lambda: '')
    mtks = []

    try: 
        asyncio.run(main())
    except Exception as e:
        print(e)