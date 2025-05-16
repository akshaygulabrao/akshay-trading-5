import asyncio
import json
import zmq
import os
from zmq.asyncio import Context
from collections import defaultdict
from utils import now

from sport_pricing import contract_price_odds
from draftkings_kalshi_sports import filter_mkts, get_all_mkts

name2mkt = defaultdict(lambda: '')
name2odds = defaultdict(lambda: 0)
mkt2bidask_id = defaultdict(lambda : ('',''))

async def save_name2odds_periodically():
    while True:
        data = dict(name2odds)
        try:
            with open('name2odds.json', 'w') as f:
                json.dump(data, f)
        except Exception as e:
            print(f"Error saving name2odds: {e}")
        await asyncio.sleep(10)

async def match_mkts():
    global name2mkt
    while True:
        try:
            with open('name2mkt.json', 'r') as file:
                data = json.load(file)
            if not isinstance(data, dict):
                print("Invalid JSON file")
            else:
                for i, j in data.items():
                    name2mkt[i] = j
        except Exception as e:
            pass
        await asyncio.sleep(10)

async def odds():
    context = Context()
    socket = context.socket(zmq.SUB)
    socket.connect("ipc:///tmp/draftkings_2team.ipc")
    socket.setsockopt_string(zmq.SUBSCRIBE, "")
    
    print("ZMQ JSON listener started. Waiting for messages...")
    
    try:
        while True:
            message = await socket.recv()
            os.system('clear')
            print(now())
            try:
                data = json.loads(message.decode('utf-8'))
                home, away = data
                home_name = home[0]
                home_odds = home[1]
                away_name = away[0]
                away_odds = away[1]
                away_odds_adj, home_odds_adj = contract_price_odds(away_odds, home_odds)
                name2odds[home_name] = home_odds_adj
                name2odds[away_name] = away_odds_adj
                for name, odds_val in name2odds.items():
                    print(f'{name},{round(odds_val*100)},{name2mkt[name]}')
            except (json.JSONDecodeError, UnicodeDecodeError, KeyError) as e:
                print(f"Error processing message: {e}")
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        socket.close()
        context.term()

async def main():
    global name2odds
    try:
        with open('name2odds.json', 'r') as f:
            data = json.load(f)
            name2odds.update(data)
    except FileNotFoundError:
        print("name2odds.json not found, starting fresh")
    except Exception as e:
        print(f"Error loading name2odds.json: {e}")
    for name, odds_val in name2odds.items():
        print(f'{name},{round(odds_val*100)},{name2mkt[name]}')
    await asyncio.gather(
        odds(),
        match_mkts(),
        save_name2odds_periodically(),
    )

if __name__ == "__main__":
    try: 
        asyncio.run(main())
    except Exception as e:
        print(e)