import asyncio
import json
import zmq
import os
from zmq.asyncio import Context
from collections import defaultdict
from utils import now,setup_prod
from original.clients import KalshiHttpClient

from sport_pricing import odds2

sports2name = defaultdict(lambda : '')
name2mkt = defaultdict(lambda: '')
name2odds = defaultdict(lambda: 0)
mkt2bidask_id = defaultdict(lambda: ('', ''))
name2udpate = defaultdict(lambda: 0)
KEYID, private_key, env = setup_prod()

client = KalshiHttpClient(KEYID,private_key,env)

def print_ui():
    global name2odds
    os.system('clear')
    print(now())
    for name, odds_val in name2odds.items():
        print(f'{name},{round(odds_val*100)}')



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
                print_ui()
        except Exception as e:
            print(e)
            pass
        await asyncio.sleep(10)

async def odds():
    context = Context()
    socket = context.socket(zmq.SUB)
    socket.connect("ipc:///tmp/draftkings.ipc")
    socket.setsockopt_string(zmq.SUBSCRIBE, "")
    
    print("ZMQ JSON listener started. Waiting for messages...")
    
    try:
        while True:
            message = await socket.recv()
            try:
                data = json.loads(message.decode('utf-8'))
                odd_sum = 0 #normalize after odds computation
                for team in data["players"]:
                    assert isinstance(team[0],str)
                    assert isinstance(team[1],int)
                    sports2name[data["sport"]] = team[team[0]]
                    name2odds[team[0]] = odds2(team[1])
                    odd_sum += team[1]
                for team in data["players"]:
                    name2odds[team[0]] /= odd_sum
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
    print_ui()
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