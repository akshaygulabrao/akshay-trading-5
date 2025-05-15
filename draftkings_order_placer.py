import asyncio
import json
import zmq
import os
from zmq.asyncio import Context
from collections import defaultdict

from sport_pricing import contract_price_odds

team2kalshi = {"LA Dodgers" : "LAD"}

async def zmq_json_listener():
    context = Context()
    socket = context.socket(zmq.SUB)
    
    socket.connect("ipc:///tmp/draftkings_baseball.ipc")
    socket.setsockopt_string(zmq.SUBSCRIBE, "")
    
    print("ZMQ JSON listener started. Waiting for messages...")
    
    try:
        while True:
            # Receive as bytes and decode
            message = await socket.recv()
            try:
                # Clear the terminal
                os.system('cls' if os.name == 'nt' else 'clear')
                
                # Parse JSON
                data = json.loads(message.decode('utf-8'))
                away = int(data["teams"][0]["odds"])
                home = int(data["teams"][1]["odds"])
                away,home = contract_price_odds(away,home)
                teams2odds[data["teams"][0]["name"]] = away
                teams2odds[data["teams"][1]["name"]] = home
                # Print all markets
                print("Current Market Data:")
                print("====================")
                for name, odds in teams2odds.items():
                    print(f"{name},{odds:.02f}")

            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                print(f"Error processing message: {e}")
                print(f"Raw message: {message}")
                
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        socket.close()
        context.term()

if __name__ == "__main__":
    markets = defaultdict(lambda: [None, None])
    teams2odds = defaultdict(lambda : 0)
    try: 
        asyncio.run(zmq_json_listener())
    except Exception as e:
        print(e)