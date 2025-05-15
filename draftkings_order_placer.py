import asyncio
import json
import zmq
import os
from zmq.asyncio import Context
from collections import defaultdict

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
                markets[data["market_id"]][0] = data["teams"][0]
                markets[data["market_id"]][1] = data["teams"][1]
                
                # Print all markets
                print("Current Market Data:")
                print("====================")
                for market_id, teams in markets.items():
                    print(f"Market ID: {market_id}")
                    print(f"  Team 1: {teams[0]["name"]}:{teams[0]["odds"]}")
                    print(f"  Team 2: {teams[1]["name"]}:{teams[1]["odds"]}")
                    print("-" * 40)

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
    try: 
        asyncio.run(zmq_json_listener())
    except Exception as e:
        print(e)