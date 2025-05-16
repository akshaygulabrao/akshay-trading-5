import asyncio
import json
import zmq
import os
import logging
from typing import Dict, Set, Tuple, DefaultDict, Any
from zmq.asyncio import Context
from collections import defaultdict
from json import JSONDecodeError
from pathlib import Path

from utils import now, setup_prod
from original.clients import KalshiHttpClient
from sport_pricing import odds2

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('sports_odds.log'), logging.StreamHandler()]
)

# Type aliases
Name = str
Sport = str
MarketId = str
Odds = float

# Global variables with type hints
sports2name: DefaultDict[Sport, Set[Name]] = defaultdict(set)
name2mkt: Dict[Name, MarketId] = {}
name2odds: Dict[Name, Odds] = {}
mkt2bidask_id: Dict[MarketId, Tuple[str, str]] = {}
name2update: Dict[Name, int] = {}

KEYID, private_key, env = setup_prod()
client = KalshiHttpClient(KEYID, private_key, env)

def print_ui() -> None:
    """Clears the screen and prints the current odds interface."""
    try:
        os.system('clear')
        print(now())
        for sport, names in sports2name.items():
            print(f"\n{sport.upper()}")
            for name in sorted(names, key=lambda x: name2odds.get(x, 0), reverse=True)[:20]:
                odds = name2odds.get(name, 0)
                print(f"  {name.ljust(30)} {round(odds * 100, 1)}%")
    except Exception as e:
        logging.error(f"Error in print_ui: {str(e)}", exc_info=True)

async def save_name2odds_periodically() -> None:
    """Periodically saves name2odds to JSON file."""
    while True:
        try:
            with open('name2odds.json', 'w') as f:
                json.dump(name2odds, f)
            logging.info("Successfully saved name2odds to file")
        except (IOError, PermissionError) as e:
            logging.error(f"File error saving name2odds: {str(e)}")
        except Exception as e:
            logging.error(f"Unexpected error saving name2odds: {str(e)}", exc_info=True)
        
        await asyncio.sleep(10)

async def match_mkts() -> None:
    """Periodically updates market mappings from JSON file."""
    while True:
        try:
            path = Path('name2mkt.json')
            if not path.exists():
                logging.warning("name2mkt.json not found")
                await asyncio.sleep(10)
                continue

            with path.open('r') as file:
                data = json.load(file)
            
            if not isinstance(data, dict):
                raise TypeError("Loaded data is not a dictionary")
                
            name2mkt.clear()
            name2mkt.update({str(k): str(v) for k, v in data.items()})
            logging.info("Successfully updated market mappings")
            print_ui()
            
        except (FileNotFoundError, JSONDecodeError, TypeError) as e:
            logging.error(f"Error loading market mappings: {str(e)}")
        except Exception as e:
            logging.error(f"Unexpected error in match_mkts: {str(e)}", exc_info=True)
        
        await asyncio.sleep(10)

async def process_odds_data(data: Dict[str, Any]) -> None:
    """Process and validate incoming odds data."""
    try:
        if not isinstance(data.get("players"), list):
            raise ValueError("Invalid players format in data")
            
        players = data["players"]
        sport = str(data.get("sport", "unknown")).lower()
        odd_sum = 0.0

        for team in players:
            if len(team) < 2:
                logging.warning(f"Invalid team data: {team}")
                continue
                
            name = str(team[0])
            raw_odds = float(team[1])
            calculated_odds = odds2(raw_odds)
            
            sports2name[sport].add(name)
            name2odds[name] = calculated_odds
            odd_sum += calculated_odds

        if odd_sum <= 0:
            logging.error("Invalid odds sum (<= 0), skipping normalization")
            return

        for team in players:
            if len(team) < 2:
                continue
            name = str(team[0])
            name2odds[name] /= odd_sum

    except KeyError as e:
        logging.error(f"Missing key in data: {str(e)}")
    except (ValueError, TypeError) as e:
        logging.error(f"Data validation error: {str(e)}")
    except Exception as e:
        logging.error(f"Error processing odds data: {str(e)}", exc_info=True)

async def odds() -> None:
    """Main ZMQ listener for odds data."""
    context = Context.instance()
    socket = context.socket(zmq.SUB)
    
    try:
        socket.connect("ipc:///tmp/draftkings.ipc")
        socket.setsockopt_string(zmq.SUBSCRIBE, "")
        logging.info("ZMQ listener started")

        while True:
            try:
                message = await socket.recv()
                data = json.loads(message.decode('utf-8'))
                await process_odds_data(data)
                print_ui()
                
            except (UnicodeDecodeError, JSONDecodeError) as e:
                logging.error(f"Message decoding error: {str(e)}")
            except Exception as e:
                logging.error(f"Error processing message: {str(e)}", exc_info=True)

    except zmq.ZMQError as e:
        logging.error(f"ZMQ error: {str(e)}")
    except KeyboardInterrupt:
        logging.info("Shutting down ZMQ listener")
    finally:
        socket.close()
        context.term()

async def main() -> None:
    """Main async entry point."""
    # Load initial data
    try:
        if Path('name2odds.json').exists():
            with open('name2odds.json', 'r') as f:
                loaded = json.load(f)
                name2odds.update({str(k): float(v) for k, v in loaded.items()})
            logging.info("Loaded initial odds data")
    except (JSONDecodeError, ValueError) as e:
        logging.error(f"Invalid data in name2odds.json: {str(e)}")
    except Exception as e:
        logging.error(f"Error loading initial data: {str(e)}")

    # Start tasks
    tasks = [
        asyncio.create_task(odds()),
        asyncio.create_task(save_name2odds_periodically()),
        asyncio.create_task(match_mkts()),
    ]

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logging.info("Shutting down...")
    except Exception as e:
        logging.error(f"Unexpected error in main: {str(e)}", exc_info=True)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logging.critical(f"Critical error: {str(e)}", exc_info=True)
        raise