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
import datetime
import time
import uuid

from utils import now, setup_prod,urls
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
name2last_update: Dict[Name, int] = {}
mkt2position: Dict[MarketId, Tuple[int, float]] = {}
mkt2bids: Dict[MarketId, str] = defaultdict(lambda: '')
mkt2asks: Dict[MarketId, str] = defaultdict(lambda: '')
mkt2prices: Dict[MarketId, Tuple[float, float]] = {}
mkt2profit: Dict[MarketId, float] = defaultdict(lambda: 0.0)

KEYID, private_key, env = setup_prod()
client = KalshiHttpClient(KEYID, private_key, env)


update_lock = asyncio.Lock()


def print_ui() -> None:
    """Clears the screen and prints the current odds interface with order data."""
    try:
        os.system('clear')
        print(now())
        for sport, names in sports2name.items():
            print(f"\n{sport.upper()}")
            for name in sorted(names):
                odds = name2odds.get(name, 0)
                if 0.05 < odds < 0.95:
                    line = f"  {name.ljust(30)} {round(odds * 100)}"
                    market_id = name2mkt.get(name)
                    if market_id:
                        pos_qty, pos_avg = mkt2position.get(market_id, (0, 0.0))
                        bid_id = mkt2bids[market_id]
                        ask_id = mkt2asks[market_id]
                        line += (
                            f" | Position: {pos_qty}@{pos_avg:.2f} "
                            f"Bids: {bid_id} Asks: {ask_id}"
                            f"Profit: {mkt2profit.get(market_id,0)}"
                        )
                    print(line)
    except Exception as e:
        logging.error(f"Error in print_ui: {str(e)}", exc_info=True)

async def update_positions_and_orders_once() -> None:
    """Updates positions and orders from Kalshi API once for all markets."""
    async with update_lock:
        try:
            # Update positions
            params = {'settlement_status': 'unsettled', 'count_filter': 'position','limit' : 1000}
            positions = client.get(urls['positions'],params=params)['market_positions']
            for pos in positions:
                if pos['ticker'] in mkt2name.keys():
                    mkt_id = pos['ticker']
                    quantity = pos.get('position', 0)
                    avg_price = pos.get('market_exposure', 0.0) / quantity
                    mkt2position[mkt_id] = (quantity, avg_price)
            
            # Update resting orders
            n = int(time.time())
            active_time = int(datetime.timedelta(hours=2).total_seconds())
            params = {'status': 'resting','min_ts': n - active_time}
            resting_orders = client.get(urls['orders'],params)['orders']
            for order in resting_orders:
                mkt_id = order['ticker']
                if order['action'] == 'buy':
                    mkt2bids[mkt_id] = order['client_order_id']
                elif order['action'] == 'sell':
                    mkt2asks[mkt_id] = order['client_order_id']
            
            logging.info("Updated positions, orders of all markets")
        except Exception as e:
            logging.error(f"Error updating positions/orders: {str(e)}", exc_info=True)

async def place_orders_based_on_position(updated_markets: Set[MarketId]) -> None:
    """Places orders based on current positions and updated odds."""
    async with update_lock:
        for market_id in updated_markets:
            try:
                name = mkt2name.get(market_id)
                if not name or name not in name2odds:
                    continue
                print(market_id,mkt2position[market_id])
                current_price = name2odds[name]
                price_cent = int(round(current_price * 100))
                position = mkt2position.get(market_id, (0, 0.0))[0]
                bid_price = max(2, price_cent - 15)
                ask_price = min(98, price_cent + 15)
                
                # Get existing order IDs
                current_bid_id = mkt2bids[market_id]
                current_ask_id = mkt2asks[market_id]
                # Position-based order logic
                if position == 1:  # Long position - sell
                    if current_bid_id != '':
                        params = {
                            'order_id': current_bid_id, 
                            'reduce_to': 0
                        }
                        response = client.post(f"{urls['orders']}/{current_bid_id}/decrease", params)
                        if response['order']['remaining_count'] == 0:
                            mkt2bids[market_id] = ''
                        else:
                            mkt2bids[market_id] = response['order']['order_id']
                    
                    # Create new ask order
                    response = client.post(
                        urls['create_order'],
                        data={
                            'market_id': market_id,
                            'side': 'sell',
                            'price': ask_price,
                            'quantity': 1,
                            'order_type': 'limit',
                            'client_order_id': str(uuid.uuid4()),
                        }
                    )
                    mkt2asks[market_id] = response['order']['order_id']
                
                elif position == -1:  # Short position - buy
                    if current_ask_id != '':
                        params = {
                            'order_id': current_ask_id,
                            'reduce_to': 0
                        }
                        response = client.post(f"{urls['orders']}/{current_ask_id}/decrease", params)
                        if response['order']['remaining_count'] == 0:
                            mkt2asks[market_id] = ''
                        else:
                            mkt2asks[market_id] = response['order']['order_id']
                    
                    # Create new bid order
                    response = client.post(
                        urls['create_order'],
                        data={
                            'market_id': market_id,
                            'side': 'buy',
                            'price': bid_price,
                            'quantity': 1,
                            'order_type': 'limit',
                            'client_order_id': str(uuid.uuid4()),
                        }
                    )
                    mkt2bids[market_id] = response['order']['order_id']
                
                elif position == 0:  # Flat - market make
                    # Cancel existing bids
                    if current_bid_id != '':
                        params = {'order_id': current_bid_id, 'reduce_to': 0}
                        response = client.post(f"{urls['orders']}/{current_bid_id}/decrease", params)
                        if response['order']['remaining_count'] == 0:
                            mkt2bids[market_id] = ''
                        else:
                            mkt2bids[market_id] = response['order']['order_id']
                    
                    # Cancel existing asks
                    if current_ask_id != '':
                        params = {'order_id': current_ask_id, 'reduce_to': 0}
                        response = client.post(f"{urls['orders']}/{current_ask_id}/decrease", params)
                        if response['order']['remaining_count'] == 0:
                            mkt2asks[market_id] = ''
                        else:
                            mkt2asks[market_id] = response['order']['order_id']
                    
                    # Create new bid and ask
                    bid_response = client.post(
                        urls['create_order'],
                        data={
                            'market_id': market_id,
                            'side': 'buy',
                            'price': bid_price,
                            'quantity': 1,
                            'order_type': 'limit',
                            'client_order_id': str(uuid.uuid4()),
                        }
                    )
                    mkt2bids[market_id] = bid_response['order']['order_id']
                    
                    ask_response = client.post(
                        urls['create_order'],
                        data={
                            'market_id': market_id,
                            'side': 'sell',
                            'price': ask_price,
                            'quantity': 1,
                            'order_type': 'limit',
                            'client_order_id': str(uuid.uuid4()),
                        }
                    )
                    mkt2asks[market_id] = ask_response['order']['order_id']
                
                logging.info(f"Updated orders for {name} ({market_id})")
            except Exception as e:
                logging.error(f"Error processing {market_id}: {str(e)}")


async def update_positions_periodically() -> None:
    """Periodically updates positions and orders from API."""
    while True:
        await update_positions_and_orders_once()
        await asyncio.sleep(1)

async def save_state_periodically() -> None:
    """Periodically saves all state variables to a JSON file with datetime handling."""
    while True:
        try:
            state = {
                'sports2name': {sport: list(names) for sport, names in sports2name.items()},
                'name2mkt': name2mkt,
                'name2odds': name2odds,
                'mkt2bidask_id': {mkt: list(bidask) for mkt, bidask in mkt2bidask_id.items()},
                'name2update': name2update,
                'name2last_update': name2last_update,
                'mkt2position': {mkt: list(pos) for mkt, pos in mkt2position.items()},
                'mkt2bids': {mkt: list(bid) for mkt, bid in mkt2bids.items()},
                'mkt2asks': {mkt: list(ask) for mkt, ask in mkt2asks.items()},
                'mkt2prices': mkt2prices,
            }
            def datetime_serializer(obj):
                """Converts datetime objects to ISO format strings."""
                if isinstance(obj, datetime.datetime):
                    return obj.isoformat()
                raise TypeError(f"Type {type(obj)} not serializable")
            
            with open('app_state.json', 'w') as f:
                json.dump(state, f, default=datetime_serializer)
            logging.info("Successfully saved application state to file")
        except (IOError, PermissionError) as e:
            logging.error(f"File error saving state: {str(e)}")
        except Exception as e:
            logging.error(f"Unexpected error saving state: {str(e)}", exc_info=True)
        
        await asyncio.sleep(10)

async def match_mkts() -> None:
    while True:
        try:
            with Path('name2mkt.json').open('r') as file:
                data = json.load(file)
            name2mkt.clear()
            name2mkt.update({str(k): str(v) for k, v in data.items()})
            global mkt2name
            mkt2name = {v: k for k, v in name2mkt.items()}  # Build reverse mapping
            logging.info("Updated market mappings")
            print_ui()
        except Exception as e:
            logging.error(f"Error loading markets: {str(e)}")
        await asyncio.sleep(10)

async def process_odds_data(data: Dict[str, Any]) -> None:
    """Process and validate incoming odds data, storing timestamps correctly."""
    try:
        players = data["players"]
        sport = str(data.get("sport", "unknown")).lower()
        odd_sum = 0.0
        updated_markets = set()

        for team in players:
            name = str(team[0])
            raw_odds = float(team[1])
            calculated_odds = odds2(raw_odds)
            
            sports2name[sport].add(name)
            name2odds[name] = calculated_odds
            current_time = now()
            if isinstance(current_time, datetime.datetime):
                name2last_update[name] = int(current_time.timestamp())
            else:
                name2last_update[name] = current_time
            odd_sum += calculated_odds

            if name in name2mkt and name2mkt[name]:
                updated_markets.add(name2mkt[name])

        if odd_sum <= 0:
            logging.error("Invalid odds sum, skipping normalization")
            return

        # Normalize odds
        for team in players:
            name = str(team[0])
            name2odds[name] /= odd_sum

        if updated_markets:
            await place_orders_based_on_position(updated_markets)
            # print_ui()
    except KeyError as e:
        logging.error(f"Missing key in data: {str(e)}")
    except (ValueError, TypeError) as e:
        logging.error(f"Data validation error: {str(e)}")
    except Exception as e:
        logging.error(f"Error processing odds data: {str(e)}", exc_info=True)

async def cleanup_inactive_names() -> None:
    """Periodically removes names not updated in the last 30 minutes."""
    while True:
        try:
            current_time = now()
            inactive_threshold = 1800  # 30 minutes in seconds
            to_remove = [name for name, last_update in name2last_update.items()
                        if (int(current_time.timestamp()) - last_update) >= inactive_threshold]

            for name in to_remove:
                # Remove from tracking dictionaries
                if name in name2last_update:
                    del name2last_update[name]
                if name in name2mkt:
                    market_id = name2mkt.pop(name)
                    if market_id in mkt2bidask_id:
                        del mkt2bidask_id[market_id]
                if name in name2odds:
                    del name2odds[name]
                if name in name2update:
                    del name2update[name]

                # Remove from sports categorization
                for sport in list(sports2name.keys()):
                    if name in sports2name[sport]:
                        sports2name[sport].remove(name)
                        # Cleanup empty sport categories
                        if not sports2name[sport]:
                            del sports2name[sport]

                logging.info(f"Removed inactive entry: {name}")

            logging.info(f"Cleanup completed. Removed {len(to_remove)} entries.")
            print_ui()

        except Exception as e:
            logging.error(f"Error during cleanup: {str(e)}", exc_info=True)
        
        await asyncio.sleep(600)  # Run every 10 minutes

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
    """Main async entry point with state loading adjusted for timestamps."""
    try:
        state_path = Path('app_state.json')
        if state_path.exists():
            with open(state_path, 'r') as f:
                state = json.load(f)
                
                # Restore sports2name
                sports2name.clear()
                for sport, names in state.get('sports2name', {}).items():
                    sports2name[sport].update(names)
                
                # Restore other dictionaries
                name2mkt.update(state.get('name2mkt', {}))
                name2odds.update(state.get('name2odds', {}))
                mkt2bidask_id.update({mkt: tuple(bidask) for mkt, bidask in state.get('mkt2bidask_id', {}).items()})
                name2update.update(state.get('name2update', {}))
                
                # Convert ISO strings back to timestamps if necessary
                name2last_update.clear()
                for name, timestamp in state.get('name2last_update', {}).items():
                    if isinstance(timestamp, str):  # ISO format
                        dt = datetime.datetime.fromisoformat(timestamp)
                        name2last_update[name] = int(dt.timestamp())
                    else:
                        name2last_update[name] = timestamp
                
            logging.info("Loaded initial application state")
    except (JSONDecodeError, ValueError, KeyError) as e:
        logging.error(f"Invalid data in app_state.json: {str(e)}")
    except Exception as e:
        logging.error(f"Error loading initial state: {str(e)}", exc_info=True)
    print_ui()
    tasks = [
        asyncio.create_task(odds()),
        asyncio.create_task(save_state_periodically()),
        asyncio.create_task(match_mkts()),
        asyncio.create_task(cleanup_inactive_names()),
        asyncio.create_task(update_positions_periodically()),  # New task
    ]

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logging.info("Shutting down...")
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
    