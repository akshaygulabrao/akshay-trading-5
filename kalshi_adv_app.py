import os
from cryptography.hazmat.primitives import serialization
from datetime import datetime,timedelta
from zoneinfo import ZoneInfo
import pandas as pd
import uuid
import time 
from dotenv import load_dotenv
import requests

from original.clients import KalshiHttpClient, KalshiWebSocketClient, Environment

from weather_info import sites2tz

def setup_prod():
    """
    Returns (KEYID, private_key, env) for production setup.
    """
    # Add your production setup code here
    load_dotenv()
    env = Environment.PROD# toggle environment here
    KEYID = os.getenv('DEMO_KEYID') if env == Environment.DEMO else os.getenv('PROD_KEYID')
    KEYFILE = os.getenv('DEMO_KEYFILE') if env == Environment.DEMO else os.getenv('PROD_KEYFILE')

    try:
        with open(KEYFILE, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None  # Provide the password if your key is encrypted
            )
    except FileNotFoundError:
        raise FileNotFoundError(f"Private key file not found at {KEYFILE}")
    except Exception as e:
        raise Exception(f"Error loading private key: {str(e)}")
    
    return KEYID, private_key, env


KEYID, private_key, env = setup_prod()

client = KalshiHttpClient(KEYID,private_key,env)

ct = datetime.now(tz=ZoneInfo("America/New_York"))

today_str = ct.strftime("%y%b%d").upper()
print(today_str)
params = {
    "event_ticker": f"KXHIGHNY-{today_str}",
}


response = requests.get('https://api.elections.kalshi.com/trade-api/v2/markets',params)
df = pd.DataFrame.from_dict(response.json()['markets'])

# Make sure we use a dead market so its not a real trade
df_dead = df[df['yes_bid'] == df['yes_bid'].max()][['ticker','yes_bid']]

buy_ticker = df_dead.iloc[0]['ticker']

params = {
    'depth':1
}
response = requests.get(f'https://api.elections.kalshi.com/trade-api/v2/markets/{buy_ticker}/orderbook',params)
print(response.json())


private_order_id = str(uuid.uuid4())

params = {
    'ticker': buy_ticker,
    'action': 'buy',
    'side': 'yes',
    'type': 'limit',
    'count' : 1, 
    'yes_price' : 10,
    'buy_max_cost': 12,
    'client_order_id' : private_order_id
}
response = client.post('/trade-api/v2/portfolio/orders',params)

public_order_id = response['order']['order_id']
time.sleep(1)
response = client.delete(f'/trade-api/v2/portfolio/orders/{public_order_id}')