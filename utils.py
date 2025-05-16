import os
from cryptography.hazmat.primitives import serialization
from datetime import datetime,timedelta
from zoneinfo import ZoneInfo

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

urls = {'status': 'https://api.elections.kalshi.com/trade-api/v2/exchange/status',
        'mkts': 'https://api.elections.kalshi.com/trade-api/v2/markets'}

def get_events_hardcoded():
    today = datetime.now() + timedelta(hours=3)
    days = [today.strftime("%y%^b%d")]
    if today.hour > 10:
        days.append((today + timedelta(days=1)).strftime("%y%^b%d"))
    sites = ["NY", "CHI","MIA","AUS","DEN","LAX","PHIL"]
    evts = []
    for site in sites:
        for day in days:
            evts.append(f"KXHIGH{site}-{day}")
    return evts

def get_events_kalshi():
    """
    Used to verify correctness of hardcoded
    events
    """
    url = "https://api.elections.kalshi.com/trade-api/v2/events"
    sites = ["NY", "CHI","MIA","AUS","DEN","LAX","PHIL"]
    evts = []
    for site in sites:
        params = {"series_ticker": f"KXHIGH{site}","status": "open"}
        response = requests.get(url, params=params).json()
        evts.extend([evt["event_ticker"] for evt in response["events"]])
    return evts

def get_markets():
    evts = get_events_hardcoded()
    url = "https://api.elections.kalshi.com/trade-api/v2/markets"
    markets = []
    for e in evts:
        params = {"event_ticker": e,"status": "open"}
        response = requests.get(url, params=params).json()
        markets.extend([m["ticker"] for m in response["markets"]])
    return markets

def now(site="KLAX"):
    time = datetime.now(tz=ZoneInfo(sites2tz[site]))
    return time

if __name__ == "__main__":
    print(get_events_hardcoded())
    print(get_events_kalshi())
    print(get_markets())