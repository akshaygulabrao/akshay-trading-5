import os
from cryptography.hazmat.primitives import serialization
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
import requests

from kalshi_ref import KalshiHttpClient, KalshiWebSocketClient, Environment

from weather_info import nws_site2tz
from weather_info import kalshi_sites


def setup_prod():
    """
    Returns (KEYID, private_key, env) for production setup.
    """
    # Add your production setup code here
    load_dotenv()
    env = Environment.PROD
    KEYID = os.getenv("PROD_KEYID")
    KEYFILE = os.getenv("PROD_KEYFILE")
    assert KEYFILE is not None
    assert KEYID is not None

    try:
        with open(KEYFILE, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,  # Provide the password if your key is encrypted
            )
    except FileNotFoundError:
        raise FileNotFoundError(f"Private key file not found at {KEYFILE}")
    except Exception as e:
        raise Exception(f"Error loading private key: {str(e)}")

    return KEYID, private_key, env


def setup_client():
    keyid, private_key, env = setup_prod()
    client = KalshiHttpClient(keyid, private_key, env)
    return client


kalshi_url = "https://api.elections.kalshi.com"
urls = {
    "status": "/trade-api/v2/exchange/status",
    "markets": "/trade-api/v2/markets",
    "positions": "/trade-api/v2/portfolio/positions",
    "orders": "/trade-api/v2/portfolio/orders",
    "fills": "/trade-api/v2/portfolio/fills",
}


def get_events_hardcoded():
    """
    returns mapping of kalshi_sites to events
    """
    today = now("KNYC")
    days = [today.strftime("%y%b%d").upper()]
    if today.hour > 10:
        days.append((today + timedelta(days=1)).strftime("%y%b%d").upper())
    sites = ["NY", "CHI", "MIA", "AUS", "DEN", "LAX", "PHIL"]
    site2days = {}
    for site in sites:
        site2days[site] = []
        for day in days:
            site2days[site].append(f"KXHIGH{site}-{day}")
    return site2days


def test_get_events_hardcoded():
    s2d = get_events_hardcoded()
    for site in s2d.keys():
        print(site)
        for day in s2d[site]:
            print(" " * 2, day)


def get_events_kalshi():
    """
    Used to verify correctness of hardcoded
    events
    """
    url = "https://api.elections.kalshi.com/trade-api/v2/events"
    sites = ["NY", "CHI", "MIA", "AUS", "DEN", "PHIL", "LAX"]
    evts = []
    for site in sites:
        params = {"series_ticker": f"KXHIGH{site}", "status": "open"}
        response = requests.get(url, params=params).json()
        evts.extend([evt["event_ticker"] for evt in response["events"]])
    return evts


def get_markets():
    url = "https://api.elections.kalshi.com/trade-api/v2/markets"
    markets = []
    for site in kalshi_sites:
        seriesTkr = f"KXHIGH{site}"
        params = {"series_ticker": seriesTkr, "status": "open"}
        response = requests.get(url, params=params)
        assert response.status_code == 200
        response = response.json()
        tkrs = [i["ticker"] for i in response["markets"]]
        tkrs = sorted(tkrs, key=lambda x: extract_num_from_mkt(x, site))
        markets.extend(tkrs)
    return markets


def extract_num_from_mkt(x, site):
    return float(x[5 + len(site) + 9 + 2 :])


def now(site="KLAX"):
    time = datetime.now(tz=ZoneInfo(nws_site2tz[site]))
    return time


if __name__ == "__main__":
    test_get_events_hardcoded()
