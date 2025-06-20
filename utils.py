import os
from cryptography.hazmat.primitives import serialization
from datetime import datetime, timedelta, time
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


def get_events_kalshi_sites(sites):
    """
    Used to verify correctness of hardcoded
    events
    """
    url = "https://api.elections.kalshi.com/trade-api/v2/events"
    evts = []
    for site in sites:
        params = {"series_ticker": f"KXHIGH{site}", "status": "open"}
        response = requests.get(url, params=params).json()
        evts.extend([evt["event_ticker"] for evt in response["events"]])
    return evts


def get_markets():
    url = "https://api.elections.kalshi.com/trade-api/v2/markets"
    markets = {}
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


def get_markets_for_sites(sites):
    mkts_endpoint = "https://api.elections.kalshi.com/trade-api/v2/markets"
    event_markets = {}
    for site in sites:
        events_site = get_events_kalshi_sites([site])
        for event_site in events_site:
            params = {"event_ticker": event_site, "status": "open"}
            response = requests.get(mkts_endpoint, params=params)
            assert response.status_code == 200
            response = response.json()
            tkrs = [i["ticker"] for i in response["markets"]]
            tkrs = sorted(tkrs, key=lambda x: extract_num_from_mkt(x, site))
            event_markets[event_site] = tkrs
    return event_markets


def extract_num_from_mkt(x, site):
    return float(x[5 + len(site) + 9 + 2 :])


def now(site="KLAX"):
    time = datetime.now(tz=ZoneInfo(nws_site2tz[site]))
    return time


def exchange_status() -> tuple[timedelta, bool]:
    """
    Returns tuple (timedelta time_left, bool is_open)
    """
    # Get current time in New York timezone
    current_datetime = datetime.now(tz=ZoneInfo("America/New_York"))
    today = current_datetime.date()

    # Define opening and closing times
    opening_time = time(8, 0)  # 8:00 AM
    closing_time = time(3, 0)  # 3:00 AM

    # Construct datetime objects for today's opening and tomorrow's closing with NY timezone
    today_opening = datetime.combine(today, opening_time).replace(
        tzinfo=ZoneInfo("America/New_York")
    )
    tomorrow_closing = datetime.combine(
        today + timedelta(days=1), closing_time
    ).replace(tzinfo=ZoneInfo("America/New_York"))

    # Determine if the store is open
    is_open = today_opening <= current_datetime < tomorrow_closing

    # Calculate time until next state change
    if is_open:
        next_change = tomorrow_closing
    else:
        if current_datetime < today_opening:
            next_change = today_opening
        else:
            next_change = datetime.combine(
                today + timedelta(days=1), opening_time
            ).replace(tzinfo=ZoneInfo("America/New_York"))

    time_until = next_change - current_datetime

    return (time_until, is_open)


def format_timedelta(td: timedelta) -> str:
    """Convert a timedelta object to 'hr:min:s' format string."""
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    result = f"{hours}:{minutes:02d}:{seconds:02d}"
    assert isinstance(result, str)
    return result


if __name__ == "__main__":
    test_get_events_hardcoded()
