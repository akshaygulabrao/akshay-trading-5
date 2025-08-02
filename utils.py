from dataclasses import dataclass
import os
from cryptography.hazmat.primitives import serialization
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
from enum import Enum
from collections import defaultdict

from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
import requests

from kalshi_ref import KalshiHttpClient, Environment

from weather_info import nws_site2tz, kalshi_sites


@dataclass(frozen=True)
class MarketTicker:
    name: str

    def __str__(self) -> str:
        return str(self.name)


@dataclass(frozen=True)
class EventTicker:
    name: str

    def __str__(self) -> str:
        return str(self.name)


class Site(Enum):
    NY = "NY"
    CHI = "CHI"
    MIA = "MIA"
    AUS = "AUS"
    DEN = "DEN"
    PHIL = "PHIL"
    LAX = "LAX"


def all_sites() -> list[Site]:
    return [Site.NY, Site.CHI, Site.MIA, Site.AUS, Site.DEN, Site.PHIL, Site.LAX]


def setup_prod() -> tuple[str, RSAPrivateKey, Environment]:
    """
    Returns (KEYID, private_key, env) for production setup.

    Raises:
        EnvironmentError: If required environment variables are not set
        FileNotFoundError: If private key file is not found
        ValueError: If private key is not an RSA key
        Exception: For other errors during key loading
    """

    KEYID = os.getenv("PROD_KEYID")
    if KEYID is None:
        raise EnvironmentError("PROD_KEYID environment variable not set")

    KEYFILE = os.getenv("PROD_KEYFILE")
    if KEYFILE is None:
        raise EnvironmentError("PROD_KEYFILE environment variable not set")

    try:
        with open(KEYFILE, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,
            )

            if not isinstance(private_key, RSAPrivateKey):
                raise ValueError("Loaded private key is not an RSA key")

    except FileNotFoundError:
        raise FileNotFoundError(f"Private key file not found at {KEYFILE}")
    except ValueError as ve:
        raise ValueError(f"Invalid private key format: {str(ve)}")
    except Exception as e:
        raise Exception(f"Error loading private key: {str(e)}")

    return KEYID, private_key, Environment.PROD


def setup_client() -> KalshiHttpClient:
    keyid, private_key, env = setup_prod()
    client = KalshiHttpClient(keyid, private_key, env)
    return client


def get_events_for_sites(sites: list[Site]) -> list[EventTicker]:
    """
    Used to verify correctness of hardcoded
    events
    """
    url = "https://api.elections.kalshi.com/trade-api/v2/events"
    evts = []
    for site in sites:
        params = {"series_ticker": f"KXHIGH{site.value}", "status": "open"}
        response = requests.get(url, params=params).json()
        evts.extend([EventTicker(evt["event_ticker"]) for evt in response["events"]])
    return evts


def get_markets_for_sites(sites: list[Site]) -> dict[EventTicker, list[MarketTicker]]:
    mkts_endpoint = "https://api.elections.kalshi.com/trade-api/v2/markets"
    event_markets = {}
    for site in sites:
        events_site = get_events_for_sites([site])
        for event_site in events_site:
            params = {"event_ticker": event_site.name, "status": "open"}
            response = requests.get(mkts_endpoint, params=params)
            assert response.status_code == 200
            response = response.json()
            tkrs = [i["ticker"] for i in response["markets"]]
            tkrs = sorted(tkrs, key=lambda x: extract_num_from_mkt(x, site.value))
            event_markets[event_site] = [MarketTicker(i) for i in tkrs]
    return event_markets


def extract_num_from_mkt(x, site) -> float:
    return float(x[5 + len(site) + 9 + 2 :])


def now(site="KLAX") -> datetime:
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
    return result
