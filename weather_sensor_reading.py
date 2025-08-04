import os
import time
import requests
from typing import Dict, Any

import asyncio
import aiohttp

API_URL = "https://api.synopticdata.com/v2/stations/timeseries"
TOKEN = os.getenv("SYNOPTIC_TOKEN", "7c76618b66c74aee913bdbae4b448bdd")

DEFAULT_PARAMS = {
    "showemptystations": 1,
    "units": "temp|F,speed|mph,english",
    "recent": 100,
    "complete": 1,
    "obtimezone": "local",
    "token": TOKEN,
}

HEADERS = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "accept-language": "en-US,en;q=0.9",
    "dnt": "1",
    "origin": "https://www.weather.gov",
    "priority": "u=1, i",
    "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "macOS",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/138.0.0.0 Safari/537.36"
    ),
}


def get_timeseries(stid: str, **extra) -> Dict[str, Any]:
    """
    Return the decoded JSON response for a given station ID.
    Any extra keyword arguments override the default query parameters.
    """
    params = {**DEFAULT_PARAMS, "STID": stid, **extra}
    resp = requests.get(API_URL, params=params, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    return parse_station_rows(data)


async def get_timeseries_async(stid: str, **extra) -> Dict[str, Any]:
    """
    Async version of get_timeseries().
    Any extra keyword arguments override the default query parameters.
    """
    params = {**DEFAULT_PARAMS, "STID": stid, **extra}
    async with aiohttp.ClientSession() as session:
        async with session.get(
            API_URL, params=params, headers=HEADERS, timeout=15
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return parse_station_rows(data)


def _timing_demo(stid: str = "KNYC", runs: int = 10) -> None:
    """Run a small timing benchmark."""
    for i in range(1, runs + 1):
        t0 = time.perf_counter()
        _ = get_timeseries(stid)
        t1 = time.perf_counter()
        print(f"Request #{i} took {t1 - t0:.2f} s")


def parse_station_rows(payload: dict) -> list[dict]:
    station = payload["STATION"][0]  # there’s only one station
    obs = station["OBSERVATIONS"]

    rows = []
    for idx, dt in enumerate(obs["date_time"]):
        rows.append(
            {
                "date_time": dt,
                "air_temp": obs["air_temp_set_1"][idx],
                "relative_humidity": obs["relative_humidity_set_1"][idx],
                "dew_point": obs["dew_point_temperature_set_1d"][idx],
                "wind_speed": obs["wind_speed_set_1"][idx],
            }
        )
    return rows


if __name__ == "__main__":
    import sys
    import asyncio

    # Allow:  python synoptic.py KSLC 5
    stid = sys.argv[1] if len(sys.argv) > 1 else "KNYC"
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    _timing_demo(stid, runs)

    # --- one-run async demo ---
    async def _async_demo(stid: str) -> None:
        t0 = time.perf_counter()
        payload = await get_timeseries_async(stid)
        t1 = time.perf_counter()
        print(f"Async request took {t1 - t0:.2f} s")

    print("\n--- Async demo ---")
    asyncio.run(_async_demo(stid))
