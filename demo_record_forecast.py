#!/usr/bin/env python3
"""
Log 48-hour NWS digital forecasts into SQLite once per hour.
"""

import argparse
import sqlite3
import time
from datetime import datetime, timezone, timedelta

import pandas as pd
import pytz
import requests
from bs4 import BeautifulSoup

# ------------------------------------------------------------------
# 0.  Station → NWS URL map
# ------------------------------------------------------------------
NWS_SITE2FORECAST = {
    "KNYC": "https://forecast.weather.gov/MapClick.php?lat=40.78&lon=-73.97&lg=english&&FcstType=digital",
    "KMDW": "https://forecast.weather.gov/MapClick.php?lat=41.78&lon=-87.76&lg=english&&FcstType=digital",
    "KAUS": "https://forecast.weather.gov/MapClick.php?lat=30.18&lon=-97.68&lg=english&&FcstType=digital",
    "KMIA": "https://forecast.weather.gov/MapClick.php?lat=25.7554&lon=-80.2262&lg=english&&FcstType=digital",
    "KDEN": "https://forecast.weather.gov/MapClick.php?lat=39.85&lon=-104.66&lg=english&&FcstType=digital",
    "KPHL": "https://forecast.weather.gov/MapClick.php?lat=40.08&lon=-75.01&lg=english&&FcstType=digital",
    "KLAX": "https://forecast.weather.gov/MapClick.php?lat=33.96&lon=-118.42&lg=english&&FcstType=digital",
}


# ------------------------------------------------------------------
# 1.  Forecast extractor (unchanged logic)
# ------------------------------------------------------------------
def extract_forecast(nws_site: str) -> pd.DataFrame:
    url = NWS_SITE2FORECAST[nws_site]
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    tables = soup.find_all("table")
    if len(tables) < 5:
        raise ValueError("Forecast table not found")

    # build raw dict
    raw = {}
    for row in tables[4].find_all("tr"):
        cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
        if cells and cells[0]:
            raw.setdefault(cells[0], []).extend(cells[1:])

    df = pd.DataFrame(raw)

    # parse date + hour
    df["Date"] = df["Date"].replace("", pd.NA).ffill() + "/2025"
    df["Date"] = pd.to_datetime(df["Date"], format="%m/%d/%Y")
    df.iloc[:, 1] = pd.to_numeric(df.iloc[:, 1], errors="coerce")
    df["Date"] = df["Date"] + pd.to_timedelta(df.iloc[:, 1], unit="h")

    return df


# ------------------------------------------------------------------
# 2.  Convert a row to canonical dict
# ------------------------------------------------------------------
def convert_forecast_dict(row: pd.Series, station: str) -> dict:
    tz_map = {
        "KNYC": "US/Eastern",
        "KMDW": "US/Central",
        "KAUS": "US/Central",
        "KMIA": "US/Eastern",
        "KDEN": "US/Mountain",
        "KPHL": "US/Eastern",
        "KLAX": "US/Pacific",
    }
    tz = pytz.timezone(tz_map[station])

    # observation_time
    date_local = row["Date"].tz_localize(None)
    hour_local = int(row.iloc[1])  # Hour column
    obs_dt = tz.localize(
        datetime(date_local.year, date_local.month, date_local.day, hour_local)
    )

    # inserted_at (UTC, truncated to whole hour)
    inserted_dt = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

    def to_float(val):
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    return {
        "inserted_at": inserted_dt.isoformat(timespec="seconds"),
        "station": station,
        "observation_time": obs_dt.isoformat(timespec="seconds"),
        "air_temp": to_float(row.get("Temperature (°F)")),
        "relative_humidity": to_float(row.get("Relative Humidity (%)")),
        "dew_point": to_float(row.get("Dewpoint (°F)")),
        "wind_speed": to_float(row.get("Surface Wind (mph)")),
    }


# ------------------------------------------------------------------
# 3.  DB bootstrap
# ------------------------------------------------------------------
DDL = """
CREATE TABLE IF NOT EXISTS forecast (
    inserted_at      TEXT NOT NULL,
    station          TEXT NOT NULL,
    observation_time TEXT NOT NULL,
    air_temp         REAL,
    relative_humidity REAL,
    dew_point        REAL,
    wind_speed       REAL,
    PRIMARY KEY (inserted_at, station, observation_time)
);
"""

INSERT_SQL = """
INSERT OR IGNORE INTO forecast
(inserted_at, station, observation_time,
 air_temp, relative_humidity, dew_point, wind_speed)
VALUES
(:inserted_at, :station, :observation_time,
 :air_temp, :relative_humidity, :dew_point, :wind_speed)
"""


def init_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute(DDL)
    conn.commit()
    return conn


# ------------------------------------------------------------------
# 4.  One-shot ingestion for a station
# ------------------------------------------------------------------
def store_forecast(conn: sqlite3.Connection, station: str) -> None:
    try:
        df = extract_forecast(station)
    except Exception as e:
        print(f"[{station}] extract failed: {e}")
        return

    records = [convert_forecast_dict(row, station) for _, row in df.iterrows()]
    conn.executemany(INSERT_SQL, records)
    conn.commit()
    print(f"[{station}] {len(records)} rows upserted.")


# ------------------------------------------------------------------
# 5.  CLI + endless loop
# ------------------------------------------------------------------
parser = argparse.ArgumentParser(description="Hourly forecast logger")
parser.add_argument(
    "db_file",
    nargs="?",
    default="forecast.db",
    help="SQLite file (default: forecast.db)",
)
args = parser.parse_args()

STATIONS = ["KNYC", "KMDW", "KAUS", "KMIA", "KDEN", "KPHL", "KLAX"]

if __name__ == "__main__":
    conn = init_db(args.db_file)
    while True:
        now = datetime.now()
        next_run = now.replace(minute=0, second=0, microsecond=0) + timedelta(
            hours=1, minutes=10
        )
        time.sleep((next_run - now).total_seconds())
        for st in STATIONS:
            try:
                store_forecast(conn, st)
            except Exception as exc:
                print("ERROR:", exc)
