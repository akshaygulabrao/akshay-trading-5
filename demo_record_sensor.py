import sqlite3
import weather_sensor_reading
import time
import argparse
from datetime import datetime as dt
from datetime import timezone
from loguru import logger

# ------------------------------------------------------------------
# Parse command line
# ------------------------------------------------------------------
parser = argparse.ArgumentParser(description="Log weather observations to SQLite")
parser.add_argument(
    "db_file",
    nargs="?",
    default="weather.db",
    help="SQLite file to use (default: weather.db)",
)
args = parser.parse_args()
DB_FILE = args.db_file

# ------------------------------------------------------------------
# 1. Create table + unique index
# ------------------------------------------------------------------
ddl = """
CREATE TABLE IF NOT EXISTS weather (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    inserted_at        TEXT NOT NULL,
    station            TEXT NOT NULL,
    observation_time   TEXT,
    air_temp           REAL,
    relative_humidity  REAL,
    dew_point          REAL,
    wind_speed         REAL,
    UNIQUE(station, observation_time)
);
"""

with sqlite3.connect(DB_FILE) as conn:
    conn.execute(ddl)
    conn.commit()

# ------------------------------------------------------------------
# 2. Insert only if new (ignore duplicate)
# ------------------------------------------------------------------
INSERT_SQL = """
INSERT OR IGNORE INTO weather
(inserted_at, station, observation_time,
 air_temp, relative_humidity, dew_point, wind_speed)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""


def store_latest_reading(station_code="KLAX"):
    """
    Fetch the latest two observations for *station_code*,
    insert them into the DB (ignoring duplicates),
    and return *all* observations that were fetched
    (duplicates included).
    """
    readings = weather_sensor_reading.get_timeseries(station_code)[-2:]

    # Build the rows to be inserted
    rows = [
        (
            dt.now(timezone.utc).isoformat(timespec="seconds"),
            station_code,
            r["date_time"],
            r["air_temp"],
            r["relative_humidity"],
            r["dew_point"],
            r["wind_speed"],
        )
        for r in readings
    ]

    # Insert into the DB (duplicates will be ignored)
    with sqlite3.connect(DB_FILE) as conn:
        conn.executemany(INSERT_SQL, rows)
        conn.commit()

    # Return the observations exactly as they came from the sensor
    return readings


# ------------------------------------------------------------------
# 3. Run forever
# ------------------------------------------------------------------
if __name__ == "__main__":
    while True:
        try:
            for i in ["KNYC", "KMDW", "KAUS", "KMIA", "KDEN", "KPHL", "KLAX"]:
                r = store_latest_reading(i)
                logger.info(r)
        except Exception as e:
            print("ERROR:", e)
        time.sleep(1)
