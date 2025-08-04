import sqlite3
import weather_sensor_reading
import time
import argparse
from datetime import datetime as dt
from datetime import timezone

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
    UNIQUE(station, observation_time)          -- prevents duplicates
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
    data = weather_sensor_reading.get_timeseries(station_code)[-1]
    record = (
        dt.now(timezone.utc).isoformat(timespec="seconds"),
        station_code,
        data["date_time"],
        data["air_temp"],
        data["relative_humidity"],
        data["dew_point"],
        data["wind_speed"],
    )
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute(INSERT_SQL, record)
        conn.commit()


# ------------------------------------------------------------------
# 3. Run forever
# ------------------------------------------------------------------
if __name__ == "__main__":
    while True:
        try:
            for i in ["KNYC", "KMDW", "KAUS", "KMIA", "KDEN", "KPHL", "KLAX"]:
                store_latest_reading(i)
        except Exception as e:
            print("ERROR:", e)
        time.sleep(1)
