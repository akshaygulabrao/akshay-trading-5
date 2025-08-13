#!.venv/bin/python
"""
merge_weather.py  –  Merge two weather databases.

Usage:
    python merge_weather.py target.db source.db

The script inserts every row from source.db into target.db.
Rows whose (station, observation_time) already exist in target.db
are skipped automatically thanks to the UNIQUE constraint.
"""

import sqlite3
import sys,os
import pathlib

DDL = """
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


def merge(target_path: pathlib.Path, source_path: pathlib.Path) -> None:
    # Open both databases
    target = sqlite3.connect(target_path)
    source = sqlite3.connect(source_path)

    # Ensure the schema exists in the target (it almost certainly does already)
    target.executescript(DDL)

    # Attach the second database so we can copy across with one SQL statement
    target.execute("ATTACH DATABASE ? AS src", (str(source_path),))

    # Insert every row from src.weather into main.weather
    # The INSERT OR IGNORE skips rows that violate the UNIQUE constraint
    target.execute(
        """
        INSERT OR IGNORE INTO main.weather
              (inserted_at, station, observation_time,
               air_temp, relative_humidity, dew_point, wind_speed)
        SELECT inserted_at, station, observation_time,
               air_temp, relative_humidity, dew_point, wind_speed
        FROM   src.weather;
    """
    )

    target.commit()
    target.close()
    source.close()
    print("Merge complete.")


if __name__ == "__main__":
    def _require_envs(*names):
        for n in names:
            p = os.getenv(n)
            if p is None or not pathlib.Path(p).exists():
                sys.exit(f"Missing or invalid env var {n}")

    _require_envs("WEATHER_DB_PATH")

    source_db = "./db_backup/weather.db"

    merge(os.getenv("WEATHER_DB_PATH"), source_db)

