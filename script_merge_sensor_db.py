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
import sys
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
    if len(sys.argv) != 3:
        print("Usage: python merge_weather.py target.db source.db")
        sys.exit(1)

    target_db = pathlib.Path(sys.argv[1])
    source_db = pathlib.Path(sys.argv[2])

    if not target_db.exists():
        print(f"Target database '{target_db}' does not exist.")
        sys.exit(1)
    if not source_db.exists():
        print(f"Source database '{source_db}' does not exist.")
        sys.exit(1)

    merge(target_db, source_db)
