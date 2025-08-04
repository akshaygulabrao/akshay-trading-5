#!.venv/bin/python
import sqlite3
import sys

DDL = """
CREATE TABLE IF NOT EXISTS forecast (
    inserted_at       TEXT NOT NULL,
    station           TEXT NOT NULL,
    observation_time  TEXT NOT NULL,
    air_temp          REAL,
    relative_humidity REAL,
    dew_point         REAL,
    wind_speed        REAL,
    PRIMARY KEY (inserted_at, station, observation_time)
);
"""


def merge(db1_path: str, db2_path: str) -> None:
    # Connect to both DBs
    db1 = sqlite3.connect(db1_path)
    db2 = sqlite3.connect(db2_path)

    # Ensure table exists in both (idempotent)
    db1.executescript(DDL)
    db2.executescript(DDL)

    # Attach db2 to db1 under alias 'src'
    db1.execute(f"ATTACH DATABASE ? AS src", (db2_path,))

    # Insert rows from src.forecast into db1.forecast, ignoring duplicates
    db1.execute(
        """
        INSERT OR IGNORE INTO main.forecast
        SELECT * FROM src.forecast;
    """
    )

    db1.commit()
    db1.close()
    db2.close()
    print("Merge complete.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python merge_forecasts.py arg1.db arg2.db")
        sys.exit(1)
    merge(sys.argv[1], sys.argv[2])
