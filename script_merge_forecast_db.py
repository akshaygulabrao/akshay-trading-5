#!.venv/bin/python
import sqlite3
import sys, os, pathlib

DDL = """
    CREATE TABLE IF NOT EXISTS forecast (
        inserted_at      TEXT,
        idx              INT,
        station          TEXT,
        observation_time TEXT,
        air_temp         REAL,
        dew_point         REAL,
        relative_humidity REAL,
        wind_speed       REAL,
        PRIMARY KEY (idx, station, observation_time)
    )
"""


def count_rows(path: str) -> int:
    """Return number of rows in forecast table."""
    con = sqlite3.connect(path)
    n = con.execute("SELECT COUNT(*) FROM forecast").fetchone()[0]
    con.close()
    return n


def merge(master_path: str, src_path: str) -> None:
    db1 = sqlite3.connect(master_path)
    db2 = sqlite3.connect(src_path)

    db1.executescript(DDL)
    db2.executescript(DDL)

    pre = count_rows(master_path)  # rows in master before

    db1.execute("ATTACH DATABASE ? AS src", (src_path,))
    db1.execute("INSERT OR IGNORE INTO main.forecast SELECT * FROM src.forecast")
    db1.commit()

    post = count_rows(master_path)  # rows in master after
    src_rows = count_rows(src_path)  # rows in src

    db1.close()
    db2.close()

    print(f"master rows: {post}")
    print(f"src rows: {src_rows}")
    print(f"rows inserted: {post - pre}")


if __name__ == "__main__":

    def _require_envs(*names):
        for n in names:
            p = os.getenv(n)
            if p is None or not pathlib.Path(p).exists():
                sys.exit(f"Missing or invalid env var {n}")

    _require_envs("FORECAST_DB_PATH")

    source_db = "./db_backup/forecast.db"

    merge(os.getenv("FORECAST_DB_PATH"), source_db)
