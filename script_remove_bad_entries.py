#!.venv/bin/python
import sqlite3
import pandas as pd
import sys

dirty_db = sys.argv[1]

# 1.  Load raw data
src = sqlite3.connect(dirty_db)
df = pd.read_sql("SELECT * FROM forecast", src)

# 2.  Compute helper columns
df["idx"] = df.groupby(["inserted_at", "station"]).cumcount()
inserted_utc = pd.to_datetime(df["inserted_at"], utc=True, format="ISO8601")
obs_utc = pd.to_datetime(df["observation_time"], utc=True, format="ISO8601")
expected = inserted_utc + pd.to_timedelta(df["idx"], unit="h")

# 3.  Filter to clean rows
clean = df[expected.dt.round("1s").eq(obs_utc.dt.round("1s"))].copy()
print("CLEAN")
print(clean)

dirty = df[~expected.dt.round("1s").eq(obs_utc.dt.round("1s"))].copy()
print("DIRTY")
print(dirty)
# 4.  Write clean rows into the SAME file with a new table that has the composite PK
dest = sqlite3.connect(dirty_db)

# 4a.  Create the new table with the desired primary key
dest.execute(
    """
    CREATE TABLE IF NOT EXISTS forecast_clean (
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
)

# 4b.  Populate it
clean.to_sql(
    "forecast_clean",
    dest,
    if_exists="append",
    index=False,
    method="multi",
    chunksize=500,
)

dest.commit()
dest.close()
src.close()

with sqlite3.connect(dirty_db) as conn:
    conn.execute("DROP TABLE IF EXISTS forecast")
    conn.execute("ALTER TABLE forecast_clean RENAME TO forecast")
    conn.execute("VACUUM")
