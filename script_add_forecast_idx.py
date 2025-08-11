#!.venv/bin/python
import sqlite3
import pandas as pd
import sys

db_wo_idx = sys.argv[1]

with sqlite3.connect(db_wo_idx) as conn:
    # Check if the 'forecast' table already has the 'idx' column
    cursor = conn.execute("PRAGMA table_info(forecast)")
    columns = [col[1] for col in cursor.fetchall()]

    if "idx" not in columns:
        # Read data from the original forecast table
        df = pd.read_sql("SELECT * FROM forecast", conn)

        # Add idx column if it doesn't exist
        df["idx"] = df.groupby(["inserted_at", "station"]).cumcount()

        # Create forecast_clean table if it doesn't exist
        conn.execute(
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

        # Clear forecast_clean if it already has data (to avoid duplicates)
        conn.execute("DELETE FROM forecast_clean")

        # Append the cleaned data
        df.to_sql(
            "forecast_clean",
            conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=500,
        )

        # Replace the original forecast table
        conn.execute("DROP TABLE IF EXISTS forecast")
        conn.execute("ALTER TABLE forecast_clean RENAME TO forecast")
        conn.execute("VACUUM")
    else:
        # If idx already exists, assume the table is already cleaned
        print("Table already cleaned. Skipping.")
