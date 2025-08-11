# Kalshi Trading

## Data Schemas

### Forecast
```sql
CREATE TABLE IF NOT EXISTS forecast (
    inserted_at      TEXT NOT NULL,
    idx              INT,
    station          TEXT NOT NULL,
    observation_time TEXT NOT NULL,
    air_temp         REAL,
    relative_humidity REAL,
    dew_point        REAL,
    wind_speed       REAL,
    PRIMARY KEY (idx, station, observation_time)
);
```

### Sensors
```sql
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
```

### Orderbook

```sql
CREATE TABLE IF NOT EXISTS orderbook_events (
    ts_micro        TEXT,
    exch_ts_micro   TEXT,
    seq_num         BIGINT,
    ticker          TEXT,
    side            SMALLINT,
    price           BIGINT,
    signed_qty      BIGINT,
    is_delta        BOOLEAN
);
```
