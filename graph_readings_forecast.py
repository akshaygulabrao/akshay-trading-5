def graph_readings_forecast(site, days):
    import sqlite3
    import pandas as pd
    site2mkt = {"KLAX":"KXHIGHLAX",
                "KNYC": "KXHIGHNY",
                "KMDW":"KXHIGHCHI",
                "KAUS":"KXHIGHAUS", 
                "KMIA": "KXHIGHMIA",
                "KDEN":"KXHIGHDEN",
                "KPHL":"KXHIGHPHIL"}
    # ---- forecast ----------------------------------------------------------
    conn = sqlite3.connect('/opt/data/forecast.db')
    fcast = pd.read_sql_query(
        f'select * from forecast where station == "{site}"', conn
    )
    fcast['dt'] = pd.to_datetime(fcast['observation_time'], utc=False)
    conn.close()

    fcast_current = fcast[fcast.idx == 0].sort_values(by='dt').tail(24 * days)
    fcast_future  = fcast.tail(47)
    fcast = pd.concat([fcast_current, fcast_future])

    # ---- observations ------------------------------------------------------
    conn = sqlite3.connect('/opt/data/weather.db')
    wthr = pd.read_sql_query(
        f'select * from weather where station == "{site}"', conn
    )
    conn.close()
    wthr['dt'] = pd.to_datetime(wthr['observation_time'], utc=False)
    wthr = wthr.dropna(axis=0).tail(13 * 24 * days)

    # ---- build dict for d3 --------------------------------------------------
    return {
        "readings": {
            "site": site2mkt[site],
            "xs": wthr['dt'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist(),
            "ys": wthr['air_temp'].tolist()
        },
        "forecasts": {
            "site": site2mkt[site],
            "xs": fcast['dt'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist(),
            "ys": fcast['air_temp'].tolist()
        }
    }

