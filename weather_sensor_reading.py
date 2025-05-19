import requests
import pandas as pd
import datetime
from zoneinfo import ZoneInfo
import utils
from loguru import logger
from weather_info import nws_sites,accurate_sensor_minute,nws_site2kalshi_site
import weather_info

base_url = "https://api.mesowest.net/v2/stations/timeseries"

params = {
    "STID": "KLAX",
    "showemptystations": "1",
    "units": "temp|F,speed|mph,english",
    "recent": "1440",
    "token": "d8c6aee36a994f90857925cea26934be",
    "complete": "1",
    "obtimezone": "local"
}

def sensor_reading_history(nws_site):
    if nws_site not in accurate_sensor_minute.keys():
        raise Exception("site invalid")
    
    params["STID"] = nws_site
    response = requests.get(base_url, params=params)

    if response.status_code != 200:
        raise Exception("Observations not found")
    try:
        data = response.json()
    except requests.JSONDecodeError:
        logger.debug(f'{response}')
    df = pd.DataFrame.from_dict(data["STATION"][0]["OBSERVATIONS"])
    df["date_time"] = pd.to_datetime(df["date_time"])
    df = df.set_index("date_time")
    return df

def latest_sensor_reading(nws_site):
    df = sensor_reading_history(nws_site)
    last_entry = df.iloc[-1]
    d = last_entry.name.to_pydatetime()
    d = d.replace(tzinfo=ZoneInfo(utils.nws_site2tz[nws_site]))
    t = last_entry.air_temp_set_1
    max_dt = df.resample('D')[[df.columns[0]]].idxmax().iloc[-1].values[0].astype('datetime64[s]').item()
    max_dt = max_dt.replace(tzinfo=ZoneInfo(utils.nws_site2tz[nws_site]))
    max_temp = df.resample('D')[[df.columns[0]]].max().iloc[-1].values[0]
    return (d,t),(max_dt,max_temp)

def test_latest_sensor_reading():
    for k_site in weather_info.kalshi_sites:
        n_site = weather_info.kalshi_site2nws_site[k_site]
        (d,t),(md,mt) = latest_sensor_reading(n_site)
        print(f"{k_site}: current: {d},{t} max: {md} {mt}")



if __name__ == "__main__":
    test_latest_sensor_reading()