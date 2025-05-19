import requests
import pandas as pd
import datetime
from zoneinfo import ZoneInfo
import utils
from weather_info import nws_sites,accurate_sensor_minute,nws_site2kalshi_site

base_url = "https://api.mesowest.net/v2/stations/timeseries"

params = {
    "STID": "KLAX",
    "showemptystations": "1",
    "units": "temp|F,speed|mph,english",
    "recent": "120",
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

    data = response.json()
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
    return d,t

if __name__ == "__main__":
    for nws_site in nws_sites.keys():
        
        d,t = latest_sensor_reading(nws_site)
        print(nws_site,t, (utils.now() - d).total_seconds() / 60)