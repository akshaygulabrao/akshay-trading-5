import requests
import pandas as pd
import datetime
from zoneinfo import ZoneInfo
import utils
from weather_info import sites,accurate_sensor_minute

def latest_sensor_reading(site):
    base_url = "https://api.mesowest.net/v2/stations/timeseries"

    params = {
        "STID": "KLAX",
        "showemptystations": "1",
        "units": "temp|C,speed|mph,english",
        "recent": "120",
        "token": "d8c6aee36a994f90857925cea26934be",
        "complete": "1",
        "obtimezone": "local"
    }
    if site not in accurate_sensor_minute.keys():
        raise Exception("site invalid")
    
    params["STID"] = site
    response = requests.get(base_url, params=params)

    if response.status_code != 200:
        raise Exception("Observations not found")

    data = response.json()
    df = pd.DataFrame.from_dict(data["STATION"][0]["OBSERVATIONS"])
    df["date_time"] = pd.to_datetime(df["date_time"])
    df = df.set_index("date_time")
    last_entry = df[df.index.minute == accurate_sensor_minute[site]].iloc[-1]
    d = last_entry.name.to_pydatetime()
    d = d.replace(tzinfo=ZoneInfo(utils.sites2tz[site]))
    t = float(last_entry.air_temp_set_1) * 1.8 + 32
    return d,t

if __name__ == "__main__":
    for site in sites.keys():
        print(latest_sensor_reading(site))