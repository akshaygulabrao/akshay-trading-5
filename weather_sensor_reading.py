import asyncio
from zoneinfo import ZoneInfo

import requests
import httpx
import pandas as pd
from loguru import logger

import utils
from weather_info import nws_sites, accurate_sensor_minute, nws_site2kalshi_site
import weather_info

base_url = "https://api.mesowest.net/v2/stations/timeseries"

params = {
    "STID": "KLAX",
    "showemptystations": "1",
    "units": "temp|F,speed|mph,english",
    "recent": "1440",
    "token": "d8c6aee36a994f90857925cea26934be",
    "complete": "1",
    "obtimezone": "local",
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
        logger.debug(f"{response}")
    df = pd.DataFrame.from_dict(data["STATION"][0]["OBSERVATIONS"])
    df["date_time"] = pd.to_datetime(df["date_time"])
    df = df.set_index("date_time")
    return df


async def sensorHistoryAsync(nws_site):
    if nws_site not in accurate_sensor_minute.keys():
        raise Exception("site invalid")

    params["STID"] = nws_site
    async with httpx.AsyncClient() as client:
        response = await client.get(base_url, params=params)
    if response.status_code != 200:
        raise ValueError(f"Bad response for {nws_site}: HTTP {response.status_code}")
    try:
        data = response.json()
    except requests.JSONDecodeError:
        logger.debug(f"{response}")
    df = pd.DataFrame.from_dict(data["STATION"][0]["OBSERVATIONS"])
    df["date_time"] = pd.to_datetime(df["date_time"])
    df = df.set_index("date_time")
    return df


async def latest_sensor_reading_async(nws_site):
    df = await sensorHistoryAsync(nws_site)
    last_entry = df.iloc[-1]
    d = last_entry.name.to_pydatetime()
    d = d.replace(tzinfo=ZoneInfo(utils.nws_site2tz[nws_site]))
    t = last_entry.air_temp_set_1
    max_dt = (
        df.resample("D")[[df.columns[0]]]
        .idxmax()
        .iloc[-1]
        .values[0]
        .astype("datetime64[s]")
        .item()
    )
    max_dt = max_dt.replace(tzinfo=ZoneInfo(utils.nws_site2tz[nws_site]))
    max_temp = df.resample("D")[[df.columns[0]]].max().iloc[-1].values[0]
    return (d, t), (max_dt, max_temp)


async def test_sensorHistoryAsync():
    tasks = [sensorHistoryAsync(nws_site) for nws_site in nws_sites]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for site, result in zip(nws_sites, results):
        if isinstance(result, Exception):
            raise KeyError("Exception")
        else:
            print(result.head())


async def test_latestSensorReading_async():
    tasks = [latest_sensor_reading_async(nws_site) for nws_site in nws_sites]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for site, result in zip(nws_sites, results):
        if isinstance(result, Exception):
            logger.error(f"Exception occurred for {site}: {result}")
            raise KeyError(f"Exception for {site}")
        (d, t), (max_dt, max_temp) = result
        if None in [d, t, max_dt, max_temp]:
            raise KeyError(f"FAIL for {site}")
        print(f"{site}: {d} Temp: {t}")


async def main():
    await test_sensorHistoryAsync()
    await test_latestSensorReading_async()


if __name__ == "__main__":
    asyncio.run(main())
