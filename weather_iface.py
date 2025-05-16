import asyncio
import os
import datetime as dt

import httpx

from utils import now,urls
from weather_extract_forecast import forecast_day
from weather_sensor_reading import weather

global exch_active,wthr_tkrs,site2forecast,site2sensor
def sensor_reading_history(site):
    async with httpx.AsyncClient() as client:
        while True:

    params["STID"] = site
    response = requests.get(base_url, params=params)

    if response.status_code != 200:
        raise Exception("Observations not found")

    data = response.json()
    df = pd.DataFrame.from_dict(data["STATION"][0]["OBSERVATIONS"])
    df["date_time"] = pd.to_datetime(df["date_time"])
    df = df.set_index("date_time")
    return df

async def ui():
    global exch_active,wthr_tkrs
    while True:
        os.system('clear')
        print(f'{now('KLAX')} {exch_active=}')
        for m in wthr_tkrs:
            print(m)
        await asyncio.sleep(1)

async def weather_markets():
    global wthr_tkrs
    markets = '/trade-api/v2/markets'
    params = {'status':'open','series_ticker':''}
    m = []
    async with httpx.AsyncClient() as client:
        while True:
            m = []
            for site in ["NY","CHI","AUS","MIA","DEN","PHIL","LAX"]:
                params['series_ticker'] = f'KXHIGH{site}'
                response = await client.get(f'{urls['mkts']}',params=params)
                if response.status_code != 200:
                    raise Exception("get_mkts did not ret 200")
                tkrs = [i['ticker'] for i in response.json()['markets']]
                tkrs = sorted(tkrs,key=lambda x: float(x[5 + len(site) + 9 + 2:]))
                m.extend(tkrs)
            wthr_tkrs = m
            await asyncio.sleep(20)

async def latest_sensor_reading(site):
    global site2sensor
    async with httpx.AsyncClient() as client:
        while True:
            m = []
            for site in ["NY","CHI","AUS","MIA","DEN","PHIL","LAX"]:
                params['series_ticker'] = f'KXHIGH{site}'
                response = await client.get(f'{urls['mkts']}',params=params)
                if response.status_code != 200:
                    raise Exception("get_mkts did not ret 200")
                tkrs = [i['ticker'] for i in response.json()['markets']]
                tkrs = sorted(tkrs,key=lambda x: float(x[5 + len(site) + 9 + 2:]))
                m.extend(tkrs)
            wthr_tkrs = m
            await asyncio.sleep(20)

async def exch_status():
    global exch_active
    status = '/trade-api/v2/exchange/status'
    async with httpx.AsyncClient() as client:
        while True:
            response = await client.get(f'{urls['status']}')
            if response.status_code != 200:
                raise Exception("exch status did not ret 200")
            exch_active = response.json()['trading_active']
            await asyncio.sleep(1)


async def main():
    global exch_active,wthr_tkrs,site2forecast,site2sensor
    exch_active = False
    wthr_tkrs = []
    site2sensor = {}
    await asyncio.gather(
        asyncio.create_task(weather_markets()),
        asyncio.create_task(exch_status()),
        asyncio.create_task(latest_sensor_reading()),
        asyncio.create_task(ui()),
    )

if __name__ == "__main__":
    asyncio.run(main())