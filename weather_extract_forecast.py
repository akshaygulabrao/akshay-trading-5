import asyncio
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from collections import defaultdict
from weather_info import nws_site2forecast
import httpx


async def extract_forecast(nws_site):
    """Fetches forecast data for a given NWS site."""
    url = nws_site2forecast.get(nws_site)
    if not url:
        raise ValueError(f"No URL found for site: {nws_site}")

    async with httpx.AsyncClient() as client:
        response = await client.get(url)
    if response.status_code != 200:
        raise ValueError(f"Bad response for {nws_site}: HTTP {response.status_code}")

    soup = BeautifulSoup(response.text, "html.parser")
    try:
        table = soup.find_all("table")[4]
    except IndexError:
        raise ValueError(f"Forecast table not found for {nws_site}")

    forecast_dict = defaultdict(list)
    for row in table.find_all("tr"):
        row_data = [cell.get_text(strip=True) for cell in row.find_all(["td", "th"])]
        if row_data and row_data[0]:
            forecast_dict[row_data[0]].extend(row_data[1:])

    df = pd.DataFrame.from_dict(forecast_dict)
    df["Date"] = df["Date"].replace("", np.nan).ffill()
    df["Date"] = df["Date"].apply(lambda x: x + "/2025")
    df["Date"] = pd.to_datetime(df["Date"], format="%m/%d/%Y")
    df.iloc[:, 1] = df.iloc[:, 1].astype(int)
    df["Date"] = df["Date"] + pd.to_timedelta(df.iloc[:, 1], unit="h")
    return df.set_index("Date")


async def forecast_day(nws_site):
    """Extracts max temp and time for a given site."""
    df = await extract_forecast(nws_site)
    hr = df.resample("D")[[df.columns[1]]].idxmax().values
    tmp = df.resample("D")[[df.columns[1]]].max().astype(float).values
    res = []
    for i in range(len(hr)):
        dt = hr[i][0].astype("datetime64[s]").item()
        formatted_date = dt.strftime("%y%b%d").upper()
        hour = dt.hour
        res.append((formatted_date, hour, float(tmp[i][0])))
    return res, df.iloc[0, 1]


async def test_extract_forecast():
    """Tests forecast extraction for all sites CONCURRENTLY."""
    tasks = [extract_forecast(site) for site in nws_site2forecast.keys()]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for site, result in zip(nws_site2forecast.keys(), results):
        if isinstance(result, Exception):
            print(f"❌ Error for {site}: {result}")
        else:
            print(f"✅ Forecast for {site}:")
            print(result.head())


async def test_forecast_day():
    """Tests daily forecast for all sites CONCURRENTLY."""
    tasks = [forecast_day(site) for site in nws_site2forecast.keys()]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for site, result in zip(nws_site2forecast.keys(), results):
        if isinstance(result, Exception):
            print(f"❌ Error for {site}: {result}")
        else:
            print(f"✅ Daily forecast for {site}: {result}")


async def main():
    """Runs all tests concurrently."""
    await asyncio.gather(test_extract_forecast(), test_forecast_day())


if __name__ == "__main__":
    asyncio.run(main())
