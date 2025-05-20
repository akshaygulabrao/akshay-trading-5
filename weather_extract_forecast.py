import scipy.stats as stats
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from utils import get_markets
from collections import defaultdict
from weather_info import nws_site2tz,nws_site2forecast

import requests

def extract_forecast(nws_site):
    """
    Returns the maximum temperature and its datetime for the specified date.
    """
    assert nws_site in nws_site2forecast
    url = nws_site2forecast.get(nws_site)
    if not url:
        raise Exception("URL not found")
    
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception("Response not status 200")
    
    soup = BeautifulSoup(response.text, 'html.parser')
    try:
        table = soup.find_all('table')[4]
    except IndexError:
        raise Exception(f"Site {nws_site} is down.")

    
    forecast_dict = defaultdict(list)
    data = []
    for row in table.find_all('tr'):
        row_data = [cell.get_text(strip=True) for cell in row.find_all(['td', 'th'])]
        data.append(row_data)
    
    for d in data:
        if d[0] == '':
            continue
        elif d[0] in forecast_dict:
            forecast_dict[d[0]].extend(d[1:])
        else:
            forecast_dict[d[0]] = d[1:]
    df = pd.DataFrame.from_dict(forecast_dict)
    df["Date"] = df["Date"].replace('', np.nan).ffill()
    df["Date"] = df["Date"].apply(lambda x: x + "/2025")
    df["Date"] = pd.to_datetime(df["Date"], format="%m/%d/%Y")
    df.iloc[:, 1] = df.iloc[:, 1].astype(int)
    
    df["Date"] = df["Date"] + pd.to_timedelta(df.iloc[:, 1], unit='h')
    df = df.set_index("Date")
    return df

def forecast_day(nws_site):
    df = extract_forecast(nws_site)
    hr = df.resample('D')[[df.columns[1]]].idxmax().values
    tmp = df.resample('D')[[df.columns[1]]].max().astype(float).values
    res = []
    for i in range(len(hr)):
        dt = hr[i][0].astype('datetime64[s]').item()
        formatted_date = dt.strftime("%y%b%d").upper()
        hour = dt.hour
        res.append((formatted_date,hour,float(tmp[i][0])))
    return res,df.iloc[0,1]

def test_forecast_day():
    for site in nws_site2forecast.keys():
        print(forecast_day(site))

if __name__ == "__main__":
    test_forecast_day()