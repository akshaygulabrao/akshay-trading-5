import scipy.stats as stats
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from utils import get_markets
import datetime
from io import StringIO
from collections import defaultdict
from weather_info import sites2tz,sites2forecast

import requests

def extract_forecast(site):
    """
    Returns the maximum temperature and its datetime for the specified date.
    """
    url = sites2forecast.get(site)
    if not url:
        raise Exception("URL not found")
    
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception("Response not status 200")
    
    soup = BeautifulSoup(response.text, 'html.parser')
    try:
        table = soup.find_all('table')[4]
    except IndexError:
        raise Exception(f"Site {site} is down.")

    
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

def forecast_day(site,date):
    df = extract_forecast(site)

    df_date = df[df.index.date == date]
    
    if len(df_date) == 0:
        return None, None
    
    # Get the time of maximum temperature for the specific date
    max_temp_time = df_date['Temperature (°F)'].idxmax()
    max_temp = df_date['Temperature (°F)'].max()
    
    return max_temp_time, max_temp

if __name__ == "__main__":
    for site in sites2forecast.keys():
        print(extract_forecast(site))