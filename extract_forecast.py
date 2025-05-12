import scipy.stats as stats
from bs4 import BeautifulSoup
import pandas as pd
from utils import get_markets
import datetime
from io import StringIO
from collections import defaultdict

import requests

sites2tz = {
    "KNYC": "America/New_York",
    "KMDW": "America/Chicago",
    "KAUS": "America/Chicago",
    "KMIA": "America/New_York",
    "KDEN": "America/Denver",
    "KPHL": "America/New_York",
    "KLAX": "America/Los_Angeles"
}

sites2forecast = {
    "KNYC": "https://forecast.weather.gov/MapClick.php?lat=40.78&lon=-73.97&lg=english&&FcstType=digital",
    "KMDW": "https://forecast.weather.gov/MapClick.php?lat=41.78&lon=-87.76&lg=english&&FcstType=digital",
    "KAUS": "https://forecast.weather.gov/MapClick.php?lat=30.18&lon=-97.68&lg=english&&FcstType=digital",
    "KMIA": "https://forecast.weather.gov/MapClick.php?lat=25.7554&lon=-80.2262&lg=english&&FcstType=digital",
    "KDEN": "https://forecast.weather.gov/MapClick.php?lat=39.85&lon=-104.66&lg=english&&FcstType=digital",
    "KPHL": "https://forecast.weather.gov/MapClick.php?lat=40.08&lon=-75.01&lg=english&&FcstType=digital",
    "KLAX": "https://forecast.weather.gov/MapClick.php?lat=33.96&lon=-118.42&lg=english&&FcstType=digital",
}

def extract_forecast(site, date):
    """
    Returns the maximum temperature and its datetime for the specified date.
    """
    url = sites2forecast.get(site)
    if not url:
        return None, None
    
    response = requests.get(url)
    if response.status_code != 200:
        return None, None
    
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find_all('table')[4]
    
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
    df.to_csv("weather_sample.csv",index=None)
    df = pd.read_csv("weather_sample.csv")
    df["Date"] = df["Date"].ffill()
    df["Date"] = df["Date"].apply(lambda x: x + "/25")
    df["Date"] = pd.to_datetime(df["Date"],format="%m/%d/%y")
    df["Date"] = df["Date"] + df.iloc[:,1].apply(lambda x: datetime.timedelta(hours=x))
    df = df.set_index("Date")

    df_date = df[df.index.date == date]
    
    if len(df_date) == 0:
        return None, None
    
    # Get the time of maximum temperature for the specific date
    max_temp_time = df_date['Temperature (°F)'].idxmax()
    max_temp = df_date['Temperature (°F)'].max()
    
    return max_temp_time, max_temp


for site in sites2forecast.keys():
    print(extract_forecast(site, datetime.date(year=2025,month=5,day=13)))