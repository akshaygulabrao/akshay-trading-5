import scipy.stats as stats
from bs4 import BeautifulSoup
import pandas as pd
from utils import get_markets
import datetime
from io import StringIO
from collections import defaultdict
from weather_info import sites2tz,sites2forecast

import requests

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

if __name__ == "__main__":
    for site in sites2forecast.keys():
        print(extract_forecast(site, datetime.date(year=2025,month=5,day=13)))