import scipy.stats as stats
from bs4 import BeautifulSoup
import pandas as pd
from utils import get_markets
import datetime
from io import StringIO

import requests

sites2forecast = {"KLAX": "https://forecast.weather.gov/MapClick.php?lat=33.96&lon=-118.42&lg=english&&FcstType=digital",
                  "KMIA": "https://forecast.weather.gov/MapClick.php?lat=25.7554&lon=-80.2262&lg=english&&FcstType=digital",
                  "KDEN": "https://forecast.weather.gov/MapClick.php?lat=39.85&lon=-104.66&lg=english&&FcstType=digital",
                  "KAUS": "https://forecast.weather.gov/MapClick.php?lat=30.18&lon=-97.68&lg=english&&FcstType=digital",
                  "KNYC": "https://forecast.weather.gov/MapClick.php?lat=40.78&lon=-73.97&lg=english&&FcstType=digital",
                  "KPHL": "https://forecast.weather.gov/MapClick.php?lat=40.08&lon=-75.01&lg=english&&FcstType=digital",
                  "KCHI": "https://forecast.weather.gov/MapClick.php?lat=41.78&lon=-87.76&lg=english&&FcstType=digital"}

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
    # print(table)

    data = []
    for row in table.find_all('tr'):
        row_data = [cell.get_text(strip=True) for cell in row.find_all(['td', 'th'])]
        data.append(row_data)

    for row in data:
        print(','.join(row))

extract_forecast("KCHI",datetime.date(2025,5,12))