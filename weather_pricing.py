import scipy.stats as stats
from bs4 import BeautifulSoup
import pandas as pd
from utils import get_markets
from weather_sensor_reading import sensor_reading_history
from weather_extract_forecast import extract_forecast
from weather_info import nws_site2kalshi_site

import requests

def contract_price(forecasted_max,hours_till,spread_error,strikes_allowed, max_so_far):
    if forecasted_max < max_so_far:
        return 0

    mu = (forecasted_max - 32) * 5/9
    #sigma computed using 0.05 error for celsius degrees, 0.1 volatility
    sigma = 0.15**2 * (hours_till**(1/2))
    spread = 0.20**2 * (hours_till**(1/2)) # we sell if they overestimate the error
    prob = 1 - stats.norm.cdf(0, loc=mu, scale=sigma)
    return prob

mkts = get_markets()
for site in nws_site2kalshi_site.keys():
    site_mkts = [i for i in mkts if i.startswith(f"KXHIGH{nws_site2kalshi_site[site]}")]
    df = sensor_reading_history(site)