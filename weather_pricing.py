import scipy.stats as stats
from bs4 import BeautifulSoup
import pandas as pd
from utils import get_markets

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




