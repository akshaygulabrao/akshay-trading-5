import utils as utils
import requests
from utils import Site
from loguru import logger

# the setup prod fn fetches all credentials needed to interact with the kalshi API

client = utils.setup_client()
print("Client set up ")
balance = client.get_balance()
print(balance)
# {'balance': 199}

ny = [Site.NY]

events = utils.get_events_for_sites(ny)
print(events)
# [EventTicker(name='KXHIGHNY-25JUL04'), EventTicker(name='KXHIGHNY-25JUL03')]

markets = utils.get_markets_for_sites([Site.NY])
print(markets)
# {EventTicker(name='KXHIGHNY-25JUL04'): [MarketTicker(name='KXHIGHNY-25JUL04-T82'), MarketTicker(name='KXHIGHNY-25JUL04-B82.5'), MarketTicker(name='KXHIGHNY-25JUL04-B84.5'), MarketTicker(name='KXHIGHNY-25JUL04-B86.5'), MarketTicker(name='KXHIGHNY-25JUL04-B88.5'), MarketTicker(name='KXHIGHNY-25JUL04-T89')], EventTicker(name='KXHIGHNY-25JUL03'): [MarketTicker(name='KXHIGHNY-25JUL03-T85'), MarketTicker(name='KXHIGHNY-25JUL03-B85.5'), MarketTicker(name='KXHIGHNY-25JUL03-B87.5'), MarketTicker(name='KXHIGHNY-25JUL03-B89.5'), MarketTicker(name='KXHIGHNY-25JUL03-B91.5'), MarketTicker(name='KXHIGHNY-25JUL03-T92')]}

response = requests.get(
    "https://api.elections.kalshi.com/trade-api/v2/markets",
    {"series_ticker": "KXHIGHNY", "status": "open"},
)

[i["ticker"] for i in response.json()["markets"]]
