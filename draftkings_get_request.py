import asyncio
import numpy as np
import pandas as pd
import os
import json
import requests
from utils import urls
from loguru import logger 

import draftkings_init

kalshi_url = 'https://api.elections.kalshi.com'
urls['markets']

t0 = asyncio.run(draftkings_init.fetch_all_sports())


sports = list(draftkings_init.draftkings_links.keys())
sport_json = {}
for sport in sports:
    with open(f'sports/{sport.lower()}.json') as f:
        sport_json[sport] = json.load(f)


sports2events_id = {}
event_id2event_name = {}
event_id2market_id = {}
market_id2event_id = {}
selection_id2market_id = {}
selection_id2participants = {}
market_id2names = {}
name2odds = {}
name2kalshi_mkts = {}
market_ids_traded = set(['KXMLBGAME-25MAY18HOUTEX-HOU'])

for sport in sports:
    for events in sport_json[sport]['events']:
        if events['status'] in ['STARTED','NOT_STARTED']:
            if sport not in sports2events_id:
                sports2events_id[sport] = []
            sports2events_id[sport].append(events['id'])
            event_id2event_name[events['id']] = events['name']
    for market in sport_json[sport]['markets']:
        if market['name'] in ['Winner','Moneyline']:
            market_id2event_id[market['id']] = market['eventId']
            event_id2market_id[market['eventId']] = market['id']
    for selection in sport_json[sport]['selections']:
        if selection['marketId'] in market_id2event_id:
            if selection['marketId'] not in market_id2names:
                market_id2names[selection['marketId']] = []
            market_id2names[selection['marketId']].append(selection['label'])
            name2odds[selection['label']] = 1 / selection['trueOdds']

sport2kalshi_series = {'Golf': 'KXPGA', 'Baseball' : 'KXMLBGAME', 'UFC': 'KXUFCFIGHT', 'EPL': 'KXEPLGAME', 'Hockey': 'KXNHLGAME', 'Basketball': 'KXNBASERIES', 'Tennis': 'KXATPIT'}

baseball_exceptions = {"KC Royals": "KC", "ARI Diamondbacks": "AZ", "SD Padres": "SD", "WAS Nationals": "WSH",
                       'TB Rays': 'TB', 'TOR Blue Jays': 'TOR', 'LA Angels': 'LAA', 'LA Dodgers': 'LAD', 'NY Yankees': 'NYY', 'NY Mets': 'NYM','CHI White Sox': 'CWS', 'CHI Cubs': 'CHC', 'Athletics' :'ATH', 'SF Giants': 'SF'}
for sport in sports2events_id.keys():
    params = {'series_ticker': sport2kalshi_series[sport],'status':'open','limit':100}
    response = requests.get(kalshi_url+urls['markets'],params=params)
    if response.status_code != 200:
        raise ValueError("status not 200")
    kalshi_markets = response.json()['markets']
    for event_id in sports2events_id[sport]:
        for name in market_id2names[event_id2market_id[event_id]]:
            for kalshi_mkt in kalshi_markets:
                if sport in ["UFC", "Golf", 'Tennis'] and name in kalshi_mkt['rules_primary']:
                    if name in name2kalshi_mkts:
                        raise ValueError('kalshi mkt already found for name')
                    name2kalshi_mkts[name] = kalshi_mkt['ticker']
                elif sport == "Baseball":
                    name_split = name.split()
                    if name in baseball_exceptions:
                        if kalshi_mkt["ticker"].count(baseball_exceptions[name]) == 2:
                            if name in name2kalshi_mkts:
                                print(name,name2kalshi_mkts[name])
                                raise ValueError('kalshi mkt already found for name')
                            name2kalshi_mkts[name] = kalshi_mkt['ticker']
                    elif len(name_split) == 2 and len(name_split[0]) == 3: #city abbrev
                        if kalshi_mkt["ticker"].count(name_split[0].upper()) == 2:
                            if name in name2kalshi_mkts:
                                print(name,name2kalshi_mkts[name])
                                raise ValueError('kalshi mkt already found for name')
                            name2kalshi_mkts[name] = kalshi_mkt['ticker']



for sport in sports2events_id.keys():
    params = {'series': sport2kalshi_series[sport],'status':'open','limit':100}
    for event_id in sports2events_id[sport]:
        print('  ',event_id, ' -> ', event_id2event_name[event_id])
        odds_total = 0
        for name in market_id2names[event_id2market_id[event_id]]:
            odds_total += name2odds[name]
        for name in market_id2names[event_id2market_id[event_id]]:
            if name in name2kalshi_mkts:
                print('    ', f'{name}({name2odds[name]/ odds_total:.03f})', ' -> ', name2kalshi_mkts[name])