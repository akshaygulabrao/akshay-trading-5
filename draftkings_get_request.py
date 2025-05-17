import asyncio
import numpy as np
import pandas as pd
import os
import json

import draftkings_init

t0 = draftkings_init.fetch_all_sports()

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

for sport in sports:
    for events in sport_json[sport]['events']:
        if events['status'] == 'STARTED':
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
            name2odds[selection['label']] = selection['trueOdds']

for sport,events in sports2events_id.items():
    print(sport)
    for event_id in events:
        print(' ', event_id,'->',event_id2event_name[event_id])
        for name in market_id2names[event_id2market_id[event_id]]:
            print('  ->',name, name2odds[name])
