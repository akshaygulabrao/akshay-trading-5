import requests
import pandas as pd

def filter_mkts(mkts,keyword):
    return [i for i in mkts if keyword in i['rules_primary']]

if __name__ == "__main__":
    url = 'https://api.elections.kalshi.com/trade-api/v2/markets'
    params = {'limit': 1000, 'status': 'open'}
    response = requests.get(url,params).json()
    mkts = []
    while response['cursor'] != '':
        mkts.extend(response['markets'])
        params['cursor'] = response['cursor']
        response = requests.get(url,params).json()
    
    print('basketball')
    mkts_basketball = filter_mkts(mkts,'basketball')
    for i in mkts_basketball:
        print(i['ticker'])
    print()

    print('baseball')
    mkts_baseball = filter_mkts(mkts,'baseball game')
    for i in mkts_baseball:
        print(i['ticker'])
    print()

    print('tennis')
    mkts_tennis = filter_mkts(mkts,'2025 WTA Rome')
    for i in mkts_tennis:
        print(i['ticker'])