import asyncio
import httpx
import time
import requests
import uuid
from utils import kalshi_url,urls,setup_prod,setup_client
from . import kalshi_ref

"""
placement/cancellation order approximation: 16.3 seconds for 100 orders
~ 161ms per order with rate_limit = 100ms
"""

def place_order(client,t,a,p,q,limit_order=True,expiration_ts=None,post_only=False):
    """
    client,ticker,action,price,quantity,order_type
    """
    assert isinstance(client, kalshi_ref.KalshiHttpClient)
    assert a == 'buy' or a == 'sell'
    private_order_id = str(uuid.uuid4())
    params = {
        'ticker': t,
        'action': a,
        'side' : 'yes',
        'client_order_id': private_order_id,
        'count' : q,
        'yes_price': p,
    }
    params['type'] = 'limit' if limit_order else 'market'

    response = client.post(urls['orders'],body=params)
    public_order_id = response['order']['order_id']
    return public_order_id
    
def cancel_order(client,public_order_id):
    assert isinstance(client, kalshi_ref.KalshiHttpClient)
    response = client.delete(f'/trade-api/v2/portfolio/orders/{public_order_id}')
    order_status = response['order']['status']
    return order_status == 'canceled'

def get_resting_orders(client, ticker):
    assert isinstance(client, kalshi_ref.KalshiHttpClient)
    params = {
        'ticker' : ticker,
        'status' : 'resting',
        'limit' : 1000
    }
    response = client.get(f'{urls['orders']}',params)
    orders = response['orders']
    while response['cursor'] != '':
        params['cursor'] = response['cursor']
        response = client.get(f'{urls['orders']}',params)
        orders.extend(response['orders'])
    resting_orders = set()
    for order in orders:
        resting_orders.add((order['order_id'], order['yes_price'], order['remaining_count']))
    return resting_orders

def get_positions(client):
    assert isinstance(client, kalshi_ref.KalshiHttpClient)
    params = {
        'count_filter' : 'position',
        'limit' : 1000
    }
    response = client.get(f'{urls['positions']}',params)
    assert isinstance(response,dict)
    while response['cursor'] != '':
        params['cusor'] = response['cursor']
        response = client.get(f'{urls['positions']}',params)
    positions_list = response['market_positions']
    
    positions = {}
    for position in positions_list:
        positions[position['ticker']] = (position['position'], position['market_exposure'] / position['position'], position['realized_pnl'])
    return positions

if __name__ == "__main__":
    client = setup_client()
    assert isinstance(client, kalshi_ref.KalshiHttpClient)
    practice_order_ticker = 'KXLLMCHESS-26'
    
    # places and cancels orders repeatedly
    # start = time.time()
    # for i in range(50):
    #     public_order_id = place_order(client,
    #                 practice_order_ticker,
    #                 'buy',
    #                 1,
    #                 1,
    #             )
    #     status = cancel_order(client,public_order_id)
    #     assert status == True
    # end = time.time()
    # print(end - start)

    # gets market overview
    print(get_resting_orders(client,practice_order_ticker))
    print(get_positions(client))