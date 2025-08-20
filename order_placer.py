import os,uuid,sqlite3,requests
from kalshi_ref import KalshiHttpClient
from cryptography.hazmat.primitives import serialization

"""
placement/cancellation order approximation: 16.3 seconds for 100 orders
~ 161ms per order with rate_limit = 100ms
"""


def place_order(
    client, ticker, action, price, quantity, limit_order=True, expiration_ts=None, post_only=False
):
    """
    client,ticker,action,price,quantity,order_type
    """
    assert isinstance(client, KalshiHttpClient)
    assert action == "buy" or action == "sell"
    private_order_id = str(uuid.uuid4())
    params = {
        "ticker": ticker,
        "action": action,
        "side": "yes",
        "client_order_id": private_order_id,
        "count": quantity,
        "yes_price": price,
    }
    params["type"] = "limit" if limit_order else "market"

    response = client.post(urls["orders"], body=params)
    public_order_id = response["order"]["order_id"]
    return public_order_id


def cancel_order(client, public_order_id):
    assert isinstance(client, KalshiHttpClient)
    response = client.delete(f"/trade-api/v2/portfolio/orders/{public_order_id}")
    order_status = response["order"]["status"]
    return order_status == "canceled"


def get_resting_orders(client, ticker):
    assert isinstance(client, KalshiHttpClient)
    params = {"ticker": ticker, "status": "resting", "limit": 1000}
    response = client.get(f"{urls['orders']}", params)
    orders = response["orders"]
    while response["cursor"] != "":
        params["cursor"] = response["cursor"]
        response = client.get(f"{urls['orders']}", params)
        orders.extend(response["orders"])
    resting_orders = set()
    for order in orders:
        resting_orders.add(
            (order["order_id"], order["yes_price"], order["remaining_count"])
        )
    return resting_orders


def get_positions(client):
    assert isinstance(client, KalshiHttpClient)
    params = {"count_filter": "position", "limit": 1000}
    response = client.get(f"{urls['positions']}", params)
    assert isinstance(response, dict)
    while response["cursor"] != "":
        params["cusor"] = response["cursor"]
        response = client.get(f"{urls['positions']}", params)
    positions_list = response["market_positions"]

    positions = {}
    for position in positions_list:
        positions[position["ticker"]] = (
            position["position"],
            position["market_exposure"] / position["position"],
            position["realized_pnl"],
        )
    return positions


if __name__ == "__main__":

    with open(os.getenv("PROD_KEYFILE"), "rb") as f:
        private_key = serialization.load_pem_private_key(f.read(), password=None)

    client = KalshiHttpClient(os.getenv("PROD_KEYID"),private_key)


    print(client.get_balance())

    response = client.get('/trade-api/v2/portfolio/positions')
    print(response['market_positions'][0].keys())
    for i in response['market_positions']:
        if 'KXHIGH' in i['ticker'] and i['position'] != 0:
            #print(f'{i['ticker'].ljust(40)}, {i['market_exposure'] + i['fees_paid']/ abs(i['position'])},  {i['position']}')
            print(i)
            conn = sqlite3.connect(os.getenv("ORDERS_DB_PATH"))
            conn.execute("INSERT INTO positions VALUES (?,?,?,?,'') ON CONFLICT DO NOTHING",
                        ("MomentumBot", i['ticker'],
                        i['market_exposure'] + i['fees_paid']/ abs(i['position']),
                        i['position']))
            conn.commit()  # Add commit to save changes
            conn.close()


    # public_order_id = client.post('/trade-api/v2/portfolio/orders', {'ticker': 'KXHIGHAUS-25AUG17-B97.5',
    #                                                                  'action' : 'sell',
    #                                                                  'side': 'yes',
    #                                                                  'count': 2,
    #                                                                  'type': 'market',
    #                                                                  'client_order_id': str(uuid.uuid4())})
    # print(public_order_id)

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
