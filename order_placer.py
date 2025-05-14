from original.clients import KalshiHttpClient
import pandas as pd
import datetime as dt
import uuid
import time
import utils


KEYID, private_key, env = utils.setup_prod()

client = KalshiHttpClient(KEYID,private_key,env)

# ct = utils.get_ct()
# print(ct)


# params = {
#     'min_ts': int(utils.get_ct().timestamp()) - int(dt.timedelta(hours=1).total_seconds())
# }
# response = client.get('/trade-api/v2/portfolio/positions',params)

# response = client.get('/trade-api/v2/portfolio/orders',params)

# print(pd.DataFrame.from_dict(response['orders']))
# https://kalshi.com/markets/kxatpit/atp-italian-open#kxatpit-25paude
alex_order_bid = str(uuid.uuid1())
alex_order_ask = uuid.uuid1()

print(alex_order_bid)
params = {
    'ticker': 'KXATPIT-25PAUDE-PAU',
    'action': 'buy',
    'side': 'yes',
    'type': 'limit',
    'count' : 1, 
    'yes_price' : 10,
    'buy_max_cost': 12,
    'expiration_ts': int(utils.get_ct().timestamp()) + 10,
    'client_order_id' : alex_order_bid
}
response = client.post('/trade-api/v2/portfolio/orders',params)

public_order_id = response['order']['order_id']
time.sleep(1)
params = {
    'order_id': alex_order_bid,
}
response = client.delete(f'/trade-api/v2/portfolio/orders/{public_order_id}')

