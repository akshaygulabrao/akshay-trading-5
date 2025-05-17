from utils import now, setup_prod,urls
from original.clients import KalshiHttpClient
import datetime

KEYID, private_key, env = setup_prod()
client = KalshiHttpClient(KEYID, private_key, env)

n = int(datetime.datetime.now().timestamp())
active_time = int(datetime.timedelta(days=2).total_seconds())
print(n,active_time)
params = {'status': 'executed','min_ts': n - active_time}
orders = client.get(urls['orders'],params=params)
print(orders)
