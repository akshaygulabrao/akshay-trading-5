import pandas as pd
import numpy as np

df = pd.read_json("/home/ox/Downloads/CUR_BTC_prices_download (1).json")

df['log_return'] = np.log(df['close'] / df['close'].shift(1))
print(df['log_return'].std() * np.sqrt(390))