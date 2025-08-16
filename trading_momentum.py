from kalshi_ref import KalshiHttpClient
import sqlite3


class TradingMomentum():
    def __init__(self):
        self.risk = 0
        self.db_file = "orders.db"
        