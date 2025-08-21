import asyncio, aiosqlite
from kalshi_ref import KalshiHttpClient
from cryptography.hazmat.primitives import serialization
import sqlite3
import sys, logging,os,uuid,time

logger = logging.getLogger("orderbook_trader")
logger.setLevel(logging.INFO)

def decide_trade(pos_qty: int, p_yes: int, p_no: int) -> tuple[int, int | None]:
    """
    Returns (order_qty, price) based on current position and market prices.
    Returns (0, None) if no trade should be made.
    """
    if pos_qty == 1 and p_yes < p_no:
        return -2, p_no
    elif pos_qty == -1 and p_no < p_yes:
        return 2, p_yes
    elif pos_qty == 0:
        if p_yes < p_no:
            return -1, p_no
        elif p_no < p_yes:
            return 1, p_yes
    return 0, None

class OrderbookTrader:
    def __init__(self, queue: asyncio.Queue, db_file, tickers=[]):
        self.queue = queue
        self.db_file = db_file
        self.name = "MomentumBot"
        self.tickers = set(tickers)
        self._positions: dict[str, dict] = {}
        self.times = []
        try:
            with open(os.getenv("PROD_KEYFILE"), "rb") as f:
                private_key = serialization.load_pem_private_key(f.read(), password=None)
            self.client = KalshiHttpClient(os.getenv("PROD_KEYID"), private_key)
        except Exception as e:
            logger.error(e)

    async def update_balance(self):
        while True:
            self.balance = self.client.get_balance()['balance']
            await asyncio.sleep(1)

    async def _sync_ticker_position(self, ticker: str, emit_update: bool = False):
        """
        Pull position from Kalshi and store it in _positions.
        """
        positions = self.client.get(
            '/trade-api/v2/portfolio/positions', {'ticker': ticker})
        if not positions['market_positions']:
            qty = 0
            price = 0
        else:
            pos = positions['market_positions'][0]
            qty = pos['position']
            price = pos['market_exposure'] + pos['fees_paid']

        self._positions[ticker] = {'price': price, 'quantity': qty, 'order_id': ''}

        if emit_update:
            await self.queue.put(
                {"type": "positionUpdate", "ticker": ticker, "pos": qty})
    # ---------- init ----------
    async def initialize_positions(self):
        for ticker in self.tickers:
            await self._sync_ticker_position(ticker, emit_update=True)
            logger.info("Initialized position for %s", ticker)

    # ---------- periodic update ----------
    async def update_positions(self):
        while True:
            for ticker in self.tickers:
                await self._sync_ticker_position(ticker, emit_update=True)
            await asyncio.sleep(5)

    async def update_balance(self):
        while True:
            self.balance = self.client.get_balance()['balance']
            await asyncio.sleep(1)

    # AN ORDERBOOK MESSAGE LOOKS LIKE THIS
    # mkt = {
    #     "ticker": market_ticker,
    #     "no": (
    #         f"{100 - yes_top}@{ob.markets[market_ticker]['yes'][yes_top]}"
    #         if yes_top
    #         else "N/A"
    #     ),
    #     "yes": (
    #         f"{100 - no_top}@{ob.markets[market_ticker]['no'][no_top]}"
    #         if no_top
    #         else "N/A"
    #     ),
    # }
    # await self.queue.put({"type": "orderbook", "data": mkt})
    def maybe_output_stats(self):
        if len(self.times) > 10:
            logging.info("last 10 packets took %f on avg to parse", sum(self.times) / len(self.times))
            self.times.clear()

    async def on_message(self, message):
        try:
            start = time.perf_counter_ns()
            logger.debug("Raw message: %s", message)
            if message.get("type") != "orderbook":
                end = time.perf_counter_ns()
                self.times.append(end - start)
                self.maybe_output_stats()
                return

            msg = message['data']
            ticker = msg['ticker']
            if ticker not in self.tickers:
                end = time.perf_counter_ns()
                self.times.append(end-start)
                self.maybe_output_stats()
                return
            if msg['yes'] == "N/A" or msg['no'] == "N/A":
                logger.debug("%s incomplete", ticker)
                end = time.perf_counter_ns()
                self.times.append(end - start)
                self.maybe_output_stats()
                return

            p_yes_str, _ = msg['yes'].split("@")
            p_no_str, _ = msg['no'].split("@")
            p_yes = int(p_yes_str)
            p_no = int(p_no_str)

            if p_yes > 97 or p_no > 97:
                logger.debug("%s not profitable", ticker)
                end = time.perf_counter_ns()
                self.times.append(end - start)
                self.maybe_output_stats()
                return
            if abs(p_no - p_yes) < 66:
                logger.debug("%s spread too tight (%d vs %d), skipping",
                             ticker, p_yes, p_no)
                end = time.perf_counter_ns()
                self.times.append(end - start)
                self.maybe_output_stats()
                return

            # read current position
            pos = self._positions.get(ticker, {'quantity': 0, 'price': 0})
            pos_qty = pos['quantity']
            order_pos_qty, price = decide_trade(pos_qty, p_yes, p_no)

            if order_pos_qty != 0 and price is not None and self.balance > abs(order_pos_qty) * 100:
                uid = str(uuid.uuid4())
                side = 'yes'
                action = 'buy'
                order_position = order_pos_qty
                if order_pos_qty < 0:
                    side = 'no'
                    order_position = abs(order_pos_qty)

                order = {
                    'ticker': ticker,
                    'action': action,
                    'side': side,
                    'type': 'market',
                    'count': order_position,
                    'client_order_id': uid
                }
                logger.info(order)
                #order_id = self.client.post('/trade-api/v2/portfolio/orders', order)

                # update in-memory position
                new_qty = pos_qty + order_pos_qty
                self._positions[ticker] = {
                    'price': price,
                    'quantity': new_qty,
                    'order_id': ''
                }

                await self.queue.put(
                    {"type": "positionUpdate", "ticker": ticker, "pos": new_qty})
            else:
                logger.debug("Ticker %s: no trade", ticker)

            end = time.perf_counter_ns()
            self.times.append(end - start)
            self.maybe_output_stats()
        except Exception as e:
            logger.exception("Error in on_message for ticker %s: %s",
                             ticker if 'ticker' in locals() else "?", e)
            raise
