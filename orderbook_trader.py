import asyncio, aiosqlite
from kalshi_ref import KalshiHttpClient
from cryptography.hazmat.primitives import serialization
import sqlite3
import sys, logging,os,uuid

logger = logging.getLogger("orderbook_trader")
logger.setLevel(logging.INFO)

# -- Positions
# CREATE TABLE positions (
#    strategy TEXT NOT NULL,
#    ticker   TEXT NOT NULL,
#    price    INTEGER NOT NULL CHECK,
#    quantity INTEGER NOT NULL CHECK,
#    order_id UUID,
#    UNIQUE (strategy, ticker)
#);

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
        try:
            with open(os.getenv("PROD_KEYFILE"), "rb") as f:
                private_key = serialization.load_pem_private_key(f.read(), password=None)
            self.client = KalshiHttpClient(os.getenv("PROD_KEYID"), private_key)
        except Exception as e:
            logger.error(e)

        with sqlite3.connect(self.db_file) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS positions (
                    strategy TEXT NOT NULL,
                    ticker   TEXT NOT NULL,
                    price    INTEGER NOT NULL,
                    quantity INTEGER NOT NULL,
                    order_id UUID,
                    UNIQUE (strategy, ticker)
                );
            """)
            conn.execute("DELETE FROM positions")
            conn.commit()

    async def update_balance(self):
        while True:
            self.balance = self.client.get_balance()['balance']
            await asyncio.sleep(1)
    # ---------- shared helper ----------
    async def _sync_ticker_position(self, conn: aiosqlite.Connection, ticker: str,
                                    emit_update: bool = False):
        """
        Fetch the position for `ticker` from Kalshi and upsert it into the DB.
        If `emit_update` is True, also push a positionUpdate message.
        """
        positions = self.client.get('/trade-api/v2/portfolio/positions',
                                    {'ticker': ticker})
        if not positions['market_positions']:
            # No position: make sure we store qty = 0
            qty = 0
            price = 0
            pos = 0
        else:
            pos   = positions['market_positions'][0]
            qty   = pos['position']
            price = pos['market_exposure'] + pos['fees_paid']
            await conn.execute("""
                INSERT INTO positions (strategy, ticker, price, quantity, order_id)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (strategy, ticker) DO UPDATE SET
                    price    = excluded.price,
                    quantity = excluded.quantity,
                    order_id = excluded.order_id
            """, (self.name, ticker, price, qty, ''))
            await conn.commit()

        if emit_update:
            msg = {"type": "positionUpdate", "ticker": ticker, "pos": qty}
            await self.queue.put(msg)

    # ---------- one-shot init ----------
    async def initialize_positions(self):
        async with aiosqlite.connect(self.db_file) as conn:
            for ticker in self.tickers:
                await self._sync_ticker_position(conn, ticker, emit_update=True)
                logger.info("Initialized position for %s", ticker)

    # ---------- periodic update ----------
    async def update_positions(self):
        while True:
            async with aiosqlite.connect(self.db_file) as conn:
                for ticker in self.tickers:
                    await self._sync_ticker_position(conn, ticker, emit_update=True)
            await asyncio.sleep(5)

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
    async def on_message(self, message):
        try:
            logger.debug("Raw message: %s", message)

            if message.get("type") != "orderbook":
                return
            msg = message['data']
            ticker = msg['ticker']
            if ticker not in self.tickers:
                return
            if msg['yes'] == "N/A" or msg['no'] == "N/A":
                logger.info("%s incomplete", ticker)
                return

            p_yes_str, _ = msg['yes'].split("@")
            p_no_str, _ = msg['no'].split("@")
            p_yes = int(p_yes_str)
            p_no = int(p_no_str)

            if p_yes > 97 or p_no > 97:
                logger.debug("%s not profitable", ticker)
                return
            if abs(p_no - p_yes) < 66:
                logger.debug("%s spread too tight (%d vs %d), skipping", ticker, p_yes, p_no)
                return
            async with aiosqlite.connect(self.db_file) as conn:
                async with conn.execute(
                    "SELECT price, quantity FROM positions WHERE ticker = ? AND strategy = ?",
                    (ticker, self.name)
                ) as cursor:
                    row = await cursor.fetchone()
                    pos_price = row[0] if row else 0
                    pos_qty = row[1] if row else 0

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
                    order_id = self.client.post('/trade-api/v2/portfolio/orders', order)

                    await conn.execute("""
                        INSERT INTO positions (strategy, ticker, price, quantity, order_id)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(strategy, ticker) DO UPDATE SET
                            price = excluded.price,
                            quantity = excluded.quantity,
                            order_id = excluded.order_id
                    """, (self.name, ticker, price, pos_qty + order_pos_qty, ''))
                    await conn.commit()

                    msg = {"type": "positionUpdate", "ticker": ticker, "pos": pos_qty + order_pos_qty}
                    await self.queue.put(msg)
                else:
                    logger.debug("Ticker %s: no trade", ticker)

        except Exception as e:
            logger.exception("Error in on_message for ticker %s: %s", ticker if 'ticker' in locals() else "?", e)
            raise
