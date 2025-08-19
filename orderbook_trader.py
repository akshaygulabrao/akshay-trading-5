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
#    price    INTEGER NOT NULL CHECK (price > 0),
#    quantity INTEGER NOT NULL CHECK (quantity <> 0),
#    order_id UUID,
#    UNIQUE (strategy, ticker)
#);

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

        # Clear positions table
        with sqlite3.connect(self.db_file) as conn:
            conn.execute("DELETE FROM positions")
            conn.commit()
    async def update_balance(self):
        while True:
            self.balance = self.client.get_balance()['balance']
            await asyncio.sleep(1)

    async def update_positions(self):
        while True:
            async with aiosqlite.connect(self.db_file) as conn:
                for ticker in self.tickers:
                    positions = self.client.get('/trade-api/v2/portfolio/positions', {'ticker': ticker})
                    #logger.info(positions['market_positions'])
                    if len(positions['market_positions']) == 0:
                        continue
                    pos = positions['market_positions'][0]
                    await conn.execute("""
                        INSERT INTO positions (strategy, ticker, price, quantity, order_id)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT (strategy, ticker)
                        DO UPDATE SET
                            price = excluded.price,
                            quantity = excluded.quantity,
                            order_id = excluded.order_id
                    """, (
                        self.name,
                        ticker,
                        pos['market_exposure'] + pos['fees_paid'],
                        pos['position'],
                        ''
                    ))
                    await conn.commit()
                    msg = {"type": "positionUpdate", "ticker": ticker, "pos": pos['position']}
                    await self.queue.put(msg)
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

            if message.get("type") != "orderbook": return
            if (ticker := (msg := message['data'])['ticker'])  not in self.tickers: return;
            if (yes_str := msg['yes'])  == "N/A" or (no_str := msg['no']) == "N/A":
                logger.info("%s incomplete", ticker); return;
            #logging.info(msg)

            p_yes_str, q_yes_str = yes_str.split("@")
            p_no_str,  q_no_str  = no_str.split("@")
            if (p_yes := int(p_yes_str)) > 97 or (p_no := int(p_no_str)) > 97:
                logger.info("%s not profitable",ticker)

            async with aiosqlite.connect(self.db_file) as conn:
                async with conn.execute(
                        "SELECT price,quantity FROM positions WHERE ticker = ? AND strategy = ?",
                        (ticker,self.name)
                        ) as cursor:
                    row = await cursor.fetchone()
                    pos_price = row[0] if row else 0
                    pos_qty = row[1] if row else 0

                logger.debug("%s found in positions: net quantity = %s", ticker, pos_qty)
                price = None
                if pos_qty == 1 and p_yes < p_no:
                    order_pos_qty = -2
                    price = p_no
                    logger.debug("%s: long 1 → want to flip short 1 (order_qty=-2)", ticker)
                elif pos_qty == -1 and p_no < p_yes:
                    order_pos_qty = 2
                    price = p_yes
                    logger.debug("%s: short 1 → want to flip long 1 (order_qty=2)", ticker)
                elif pos_qty == 0:
                    if p_yes < p_no:
                        order_pos_qty = -1
                        price = p_no
                        logger.debug("%s: flat → want to short 1 (order_qty=-1)", ticker)
                    elif p_no < p_yes:
                        order_pos_qty = 1
                        price = p_yes
                        logger.debug("%s: flat → want to long 1 (order_qty=1)", ticker)
                    else:
                        order_pos_qty = 0
                        logger.debug("%s: flat, prices equal; no trade", ticker)
                else:
                    order_pos_qty = 0
                    logger.debug("%s: no rule matched (qty=%s); no trade", ticker, pos_qty)


                if order_pos_qty != 0 and price is not None and self.balance > order_pos_qty * 100:
                    uid = str(uuid.uuid4())
                    side = 'yes'
                    action = 'buy'
                    order_position = order_pos_qty
                    if order_pos_qty < 0:
                        action = 'buy'
                        side = 'no'
                        order_position = abs(order_pos_qty)
                    order = {'ticker':ticker,
                        'action':action,
                        'side': side,
                        'type' : 'market',
                        'count': order_position,
                        'client_order_id':uid}
                    logger.info(order)
                    #order_id = self.client.post('/trade-api/v2/portfolio/orders', order)
                    await conn.execute("""
                            INSERT INTO positions (strategy, ticker, price, quantity, order_id)
                            VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT(strategy, ticker) DO UPDATE SET
                            price = excluded.price,
                            quantity = excluded.quantity,
                            order_id = excluded.order_id
                            """, (self.name, ticker, price, pos_qty + order_pos_qty, ''))
                    await conn.commit()
                    msg = {"type":"positionUpdate", "ticker" : ticker, "pos" : pos_qty + order_pos_qty}
                    await self.queue.put(msg)
                else:
                    logger.debug("Ticker %s: order_pos_qty=0, nothing inserted", ticker)

        except Exception as e:
            logger.exception("Error in on_message for ticker %s: %s", ticker if 'ticker' in locals() else "?", e)
            raise
