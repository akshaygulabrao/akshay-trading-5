import asyncio, aiosqlite
from kalshi_ref import KalshiHttpClient
from cryptography.hazmat.primitives import serialization
import sqlite3
import sys, logging

# Set logging level to DEBUG
logger = logging.getLogger("orderbook_trader")
logger.setLevel(logging.INFO)
_hdlr = logging.StreamHandler()
_hdlr.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
logger.addHandler(_hdlr)
logger.propagate = False

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
    def __init__(self,db_file):
        self.db_file = db_file
        self.name = "MomentumBot"
        self.tickers = set()
        self.tickers.add('KXHIGHAUS-25AUG17-B99.5')

        #with sqlite3.connect(self.db_file) as conn:
        #    conn.execute("DELETE FROM positions")
        #    conn.commit()

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
                logger.debug("Skipping non-orderbook message")
                return

            ticker = message["data"]["ticker"]
            #if ticker not in self.tickers:
            #    logger.debug("Ticker %s not in watch-list; skipping", ticker)
            #    return

            yes_str = message["data"]["yes"]
            no_str  = message["data"]["no"]
            if yes_str == "N/A" or no_str == "N/A":
                logger.debug("Ticker %s has incomplete book (yes=%s, no=%s); skipping", ticker, yes_str, no_str)
                return;

            p_yes_str, q_yes_str = yes_str.split("@")
            p_no_str,  q_no_str  = no_str.split("@")
            p_yes = int(p_yes_str)
            p_no = int(p_no_str)
            if p_yes > 97 or p_no > 97:
                logging.info("No profitable prices")

            async with aiosqlite.connect(self.db_file) as conn:
                async with conn.execute(
                        "SELECT price,quantity FROM positions WHERE ticker = ? AND strategy = ?",
                        (ticker,self.name)
                        ) as cursor:
                    row = await cursor.fetchone()
                    pos_price = row[0] if row else 0
                    pos_qty = row[1] if row else 0

                logger.info("Ticker %s found in positions: net quantity = %s", ticker, pos_qty)
                price = None
                if pos_qty == 1 and p_yes < p_no:
                    order_pos_qty = -2
                    price = p_no
                    logger.info("Ticker %s: long 1 → want to flip short 1 (order_qty=-2)", ticker)
                elif pos_qty == -1 and p_no < p_yes:
                    order_pos_qty = 2
                    price = p_yes
                    logger.info("Ticker %s: short 1 → want to flip long 1 (order_qty=2)", ticker)
                elif pos_qty == 0:
                    if p_yes < p_no:
                        order_pos_qty = -1
                        price = p_no
                        logger.info("Ticker %s: flat → want to short 1 (order_qty=-1)", ticker)
                    elif p_no < p_yes:
                        order_pos_qty = 1
                        price = p_yes
                        logger.info("Ticker %s: flat → want to long 1 (order_qty=1)", ticker)
                    else:
                        order_pos_qty = 0
                        logger.info("Ticker %s: flat, prices equal; no trade", ticker)
                else:
                    order_pos_qty = 0
                    logger.debug("Ticker %s: no rule matched (qty=%s); no trade", ticker, pos_qty)


                if order_pos_qty != 0 and price is not None:
                    await conn.execute("""
                            INSERT INTO positions (strategy, ticker, price, quantity, order_id)
                            VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT(strategy, ticker) DO UPDATE SET
                            price = excluded.price,
                            quantity = excluded.quantity,
                            order_id = excluded.order_id
                            """, (self.name, ticker, price, pos_qty + order_pos_qty, ""))
                    await conn.commit()
                    logger.info("Ticker %s: inserted position row (qty=%s, price=%s)", ticker, order_pos_qty,price)
                else:
                    logger.debug("Ticker %s: order_pos_qty=0, nothing inserted", ticker)

        except Exception as e:
            logger.exception("Error in on_message for ticker %s: %s", ticker if 'ticker' in locals() else "?", e)
            raise
