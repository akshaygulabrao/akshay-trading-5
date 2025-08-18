import asyncio, aiosqlite
from kalshi_ref import KalshiHttpClient
from cryptography.hazmat.primitives import serialization
import sqlite3
import sys, logging

# Set logging level to DEBUG
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

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
        self.tickers = set()
        self.tickers.add('KXHIGHAUS-25AUG17-B99.5')
        with sqlite3.connect(self.db_file) as conn:
            conn.execute("DELETE FROM positions")
            conn.commit()

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
            logging.debug("Raw message: %s", message)

            if message.get("type") != "orderbook":
                logging.debug("Skipping non-orderbook message")
                return

            ticker = message["data"]["ticker"]
            if ticker not in self.tickers:
                logging.debug("Ticker %s not in watch-list; skipping", ticker)
                return

            yes_str = message["data"]["yes"]
            no_str  = message["data"]["no"]
            if yes_str == "N/A" or no_str == "N/A":
                logging.debug("Ticker %s has incomplete book (yes=%s, no=%s); skipping",
                              ticker, yes_str, no_str)
                return
            async with aiosqlite.connect(self.db_file) as conn:
                async with conn.execute(
                        "SELECT COALESCE(SUM(quantity), 0), COUNT(*) FROM positions WHERE ticker = ?",
                        (ticker,)
                        ) as cursor:
                    row = await cursor.fetchone()
                    quantity = row[0] if row else 0
                    count = row[1] if row else 0

                logging.info("Ticker %s: found %d position rows, net quantity = %s", ticker, count, quantity)
                p_yes_str, q_yes_str = yes_str.split("@")
                p_no_str,  q_no_str  = no_str.split("@")
                p_yes = int(p_yes_str)
                p_no  = int(p_no_str)
                logging.info("Ticker %s book: yes=%s@%s, no=%s@%s",
                             ticker, p_yes, q_yes_str, p_no, q_no_str)

                if quantity == 1 and p_yes < p_no:
                    order_quantity = -2
                    logging.info("Ticker %s: long 1 → want to flip short 1 (order_qty=-2)", ticker)
                elif quantity == -1 and p_no < p_yes:
                    order_quantity = 2
                    logging.info("Ticker %s: short 1 → want to flip long 1 (order_qty=2)", ticker)
                elif quantity == 0:
                    if p_yes < p_no:
                        order_quantity = -1
                        logging.info("Ticker %s: flat → want to short 1 (order_qty=-1)", ticker)
                    elif p_no < p_yes:
                        order_quantity = 1
                        logging.info("Ticker %s: flat → want to long 1 (order_qty=1)", ticker)
                    else:
                        order_quantity = 0
                        logging.info("Ticker %s: flat, prices equal; no trade", ticker)
                else:
                    order_quantity = 0
                    logging.info("Ticker %s: no rule matched (qty=%s); no trade", ticker, quantity)


                if order_quantity != 0:
                    # Check if we already hold the desired side
                    desired_side = 1 if order_quantity > 0 else -1
                    if quantity == desired_side:
                        logging.info("Ticker %s: already on correct side (qty=%s); no action", ticker, quantity)
                    else:
                        await conn.execute(
                                "INSERT INTO positions VALUES (?,?,?,?,?)",
                                ("MomentumBot", ticker, p_no, order_quantity, "")
                                )
                        await conn.commit()
                        logging.info("Ticker %s: inserted position row (qty=%s, price=%s)",
                                     ticker, order_quantity, p_no)
                else:
                    logging.debug("Ticker %s: order_quantity=0, nothing inserted", ticker)

        except Exception as e:
            logging.exception("Error in on_message for ticker %s: %s", ticker if 'ticker' in locals() else "?", e)
            raise
