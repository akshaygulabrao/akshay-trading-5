import asyncio

from PyQt6.QtWidgets import QApplication, QMainWindow, QLabel, QVBoxLayout, QWidget
from loguru import logger

from utils import setup_client


class User:
    def __init__(self):
        self.client = setup_client()
        self.label = None

    def getBalance(self):
        return self.client.get_balance()

    async def getBalanceAsync(self):
        assert self.label is not None
        while True:
            self.balance = self.client.get_balance()
            self.label.setText(str(self.balance))
            await asyncio.sleep(0.5)


if __name__ == "__main__":
    u = User()
    try:
        asyncio.run(u.run())
    except KeyboardInterrupt:
        logger.error(f"Received Keyboard Interrupt")
