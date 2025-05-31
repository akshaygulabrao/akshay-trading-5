import asyncio

from PySide6.QtWidgets import QApplication, QMainWindow, QLabel, QVBoxLayout, QWidget
from loguru import logger

from utils import setup_client


class User:
    def __init__(self):
        self.client = setup_client()
        self.label = None

    def getBalance(self) -> dict:
        result = self.client.get_balance()
        assert isinstance(result, dict)
        assert "balance" in result
        return result


if __name__ == "__main__":
    u = User()
    try:
        asyncio.run(u.run())
    except KeyboardInterrupt:
        logger.error(f"Received Keyboard Interrupt")
