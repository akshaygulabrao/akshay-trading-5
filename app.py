import sys
import asyncio
import datetime as dt
from collections import defaultdict
import json

from PySide6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QTabWidget,
    QMenu,
    QMenuBar,
    QTableWidget,
    QHeaderView,
)
from PySide6.QtCore import Qt, QObject, Signal, Slot
from PySide6.QtGui import QKeySequence
import PySide6.QtAsyncio as QtAsyncio
import zmq
import websockets
from loguru import logger

from user import User
from kalshi_ref import KalshiWebSocketClient
import utils
from utils import exchange_status, format_timedelta


class OrderbookWebSocketClient(KalshiWebSocketClient):
    def __init__(self, pub):
        key_id, private_key, env = utils.setup_prod()
        super().__init__(key_id, private_key, env)
        self.order_books = {}
        self.pub = pub
        self.delta_count = 0

    async def connect(self, tickers):
        """Establishes a WebSocket connection and runs concurrent tasks."""
        host = self.WS_BASE_URL + self.url_suffix
        auth_headers = self.request_headers("GET", self.url_suffix)
        self.ws = websockets.connect(host, additional_headers=auth_headers)
        async with websockets.connect(
            host, additional_headers=auth_headers
        ) as websocket:
            self.ws = websocket
            await self.on_open(tickers)

            # Create tasks for handler and periodic publishing
            handler_task = asyncio.create_task(self.handler())
            periodic_task = asyncio.create_task(self.periodic_publish())

            # Run both tasks concurrently
            await asyncio.gather(handler_task, periodic_task)

    async def periodic_publish(self):
        """Periodically publishes orderbook data every second."""
        while True:
            await asyncio.sleep(10)
            for market_id in list(
                self.order_books.keys()
            ):  # Use a copy to avoid dict size change during iteration
                self.publish_orderbook(market_id)
            logger.info(
                f"periodic published finished, processed {self.delta_count} updates"
            )
            self.delta_count = 0

    async def on_message(self, message_str):
        try:
            message = json.loads(message_str)
            if message["type"] == "orderbook_delta":
                self.handle_orderbook_delta(message)
            elif message["type"] == "orderbook_snapshot":
                self.handle_orderbook_snapshot(message)
            elif message["type"] == "subscribed":
                logger.info("Websocket connected and subscribed to orderbook.")
            else:
                logger.info("Unknown message type:", message["type"])
        except Exception as e:
            logger.error(f"err: {e}")

    def handle_orderbook_delta(self, delta):
        market_id = delta["msg"]["market_ticker"]
        if market_id not in self.order_books:
            logger.info(f"Warning: Received delta for unknown market {market_id}")
            return
        side = delta["msg"]["side"]
        price = delta["msg"]["price"]
        size = delta["msg"]["delta"]
        book = self.order_books[market_id][side]
        book[price] += size
        if book[price] <= 0:
            del book[price]
        self.publish_orderbook(market_id)
        self.delta_count += 1

    def handle_orderbook_snapshot(self, snapshot):
        market_id = snapshot["msg"]["market_ticker"]
        if market_id not in self.order_books:
            self.order_books[market_id] = {
                "yes": defaultdict(int),
                "no": defaultdict(int),
            }
        if "no" in snapshot["msg"]:
            for price, volume in snapshot["msg"]["no"]:
                self.order_books[market_id]["no"][price] = volume
        if "yes" in snapshot["msg"]:
            for price, volume in snapshot["msg"]["yes"]:
                self.order_books[market_id]["yes"][price] = volume
        self.publish_orderbook(market_id)
        logger.info("Received Orderbook Snapshot")

    def publish_orderbook(self, market_id):
        """Publishes both Yes and converted No orderbooks"""
        yes_book = self.order_books[market_id]["yes"]
        no_book = self.order_books[market_id]["no"]

        self.pub.send_json(
            {
                "market_ticker": market_id,
                "yes": dict(yes_book),
                "no": dict(no_book),
            }
        )

    async def on_close(self):
        """Clean up resources"""
        self.periodic_task.cancel()
        try:
            await self.periodic_task
        except asyncio.CancelledError:
            pass
        await super().on_close()


class TradingApp(QMainWindow):
    setBal = Signal(str)
    exchStatus = Signal(str)

    def __init__(self, user: User):
        assert isinstance(user, User)
        super().__init__()
        self.user = user
        self.setWindowTitle("Trading Application")
        self.resize(1000, 600)

        # Main widget and layout
        main_widget = QWidget()
        main_layout = QVBoxLayout(main_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        # Top pane (30% height)
        top_pane = QWidget()
        top_pane.setFixedHeight(80)
        top_layout = QHBoxLayout(top_pane)

        # Account balance widget (left)
        balance_widget = QWidget()
        balance_layout = QVBoxLayout(balance_widget)
        balance_label = QLabel("Account Balance")
        balance_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.balance = QLabel("")

        self.balance.setAlignment(Qt.AlignmentFlag.AlignCenter)
        balance_layout.addWidget(balance_label)
        balance_layout.addWidget(self.balance)

        # Exchange status widget (right)
        status_widget = QWidget()
        status_layout = QVBoxLayout(status_widget)
        status_label = QLabel("Exchange Status")
        status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.status_value = QLabel("")
        self.status_value.setAlignment(Qt.AlignmentFlag.AlignCenter)
        status_layout.addWidget(status_label)
        status_layout.addWidget(self.status_value)

        # Add widgets to top pane
        top_layout.addWidget(balance_widget, 1)
        top_layout.addWidget(status_widget, 1)

        # Tab widget (70% height)
        tab_widget = QTabWidget()
        tab_titles = ["NY", "CHI", "AUS", "MIA", "DEN", "PHIL", "LAX"]
        for title in tab_titles:
            tab = QWidget()
            tab_layout = QHBoxLayout(tab)  # Main left-right split

            # Create left container (will hold top and bottom tables)
            left_container = QWidget()
            left_layout = QVBoxLayout(left_container)  # Top-bottom split

            # Create top table
            top_table = QTableWidget(6, 3)
            top_table.setHorizontalHeaderLabels(
                ["Ticker", "Bid (Price x Size)", "Ask (Price x Size)"]
            )
            top_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
            top_table.verticalHeader().setVisible(False)
            top_table.setEditTriggers(QTableWidget.NoEditTriggers)

            # Create bottom table
            bottom_table = QTableWidget(6, 3)
            bottom_table.setHorizontalHeaderLabels(
                ["Ticker", "Bid (Price x Size)", "Ask (Price x Size)"]
            )
            bottom_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
            bottom_table.verticalHeader().setVisible(False)
            bottom_table.setEditTriggers(QTableWidget.NoEditTriggers)

            # Add tables to left container with stretch factors
            left_layout.addWidget(top_table, 1)  # Takes 1 part of space
            left_layout.addWidget(bottom_table, 1)  # Takes 1 part of space

            # Create right container
            right_container = QWidget()
            right_layout = QVBoxLayout(right_container)
            right_layout.addWidget(QLabel(f"Additional content for {title}"))

            # Add left and right containers to main tab layout
            tab_layout.addWidget(left_container, 1)  # Takes 1 part of space
            tab_layout.addWidget(right_container, 1)  # Takes 1 part of space

            tab_widget.addTab(tab, title)

        # Add widgets to main layout
        main_layout.addWidget(top_pane)
        main_layout.addWidget(tab_widget)
        self.setCentralWidget(main_widget)

        # Create menu bar
        menubar = QMenuBar()
        file_menu = QMenu("File")

        # Add close action with Cmd+W shortcut
        close_action = file_menu.addAction("Close")
        close_action.setShortcut(QKeySequence("Ctrl+W"))
        close_action.triggered.connect(self.close)

        menubar.addMenu(file_menu)

        # Set native menu bar on macOS
        if sys.platform == "darwin":
            menubar.setNativeMenuBar(True)

        self.setMenuBar(menubar)
        self.setBal.connect(self.setBalance)
        self.exchStatus.connect(self.setExchStatus)

    @Slot(str)
    def setBalance(self, bal):
        assert isinstance(self.balance, QLabel)
        self.balance.setText(bal)

    @Slot(str)
    def setExchStatus(self, status: str):
        assert isinstance(self.status_value, QLabel)
        self.status_value.setText(status)


class Balance(QObject):
    def __init__(self, user: User, window: QMainWindow):
        assert isinstance(user, User)
        assert isinstance(window, QMainWindow)
        self.user = user
        self.window = window

    async def stream(self):
        while True:
            float_val = user.getBalance()["balance"] / 100
            str_val = f"$ {float_val:.02f}"
            self.window.setBal.emit(str_val)
            await asyncio.sleep(0.2)


class Exchange(QObject):
    def __init__(self, window: QMainWindow):
        assert isinstance(window, QMainWindow)
        self.window = window

    async def stream(self):
        while True:
            time_left, is_open = exchange_status()
            assert isinstance(time_left, dt.timedelta)
            assert isinstance(is_open, bool)
            fmt_str = format_timedelta(time_left)
            self.window.exchStatus.emit(fmt_str)
            await asyncio.sleep(1)



if __name__ == "__main__":

    context = zmq.Context()
    pub = context.socket(zmq.PUB)
    pub.bind("ipc:///tmp/orderbook.ipc")
    mkts = utils.get_markets()
    client = OrderbookWebSocketClient(pub)
    user = User()
    app = QApplication(sys.argv)
    window = TradingApp(user)
    balance = Balance(user, window)
    exchange = Exchange(window)

    tasks = [balance.stream(), exchange.stream()]

    async def run_streams(tasks):
        await asyncio.gather(*tasks)

    window.show()
    QtAsyncio.run(run_streams(tasks), handle_sigint=True)