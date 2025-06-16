import sys
import asyncio
import datetime as dt
from collections import defaultdict
import subprocess
from dataclasses import dataclass
import json

from PySide6.QtCore import QObject, Qt, QUrl, Signal, Slot
from PySide6.QtGui import QKeySequence
from PySide6.QtNetwork import QAbstractSocket, QNetworkRequest
from PySide6.QtWebSockets import QWebSocket
from PySide6.QtWidgets import (
    QApplication,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMainWindow,
    QMenu,
    QMenuBar,
    QTabWidget,
    QTableWidget,
    QVBoxLayout,
    QWidget,
)
import PySide6.QtAsyncio as QtAsyncio

from user import User
import utils
from utils import exchange_status, format_timedelta


@dataclass
class Market:
    """Represents a market with yes and no tuples."""

    yes: list[list[int, int]]
    no: list[list[int, int]]
    yes_str: str
    no_str: str

    def find_top_yes(self):
        m = max(self.yes)
        self.yes_str = f"{m[0]} x {m[1]}"

    def find_top_no(self):
        m = max(self.no)
        self.no_str = f"{m[0]} x {m[1]}"


class TradingApp(QMainWindow):
    setBal = Signal(str)
    exchStatus = Signal(str)
    orderbook_update = Signal(str, int, int, int, int)

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

        self.orderbook = {}

    @Slot(str)
    def setBalance(self, bal):
        assert isinstance(self.balance, QLabel)
        self.balance.setText(bal)

    @Slot(str)
    def setExchStatus(self, status: str):
        assert isinstance(self.status_value, QLabel)
        self.status_value.setText(status)

    @Slot(str, int, int, int, int)
    def orderbook_update(
        self, ticker: str, bid_p: int, bid_s: int, ask_p: int, ask_s: int
    ):
        assert isinstance(self.orderbook[ticker], Market)
        self.orderbook[ticker].bid_price = bid_p


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


class WebSocketClient(QObject):
    message_received = Signal(str)
    connected = Signal()
    disconnected = Signal()
    error_occurred = Signal(str)

    def __init__(self):
        super().__init__()
        self.websocket = QWebSocket()
        self.message_id = 1

        # Connect signals
        self.websocket.connected.connect(self.on_connected)
        self.websocket.disconnected.connect(self.on_disconnected)
        self.websocket.textMessageReceived.connect(self.on_text_message_received)
        self.websocket.errorOccurred.connect(self.on_error)
        self.orderbook = {}

    def connect_to_server(self, url, headers):
        """Connect to the WebSocket server with custom headers"""
        request = QNetworkRequest(QUrl(url))

        # Set headers
        for key, value in headers.items():
            request.setRawHeader(key.encode("utf-8"), value.encode("utf-8"))

        self.websocket.open(request)

    def on_connected(self):
        print("✅ WebSocket connected")
        self.connected.emit()

        # Subscribe to desired channels after connection
        tickers = utils.get_markets()
        print(tickers)
        self.subscribe_to_portfolio(tickers)

    def on_disconnected(self):
        print("❌ WebSocket disconnected")
        self.disconnected.emit()

    def on_text_message_received(self, message):
        msg = json.loads(message)
        if msg["type"] == "orderbook_delta":
            mkt_ticker = msg["msg"]["market_ticker"]
            mkt_found = False
            for k, v in self.orderbook.items():
                if k == mkt_ticker:
                    mkt_found = True
            assert mkt_found
            side = msg["msg"]["side"]
            price = msg["msg"]["price"]
            delta = msg["msg"]["delta"]
            if side == "yes":
                mkt = self.orderbook[mkt_ticker]
                yes_ob = self.orderbook[mkt_ticker].yes
                for p, d in yes_ob:
                    if price == p:
                        d += delta
                mkt.find_top_yes()
            if side == "no":
                mkt = self.orderbook[mkt_ticker]
                no_ob = self.orderbook[mkt_ticker].no
                for p, d in no_ob:
                    if price == p:
                        d += delta
                mkt.find_top_no()

        elif msg["type"] == "orderbook_snapshot":
            mkt_ticker = msg["msg"]["market_ticker"]
            self.orderbook[mkt_ticker] = Market()
            if "yes" in msg["msg"]:
                self.orderbook[mkt_ticker].yes = msg["msg"]["yes"]
            else:
                self.orderbook[mkt_ticker].no = msg["msg"]["no"]

    def on_error(self, error):
        error_msg = f"WebSocket error: {error}"
        print(error_msg)
        self.error_occurred.emit(error_msg)

    def subscribe_to_portfolio(self, tickers):
        """Example: Subscribe to portfolio updates"""
        subscribe_message = {
            "id": self.message_id,
            "cmd": "subscribe",
            "params": {"channels": ["orderbook_delta"], "market_tickers": tickers},
        }
        self.send_message(json.dumps(subscribe_message))
        self.message_id += 1

    def send_message(self, message):
        if self.websocket.state() == QAbstractSocket.SocketState.ConnectedState:
            self.websocket.sendTextMessage(message)
        else:
            print("Cannot send message - WebSocket is not connected")

    def close_connection(self):
        self.websocket.close()


if __name__ == "__main__":
    mkts = utils.get_markets()
    user = User()
    app = QApplication(sys.argv)
    window = TradingApp(user)
    balance = Balance(user, window)
    exchange = Exchange(window)
    path = "/trade-api/ws/v2"
    base = "wss://api.elections.kalshi.com"

    client = utils.setup_client()
    headers = client.request_headers("GET", "/trade-api/ws/v2")
    websocket_url = base + path

    # Create and connect WebSocket client
    ws_client = WebSocketClient()
    ws_client.connect_to_server(websocket_url, headers)

    # Connect signals to handle events
    ws_client.error_occurred.connect(lambda err: print(f"Error: {err}"))
    tasks = [balance.stream(), exchange.stream()]

    async def run_streams(tasks):
        await asyncio.gather(*tasks)

    window.show()
    QtAsyncio.run(run_streams(tasks), handle_sigint=True)
