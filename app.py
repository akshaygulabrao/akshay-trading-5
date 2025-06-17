import sys
import asyncio
import datetime as dt
from loguru import logger
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
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)
import PySide6.QtAsyncio as QtAsyncio

from user import User
import utils
from utils import exchange_status, format_timedelta
import jsonargparse


class TradingApp(QMainWindow):
    setBal = Signal(str)
    exchStatus = Signal(str)
    orderbookUpdate = Signal(str, str, str)

    def __init__(self, sites: list[str]):
        super().__init__()
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

        for title in sites:
            event_markets = utils.get_markets_for_sites([title])
            tab = QWidget()
            tab_layout = QHBoxLayout(tab)  # Main left-right split

            # Create left container (will hold top and bottom tables)
            left_container = QWidget()
            left_layout = QVBoxLayout(left_container)  # Top-bottom split

            self.market2table = {}
            self.market2row = {}
            for event, markets in event_markets.items():
                table = QTableWidget(len(markets), 3)
                table.setHorizontalHeaderLabels(
                    ["Ticker", "Yes (Price x Size)", "No (Price x Size)"]
                )
                table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
                table.verticalHeader().setVisible(False)
                table.setEditTriggers(QTableWidget.NoEditTriggers)
                for row, market in enumerate(markets):
                    ticker_item = QTableWidgetItem(market)
                    self.market2table[market] = table
                    self.market2row[market] = row
                left_layout.addWidget(table, 1)  # Takes 1 part of space

            for event, markets in event_markets.items():
                for row, market in enumerate(markets):
                    ticker_item = QTableWidgetItem(market)
                    self.market2table[market].setItem(
                        self.market2row[market], 0, ticker_item
                    )

            tab_layout.addWidget(left_container, 1)  # Takes 1 part of space

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
        self.orderbookUpdate.connect(self.changeOrderbookState)

    @Slot(str)
    def setBalance(self, bal):
        assert isinstance(self.balance, QLabel)
        self.balance.setText(bal)

    @Slot(str)
    def setExchStatus(self, status: str):
        assert isinstance(self.status_value, QLabel)
        self.status_value.setText(status)

    @Slot(str, str, str)
    def changeOrderbookState(self, ticker: str, yes_str: str, no_str: str):
        table = self.market2table[ticker]
        row = self.market2row[ticker]
        table.setItem(row, 1, QTableWidgetItem(yes_str))
        table.setItem(row, 2, QTableWidgetItem(no_str))


class InfoStream(QObject):
    def __init__(self, user: User, window: QMainWindow):
        assert isinstance(user, User)
        assert isinstance(window, QMainWindow)
        self.user = user
        self.window = window

    async def stream_balance(self):
        while True:
            float_val = self.user.getBalance()["balance"] / 100
            str_val = f"$ {float_val:.02f}"
            self.window.setBal.emit(str_val)
            await asyncio.sleep(0.2)

    async def stream_exchange_status(self):
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

    def __init__(self, mkts, orderbook_update_signal):
        super().__init__()
        self.websocket = QWebSocket()
        self.message_id = 1

        # Connect signals
        self.websocket.connected.connect(self.on_connected)
        self.websocket.disconnected.connect(self.on_disconnected)
        self.websocket.textMessageReceived.connect(self.on_text_message_received)
        self.websocket.errorOccurred.connect(self.on_error)
        self.orderbook = {}
        self.orderbook_updated = orderbook_update_signal
        self.tickers = mkts

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
        self.subscribe_to_portfolio(self.tickers)

    def on_disconnected(self):
        print("❌ WebSocket disconnected")
        self.disconnected.emit()

    def on_text_message_received(self, message):
        msg = json.loads(message)
        # Skip subscribed messages
        if msg.get("type") == "subscribed":
            return

        msg_data = msg.get("msg", {})
        market_ticker = msg_data.get("market_ticker")

        if not market_ticker:
            return

        # Initialize market if not present
        if market_ticker not in self.orderbook:
            self.orderbook[market_ticker] = {"yes": {}, "no": {}}

        order_book = self.orderbook[market_ticker]

        if msg["type"] == "orderbook_snapshot":
            # Process snapshot
            if "yes" in msg_data:
                order_book["yes"] = {}
                for price, qty in msg_data["yes"]:
                    order_book["yes"][price] = qty
            if "no" in msg_data:
                order_book["no"] = {}
                for price, qty in msg_data["no"]:
                    order_book["no"][price] = qty

        elif msg["type"] == "orderbook_delta":
            # Process delta
            side = msg_data.get("side")
            price = msg_data.get("price")
            delta = msg_data.get("delta")

            if side not in ["yes", "no"] or not price:
                return

            current_qty = order_book[side].get(price, 0)
            new_qty = current_qty + delta

            if new_qty <= 0:
                if price in order_book[side]:
                    del order_book[side][price]
            else:
                order_book[side][price] = new_qty

        # Remember that order_book lists sell prices
        top_yes = max(order_book["yes"].keys()) if order_book["yes"] else None
        top_no = max(order_book["no"].keys()) if order_book["no"] else None

        # Format strings for display
        no_str = (
            f"{100 - top_yes} x {order_book['yes'].get(top_yes, 0)}" if top_yes else ""
        )
        yes_str = (
            f"{100 - top_no} x {order_book['no'].get(top_no, 0)}" if top_no else ""
        )

        # Emit update signal
        self.orderbook_updated.emit(market_ticker, yes_str, no_str)

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


def main(all_sites: bool = False):
    """Renders a GUI that displays the orderbook for
    each site/event/market

    Args:
        all_sites: whether to include orderbook information for all sites. Only displays
        orderbook data for NY by default.

    """
    app = QApplication(sys.argv)
    sites = ["NY", "CHI", "AUS", "MIA", "DEN", "PHIL", "LAX"] if all_sites else ["NY"]
    window = TradingApp(sites)

    path = "/trade-api/ws/v2"
    base = "wss://api.elections.kalshi.com"
    client = utils.setup_client()
    headers = client.request_headers("GET", "/trade-api/ws/v2")
    websocket_url = base + path
    event2markets = utils.get_markets_for_sites(sites)
    logger.debug(event2markets)
    mkts = []
    for k, v in event2markets.items():
        mkts.extend(v)
    ws_client = WebSocketClient(mkts, window.orderbookUpdate)
    ws_client.connect_to_server(websocket_url, headers)
    ws_client.error_occurred.connect(lambda err: print(f"Error: {err}"))

    user = User()
    info_stream = InfoStream(user, window)
    tasks = [info_stream.stream_balance(), info_stream.stream_exchange_status()]

    async def run_streams(tasks):
        await asyncio.gather(*tasks)

    window.show()
    QtAsyncio.run(run_streams(tasks), handle_sigint=True)
    sys.exit(app.exec())


if __name__ == "__main__":
    jsonargparse.auto_cli(main)
