import sys
import asyncio
import datetime as dt
from collections import defaultdict
import subprocess
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
from zmq.asyncio import Context,Poller

from user import User
import utils
from utils import exchange_status, format_timedelta


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



class OrderBookListener:
    def __init__(self, window: TradingApp):
        assert isinstance(window, TradingApp)
        self.window = window
        ctx = zmq.Context()

        self.subscriber = ctx.socket(zmq.PULL)
        self.subscriber.connect("ipc:///tmp/orderbook.ipc")
        self.subscriber.setsockopt_string(zmq.SUBSCRIBE, "")
        self.poller = Poller()
        self.poller.register(self.subscriber, zmq.POLLIN)

    async def stream(self):
        while True:
            try:
                events = await self.poller.poll()
                print(dict(events))
                if self.subscriber in dict(events):
                    print('a')
                print()
                # print("Received orderbook update:")
                # print(f"Market: {message['market_ticker']}")
                # print("Yes bids:")
                # for price, size in message['yes'].items():
                #     print(f"  {price}: {size}")
                # print("No bids:")
                # for price, size in message['no'].items():
                #     print(f"  {price}: {size}")
                print("-" * 40)
            except zmq.ZMQError as e:
                print(f"ZMQ error occurred: {e}")
                break
            except asyncio.CancelledError:
                print("Orderbook subscription cancelled")
                break
            except Exception as e:
                print(f"Error processing orderbook message: {e}")

def launch_orderbook():
    common_dir = "/Users/trading/workspace/akshay-trading-5"
    py = f"{common_dir}/.venv/bin/python"
    executable = f"{common_dir}/orderbook.py"
    proc = subprocess.Popen([py, executable])
    return proc


if __name__ == "__main__":
    proc = launch_orderbook()
    mkts = utils.get_markets()
    user = User()
    app = QApplication(sys.argv)
    window = TradingApp(user)
    balance = Balance(user, window)
    exchange = Exchange(window)
    ob = OrderBookListener(window)

    tasks = [balance.stream(), exchange.stream(), ob.stream()]

    async def run_streams(tasks):
        await asyncio.gather(*tasks)

    window.show()
    QtAsyncio.run(run_streams(tasks), handle_sigint=True)
    proc.terminate()
