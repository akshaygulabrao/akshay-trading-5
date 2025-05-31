import sys
import asyncio
import datetime as dt

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
)
from PySide6.QtCore import Qt, QObject, Signal, Slot
from PySide6.QtGui import QKeySequence
import PySide6.QtAsyncio as QtAsyncio

from user import User
from utils import exchange_status, format_timedelta


class TradingApp(QMainWindow):
    setBal = Signal(str)
    exchStatus = Signal(str)

    def __init__(self, user: User):
        assert isinstance(user, User)
        super().__init__()
        self.user = user
        self.setWindowTitle("Trading Application")
        self.resize(800, 600)

        # Main widget and layout
        main_widget = QWidget()
        main_layout = QVBoxLayout(main_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        # Top pane (30% height)
        top_pane = QWidget()
        top_pane.setFixedHeight(int(self.height() * 0.3))
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
            tab_layout = QVBoxLayout(tab)
            label = QLabel(f"Content for {title} market")
            label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            tab_layout.addWidget(label)
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
