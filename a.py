import asyncio
import sys
from PyQt6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QTabWidget,
)
from PyQt6.QtCore import Qt
import qasync
from loguru import logger

from user import User


class TradingApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.user = User()
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
        balance_value = QLabel("$1,234,567.89")
        self.user.label = balance_value

        balance_value.setAlignment(Qt.AlignmentFlag.AlignCenter)
        balance_layout.addWidget(balance_label)
        balance_layout.addWidget(balance_value)

        # Exchange status widget (right)
        status_widget = QWidget()
        status_layout = QVBoxLayout(status_widget)
        status_label = QLabel("Exchange Status")
        status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        status_value = QLabel("OPEN")
        status_value.setAlignment(Qt.AlignmentFlag.AlignCenter)
        status_layout.addWidget(status_label)
        status_layout.addWidget(status_value)

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

    def startAsyncTasks(self):
        asyncio.create_task(self.user.getBalanceAsync())


async def main():
    app = QApplication(sys.argv)
    window = TradingApp()
    window.startAsyncTasks()
    window.show()

    await qasync.QEventLoop(app).run_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except asyncio.exceptions.CancelledError:
        logger.error("keyboard interrupt")
        pass
