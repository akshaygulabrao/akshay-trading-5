import sys
import zmq
from PySide6.QtWidgets import (
    QApplication,
    QMainWindow,
    QTableWidget,
    QTableWidgetItem,
    QScrollArea,
    QVBoxLayout,
    QWidget,
    QLabel,
    QTabWidget,
)
from PySide6.QtCore import QThread
from collections import defaultdict

import utils


def extract_number(ticker):
    last_part = ticker.split("-")[-1]
    number_str = last_part[1:]
    return float(number_str)


class ZMQListener(QThread):
    data_received = pyqtSignal(dict)

    def __init__(self):
        super().__init__()
        self.context = zmq.Context()
        self.sub = self.context.socket(zmq.SUB)
        self.sub.connect("ipc:///tmp/orderbook.ipc")
        self.sub.setsockopt_string(zmq.SUBSCRIBE, "")

    def run(self):
        while True:
            data = self.sub.recv_json()
            self.data_received.emit(data)


class OrderbookWindow(QMainWindow):
    def __init__(self, ny_mkts):
        super().__init__()
        self.ny_mkts = ny_mkts
        self.initUI()
        self.market_data = {}
        self.zeromq_listener = ZMQListener()
        self.zeromq_listener.data_received.connect(self.update_orderbook)
        self.zeromq_listener.start()

    def initUI(self):
        self.setWindowTitle("Akshay's Weather Orderbook")
        self.tab_widget = QTabWidget()
        self.setCentralWidget(self.tab_widget)
        self.resize(800, 600)

        # Group markets by city and date
        city_groups = defaultdict(
            lambda: defaultdict(list)
        )  # city_code -> date_part -> [markets]
        for market in self.ny_mkts:
            parts = market.split("-")
            if len(parts) >= 2:
                city_part = parts[0]
                city_code = city_part[6:]  # Extract 'NY' from 'KXHIGHNY'
                date_part = parts[1]
                city_groups[city_code][date_part].append(market)
        tab_order = ["NY", "CHI", "AUS", "MIA", "DEN", "PHIL", "LAX"]
        sorted_cities = [city for city in tab_order if city in city_groups]
        self.groups = {}  # city_code -> date_part -> group data
        self.market_to_group = {}  # market -> (city_code, date_part, row_idx)

        # Process each city group
        for city_code in sorted_cities:
            # Create scroll area for the city tab
            scroll_area = QScrollArea()
            scroll_area.setWidgetResizable(True)
            container = QWidget()
            layout = QVBoxLayout(container)
            scroll_area.setWidget(container)

            # Process each date group in the city
            date_groups = city_groups[city_code]
            sorted_dates = sorted(date_groups.keys())
            city_data = {}
            for date_part in sorted_dates:
                markets = date_groups[date_part]
                sorted_markets = sorted(markets, key=lambda m: extract_number(m))

                # Create date label and table
                label = QLabel(f"<h2>{date_part}</h2>")
                label.setStyleSheet("font-weight: bold; margin-bottom: 5px;")
                table = QTableWidget()
                table.setColumnCount(3)
                table.setHorizontalHeaderLabels(["Market", "Yes", "No"])
                table.setRowCount(len(sorted_markets))

                # Populate table rows
                for row_idx, market in enumerate(sorted_markets):
                    table.setItem(row_idx, 0, QTableWidgetItem(market))
                    table.setItem(row_idx, 1, QTableWidgetItem(""))
                    table.setItem(row_idx, 2, QTableWidgetItem(""))
                    self.market_to_group[market] = (city_code, date_part, row_idx)

                layout.addWidget(label)
                layout.addWidget(table)
                city_data[date_part] = {
                    "label": label,
                    "table": table,
                    "markets": sorted_markets,
                }
                table.resizeColumnToContents(0)
            self.groups[city_code] = city_data
            self.tab_widget.addTab(scroll_area, city_code)

    def update_orderbook(self, data):
        market_ticker = data.get("market_ticker")
        yes_book = data.get("yes", {})
        no_book = data.get("no", {})

        # Process Yes book
        filtered_yes = {p: s for p, s in yes_book.items() if s > 0}
        top_yes = (
            max(filtered_yes.items(), key=lambda x: float(x[0]))
            if filtered_yes
            else (None, 0)
        )
        yes_price, yes_size = top_yes

        # Process No book
        filtered_no = {p: s for p, s in no_book.items() if s > 0}
        top_no = (
            max(filtered_no.items(), key=lambda x: float(x[0]))
            if filtered_no
            else (None, 0)
        )
        no_price, no_size = top_no

        self.market_data[market_ticker] = {
            "yes_price": yes_price,
            "yes_size": yes_size,
            "no_price": no_price,
            "no_size": no_size,
        }

        # Update UI if market is tracked
        if market_ticker in self.market_to_group:
            city_code, date_part, row_idx = self.market_to_group[market_ticker]
            city_group = self.groups.get(city_code, {})
            date_group = city_group.get(date_part, {})
            table = date_group.get("table")
            if not table:
                return

            data_entry = self.market_data.get(market_ticker, {})
            yes_str = no_str = ""

            if data_entry.get("yes_price") is not None:
                yes_price_num = int(data_entry["yes_price"])
                yes_str = f"{100 - yes_price_num} x {data_entry['yes_size']}"

            if data_entry.get("no_price") is not None:
                no_price_num = int(data_entry["no_price"])
                no_str = f"{100 - no_price_num} x {data_entry['no_size']}"

            # Update table items
            if row_idx < table.rowCount():
                table.item(row_idx, 1).setText(no_str)
                table.item(row_idx, 2).setText(yes_str)


if __name__ == "__main__":
    mkts = utils.get_markets()
    app = QApplication(sys.argv)
    window = OrderbookWindow(mkts)
    window.show()
    sys.exit(app.exec())
