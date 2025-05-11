import sys
import zmq
from PyQt6.QtWidgets import (QApplication, QMainWindow, QTableWidget, QTableWidgetItem,
                             QScrollArea, QVBoxLayout, QWidget, QLabel)
from PyQt6.QtCore import QThread, pyqtSignal

class ZMQListener(QThread):
    data_received = pyqtSignal(dict)

    def __init__(self):
        super().__init__()
        self.context = zmq.Context()
        self.sub = self.context.socket(zmq.SUB)
        self.sub.connect("ipc:///tmp/orderbook.ipc")
        self.sub.setsockopt_string(zmq.SUBSCRIBE, '')

    def run(self):
        while True:
            data = self.sub.recv_json()
            self.data_received.emit(data)

class OrderbookWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.initUI()
        self.market_data = {}
        self.group_tables = {}  # Maps group names to (label, table) tuples
        self.zeromq_listener = ZMQListener()
        self.zeromq_listener.data_received.connect(self.update_orderbook)
        self.zeromq_listener.start()

    def initUI(self):
        self.setWindowTitle('Top of Orderbook')
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.container_widget = QWidget()
        self.container_layout = QVBoxLayout(self.container_widget)
        self.scroll_area.setWidget(self.container_widget)
        self.setCentralWidget(self.scroll_area)
        self.resize(800, 600)

    def update_orderbook(self, data):
        market_ticker = data.get('market_ticker')
        yes_book = data.get('yes', {})
        no_book = data.get('no', {})

        # Process Yes book
        filtered_yes = {p: s for p, s in yes_book.items() if s > 0}
        if filtered_yes:
            top_yes = max(filtered_yes.items(), key=lambda x: float(x[0]))
            yes_price, yes_size = top_yes
        else:
            yes_price, yes_size = None, 0

        # Process No book
        filtered_no = {p: s for p, s in no_book.items() if s > 0}
        if filtered_no:
            top_no = max(filtered_no.items(), key=lambda x: float(x[0]))
            no_price, no_size = top_no
        else:
            no_price, no_size = None, 0

        self.market_data[market_ticker] = {
            'yes_price': yes_price,
            'yes_size': yes_size,
            'no_price': no_price,
            'no_size': no_size
        }
        self.display_orderbook()

    def display_orderbook(self):
        # Group the market data by their date part
        groups = {}
        for market_ticker, data in self.market_data.items():
            parts = market_ticker.split('-')
            if len(parts) >= 2:
                date_part = parts[1]
                group_name = f"NY-{date_part}"
            else:
                group_name = "Unknown"
            if group_name not in groups:
                groups[group_name] = {}
            groups[group_name][market_ticker] = data

        # Remove groups that no longer exist (not handling here for simplicity)
        # Process each group in sorted order
        sorted_group_names = sorted(groups.keys())
        for group_name in sorted_group_names:
            group_data = groups[group_name]
            if group_name not in self.group_tables:
                # Create new label and table
                label = QLabel(f"<h2>{group_name}</h2>")
                label.setStyleSheet("font-weight: bold; margin-bottom: 5px;")
                table = QTableWidget()
                table.setColumnCount(3)
                table.setHorizontalHeaderLabels(['Market', 'Yes', 'No'])
                self.container_layout.addWidget(label)
                self.container_layout.addWidget(table)
                self.group_tables[group_name] = (label, table)
            else:
                label, table = self.group_tables[group_name]

            # Populate the table with current group data
            sorted_markets = sorted(group_data.items(), key=lambda x: x[0])
            table.setRowCount(len(sorted_markets))
            for row, (market, data) in enumerate(sorted_markets):
                yes_str = ""
                if data['yes_price'] is not None:
                    yes_price = int(data['yes_price'])
                    yes_str = f"{100 - yes_price} x {data['yes_size']}"

                no_str = ""
                if data['no_price'] is not None:
                    no_price = int(data['no_price'])
                    no_str = f"{100 - no_price} x {data['no_size']}"

                table.setItem(row, 0, QTableWidgetItem(market))
                table.setItem(row, 1, QTableWidgetItem(no_str))
                table.setItem(row, 2, QTableWidgetItem(yes_str))

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = OrderbookWindow()
    window.show()
    sys.exit(app.exec())