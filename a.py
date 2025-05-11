import sys
import zmq
from PyQt6.QtWidgets import QApplication, QMainWindow, QTableWidget, QTableWidgetItem
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
        self.zeromq_listener = ZMQListener()
        self.zeromq_listener.data_received.connect(self.update_orderbook)
        self.zeromq_listener.start()

    def initUI(self):
        self.setWindowTitle('Top of Orderbook')
        self.table = QTableWidget()
        self.setCentralWidget(self.table)
        self.table.setColumnCount(3)
        self.table.setHorizontalHeaderLabels(['Market', 'Yes', 'No'])
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

        # Process converted No book
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
        sorted_markets = sorted(self.market_data.items(), key=lambda x: x[0])
        self.table.setRowCount(len(sorted_markets))
        
        for row, (market, data) in enumerate(sorted_markets):
            yes_str = ""
            if 'yes_price' in data and data['yes_price']:
                yes_str = f"{100 - int(data['yes_price'])} x {data['yes_size']}"

            no_str = ""
            if 'no_price' in data and data['no_price']:
                no_str = f"{100 - int(data['no_price'])} x {data['no_size']}"

            self.table.setItem(row, 0, QTableWidgetItem(market))
            self.table.setItem(row, 1, QTableWidgetItem(no_str))
            self.table.setItem(row, 2, QTableWidgetItem(yes_str))

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = OrderbookWindow()
    window.show()
    sys.exit(app.exec())