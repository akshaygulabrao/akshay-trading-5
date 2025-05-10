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
        self.market_data = {}  # Stores top of book per market: {market_ticker: {price, size}}
        self.zeromq_listener = ZMQListener()
        self.zeromq_listener.data_received.connect(self.update_orderbook)
        self.zeromq_listener.start()

    def initUI(self):
        self.setWindowTitle('Top of Orderbook')
        self.table = QTableWidget()
        self.setCentralWidget(self.table)
        self.table.setColumnCount(3)
        self.table.setHorizontalHeaderLabels(['Market', 'Price', 'Size'])
        self.resize(800, 600)

    def update_orderbook(self, data):
        market_ticker = data.get('market_ticker')
        yes_book = data.get('yes', {})
        
        # Filter out entries with zero or negative size
        filtered_yes = {price: size for price, size in yes_book.items() if size > 0}
        
        # Calculate top of the book from filtered entries
        if filtered_yes:
            sorted_prices = sorted(
                filtered_yes.items(),
                key=lambda x: float(x[0]),
                reverse=True
            )
            top_price, top_size = sorted_prices[0]
        else:
            top_price = None
            top_size = 0
        
        # Update market data
        self.market_data[market_ticker] = {
            'price': top_price,
            'size': top_size
        }
        self.display_orderbook()

    def display_orderbook(self):
        # Sort markets alphabetically
        sorted_markets = sorted(self.market_data.items(), key=lambda item: item[0])
        self.table.setRowCount(len(sorted_markets))
        
        for row, (market, data) in enumerate(sorted_markets):
            price = str(data['price']) if data['price'] is not None else 'N/A'
            size = str(data['size'])
            
            self.table.setItem(row, 0, QTableWidgetItem(market))
            self.table.setItem(row, 1, QTableWidgetItem(price))
            self.table.setItem(row, 2, QTableWidgetItem(size))

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = OrderbookWindow()
    window.show()
    sys.exit(app.exec())