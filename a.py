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
        self.current_yes_book = {}
        self.zeromq_listener = ZMQListener()
        self.zeromq_listener.data_received.connect(self.update_orderbook)
        self.zeromq_listener.start()

    def initUI(self):
        self.setWindowTitle('Yes Side Orderbook')
        self.table = QTableWidget()
        self.setCentralWidget(self.table)
        self.table.setColumnCount(2)
        self.table.setHorizontalHeaderLabels(['Price', 'Size'])
        self.resize(600, 800)

    def update_orderbook(self, data):
        self.current_yes_book = data.get('yes', {})
        self.display_orderbook()

    def display_orderbook(self):
        sorted_prices = sorted(
            self.current_yes_book.items(),
            key=lambda x: float(x[0]),
            reverse=True
        )
        self.table.setRowCount(len(sorted_prices))
        for row, (price, size) in enumerate(sorted_prices):
            self.table.setItem(row, 0, QTableWidgetItem(str(price)))
            self.table.setItem(row, 1, QTableWidgetItem(str(size)))

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = OrderbookWindow()
    window.show()
    sys.exit(app.exec())