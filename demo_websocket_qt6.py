import sys
import json
from PySide6.QtCore import QUrl, Qt, Signal, QObject
from PySide6.QtWidgets import QApplication
from PySide6.QtWebSockets import QWebSocket
from PySide6.QtNetwork import QNetworkRequest,QAbstractSocket
from utils import setup_client,get_markets

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

    def connect_to_server(self, url, headers):
        """Connect to the WebSocket server with custom headers"""
        request = QNetworkRequest(QUrl(url))
        
        # Set headers
        for key, value in headers.items():
            request.setRawHeader(key.encode('utf-8'), value.encode('utf-8'))
            
        self.websocket.open(request)

    def on_connected(self):
        print("✅ WebSocket connected")
        self.connected.emit()
        
        # Subscribe to desired channels after connection
        tickers = get_markets()
        print(tickers)
        self.subscribe_to_portfolio(tickers)

    def on_disconnected(self):
        print("❌ WebSocket disconnected")
        self.disconnected.emit()

    def on_text_message_received(self, message):
        print("📨 Message received:")
        print(message)
        self.message_received.emit(message)

    def on_error(self, error):
        error_msg = f"WebSocket error: {error}"
        print(error_msg)
        self.error_occurred.emit(error_msg)

    def subscribe_to_portfolio(self,tickers):
        """Example: Subscribe to portfolio updates"""
        subscribe_message = {
            "id": self.message_id,
            "cmd": "subscribe",
            "params": {"channels": ["orderbook_delta"], "market_tickers": tickers},
        }
        self.send_message(json.dumps(subscribe_message))
        self.message_id +=1

    def send_message(self, message):
        if self.websocket.state() == QAbstractSocket.SocketState.ConnectedState:
            self.websocket.sendTextMessage(message)
        else:
            print("Cannot send message - WebSocket is not connected")

    def close_connection(self):
        self.websocket.close()

def main():
    # Application setup
    QApplication.setAttribute(Qt.ApplicationAttribute.AA_ShareOpenGLContexts, True)
    app = QApplication(sys.argv)
    path = "/trade-api/ws/v2"
    base = "wss://api.elections.kalshi.com"

    client = setup_client()
    headers = client.request_headers("GET", "/trade-api/ws/v2")
    websocket_url = base + path
    
    # Create and connect WebSocket client
    ws_client = WebSocketClient()
    ws_client.connect_to_server(websocket_url, headers)
    
    # Connect signals to handle events
    ws_client.message_received.connect(lambda msg: print(f"Processed message: {msg}"))
    ws_client.error_occurred.connect(lambda err: print(f"Error: {err}"))
    
    # Set up a timer to exit after some time (for demonstration)
    # In a real app, you'd keep it running and handle user input
    from PySide6.QtCore import QTimer
    QTimer.singleShot(10000, lambda: (ws_client.close_connection(), app.quit()))
    
    sys.exit(app.exec())

if __name__ == "__main__":
    main()