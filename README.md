# Kalshi Orderbook Logger

Dumps websocket messages in python.

examples/demo_markets.py
- examples to use

trading/kalshi_ref.py
- starter code that kalshi gives you

trading/order_placer.py
- stale code that used to work, needs updating

trading/orderbook.py
- This script tracks and periodically logs WebSocket orderbook data for multiple market tickers, with graceful shutdown handling.

trading/utils.py
- utility functions for easily fetching exchange data for weather-betting tickers

trading/weather_extract_forecast.py
- fetches NWS forecasts given site

trading/weather_info.py
- helper info for weather_extract_forecast

trading/weather_sensor_reading.py
- fetches latest sensor readings

trading/orderbook_update.py
- parses orderbook messages to orderbook Data structure
