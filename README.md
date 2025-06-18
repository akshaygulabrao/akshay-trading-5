# Kalshi Climate Orderbook Visualizer

This repository is a PyQt Application that succinctly shows the orderbook for all of [Kalshi's](https://kalshi.com/?category=all) daily high weather contracts. It utilizes websockets to communicate with the Kalshi server with all orderbook updates.

## Getting Started
This application relies on PROD_KEYID and PROD_KEYFILE being already set up. See `utils.py` for more details on how to configure them.

```bash
uv sync
uv run app.py --all-sites=true
```
