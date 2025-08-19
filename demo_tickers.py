import requests

def weather_tickers():
    tickers = []
    cursor = None
    while True:
        params = {"status": "open", "limit": 1000}
        if cursor:
            params['cursor'] = cursor

        try:
            r = requests.get(
                "https://api.elections.kalshi.com/trade-api/v2/markets",
                params=params,
                timeout=5
            ).json()
        except requests.exceptions.RequestException as e:
            print("Request failed:", e)
            break

        markets = r.get("markets", [])
        tickers.extend([m["ticker"] for m in markets])

        cursor = r.get("cursor")
        if not cursor:
            break

    return tickers

print(len(weather_tickers()))
