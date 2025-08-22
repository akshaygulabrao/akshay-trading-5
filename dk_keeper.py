#!.venv/bin/python
import requests

# Create a reusable session
session = requests.Session()

# Common headers that mirror your curl
session.headers.update({
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    'dnt': '1',
    'origin': 'https://sportsbook.draftkings.com',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://sportsbook.draftkings.com/',
    'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'
})

# 1) Warm-up call to pick up cookies
url = 'https://sportsbook.draftkings.com/live'
resp = session.get(url)
resp.raise_for_status()

# 2) Request the JWT token (cookies already in the session)
jwt_url = 'https://gaming-us-nj.draftkings.com/api/wager/v1/generateAnonymousEnterpriseJWT'
session.headers.update({
    'accept': 'application/json',
    'content-type': 'application/json'
})

jwt_resp = session.get(jwt_url, json={})
jwt_resp.raise_for_status()

token = jwt_resp.json().get('token')
print('JWT token:', token)
