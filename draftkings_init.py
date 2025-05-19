import time
import random
import string
import base64
import json
from datetime import datetime, timezone
import asyncio

import httpx

# Helper functions
def current_time_ms():
    return int(time.time() * 1000)

def current_time_s():
    return int(time.time())

def random_string(length=12):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

# Timestamps
now_ms = current_time_ms()
now_s = current_time_s()
random_id = random_string(12)
session_start = now_ms - random.randint(1000, 5000)  # Session started a few seconds ago
# Corrected hgg cookie generation
hgg_payload = {
    "vid": "22589236644",
    "nbf": now_s - 300,  # Not Before
    "exp": now_s + 300,  # Expiration
    "iat": now_s,       # Issued At
    "iss": "dk"
}

# Simulating base64 encoding (real implementation would use base64.urlsafe_b64encode)
hgg_header = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"  # Static header
hgg_encoded_payload = base64.urlsafe_b64encode(json.dumps(hgg_payload).encode('utf-8')).decode('utf-8').rstrip("=")
hgg_signature = "Kv8aTz-T1dL_c6eyqXRFUQX6Wvy9Pclk4SLshIQdUvo"  # Mock signature


cookies = {
    # Previously hardcoded timestamps now dynamic
    '_gcl_au': f'1.1.91236377.{now_s - 86400}',  # 1 day ago
    '_sctr': f'1%7C{now_ms + 86400000}',  # 1 day in future
    'ttcsid': f'{now_ms}::{random_string()}.4.{now_ms + 50000}',
    'ttcsid_C9QNSUJC77U8C02RC95G': f'{now_ms}::{random_string()}.4.{now_ms + 50000}',
    '_ga_MCSJH508Q0': f'GS2.2.s{now_ms}$o4$g1$t{now_ms + 50000}$j60$l0$h0',
    'PRV': f'3P=1&V=22589236644&E={now_s + 86400}',  # 1 day in future
    '_gid': f'GA1.2.426596298.{now_s}',
    '_tglksd': f'eyJzIjoiNzZlOTQwNDktM2E4ZS01OTMwLThjYjAtZDc1ZWFhZWI4ZWYyIiwic3QiOjE3NDcxNjM2NDUwOTMsInNvZCI6InNwb3J0c2Jvb2suZHJhZnRraW5ncy5jb20iLCJzb2R0IjoxNzQ3MTYzNjQ1MDkzLCJzb2RzIjoiciIsInNvZHN0IjoxNzQ3MTYzNjQ1MDkzfQ=='.replace("1747163645093", str(now_ms)),
    '_ga_BVKEVF9TVH': f'GS2.2.s{now_ms}$o1$g0$t{now_ms}$j0$l0$h0',
    'ab.storage.sessionId.b543cb99-2762-451f-9b3e-91b2b1538a42': f'%7B%22g%22%3A%22{random_string()}%22%2C%22e%22%3A{now_ms + 3600000}%2C%22c%22%3A{now_ms}%2C%22l%22%3A{now_ms + 1800000}%7D',
    '_ga_QG8WHJSQMJ': f'GS2.1.s{now_ms}$o10$g1$t{now_ms + 30000}$j60$l0$h0',
    'STE': f'"{datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")}"',
    '_ga': f'GA1.2.{random.randint(100000000, 999999999)}.{now_s - 86400}',
    '_dpm_id.16f4': f'{random_string()}-{random_string()}-{random_string()}-{random_string()}-{random_string()}.{now_s - 86400}.10.{now_ms}.{now_ms - 5000}.{random_string()}',
    '_abck': f'{random_string(32)}~0~{random_string(1000)}~-1~||0||~-1',
    '_ga_M8T3LWXCC5': f'GS2.2.s{now_ms}$o5$g1$t{now_ms + 3000}$j57$l0$h0',
    '_rdt_uuid': f'{now_ms}.{random_string(8)}-{random_string(4)}-{random_string(4)}-{random_string(4)}-{random_string(12)}',

    # Static cookies (no timestamps)
    '_scid': 'euAzEi-thU6GiQ2oiM4W93D1_5DPeRKW',
    '_svsid': '5622c02bc5143b5b536c3f5141856958',
    '__ssid': 'c6aad0c1504f22a359305598f9e7790',
    'site': 'US-DK',
    'SN': '1223548523',
    'LID': '1',
    '_tgpc': '73cb5892-0f4d-5845-b092-4a3bd7ccf6c5',
    '_tt_enable_cookie': '1',
    '_ttp': '01JTD6AN0D9KM0BXD6J5ETY1DT_.tt.1',
    '_ScCbts': '%5B%5D',
    'VIDN': '22589236644',
    'SINFN': 'PID=&AOID=&PUID=22456344&SSEG=&GLI=0&LID=1&site=US-DK',
    'knownUser': 'true',
    '_hjSessionUser_2150570': 'eyJpZCI6ImQ3Y2EyMmMwLWUxMDYtNTBmYy1iMGE0LTY1MzU1NmQzMjIyYiIsImNyZWF0ZWQiOjE3NDYyMzMzNjI3NzksImV4aXN0aW5nIjp0cnVlfQ==',
    '_csrf': 'e392f225-4305-43db-b2ec-6323a431f997',
    'ss-id': 'XDV1Dlimp7bepE0GT92U',
    'ss-pid': 'cwuAQcEmOQAeEHE36XGs',
    '_hjSessionUser_3032622': 'eyJpZCI6ImExYzAwNjYwLWQ2NmQtNTkwNy05NDc3LTVhMzg2NDk5NzliYyIsImNyZWF0ZWQiOjE3NDcxNjM2NDYyODksImV4aXN0aW5nIjpmYWxzZX0=',
    'ASP.NET_SessionId': 'g4apbej5a2duhagjjw5nmodg',
    'STIDN': 'eyJDIjoxMjIzNTQ4NTIzLCJTIjo5NTc1NzQ0NzE2OCwiU1MiOjk5NzQ0NTk4MDM2LCJWIjoyMjU4OTIzNjY0NCwiTCI6MSwiRSI6IjIwMjUtMDUtMTNUMjM6NTQ6MzEuMjk4NDk1NVoiLCJTRSI6IlVTLURLIiwiVUEiOiJWN3dIMHBSVXlyVUFTR0pFWXB4ZXdOTENYMnljNTBCTXFEMkhpTkdjL2VVPSIsIkRLIjoiM2MwOWJjMDUtYzM4MS00MWY2LThjZDEtMDk1Y2JkZGM1OWUxIiwiREkiOiJmNDhjMTdkMC1hZmE2LTRjNTktYWRhMi05MGRhZWMyZjQ1MWIiLCJERCI6NjI1MTQ5OTk0Mjd9',
    'STH': '79beb2bb8cf7d110569d2c49312c69e5a73d0fe60cd7b5d7db87853572c94823',
    'ak_bmsc': '87275CBAB9311C341C7F53AB947BAC9F~000000000000000000000000000000~YAAQZvfVF+NFpsSWAQAA7s32yxvkQhAzG9rNf+EPG/MgseDy4WGwo+wqmufDygLFVYZhWh7yQyb473ulXd5DRYIbkNfGlpODzCD2blLHOU8CDzpF2cqfHrOvYsZV0pLyO0NT14FGANwwQ1cgFJeCVJLZevrzPISVcHb8JUAHucra8y0hrWUmy2/YiGSWDrxVYvO8UTOey7LLv7vtxvHiS4Bir8uB4fq25alLBm3Xx8cZDv/nvf4BglBCOjxFDPAFBDItxzK+bGAnGP8Na99Wp+WqKkVy6NZSKNzdwJtglo0EVi7JEU/eOABpwtPcP/ylhwSeQ3np0110p7zOwP6x5zbxgdzf7mQX1GhnqO51HTeV/jUMnuqlP7Wcr58DmQYjKqtYKppT1028dC9nEba8B2KlH2nuvBJo7U4=',
    '_dpm_ses.16f4': '*',
    '_hjSession_2150570': 'eyJpZCI6IjdhZWIzZWFjLTA0NjMtNGQ1Mi05YTEzLTM3ZDQ3N2Y3NjAzYyIsImMiOjE3NDcxODAzODcwNjgsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowfQ==',
    'bm_sv': '3C8C97C223A59367259DE597496CAB97~YAAQaffVF7foj4yWAQAAeRYkzBskANBSYVSNmTjq6DEI0wzolFkTtZvrr8c0gzBWIInn5mhv3nPDSncgRv2QPvJ/77iR45Ovaak1tQ8zLTNZ6prx5gX11FvBfX0/H6iufQPe7D65Nf/GnzM16c9URlPgsp/KTibevJ6zMgRsOmG6Nd2/tjknTn6WdgjuoLB3dauzkK5H22zQTmNa7i/aIBZ0vFrU7YjU9xEbIc5HqDHVb0v9FoSlDF4Pce46kBf+97hrK8=~1',
    '_gat_UA-28146424-9': '1',
    '_gat_UA-28146424-14': '1',
    '_scid_r': 'nmAzEi-thU6GiQ2oiM4W93D1_5DPeRKWQ7Yjbg',
    '_uetsid': '9fb62440302011f081c1a37a10401657',
    '_uetvid': '6b40975027b811f0a0050ba145863e33',
}
cookies['hgg'] = f"{hgg_header}.{hgg_encoded_payload}.{hgg_signature}"

headers = {
    'sec-ch-ua-platform': '"Linux"',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
    'dnt': '1',
    'sec-ch-ua-mobile': '?0',
    'accept': '*/*',
    'origin': 'https://sportsbook.draftkings.com',
    'sec-fetch-site': 'same-site',
    'sec-fetch-mode': 'cors',
    'sec-fetch-dest': 'empty',
    'referer': 'https://sportsbook.draftkings.com/',
    'accept-language': 'en-US,en;q=0.9',
    'priority': 'u=1, i',
}
draftkings_links = {
    # "Golf": "https://sportsbook-nash.draftkings.com/api/sportscontent/dkusnj/v1/leagues/79720/categories/484/subcategories/4508",
    "Baseball": "https://sportsbook-nash.draftkings.com/api/sportscontent/dkusnj/v1/leagues/84240",
    "Basketball": "https://sportsbook-nash.draftkings.com/api/sportscontent/dkusnj/v1/leagues/42648",
    "Hockey" : "https://sportsbook-nash.draftkings.com/api/sportscontent/dkusnj/v1/leagues/42133",
    "EPL" : "https://sportsbook-nash.draftkings.com/api/sportscontent/dkusnj/v1/leagues/40253",
    "UFC": "https://sportsbook-nash.draftkings.com/api/sportscontent/dkusnj/v1/leagues/9034",
}

async def fetch_all_sports():
    for sport in draftkings_links.keys():
        async with httpx.AsyncClient(http2=True) as client:
            response = await client.get(draftkings_links[sport], headers=headers)
            response = response.json()

        with open(f'sports/{sport.lower()}.json','w',encoding='utf-8') as f:
            json.dump(response,f,indent=4)
    
if __name__ == "__main__":
    asyncio.run(fetch_all_sports())