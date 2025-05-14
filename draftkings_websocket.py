import asyncio
import uuid

import websockets
from loguru import logger
import msgpack

params = {'entity': 'events', 'queryParams': {'query': "$filter=leagueId eq '84240'", 'initialData': False, 'includeMarkets': 'none', 'projection': 'betOffers', 'locale': 'en-US'}, 'forwardedHeaders': {}, 'clientMetadata': {'feature': 'LEAGUE_PAGE', 'X-Client-Name': 'web', 'X-Client-Version': '2519.1.1.7'}, 'jwt': 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImN0eSI6IkpXVCJ9.eyJJc0Fub255bW91cyI6IlRydWUiLCJTaXRlR3JvdXBJZCI6IjM5IiwiU2l0ZUlkIjoiMTUwNjUiLCJEb21haW5JZCI6IjQ0OSIsIkNvdW50cnlDb2RlIjoiVVMiLCJuYmYiOjE3NDcxODcyNjcsImV4cCI6MTc0NzE4NzM4NywiaWF0IjoxNzQ3MTg3MjY3LCJpc3MiOiJ1cm46ZGsiLCJhdWQiOiJ1cm46ZGsifQ.upzfrgLCAIAk_iOEL0kqP1vBa6gEGbnwlxxMjFA6xl5ye75WFx1f1bVS0hHTt2_UJQFNL14qeu0FI4TaTKdBxDZnRi0mtPPmQoEDt9ym4TXxMosZM4nQOZETgbUGFhgxj2gw-piY9gA9JTnkSjarmX3AywYNFmxnmyYQ1zus0IUyM2r-alYI7V7wPZQ5pBY54ExOyfRB7Tu24uprHUq3uroX2jaE2NmciZULD9fQIDWMAJgeQncAYOHtKAJOtJOVhZICNnYs-oE2UGNewc0kohOl0GdX5e-0KvcJsIc8HR9hZJHsYWZnCufzrdIi-EmR8R2g7smiaOdQ86QVlsFmsOfa9VaBKHiyDGgcJx7L7YCW1q9jbx8zapgIqT4VDVQVVOC7t0pBjRoqBi3zcabRtJYfwxZT3WH7VOctCVPQy5ZQyk3scqh09hMd_2CNB_vqlUw7cPcsL7LEqUQJ1BN66ODuJqwk6d_wEnxUQxQKvnYlYZ-gZbTPNlSnW6QJXTzpp1ZTLEAhy-IG_NmwBQ2QuWe5wLohGD_eY9RNtAN4imDbdB6dFvtHq6Fr-plV7X_voA6IVdiajip8aB0WgKulWGTfK3_So_IIRP_8btWvubwny65zYJiFMloKUrzrcISx1yx6p4Nv8XIATDCmgTw6YUJxz74slSo5yY0fIforwng', 'siteName': 'dkusnj'}

ws = None

async def connect():
    global ws
    host = 'wss://sportsbook-ws-us-nj.draftkings.com/websocket?format=msgpack'
    async with websockets.connect(host) as websocket:
        ws = websocket
        await on_open()
        try:
            async for msg in ws:
                await on_msg(msg)
        except Exception as e:
            logger.error(e)
            await on_err(e)

async def on_open():
    global ws
    data = msgpack.packb({'jsonrpc':2.0,
                          'params':params,
                          'method':'subscribe',
                          'id':str(uuid.uuid4())})
    await ws.send(data)

async def on_err(e):
    global ws
    logger.error(e)

async def on_msg(msg):
    global ws
    logger.info(f'{msgpack.unpackb(msg)}')

async def main():
    await connect()

if __name__ == "__main__":
    asyncio.run(main())
