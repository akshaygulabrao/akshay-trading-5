"""
Module correctly subscribes to everything in baseball

Improvments:
1) Find a way to subscribe to only the moneyline instead of {Run Line, Total, Moneyline}

"""
import asyncio
import uuid

import websockets
from loguru import logger
import msgpack

query = {'jsonrpc': '2.0', 'params': {'entity': 'markets', 'queryParams': {'query': "$filter=leagueId eq '84240' and clientMetadata/subCategoryId eq '4519' and tags/all(t: t ne 'SportcastBetBuilder')", 'initialData': False, 'projection': 'betOffers', 'locale': 'en-US'}, 'forwardedHeaders': {}, 'clientMetadata': {'feature': 'LEAGUE_PAGE', 'X-Client-Name': 'web', 'X-Client-Version': '2519.1.1.7'}, 'jwt': 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImN0eSI6IkpXVCJ9.eyJJc0Fub255bW91cyI6IlRydWUiLCJTaXRlR3JvdXBJZCI6IjM5IiwiU2l0ZUlkIjoiMTUwNjUiLCJEb21haW5JZCI6IjQ0OSIsIkNvdW50cnlDb2RlIjoiVVMiLCJuYmYiOjE3NDcyNTMxNTUsImV4cCI6MTc0NzI1MzI3NSwiaWF0IjoxNzQ3MjUzMTU1LCJpc3MiOiJ1cm46ZGsiLCJhdWQiOiJ1cm46ZGsifQ.mPrfLxIzrqF7jRliaFna3cC6A4gg_H_cJV15n8_qIjhbp3cFJYYXa5bZOpv-hnDvjJ7dTi13H-ocpUYxhvyDFKsTJeEZ0-NEYlJwX3TLHwCtiOul56NBcjRbfLRSmW5JOSfX2RNObAvAw6ite72rdl-hXSmx7YrvQMOcjDojIym1uY9Fe8jm9NSnisAS7d4JHYIGWG0gsi5tOy52Tp-YM7Pba8ZhFAdluGdYcEZ9fc99IwuZDh4XWQYOqcd1jJMrmJfvLXIAtnvEGkycKKofwSmsPOfBCZegz4JU9AMMIv-NsQTWvzbhvMLq0FFaISb5ig6svEvjXKGFKmcmxlzTe6R1zI-DERN7umrS_mnRWv-HBPIhh0LDc-DchYGhjx7iGbifkMYs7-BfczQ05jRkRRvsPZ1BG48NhalHcdXRrGjU3mPr7S_17KGA8nAvlhcRxLM-67XqmlU8OE_RN5KE97U_nEyvsBpqawF88GUkG_emKemCL6WMUPcKUeXTISfWlBmEVWcRBA6ErGX3RUWU283k8uZ6sbIFoQvWuf3RQEMxCw0zOIbgo4dh7UPG8XDJHzydOlN6DEvb2i73NcpBKkk4mDU3gNI5EdAqJiSfhihsJ70AMQqFrRpITWC0m9QViUucKiad1rnHMGX_O0pi2SBcH1tJ6QO9g17Vavf0u58', 'siteName': 'dkusnj'}, 'method': 'subscribe', 'id': '4ec8ee8f-ad1c-4aaa-9d2e-9f4a21892a3e'}

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
            await websocket.close(code=1000, reason="Normal closure")

async def on_open():
    global ws
    data = msgpack.packb(query)
    await ws.send(data)

async def on_err(e):
    global ws
    logger.error(e)

async def on_msg(msg):
    global ws
    msg = msgpack.unpackb(msg)
    msg
    logger.info(f'{msgpack.unpackb(msg)}')

async def main():
    try:
        await connect()
    except:
        logger.error("main loop cancelled")

if __name__ == "__main__":
    asyncio.run(main())
