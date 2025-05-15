"""
Module correctly subscribes to everything in baseball
"""
import asyncio
import uuid
from datetime import datetime, timedelta
import websockets
from loguru import logger
import msgpack
import zmq

query = {
    'jsonrpc': '2.0',
    'params': {
        'entity': 'markets',
        'queryParams': {
            'query': "$filter=leagueId eq '84240' and clientMetadata/subCategoryId eq '4519' and tags/all(t: t ne 'SportcastBetBuilder')",
            'initialData': False,
            'projection': 'betOffers',
            'locale': 'en-US'
        },
        'forwardedHeaders': {},
        'clientMetadata': {
            'feature': 'LEAGUE_PAGE',
            'X-Client-Name': 'web',
            'X-Client-Version': '2519.1.1.7'
        },
        'jwt': 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImN0eSI6IkpXVCJ9.eyJJc0Fub255bW91cyI6IlRydWUiLCJTaXRlR3JvdXBJZCI6IjM5IiwiU2l0ZUlkIjoiMTUwNjUiLCJEb21haW5JZCI6IjQ0OSIsIkNvdW50cnlDb2RlIjoiVVMiLCJuYmYiOjE3NDcyNTMxNTUsImV4cCI6MTc0NzI1MzI3NSwiaWF0IjoxNzQ3MjUzMTU1LCJpc3MiOiJ1cm46ZGsiLCJhdWQiOiJ1cm46ZGsifQ.mPrfLxIzrqF7jRliaFna3cC6A4gg_H_cJV15n8_qIjhbp3cFJYYXa5bZOpv-hnDvjJ7dTi13H-ocpUYxhvyDFKsTJeEZ0-NEYlJwX3TLHwCtiOul56NBcjRbfLRSmW5JOSfX2RNObAvAw6ite72rdl-hXSmx7YrvQMOcjDojIym1uY9Fe8jm9NSnisAS7d4JHYIGWG0gsi5tOy52Tp-YM7Pba8ZhFAdluGdYcEZ9fc99IwuZDh4XWQYOqcd1jJMrmJfvLXIAtnvEGkycKKofwSmsPOfBCZegz4JU9AMMIv-NsQTWvzbhvMLq0FFaISb5ig6svEvjXKGFKmcmxlzTe6R1zI-DERN7umrS_mnRWv-HBPIhh0LDc-DchYGhjx7iGbifkMYs7-BfczQ05jRkRRvsPZ1BG48NhalHcdXRrGjU3mPr7S_17KGA8nAvlhcRxLM-67XqmlU8OE_RN5KE97U_nEyvsBpqawF88GUkG_emKemCL6WMUPcKUeXTISfWlBmEVWcRBA6ErGX3RUWU283k8uZ6sbIFoQvWuf3RQEMxCw0zOIbgo4dh7UPG8XDJHzydOlN6DEvb2i73NcpBKkk4mDU3gNI5EdAqJiSfhihsJ70AMQqFrRpITWC0m9QViUucKiad1rnHMGX_O0pi2SBcH1tJ6QO9g17Vavf0u58',
        'siteName': 'dkusnj'
    },
    'method': 'subscribe',
    'id': '4ec8ee8f-ad1c-4aaa-9d2e-9f4a21892a3e'
}

class BaseballFeed:
    def __init__(self):
        self.ws = None
        self.context = zmq.Context()
        self.pub = self.context.socket(zmq.PUB)
        self.pub.bind("ipc:///tmp/draftkings_baseball.ipc")
        self.reconnect_interval = 15 * 60  # 15 minutes in seconds
        self.last_connection_time = None

    async def connect(self):
        host = 'wss://sportsbook-ws-us-nj.draftkings.com/websocket?format=msgpack'
        self.last_connection_time = datetime.now()
        
        try:
            async with websockets.connect(host) as websocket:
                self.ws = websocket
                await self.on_open()
                
                # Start a task to check for reconnection time
                asyncio.create_task(self.check_reconnection_time())
                
                try:
                    async for msg in websocket:
                        await self.on_msg(msg)
                except websockets.exceptions.ConnectionClosed as e:
                    logger.warning(f"Connection closed: {e}")
                    raise
                except Exception as e:
                    logger.error(f"Unexpected error: {e}")
                    raise
        except Exception as e:
            logger.error(f"Connection error: {e}")
            await self.on_err(e)

    async def check_reconnection_time(self):
        while True:
            await asyncio.sleep(60)  # Check every minute
            if (datetime.now() - self.last_connection_time) > timedelta(seconds=self.reconnect_interval):
                logger.info("15 minutes elapsed - reconnecting...")
                if self.ws:
                    await self.ws.close()
                break

    async def on_open(self):
        data = msgpack.packb(query)
        await self.ws.send(data)
        logger.info("Successfully connected and subscribed")

    async def on_err(self, e):
        logger.error(f"Error occurred: {e}")
        await asyncio.sleep(5)  # Wait before reconnecting

    async def on_msg(self, msg):
        try:
            msg = msgpack.unpackb(msg)
            if msg[1] != "update":
                return
                
            logger.info(f"{msg[2][2]}")
            market_updates = msg[2][0][2]
            
            for market in market_updates:
                market_details = market[1]
                market_name = market_details[1]
                
                if market_name == "Moneyline":
                    m = market_details[0]
                    teams_data = market_details[8]
                    
                    t = []
                    for team_data in teams_data:
                        t.append({
                            'name': team_data[1],
                            'odds': team_data[3],
                        })
                    ts = msg[2][2]
                    data = {
                        'market_id': m,
                        'teams': t,
                        'create': ts['createdTime'],
                        'publish': ts['publishedTime']
                    }
                    self.pub.send_json(data)
                    
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    async def run(self):
        while True:
            try:
                await self.connect()
            except Exception as e:
                logger.error(f"Connection failed: {e}")
                await asyncio.sleep(5)  # Wait before reconnecting

async def main():
    feed = BaseballFeed()
    await feed.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")