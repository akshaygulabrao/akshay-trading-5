import logging
import msgpack
import os
import logging
import zmq
import utils
from collections import defaultdict

logging.disable(logging.CRITICAL)
from mitmproxy import http

context_golf = zmq.Context()
pub_golf = context_golf.socket(zmq.PUB)
pub_golf.bind("ipc:///tmp/draftkings.ipc")

def websocket_message(flow: http.HTTPFlow):
    assert flow.websocket is not None  # make type checker happy
    message = flow.websocket.messages[-1]
    if message.type == 2:
        decoded = msgpack.unpackb(message.content, raw=False)
        if decoded[1] == "update":
            market_info = decoded[2][0][2][0]
            market_name = market_info[1][1]
            sport_id = market_info[1][5]
            if market_name in ["Winner", "Moneyline"]:
                players = market_info[1][8]
                players_critical = []
                for p in players:
                    players_critical.append((p[1],int(p[3])))
                print(players_critical)
                pub_golf.send_json({"sport": sport_id, "players": players_critical})
                print(utils.now("KLAX").time())
        
    else:
        print(message.text.splitlines()[-1])