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
pub_golf.bind("ipc:///tmp/draftkings_golf.ipc")


def websocket_message(flow: http.HTTPFlow):
    assert flow.websocket is not None  # make type checker happy
    message = flow.websocket.messages[-1]
    if message.type == 2:
        decoded = msgpack.unpackb(message.content, raw=False)
        print(decoded)
        market_info = decoded[2][0][2][0]
        market_name = market_info[1][1]
        if market_name == "Winner": # GOLF
            os.system("clear")  
            players = market_info[1][8]
            for p in players[:10]:
                print(p[1],int(p[3]))
            players_critical = defaultdict(lambda : int)
            for p in players:
                players_critical[p[1]] = int(p[3])
            pub_golf.send_json(players_critical)
            print(utils.now())
        
    else:
        print(message.text.splitlines()[-1])