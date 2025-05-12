import logging
import msgpack
from mitmproxy import http

def websocket_message(flow: http.HTTPFlow):
    assert flow.websocket is not None  # make type checker happy
    message = flow.websocket.messages[-1]

    try:
        decoded = msgpack.unpackb(message.content, raw=True)  # Keep strings as bytes
        print("Decoded MessagePack data (raw bytes):")
        print(decoded)

        # Convert only known text fields to str (optional)
        if isinstance(decoded, dict):
            decoded = {
                k: v.decode('utf-8') if isinstance(v, bytes) else v
                for k, v in decoded.items()
            }
            print("\nDecoded with UTF-8 strings:")
            print(decoded)

    except Exception as e:
        print(f"Error decoding MessagePack: {e}")
        logging.info(f"Server sent raw message: {message.content!r}")