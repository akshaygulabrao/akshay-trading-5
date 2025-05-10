import os
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization
import asyncio

from original.clients import KalshiHttpClient, KalshiWebSocketClient, Environment

def setup_prod():
    """
    Returns (KEYID, private_key, env) for production setup.
    """
    # Add your production setup code here
    load_dotenv()
    env = Environment.PROD# toggle environment here
    KEYID = os.getenv('DEMO_KEYID') if env == Environment.DEMO else os.getenv('PROD_KEYID')
    KEYFILE = os.getenv('DEMO_KEYFILE') if env == Environment.DEMO else os.getenv('PROD_KEYFILE')

    try:
        with open(KEYFILE, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None  # Provide the password if your key is encrypted
            )
    except FileNotFoundError:
        raise FileNotFoundError(f"Private key file not found at {KEYFILE}")
    except Exception as e:
        raise Exception(f"Error loading private key: {str(e)}")
    
    return KEYID, private_key, env
