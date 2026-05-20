import json
import os
from avanza import Avanza

_AVANZA_INSTANCE = None
_ACCOUNT_ID = None

def _init_avanza():
    global _AVANZA_INSTANCE, _ACCOUNT_ID
    if _AVANZA_INSTANCE is None:
        secrets_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../secret.json"))
        with open(secrets_path, "r") as f:
            secrets = json.load(f)
        try:
            _AVANZA_INSTANCE = Avanza(secrets)
            _ACCOUNT_ID = secrets["accountId"]
        except Exception as e:
            import time
            import logging
            logging.basicConfig(level=logging.INFO)
            logger = logging.getLogger("avanza_init")
            logger.error(f"Failed to initialize Avanza: {e}. Sleeping 30s to prevent rapid restarts/TOTP reuse...")
            time.sleep(30)
            raise e

def get_avanza() -> Avanza:
    _init_avanza()
    return _AVANZA_INSTANCE

def get_account_id() -> str:
    _init_avanza()
    return _ACCOUNT_ID
