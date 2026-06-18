import json
import os
import time
import logging
import requests
from avanza import Avanza

_AVANZA_INSTANCE = None
_ACCOUNT_ID = None

SESSION_FILE = "/tmp/pacavanza_session.json"

def _init_avanza():
    global _AVANZA_INSTANCE, _ACCOUNT_ID
    if _AVANZA_INSTANCE is not None:
        return

    # Try to load from session file to avoid TOTP reuse and fork requirements
    if os.path.exists(SESSION_FILE):
        try:
            with open(SESSION_FILE, "r") as f:
                data = json.load(f)
            
            # Reconstruct Avanza without calling __init__ (bypasses login)
            avanza = Avanza.__new__(Avanza)
            avanza._retry_with_next_otp = True
            avanza._quiet = False
            avanza._authenticationTimeout = 60 * 24
            avanza._session = requests.Session()
            for k, v in data.get("cookies", {}).items():
                avanza._session.cookies.set(k, v)
                
            avanza._security_token = data.get("security_token")
            avanza._authentication_session = data.get("authentication_session")
            avanza._push_subscription_id = data.get("push_subscription_id")
            avanza._customer_id = data.get("customer_id")
            
            _AVANZA_INSTANCE = avanza
            _ACCOUNT_ID = data.get("account_id")
            
            # Simple health check to verify the session is still valid
            try:
                # Making a simple overview call to ensure the session is alive
                _AVANZA_INSTANCE.get_overview()
                return
            except Exception:
                # If it fails, we fall through to do a full login
                _AVANZA_INSTANCE = None
                _ACCOUNT_ID = None
        except Exception:
            pass

    secrets_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../secret.json"))
    with open(secrets_path, "r") as f:
        secrets = json.load(f)
    try:
        _AVANZA_INSTANCE = Avanza(secrets)
        _ACCOUNT_ID = secrets["accountId"]
        
        # Save session data for child processes
        data = {
            "cookies": _AVANZA_INSTANCE._session.cookies.get_dict(),
            "security_token": _AVANZA_INSTANCE._security_token,
            "authentication_session": getattr(_AVANZA_INSTANCE, "_authentication_session", None),
            "push_subscription_id": getattr(_AVANZA_INSTANCE, "_push_subscription_id", None),
            "customer_id": getattr(_AVANZA_INSTANCE, "_customer_id", None),
            "account_id": _ACCOUNT_ID
        }
        with open(SESSION_FILE, "w") as f:
            json.dump(data, f)
            
    except Exception as e:
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
