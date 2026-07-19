import os
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_FILE = PROJECT_ROOT / "config.json"

IBKR_HOST = "127.0.0.1"
IBKR_PORT = 7497
IBKR_CLIENT_ID = 3

if CONFIG_FILE.exists():
    with open(CONFIG_FILE, "r") as f:
        try:
            _config = json.load(f)
            _ibkr_config = _config.get("IBKR", {})
            if "host" in _ibkr_config:
                IBKR_HOST = _ibkr_config["host"]
            if "port" in _ibkr_config:
                IBKR_PORT = int(_ibkr_config["port"])
            if "client_id" in _ibkr_config:
                IBKR_CLIENT_ID = int(_ibkr_config["client_id"])
        except Exception as e:
            print(f"Error loading config.json: {e}")
