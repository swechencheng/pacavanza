import os
import sys
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_FILE = PROJECT_ROOT / "config.json"

IBKR_HOST = "127.0.0.1"
IBKR_PORT = 7497
IBKR_CLIENT_ID = 3
IBKR_ACCOUNT = None

if CONFIG_FILE.exists():
    with open(CONFIG_FILE, "r") as f:
        try:
            _config = json.load(f)
            _ibkr_config = _config.get("IBKR", {})

            # Use "paper" as default, if --real is in sys.argv, use "real"
            use_real = "--real" in sys.argv
            mode = "real" if use_real else "paper"

            # fallback to legacy config structure if "real" and "paper" keys do not exist
            if "real" in _ibkr_config or "paper" in _ibkr_config:
                _mode_config = _ibkr_config.get(mode, {})
                if "host" in _mode_config:
                    IBKR_HOST = _mode_config["host"]
                if "port" in _mode_config:
                    IBKR_PORT = int(_mode_config["port"])
                if "client_id" in _mode_config:
                    IBKR_CLIENT_ID = int(_mode_config["client_id"])
                if "account" in _mode_config:
                    IBKR_ACCOUNT = _mode_config["account"]
            else:
                if "host" in _ibkr_config:
                    IBKR_HOST = _ibkr_config["host"]
                if "port" in _ibkr_config:
                    IBKR_PORT = int(_ibkr_config["port"])
                if "client_id" in _ibkr_config:
                    IBKR_CLIENT_ID = int(_ibkr_config["client_id"])
                if "account" in _ibkr_config:
                    IBKR_ACCOUNT = _ibkr_config["account"]
        except Exception as e:
            print(f"Error loading config.json: {e}")
