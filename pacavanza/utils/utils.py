import tempfile
import os
import json


def save_json_atomic(path: str, obj):
    """
    Safely write JSON `obj` to `path` atomically and fsync both file and directory (POSIX best-effort).
    """
    d = os.path.dirname(path) or "."
    # mkstemp ensures the temp file is on the same filesystem for atomic os.replace
    fd, tmp = tempfile.mkstemp(dir=d)
    try:
        # write text then fsync file
        with os.fdopen(fd, "w") as f:
            json.dump(obj, f, default=str)
            f.flush()
            os.fsync(f.fileno())
        # atomic rename
        os.replace(tmp, path)
        # try to fsync directory to make rename durable
        try:
            dirfd = os.open(d, os.O_DIRECTORY)
            try:
                os.fsync(dirfd)
            finally:
                os.close(dirfd)
        except Exception:
            # best-effort; ignore on platforms where not supported
            pass
    finally:
        # cleanup leftover temp file if something failed before rename
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except Exception:
                pass


def flatten_instrument_list(nested_data):
    """
    Flattens a hierarchical instrument list (Asset -> {Metadata, Instruments...})
    into a flat dictionary (InstrumentID -> InstrumentData) with inherited metadata.
    """
    flat = {}
    for _asset, data in nested_data.items():
        # Common inheritance fields
        common = {
            k: v
            for k, v in data.items()
            if k in ["timezone", "market_open", "market_close"]
        }
        for k, v in data.items():
            if isinstance(v, dict) and "orderbookId" in v:
                # It's an instrument
                merged = v.copy()
                # Merge inheritables if not present/override (usually parent is default)
                for ck, cv in common.items():
                    if ck not in merged:
                        merged[ck] = cv
                flat[k] = merged
    return flat


def find_key_by_orderbook_id(data, target_id):
    # Try flat lookup first (if data is already flattened or old format)
    for key, item in data.items():
        if isinstance(item, dict):
            if item.get("orderbookId") == target_id:
                return key
            # Check nested children
            for child_key, child_item in item.items():
                if (
                    isinstance(child_item, dict)
                    and child_item.get("orderbookId") == target_id
                ):
                    return child_key
    return None


def fetch_active_omxs30_future(roll_days_before_expiry: int = 5):
    """
    Fetch the active OMXS30 future from Avanza.

    Implements a roll-window: if the nearest (front-month) contract expires
    within ``roll_days_before_expiry`` calendar days, return the *next* contract
    instead.  This keeps the Avanza data stream in sync with IBKR, which
    automatically rolls to the back-month ~5 days before expiry.

    Args:
        roll_days_before_expiry: Number of calendar days before expiry at which
            we consider the front month "rolled".  Default is 5.
    """
    from curl_cffi import requests
    from datetime import datetime, timedelta

    url = "https://www.avanza.se/_api/market-option-future-forward-list/"
    headers = {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json;charset=UTF-8",
    }
    payload = {
        "filter": {
            "underlyingInstruments": [],
            "optionTypes": [],
            "endDates": [],
            "callIndicators": [],
        },
        "offset": 0,
        "limit": 20,
        "sortBy": {"field": "strikePrice", "order": "desc"},
    }

    response = requests.post(
        url, headers=headers, json=payload, impersonate="chrome110"
    )
    response.raise_for_status()
    data = response.json()

    futures = data.get("futureForwards", [])
    if not futures:
        raise ValueError("No future contracts found")

    today = datetime.now().date()
    roll_cutoff = today + timedelta(days=roll_days_before_expiry)
    import re

    valid_futures = [
        f
        for f in futures
        if f["endDate"] >= today.isoformat()
        and f["name"].startswith("OMXS30")
        and re.fullmatch(r"OMXS30\d+[A-Z]", f["name"])
    ]

    if not valid_futures:
        valid_futures = futures  # fallback

    # Sort by endDate ascending (nearest first)
    valid_futures.sort(key=lambda x: x["endDate"])

    # Apply roll-window: if the front month expires within roll_days_before_expiry
    # days, skip it and use the next contract (the back month).
    active_future = valid_futures[0]
    front_expiry = datetime.strptime(active_future["endDate"], "%Y-%m-%d").date()
    if front_expiry <= roll_cutoff and len(valid_futures) > 1:
        active_future = valid_futures[1]

    return {
        "OMXS30": {
            "timezone": "Europe/Stockholm",
            "market_open": "09:00",
            "market_close": "17:45",
            active_future["name"].lower(): {
                "name": active_future["name"],
                "orderbookId": str(active_future["orderbookId"]),
                "tick_size": 0.25,
                "tick_coefficient": 1.0,
            },
        }
    }
