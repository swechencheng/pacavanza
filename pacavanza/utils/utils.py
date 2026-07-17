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


def fetch_active_omxs30_future(
    roll_days_before_expiry: int = 5, target_name: str = None
):
    """
    Fetch the active OMXS30 future from Avanza.

    Implements a roll-window: if the nearest (front-month) contract expires
    within ``roll_days_before_expiry`` calendar days, return the *next* contract
    instead.  This keeps the Avanza data stream in sync with IBKR, which
    automatically rolls to the back-month ~5 days before expiry.

    Args:
        roll_days_before_expiry: Number of calendar days before expiry at which
            we consider the front month "rolled".  Default is 5.
        target_name: Optional explicit contract name (e.g. "OMXS306G"). If provided,
            this exact contract will be selected instead of applying the roll logic.
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
        url, headers=headers, json=payload, impersonate="chrome110", timeout=10
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

    active_future = None
    if target_name:
        for f in valid_futures:
            if f["name"].upper() == target_name.upper():
                active_future = f
                break
        if not active_future:
            # Fallback if target_name not found
            active_future = valid_futures[0]
    else:
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


def is_outside_market_hours(
    start_time, tz_name: str, open_str: str, close_str: str
) -> bool:
    """Return True if start_time (UTC datetime) falls before market open or at/after market close."""
    from zoneinfo import ZoneInfo
    from datetime import timezone

    try:
        zone = ZoneInfo(tz_name)
    except Exception:
        zone = timezone.utc

    try:
        oh, om = (int(x) for x in open_str.split(":"))
        ch, cm = (int(x) for x in close_str.split(":"))
    except Exception:
        return False

    local = start_time.astimezone(zone)
    if local.weekday() >= 5:
        return True
    t = (local.hour, local.minute)
    return t < (oh, om) or t >= (ch, cm)


def fetch_avanza_chart_history(
    orderbook_id: str,
    interval_seconds: int = 300,
    tz_name: str = "Europe/Stockholm",
    open_str: str = "09:00",
    close_str: str = "17:25",
) -> list:
    """
    Fetch the price chart from Avanza for today, parse into our OHLC format,
    and filter out bars outside of regular market hours.
    Returns a list of dicts with datetime start_time/end_time.
    """
    try:
        from curl_cffi import requests
    except ImportError:
        return []

    from datetime import datetime, timezone, timedelta

    interval_map = {
        60: "minute",
        120: "two_minutes",
        300: "five_minutes",
        600: "ten_minutes",
        1800: "thirty_minutes",
        3600: "hour",
    }
    resolution = interval_map.get(interval_seconds, "five_minutes")

    from datetime import datetime, timezone, timedelta

    today = datetime.now()
    # Avanza only allows five minutes bar on two trading days range
    from_date = today - timedelta(days=1)

    today_str = today.strftime("%Y-%m-%d")
    from_str = from_date.strftime("%Y-%m-%d")

    url = f"https://www.avanza.se/_api/price-chart/stock/{orderbook_id}?from={from_str}&to={today_str}&resolution={resolution}"
    try:
        response = requests.get(url, impersonate="chrome110", timeout=10)
        if response.status_code != 200:
            return []
        data = response.json()
    except Exception:
        return []

    ohlc_data = data.get("ohlc", [])
    new_bars = []

    for row in ohlc_data:
        start_time = datetime.fromtimestamp(row["timestamp"] / 1000.0, tz=timezone.utc)
        if is_outside_market_hours(start_time, tz_name, open_str, close_str):
            continue

        end_time = start_time + timedelta(seconds=interval_seconds)
        bar = {
            "start_time": start_time,
            "end_time": end_time,
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(row["totalVolumeTraded"]),
        }
        new_bars.append(bar)

    return new_bars
