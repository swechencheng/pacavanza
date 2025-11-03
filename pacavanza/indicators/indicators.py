from typing import List, Dict, Any, Optional
import pandas as pd
from datetime import timedelta
from zoneinfo import ZoneInfo


def compute_emas_from_bars(bars: List[Dict[str, Any]], lengths=[20, 50, 100, 220]):
    """
    bars: list of dicts with 'start_time' (aware dt), 'end_time', 'open','high','low','close','volume'
    returns: dict length -> list of {time: iso, value: float} aligned with bars' end_time
    """
    if not bars:
        return {l: [] for l in lengths}
    df = pd.DataFrame(bars)
    # ensure close and time are present
    df["close"] = df["close"].astype(float)
    # use end_time as the timestamp for series
    df["time"] = pd.to_datetime(df["end_time"])
    df = df.sort_values("time")
    out = {}
    close = df["close"]
    for L in lengths:
        # pandas ewm alpha = 2/(L+1) with adjust=False produces same recursive EMA
        ema_series = close.ewm(span=L, adjust=False).mean()
        out[L] = [
            {"time": t.isoformat(), "value": float(v)}
            for t, v in zip(
                df["time"].dt.tz_localize(None).tolist(), ema_series.tolist()
            )
        ]
    return out


def incremental_ema_update(
    prev_value: Optional[float],
    close: float,
    length: int,
    historical_buffer: Optional[List[Dict[str, Any]]] = None,
) -> Optional[float]:
    """
    Returns the EMA after incorporating `close`. If prev_value is None,
    will compute EMA over historical_buffer + [close] using pandas ewm to
    reproduce compute_emas_from_bars(..., adjust=False).
    Returns None if no valid closes available to compute.
    """
    alpha = 2.0 / (length + 1)

    if prev_value is None:
        # Need to initialize from history. If no history - return None (not safe to guess).
        if not historical_buffer:
            return None

        # Build closes list from historical_buffer, preserving order (old -> new)
        closes = [float(b["close"]) for b in historical_buffer if "close" in b]
        if not closes:
            return None

        # Append the new close and compute ewm exactly as compute_emas_from_bars uses pandas
        s = pd.Series(closes + [float(close)])
        ema = s.ewm(span=length, adjust=False).mean().iloc[-1]
        return float(ema)

    # Standard incremental formula when prev_value is available
    ema = (float(close) - float(prev_value)) * alpha + float(prev_value)
    return float(ema)


def generate_bar_group_label_incremental(
    stock_id: str,
    bars: List[Dict[str, Any]],
    state: Dict[str, Dict[str, Any]],
    meta: Optional[Dict[str, Any]] = None,
    tf_str="5",
    c_contador=2,
    use_rth_hours=True,
):
    """
    Generate a single label if appropriate for the latest bar given the state.
    state: dictionary used to keep persistent per-symbol counters across calls.
      state[stock_id] is expected to be a dict that may contain:
         - 'count' (int)
         - 'bar_group_count' (int)
         - 'last_date' (date)
    Returns label dict {'time': iso, 'text': str} or None.
    """
    if not bars:
        return None
    last = bars[-1]
    dt = last["start_time"]
    st = state.setdefault(stock_id, {})

    def is_trading_hour(bar_dt):
        if not use_rth_hours or not meta:
            return True
        tzname = meta.get("timezone")
        if not tzname:
            return True
        zone = ZoneInfo(tzname)
        local_dt = bar_dt.astimezone(zone)
        opening = meta.get("market_open", "00:00")
        closing = meta.get("market_close", "23:59")
        try:
            oh, om = (int(x) for x in opening.split(":"))
            ch, cm = (int(x) for x in closing.split(":"))
        except Exception:
            return True
        s = local_dt.replace(hour=oh, minute=om, second=0, microsecond=0)
        e = local_dt.replace(hour=ch, minute=cm, second=0, microsecond=0)
        if e <= s:
            e = e + timedelta(days=1)
        return (local_dt >= s) and (local_dt < e)

    prev_date = st.get("last_date")
    if prev_date is None or dt.date() != prev_date:
        st["count"] = 1
        st["bar_group_count"] = 1
    else:
        if use_rth_hours:
            if is_trading_hour(dt):
                st["count"] = st.get("count", 1) + 1
                if tf_str == "1":
                    if st["count"] % 5 == 1:
                        st["bar_group_count"] = st.get("bar_group_count", 1) + 1
                else:
                    st["bar_group_count"] = st.get("count", 1)
            else:
                # outside rth, no increment
                pass
        else:
            st["count"] = st.get("count", 1) + 1
            if tf_str == "1":
                if st["count"] % 5 == 1:
                    st["bar_group_count"] = st.get("bar_group_count", 1) + 1
            else:
                st["bar_group_count"] = st.get("count", 1)

    st["last_date"] = dt.date()

    emit = False
    if (not use_rth_hours) or (use_rth_hours and is_trading_hour(dt)):
        if (tf_str == "5" and (st["bar_group_count"] % c_contador == 0)) or (
            tf_str == "1" and (st["count"] % 5 == 1)
        ):
            emit = True

    if emit:
        return {"time": dt.isoformat(), "text": str(st["bar_group_count"])}
    return None
