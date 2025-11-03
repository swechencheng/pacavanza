from typing import List, Dict, Any, Optional
import pandas as pd


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
