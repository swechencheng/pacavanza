from typing import List, Dict, Any, Optional
import pandas as pd


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


def calculate_volume_sma(volumes: List[float], length: int = 20) -> List[float]:
    """
    Calculate simple moving average of volume over `length` periods.
    For bars with fewer than `length` prior bars, computes SMA over available bars.
    """
    if not volumes:
        return []
    s = pd.Series(volumes, dtype=float)
    sma = s.rolling(window=length, min_periods=1).mean()
    return [float(x) for x in sma]


def calculate_rvol(volumes: List[float], length: int = 20) -> List[float]:
    """
    Calculate Relative Volume (RVOL) for each bar by comparing current volume
    to the moving average of volume over the preceding `length` bars (TradingView standard).
    RVOL = volume[i] / SMA(volume[i-length .. i-1]).
    If i == 0 or prior volume average is 0, defaults to 1.0.
    """
    if not volumes:
        return []
    result = []
    for i in range(len(volumes)):
        curr_vol = float(volumes[i])
        if i == 0:
            result.append(1.0)
            continue
        lookback = min(length, i)
        prior_slice = volumes[i - lookback : i]
        prior_avg = (
            sum(float(v) for v in prior_slice) / lookback if lookback > 0 else 0.0
        )
        if prior_avg > 0:
            result.append(round(curr_vol / prior_avg, 2))
        else:
            result.append(1.0)
    return result


def incremental_rvol_update(
    current_volume: float, prev_volumes: List[float], length: int = 20
) -> float:
    """
    Calculate live RVOL for forming bar given current volume and previous bars' volumes.
    """
    if not prev_volumes:
        return 1.0
    lookback = min(length, len(prev_volumes))
    prior_slice = prev_volumes[-lookback:]
    prior_avg = sum(float(v) for v in prior_slice) / lookback if lookback > 0 else 0.0
    if prior_avg > 0:
        return round(float(current_volume) / prior_avg, 2)
    return 1.0


def calculate_rvol_tod(
    bars: List[Dict[str, Any]], timezone_str: str = "Europe/Stockholm"
) -> List[float]:
    """
    Calculate Relative Volume at Time (RVAT / Time-of-Day RVOL) per SMB Capital methodology.
    Compares each bar's volume to the average volume of that same time-of-day (HH:MM)
    across the provided dataset.
    """
    if not bars:
        return []
    from datetime import datetime
    from zoneinfo import ZoneInfo

    tz = ZoneInfo(timezone_str)
    # 1. Group volumes by HH:MM
    tod_groups: Dict[str, List[float]] = {}
    bar_tod_keys: List[str] = []

    for b in bars:
        st_raw = b.get("start_time")
        if isinstance(st_raw, str):
            dt = datetime.fromisoformat(st_raw)
        elif isinstance(st_raw, datetime):
            dt = st_raw
        else:
            bar_tod_keys.append("")
            continue
        local_dt = dt.astimezone(tz)
        hhmm = f"{local_dt.hour:02d}:{local_dt.minute:02d}"
        bar_tod_keys.append(hhmm)
        vol = float(b.get("volume", 0))
        tod_groups.setdefault(hhmm, []).append(vol)

    # 2. Compute averages per HH:MM
    tod_averages = {
        hhmm: sum(vols) / len(vols) for hhmm, vols in tod_groups.items() if vols
    }

    # 3. Calculate RVOL at time for each bar
    results = []
    for i, b in enumerate(bars):
        hhmm = bar_tod_keys[i]
        vol = float(b.get("volume", 0))
        avg = tod_averages.get(hhmm, 0.0)
        if avg > 0:
            results.append(round(vol / avg, 2))
        else:
            results.append(1.0)
    return results


def calculate_cumulative_rvol(
    bars: List[Dict[str, Any]],
    length_days: int = 20,
    timezone_str: str = "Europe/Stockholm",
) -> List[float]:
    """
    Calculate TradingView Relative Volume at Time (Rel vol) with:
      - anchor timeframe: 1D
      - calculation mode: Cumulative
      - length: length_days (default 20)

    For each bar t on day D:
      currentVolume = cumulative volume from day D session open up to bar t.
      pastVolume = mean of cumulative volume up to that same time-of-day (HH:MM)
                   over the preceding length_days trading days.
      ratio = currentVolume / pastVolume.
    """
    if not bars:
        return []
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from collections import defaultdict

    tz = ZoneInfo(timezone_str)

    # 1. Group bars by day
    day_bars = defaultdict(list)
    bar_day_and_hhmm = []

    for b in bars:
        st_raw = b.get("start_time")
        if isinstance(st_raw, str):
            dt = datetime.fromisoformat(st_raw)
        elif isinstance(st_raw, datetime):
            dt = st_raw
        else:
            bar_day_and_hhmm.append(("", ""))
            continue
        local_dt = dt.astimezone(tz)
        day_str = local_dt.strftime("%Y-%m-%d")
        hhmm = f"{local_dt.hour:02d}:{local_dt.minute:02d}"
        vol = float(b.get("volume", 0))
        day_bars[day_str].append({"hhmm": hhmm, "volume": vol})
        bar_day_and_hhmm.append((day_str, hhmm))

    sorted_days = sorted(day_bars.keys())
    day_index_map = {d: i for i, d in enumerate(sorted_days)}

    # 2. Cumulative volume up to each hhmm on each day
    day_cum_vol = {}
    for d in sorted_days:
        cum = 0.0
        cum_map = {}
        for item in day_bars[d]:
            cum += item["volume"]
            cum_map[item["hhmm"]] = cum
        day_cum_vol[d] = cum_map

    # 3. Calculate ratio for each bar
    results = []
    for d, hhmm in bar_day_and_hhmm:
        if not d:
            results.append(1.0)
            continue

        cur_cum = day_cum_vol[d].get(hhmm, 0.0)
        idx = day_index_map[d]
        prior_days = sorted_days[max(0, idx - length_days) : idx]

        if not prior_days:
            results.append(1.0)
            continue

        prior_cums = []
        for pd in prior_days:
            pd_map = day_cum_vol[pd]
            if hhmm in pd_map:
                prior_cums.append(pd_map[hhmm])
            else:
                times_le = [t for t in pd_map.keys() if t <= hhmm]
                if times_le:
                    prior_cums.append(pd_map[max(times_le)])

        if prior_cums:
            avg_past = sum(prior_cums) / len(prior_cums)
            ratio = cur_cum / avg_past if avg_past > 0 else 1.0
            results.append(round(ratio, 2))
        else:
            results.append(1.0)

    return results


def incremental_cumulative_rvol_update(
    today_cumulative_volume: float,
    current_hhmm: str,
    prior_days_cum_map: Dict[str, Dict[str, float]],
    length_days: int = 20,
) -> float:
    """
    Calculate live cumulative relative volume ratio for forming bar.
    """
    if not prior_days_cum_map:
        return 1.0

    prior_days = sorted(prior_days_cum_map.keys())[-length_days:]
    prior_cums = []
    for pd in prior_days:
        pd_map = prior_days_cum_map[pd]
        if current_hhmm in pd_map:
            prior_cums.append(pd_map[current_hhmm])
        else:
            times_le = [t for t in pd_map.keys() if t <= current_hhmm]
            if times_le:
                prior_cums.append(pd_map[max(times_le)])

    if prior_cums:
        avg_past = sum(prior_cums) / len(prior_cums)
        if avg_past > 0:
            return round(today_cumulative_volume / avg_past, 2)
    return 1.0
