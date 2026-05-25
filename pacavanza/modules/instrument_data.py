import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone

INTERVAL_MAP = {"10s": 10, "1m": 60, "5m": 300, "15m": 900, "1h": 3600}


class InstrumentData:
    def __init__(
        self, interval_seconds: int, instrument_id: str = "TEST", max_history_hours: int = 720
    ):
        self.interval_seconds = interval_seconds
        self.instrument_id = instrument_id
        self.max_history_hours = max_history_hours
        self.current_bars = defaultdict(dict)
        self.completed_ohlc = defaultdict(list)
        self.lock = threading.Lock()

    def initialize_new_bar(self, timestamp: datetime, price: float):
        # Ensure timestamp is timezone-aware in UTC
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        else:
            timestamp = timestamp.astimezone(timezone.utc)

        seconds_since_midnight = (
            timestamp.hour * 3600 + timestamp.minute * 60 + timestamp.second
        )
        floored = (
            seconds_since_midnight // self.interval_seconds
        ) * self.interval_seconds
        bar_start = timestamp.replace(
            hour=0, minute=0, second=0, microsecond=0
        ) + timedelta(seconds=floored)

        # Keep bar_start and end_time timezone-aware (UTC)
        self.current_bars[self.instrument_id] = {
            "start_time": bar_start,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "end_time": bar_start + timedelta(seconds=self.interval_seconds),
        }

    def update_ohlc_bar(self, price: float, timestamp: datetime):
        with self.lock:
            current_bar = self.current_bars.get(self.instrument_id)
            if current_bar is None or timestamp >= current_bar["end_time"]:
                if current_bar is not None:
                    # Only append if current_bar is chronologically newer than the last completed bar
                    lst = self.completed_ohlc[self.instrument_id]
                    if not lst or current_bar["start_time"] > lst[-1]["start_time"]:
                        self.completed_ohlc[self.instrument_id].append(current_bar.copy())
                        cutoff = datetime.now(timezone.utc) - timedelta(
                            hours=self.max_history_hours
                        )
                        self.completed_ohlc[self.instrument_id] = [
                            bar
                            for bar in self.completed_ohlc[self.instrument_id]
                            if bar["end_time"] >= cutoff
                        ]
                self.initialize_new_bar(timestamp, price)
            else:
                current_bar["high"] = max(current_bar["high"], price)
                current_bar["low"] = min(current_bar["low"], price)
                current_bar["close"] = price

