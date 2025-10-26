import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import copy

INTERVAL_MAP = {"10s": 10, "1m": 60, "5m": 300, "15m": 900, "1h": 3600}


class StockData:
    def __init__(self, interval_seconds: int, stock_id: str = "TEST", max_history_hours: int = 96):
        self.interval_seconds = interval_seconds
        self.stock_id = stock_id
        self.max_history_hours = max_history_hours
        self.current_bars = defaultdict(dict)
        self.completed_ohlc = defaultdict(list)
        self.lock = threading.Lock()

    def initialize_new_bar(self, timestamp: datetime, price: float):
        seconds_since_midnight = (
            timestamp.hour * 3600 + timestamp.minute * 60 + timestamp.second
        )
        floored = (
            seconds_since_midnight // self.interval_seconds
        ) * self.interval_seconds
        bar_start = timestamp.replace(
            hour=0, minute=0, second=0, microsecond=0
        ) + timedelta(seconds=floored)

        self.current_bars[self.stock_id] = {
            "start_time": bar_start,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "end_time": bar_start + timedelta(seconds=self.interval_seconds),
        }

    def update_ohlc_bar(self, price: float, timestamp: datetime):
        with self.lock:
            current_bar = self.current_bars.get(self.stock_id)
            if current_bar is None or timestamp >= current_bar["end_time"]:
                if current_bar is not None:
                    self.completed_ohlc[self.stock_id].append(current_bar.copy())
                    cutoff = datetime.now(timezone.utc) - timedelta(
                        hours=self.max_history_hours
                    )
                    self.completed_ohlc[self.stock_id] = [
                        bar
                        for bar in self.completed_ohlc[self.stock_id]
                        if bar["end_time"] >= cutoff
                    ]
                self.initialize_new_bar(timestamp, price)
            else:
                current_bar["high"] = max(current_bar["high"], price)
                current_bar["low"] = min(current_bar["low"], price)
                current_bar["close"] = price

    def get_dataframes(self):
        """Thread-safe deep copy for use in Dash chart updates."""
        with self.lock:
            completed = copy.deepcopy(self.completed_ohlc[self.stock_id])
            current = copy.deepcopy(self.current_bars.get(self.stock_id))
        return completed, current
