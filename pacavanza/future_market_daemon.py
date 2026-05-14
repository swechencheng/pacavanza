import logging
import sys
from datetime import datetime, timezone

from .base_market_collector import BaseMarketCollector
from .modules.instrument_data import INTERVAL_MAP

logging.basicConfig(level=logging.INFO)
logging.getLogger("future_market_daemon").setLevel(logging.INFO)
LOGGER = logging.getLogger("future_market_daemon")

from .utils.utils import fetch_active_omxs30_future

class FutureMarketCollector(BaseMarketCollector):
    """
    SSE collector for classic futures (e.g. OMXS306D) using Avanza's
    quote-web-push endpoint.

    Inherits all infrastructure from BaseMarketCollector and provides:
      - quote-web-push specific SSE URL
      - _sse_callback handling buyPrice/sellPrice/lastPrice/updated fields

    Quote event example payload:
    {
        "orderbookId": "2279188",
        "buyPrice": 2863.00,
        "sellPrice": 2863.50,
        "closingPrice": 2856.00,
        "highestPrice": 2876.50,
        "lowestPrice": 2837.50,
        "lastPrice": 2863.25,
        "totalValueTraded": 69291945,
        "totalVolumeTraded": 24248,
        "change": 7.25,
        "changePercent": 0.0025,
        "spreadPercent": 0.0002,
        "volumeWeightedAveragePrice": null,
        "updated": "2026-03-30T10:02:31.024Z",
        "lastPriceUpdated": "2026-03-30T10:02:30.000Z"
    }
    """

    sse_base_url = "https://www.avanza.se/_push/quote-web-push/"
    default_redis_channel = "pacavanza:future_updates"
    logger_name = "future_market_daemon"

    def __init__(self, interval_seconds, **kwargs):
        active_future = fetch_active_omxs30_future()
        super().__init__(interval_seconds, instrument_list=active_future, **kwargs)

    def _init_price_tracking(self):
        self.last_buy_price = {sid: None for sid in self.instrument_ids}
        self.last_sell_price = {sid: None for sid in self.instrument_ids}
        self.last_price = {sid: None for sid in self.instrument_ids}

    async def _sse_callback(self, instrument_id, _id, event, data):
        """
        Async callback for quote-web-push SSE events.
        Handles quote data with buyPrice, sellPrice, lastPrice, and updated.
        """
        try:
            if event != "QUOTE" or not isinstance(data, dict):
                return

            self.logger.debug(f"[{instrument_id}] [{event}] {data}")

            ts = data.get("updated")
            dt = datetime.now(timezone.utc)
            readable_ts = "(no timestamp)"
            if ts:
                try:
                    parsed = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f%z")
                    dt = parsed.astimezone(timezone.utc)
                    milli = dt.microsecond // 1000
                    readable_ts = f"{dt.strftime('%Y-%m-%d %H:%M:%S')}.{milli:03d} {dt.strftime('%Z')}"
                except Exception:
                    readable_ts = str(ts)

            buy_price = data.get("buyPrice")
            sell_price = data.get("sellPrice")
            last_price = data.get("lastPrice")

            if last_price is None:
                self.logger.warning(
                    f"[{instrument_id}] {readable_ts} - lastPrice missing: {data}"
                )
                return

            self.logger.debug(
                f"[{instrument_id}] {readable_ts} B: {buy_price}  S: {sell_price}  L: {last_price:.2f}"
            )

            # Store last prices
            self.last_buy_price[instrument_id] = buy_price
            self.last_sell_price[instrument_id] = sell_price
            self.last_price[instrument_id] = last_price

            # Update OHLC bar using lastPrice
            self.instrument_data[instrument_id].update_ohlc_bar(last_price, dt)

            # Mark current bar as dirty for periodic snapshot
            self._dirty_current.add(instrument_id)

            # Publish bar update to Redis
            await self._publish_bar_update(
                instrument_id,
                extra_meta={
                    "last_buy": self.last_buy_price[instrument_id],
                    "last_sell": self.last_sell_price[instrument_id],
                    "last_price": self.last_price[instrument_id],
                },
            )

        except Exception as e:
            self.logger.exception(f"[{instrument_id}] Exception in quote callback: {e}")

    def load_all_ohlc_from_disk(self):
        super().load_all_ohlc_from_disk()
        self._sync_ibkr_history()

    def _sync_ibkr_history(self):
        try:
            from ib_async import IB, ContFuture, util
            import pandas as pd
            from datetime import timedelta
        except ImportError:
            self.logger.warning("ib_async or pandas not installed, skipping IBKR sync")
            return
            
        for sid, sd in self.instrument_data.items():
            try:
                ib = IB()
                # 7497 is default for TWS paper trading. We use a random client ID to avoid conflicts.
                ib.connect('127.0.0.1', 7497, clientId=999)
                contract = ContFuture('OMXS30', 'OMS')
                ib.qualifyContracts(contract)
                
                # Determine barSizeSetting based on interval_seconds
                interval_map = {60: '1 min', 300: '5 mins', 900: '15 mins', 3600: '1 hour'}
                bar_size = interval_map.get(self.interval_seconds, '5 mins')
                
                bars = ib.reqHistoricalData(
                    contract,
                    endDateTime='',
                    durationStr='1 M',
                    barSizeSetting=bar_size,
                    whatToShow='TRADES',
                    useRTH=False,
                    formatDate=2 # Return UTC timestamps
                )
                df = util.df(bars)
                ib.disconnect()
                
                if df is None or df.empty:
                    self.logger.warning(f"[{sid}] No IBKR data for ContFuture OMXS30")
                    continue

                new_bars = []
                for idx, row in df.iterrows():
                    # idx is not the index here if we didn't set it, date is a column
                    start_time = row['date']
                    if start_time.tzinfo is None:
                        start_time = start_time.replace(tzinfo=timezone.utc)
                    else:
                        start_time = start_time.astimezone(timezone.utc)
                    
                    end_time = start_time + timedelta(seconds=self.interval_seconds)
                    
                    bar = {
                        "start_time": start_time,
                        "end_time": end_time,
                        "open": float(row["open"]),
                        "high": float(row["high"]),
                        "low": float(row["low"]),
                        "close": float(row["close"]),
                        "volume": float(row["volume"])
                    }
                    new_bars.append(bar)
                
                with sd.lock:
                    local_bars = sd.completed_ohlc.get(sid, [])
                    merged_bars_dict = {b["start_time"]: b for b in local_bars}
                    
                    for b in new_bars:
                        merged_bars_dict[b["start_time"]] = b
                        
                    if new_bars:
                        last_ib_start = new_bars[-1]["start_time"]
                        for b in local_bars:
                            if b["start_time"] > last_ib_start:
                                merged_bars_dict[b["start_time"]] = b
                                
                    sorted_bars = sorted(merged_bars_dict.values(), key=lambda x: x["start_time"])
                    cutoff = datetime.now(timezone.utc) - timedelta(hours=sd.max_history_hours)
                    sd.completed_ohlc[sid] = [b for b in sorted_bars if b["end_time"] >= cutoff]
                    
                self.logger.info(f"[{sid}] Synced {len(new_bars)} bars from IBKR ContFuture. Total bars: {len(sd.completed_ohlc[sid])}")
                self.force_save_instrument(sid)

            except Exception as e:
                self.logger.exception(f"[{sid}] Failed to sync IBKR history: {e}")


def parse_args():
    """
    Usage:
      python -m pacavanza.future_market_daemon [-i interval]
    """
    args = sys.argv[1:]
    interval_str = "5m"
    i = 0
    while i < len(args):
        if args[i] == "-i" and i + 1 < len(args):
            val = args[i + 1]
            if val not in INTERVAL_MAP:
                LOGGER.error(f"Invalid interval '{val}'")
                sys.exit(1)
            interval_str = val
            i += 2
        else:
            LOGGER.debug(
                f"Ignoring CLI arg '{args[i]}' (instrument ids loaded from dynamically fetched future list)"
            )
            i += 1

    return interval_str


if __name__ == "__main__":
    interval_str = parse_args()
    interval_seconds = INTERVAL_MAP[interval_str]
    collector = FutureMarketCollector(interval_seconds)
    collector.run()
