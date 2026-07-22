import json
import logging
import sys
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

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
    depth_sse_base_url = "https://www.avanza.se/_push/order-depth-web-push/"
    trade_sse_base_url = "https://www.avanza.se/_push/trade-web-push/"
    default_redis_channel = "pacavanza:future_updates"
    logger_name = "future_market_daemon"

    def __init__(self, interval_seconds, **kwargs):
        from pacavanza.modules.ibkr_client_instance import resolve_ibkr_local_symbol

        ibkr_local_symbol = resolve_ibkr_local_symbol()
        if ibkr_local_symbol:
            LOGGER.info(f"Resolved IBKR active future: {ibkr_local_symbol}")
        else:
            LOGGER.warning(
                "Failed to resolve IBKR active future, falling back to Avanza roll logic"
            )

        active_future = fetch_active_omxs30_future(target_name=ibkr_local_symbol)
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

            if not hasattr(self, "_prev_last_price"):
                self._prev_last_price = {}

            prev_price = self._prev_last_price.get(instrument_id)
            self._prev_last_price[instrument_id] = last_price

            # Only proceed if market is open
            if not self.is_market_open(instrument_id, dt):
                self.logger.debug(
                    f"[{instrument_id}] Outside market hours ({readable_ts}), ignoring price update."
                )
                return

            # Store last prices
            self.last_buy_price[instrument_id] = buy_price
            self.last_sell_price[instrument_id] = sell_price
            self.last_price[instrument_id] = last_price

            # To avoid capturing the pre-market stale price exactly at market open,
            # we delay initializing the FIRST bar of the day until the price actually moves.
            current_bar = self.instrument_data[instrument_id].current_bars.get(
                instrument_id
            )

            is_new_session = False
            if current_bar is None:
                is_new_session = True
            else:
                # If current_bar ended more than 1 hour ago, this is a new trading session
                if (dt - current_bar["end_time"]).total_seconds() > 3600:
                    is_new_session = True

            if is_new_session:
                if prev_price is not None and last_price == prev_price:
                    self.logger.debug(
                        f"[{instrument_id}] Delaying new session bar init until price moves from {last_price}"
                    )
                    # Publish B/S updates without creating the bar
                    await self._publish_bar_update(
                        instrument_id,
                        extra_meta={
                            "last_buy": self.last_buy_price[instrument_id],
                            "last_sell": self.last_sell_price[instrument_id],
                            "last_price": self.last_price[instrument_id],
                        },
                    )
                    return

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

    async def _depth_sse_callback(self, instrument_id, _id, event, data):
        """
        Async callback for order-depth-web-push SSE events.
        Publishes depth snapshots directly to Redis — no buffering or disk storage.
        """
        try:
            if not isinstance(data, dict):
                return

            levels = data.get("levels")
            if not levels:
                return

            msg = {
                "type": "depth",
                "instrument": instrument_id,
                "levels": levels,
                "updated": data.get("updated"),
            }

            if self._redis is not None:
                try:
                    await self._redis.publish(self.redis_channel, json.dumps(msg))
                except Exception as e:
                    self.logger.error(
                        f"[{instrument_id}] Failed to publish depth to Redis: {e}"
                    )
        except Exception as e:
            self.logger.exception(f"[{instrument_id}] Exception in depth callback: {e}")

    async def _trade_sse_callback(self, instrument_id, _id, event, data):
        """
        Async callback for trade-web-push SSE events.
        Publishes trade events directly to Redis.
        """
        try:
            if not isinstance(data, dict):
                return

            if "price" not in data or "volume" not in data:
                return

            msg = {"type": "trade", "instrument": instrument_id, "data": data}

            if self._redis is not None:
                try:
                    await self._redis.publish(self.redis_channel, json.dumps(msg))
                except Exception as e:
                    self.logger.error(
                        f"[{instrument_id}] Failed to publish trade to Redis: {e}"
                    )
        except Exception as e:
            self.logger.exception(f"[{instrument_id}] Exception in trade callback: {e}")

    # ── Market hours helper ──────────────────────────────────────────

    def _get_market_hours(self):
        """Return (ZoneInfo, open_hour, open_minute, close_hour, close_minute) from instrument_list."""
        for sid, info in self.instrument_list.items():
            tz_name = info.get("timezone", "Europe/Stockholm")
            market_open_str = info.get("market_open", "09:00")
            market_close_str = info.get("market_close", "17:45")
            break
        else:
            tz_name, market_open_str, market_close_str = (
                "Europe/Stockholm",
                "09:00",
                "17:45",
            )
        zone = ZoneInfo(tz_name)
        oh, om = (int(x) for x in market_open_str.split(":"))
        ch, cm = (int(x) for x in market_close_str.split(":"))
        return zone, oh, om, ch, cm

    def _is_outside_market_hours(self, start_time: datetime) -> bool:
        """Return True if start_time (UTC) falls before market open or at/after market close."""
        zone, oh, om, ch, cm = self._get_market_hours()
        local = start_time.astimezone(zone)
        if local.weekday() >= 5:
            return True
        t = (local.hour, local.minute)
        return t < (oh, om) or t >= (ch, cm)

    # ── Disk loading with market-hours filter ─────────────────────────

    def load_all_ohlc_from_disk(self):
        super().load_all_ohlc_from_disk()
        # Strip pre-market and post-market bars that may have been persisted
        for sid, sd in self.instrument_data.items():
            before = len(sd.completed_ohlc.get(sid, []))
            sd.completed_ohlc[sid] = [
                b
                for b in sd.completed_ohlc.get(sid, [])
                if not self._is_outside_market_hours(b["start_time"])
            ]
            after = len(sd.completed_ohlc.get(sid, []))
            if before != after:
                self.logger.info(
                    f"[{sid}] Filtered {before - after} out-of-hours bars from disk data."
                )
        self._sync_avanza_history()

    def _sync_avanza_history(self):
        from pacavanza.utils.utils import fetch_avanza_chart_history

        for sid, sd in self.instrument_data.items():
            try:
                info = self.instrument_list.get(sid, {})
                orderbook_id = info.get("orderbookId")
                if not orderbook_id:
                    self.logger.warning(
                        f"[{sid}] No orderbookId found, skipping Avanza history sync"
                    )
                    continue

                tz_name = info.get("timezone", "Europe/Stockholm")
                open_str = info.get("market_open", "09:00")
                close_str = info.get("market_close", "17:45")

                new_bars = fetch_avanza_chart_history(
                    orderbook_id=str(orderbook_id),
                    interval_seconds=self.interval_seconds,
                    tz_name=tz_name,
                    open_str=open_str,
                    close_str=close_str,
                )

                if not new_bars:
                    self.logger.warning(
                        f"[{sid}] No Avanza history data for orderbookId {orderbook_id}"
                    )
                    continue

                with sd.lock:
                    local_bars = sd.completed_ohlc.get(sid, [])
                    merged_bars_dict = {b["start_time"]: b for b in local_bars}

                    for b in new_bars:
                        merged_bars_dict[b["start_time"]] = b

                    sorted_bars = sorted(
                        merged_bars_dict.values(), key=lambda x: x["start_time"]
                    )
                    cutoff = datetime.now(timezone.utc) - timedelta(
                        hours=sd.max_history_hours
                    )
                    sd.completed_ohlc[sid] = [
                        b for b in sorted_bars if b["end_time"] >= cutoff
                    ]

                self.logger.info(
                    f"[{sid}] Synced {len(new_bars)} bars from Avanza. Total bars: {len(sd.completed_ohlc[sid])}"
                )
                self.force_save_instrument(sid)

            except Exception as e:
                self.logger.exception(f"[{sid}] Failed to sync Avanza history: {e}")


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


def main():
    interval_str = parse_args()
    interval_seconds = INTERVAL_MAP[interval_str]
    collector = FutureMarketCollector(interval_seconds)
    collector.run()


if __name__ == "__main__":
    main()
