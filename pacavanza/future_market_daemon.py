import logging
import sys
from datetime import datetime, timezone

from .base_market_collector import BaseMarketCollector
from .modules.instrument_data import INTERVAL_MAP

logging.basicConfig(level=logging.INFO)
logging.getLogger("future_market_daemon").setLevel(logging.INFO)
LOGGER = logging.getLogger("future_market_daemon")

FUTURE_LIST_PATH = "./pacavanza/future_list.json"


class FutureMarketCollector(BaseMarketCollector):
    """
    SSE collector for classic futures (e.g. OMXS306D) using Avanza's
    trade-web-push endpoint.

    Inherits all infrastructure from BaseMarketCollector and provides:
      - trade-web-push specific SSE URL
      - _callback_trade_web_push handling price/volume/dealTime fields

    Trade event example payload:
    {
        "tradeId": "8207612_59746",
        "orderbookId": "2279188",
        "buyer": "",
        "seller": "",
        "dealTime": 1774024058949,
        "price": 2834.25,
        "volume": 658,
        "matchedOnMarket": true,
        "cancelled": false,
        "initialSubscription": false
    }
    """

    sse_base_url = "https://www.avanza.se/_push/trade-web-push/"
    default_instrument_list_path = FUTURE_LIST_PATH
    default_redis_channel = "pacavanza:future_updates"
    logger_name = "future_market_daemon"

    def _init_price_tracking(self):
        self.last_price = {sid: None for sid in self.instrument_ids}

    async def _sse_callback(self, instrument_id, _id, event, data):
        """
        Async callback for trade-web-push SSE events.
        Handles trade data with price, volume, and dealTime (epoch ms).
        """
        try:
            if not isinstance(data, dict):
                return

            # Skip cancelled trades
            if data.get("cancelled", False):
                return

            # Skip initial subscription data (historical backfill)
            if data.get("initialSubscription", False):
                return

            self.logger.debug(f"[{instrument_id}] [{event}] {data}")

            price = data.get("price")
            volume = data.get("volume", 0)
            deal_time_ms = data.get("dealTime")

            if price is None:
                self.logger.warning(
                    f"[{instrument_id}] Trade event missing 'price': {data}"
                )
                return

            # Parse dealTime (epoch milliseconds) into UTC datetime
            dt = datetime.now(timezone.utc)
            readable_ts = "(no timestamp)"
            if deal_time_ms is not None:
                try:
                    dt = datetime.fromtimestamp(deal_time_ms / 1000.0, tz=timezone.utc)
                    milli = dt.microsecond // 1000
                    readable_ts = f"{dt.strftime('%Y-%m-%d %H:%M:%S')}.{milli:03d} {dt.strftime('%Z')}"
                except Exception:
                    readable_ts = str(deal_time_ms)

            self.logger.debug(
                f"[{instrument_id}] {readable_ts} P: {price:.2f}  V: {volume}"
            )

            # Store last price
            self.last_price[instrument_id] = price

            # Update OHLC bar
            self.instrument_data[instrument_id].update_ohlc_bar(price, dt)

            # Mark current bar as dirty for periodic snapshot
            self._dirty_current.add(instrument_id)

            # Publish bar update to Redis
            await self._publish_bar_update(
                instrument_id,
                extra_meta={
                    "last_price": self.last_price[instrument_id],
                    "last_volume": volume,
                },
            )

        except Exception as e:
            self.logger.exception(f"[{instrument_id}] Exception in trade callback: {e}")


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
                f"Ignoring CLI arg '{args[i]}' (instrument ids loaded from future_list.json)"
            )
            i += 1

    return interval_str


if __name__ == "__main__":
    interval_str = parse_args()
    interval_seconds = INTERVAL_MAP[interval_str]
    collector = FutureMarketCollector(interval_seconds)
    collector.run()
