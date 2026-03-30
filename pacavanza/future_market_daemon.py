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
    default_instrument_list_path = FUTURE_LIST_PATH
    default_redis_channel = "pacavanza:future_updates"
    logger_name = "future_market_daemon"

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
