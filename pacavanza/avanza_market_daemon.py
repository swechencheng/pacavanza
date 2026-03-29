import logging
import sys
from datetime import datetime, timezone

from .base_market_collector import BaseMarketCollector
from .modules.instrument_data import INTERVAL_MAP

logging.basicConfig(level=logging.INFO)
logging.getLogger("avanza_market_daemon").setLevel(logging.INFO)
LOGGER = logging.getLogger("avanza_market_daemon")

INSTRUMENT_LIST_PATH = "./pacavanza/ava_mini_future_list.json"


class AvanzaMarketCollector(BaseMarketCollector):
    """
    SSE collector for Avanza mini-futures using the quote-web-push endpoint.

    Inherits all infrastructure from BaseMarketCollector and provides:
      - quote-web-push specific SSE URL
      - _callback_quote_web_push handling buyPrice/sellPrice/updated fields
    """

    sse_base_url = "https://www.avanza.se/_push/quote-web-push/"
    default_instrument_list_path = INSTRUMENT_LIST_PATH
    default_redis_channel = "pacavanza:ticker_updates"
    logger_name = "avanza_market_daemon"

    def _init_price_tracking(self):
        self.last_buy_price = {sid: None for sid in self.instrument_ids}
        self.last_sell_price = {sid: None for sid in self.instrument_ids}

    async def _sse_callback(self, instrument_id, _id, event, data):
        """
        Async callback for quote-web-push SSE events.

        Example QUOTE event:
        {'orderbookId': '2026354', 'buyPrice': 201.57, 'sellPrice': 201.63,
         'closingPrice': 204.51, 'highestPrice': 201.98, 'lowestPrice': 200.25,
         'lastPrice': 201.98, 'totalValueTraded': 21043.55, 'totalVolumeTraded': 105,
         'change': -2.53, 'changePercent': -0.0124, 'spreadPercent': 0.0003,
         'volumeWeightedAveragePrice': 200.41, 'updated': '2025-10-17T09:54:00.916Z',
         'lastPriceUpdated': '2025-10-17T09:54:00.000Z'}
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
            if buy_price is None or sell_price is None:
                self.logger.warning(
                    f"[{instrument_id}] {readable_ts} - buy/sell missing (buy={buy_price}, sell={sell_price})"
                )
                return

            self.logger.debug(
                f"[{instrument_id}] {readable_ts} B: {buy_price:.2f}  S: {sell_price:.2f}"
            )

            # Only proceed if market is open
            market_check = self.is_market_open(instrument_id, dt)
            if not market_check:
                self.last_market_check[instrument_id] = market_check
                self.logger.debug(
                    f"[{instrument_id}] Outside market hours ({readable_ts}), ignoring price update."
                )
                return

            if not self.last_market_check[instrument_id]:
                # Market just opened
                self.logger.info(f"[{instrument_id}] Market just opened at {readable_ts}.")
                self.anomaly_buffer[instrument_id] = []
            else:
                # Anomaly detection: ignore new prices beyond 3.0% of last stored prices
                # Recovery: if 9 consecutive anomalous quotes are stable (<= 3% diff), accept.
                last_buy = self.last_buy_price.get(instrument_id)
                last_sell = self.last_sell_price.get(instrument_id)
                is_anomalous = False

                if last_buy is not None and abs(buy_price - last_buy) / last_buy > 0.03:
                    is_anomalous = True
                elif (
                    last_sell is not None
                    and abs(sell_price - last_sell) / last_sell > 0.03
                ):
                    is_anomalous = True

                if is_anomalous:
                    buffer = self.anomaly_buffer[instrument_id]
                    consistent = True
                    if buffer:
                        prev_buy, prev_sell = buffer[-1]
                        if abs(buy_price - prev_buy) / prev_buy > 0.03:
                            consistent = False
                        if (
                            consistent
                            and abs(sell_price - prev_sell) / prev_sell > 0.03
                        ):
                            consistent = False

                    if consistent:
                        buffer.append((buy_price, sell_price))
                        if len(buffer) >= 9:
                            self.logger.info(
                                f"[{instrument_id}] Anomaly recovery: 9 consecutive stable quotes. "
                                f"Accepting new level (B:{buy_price:.2f}, S:{sell_price:.2f})."
                            )
                            self.anomaly_buffer[instrument_id] = []
                            # Fall through to accept logic
                        else:
                            self.logger.warning(
                                f"[{instrument_id}] Anomalous quote (B:{buy_price:.2f}, S:{sell_price:.2f}) "
                                f"vs last (B:{last_buy}, S:{last_sell}). Stable count: {len(buffer)}/9. Ignoring."
                            )
                            return
                    else:
                        self.logger.warning(
                            f"[{instrument_id}] Erratic anomaly. Resetting recovery buffer."
                        )
                        self.anomaly_buffer[instrument_id] = [(buy_price, sell_price)]
                        return
                else:
                    self.anomaly_buffer[instrument_id] = []
            self.last_market_check[instrument_id] = market_check

            # store last prices for this instrument
            self.last_buy_price[instrument_id] = buy_price
            self.last_sell_price[instrument_id] = sell_price

            # Update OHLC
            self.instrument_data[instrument_id].update_ohlc_bar(buy_price, dt)

            # Mark current bar as dirty for periodic snapshot
            self._dirty_current.add(instrument_id)

            # Publish bar update to Redis
            await self._publish_bar_update(
                instrument_id,
                extra_meta={
                    "last_buy": self.last_buy_price[instrument_id],
                    "last_sell": self.last_sell_price[instrument_id],
                },
            )

        except Exception as e:
            self.logger.exception(f"[{instrument_id}] Exception in callback: {e}")


def parse_args():
    """
    Usage:
      python -m pacavanza.avanza_market_daemon [-i interval]
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
                f"Ignoring CLI arg '{args[i]}' (instrument ids loaded from ava_mini_future_list.json)"
            )
            i += 1

    return interval_str


if __name__ == "__main__":
    interval_str = parse_args()
    interval_seconds = INTERVAL_MAP[interval_str]
    collector = AvanzaMarketCollector(interval_seconds)
    collector.run()
