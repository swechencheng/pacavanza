# avanza_market_multi.py
import asyncio
import json
import copy
import logging
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from functools import partial

from avanza import Avanza
from ..modules.avanza_sse_client import AvanzaSSEClient as SSEClient
from ..modules.stock_data import StockData, INTERVAL_MAP

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class MultiMarketCollector:
    """
    Run multiple SSE clients (one per stock/warrant) while using a single Avanza instance.
    Each stock keeps its own StockData, own file, and its own SSEClient.
    """

    quote_base_url = "https://www.avanza.se/_push/quote-web-push/"

    def __init__(
        self,
        stock_ids,
        interval_seconds,
        secret_path="./pacavanza/../secret.json",
        warrant_list_path="./pacavanza/warrant_list.json",
    ):
        self.interval_seconds = interval_seconds
        self.secret = json.load(open(secret_path))
        self.warrant_list = json.load(open(warrant_list_path))
        self.stock_ids = stock_ids

        # per-stock storage objects
        self.stock_data = {
            stock_id: StockData(interval_seconds, stock_id) for stock_id in stock_ids
        }
        # per-stock metadata
        self.last_buy_price = {sid: None for sid in stock_ids}
        self.last_sell_price = {sid: None for sid in stock_ids}

        # validate warrant list (and resolve warrant ids)
        self.warrant_ids = {}
        for sid in stock_ids:
            if sid not in self.warrant_list:
                raise ValueError(f"Stock ID {sid} not found in warrant list")
            info = self.warrant_list[sid]
            wid = info.get("ID")
            if not wid:
                raise ValueError(f"Warrant ID not found for {sid}")
            self.warrant_ids[sid] = wid

    def _market_window_utc_for_local_date(self, stock_id, local_date):
        """
        Returns (open_utc, close_utc) for the given stock_id and local_date (a date object).
        Handles case where close <= open (overnight session) by moving close to next day.
        """
        info = self.warrant_list[stock_id]
        tzname = info.get("timezone", "UTC")
        zone = ZoneInfo(tzname)

        # parse market_open/market_close strings such as "09:30" or "17:30"
        mo = info.get("market_open", "00:00")
        mc = info.get("market_close", "23:59")
        try:
            oh, om = (int(x) for x in mo.split(":"))
            ch, cm = (int(x) for x in mc.split(":"))
        except Exception:
            # fallback to full-day if parsing fails
            oh, om = 0, 0
            ch, cm = 23, 59

        local_open = datetime(
            year=local_date.year,
            month=local_date.month,
            day=local_date.day,
            hour=oh,
            minute=om,
            second=0,
            microsecond=0,
            tzinfo=zone,
        )
        local_close = datetime(
            year=local_date.year,
            month=local_date.month,
            day=local_date.day,
            hour=ch,
            minute=cm,
            second=0,
            microsecond=0,
            tzinfo=zone,
        )

        # if close <= open, assume close is next calendar day (overnight session)
        if local_close <= local_open:
            local_close = local_close + timedelta(days=1)

        open_utc = local_open.astimezone(timezone.utc)
        close_utc = local_close.astimezone(timezone.utc)
        return open_utc, close_utc

    def is_market_open(self, stock_id, dt_utc: datetime):
        """
        Check if market for stock_id is open at dt_utc (aware, in UTC).
        Returns True if within local open/close window and not weekend.
        """
        if dt_utc.tzinfo is None:
            dt_utc = dt_utc.replace(tzinfo=timezone.utc)
        else:
            dt_utc = dt_utc.astimezone(timezone.utc)

        info = self.warrant_list[stock_id]
        tzname = info.get("timezone", "UTC")
        zone = ZoneInfo(tzname)
        local_dt = dt_utc.astimezone(zone)

        # Skip weekends (Saturday=5, Sunday=6)
        if local_dt.weekday() >= 5:
            return False

        local_date = local_dt.date()
        open_utc, close_utc = self._market_window_utc_for_local_date(
            stock_id, local_date
        )

        # Market considered open if dt is in [open_utc, close_utc]
        return (open_utc <= dt_utc) and (dt_utc <= close_utc)

    async def _callback_quote_web_push(self, stock_id, _id, event, data):
        """
        stock-specific async callback for SSE events.
        Defensively handles parsing errors and ensures `dt` is always set.
        Only updates OHLC while market is open for the stock.
        An example QUOTE event callback:
        [RdvXmj1XLHFj_AEZKkiSmbcFx] [QUOTE] {'orderbookId': '2026354', 'buyPrice': 201.57, 'sellPrice': 201.63, 'closingPrice': 204.51, 'highestPrice': 201.98, 'lowestPrice': 200.25, 'lastPrice': 201.98, 'totalValueTraded': 21043.55, 'totalVolumeTraded': 105, 'change': -2.53, 'changePercent': -0.0124, 'spreadPercent': 0.0003, 'volumeWeightedAveragePrice': 200.41, 'updated': '2025-10-17T09:54:00.916Z', 'lastPriceUpdated': '2025-10-17T09:54:00.000Z'}
        """
        try:
            if event != "QUOTE" or not isinstance(data, dict):
                LOGGER.debug(f"[{stock_id}] [{event}] {data}")
                return

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
                LOGGER.warning(
                    f"[{stock_id}] {readable_ts} - buy/sell missing (buy={buy_price}, sell={sell_price})"
                )
                return

            LOGGER.info(
                f"[{stock_id}] {readable_ts} B: {buy_price:.2f}  S: {sell_price:.2f}"
            )

            # Only proceed if market is open for this stock at the event's timestamp
            if not self.is_market_open(stock_id, dt):
                LOGGER.debug(
                    f"[{stock_id}] Outside market hours ({readable_ts}), ignoring price update."
                )
                return

            # store last prices for this stock
            self.last_buy_price[stock_id] = buy_price
            self.last_sell_price[stock_id] = sell_price

            # Update the correct StockData instance (only during market open)
            self.stock_data[stock_id].update_ohlc_bar(buy_price, dt)

        except Exception as e:
            LOGGER.error(f"[{stock_id}] Exception in callback: {e!r}")

    async def periodic_saver(self):
        """
        Save all stock OHLCs to disk periodically, but only for stocks that are currently in market hours.
        """
        while True:
            await asyncio.sleep(self.interval_seconds)
            now_utc = datetime.now(timezone.utc)
            for sid, sd in self.stock_data.items():
                # only save if market is open right now for this stock
                if not self.is_market_open(sid, now_utc):
                    LOGGER.debug(f"[{sid}] Market closed now; skipping save.")
                    continue
                try:
                    with sd.lock:
                        bars = copy.deepcopy(sd.completed_ohlc[sid])
                    data = []
                    for b in bars:
                        bar = b.copy()
                        bar["start_time"] = bar["start_time"].isoformat()
                        bar["end_time"] = bar["end_time"].isoformat()
                        data.append(bar)
                    data_file = f"ohlc_{sid}.json"
                    with open(data_file, "w") as f:
                        json.dump(data, f)
                    LOGGER.debug(f"[{sid}] Saved {len(data)} bars to {data_file}")
                except Exception as e:
                    LOGGER.error(f"[{sid}] Failed to save OHLC: {e}")

    def load_all_ohlc_from_disk(self):
        for sid, sd in self.stock_data.items():
            data_file = f"ohlc_{sid}.json"
            try:
                with open(data_file, "r") as f:
                    data = json.load(f)
                bars = []
                for bar in data:
                    start = datetime.fromisoformat(bar["start_time"])
                    end = datetime.fromisoformat(bar["end_time"])
                    if start.tzinfo is None:
                        start = start.replace(tzinfo=timezone.utc)
                    else:
                        start = start.astimezone(timezone.utc)
                    if end.tzinfo is None:
                        end = end.replace(tzinfo=timezone.utc)
                    else:
                        end = end.astimezone(timezone.utc)
                    bar["start_time"] = start
                    bar["end_time"] = end
                    bars.append(bar)
                cutoff = datetime.now(timezone.utc) - timedelta(
                    hours=sd.max_history_hours
                )
                sd.completed_ohlc[sid] = [b for b in bars if b["end_time"] >= cutoff]
                LOGGER.info(f"[{sid}] Loaded {len(bars)} bars from {data_file}")
            except FileNotFoundError:
                LOGGER.info(f"[{sid}] No previous data file {data_file}.")
            except Exception as e:
                LOGGER.error(f"[{sid}] Failed to load OHLC data: {e}")

    async def _run_sse_client_loop(self, avanza, stock_id, warrant_id):
        """
        Create and (re)start an SSE client for a single stock. This function loops forever,
        handling reconnects for that stock, but *reuses* the provided `avanza` instance.
        """
        while True:
            try:
                client = SSEClient(avanza, self.quote_base_url + warrant_id)
                # Add a stock-specific listener (the SSE client will call this async function)
                client.add_listener(partial(self._callback_quote_web_push, stock_id))
                LOGGER.info(
                    f"[{stock_id}] Starting SSE client for warrant {warrant_id}"
                )
                await client.start()
                LOGGER.info(
                    f"[{stock_id}] SSE client stopped cleanly (will reconnect)."
                )
            except Exception as e:
                LOGGER.error(
                    f"[{stock_id}] SSE client error: {e}. Reconnecting in 5s..."
                )
                await asyncio.sleep(5)

    async def real_market_loop(self):
        """
        Create one Avanza instance and start an SSE client loop for every stock.
        If Avanza creation fails we retry (so the whole set reconnects together).
        """
        while True:
            avanza = None
            try:
                # create single Avanza instance (one login)
                avanza = Avanza(self.secret)
                LOGGER.info("Avanza login OK.")

                # start per-stock SSE loops (each loop handles its own reconnects)
                tasks = []
                for sid, wid in self.warrant_ids.items():
                    tasks.append(
                        asyncio.create_task(self._run_sse_client_loop(avanza, sid, wid))
                    )

                # Wait for all tasks (they are infinite loops that only stop on unexpected error)
                await asyncio.gather(*tasks)
            except Exception as e:
                LOGGER.error(
                    f"Error in real_market_loop: {e}. Recreating Avanza in 5s..."
                )
                await asyncio.sleep(5)
            finally:
                # cleanup Avanza if possible
                try:
                    if avanza and hasattr(avanza, "close"):
                        await avanza.close()
                except Exception:
                    pass

    def run(self):
        """
        Entry point: load disk data and start the asyncio loop.
        """
        self.load_all_ohlc_from_disk()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(
            asyncio.gather(self.real_market_loop(), self.periodic_saver())
        )


def parse_args():
    """
    Usage:
      python -m pacavanza.data_sources.avanza_market_multi STOCK1 STOCK2 ... [-i interval]
    """
    args = sys.argv[1:]
    if not args:
        print(
            "Usage: python -m pacavanza.data_sources.avanza_market_multi STOCK1 [STOCK2 ...] [-i interval]"
        )
        sys.exit(1)
    interval_str = "5m"
    # collect any tokens that are not -i and not interval value as stocks
    stocks = []
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
            stocks.append(args[i])
            i += 1

    if not stocks:
        LOGGER.error("No stock ids provided.")
        sys.exit(1)
    return stocks, interval_str


if __name__ == "__main__":
    stock_ids, interval_str = parse_args()
    interval_seconds = INTERVAL_MAP[interval_str]
    collector = MultiMarketCollector(stock_ids, interval_seconds)
    collector.run()
