import asyncio, json, copy, logging
import sys, threading
from datetime import datetime, timedelta, timezone
from avanza import Avanza
from ..modules.avanza_sse_client import AvanzaSSEClient as SSEClient
from ..modules.stock_data import StockData, INTERVAL_MAP

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class RealMarketData:
    def __init__(
        self,
        stock_data: StockData,
        secret_path="./pacavanza/../secret.json",
        warrant_list_path="./pacavanza/warrant_list.json",
    ):
        self.stock_data = stock_data
        self.secret = json.load(open(secret_path))
        self.warrant_list = json.load(open(warrant_list_path))
        self.stock_id = stock_data.stock_id
        self.data_file = f"ohlc_{self.stock_id}.json"
        self.max_history_hours = stock_data.max_history_hours
        self.quote_base_url = "https://www.avanza.se/_push/quote-web-push/"
        self.financing_level = None
        self.last_buy_price = None
        self.last_sell_price = None
        self.lock = threading.Lock()

        if not self.stock_id in self.warrant_list:
            raise ValueError(f"Stock ID {self.stock_id} not found in warrant list")
        else:
            info = self.warrant_list[self.stock_id]
            self.warrant_id = info.get("ID")
            if not self.warrant_id:
                raise ValueError(f"Warrant ID not found for {self.stock_id}")

    def load_ohlc_from_disk(self):
        try:
            with open(self.data_file, "r") as f:
                data = json.load(f)
            bars = []
            for bar in data:
                # parse and force UTC timezone if missing
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
                hours=self.max_history_hours
            )
            self.stock_data.completed_ohlc[self.stock_id] = [
                b for b in bars if b["end_time"] >= cutoff
            ]
            LOGGER.info(f"Loaded {len(bars)} bars from {self.data_file}")
        except FileNotFoundError:
            LOGGER.info("No previous data found.")
        except Exception as e:
            LOGGER.error(f"Failed to load OHLC data: {e}")

    def save_ohlc_to_disk(self):
        try:
            with self.stock_data.lock:
                bars = copy.deepcopy(self.stock_data.completed_ohlc[self.stock_id])
            data = []
            for b in bars:
                bar = b.copy()
                bar["start_time"] = bar["start_time"].isoformat()
                bar["end_time"] = bar["end_time"].isoformat()
                data.append(bar)
            with open(self.data_file, "w") as f:
                json.dump(data, f)
        except Exception as e:
            LOGGER.error(f"Failed to save OHLC data: {e}")

    async def callback_quote_web_push(self, id, event, data):
        """
        This runs in the SSE callback from Avanza.
        Defensively handles parsing errors and ensures `dt` is always set.
        An example QUOTE event callback:
        [RdvXmj1XLHFj_AEZKkiSmbcFx] [QUOTE] {'orderbookId': '2026354', 'buyPrice': 201.57, 'sellPrice': 201.63, 'closingPrice': 204.51, 'highestPrice': 201.98, 'lowestPrice': 200.25, 'lastPrice': 201.98, 'totalValueTraded': 21043.55, 'totalVolumeTraded': 105, 'change': -2.53, 'changePercent': -0.0124, 'spreadPercent': 0.0003, 'volumeWeightedAveragePrice': 200.41, 'updated': '2025-10-17T09:54:00.916Z', 'lastPriceUpdated': '2025-10-17T09:54:00.000Z'}
        """
        try:
            # Check if data is a dict or not
            if event != "QUOTE" or not isinstance(data, dict):
                LOGGER.debug(f"[{id}] [{event}] {data}")
                return

            ts = data.get("updated")

            # default fallback timestamp (timezone-aware)
            dt = datetime.now(timezone.utc)

            # format readable timestamp from the message if needed
            if ts:
                try:
                    parsed = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f%z")
                    dt = parsed.astimezone(timezone.utc)
                    milli = dt.microsecond // 1000
                    readable_ts = f"{dt.strftime('%Y-%m-%d %H:%M:%S')}.{milli:03d} {dt.strftime('%Z')}"
                except Exception:
                    # keep dt as fallback (now in UTC)
                    readable_ts = str(ts)
            else:
                readable_ts = "(no timestamp)"

            buy_price = data.get("buyPrice")
            sell_price = data.get("sellPrice")

            # Ensure valid and consistent MM detection
            if buy_price is None or sell_price is None:
                LOGGER.warning(
                    f"{readable_ts} - Valid buy/sell price not found (buy={buy_price}, sell={sell_price})"
                )
                return

            with self.lock:
                self.last_buy_price = buy_price
                self.last_sell_price = sell_price

            LOGGER.info(f"{readable_ts} B: {buy_price:.2f}  S: {sell_price:.2f}")
            # `dt` guaranteed to be defined (UTC)
            self.stock_data.update_ohlc_bar(buy_price, dt)

        except Exception as e:
            # Catch *anything* so this callback never bubbles an exception to the websocket loop.
            # Keep the log message small but informative.
            LOGGER.error(f"Exception in callback_quote_web_push: {e!r}")

    async def real_market_loop(self):
        while True:
            avanza = None
            try:
                avanza = Avanza(self.secret)
                warrant_info = avanza.get_warrant_info(self.warrant_id)
                self.financing_level = float(
                    warrant_info.get("keyIndicators", {}).get("financingLevel", 0)
                )
                LOGGER.info(f"Financing Level: {self.financing_level}")
                client = SSEClient(avanza, self.quote_base_url + self.warrant_id)
                client.add_listener(self.callback_quote_web_push)
                await client.start()
            except Exception as e:
                LOGGER.error(f"Error in real_market_loop: {e}. Reconnecting...")
                await asyncio.sleep(5)
            finally:
                try:
                    if avanza and hasattr(avanza, "close"):
                        await avanza.close()
                except Exception:
                    pass

    async def periodic_saver(self):
        while True:
            await asyncio.sleep(self.stock_data.interval_seconds)
            self.save_ohlc_to_disk()

    def run(self):
        """Entry point for real market collection"""
        self.load_ohlc_from_disk()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(
            asyncio.gather(self.real_market_loop(), self.periodic_saver())
        )


def parse_args():
    args = sys.argv[1:]
    stock_id = None
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
            stock_id = args[i]
            i += 1
    if not stock_id:
        LOGGER.error(
            "Usage: python -m pacavanza.data_sources.avanza_market STOCK_ID [-i interval]"
        )
        sys.exit(1)
    return stock_id, interval_str


if __name__ == "__main__":
    stock_id, interval_str = parse_args()
    interval_seconds = INTERVAL_MAP[interval_str]
    stock_data = StockData(interval_seconds, stock_id)
    collector = RealMarketData(stock_data)
    collector.run()
