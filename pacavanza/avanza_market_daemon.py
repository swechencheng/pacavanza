import asyncio
import json
import copy
import logging
import signal
import sys
import threading
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from functools import partial
from typing import Optional

# third-party
import redis.asyncio as aioredis

from avanza import Avanza
from .modules.avanza_sse_client import AvanzaSSEClient as SSEClient
from .modules.stock_data import StockData, INTERVAL_MAP
from .utils.utils import save_json_atomic

logging.basicConfig(level=logging.INFO)
logging.getLogger("avanza_market_daemon").setLevel(logging.INFO)
LOGGER = logging.getLogger("avanza_market_daemon")

WARRANT_LIST_PATH = "./pacavanza/warrant_list.json"


class MultiMarketCollector:
    """
    Run multiple SSE clients (one per stock/warrant) while using a single Avanza instance.
    Each stock keeps its own StockData, own file, and its own SSEClient.

    This variant publishes minimal updates to Redis pub/sub so other processes (web server)
    can subscribe and broadcast to clients without blocking the collector.
    """

    quote_base_url = "https://www.avanza.se/_push/quote-web-push/"

    def __init__(
        self,
        stock_ids,
        interval_seconds,
        secret_path="./pacavanza/../secret.json",
        warrant_list_path=WARRANT_LIST_PATH,
        stock_datas: dict = None,
        redis_url: str = "redis://localhost:6379/0",
        redis_channel: str = "pacavanza:ticker_updates",
        # how often to persist completed bars to disk (seconds). Default: max(30, interval_seconds)
        completed_save_interval: Optional[float] = None,
        # how often to persist current (in-progress) bars snapshot to disk (seconds).
        current_snapshot_interval: float = 3.0,
    ):
        self.interval_seconds = interval_seconds
        self.secret = json.load(open(secret_path))
        self.warrant_list = json.load(open(warrant_list_path))
        self.stock_ids = stock_ids

        # per-stock storage objects
        # If caller provided existing StockData instances, use them so the
        # collector updates the same objects the chart reads from.
        # Otherwise create new StockData objects as before.
        if stock_datas is not None:
            # only pick the stocks we were asked to collect
            self.stock_data = {
                stock_id: stock_datas[stock_id] for stock_id in stock_ids
            }
        else:
            self.stock_data = {
                stock_id: StockData(interval_seconds, stock_id)
                for stock_id in stock_ids
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

        # track tasks & clients for graceful shutdown
        self._tasks = []  # list of asyncio.Task objects we create
        self._sse_clients = {}  # stock_id -> SSEClient instance (if created)
        self._avanza = None  # will hold the Avanza instance when created
        self._shutting_down = False
        self._loop = None

        # Redis pub/sub config
        self.redis_url = redis_url
        self.redis_channel = redis_channel
        self._redis = None  # will be aioredis.Redis when connected

        # saver config
        self.current_snapshot_interval = current_snapshot_interval
        self.completed_save_interval = (
            completed_save_interval
            if completed_save_interval is not None
            else max(30.0, float(self.interval_seconds))
        )

        # bookkeeping to limit snapshot IO
        # dirty_current stores stock ids whose current bar has changed since last snapshot
        self._dirty_current = set()
        self._last_current_snapshot = datetime.now(timezone.utc)

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
                return

            LOGGER.debug(f"[{stock_id}] [{event}] {data}")
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

            # Anomaly detection: ignore new prices that are beyond 1.0% of last stored prices, usually caused by other market participants' orders
            last_buy = self.last_buy_price.get(stock_id)
            last_sell = self.last_sell_price.get(stock_id)
            if last_buy is not None:
                if abs(buy_price - last_buy) / last_buy > 0.01:
                    LOGGER.warning(
                        f"[{stock_id}] Anomalous buy price {buy_price:.2f} vs last {last_buy:.2f}, ignoring."
                    )
                    return
            if last_sell is not None:
                if abs(sell_price - last_sell) / last_sell > 0.01:
                    LOGGER.warning(
                        f"[{stock_id}] Anomalous sell price {sell_price:.2f} vs last {last_sell:.2f}, ignoring."
                    )
                    return

            # store last prices for this stock
            self.last_buy_price[stock_id] = buy_price
            self.last_sell_price[stock_id] = sell_price

            # Update the correct StockData instance (only during market open)
            # This call updates internal current bar / completed ohlc lists.
            self.stock_data[stock_id].update_ohlc_bar(buy_price, dt)

            # mark current bar as dirty for periodic snapshot
            self._dirty_current.add(stock_id)

            # Build a tiny message describing the current bar (the StockData class should return bar dicts)
            try:
                # fetch current bar & last completed bar for the stock in a thread-safe manner
                with self.stock_data[stock_id].lock:
                    curr = copy.deepcopy(
                        self.stock_data[stock_id].current_bars.get(stock_id)
                    )
                    latest_completed = (
                        copy.deepcopy(
                            self.stock_data[stock_id].completed_ohlc[stock_id][-1]
                        )
                        if (
                            self.stock_data[stock_id].completed_ohlc.get(stock_id)
                            and len(self.stock_data[stock_id].completed_ohlc[stock_id])
                            > 0
                        )
                        else None
                    )

                # prefer to send only the current bar (update) and indicate if it is a new completed bar
                if curr:
                    msg = {
                        "type": "update",
                        "stock": stock_id,
                        "bar": {
                            "start_time": curr["start_time"].isoformat(),
                            "end_time": curr["end_time"].isoformat(),
                            "open": curr["open"],
                            "high": curr["high"],
                            "low": curr["low"],
                            "close": curr["close"],
                            "volume": curr.get("volume", 0),
                        },
                        "completed": False,
                        "meta": {
                            "timezone": self.warrant_list[stock_id].get(
                                "timezone", "UTC"
                            ),
                            "market_open": self.warrant_list[stock_id].get(
                                "market_open", "00:00"
                            ),
                            "market_close": self.warrant_list[stock_id].get(
                                "market_close", "23:59"
                            ),
                        },
                    }

                    # Publish to Redis with error handling
                    if self._redis is not None:
                        try:
                            # Use await instead of create_task to ensure message is sent
                            await self._redis.publish(
                                self.redis_channel, json.dumps(msg)
                            )
                            LOGGER.debug(f"[{stock_id}] Published update to Redis")
                        except Exception as e:
                            LOGGER.error(
                                f"[{stock_id}] Failed to publish to Redis: {e}"
                            )
                    else:
                        LOGGER.warning(
                            f"[{stock_id}] Redis not connected - skipping publish"
                        )

                # Publish completed bar if detected
                if latest_completed and curr:
                    try:
                        # Compare latest_completed['end_time'] to current bar's start_time
                        if latest_completed["end_time"] <= curr["start_time"]:
                            msg_completed = {
                                "type": "completed",
                                "stock": stock_id,
                                "bar": {
                                    "start_time": latest_completed[
                                        "start_time"
                                    ].isoformat(),
                                    "end_time": latest_completed[
                                        "end_time"
                                    ].isoformat(),
                                    "open": latest_completed["open"],
                                    "high": latest_completed["high"],
                                    "low": latest_completed["low"],
                                    "close": latest_completed["close"],
                                    "volume": latest_completed.get("volume", 0),
                                },
                            }
                            if self._redis is not None:
                                try:
                                    await self._redis.publish(
                                        self.redis_channel, json.dumps(msg_completed)
                                    )
                                    LOGGER.debug(
                                        f"[{stock_id}] Published completed bar to Redis"
                                    )
                                except Exception as e:
                                    LOGGER.error(
                                        f"[{stock_id}] Failed to publish completed bar to Redis: {e}"
                                    )
                    except Exception as e:
                        LOGGER.exception(
                            f"[{stock_id}] Error detecting/publishing completed bar: {e}"
                        )

            except Exception as e:
                LOGGER.exception(
                    f"[{stock_id}] Failed to build or publish update message: {e}"
                )

        except Exception as e:
            LOGGER.exception(f"[{stock_id}] Exception in callback: {e}")

    async def periodic_saver(self):
        """
        Save all stock OHLCs to disk periodically, but only for stocks that are currently in market hours.
        Also save current (in-progress) bars every `current_snapshot_interval` seconds for durability.
        """
        last_completed_save = datetime.now(timezone.utc)
        last_current_save = datetime.now(timezone.utc)
        while True:
            await asyncio.sleep(0.5)
            now_utc = datetime.now(timezone.utc)

            # save current bars periodically if any are dirty
            if (
                now_utc - last_current_save
            ).total_seconds() >= self.current_snapshot_interval:
                # snapshot dirty current bars
                dirty = list(self._dirty_current)
                for sid in dirty:
                    try:
                        sd = self.stock_data[sid]
                        with sd.lock:
                            curr = sd.current_bars.get(sid)
                        if not curr:
                            # nothing to persist
                            self._dirty_current.discard(sid)
                            continue
                        # write snapshot for current bar atomically
                        snap_file = f"ohlc_current_{sid}.json"
                        snap = {
                            "start_time": curr["start_time"].isoformat(),
                            "end_time": curr["end_time"].isoformat(),
                            "open": curr["open"],
                            "high": curr["high"],
                            "low": curr["low"],
                            "close": curr["close"],
                            "volume": curr.get("volume", 0),
                        }
                        save_json_atomic(snap_file, snap)
                        # mark as not dirty (we just persisted)
                        try:
                            self._dirty_current.discard(sid)
                        except Exception:
                            pass
                    except Exception as e:
                        LOGGER.error(f"[{sid}] Failed to snapshot current bar: {e}")
                last_current_save = now_utc

            # save completed bars less frequently (configurable)
            if (
                now_utc - last_completed_save
            ).total_seconds() >= self.completed_save_interval:
                for sid, sd in self.stock_data.items():
                    # only save if market is open right now for this stock (or optionally always)
                    try:
                        # We choose to save regardless of market open to not lose completed bars.
                        with sd.lock:
                            bars = copy.deepcopy(sd.completed_ohlc[sid])
                        data = []
                        for b in bars:
                            bar = b.copy()
                            bar["start_time"] = bar["start_time"].isoformat()
                            bar["end_time"] = bar["end_time"].isoformat()
                            data.append(bar)
                        data_file = f"ohlc_{sid}.json"
                        save_json_atomic(data_file, data)
                        LOGGER.debug(f"[{sid}] Saved {len(data)} bars to {data_file}")
                    except Exception as e:
                        LOGGER.error(f"[{sid}] Failed to save OHLC: {e}")
                last_completed_save = now_utc

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

        # attempt to load current bar snapshots (if any) to restore in-progress bars after crash
        for sid, sd in self.stock_data.items():
            snap_file = f"ohlc_current_{sid}.json"
            try:
                with open(snap_file, "r") as f:
                    snap = json.load(f)
                start = datetime.fromisoformat(snap["start_time"])
                end = datetime.fromisoformat(snap["end_time"])
                if start.tzinfo is None:
                    start = start.replace(tzinfo=timezone.utc)
                else:
                    start = start.astimezone(timezone.utc)
                if end.tzinfo is None:
                    end = end.replace(tzinfo=timezone.utc)
                else:
                    end = end.astimezone(timezone.utc)
                curr = {
                    "start_time": start,
                    "end_time": end,
                    "open": snap["open"],
                    "high": snap["high"],
                    "low": snap["low"],
                    "close": snap["close"],
                    "volume": snap.get("volume", 0),
                }
                # restore into current_bars to avoid losing current in-progress bar
                with sd.lock:
                    sd.current_bars[sid] = curr
                LOGGER.info(f"[{sid}] Restored in-progress bar from {snap_file}")
            except FileNotFoundError:
                # ignore
                pass
            except Exception as e:
                LOGGER.error(f"[{sid}] Failed to restore current snapshot: {e}")

    async def _run_sse_client_loop(self, avanza, stock_id, warrant_id):
        while True:
            # if shutdown requested, exit loop instead of creating new clients
            if self._shutting_down:
                LOGGER.info(
                    f"[{stock_id}] Shutdown requested — exiting _run_sse_client_loop."
                )
                break

            client = None
            try:
                client = SSEClient(avanza, self.quote_base_url + warrant_id)
                self._sse_clients[stock_id] = client
                client.add_listener(partial(self._callback_quote_web_push, stock_id))
                LOGGER.info(
                    f"[{stock_id}] Starting SSE client for warrant {warrant_id}"
                )
                await client.start()
                LOGGER.info(
                    f"[{stock_id}] SSE client stopped cleanly (will reconnect)."
                )

                # after client.start() returns, check if shutdown was requested
                if self._shutting_down:
                    LOGGER.info(
                        f"[{stock_id}] Shutdown requested after client stopped — exiting loop."
                    )
                    # attempt to remove client reference and break
                    self._sse_clients.pop(stock_id, None)
                    break

            except asyncio.CancelledError:
                LOGGER.info(
                    f"[{stock_id}] _run_sse_client_loop cancelled: attempting client stop."
                )
                try:
                    if client is not None:
                        stop_fn = getattr(client, "stop", None) or getattr(
                            client, "close", None
                        )
                        if stop_fn:
                            res = stop_fn()
                            if asyncio.iscoroutine(res):
                                await res
                except Exception as e:
                    LOGGER.debug(
                        f"[{stock_id}] Exception while stopping client on cancel: {e}"
                    )
                finally:
                    self._sse_clients.pop(stock_id, None)
                    raise
            except Exception as e:
                LOGGER.error(
                    f"[{stock_id}] SSE client error: {e}. Reconnecting in 5s..."
                )
                try:
                    if client is not None:
                        stop_fn = getattr(client, "stop", None) or getattr(
                            client, "close", None
                        )
                        if stop_fn:
                            res = stop_fn()
                            if asyncio.iscoroutine(res):
                                await res
                except Exception:
                    pass
                self._sse_clients.pop(stock_id, None)
                # if shutdown flag set, don't sleep & reconnect — break
                if self._shutting_down:
                    LOGGER.info(
                        f"[{stock_id}] Shutdown requested during error; exiting client loop."
                    )
                    break
                await asyncio.sleep(5)

    async def real_market_loop(self):
        """
        Create one Avanza instance and start an SSE client loop for every stock.
        If Avanza creation fails we retry (so the whole set reconnects together).
        """
        # create / connect redis client for publishing
        try:
            self._redis = aioredis.from_url(self.redis_url)
            # test connection with PING
            await self._redis.ping()
            LOGGER.info("Redis connected for publishing.")
        except Exception as e:
            LOGGER.error(f"Failed to connect to Redis at {self.redis_url}: {e}")
            self._redis = None

        while True:
            if self._shutting_down:
                LOGGER.info("real_market_loop: shutting down flag set — exiting loop.")
                break
            avanza = None
            try:
                # create single Avanza instance (one login)
                avanza = Avanza(self.secret)
                self._avanza = avanza
                LOGGER.info("Avanza login OK.")

                # start per-stock SSE loops (each loop handles its own reconnects)
                self._tasks = []
                for sid, wid in self.warrant_ids.items():
                    t = asyncio.create_task(self._run_sse_client_loop(avanza, sid, wid))
                    self._tasks.append(t)

                # Wait for all tasks (they are infinite loops that only stop on unexpected error)
                await asyncio.gather(*self._tasks)
            except Exception as e:
                LOGGER.error(
                    f"Error in real_market_loop: {e}. Recreating Avanza in 5s..."
                )
                await asyncio.sleep(5)
            finally:
                try:
                    if avanza and hasattr(avanza, "close"):
                        await avanza.close()
                except Exception:
                    pass
                finally:
                    self._avanza = None

    def force_save_stock(self, stock_id, timestamp: datetime = None):
        """
        Immediately save OHLC data for `stock_id`.
        If timestamp is provided (aware UTC), use it as the current-bar end_time;
        otherwise use the current UTC time.
        """
        ts = timestamp if timestamp is not None else datetime.now(timezone.utc)
        sd = self.stock_data[stock_id]
        try:
            with sd.lock:
                # copy completed bars
                bars = copy.deepcopy(sd.completed_ohlc[stock_id])
                # snapshot current bar if present
                current = sd.current_bars.get(stock_id)
                if current:
                    curr_copy = copy.deepcopy(current)
                    # set end_time to provided timestamp (ensure tz-aware)
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    curr_copy["end_time"] = ts
                    if curr_copy["start_time"].tzinfo is None:
                        curr_copy["start_time"] = curr_copy["start_time"].replace(
                            tzinfo=timezone.utc
                        )
                    bars.append(curr_copy)

            # write to disk outside lock
            data = []
            for b in bars:
                bar = b.copy()
                bar["start_time"] = bar["start_time"].isoformat()
                bar["end_time"] = bar["end_time"].isoformat()
                data.append(bar)
            data_file = f"ohlc_{stock_id}.json"
            save_json_atomic(data_file, data)
            LOGGER.info(
                f"[{stock_id}] Force-saved {len(data)} bars at {ts.isoformat()}"
            )
            # also persist current snapshot for fast recovery
            if current:
                snap_file = f"ohlc_current_{stock_id}.json"
                snap = {
                    "start_time": current["start_time"].isoformat(),
                    "end_time": current["end_time"].isoformat(),
                    "open": current["open"],
                    "high": current["high"],
                    "low": current["low"],
                    "close": current["close"],
                    "volume": current.get("volume", 0),
                }
                save_json_atomic(snap_file, snap)
        except Exception as e:
            LOGGER.error(f"[{stock_id}] Failed force-save: {e}")

    def force_save_all(self, timestamp: datetime = None):
        """
        Force-save OHLC for all stocks immediately.
        If timestamp is provided it's used as the end_time for in-progress bars;
        otherwise current UTC time is used.
        """
        ts = timestamp if timestamp is not None else datetime.now(timezone.utc)
        LOGGER.info(f"Force-saving all stocks at {ts.isoformat()}")
        for sid in list(self.stock_data.keys()):
            try:
                self.force_save_stock(sid, ts)
            except Exception as e:
                LOGGER.error(f"[{sid}] Exception during force_save_all: {e}")

    async def _shutdown(self, loop, signum):
        """
        Coroutine called from signal handlers. Force-saves, stops clients, closes Avanza,
        cancels tasks and waits for them to finish before stopping the loop.
        """
        LOGGER.info(
            f"Received signal {signum}. Initiating graceful shutdown: forcing save and cancelling tasks..."
        )
        # set the flag so loops stop creating new clients
        self._shutting_down = True
        # 1) Force-save synchronously (quick)
        try:
            self.force_save_all()
        except Exception as e:
            LOGGER.error(f"Error during force_save_all in shutdown: {e}")

        # 2) Stop SSE clients (await if they provide async stop)
        for sid, client in list(self._sse_clients.items()):
            try:
                LOGGER.info(f"[{sid}] Stopping SSE client...")
                stop_fn = getattr(client, "stop", None) or getattr(
                    client, "close", None
                )
                if stop_fn:
                    res = stop_fn()
                    if asyncio.iscoroutine(res):
                        await res
            except Exception as e:
                LOGGER.debug(f"[{sid}] Exception while stopping SSE client: {e}")
            finally:
                self._sse_clients.pop(sid, None)

        # 3) Close Avanza session if exists (await if coroutine)
        if self._avanza is not None:
            try:
                close_fn = getattr(self._avanza, "close", None)
                if close_fn:
                    res = close_fn()
                    if asyncio.iscoroutine(res):
                        await res
                self._avanza = None
            except Exception as e:
                LOGGER.debug(f"Exception while closing Avanza: {e}")

        # 4) Cancel outstanding tasks we created and await them
        # include self._tasks (per-stock loops), plus other tasks except current
        to_cancel = list(self._tasks) if self._tasks else []
        # gather other tasks (exclude current task)
        for t in asyncio.all_tasks(loop):
            if t is asyncio.current_task(loop):
                continue
            if t not in to_cancel:
                to_cancel.append(t)

        if to_cancel:
            for t in to_cancel:
                try:
                    t.cancel()
                except Exception:
                    pass

            # Wait for tasks to finish, but don't hang forever
            try:
                await asyncio.wait_for(
                    asyncio.gather(*to_cancel, return_exceptions=True), timeout=10.0
                )
            except asyncio.TimeoutError:
                LOGGER.warning(
                    "Timeout while waiting for tasks to finish during shutdown."
                )

        # 5) stop the loop (will cause run_until_complete to return)
        try:
            loop.stop()
        except Exception:
            pass

        # close redis
        if self._redis is not None:
            try:
                await self._redis.aclose()
            except Exception:
                pass

    def run(self):
        """
        Entry point: load disk data and start the asyncio loop.
        Registers signal handlers to force-save on SIGINT/SIGTERM only if running in main thread.
        """
        self.load_all_ohlc_from_disk()
        loop = asyncio.new_event_loop()
        self._loop = loop  # save reference for external stop()
        asyncio.set_event_loop(loop)

        # create the main tasks
        main_tasks = [
            loop.create_task(self.real_market_loop()),
            loop.create_task(self.periodic_saver()),
        ]
        # keep reference so shutdown can cancel them
        self._tasks = main_tasks.copy()

        # install signal handlers only if we're running in main thread.
        if threading.current_thread() is threading.main_thread():

            def _schedule_shutdown(s):
                try:
                    asyncio.create_task(self._shutdown(loop, s))
                except Exception:
                    try:
                        asyncio.run_coroutine_threadsafe(self._shutdown(loop, s), loop)
                    except Exception:
                        pass

            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, lambda s=sig: _schedule_shutdown(s))
                except Exception:
                    # if add_signal_handler fails for some reason, skip it.
                    LOGGER.debug(
                        "run(): loop.add_signal_handler failed; skipping signal handler registration."
                    )
        else:
            LOGGER.debug(
                "run(): not running in main thread — skipping signal handler registration (caller should call stop())."
            )

        try:
            loop.run_forever()
        except KeyboardInterrupt:
            LOGGER.info("KeyboardInterrupt received in run()")
        finally:
            # final cleanup: ensure tasks stopped
            try:
                LOGGER.info("Final force-save for all stocks (final cleanup)")
                self.force_save_all()
            except Exception as e:
                LOGGER.error(f"Final force-save failed: {e}")
            try:
                loop.run_until_complete(asyncio.sleep(0.1))
            except Exception:
                pass
            try:
                loop.close()
            except Exception:
                pass

    def stop(self, timeout: float = 15.0):
        """
        Synchronous method to request graceful shutdown from another thread (e.g. main thread).
        Sets shutdown flag and schedules the async _shutdown coroutine onto the collector's loop.
        Waits up to `timeout` seconds for the shutdown coroutine to complete.
        """
        LOGGER.info(
            "Stop requested (external). Setting shutting_down flag and scheduling shutdown."
        )
        self._shutting_down = True

        if not getattr(self, "_loop", None):
            LOGGER.debug("stop(): no event loop reference; nothing to schedule.")
            return

        try:
            # schedule the coroutine on the collector's loop and wait for result (best-effort)
            fut = asyncio.run_coroutine_threadsafe(
                self._shutdown(self._loop, "external"), self._loop
            )
            try:
                fut.result(timeout=timeout)
            except Exception as e:
                LOGGER.debug(f"stop(): shutdown coroutine finished/failed/timeout: {e}")
        except Exception as e:
            LOGGER.error(f"stop(): failed to schedule shutdown on collector loop: {e}")


def parse_args():
    """
    Usage:
      python -m pacavanza.data_sources.avanza_market_daemon [-i interval]
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
            # ignore other positional args — stock ids come from warrant_list.json
            LOGGER.debug(
                f"Ignoring CLI arg '{args[i]}' (stock ids loaded from warrant_list.json)"
            )
            i += 1

    try:
        with open(WARRANT_LIST_PATH, "r") as f:
            wl = json.load(f)
        stocks = list(wl.keys())
    except Exception as e:
        LOGGER.error(f"Failed to read warrant_list from {WARRANT_LIST_PATH}: {e}")
        sys.exit(1)

    if not stocks:
        LOGGER.error(f"No warrant ids found in {WARRANT_LIST_PATH}")
        sys.exit(1)
    return stocks, interval_str


if __name__ == "__main__":
    stock_ids, interval_str = parse_args()
    interval_seconds = INTERVAL_MAP[interval_str]
    # Default redis URL and channel; adjust with env vars or CLI wrapper if you want
    collector = MultiMarketCollector(
        stock_ids, interval_seconds, redis_url="redis://localhost:6379/0"
    )
    collector.run()
