"""
Base class for Avanza market collectors.

Subclasses only need to provide:
  - sse_base_url           (class attribute, e.g. "https://www.avanza.se/_push/quote-web-push/")
  - default_instrument_list_path  (class attribute)
  - default_redis_channel  (class attribute)
  - logger_name            (class attribute)
  - _init_price_tracking() (hook, called at end of __init__)
  - _sse_callback()        (async method: the per-event handler)
"""

import asyncio
import json
import copy
import logging
import signal
import threading
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from functools import partial
from typing import Optional

import redis.asyncio as aioredis

from .modules.avanza_instance import get_avanza
from .modules.avanza_sse_client import AvanzaSSEClient as SSEClient
from .modules.instrument_data import InstrumentData
from .utils.utils import save_json_atomic, flatten_instrument_list


class BaseMarketCollector:
    """
    Abstract base for SSE-based market collectors.

    Provides: init, market-hours, SSE client loop, periodic saving,
    disk load/restore, force-save, graceful shutdown, run/stop entry points,
    and a helper to publish bar updates to Redis.

    Subclasses must set the class attributes and implement the two methods
    listed in the module docstring.
    """

    # ── override in subclass ──────────────────────────────────────────
    sse_base_url: str = ""
    default_instrument_list_path: str = ""
    default_redis_channel: str = ""
    logger_name: str = "base_market_collector"

    # ── constructor ───────────────────────────────────────────────────

    def __init__(
        self,
        interval_seconds,
        instrument_list_path=None,
        instrument_datas: dict = None,
        instrument_list: dict = None,
        redis_url: str = "redis://localhost:6379/0",
        redis_channel: str = None,
        completed_save_interval: Optional[float] = None,
        current_snapshot_interval: float = 3.0,
    ):
        self.logger = logging.getLogger(self.logger_name)

        self.interval_seconds = interval_seconds

        if instrument_list is not None:
            self.instrument_list = flatten_instrument_list(instrument_list)
        else:
            instrument_list_path = (
                instrument_list_path or self.default_instrument_list_path
            )
            loaded_list = json.load(open(instrument_list_path))
            self.instrument_list = flatten_instrument_list(loaded_list)
        self.instrument_ids = list(self.instrument_list.keys())

        # per-instrument storage objects
        if instrument_datas is not None:
            self.instrument_data = {
                sid: instrument_datas[sid] for sid in self.instrument_ids
            }
        else:
            self.instrument_data = {
                sid: InstrumentData(interval_seconds, sid)
                for sid in self.instrument_ids
            }

        # market open/close detection
        self.last_market_check = {sid: False for sid in self.instrument_ids}
        self.anomaly_buffer = {sid: [] for sid in self.instrument_ids}

        # validate product list (and resolve product ids)
        self.orderbook_ids = {}
        for sid in self.instrument_ids:
            info = self.instrument_list.get(sid, {})
            obid = info.get("orderbookId")
            if not obid:
                raise ValueError(f"orderbookId not found for {sid}")
            self.orderbook_ids[sid] = obid

        # track tasks & clients for graceful shutdown
        self._tasks = []
        self._sse_clients = {}
        self._avanza = None
        self._shutting_down = False
        self._loop = None

        # Redis pub/sub config
        self.redis_url = redis_url
        self.redis_channel = redis_channel or self.default_redis_channel
        self._redis = None

        # saver config
        self.current_snapshot_interval = current_snapshot_interval
        self.completed_save_interval = (
            completed_save_interval
            if completed_save_interval is not None
            else max(30.0, float(self.interval_seconds))
        )

        # bookkeeping to limit snapshot IO
        self._dirty_current = set()
        self._last_current_snapshot = datetime.now(timezone.utc)

        # hook: subclass sets up its own price-tracking dicts
        self._init_price_tracking()

    # ── hook for subclasses ───────────────────────────────────────────

    def _init_price_tracking(self):
        """Override to initialise price-tracking attributes (e.g. last_buy_price)."""

    async def _sse_callback(self, instrument_id, _id, event, data):
        """Override: the per-SSE-event handler. Signature matches SSEClient listener."""
        raise NotImplementedError

    # ── Market-hours helpers ──────────────────────────────────────────

    def _market_window_utc_for_local_date(self, instrument_id, local_date):
        """
        Returns (open_utc, close_utc) for the given instrument_id and local_date.
        Handles overnight sessions where close <= open.
        """
        info = self.instrument_list[instrument_id]
        tzname = info.get("timezone", "UTC")
        zone = ZoneInfo(tzname)

        mo = info.get("market_open", "00:00")
        mc = info.get("market_close", "23:59")
        try:
            oh, om = (int(x) for x in mo.split(":"))
            ch, cm = (int(x) for x in mc.split(":"))
        except Exception:
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

        if local_close <= local_open:
            local_close = local_close + timedelta(days=1)

        return local_open.astimezone(timezone.utc), local_close.astimezone(timezone.utc)

    def is_market_open(self, instrument_id, dt_utc: datetime):
        """Check if market for instrument_id is open at dt_utc."""
        if dt_utc.tzinfo is None:
            dt_utc = dt_utc.replace(tzinfo=timezone.utc)
        else:
            dt_utc = dt_utc.astimezone(timezone.utc)

        info = self.instrument_list[instrument_id]
        tzname = info.get("timezone", "UTC")
        zone = ZoneInfo(tzname)
        local_dt = dt_utc.astimezone(zone)

        if local_dt.weekday() >= 5:
            return False

        local_date = local_dt.date()
        open_utc, close_utc = self._market_window_utc_for_local_date(
            instrument_id, local_date
        )
        return (open_utc <= dt_utc) and (dt_utc <= close_utc)

    # ── Redis bar-publish helper ──────────────────────────────────────

    async def _publish_bar_update(self, instrument_id, extra_meta=None):
        """
        Build and publish a current-bar update + completed-bar notification to Redis.
        `extra_meta` is merged into the 'meta' dict of the update message.
        """
        try:
            with self.instrument_data[instrument_id].lock:
                curr = copy.deepcopy(
                    self.instrument_data[instrument_id].current_bars.get(instrument_id)
                )
                latest_completed = (
                    copy.deepcopy(
                        self.instrument_data[instrument_id].completed_ohlc[
                            instrument_id
                        ][-1]
                    )
                    if (
                        self.instrument_data[instrument_id].completed_ohlc.get(
                            instrument_id
                        )
                        and len(
                            self.instrument_data[instrument_id].completed_ohlc[
                                instrument_id
                            ]
                        )
                        > 0
                    )
                    else None
                )

            if curr:
                meta = {
                    "timezone": self.instrument_list[instrument_id].get(
                        "timezone", "UTC"
                    ),
                    "market_open": self.instrument_list[instrument_id].get(
                        "market_open", "00:00"
                    ),
                    "market_close": self.instrument_list[instrument_id].get(
                        "market_close", "23:59"
                    ),
                }
                if extra_meta:
                    meta.update(extra_meta)

                msg = {
                    "type": "update",
                    "instrument": instrument_id,
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
                    "meta": meta,
                }

                if self._redis is not None:
                    try:
                        await self._redis.publish(self.redis_channel, json.dumps(msg))
                        self.logger.debug(
                            f"[{instrument_id}] Published update to Redis"
                        )
                    except Exception as e:
                        self.logger.error(
                            f"[{instrument_id}] Failed to publish to Redis: {e}"
                        )
                else:
                    self.logger.warning(
                        f"[{instrument_id}] Redis not connected - skipping publish"
                    )

            # Publish completed bar if detected
            if latest_completed and curr:
                try:
                    if latest_completed["end_time"] <= curr["start_time"]:
                        msg_completed = {
                            "type": "completed",
                            "instrument": instrument_id,
                            "bar": {
                                "start_time": latest_completed[
                                    "start_time"
                                ].isoformat(),
                                "end_time": latest_completed["end_time"].isoformat(),
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
                                self.logger.debug(
                                    f"[{instrument_id}] Published completed bar to Redis"
                                )
                            except Exception as e:
                                self.logger.error(
                                    f"[{instrument_id}] Failed to publish completed bar to Redis: {e}"
                                )
                except Exception as e:
                    self.logger.exception(
                        f"[{instrument_id}] Error detecting/publishing completed bar: {e}"
                    )

        except Exception as e:
            self.logger.exception(
                f"[{instrument_id}] Failed to build or publish update message: {e}"
            )

    # ── Periodic saver ────────────────────────────────────────────────

    async def periodic_saver(self):
        """
        Save all instrument OHLCs to disk periodically.
        Also save current (in-progress) bars every `current_snapshot_interval` seconds.
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
                dirty = list(self._dirty_current)
                for sid in dirty:
                    try:
                        sd = self.instrument_data[sid]
                        with sd.lock:
                            curr = sd.current_bars.get(sid)
                        if not curr:
                            self._dirty_current.discard(sid)
                            continue
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
                        try:
                            self._dirty_current.discard(sid)
                        except Exception:
                            pass
                    except Exception as e:
                        self.logger.error(
                            f"[{sid}] Failed to snapshot current bar: {e}"
                        )
                last_current_save = now_utc

            # save completed bars less frequently
            if (
                now_utc - last_completed_save
            ).total_seconds() >= self.completed_save_interval:
                for sid, sd in self.instrument_data.items():
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
                        save_json_atomic(data_file, data)
                        self.logger.debug(
                            f"[{sid}] Saved {len(data)} bars to {data_file}"
                        )
                    except Exception as e:
                        self.logger.error(f"[{sid}] Failed to save OHLC: {e}")
                last_completed_save = now_utc

    # ── Disk load / restore ───────────────────────────────────────────

    def load_all_ohlc_from_disk(self):
        for sid, sd in self.instrument_data.items():
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
                self.logger.info(f"[{sid}] Loaded {len(bars)} bars from {data_file}")
            except FileNotFoundError:
                self.logger.warning(f"[{sid}] No previous data file {data_file}.")
            except Exception as e:
                self.logger.error(f"[{sid}] Failed to load OHLC data: {e}")

        # attempt to load current bar snapshots
        for sid, sd in self.instrument_data.items():
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
                with sd.lock:
                    sd.current_bars[sid] = curr
                self.logger.info(f"[{sid}] Restored in-progress bar from {snap_file}")
            except FileNotFoundError:
                pass
            except Exception as e:
                self.logger.error(f"[{sid}] Failed to restore current snapshot: {e}")

    # ── SSE client loop ───────────────────────────────────────────────

    async def _run_sse_client_loop(self, avanza, instrument_id, product_id):
        while True:
            if self._shutting_down:
                self.logger.info(
                    f"[{instrument_id}] Shutdown requested — exiting _run_sse_client_loop."
                )
                break

            client = None
            try:
                client = SSEClient(avanza, self.sse_base_url + product_id)
                self._sse_clients[instrument_id] = client
                client.add_listener(partial(self._sse_callback, instrument_id))
                self.logger.info(
                    f"[{instrument_id}] Starting SSE client for product {product_id}"
                )
                await client.start()
                self.logger.info(
                    f"[{instrument_id}] SSE client stopped cleanly (will reconnect)."
                )

                if self._shutting_down:
                    self.logger.info(
                        f"[{instrument_id}] Shutdown requested after client stopped — exiting loop."
                    )
                    self._sse_clients.pop(instrument_id, None)
                    break

            except asyncio.CancelledError:
                self.logger.info(
                    f"[{instrument_id}] _run_sse_client_loop cancelled: attempting client stop."
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
                    self.logger.debug(
                        f"[{instrument_id}] Exception while stopping client on cancel: {e}"
                    )
                finally:
                    self._sse_clients.pop(instrument_id, None)
                    raise
            except Exception as e:
                self.logger.error(
                    f"[{instrument_id}] SSE client error: {e}. Reconnecting in 5s..."
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
                self._sse_clients.pop(instrument_id, None)
                if self._shutting_down:
                    self.logger.info(
                        f"[{instrument_id}] Shutdown requested during error; exiting client loop."
                    )
                    break
                await asyncio.sleep(5)

    # ── Main market loop ──────────────────────────────────────────────

    async def real_market_loop(self):
        """
        Create one Avanza instance and start an SSE client loop for every instrument.
        """
        try:
            self._redis = aioredis.from_url(self.redis_url)
            await self._redis.ping()
            self.logger.info("Redis connected for publishing.")
        except Exception as e:
            self.logger.error(f"Failed to connect to Redis at {self.redis_url}: {e}")
            self._redis = None

        while True:
            if self._shutting_down:
                self.logger.info(
                    "real_market_loop: shutting down flag set — exiting loop."
                )
                break
            avanza = None
            try:
                avanza = get_avanza()
                self._avanza = avanza
                self.logger.info("Avanza login OK.")

                self._tasks = []
                for sid, obid in self.orderbook_ids.items():
                    t = asyncio.create_task(
                        self._run_sse_client_loop(avanza, sid, obid)
                    )
                    self._tasks.append(t)

                await asyncio.gather(*self._tasks)
            except Exception as e:
                self.logger.error(
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

    # ── Force save ────────────────────────────────────────────────────

    def force_save_instrument(self, instrument_id, timestamp: datetime = None):
        """Immediately save OHLC data for `instrument_id`."""
        ts = timestamp if timestamp is not None else datetime.now(timezone.utc)
        sd = self.instrument_data[instrument_id]
        try:
            with sd.lock:
                bars = copy.deepcopy(sd.completed_ohlc[instrument_id])
                current = sd.current_bars.get(instrument_id)
                if current:
                    curr_copy = copy.deepcopy(current)
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    curr_copy["end_time"] = ts
                    if curr_copy["start_time"].tzinfo is None:
                        curr_copy["start_time"] = curr_copy["start_time"].replace(
                            tzinfo=timezone.utc
                        )
                    bars.append(curr_copy)

            data = []
            for b in bars:
                bar = b.copy()
                bar["start_time"] = bar["start_time"].isoformat()
                bar["end_time"] = bar["end_time"].isoformat()
                data.append(bar)
            data_file = f"ohlc_{instrument_id}.json"
            save_json_atomic(data_file, data)
            self.logger.info(
                f"[{instrument_id}] Force-saved {len(data)} bars at {ts.isoformat()}"
            )
            if current:
                snap_file = f"ohlc_current_{instrument_id}.json"
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
            self.logger.error(f"[{instrument_id}] Failed force-save: {e}")

    def force_save_all(self, timestamp: datetime = None):
        """Force-save OHLC for all instruments immediately."""
        ts = timestamp if timestamp is not None else datetime.now(timezone.utc)
        self.logger.info(f"Force-saving all instruments at {ts.isoformat()}")
        for sid in list(self.instrument_data.keys()):
            try:
                self.force_save_instrument(sid, ts)
            except Exception as e:
                self.logger.error(f"[{sid}] Exception during force_save_all: {e}")

    # ── Shutdown ──────────────────────────────────────────────────────

    async def _shutdown(self, loop, signum):
        """
        Graceful shutdown: force-saves, stops clients, closes Avanza,
        cancels tasks and waits for them to finish before stopping the loop.
        """
        self.logger.info(
            f"Received signal {signum}. Initiating graceful shutdown: forcing save and cancelling tasks..."
        )
        self._shutting_down = True

        # 1) Force-save synchronously
        try:
            self.force_save_all()
        except Exception as e:
            self.logger.error(f"Error during force_save_all in shutdown: {e}")

        # 2) Stop SSE clients
        for sid, client in list(self._sse_clients.items()):
            try:
                self.logger.info(f"[{sid}] Stopping SSE client...")
                stop_fn = getattr(client, "stop", None) or getattr(
                    client, "close", None
                )
                if stop_fn:
                    res = stop_fn()
                    if asyncio.iscoroutine(res):
                        await res
            except Exception as e:
                self.logger.debug(f"[{sid}] Exception while stopping SSE client: {e}")
            finally:
                self._sse_clients.pop(sid, None)

        # 3) Close Avanza session
        if self._avanza is not None:
            try:
                close_fn = getattr(self._avanza, "close", None)
                if close_fn:
                    res = close_fn()
                    if asyncio.iscoroutine(res):
                        await res
                self._avanza = None
            except Exception as e:
                self.logger.debug(f"Exception while closing Avanza: {e}")

        # 4) Cancel outstanding tasks and await
        to_cancel = list(self._tasks) if self._tasks else []
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
            try:
                await asyncio.wait_for(
                    asyncio.gather(*to_cancel, return_exceptions=True), timeout=10.0
                )
            except asyncio.TimeoutError:
                self.logger.warning(
                    "Timeout while waiting for tasks to finish during shutdown."
                )

        # 5) stop the loop
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

    # ── Entry point ───────────────────────────────────────────────────

    def run(self):
        """
        Entry point: load disk data and start the asyncio loop.
        Registers signal handlers only if running in main thread.
        """
        self.load_all_ohlc_from_disk()
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)

        main_tasks = [
            loop.create_task(self.real_market_loop()),
            loop.create_task(self.periodic_saver()),
        ]
        self._tasks = main_tasks.copy()

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
                    self.logger.debug(
                        "run(): loop.add_signal_handler failed; skipping signal handler registration."
                    )
        else:
            self.logger.debug(
                "run(): not running in main thread — skipping signal handler registration (caller should call stop())."
            )

        try:
            loop.run_forever()
        except KeyboardInterrupt:
            self.logger.info("KeyboardInterrupt received in run()")
        finally:
            try:
                self.logger.info("Final force-save for all instruments (final cleanup)")
                self.force_save_all()
            except Exception as e:
                self.logger.error(f"Final force-save failed: {e}")
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
        Synchronous method to request graceful shutdown from another thread.
        """
        self.logger.info(
            "Stop requested (external). Setting shutting_down flag and scheduling shutdown."
        )
        self._shutting_down = True

        if not getattr(self, "_loop", None):
            self.logger.debug("stop(): no event loop reference; nothing to schedule.")
            return

        try:
            fut = asyncio.run_coroutine_threadsafe(
                self._shutdown(self._loop, "external"), self._loop
            )
            try:
                fut.result(timeout=timeout)
            except Exception as e:
                self.logger.debug(
                    f"stop(): shutdown coroutine finished/failed/timeout: {e}"
                )
        except Exception as e:
            self.logger.error(
                f"stop(): failed to schedule shutdown on collector loop: {e}"
            )
