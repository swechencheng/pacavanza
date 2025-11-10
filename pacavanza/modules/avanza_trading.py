import asyncio
import json
import logging
from typing import Dict, Any, List, Optional
import pandas as pd
# import pandas_ta as ta
from datetime import datetime, timedelta, timezone
from fastapi import HTTPException

logging.basicConfig(level=logging.INFO)
logging.getLogger("avanza_trading").setLevel(logging.INFO)
LOGGER = logging.getLogger("avanza_trading")

class AvanzaTrading:
    """
    AvanzaTrading class (simulated) that schedules and logs orders using the same in-memory data
    (recent_bars, metadata, instrument_list). It does not place real orders — it only logs
    the simulated order details.

    It expects:
      - recent_bars: Dict[instrument, List[bar_dict]] (bar_dict fields: start_time (dt), end_time (dt), open, high, low, close, volume)
      - instrument_list: dict loaded from instrument_list.json
      - metadata: optional instrument metadata dict
      - logger: logging.Logger
      - bars_locks_map: Dict[str, asyncio.Lock] for per-instrument bars lock
      - metadata_locks_map: Dict[str, asyncio.Lock] for per-instrument metadata lock
      - map locks to protect creation: bars_map_lock, metadata_map_lock
    """

    def __init__(
        self,
        recent_bars: Dict[str, List[Dict[str, Any]]],
        instrument_list: Dict[str, Any],
        metadata: Dict[str, Dict[str, Any]],
        logger: logging.Logger,
        bars_locks_map: Optional[Dict[str, asyncio.Lock]] = None,
        metadata_locks_map: Optional[Dict[str, asyncio.Lock]] = None,
        bars_map_lock: Optional[asyncio.Lock] = None,
        metadata_map_lock: Optional[asyncio.Lock] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        self.recent_bars = recent_bars
        self.instrument_list = instrument_list
        self.metadata = metadata
        self.logger = logger
        self.loop = loop or asyncio.get_event_loop()
        # per-instrument lock maps
        self._bars_locks_map: Dict[str, asyncio.Lock] = bars_locks_map or {}
        self._metadata_locks_map: Dict[str, asyncio.Lock] = metadata_locks_map or {}
        # map-protection locks for creating per-instrument locks safely
        self._bars_map_lock: asyncio.Lock = bars_map_lock or asyncio.Lock()
        self._metadata_map_lock: asyncio.Lock = metadata_map_lock or asyncio.Lock()
        # scheduled tasks per instrument: { instrument_id: { "buy_stop": {"task": task,"bar_start": dt}, "sell_stop": {...} } }
        self._scheduled_tasks: Dict[str, Dict[str, Dict[str, Any]]] = {}

    # helper: validate instrument exists in instrument_list.json.
    def _get_instrument_info(self, instrument_id: str) -> Dict[str, Any]:
        info = self.instrument_list.get(instrument_id)
        if not info:
            raise HTTPException(
                status_code=400, detail=f"Unknown instrument: {instrument_id}"
            )
        return info

    # helper: get tick_size (float)
    def _get_tick_size(self, instrument_id: str) -> float:
        info = self._get_instrument_info(instrument_id)
        tick_sz = info.get("tick_size")
        if not tick_sz:
            raise ValueError(f"No tick_size for {instrument_id}")
        return float(tick_sz)

    # helper: get tick_coefficient (float)
    def _get_tick_coeff(self, instrument_id: str) -> float:
        info = self._get_instrument_info(instrument_id)
        tick_coeff = info.get("tick_coefficient")
        if not tick_coeff:
            raise ValueError(f"No tick_coefficient for {instrument_id}")
        return float(tick_coeff)

    # helper: prune bars older than 7 days for memory saving (called under lock by caller)
    def prune_old_bars_snapshot(
        self, lst: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        if not lst:
            return lst
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=7)
        try:
            return [
                b for b in lst if b["start_time"].astimezone(timezone.utc) >= cutoff
            ]
        except Exception:
            return lst

    # helper: get/create per-instrument bars lock
    async def _get_bars_lock(self, instrument_id: str) -> asyncio.Lock:
        # fast path
        lock = self._bars_locks_map.get(instrument_id)
        if lock:
            return lock
        # create under map lock
        async with self._bars_map_lock:
            lock = self._bars_locks_map.get(instrument_id)
            if not lock:
                lock = asyncio.Lock()
                self._bars_locks_map[instrument_id] = lock
            return lock

    # helper: get/create per-instrument metadata lock
    async def _get_metadata_lock(self, instrument_id: str) -> asyncio.Lock:
        lock = self._metadata_locks_map.get(instrument_id)
        if lock:
            return lock
        async with self._metadata_map_lock:
            lock = self._metadata_locks_map.get(instrument_id)
            if not lock:
                lock = asyncio.Lock()
                self._metadata_locks_map[instrument_id] = lock
            return lock

    # helper: create a snapshot copy under per-instrument lock for safe outside-lock processing
    async def _get_snapshot(self, instrument_id: str) -> List[Dict[str, Any]]:
        lock = await self._get_bars_lock(instrument_id)
        async with lock:
            lst = list(self.recent_bars.get(instrument_id, []))
        return lst

    # helper: snapshot metadata safely via per-instrument lock
    async def _get_metadata_snapshot(self, instrument_id: str) -> Dict[str, Any]:
        lock = await self._get_metadata_lock(instrument_id)
        async with lock:
            meta = dict(self.metadata.get(instrument_id, {}))
        return meta

    # helper: find last completed bar index (end_time <= now) using a provided snapshot
    def _last_completed_index_from_snapshot(
        self, lst: List[Dict[str, Any]]
    ) -> Optional[int]:
        if not lst:
            return None
        now = datetime.now(tz=timezone.utc)
        for i in range(len(lst) - 1, -1, -1):
            b = lst[i]
            end = b["end_time"]
            if end.tzinfo is None:
                end = end.replace(tzinfo=timezone.utc)
            if end <= now:
                return i
        return None

    # helper: get last buy/sell price from metadata or fallback to last bar close (reads snapshot if needed)
    async def _get_last_sell_price(self, instrument_id: str) -> Optional[float]:
        meta = await self._get_metadata_snapshot(instrument_id)
        # try common keys
        for key in ("last_sell", "lastAsk", "ask", "last_ask"):
            if meta.get(key) is not None:
                try:
                    return float(meta[key])
                except Exception:
                    pass
        lst = await self._get_snapshot(instrument_id)
        if lst:
            return float(lst[-1]["close"])
        return None

    async def _get_last_buy_price(self, instrument_id: str) -> Optional[float]:
        meta = await self._get_metadata_snapshot(instrument_id)
        for key in ("last_buy", "lastBid", "bid", "last_bid"):
            if meta.get(key) is not None:
                try:
                    return float(meta[key])
                except Exception:
                    pass
        lst = await self._get_snapshot(instrument_id)
        if lst:
            return float(lst[-1]["close"])
        return None

    # helper: find consecutive bull leg low using pandas/pandas_ta on a snapshot
    def _find_consecutive_bull_leg_low_from_snapshot(
        self, lst: List[Dict[str, Any]], end_index: Optional[int] = None
    ) -> float:
        """
        Return low price of consecutive bull bar(s) ending at end_index within the provided snapshot.
        If none found, return low of the bar at end_index (fallback).
        """
        if not lst:
            raise HTTPException(
                status_code=400, detail="No bar data available for instrument"
            )

        if end_index is None:
            end_index = len(lst) - 1

        # guard indexes
        end_index = min(end_index, len(lst) - 1)
        if end_index < 0:
            raise HTTPException(status_code=400, detail="No completed bars")

        # Build a small DataFrame for TA usage (though we just need bull sequences)
        df = pd.DataFrame(
            {
                "open": [b["open"] for b in lst],
                "high": [b["high"] for b in lst],
                "low": [b["low"] for b in lst],
                "close": [b["close"] for b in lst],
            }
        )

        # Determine bull bars: close > open
        bull_mask = df["close"] > df["open"]

        # Walk backwards from end_index and collect consecutive bulls
        i = end_index
        bull_indices = []
        while i >= 0 and bull_mask.iloc[i]:
            bull_indices.append(i)
            i -= 1

        if not bull_indices:
            # fallback to low at end_index
            return float(lst[end_index]["low"])
        lows = [lst[idx]["low"] for idx in bull_indices]
        return float(min(lows))

    # Simulated order logger
    def _log_order(self, order: Dict[str, Any]):
        # include timestamp
        order_out = dict(order)
        order_out.setdefault("ts", datetime.now(tz=timezone.utc).isoformat())
        self.logger.info("Simulated order: %s", json.dumps(order_out, default=str))

    # scheduled-task helpers: set/clear/check under per-instrument bars lock
    async def _get_scheduled_for(self, instrument_id: str) -> Dict[str, Dict[str, Any]]:
        # ensure dict exists
        lock = await self._get_bars_lock(instrument_id)
        async with lock:
            d = self._scheduled_tasks.get(instrument_id)
            if d is None:
                d = {}
                self._scheduled_tasks[instrument_id] = d
            # return a shallow copy to avoid external mutation
            return dict(d)

    async def _set_scheduled(
        self, instrument_id: str, kind: str, bar_start: datetime, task: asyncio.Task
    ):
        lock = await self._get_bars_lock(instrument_id)
        async with lock:
            d = self._scheduled_tasks.get(instrument_id)
            if d is None:
                d = {}
                self._scheduled_tasks[instrument_id] = d
            d[kind] = {"task": task, "bar_start": bar_start}

    async def _clear_scheduled(self, instrument_id: str, kind: str):
        lock = await self._get_bars_lock(instrument_id)
        async with lock:
            d = self._scheduled_tasks.get(instrument_id)
            if not d:
                return
            if kind in d:
                # attempt cancellation removal only; do not cancel if already running
                d.pop(kind, None)
            if not d:
                # cleanup empty map
                self._scheduled_tasks.pop(instrument_id, None)

    async def _cancel_scheduled(self, instrument_id: str, kind: str) -> bool:
        """
        Cancel scheduled task for instrument/kind if present and not done.
        Returns True if we cancelled something, False otherwise.
        """
        lock = await self._get_bars_lock(instrument_id)
        async with lock:
            d = self._scheduled_tasks.get(instrument_id)
            if not d:
                return False
            entry = d.get(kind)
            if not entry:
                return False
            task: asyncio.Task = entry.get("task")
            if task and not task.done():
                task.cancel()
                # remove entry
                d.pop(kind, None)
                if not d:
                    self._scheduled_tasks.pop(instrument_id, None)
                return True
            else:
                # task already done
                d.pop(kind, None)
                if not d:
                    self._scheduled_tasks.pop(instrument_id, None)
                return False

    # Public API methods used by endpoints

    async def place_market_buy(
        self, instrument_id: str, volume: float
    ) -> Dict[str, Any]:
        # instrumentId should be inside instrument_list.json.
        info = self._get_instrument_info(instrument_id)
        # Place a market buy order, using input parameters: instrumentId, volume.
        # The price should be the current last sell price from the redis data.
        price = await self._get_last_sell_price(instrument_id)
        if price is None:
            raise HTTPException(status_code=400, detail="No price data available")
        order = {
            "side": "buy",
            "type": "market",
            "instrument": instrument_id,
            "volume": volume,
            "price": float(price),
            "note": "market buy simulated (price: last sell)",
        }
        self._log_order(order)
        return order

    async def place_market_sell(
        self, instrument_id: str, volume: float
    ) -> Dict[str, Any]:
        info = self._get_instrument_info(instrument_id)
        price = await self._get_last_buy_price(instrument_id)
        if price is None:
            raise HTTPException(status_code=400, detail="No price data available")
        order = {
            "side": "sell",
            "type": "market",
            "instrument": instrument_id,
            "volume": volume,
            "price": float(price),
            "note": "market sell simulated (price: last buy)",
        }
        self._log_order(order)
        return order

    async def schedule_buy_stop(
        self, instrument_id: str, volume: float
    ) -> Dict[str, Any]:
        """
        Place a buy stop order:
        - Wait for the current bar to complete. Exactly at the 0 second of the new bar,
          calculate the stop price = 1 tick_size above the high of the last completed bar.
        - Place buy stop at stop price.
        - Also place a sell stop at 1 tick_size below the low of the current swing leg (consecutive bull bars).
        - Use profit ratio 2:1 to compute take-profit and place sell limit:
          take-profit = high_last_completed + 2*(high_last_completed - low_swing_leg) - 1 tick_size.
        - Log orders.
        - If a buy_stop is already scheduled for the same instrument for the same next bar boundary, ignore duplicate calls.
        """
        info = self._get_instrument_info(instrument_id)
        tick = self._get_tick_size(instrument_id)
        tick_coeff = self._get_tick_coeff(instrument_id)

        # compute next bar start aligned to 5-minute grid (bar interval is 5m)
        now = datetime.now(tz=timezone.utc)
        minute = (now.minute // 5) * 5
        bar_start = now.replace(minute=minute, second=0, microsecond=0)
        # next bar start:
        next_bar_start = bar_start + timedelta(minutes=5)

        # check if already scheduled for this instrument/kind at the same bar
        existing = await self._get_scheduled_for(instrument_id)
        entry = existing.get("buy_stop")
        if (
            entry
            and entry.get("bar_start") == next_bar_start
            and not entry.get("task").done()
        ):
            return {
                "status": "already_scheduled",
                "note": "buy_stop already scheduled for this bar",
            }

        # schedule background task that waits until the current bar completes
        async def _task():
            try:
                now_inner = datetime.now(tz=timezone.utc)
                minute_inner = (now_inner.minute // 5) * 5
                bar_start_inner = now_inner.replace(
                    minute=minute_inner, second=0, microsecond=0
                )
                next_bar_start_inner = bar_start_inner + timedelta(minutes=5)
                sleep_seconds = (next_bar_start_inner - now_inner).total_seconds()
                if sleep_seconds > 0:
                    await asyncio.sleep(sleep_seconds)
                # now exactly at 0 second of new bar; take a snapshot
                lst = await self._get_snapshot(instrument_id)
                if not lst:
                    self.logger.warning("No last completed bar to schedule buy_stop")
                    return
                # last completed bar index in snapshot
                last_idx = self._last_completed_index_from_snapshot(lst)
                if last_idx is None:
                    self.logger.warning("No last completed bar found in snapshot")
                    return
                last_bar = lst[last_idx]
                high_last = float(last_bar["high"])
                stop_price = high_last + (tick * tick_coeff)
                # Need to add an extra tick to match sell side since the data is only buy side
                stop_price += tick
                stop_price = round(stop_price, 2)
                # swing leg low (consecutive bull bars ending at last_completed_idx)
                try:
                    low_swing = self._find_consecutive_bull_leg_low_from_snapshot(
                        lst, end_index=last_idx
                    )
                except Exception:
                    low_swing = float(last_bar["low"])
                sell_stop_price = round(low_swing - tick, 2)
                # take-profit calculation
                distance = high_last - low_swing
                take_profit = round(high_last + 2 * distance - tick, 2)

                buy_order = {
                    "side": "buy",
                    "type": "stop",
                    "instrument": instrument_id,
                    "volume": volume,
                    "stop_price": float(stop_price),
                    "note": "scheduled buy stop (placed at new bar 0s)",
                }
                sell_stop_order = {
                    "side": "sell",
                    "type": "stop",
                    "instrument": instrument_id,
                    "volume": volume,
                    "stop_price": float(sell_stop_price),
                    "note": "scheduled sell stop (stop-loss)",
                }
                take_profit_order = {
                    "side": "sell",
                    "type": "limit",
                    "instrument": instrument_id,
                    "volume": volume,
                    "price": float(take_profit),
                    "note": "scheduled take-profit (2:1)",
                }

                # Log them (simulate immediate placement)
                self._log_order(buy_order)
                self._log_order(sell_stop_order)
                self._log_order(take_profit_order)
            except asyncio.CancelledError:
                self.logger.info("Scheduled buy_stop cancelled for %s", instrument_id)
                raise
            except Exception as e:
                self.logger.exception("Error in scheduled buy_stop: %s", e)
            finally:
                # cleanup scheduled entry
                await self._clear_scheduled(instrument_id, "buy_stop")

        # create the asyncio task and register it
        task = asyncio.create_task(_task())
        await self._set_scheduled(instrument_id, "buy_stop", next_bar_start, task)
        return {"status": "scheduled", "bar_start": next_bar_start.isoformat()}

    async def late_buy_stop(self, instrument_id: str, volume: float) -> Dict[str, Any]:
        """
        Place a late buy stop order immediately:
        - Immediately calculate stop price = 1 tick_size above high of last completed bar.
        - Place buy stop immediately.
        - Sell stop (stop-loss) at 1 tick_size below low of current swing leg (excluding current bar which is not completed).
        - Take profit same formula but using last completed bar high.
        - Log orders.
        """
        info = self._get_instrument_info(instrument_id)
        tick = self._get_tick_size(instrument_id)
        tick_coeff = self._get_tick_coeff(instrument_id)

        lst = await self._get_snapshot(instrument_id)
        if not lst:
            raise HTTPException(status_code=400, detail="No completed bar available")
        last_idx = self._last_completed_index_from_snapshot(lst)
        if last_idx is None:
            raise HTTPException(status_code=400, detail="No completed bar available")
        last_bar = lst[last_idx]
        high_last = float(last_bar["high"])
        stop_price = high_last + tick * tick_coeff
        # Need to add an extra tick to match sell side since the data is only buy side
        stop_price += tick
        stop_price = round(stop_price, 2)
        # swing leg excluding the current bar which is not completed => use end_index = last_idx (already excludes in-progress)
        try:
            low_swing = self._find_consecutive_bull_leg_low_from_snapshot(
                lst, end_index=last_idx
            )
        except Exception:
            low_swing = float(last_bar["low"])
        sell_stop_price = round(low_swing - tick, 2)
        distance = high_last - low_swing
        take_profit = round(high_last + 2 * distance - tick, 2)

        buy_order = {
            "side": "buy",
            "type": "stop",
            "instrument": instrument_id,
            "volume": volume,
            "stop_price": float(stop_price),
            "note": "late buy stop placed immediately (using last completed bar)",
        }
        sell_stop_order = {
            "side": "sell",
            "type": "stop",
            "instrument": instrument_id,
            "volume": volume,
            "stop_price": float(sell_stop_price),
            "note": "late buy stop stop-loss",
        }
        take_profit_order = {
            "side": "sell",
            "type": "limit",
            "instrument": instrument_id,
            "volume": volume,
            "price": float(take_profit),
            "note": "late buy stop take-profit (2:1)",
        }

        # Log them (simulate immediate placement)
        self._log_order(buy_order)
        self._log_order(sell_stop_order)
        self._log_order(take_profit_order)
        return {
            "status": "placed",
            "orders": [buy_order, sell_stop_order, take_profit_order],
        }

    async def schedule_sell_stop(
        self, instrument_id: str, volume: float
    ) -> Dict[str, Any]:
        """
        Place a sell stop order:
        - Wait for current bar to complete. Exactly at new bar 0s, stop price = 1 tick_size below low of last completed bar.
        - Place sell stop order immediately at that stop price.
        - If a sell_stop is already scheduled for the same instrument for the same next bar boundary, ignore duplicate calls.
        """
        info = self._get_instrument_info(instrument_id)
        tick = self._get_tick_size(instrument_id)
        tick_coeff = self._get_tick_coeff(instrument_id)

        # compute next bar start aligned to 5-minute grid (bar interval is 5m)
        now = datetime.now(tz=timezone.utc)
        minute = (now.minute // 5) * 5
        bar_start = now.replace(minute=minute, second=0, microsecond=0)
        # next bar start:
        next_bar_start = bar_start + timedelta(minutes=5)

        # check if already scheduled for this instrument/kind at the same bar
        existing = await self._get_scheduled_for(instrument_id)
        entry = existing.get("sell_stop")
        if (
            entry
            and entry.get("bar_start") == next_bar_start
            and not entry.get("task").done()
        ):
            return {
                "status": "already_scheduled",
                "note": "sell_stop already scheduled for this bar",
            }

        async def _task():
            try:
                now_inner = datetime.now(tz=timezone.utc)
                minute_inner = (now_inner.minute // 5) * 5
                bar_start_inner = now_inner.replace(
                    minute=minute_inner, second=0, microsecond=0
                )
                next_bar_start_inner = bar_start_inner + timedelta(minutes=5)
                sleep_seconds = (next_bar_start_inner - now_inner).total_seconds()
                if sleep_seconds > 0:
                    await asyncio.sleep(sleep_seconds)
                lst = await self._get_snapshot(instrument_id)
                if not lst:
                    self.logger.warning("No last completed bar to schedule sell_stop")
                    return
                last_idx = self._last_completed_index_from_snapshot(lst)
                if last_idx is None:
                    self.logger.warning("No last completed bar to schedule sell_stop")
                    return
                last_bar = lst[last_idx]
                low_last = float(last_bar["low"])
                stop_price = round(low_last - tick * tick_coeff, 2)
                sell_order = {
                    "side": "sell",
                    "type": "stop",
                    "instrument": instrument_id,
                    "volume": volume,
                    "stop_price": float(stop_price),
                    "note": "scheduled sell stop (placed at new bar 0s)",
                }
                self._log_order(sell_order)
            except asyncio.CancelledError:
                self.logger.info("Scheduled sell_stop cancelled for %s", instrument_id)
                raise
            except Exception as e:
                self.logger.exception("Error in scheduled sell_stop: %s", e)
            finally:
                # cleanup scheduled entry
                await self._clear_scheduled(instrument_id, "sell_stop")

        task = asyncio.create_task(_task())
        await self._set_scheduled(instrument_id, "sell_stop", next_bar_start, task)
        return {"status": "scheduled", "bar_start": next_bar_start.isoformat()}

    async def late_sell_stop(self, instrument_id: str, volume: float) -> Dict[str, Any]:
        """
        Place a late sell stop order immediately:
        - Immediately calculate stop price = 1 tick_size below low of last completed bar.
        - Place sell stop at that price.
        """
        info = self._get_instrument_info(instrument_id)
        tick = self._get_tick_size(instrument_id)
        tick_coeff = self._get_tick_coeff(instrument_id)

        lst = await self._get_snapshot(instrument_id)
        if not lst:
            raise HTTPException(status_code=400, detail="No completed bar available")
        last_idx = self._last_completed_index_from_snapshot(lst)
        if last_idx is None:
            raise HTTPException(status_code=400, detail="No completed bar available")
        last_bar = lst[last_idx]
        low_last = float(last_bar["low"])
        stop_price = round(low_last - tick * tick_coeff, 2)
        sell_order = {
            "side": "sell",
            "type": "stop",
            "instrument": instrument_id,
            "volume": volume,
            "stop_price": float(stop_price),
            "note": "late sell stop placed immediately",
        }
        self._log_order(sell_order)
        return {"status": "placed", "order": sell_order}

    # cancellation API helpers
    async def cancel_buy_stop(self, instrument_id: str) -> Dict[str, Any]:
        cancelled = await self._cancel_scheduled(instrument_id, "buy_stop")
        return {"cancelled": cancelled}

    async def cancel_sell_stop(self, instrument_id: str) -> Dict[str, Any]:
        cancelled = await self._cancel_scheduled(instrument_id, "sell_stop")
        return {"cancelled": cancelled}
