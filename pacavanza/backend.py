import asyncio
import json
import logging
from typing import Dict, Any, List, AsyncIterator, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request
from fastapi.responses import JSONResponse, HTMLResponse, Response, FileResponse
from fastapi.staticfiles import StaticFiles
import uvicorn
import redis.asyncio as aioredis
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from pathlib import Path
from contextlib import asynccontextmanager

# compute pacavanza package root (pacavanza/)
ROOT = Path(__file__).resolve().parent  # pacavanza/
STATIC_DIR = ROOT / "static"
STATIC_HTML = STATIC_DIR / "chart.html"

from .indicators.indicators import (
    incremental_ema_update,
)

# NEW: pandas + pandas_ta for swing detection
import pandas as pd
# import pandas_ta as ta

logging.basicConfig(level=logging.INFO)
logging.getLogger("main").setLevel(logging.INFO)
LOGGER = logging.getLogger("main")


class WebSocketManager:
    def __init__(self):
        self._conns: List[WebSocket] = []
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self._lock:
            self._conns.append(ws)
        LOGGER.info("WebSocket connected (total=%d)", len(self._conns))

    async def disconnect(self, ws: WebSocket):
        async with self._lock:
            if ws in self._conns:
                self._conns.remove(ws)
        LOGGER.info("WebSocket disconnected (total=%d)", len(self._conns))

    async def broadcast(self, message: Dict[str, Any]):
        text = json.dumps(message, default=str)
        async with self._lock:
            conns = list(self._conns)
        for ws in conns:
            try:
                await ws.send_text(text)
            except Exception:
                LOGGER.debug(
                    "Failed to send ws message, removing connection", exc_info=True
                )
                try:
                    await self.disconnect(ws)
                except Exception:
                    pass


# NEW: AvanzaTrading class to provide trading endpoints (simulated: logs orders)
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
        return float(info.get("tick_size", 0.01))

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

    # small rounding to tick grid
    def _round_to_tick(self, price: float, tick: float, ceil: bool = False) -> float:
        # avoid floating rounding surprises
        if tick == 0:
            return price
        # Use integer math to avoid floats as much as possible
        q = price / tick
        if ceil:
            q = float(int(price // tick) + (1 if price % tick != 0 else 0))
            return round(q * tick, 8)
        else:
            q = round(q)
            return round(q * tick, 8)

    # Simulated order logger
    def _log_order(self, order: Dict[str, Any]):
        # include timestamp
        order_out = dict(order)
        order_out.setdefault("ts", datetime.now(tz=timezone.utc).isoformat())
        self.logger.info("Simulated order: %s", json.dumps(order_out, default=str))

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
        """
        info = self._get_instrument_info(instrument_id)
        tick = self._get_tick_size(instrument_id)

        # schedule background task that waits until the current bar completes
        async def _task():
            try:
                # compute next bar start aligned to 5-minute grid (bar interval is 5m)
                now = datetime.now(tz=timezone.utc)
                minute = (now.minute // 5) * 5
                bar_start = now.replace(minute=minute, second=0, microsecond=0)
                # next bar start:
                next_bar_start = bar_start + timedelta(minutes=5)
                sleep_seconds = (next_bar_start - now).total_seconds()
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
                stop_price = high_last + tick
                # swing leg low (consecutive bull bars ending at last_completed_idx)
                try:
                    low_swing = self._find_consecutive_bull_leg_low_from_snapshot(
                        lst, end_index=last_idx
                    )
                except Exception:
                    low_swing = float(last_bar["low"])
                sell_stop_price = low_swing - tick
                # take-profit calculation
                distance = high_last - low_swing
                take_profit = high_last + 2 * distance - tick

                # round these to tick grid (ceil for entry stop to ensure crossing)
                if tick:
                    # ceil the stop_price to next tick
                    stop_price = (
                        int(stop_price // tick) + (1 if (stop_price % tick) != 0 else 0)
                    ) * tick
                    sell_stop_price = int(sell_stop_price // tick) * tick
                    take_profit = int(take_profit // tick) * tick
                stop_price = round(stop_price, 8)
                sell_stop_price = round(sell_stop_price, 8)
                take_profit = round(take_profit, 8)

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
            except Exception as e:
                self.logger.exception("Error in scheduled buy_stop: %s", e)

        asyncio.create_task(_task())
        return {
            "status": "scheduled",
            "note": "buy_stop scheduled at next bar boundary",
        }

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

        lst = await self._get_snapshot(instrument_id)
        if not lst:
            raise HTTPException(status_code=400, detail="No completed bar available")
        last_idx = self._last_completed_index_from_snapshot(lst)
        if last_idx is None:
            raise HTTPException(status_code=400, detail="No completed bar available")
        last_bar = lst[last_idx]
        high_last = float(last_bar["high"])
        stop_price = high_last + tick
        # swing leg excluding the current bar which is not completed => use end_index = last_idx (already excludes in-progress)
        try:
            low_swing = self._find_consecutive_bull_leg_low_from_snapshot(
                lst, end_index=last_idx
            )
        except Exception:
            low_swing = float(last_bar["low"])
        sell_stop_price = low_swing - tick
        distance = high_last - low_swing
        take_profit = high_last + 2 * distance - tick

        # align to ticks (simple rounding/quantize)
        if tick:
            stop_price = (
                int(stop_price // tick) + (1 if (stop_price % tick) != 0 else 0)
            ) * tick
            sell_stop_price = int(sell_stop_price // tick) * tick
            take_profit = int(take_profit // tick) * tick
        stop_price = round(stop_price, 8)
        sell_stop_price = round(sell_stop_price, 8)
        take_profit = round(take_profit, 8)

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
        """
        info = self._get_instrument_info(instrument_id)
        tick = self._get_tick_size(instrument_id)

        async def _task():
            try:
                # compute next bar start aligned to 5-minute grid
                now = datetime.now(tz=timezone.utc)
                minute = (now.minute // 5) * 5
                bar_start = now.replace(minute=minute, second=0, microsecond=0)
                next_bar_start = bar_start + timedelta(minutes=5)
                sleep_seconds = (next_bar_start - now).total_seconds()
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
                stop_price = low_last - tick
                if tick:
                    stop_price = int(stop_price // tick) * tick
                stop_price = round(stop_price, 8)
                sell_order = {
                    "side": "sell",
                    "type": "stop",
                    "instrument": instrument_id,
                    "volume": volume,
                    "stop_price": float(stop_price),
                    "note": "scheduled sell stop (placed at new bar 0s)",
                }
                self._log_order(sell_order)
            except Exception as e:
                self.logger.exception("Error in scheduled sell_stop: %s", e)

        asyncio.create_task(_task())
        return {
            "status": "scheduled",
            "note": "sell_stop scheduled at next bar boundary",
        }

    async def late_sell_stop(self, instrument_id: str, volume: float) -> Dict[str, Any]:
        """
        Place a late sell stop order immediately:
        - Immediately calculate stop price = 1 tick_size below low of last completed bar.
        - Place sell stop at that price.
        """
        info = self._get_instrument_info(instrument_id)
        tick = self._get_tick_size(instrument_id)

        lst = await self._get_snapshot(instrument_id)
        if not lst:
            raise HTTPException(status_code=400, detail="No completed bar available")
        last_idx = self._last_completed_index_from_snapshot(lst)
        if last_idx is None:
            raise HTTPException(status_code=400, detail="No completed bar available")
        last_bar = lst[last_idx]
        low_last = float(last_bar["low"])
        stop_price = low_last - tick
        if tick:
            stop_price = int(stop_price // tick) * tick
        stop_price = round(stop_price, 8)
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


def create_app(
    redis_url="redis://localhost:6379/0",
    redis_channel="pacavanza:ticker_updates",
    static_html_path: str | Path = STATIC_HTML,
):
    """
    Create FastAPI app. Uses lifespan async context manager to start/stop background
    Redis subscriber task and to close the redis client cleanly (uses aclose()).
    """
    # redis client (async)
    redis_client = aioredis.from_url(redis_url)

    manager = WebSocketManager()

    # in-memory per-instrument state (keeps recent history for EMA calculation).
    # For many symbols or long history you might want to persist this or cap size.
    recent_bars: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    # per-instrument locks map and map lock to protect creation
    recent_bars_locks: Dict[str, asyncio.Lock] = {}
    recent_bars_locks_map_lock = asyncio.Lock()

    # metadata and per-instrument locks + map lock
    metadata: Dict[str, Dict[str, Any]] = {}
    metadata_locks: Dict[str, asyncio.Lock] = {}
    metadata_locks_map_lock = asyncio.Lock()

    # ema_state[instrument][length] = last EMA value
    ema_state: Dict[str, Dict[int, float]] = defaultdict(dict)

    # NEW: load instrument_list.json into memory for quick access
    instrument_list: Dict[str, Any] = {}
    try:
        with open(ROOT / "instrument_list.json", "r", encoding="utf-8") as f:
            instrument_list = json.load(f)
    except Exception as e:
        LOGGER.warning("Could not load instrument_list.json: %s", e)

    # instantiate AvanzaTrading with references to in-memory state and the lock maps
    trading = AvanzaTrading(
        recent_bars,
        instrument_list,
        metadata,
        LOGGER,
        bars_locks_map=recent_bars_locks,
        metadata_locks_map=metadata_locks,
        bars_map_lock=recent_bars_locks_map_lock,
        metadata_map_lock=metadata_locks_map_lock,
    )

    # Background task: subscribe to redis channel and forward events
    async def _redis_subscriber_task():
        LOGGER.info("Starting Redis subscriber task")
        try:
            pubsub = redis_client.pubsub()
            await pubsub.subscribe(redis_channel)
            LOGGER.info(f"Subscribed to Redis channel: {redis_channel}")

            # Test message to verify Redis is working
            test_msg = {"type": "test", "message": "Redis connection established"}
            await redis_client.publish(redis_channel, json.dumps(test_msg))

            async for msg in pubsub.listen():
                if msg and msg["type"] == "message":
                    data = msg["data"]
                    LOGGER.debug(f"Redis subscriber received raw message: {data}")

                    if isinstance(data, (bytes, bytearray)):
                        try:
                            payload = json.loads(data.decode("utf-8"))
                            LOGGER.debug(f"Decoded Redis message: {payload}")
                        except Exception as e:
                            LOGGER.error(
                                f"Invalid JSON from redis (bytes): {e}, data: {data}"
                            )
                            continue
                    elif isinstance(data, str):
                        try:
                            payload = json.loads(data)
                            LOGGER.debug(f"Decoded Redis message: {payload}")
                        except Exception as e:
                            LOGGER.error(
                                f"Invalid JSON from redis (str): {e}, data: {data}"
                            )
                            continue
                    else:
                        LOGGER.warning(f"Unexpected message type: {type(data)}")
                        continue

                    # Handle test message
                    if payload.get("type") == "test":
                        LOGGER.info(
                            f"Redis test message received: {payload.get('message')}"
                        )
                        continue

                    try:
                        await _handle_redis_payload(payload)
                    except Exception as e:
                        LOGGER.exception(f"Error handling redis payload: {e}")

        except asyncio.CancelledError:
            LOGGER.info("Redis subscriber task cancelled (normal shutdown)")
            return  # Do not re-raise — clean exit
        except Exception as e:
            LOGGER.exception(f"Redis subscriber error: {e}")
            # Try to reconnect after delay
            await asyncio.sleep(5)
            # Restart the subscriber task
            asyncio.create_task(_redis_subscriber_task())
        finally:
            try:
                await pubsub.unsubscribe(redis_channel)
            except Exception:
                pass
            try:
                await pubsub.aclose()
            except Exception:
                pass

    # helper to get/create per-instrument bars lock (used by redis handler)
    async def _get_bars_lock_for(sid: str) -> asyncio.Lock:
        lock = recent_bars_locks.get(sid)
        if lock:
            return lock
        async with recent_bars_locks_map_lock:
            lock = recent_bars_locks.get(sid)
            if not lock:
                lock = asyncio.Lock()
                recent_bars_locks[sid] = lock
            return lock

    # helper to get/create per-instrument metadata lock (used by redis handler)
    async def _get_metadata_lock_for(sid: str) -> asyncio.Lock:
        lock = metadata_locks.get(sid)
        if lock:
            return lock
        async with metadata_locks_map_lock:
            lock = metadata_locks.get(sid)
            if not lock:
                lock = asyncio.Lock()
                metadata_locks[sid] = lock
            return lock

    async def _handle_redis_payload(payload: Dict[str, Any]):
        """
        Process messages from Redis, keep in-memory state for EMA/labels and
        broadcast enriched messages to websocket clients.
        """
        LOGGER.debug("Handling redis payload: %s", payload)
        mtype = payload.get("type")
        sid = payload.get("instrument")
        if not sid:
            return

        # store metadata if provided (use per-instrument lock)
        if payload.get("meta"):
            meta_lock = await _get_metadata_lock_for(sid)
            async with meta_lock:
                metadata[sid] = payload["meta"]

        if mtype == "update":
            # in-progress/current bar updates
            bar = payload.get("bar")
            if not bar:
                return
            # use start_time as canonical timestamp
            ts = datetime.fromisoformat(bar["start_time"])
            # append/replace last bar in recent_bars (keep them bounded)
            # Use per-instrument lock to avoid concurrent mutation while trading reads snapshots
            bars_lock = await _get_bars_lock_for(sid)
            async with bars_lock:
                lst = recent_bars[sid]
                if not lst or lst[-1]["start_time"] != bar["start_time"]:
                    lst.append(
                        {
                            "start_time": datetime.fromisoformat(bar["start_time"]),
                            "end_time": datetime.fromisoformat(bar["end_time"]),
                            "open": bar["open"],
                            "high": bar["high"],
                            "low": bar["low"],
                            "close": bar["close"],
                            "volume": bar.get("volume", 0),
                        }
                    )
                else:
                    # replace last (update)
                    lst[-1].update(
                        {
                            "open": bar["open"],
                            "high": bar["high"],
                            "low": bar["low"],
                            "close": bar["close"],
                            "volume": bar.get("volume", 0),
                        }
                    )
                # limit history length (e.g. 5000 bars)
                if len(lst) > 5000:
                    lst[:] = lst[-5000:]

                # NEW: prune bars older than one week to save memory
                try:
                    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=7)
                    lst[:] = [
                        b
                        for b in lst
                        if (b["start_time"].astimezone(timezone.utc) >= cutoff)
                    ]
                except Exception:
                    if len(lst) > 5000:
                        lst[:] = lst[-5000:]

            # compute incremental EMA updates (fast) - uses the global recent_bars dict; reading latest snapshot is fine
            emas = {}
            for L in (20, 50, 100, 220):
                prev = ema_state[sid].get(L)
                new = incremental_ema_update(
                    prev, bar["close"], L, historical_buffer=recent_bars[sid]
                )
                if new is not None:
                    ema_state[sid][L] = new
                    emas[str(L)] = {"time": bar["start_time"], "value": new}

            out = {
                "type": "update",
                "instrument": sid,
                "bar": bar,
                "emas": emas,
            }
            await manager.broadcast(out)

        elif mtype == "completed":
            # a completed bar (append to history)
            bar = payload.get("bar")
            if not bar:
                return
            # convert and append under per-instrument lock
            bars_lock = await _get_bars_lock_for(sid)
            async with bars_lock:
                lst = recent_bars[sid]
                lst.append(
                    {
                        "start_time": datetime.fromisoformat(bar["start_time"]),
                        "end_time": datetime.fromisoformat(bar["end_time"]),
                        "open": bar["open"],
                        "high": bar["high"],
                        "low": bar["low"],
                        "close": bar["close"],
                        "volume": bar.get("volume", 0),
                    }
                )
                if len(lst) > 5000:
                    lst[:] = lst[-5000:]
                # NEW: prune to one week
                try:
                    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=7)
                    lst[:] = [
                        b
                        for b in lst
                        if (b["start_time"].astimezone(timezone.utc) >= cutoff)
                    ]
                except Exception:
                    if len(lst) > 5000:
                        lst[:] = lst[-5000:]
            # recompute EMAs using incremental update with the finalized close
            emas = {}
            for L in (20, 50, 100, 220):
                prev = ema_state[sid].get(L)
                new = incremental_ema_update(
                    prev, bar["close"], L, historical_buffer=recent_bars[sid]
                )
                if new is not None:
                    ema_state[sid][L] = new
                    emas[str(L)] = {"time": bar["start_time"], "value": new}

            out = {
                "type": "completed",
                "instrument": sid,
                "bar": bar,
                "emas": emas,
            }
            await manager.broadcast(out)

    # Lifespan context manager: start subscriber on startup and close on shutdown
    @asynccontextmanager
    async def lifespan(app) -> AsyncIterator[None]:
        # start subscriber task in background
        app.state._redis_task = asyncio.create_task(_redis_subscriber_task())
        LOGGER.info("FastAPI Redis WS app started (lifespan)")
        try:
            yield
        finally:
            # cancel and await subscriber task
            task = getattr(app.state, "_redis_task", None)
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    LOGGER.debug(
                        "Redis subscriber task cancelled cleanly during shutdown"
                    )
                except Exception:
                    LOGGER.exception(
                        "Error awaiting redis subscriber task during shutdown"
                    )
            # close redis client using aclose() to avoid deprecation
            try:
                await redis_client.aclose()
            except Exception:
                pass
            LOGGER.info("FastAPI Redis WS app stopped (lifespan)")

    # create app with lifespan
    app = FastAPI(lifespan=lifespan)

    # MOUNT STATIC FILES - ADD THIS LINE
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    # Expose instrument_list.json
    @app.get("/instrument_list.json")
    async def get_bar_json():
        return FileResponse(
            ROOT / "instrument_list.json",
            media_type="application/json",
            headers={"Cache-Control": "public, max-age=3600"},
        )

    # Useful tiny endpoints to silence noisy probes from browser/devtools
    @app.get("/.well-known/appspecific/com.chrome.devtools.json")
    async def chrome_devtools_probe():
        # return an empty object (200) so devtools probe doesn't show 404
        return JSONResponse({})

    @app.get("/favicon.ico")
    async def favicon():
        # return an empty 204 response (no body, no Content-Length mismatch)
        return Response(status_code=204)

    @app.get("/history/{instrument_id}")
    async def get_history(instrument_id: str, limit: int = 500):
        """
        Return the most recent completed_ohlc for instrument.
        This reads existing disk file (preferred) or uses in-memory snapshot if available.
        """
        # first attempt to read disk file (collector is authoritative)
        data_file = f"ohlc_{instrument_id}.json"
        try:
            with open(data_file, "r") as f:
                data = json.load(f)
            # ensure we only return up to limit
            if limit and len(data) > limit:
                data = data[-limit:]
            return JSONResponse(content=data)
        except FileNotFoundError:
            # fallback to in-memory recent_bars
            # Use a per-instrument snapshot under the instrument lock to avoid inconsistent reads
            bars_lock = await _get_bars_lock_for(instrument_id)
            async with bars_lock:
                lst = list(recent_bars.get(instrument_id, []))
            out = []
            for b in lst[-limit:]:
                out.append(
                    {
                        "start_time": b["start_time"].isoformat(),
                        "end_time": b["end_time"].isoformat(),
                        "open": b["open"],
                        "high": b["high"],
                        "low": b["low"],
                        "close": b["close"],
                        "volume": b["volume"],
                    }
                )
            if not out:
                raise HTTPException(status_code=404, detail="no data")
            return JSONResponse(content=out)

    @app.websocket("/ws")
    async def websocket_endpoint(ws: WebSocket):
        await manager.connect(ws)
        try:
            # client may send subscription messages in future; for now we just broadcast all
            while True:
                # keep receive to allow client ping/pong; ignore messages
                _ = await ws.receive_text()
        except WebSocketDisconnect:
            await manager.disconnect(ws)
        except Exception:
            await manager.disconnect(ws)

    # serve a static HTML for convenience if requested
    @app.get("/")
    async def index():
        try:
            with open(str(static_html_path), "r", encoding="utf-8") as f:
                return HTMLResponse(f.read())
        except Exception:
            return JSONResponse({"status": "ok", "note": "Static file not available"})

    # --------------------
    # Trading endpoints (used by trading.js)
    # --------------------

    @app.post("/trade/market_buy")
    async def trade_market_buy(req: Request):
        """
        Place a market buy order, using input parameters: instrumentId, volume.
        The price should be the current last sell price from the redis data.
        """
        body = await req.json()
        instrument_id = body.get("instrumentId")
        volume = body.get("volume")
        if not instrument_id or volume is None:
            raise HTTPException(
                status_code=400, detail="instrumentId and volume required"
            )
        # instrumentId should be inside instrument_list.json.
        if instrument_id not in instrument_list:
            raise HTTPException(
                status_code=400, detail="instrumentId not found in instrument_list.json"
            )
        order = await trading.place_market_buy(instrument_id, volume)
        return JSONResponse(content={"status": "ok", "order": order})

    @app.post("/trade/market_sell")
    async def trade_market_sell(req: Request):
        """
        Place a market sell order, using input parameters: instrumentId, volume.
        The price should be the current last buy price from the redis data.
        """
        body = await req.json()
        instrument_id = body.get("instrumentId")
        volume = body.get("volume")
        if not instrument_id or volume is None:
            raise HTTPException(
                status_code=400, detail="instrumentId and volume required"
            )
        if instrument_id not in instrument_list:
            raise HTTPException(
                status_code=400, detail="instrumentId not found in instrument_list.json"
            )
        order = await trading.place_market_sell(instrument_id, volume)
        return JSONResponse(content={"status": "ok", "order": order})

    @app.post("/trade/buy_stop")
    async def trade_buy_stop(req: Request):
        """
        Place a buy stop order:
        Wait for the current bar to complete. Exactly at the 0 second of the new bar, calculate the stop price which should be 1 tick_size above the high of the last completed bar.
        Then immediately place the buy stop order at that stop price.
        Also place a sell stop order at 1 tick_size below the low of the current swing leg. This calculation should use pandas-ta library. This leg should contain only consecutive bull bar(s). So that we have a stop-loss.
        Then use a profit ratio of 2:1 stop-loss to calculate the take-profit price and place a sell limit order:
        take-profit price = high of the current bar + 2 * (high of the current bar - low of the current swing leg) - 1 tick_size.
        The 1 tick_size is to ensure the take-profit order can be filled.
        For now, just log the order details instead of really placing the orders.
        """
        body = await req.json()
        instrument_id = body.get("instrumentId")
        volume = body.get("volume")
        if not instrument_id or volume is None:
            raise HTTPException(
                status_code=400, detail="instrumentId and volume required"
            )
        if instrument_id not in instrument_list:
            raise HTTPException(
                status_code=400, detail="instrumentId not found in instrument_list.json"
            )
        res = await trading.schedule_buy_stop(instrument_id, volume)
        return JSONResponse(content=res)

    @app.post("/trade/late_buy_stop")
    async def trade_late_buy_stop(req: Request):
        """
        Place a late buy stop order:
        Immediately calculate the stop price which should be 1 tick_size above the high of the last completed bar.
        Then immediately place the buy stop order at that stop price.
        Also place a sell stop order at 1 tick_size below the low of the current swing leg (excluding the current bar which is not completed). This calculation should use pandas-ta library. This leg should contain only consecutive bull bar(s). So that we have a stop-loss.
        Then use a profit ratio of 2:1 stop-loss to calculate the take-profit price and place a sell limit order:
        take-profit price = high of the last completed bar + 2 * (high of the last completed bar - low of the current swing leg) - 1 tick_size.
        The 1 tick_size is to ensure the take-profit order can be filled.
        For now, just log the order details instead of really placing the orders.
        """
        body = await req.json()
        instrument_id = body.get("instrumentId")
        volume = body.get("volume")
        if not instrument_id or volume is None:
            raise HTTPException(
                status_code=400, detail="instrumentId and volume required"
            )
        if instrument_id not in instrument_list:
            raise HTTPException(
                status_code=400, detail="instrumentId not found in instrument_list.json"
            )
        res = await trading.late_buy_stop(instrument_id, volume)
        return JSONResponse(content=res)

    @app.post("/trade/sell_stop")
    async def trade_sell_stop(req: Request):
        """
        Place a sell stop order:
        Wait for the current bar to complete. Exactly at the 0 second of the new bar, calculate the stop price which should be 1 tick_size below the low of the last completed bar.
        Then immediately place the sell stop order at that stop price.
        For now, just log the order details instead of really placing the orders.
        """
        body = await req.json()
        instrument_id = body.get("instrumentId")
        volume = body.get("volume")
        if not instrument_id or volume is None:
            raise HTTPException(
                status_code=400, detail="instrumentId and volume required"
            )
        if instrument_id not in instrument_list:
            raise HTTPException(
                status_code=400, detail="instrumentId not found in instrument_list.json"
            )
        res = await trading.schedule_sell_stop(instrument_id, volume)
        return JSONResponse(content=res)

    @app.post("/trade/late_sell_stop")
    async def trade_late_sell_stop(req: Request):
        """
        Place a late sell stop order:
        Immediately calculate the stop price which should be 1 tick_size below the low of the last completed bar.
        Then immediately place the sell stop order at that stop price.
        For now, just log the order details instead of really placing the orders.
        """
        body = await req.json()
        instrument_id = body.get("instrumentId")
        volume = body.get("volume")
        if not instrument_id or volume is None:
            raise HTTPException(
                status_code=400, detail="instrumentId and volume required"
            )
        if instrument_id not in instrument_list:
            raise HTTPException(
                status_code=400, detail="instrumentId not found in instrument_list.json"
            )
        res = await trading.late_sell_stop(instrument_id, volume)
        return JSONResponse(content=res)

    return app


def main():
    # run as: python -m pacavanza.backend.main
    app = create_app(
        redis_url="redis://localhost:6379/0", redis_channel="pacavanza:ticker_updates"
    )
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")


if __name__ == "__main__":
    main()
