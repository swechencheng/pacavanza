import asyncio
import json
import logging
from typing import Dict, Any, List, Optional
import pandas as pd

from datetime import datetime, timedelta, timezone
from avanza import Avanza
from avanza.entities import (
    StopLossTrigger,
    StopLossTriggerType,
    StopLossPriceType,
    StopLossOrderEvent,
)
from avanza.constants import OrderType
from pacavanza.utils.utils import find_key_by_orderbook_id

SECRETS = json.load(open("./pacavanza/../secret.json"))
AVANZA = Avanza(SECRETS)
ACCOUNT_ID = SECRETS["accountId"]
PROFIT_LOSS_RATIO = 2

logging.basicConfig(level=logging.INFO)
logging.getLogger("base_trading").setLevel(logging.DEBUG)
LOGGER = logging.getLogger("base_trading")


class BaseAvanzaTrading:
    """
    Base class for Avanza trading implementations.

    Contains all shared infrastructure:
      - Instrument helpers, position/account queries
      - Per-instrument locking, snapshots, bar pruning
      - Scheduled-task management
      - Order editing, stop-loss cleanup
      - Swing leg calculation

    Subclasses must implement the order-placement methods that differ by
    instrument type (market orders, buy/sell stop price/trigger calculation).
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

    # helper: validate instrument exists in instrument_list.
    def _get_instrument_info(self, instrument_id: str) -> Dict[str, Any]:
        info = self.instrument_list.get(instrument_id)
        if not info:
            raise Exception(f"Unknown instrument: {instrument_id}")
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

    # helper: get accounts and positions
    def _get_accounts_and_positions(self) -> Dict[str, Any]:
        with AVANZA._session.get(
            "https://www.avanza.se/_api/trading-critical/rest/accountsandpositions",
            headers={
                "X-SecurityToken": AVANZA._security_token,
            },
        ) as response:
            response.raise_for_status()
            accounts_data = response.json()
            for account in accounts_data:
                if account["accountId"] == ACCOUNT_ID:
                    return account
            raise ValueError(f"Account with ID {ACCOUNT_ID} not found.")

    # helper: get account balance
    def _get_account_balance(self) -> float:
        account_data = self._get_accounts_and_positions()

        balance = account_data["availableForPurchase"]
        LOGGER.debug(f"Account balance: {balance}")
        return float(balance)

    # helper: get account positions
    def _get_account_positions(self) -> List[Dict[str, Any]]:
        account_data = self._get_accounts_and_positions()
        return account_data["positions"]

    # helper: get instrument position
    def _get_instrument_position(self, instrument_id: str) -> Dict[str, Any]:
        positions = self._get_account_positions()
        for position in positions:
            sid = find_key_by_orderbook_id(
                self.instrument_list, position["orderbookId"]
            )
            if instrument_id == sid:
                return position
        return None

    # helper: get instrument position size
    def _get_instrument_position_size(self, instrument_id: str) -> int:
        position = self._get_instrument_position(instrument_id)
        if not position:
            return 0
        volume = position["volume"]
        LOGGER.debug(f"Instrument {instrument_id} position size: {volume}")
        return volume

    # helper: calculate volume size based on account balance and price
    def _calculate_volume_size(
        self, instrument_id: str, price: float, percentage: float
    ) -> int:
        balance = self._get_account_balance()
        # This is of strong personal preference, the number must be multiple of 3
        vol = int((balance / price * percentage / 100) // 3) * 3
        if (price * vol) < 1000.01:
            raise ValueError(f"Order size too small for {instrument_id}")
        return vol

    # helper: delete stop-losses
    def delete_stop_losses(self, instrument_id: str) -> None:
        info = self._get_instrument_info(instrument_id)
        if not info:
            raise ValueError(f"Instrument {instrument_id} not found")
        ob_id = info.get("orderbookId")
        if not ob_id:
            raise ValueError(f"orderbookId for {instrument_id} not found")
        all_stop_losses = AVANZA.get_all_stop_losses()
        for sl in all_stop_losses:
            if sl["orderbook"]["id"] == ob_id:
                try:
                    AVANZA.delete_stop_loss_order(
                        account_id=ACCOUNT_ID, stop_loss_id=sl["id"]
                    )
                except Exception as e:
                    LOGGER.error(f"Failed to delete stop loss {instrument_id}: {e}")

    # helper: cleanup residual stop-losses
    def cleanup_residual_sell_stop_losses(self) -> List[Dict[str, Any]]:
        all_stop_losses = AVANZA.get_all_stop_losses()
        all_positions = self._get_account_positions()

        # Group stop losses by orderbook id
        sl_by_orderbook: Dict[str, List[Dict[str, Any]]] = {}
        for sl in all_stop_losses:
            ob_id = sl["orderbook"]["id"]
            if ob_id not in sl_by_orderbook:
                sl_by_orderbook[ob_id] = []
            sl_by_orderbook[ob_id].append(sl)

        # Get set of orderbook ids that have positions
        position_orderbook_ids = set(p["orderbookId"] for p in all_positions)

        # Iterate over grouped stop losses
        for ob_id, sl_list in sl_by_orderbook.items():
            # Check if we have a position for this orderbook
            if ob_id in position_orderbook_ids:
                continue

            # Check if there are any BUY stop losses
            has_buy_sl = any(sl["order"]["type"] == "BUY" for sl in sl_list)
            if has_buy_sl:
                continue

            # If we are here:
            # 1. No active position for this orderbook
            # 2. No BUY stop losses for this orderbook
            # We should delete all SELL stop losses for this orderbook
            for sl in sl_list:
                if sl["order"]["type"] == "SELL":
                    LOGGER.info(
                        f"Deleting residual sell stop loss: {sl['id']} for orderbook {ob_id}"
                    )
                    try:
                        AVANZA.delete_stop_loss_order(
                            account_id=ACCOUNT_ID, stop_loss_id=sl["id"]
                        )
                    except Exception as e:
                        LOGGER.error(f"Failed to delete stop loss {sl['id']}: {e}")

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
        LOGGER.debug(f"len(lst): {len(lst)}")
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
            if end < now:
                LOGGER.debug(f"Return idx: {i}")
                return i
        return None

    # helper: find consecutive bull leg low using pandas on a snapshot
    def _find_consecutive_bull_leg_low_from_snapshot(
        self, lst: List[Dict[str, Any]], end_index: Optional[int] = None
    ) -> float:
        """
        Return low price of consecutive bull bar(s) ending at end_index within the provided snapshot.
        If none found, return low of the bar at end_index (fallback).
        """
        if not lst:
            raise Exception("No bar data available for instrument")

        if end_index is None:
            LOGGER.warning(f"Idx missing, using {len(lst) - 1}")
            end_index = len(lst) - 1

        # guard indexes
        end_index = min(end_index, len(lst) - 1)
        if end_index < 0:
            raise Exception("No completed bars")

        # Build a small DataFrame for TA usage (though we just need bull sequences)
        df = pd.DataFrame(
            {
                "open": [b["open"] for b in lst],
                "high": [b["high"] for b in lst],
                "low": [b["low"] for b in lst],
                "close": [b["close"] for b in lst],
            }
        )

        # Determine bull bars: close >= open
        bull_mask = df["close"] >= df["open"]

        # Walk backwards from end_index and collect consecutive bulls
        LOGGER.debug(f"start from end idx: {end_index}")
        i = end_index
        bull_indices = []
        while i >= 0 and bull_mask.iloc[i]:
            bull_indices.append(i)
            i -= 1
        LOGGER.debug(json.dumps(bull_indices))

        if not bull_indices:
            # fallback to low at end_index
            return float(lst[end_index]["low"])
        lows = [lst[idx]["low"] for idx in bull_indices]
        return float(min(lows))

    # Order logger
    def _log_order(self, order: Dict[str, Any]):
        # include timestamp
        order_out = dict(order)
        order_out.setdefault("ts", datetime.now(tz=timezone.utc).isoformat())
        self.logger.info("Reference order: %s", json.dumps(order_out, default=str))

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

    # --------------------------------------------------------------------------
    # Methods that subclasses MUST override (order-type specific logic)
    # --------------------------------------------------------------------------

    async def _get_market_buy_price(self, instrument_id: str) -> Optional[float]:
        """Return the price to use for a market buy order."""
        raise NotImplementedError

    async def _get_market_sell_price(self, instrument_id: str) -> Optional[float]:
        """Return the price to use for a market sell order."""
        raise NotImplementedError

    def _compute_buy_stop_trigger_and_limit(
        self,
        high_last: float,
        tick: float,
        tick_coeff: float,
    ) -> tuple:
        """
        Compute (trigger_price, limit_price, trigger_on_market_maker_quote) for a buy stop.
        Returns a tuple of (trigger, limit, trigger_on_mm).
        """
        raise NotImplementedError

    def _compute_sell_stop_trigger_and_limit(
        self,
        low_last: float,
        tick: float,
        tick_coeff: float,
    ) -> tuple:
        """
        Compute (trigger_price, limit_price, trigger_on_market_maker_quote) for a sell stop.
        Returns a tuple of (trigger, limit, trigger_on_mm).
        """
        raise NotImplementedError

    def _compute_take_profit_limit(
        self,
        take_profit: float,
        tick: float,
    ) -> tuple:
        """
        Compute (limit_price, trigger_on_market_maker_quote) for a take-profit sell.
        Returns a tuple of (limit, trigger_on_mm).
        """
        raise NotImplementedError

    async def _get_edit_order_follow_market_price(
        self, side: str, instrument_id: str
    ) -> Optional[float]:
        """Return the updated price for an edit_order_follow_market based on side."""
        raise NotImplementedError

    # --------------------------------------------------------------------------
    # Public API methods — shared flow, delegates to subclass hooks
    # --------------------------------------------------------------------------

    async def place_market_buy(
        self, instrument_id: str, percentage: float
    ) -> Dict[str, Any]:
        info = self._get_instrument_info(instrument_id)
        price = await self._get_market_buy_price(instrument_id)
        if price is None:
            raise Exception("No price data available")
        volume = self._calculate_volume_size(instrument_id, price, percentage)

        order = {
            "side": "buy",
            "type": "market",
            "instrument": instrument_id,
            "volume": volume,
            "price": float(price),
            "note": "market buy reference",
        }
        ret = AVANZA.place_order(
            account_id=ACCOUNT_ID,
            order_book_id=info["orderbookId"],
            order_type=OrderType.BUY,
            price=price,
            volume=volume,
            valid_until=datetime.now(tz=timezone.utc).date(),
        )
        LOGGER.info(f"Placed market buy order via Avanza API: {ret}")
        self._log_order(order)
        return order

    async def place_market_sell(self, instrument_id: str) -> Dict[str, Any]:
        info = self._get_instrument_info(instrument_id)
        price = await self._get_market_sell_price(instrument_id)
        if price is None:
            raise Exception("No price data available")
        volume = self._get_instrument_position_size(instrument_id)
        if volume is None:
            raise Exception("No position data available")
        if volume == 0:
            raise Exception("No position found")

        order = {
            "side": "sell",
            "type": "market",
            "instrument": instrument_id,
            "volume": volume,
            "price": float(price),
            "note": "market sell reference",
        }
        ret = AVANZA.place_order(
            account_id=ACCOUNT_ID,
            order_book_id=info["orderbookId"],
            order_type=OrderType.SELL,
            price=price,
            volume=volume,
            valid_until=datetime.now(tz=timezone.utc).date(),
        )
        LOGGER.info(f"Placed market sell order via Avanza API: {ret}")
        self._log_order(order)
        return order

    async def schedule_buy_stop(
        self, instrument_id: str, percentage: float
    ) -> Dict[str, Any]:
        """
        Place a buy stop order:
        - Wait for the current bar to complete. Exactly at the 0 second of the new bar,
          calculate the stop price = 1 tick above the high of the last completed bar.
        - Place buy stop at stop price.
        - Also place a sell stop at 1 tick below the low of the current swing leg (consecutive bull bars).
        - Use profit ratio PROFIT_LOSS_RATIO:1 to compute take-profit.
        - If a buy_stop is already scheduled for the same instrument for the same next bar boundary, ignore.
        """
        tick = self._get_tick_size(instrument_id)
        tick_coeff = self._get_tick_coeff(instrument_id)

        # compute next bar start aligned to 5-minute grid
        now = datetime.now(tz=timezone.utc)
        minute = (now.minute // 5) * 5
        bar_start = now.replace(minute=minute, second=0, microsecond=0)
        next_bar_start = bar_start + timedelta(minutes=5, seconds=1)

        # check if already scheduled
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
                last_idx = self._last_completed_index_from_snapshot(lst)
                if last_idx is None:
                    self.logger.warning("No last completed bar found in snapshot")
                    return
                last_bar = lst[last_idx]
                high_last = float(last_bar["high"])

                # Delegate trigger/limit calculation to subclass
                stop_price, limit_price, trigger_on_mm = (
                    self._compute_buy_stop_trigger_and_limit(high_last, tick, tick_coeff)
                )

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
                take_profit = round(high_last + PROFIT_LOSS_RATIO * distance - tick, 2)
                volume = self._calculate_volume_size(
                    instrument_id, stop_price, percentage
                )

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

                info = self._get_instrument_info(instrument_id)
                sl_buy_trig = StopLossTrigger(
                    type=StopLossTriggerType.MORE_OR_EQUAL,
                    value=stop_price,
                    valid_until=datetime.now(tz=timezone.utc).date(),
                    value_type=StopLossPriceType.MONETARY,
                    trigger_on_market_maker_quote=trigger_on_mm,
                )
                sl_buy_evt = StopLossOrderEvent(
                    type=OrderType.BUY,
                    price=limit_price,
                    volume=volume,
                    valid_days=1,
                    price_type=StopLossPriceType.MONETARY,
                    short_selling_allowed=False,
                )
                ret = AVANZA.place_stop_loss_order(
                    parent_stop_loss_id="0",
                    account_id=ACCOUNT_ID,
                    order_book_id=info["orderbookId"],
                    stop_loss_trigger=sl_buy_trig,
                    stop_loss_order_event=sl_buy_evt,
                )
                LOGGER.info(f"Placed buy stop loss order via Avanza API: {ret}")
                self._log_order(buy_order)

                # Sell stop (stop-loss)
                sell_trigger, sell_limit, sell_trigger_on_mm = (
                    self._compute_sell_stop_trigger_and_limit(
                        sell_stop_price, tick, tick_coeff
                    )
                )
                sl_sell_trig = StopLossTrigger(
                    type=StopLossTriggerType.LESS_OR_EQUAL,
                    value=sell_stop_price,
                    valid_until=datetime.now(tz=timezone.utc).date(),
                    value_type=StopLossPriceType.MONETARY,
                    trigger_on_market_maker_quote=sell_trigger_on_mm,
                )
                sl_sell_evt = StopLossOrderEvent(
                    type=OrderType.SELL,
                    price=sell_limit,
                    volume=volume,
                    valid_days=1,
                    price_type=StopLossPriceType.MONETARY,
                    short_selling_allowed=False,
                )
                ret = AVANZA.place_stop_loss_order(
                    parent_stop_loss_id="0",
                    account_id=ACCOUNT_ID,
                    order_book_id=info["orderbookId"],
                    stop_loss_trigger=sl_sell_trig,
                    stop_loss_order_event=sl_sell_evt,
                )
                LOGGER.info(f"Placed sell stop loss order via Avanza API: {ret}")
                self._log_order(sell_stop_order)

                # Take-profit
                tp_limit, tp_trigger_on_mm = self._compute_take_profit_limit(
                    take_profit, tick
                )
                tp_sell_trig = StopLossTrigger(
                    type=StopLossTriggerType.MORE_OR_EQUAL,
                    value=take_profit,
                    valid_until=datetime.now(tz=timezone.utc).date(),
                    value_type=StopLossPriceType.MONETARY,
                    trigger_on_market_maker_quote=tp_trigger_on_mm,
                )
                tp_sell_evt = StopLossOrderEvent(
                    type=OrderType.SELL,
                    price=tp_limit,
                    volume=volume,
                    valid_days=1,
                    price_type=StopLossPriceType.MONETARY,
                    short_selling_allowed=False,
                )
                ret = AVANZA.place_stop_loss_order(
                    parent_stop_loss_id="0",
                    account_id=ACCOUNT_ID,
                    order_book_id=info["orderbookId"],
                    stop_loss_trigger=tp_sell_trig,
                    stop_loss_order_event=tp_sell_evt,
                )
                LOGGER.info(f"Placed sell stop loss order via Avanza API: {ret}")
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

    async def late_buy_stop(
        self, instrument_id: str, percentage: float
    ) -> Dict[str, Any]:
        """
        Place a late buy stop order immediately (using last completed bar).
        """
        tick = self._get_tick_size(instrument_id)
        tick_coeff = self._get_tick_coeff(instrument_id)

        lst = await self._get_snapshot(instrument_id)
        if not lst:
            raise Exception("No completed bar available")
        last_idx = self._last_completed_index_from_snapshot(lst)
        if last_idx is None:
            raise Exception("No completed bar available")
        last_bar = lst[last_idx]
        high_last = float(last_bar["high"])

        # Delegate trigger/limit calculation to subclass
        stop_price, limit_price, trigger_on_mm = (
            self._compute_buy_stop_trigger_and_limit(high_last, tick, tick_coeff)
        )

        # swing leg excluding the current bar
        try:
            low_swing = self._find_consecutive_bull_leg_low_from_snapshot(
                lst, end_index=last_idx
            )
        except Exception:
            low_swing = float(last_bar["low"])
        sell_stop_price = round(low_swing - tick, 2)
        distance = high_last - low_swing
        take_profit = round(high_last + PROFIT_LOSS_RATIO * distance - tick, 2)
        volume = self._calculate_volume_size(instrument_id, stop_price, percentage)

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

        info = self._get_instrument_info(instrument_id)

        # Buy stop
        sl_buy_trig = StopLossTrigger(
            type=StopLossTriggerType.MORE_OR_EQUAL,
            value=stop_price,
            valid_until=datetime.now(tz=timezone.utc).date(),
            value_type=StopLossPriceType.MONETARY,
            trigger_on_market_maker_quote=trigger_on_mm,
        )
        sl_buy_evt = StopLossOrderEvent(
            type=OrderType.BUY,
            price=limit_price,
            volume=volume,
            valid_days=1,
            price_type=StopLossPriceType.MONETARY,
            short_selling_allowed=False,
        )
        ret = AVANZA.place_stop_loss_order(
            parent_stop_loss_id="0",
            account_id=ACCOUNT_ID,
            order_book_id=info["orderbookId"],
            stop_loss_trigger=sl_buy_trig,
            stop_loss_order_event=sl_buy_evt,
        )
        LOGGER.info(f"Placed buy stop loss order via Avanza API: {ret}")
        self._log_order(buy_order)

        # Sell stop (stop-loss)
        sell_trigger, sell_limit, sell_trigger_on_mm = (
            self._compute_sell_stop_trigger_and_limit(sell_stop_price, tick, tick_coeff)
        )
        sl_sell_trig = StopLossTrigger(
            type=StopLossTriggerType.LESS_OR_EQUAL,
            value=sell_stop_price,
            valid_until=datetime.now(tz=timezone.utc).date(),
            value_type=StopLossPriceType.MONETARY,
            trigger_on_market_maker_quote=sell_trigger_on_mm,
        )
        sl_sell_evt = StopLossOrderEvent(
            type=OrderType.SELL,
            price=sell_limit,
            volume=volume,
            valid_days=1,
            price_type=StopLossPriceType.MONETARY,
            short_selling_allowed=False,
        )
        ret = AVANZA.place_stop_loss_order(
            parent_stop_loss_id="0",
            account_id=ACCOUNT_ID,
            order_book_id=info["orderbookId"],
            stop_loss_trigger=sl_sell_trig,
            stop_loss_order_event=sl_sell_evt,
        )
        LOGGER.info(f"Placed sell stop loss order via Avanza API: {ret}")
        self._log_order(sell_stop_order)

        # Take-profit
        tp_limit, tp_trigger_on_mm = self._compute_take_profit_limit(
            take_profit, tick
        )
        tp_sell_trig = StopLossTrigger(
            type=StopLossTriggerType.MORE_OR_EQUAL,
            value=take_profit,
            valid_until=datetime.now(tz=timezone.utc).date(),
            value_type=StopLossPriceType.MONETARY,
            trigger_on_market_maker_quote=tp_trigger_on_mm,
        )
        tp_sell_evt = StopLossOrderEvent(
            type=OrderType.SELL,
            price=tp_limit,
            volume=volume,
            valid_days=1,
            price_type=StopLossPriceType.MONETARY,
            short_selling_allowed=False,
        )
        ret = AVANZA.place_stop_loss_order(
            parent_stop_loss_id="0",
            account_id=ACCOUNT_ID,
            order_book_id=info["orderbookId"],
            stop_loss_trigger=tp_sell_trig,
            stop_loss_order_event=tp_sell_evt,
        )
        LOGGER.info(f"Placed sell stop loss order via Avanza API: {ret}")
        self._log_order(take_profit_order)
        return {
            "status": "placed",
            "orders": [buy_order, sell_stop_order, take_profit_order],
        }

    async def schedule_sell_stop(self, instrument_id: str) -> Dict[str, Any]:
        """
        Place a sell stop order:
        - Wait for current bar to complete. At new bar 0s, stop price = 1 tick below low of last completed bar.
        - Place sell stop order immediately.
        """
        tick = self._get_tick_size(instrument_id)
        tick_coeff = self._get_tick_coeff(instrument_id)

        now = datetime.now(tz=timezone.utc)
        minute = (now.minute // 5) * 5
        bar_start = now.replace(minute=minute, second=0, microsecond=0)
        next_bar_start = bar_start + timedelta(minutes=5, seconds=1)

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
                volume = self._get_instrument_position_size(instrument_id)
                if volume is None:
                    raise Exception("No position data available")
                if volume == 0:
                    raise Exception("No position found")

                sell_order = {
                    "side": "sell",
                    "type": "stop",
                    "instrument": instrument_id,
                    "volume": volume,
                    "stop_price": float(stop_price),
                    "note": "scheduled sell stop (placed at new bar 0s)",
                }
                info = self._get_instrument_info(instrument_id)

                # Delegate limit price calculation to subclass
                _trigger, sell_limit, sell_trigger_on_mm = (
                    self._compute_sell_stop_trigger_and_limit(
                        stop_price, tick, tick_coeff
                    )
                )
                sl_sell_trig = StopLossTrigger(
                    type=StopLossTriggerType.LESS_OR_EQUAL,
                    value=stop_price,
                    valid_until=datetime.now(tz=timezone.utc).date(),
                    value_type=StopLossPriceType.MONETARY,
                    trigger_on_market_maker_quote=sell_trigger_on_mm,
                )
                sl_sell_evt = StopLossOrderEvent(
                    type=OrderType.SELL,
                    price=sell_limit,
                    volume=volume,
                    valid_days=1,
                    price_type=StopLossPriceType.MONETARY,
                    short_selling_allowed=False,
                )
                ret = AVANZA.place_stop_loss_order(
                    parent_stop_loss_id="0",
                    account_id=ACCOUNT_ID,
                    order_book_id=info["orderbookId"],
                    stop_loss_trigger=sl_sell_trig,
                    stop_loss_order_event=sl_sell_evt,
                )
                LOGGER.info(f"Placed sell stop loss order via Avanza API: {ret}")
                self._log_order(sell_order)
            except asyncio.CancelledError:
                self.logger.info("Scheduled sell_stop cancelled for %s", instrument_id)
                raise
            except Exception as e:
                self.logger.exception("Error in scheduled sell_stop: %s", e)
            finally:
                await self._clear_scheduled(instrument_id, "sell_stop")

        task = asyncio.create_task(_task())
        await self._set_scheduled(instrument_id, "sell_stop", next_bar_start, task)
        return {"status": "scheduled", "bar_start": next_bar_start.isoformat()}

    async def late_sell_stop(self, instrument_id: str) -> Dict[str, Any]:
        """
        Place a late sell stop order immediately.
        """
        tick = self._get_tick_size(instrument_id)
        tick_coeff = self._get_tick_coeff(instrument_id)

        lst = await self._get_snapshot(instrument_id)
        if not lst:
            raise Exception("No completed bar available")
        last_idx = self._last_completed_index_from_snapshot(lst)
        if last_idx is None:
            raise Exception("No completed bar available")
        last_bar = lst[last_idx]
        low_last = float(last_bar["low"])
        stop_price = round(low_last - tick * tick_coeff, 2)
        volume = self._get_instrument_position_size(instrument_id)
        if volume is None:
            raise Exception("No position data available")
        if volume == 0:
            raise Exception("No position found")

        sell_order = {
            "side": "sell",
            "type": "stop",
            "instrument": instrument_id,
            "volume": volume,
            "stop_price": float(stop_price),
            "note": "late sell stop placed immediately",
        }
        info = self._get_instrument_info(instrument_id)

        # Delegate limit price calculation to subclass
        _trigger, sell_limit, sell_trigger_on_mm = (
            self._compute_sell_stop_trigger_and_limit(stop_price, tick, tick_coeff)
        )
        sl_sell_trig = StopLossTrigger(
            type=StopLossTriggerType.LESS_OR_EQUAL,
            value=stop_price,
            valid_until=datetime.now(tz=timezone.utc).date(),
            value_type=StopLossPriceType.MONETARY,
            trigger_on_market_maker_quote=sell_trigger_on_mm,
        )
        sl_sell_evt = StopLossOrderEvent(
            type=OrderType.SELL,
            price=sell_limit,
            volume=volume,
            valid_days=1,
            price_type=StopLossPriceType.MONETARY,
            short_selling_allowed=False,
        )
        ret = AVANZA.place_stop_loss_order(
            parent_stop_loss_id="0",
            account_id=ACCOUNT_ID,
            order_book_id=info["orderbookId"],
            stop_loss_trigger=sl_sell_trig,
            stop_loss_order_event=sl_sell_evt,
        )
        LOGGER.info(f"Placed sell stop loss order via Avanza API: {ret}")
        self._log_order(sell_order)
        return {"status": "placed", "order": sell_order}

    # cancellation API helpers
    async def cancel_buy_stop(self, instrument_id: str) -> Dict[str, Any]:
        cancelled = await self._cancel_scheduled(instrument_id, "buy_stop")
        return {"cancelled": cancelled}

    async def cancel_sell_stop(self, instrument_id: str) -> Dict[str, Any]:
        cancelled = await self._cancel_scheduled(instrument_id, "sell_stop")
        return {"cancelled": cancelled}

    # Order API helpers
    def edit_order(
        self,
        order_id: str,
        account_id: str,
        price: float,
        volume: int,
        valid_until: str,
    ) -> Dict[str, Any]:
        """Sample successful order return:
        {'orderRequestStatus': 'SUCCESS', 'message': '', 'parameters': [''], 'orderId': '123456789'}
        """
        ret = AVANZA.edit_order(
            order_id=order_id,
            account_id=account_id,
            price=price,
            volume=volume,
            valid_until=datetime.strptime(valid_until, "%Y-%m-%d"),
        )
        return ret

    async def edit_order_follow_market(
        self,
        order_id: str,
        account_id: str,
    ) -> Dict[str, Any]:
        """Sample successful order return:
        {'orderRequestStatus': 'SUCCESS', 'message': '', 'parameters': [''], 'orderId': '123456789'}
        """
        order = AVANZA.get_order(
            order_id=order_id,
            account_id=account_id,
        )
        LOGGER.debug(f"Found: {order}")
        orderbook_id = order.get("orderbookId")
        if not orderbook_id:
            raise Exception("No orderbookId available")
        volume = order.get("volume")
        if not volume:
            raise Exception("No volume available")
        side = order.get("side")
        if not side:
            raise Exception("No side available")
        state = order.get("state")
        if not state:
            raise Exception("No state available")
        valid_until = order.get("validUntil")
        if not valid_until:
            raise Exception("No validUntil available")
        modifiable = order.get("modifiable")
        if not modifiable:
            raise Exception("Un-modifiable")

        instrument_id = find_key_by_orderbook_id(self.instrument_list, orderbook_id)
        if not instrument_id:
            raise Exception(f"No instrument available for {orderbook_id}")

        new_price = await self._get_edit_order_follow_market_price(side, instrument_id)
        if not new_price:
            raise Exception("No market buy/sell available")
        return self.edit_order(
            order_id=order_id,
            account_id=account_id,
            price=new_price,
            volume=volume,
            valid_until=valid_until,
        )
