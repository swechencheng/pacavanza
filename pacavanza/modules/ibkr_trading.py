import asyncio
import json
import logging
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd
from datetime import datetime, timezone

from ib_async import IB, Future, MarketOrder, StopOrder, LimitOrder, Trade

from .base_trading import BaseAvanzaTrading, PROFIT_LOSS_RATIO

LOGGER = logging.getLogger("ibkr_trading")

# IBKR uses the maximum 64-bit signed integer value (2^63 - 1) to indicate uninitialized/unset integer fields (like parentId or parentPermId).
IB_UNSET_INT = 9223372036854775807


class IbkrTrading(BaseAvanzaTrading):
    """
    IbkrTrading for OMXS30 futures via Interactive Brokers (ib_async).

    Uses ib_async to place Market, Stop, and Limit orders on IBKR.
    Inherits scheduling, bar computation, swing leg calculation, and
    cancellation logic from BaseAvanzaTrading.

    Position-aware order logic:
      - Checks existing position before placing stop orders.
      - Determines whether the order is: enter, close, scale-up, or
        close-and-reverse, then places the appropriate set of orders.
    """

    def __init__(
        self,
        ib: IB,
        contract: Future,
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
        super().__init__(
            recent_bars=recent_bars,
            instrument_list=instrument_list,
            metadata=metadata,
            logger=logger,
            bars_locks_map=bars_locks_map,
            metadata_locks_map=metadata_locks_map,
            bars_map_lock=bars_map_lock,
            metadata_map_lock=metadata_map_lock,
            loop=loop,
        )
        self.ib = ib
        self.contract = contract
        self.order_placed_times = {}
        self.perm_id_to_parent_perm_id = {}
        self.perm_id_to_oca_group = {}

        # Subscribe to position updates from IBKR so that ib.positions()
        # is automatically populated and updated.
        # We must use client.reqPositions() instead of ib.reqPositions()
        # to avoid blocking the already-running asyncio event loop.
        self.ib.client.reqPositions()

    # --------------------------------------------------------------------------
    # Override: volume is numberOfContracts (not calculated from balance)
    # --------------------------------------------------------------------------

    def _calculate_volume_size(
        self, instrument_id: str, price: float, percentage: float
    ) -> int:
        """
        For IBKR futures, 'percentage' is repurposed as the numberOfContracts
        directly (an integer), since futures margin is broker-managed.
        """
        return int(percentage)

    # --------------------------------------------------------------------------
    # Position helpers
    # --------------------------------------------------------------------------

    def _get_signed_position(self) -> int:
        """
        Return signed position for the contract.
        Positive = long, negative = short, 0 = flat.
        """
        positions = self.ib.positions()
        for pos in positions:
            if pos.contract.conId == self.contract.conId:
                return int(pos.position)
        return 0

    def _get_instrument_position_size(self, instrument_id: str) -> int:
        """Query current position size (absolute) from IBKR for the contract."""
        return abs(self._get_signed_position())

    def _round_price(self, price: float) -> float:
        """Snap price to tick size to prevent IBKR from rejecting unrounded floats."""
        if price is None:
            return price
        tick = 0.25  # fallback
        if getattr(self, "instrument_list", None):
            first_instrument = next(iter(self.instrument_list.values()))
            tick = float(first_instrument.get("tick_size", 0.25))
        return round(price / tick) * tick

    def _find_existing_sl_order(self, closing_action: str) -> Optional[Trade]:
        """
        Find an existing SL (stop) order that is part of a bracket or OCA group
        and matches the given closing action (e.g., "SELL" for closing a long).

        Only returns SL orders whose parent entry has already filled (or OCA
        orders which have no parent). This avoids accidentally modifying an SL
        that protects a pending, unfilled entry.

        Returns the Trade object if found, None otherwise.
        """
        open_trades = self.ib.openTrades()
        for t in open_trades:
            if t.contract.conId != self.contract.conId or not t.isActive():
                continue
            order = t.order
            if order.orderType != "STP":
                continue
            if order.action != closing_action:
                continue
            # Must belong to a bracket (parentId) or OCA group
            parent_id = getattr(order, "parentId", 0)
            oca_group = getattr(order, "ocaGroup", "")
            if parent_id == 0 and not oca_group:
                continue  # standalone order, not a bracket/OCA SL
            # For bracket children, only consider SL whose parent is filled
            if parent_id != 0:
                parent_trade = next(
                    (pt for pt in open_trades if pt.order.orderId == parent_id),
                    None,
                )
                # Parent still active → SL protects a pending entry, skip
                if parent_trade and parent_trade.isActive():
                    continue
            return t
        return None

    def _sync_sl_tp_volume(self) -> None:
        """
        Synchronize the volume of all active SL and TP orders
        to match the current position size.
        """
        signed_pos = self._get_signed_position()
        abs_pos = abs(signed_pos)
        closing_action = "SELL" if signed_pos > 0 else "BUY"

        open_trades = self.ib.openTrades()
        for t in open_trades:
            if t.contract.conId != self.contract.conId or not t.isActive():
                continue

            parent_id = getattr(t.order, "parentId", 0)

            # Identify if this is a closing SL or TP.
            # We assume any active order with an ocaGroup or parentId is an SL/TP bracket child.
            is_sl_tp = False
            if t.order.ocaGroup:
                is_sl_tp = True
            elif parent_id != 0:
                is_sl_tp = True

            if not is_sl_tp:
                continue

            # Check if this child's parent is still active. If so, it protects an unfilled entry,
            # NOT the current position. Do not sync or cancel it yet.
            if parent_id != 0:
                parent_trade = next(
                    (pt for pt in open_trades if pt.order.orderId == parent_id), None
                )
                if parent_trade and parent_trade.isActive():
                    LOGGER.debug(
                        f"Sync: Ignoring child orderId={t.order.orderId} because "
                        f"parentId={parent_id} is still active."
                    )
                    continue

            # If position is 0, cancel all SL/TP
            if abs_pos == 0:
                LOGGER.info(
                    f"Sync: Position is 0, cancelling SL/TP orderId={t.order.orderId}"
                )
                self.ib.cancelOrder(t.order)
                continue

            # If position is non-zero, check direction
            if t.order.action != closing_action:
                LOGGER.info(
                    f"Sync: Wrong direction, cancelling SL/TP orderId={t.order.orderId}"
                )
                self.ib.cancelOrder(t.order)
                continue

            # Right direction, update volume if different
            if t.order.totalQuantity != abs_pos:
                LOGGER.info(
                    f"Sync: Updating SL/TP orderId={t.order.orderId} volume {t.order.totalQuantity} -> {abs_pos}"
                )
                try:
                    self.edit_order(t.order.orderId, quantity=abs_pos)
                except Exception as e:
                    LOGGER.error(
                        f"Sync: Failed to update orderId={t.order.orderId}: {e}"
                    )

    # --------------------------------------------------------------------------
    # Bear swing leg calculation (symmetric to bull leg in base)
    # --------------------------------------------------------------------------

    def _find_consecutive_bear_leg_high_from_snapshot(
        self, lst: List[Dict[str, Any]], end_index: Optional[int] = None
    ) -> float:
        """
        Return high price of consecutive bear bar(s) ending at end_index.
        Bear bar: close < open.
        If none found, return high of the bar at end_index (fallback).
        """
        if not lst:
            raise Exception("No bar data available for instrument")

        if end_index is None:
            end_index = len(lst) - 1

        end_index = min(end_index, len(lst) - 1)
        if end_index < 0:
            raise Exception("No completed bars")

        df = pd.DataFrame(
            {
                "open": [b["open"] for b in lst],
                "high": [b["high"] for b in lst],
                "low": [b["low"] for b in lst],
                "close": [b["close"] for b in lst],
            }
        )

        # Bear bars: close < open
        bear_mask = df["close"] < df["open"]

        i = end_index
        bear_indices = []
        while i >= 0 and bear_mask.iloc[i]:
            bear_indices.append(i)
            i -= 1
        LOGGER.debug(f"Bear leg indices: {json.dumps(bear_indices)}")

        if not bear_indices:
            return float(lst[end_index]["high"])
        highs = [lst[idx]["high"] for idx in bear_indices]
        return float(max(highs))

    # --------------------------------------------------------------------------
    # Override: market prices from redis metadata
    # --------------------------------------------------------------------------

    async def _get_market_buy_price(self, instrument_id: str) -> Optional[float]:
        """Buy price = last sell (ask) from redis metadata."""
        meta = await self._get_metadata_snapshot(instrument_id)
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

    async def _get_market_sell_price(self, instrument_id: str) -> Optional[float]:
        """Sell price = last buy (bid) from redis metadata."""
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

    # --------------------------------------------------------------------------
    # Subclass hook implementations: trigger/limit calculation
    # --------------------------------------------------------------------------

    def _compute_buy_stop_trigger_and_limit(
        self,
        high_last: float,
        tick: float,
        tick_coeff: float,
    ) -> tuple:
        """
        IBKR buy stop:
        - stop_price = high_last + tick (1 tick above high)
        - limit_price = stop_price (not used for pure stop orders on IBKR)
        - trigger_on_mm = False
        """
        stop_price = round(high_last + tick, 2)
        return stop_price, stop_price, False

    def _compute_sell_stop_trigger_and_limit(
        self,
        trigger_price: float,
        tick: float,
        tick_coeff: float,
    ) -> tuple:
        """
        IBKR sell stop:
        - trigger = trigger_price (already computed by caller)
        - limit = trigger (not used for pure stop orders on IBKR)
        - trigger_on_mm = False
        """
        return trigger_price, trigger_price, False

    def _compute_take_profit_limit(
        self,
        take_profit: float,
        tick: float,
    ) -> tuple:
        """
        IBKR take-profit:
        - limit = take_profit
        - trigger_on_mm = False
        """
        return take_profit, False

    # --------------------------------------------------------------------------
    # Bracket order helper
    # --------------------------------------------------------------------------

    def _place_bracket_stop_entry(
        self,
        action: str,
        volume: int,
        stop_price: float,
        sl_price: float,
        tp_price: float,
    ) -> Trade:
        """
        Place a bracket order: parent stop entry + attached SL and TP children.

        The SL and TP are child orders (parentId = parent.orderId) so they only
        become active after the parent entry order fills. This is the proper IBKR
        bracket pattern (equivalent to attaching SL/TP in TWS).

        Args:
            action: 'BUY' or 'SELL' for the entry direction.
            volume: number of contracts.
            stop_price: trigger price for the parent stop entry.
            sl_price: stop-loss price (opposite direction stop).
            tp_price: take-profit price (opposite direction limit).

        Returns:
            The parent Trade object.
        """
        opposite = "SELL" if action == "BUY" else "BUY"

        # Pre-assign order IDs in the same sequence as placement: parent → TP → SL
        parent_id = self.ib.client.getReqId()
        tp_id = self.ib.client.getReqId()
        sl_id = self.ib.client.getReqId()

        # Parent entry: stop order, don't transmit yet
        parent = StopOrder(action, volume, self._round_price(stop_price), tif="DAY")
        parent.orderId = parent_id
        parent.transmit = False

        # Take-profit child: limit order (placed second, don't transmit yet)
        tp_order = LimitOrder(opposite, volume, self._round_price(tp_price), tif="DAY")
        tp_order.orderId = tp_id
        tp_order.parentId = parent_id
        tp_order.transmit = False

        # Stop-loss child: stop order (placed last, transmit=True triggers the whole group)
        sl_order = StopOrder(opposite, volume, self._round_price(sl_price), tif="DAY")
        sl_order.orderId = sl_id
        sl_order.parentId = parent_id
        sl_order.transmit = True  # transmit all orders in the bracket

        parent_trade = self.ib.placeOrder(self.contract, parent)
        LOGGER.info(
            f"Bracket parent {action} STOP: orderId={parent_id}, "
            f"stopPrice={stop_price}"
        )
        tp_trade = self.ib.placeOrder(self.contract, tp_order)
        LOGGER.info(
            f"Bracket child {opposite} LIMIT (TP): orderId={tp_id}, "
            f"parentId={parent_id}, limitPrice={tp_price}"
        )
        sl_trade = self.ib.placeOrder(self.contract, sl_order)
        LOGGER.info(
            f"Bracket child {opposite} STOP (SL): orderId={sl_id}, "
            f"parentId={parent_id}, stopPrice={sl_price}"
        )
        return parent_trade

    # --------------------------------------------------------------------------
    # Override: broker-specific order execution hooks (ib_async)
    # Position-aware logic for buy stop and sell stop.
    # --------------------------------------------------------------------------

    def _execute_market_buy_order(
        self, instrument_id: str, info: Dict[str, Any], price: float, volume: int
    ) -> Any:
        """Place a market buy order via IBKR."""
        order = MarketOrder("BUY", volume, tif="DAY")
        trade = self.ib.placeOrder(self.contract, order)
        LOGGER.info(
            f"Placed market BUY order via IBKR: "
            f"orderId={trade.order.orderId}, status={trade.orderStatus.status}"
        )
        return trade

    def _execute_market_sell_order(
        self, instrument_id: str, info: Dict[str, Any], price: float, volume: int
    ) -> Any:
        """Place a market sell order via IBKR."""
        order = MarketOrder("SELL", volume, tif="DAY")
        trade = self.ib.placeOrder(self.contract, order)
        LOGGER.info(
            f"Placed market SELL order via IBKR: "
            f"orderId={trade.order.orderId}, status={trade.orderStatus.status}"
        )
        return trade

    def _execute_buy_stop_orders(
        self,
        instrument_id: str,
        info: Dict[str, Any],
        volume: int,
        stop_price: float,
        limit_price: float,
        trigger_on_mm: bool,
        sell_stop_price: float,
        sell_limit: float,
        sell_trigger_on_mm: bool,
        take_profit: float,
        tp_limit: float,
        tp_trigger_on_mm: bool,
        tick: float,
        tick_coeff: float,
    ) -> None:
        """
        Position-aware buy stop execution via IBKR.

        Scenarios based on existing position:
        1. Flat (no position) → Enter long: bracket order (stop + SL + TP)
        2. Long (same direction) → Scale up: stop + standalone SL/TP (reuse prices)
        3. Short, abs(pos) == volume → Close short: stop only
        4. Short, abs(pos) < volume → Close + enter long: bracket for full
           volume with SL/TP for net (volume - abs(pos)) contracts
        """
        signed_pos = self._get_signed_position()
        LOGGER.info(
            f"Buy stop: volume={volume}, signed_pos={signed_pos}, "
            f"stop_price={stop_price}"
        )

        if signed_pos < 0 and abs(signed_pos) == volume:
            # Scenario 3: Close short — standalone stop only, no SL/TP
            # Check if there is an existing SL order (from bracket/OCA) to adjust
            existing_sl = self._find_existing_sl_order("BUY")
            if existing_sl:
                new_price = self._round_price(stop_price)
                LOGGER.info(
                    f"Buy stop: adjusting existing SL orderId="
                    f"{existing_sl.order.orderId} price to {new_price}"
                )
                self.edit_order(existing_sl.order.orderId, price=new_price)
                return
            LOGGER.info("Buy stop: closing short position (exact match)")
            order = StopOrder("BUY", volume, self._round_price(stop_price), tif="DAY")
            order.orderRef = "CloseOnly"
            trade = self.ib.placeOrder(self.contract, order)
            LOGGER.info(
                f"Placed buy STOP (close short) via IBKR: "
                f"orderId={trade.order.orderId}, stopPrice={stop_price}"
            )
            return

        if signed_pos > 0:
            # Scenario 2: Scale up long — standalone stop only
            LOGGER.info("Buy stop: scaling up long position")
            order = StopOrder("BUY", volume, self._round_price(stop_price), tif="DAY")
            order.orderRef = "ScaleUp"
            trade = self.ib.placeOrder(self.contract, order)
            LOGGER.info(
                f"Placed buy STOP (scale up) via IBKR: "
                f"orderId={trade.order.orderId}, stopPrice={stop_price}"
            )
            return

        if signed_pos < 0 and abs(signed_pos) < volume:
            # Scenario 4: Close short + enter long
            # Two separate orders:
            #   1. Standalone stop to close the existing short position
            #   2. Bracket stop entry for the net new long position
            close_volume = abs(signed_pos)
            net_volume = volume - close_volume
            LOGGER.info(
                f"Buy stop: close short ({close_volume}) + "
                f"enter long ({net_volume})"
            )
            # 1. Close existing short
            close_order = StopOrder(
                "BUY", close_volume, self._round_price(stop_price), tif="DAY"
            )
            close_order.orderRef = "CloseOnly"
            close_trade = self.ib.placeOrder(self.contract, close_order)
            LOGGER.info(
                f"Placed buy STOP (close short) via IBKR: "
                f"orderId={close_trade.order.orderId}, "
                f"volume={close_volume}, stopPrice={stop_price}"
            )
            # 2. Enter new long with bracket SL/TP
            self._place_bracket_stop_entry(
                action="BUY",
                volume=net_volume,
                stop_price=stop_price,
                sl_price=sell_stop_price,
                tp_price=tp_limit,
            )
            return

        # Scenario 1: Flat — enter long with bracket (stop + SL + TP)
        LOGGER.info("Buy stop: entering new long position")
        self._place_bracket_stop_entry(
            action="BUY",
            volume=volume,
            stop_price=stop_price,
            sl_price=sell_stop_price,
            tp_price=tp_limit,
        )

    def _execute_sell_stop_order(
        self,
        instrument_id: str,
        info: Dict[str, Any],
        volume: int,
        stop_price: float,
        sell_limit: float,
        sell_trigger_on_mm: bool,
    ) -> None:
        """
        Position-aware sell stop execution via IBKR.

        Scenarios based on existing position:
        1. Flat (no position) → Enter short: bracket order (stop + SL + TP)
        2. Short (same direction) → Scale up: stop + standalone SL/TP (reuse prices)
        3. Long, pos == volume → Close long: stop only
        4. Long, pos < volume → Close + enter short: bracket for full volume
           with SL/TP for net (volume - pos) contracts

        For short entries, SL/TP are computed from bar data:
        - SL (buy stop) = swing_high + tick (consecutive bear bars)
        - TP (buy limit) = low - PROFIT_LOSS_RATIO * (swing_high - low) + tick
        """
        signed_pos = self._get_signed_position()
        tick = self._get_tick_size(instrument_id)
        LOGGER.info(
            f"Sell stop: volume={volume}, signed_pos={signed_pos}, "
            f"stop_price={stop_price}"
        )

        if signed_pos > 0 and signed_pos == volume:
            # Scenario 3: Close long — standalone stop only, no SL/TP
            # Check if there is an existing SL order (from bracket/OCA) to adjust
            existing_sl = self._find_existing_sl_order("SELL")
            if existing_sl:
                new_price = self._round_price(stop_price)
                LOGGER.info(
                    f"Sell stop: adjusting existing SL orderId="
                    f"{existing_sl.order.orderId} price to {new_price}"
                )
                self.edit_order(existing_sl.order.orderId, price=new_price)
                return
            LOGGER.info("Sell stop: closing long position (exact match)")
            order = StopOrder("SELL", volume, self._round_price(stop_price), tif="DAY")
            order.orderRef = "CloseOnly"
            trade = self.ib.placeOrder(self.contract, order)
            LOGGER.info(
                f"Placed sell STOP (close long) via IBKR: "
                f"orderId={trade.order.orderId}, stopPrice={stop_price}"
            )
            return

        if signed_pos < 0:
            # Scenario 2: Scale up short — standalone stop only
            LOGGER.info("Sell stop: scaling up short position")
            order = StopOrder("SELL", volume, self._round_price(stop_price), tif="DAY")
            order.orderRef = "ScaleUp"
            trade = self.ib.placeOrder(self.contract, order)
            LOGGER.info(
                f"Placed sell STOP (scale up) via IBKR: "
                f"orderId={trade.order.orderId}, stopPrice={stop_price}"
            )
            return

        # For flat or close+enter, compute SL/TP for the short entry
        sl_price, tp_price = self._compute_short_entry_sl_tp(
            instrument_id, stop_price, tick
        )

        if signed_pos > 0 and signed_pos < volume:
            # Scenario 4: Close long + enter short
            # Two separate orders:
            # 1. Standalone stop to close the existing long position
            #   2. Bracket stop entry for the net new short position
            close_volume = signed_pos
            net_volume = volume - close_volume
            LOGGER.info(
                f"Sell stop: close long ({close_volume}) + "
                f"enter short ({net_volume})"
            )
            # 1. Close existing long
            close_order = StopOrder(
                "SELL", close_volume, self._round_price(stop_price), tif="DAY"
            )
            close_order.orderRef = "CloseOnly"
            close_trade = self.ib.placeOrder(self.contract, close_order)
            LOGGER.info(
                f"Placed sell STOP (close long) via IBKR: "
                f"orderId={close_trade.order.orderId}, "
                f"volume={close_volume}, stopPrice={stop_price}"
            )
            # 2. Enter new short with bracket SL/TP
            self._place_bracket_stop_entry(
                action="SELL",
                volume=net_volume,
                stop_price=stop_price,
                sl_price=sl_price,
                tp_price=tp_price,
            )
            return

        # Scenario 1: Flat — enter short with bracket
        LOGGER.info("Sell stop: entering new short position")
        self._place_bracket_stop_entry(
            action="SELL",
            volume=volume,
            stop_price=stop_price,
            sl_price=sl_price,
            tp_price=tp_price,
        )

    def _compute_short_entry_sl_tp(
        self, instrument_id: str, stop_price: float, tick: float
    ) -> Tuple[float, float]:
        """
        Compute SL and TP prices for a new short entry.

        - SL (buy stop) = high of consecutive bear swing leg + tick
        - TP (buy limit) = stop_price - PROFIT_LOSS_RATIO * distance + tick
          where distance = swing_high - low_last (from last completed bar)

        Uses synchronous access to recent_bars (safe because the caller's
        async task already holds a consistent view of the bars).
        """
        lst = list(self.recent_bars.get(instrument_id, []))
        if not lst:
            raise Exception("No bar data available for short entry SL/TP")

        last_idx = self._last_completed_index_from_snapshot(lst)
        if last_idx is None:
            raise Exception("No completed bar for short entry SL/TP")

        last_bar = lst[last_idx]
        low_last = float(last_bar["low"])

        try:
            swing_high = self._find_consecutive_bear_leg_high_from_snapshot(
                lst, end_index=last_idx
            )
        except Exception:
            swing_high = float(last_bar["high"])

        sl_price = round(swing_high + tick, 2)
        distance = swing_high - low_last
        tp_price = round(low_last - PROFIT_LOSS_RATIO * distance + tick, 2)

        LOGGER.info(
            f"Short entry SL/TP: swing_high={swing_high}, low_last={low_last}, "
            f"sl_price={sl_price}, tp_price={tp_price}"
        )
        return sl_price, tp_price

    # --------------------------------------------------------------------------
    # Override: Avanza-specific methods that don't apply to IBKR
    # --------------------------------------------------------------------------

    def delete_stop_losses(self, instrument_id: str) -> None:
        """Cancel all open stop orders for the contract via IBKR."""
        open_trades = self.ib.openTrades()
        for t in open_trades:
            if (
                t.contract.conId == self.contract.conId
                and t.order.orderType in ("STP", "STP LMT")
                and t.isActive()
            ):
                LOGGER.info(f"Cancelling IBKR stop order: orderId={t.order.orderId}")
                self.ib.cancelOrder(t.order)

    def cleanup_residual_sell_stop_losses(self) -> List[Dict[str, Any]]:
        raise NotImplementedError(
            "cleanup_residual_sell_stop_losses is Avanza-specific"
        )

    # --------------------------------------------------------------------------
    # Order editing: IBKR implementations
    # --------------------------------------------------------------------------

    def _find_trade_by_order_id(self, order_id: int) -> Optional[Trade]:
        """Find an active Trade object by its orderId or permId."""
        for t in self.ib.openTrades():
            if (
                t.order.orderId == order_id or t.order.permId == order_id
            ) and t.isActive():
                return t
        return None

    def edit_order(
        self,
        order_id: int,
        price: Optional[float] = None,
        quantity: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Change the price and/or quantity of an existing active order.
        For STP orders, price updates auxPrice. For LMT orders, price updates lmtPrice.
        """
        trade = self._find_trade_by_order_id(order_id)
        if not trade:
            raise Exception(f"No active order found with orderId={order_id}")

        order = trade.order

        # Apply quantity update if provided
        if quantity is not None:
            if quantity <= 0:
                raise Exception("Quantity must be greater than 0")
            order.totalQuantity = quantity

        # Apply price update if provided
        if price is not None:
            if order.orderType == "MKT":
                raise Exception("Cannot edit price of a market order")

            price = self._round_price(price)

            if order.orderType == "STP":
                order.auxPrice = price
            elif order.orderType == "LMT":
                order.lmtPrice = price
            elif order.orderType == "STP LMT":
                order.auxPrice = price
            else:
                raise Exception(
                    f"Unsupported order type for price edit: {order.orderType}"
                )

        # Sanitize order fields that TWS might have populated with localized strings
        if getattr(order, "deltaNeutralOrderType", "") == "无":
            order.deltaNeutralOrderType = ""
        if getattr(order, "adjustedOrderType", "") == "无":
            order.adjustedOrderType = ""

        # If parent is no longer active (e.g. fulfilled), clear parentId
        # Otherwise TWS will reject the modify with "Cannot find parent order"
        if getattr(order, "parentId", 0) != 0:
            parent_trade = self._find_trade_by_order_id(order.parentId)
            if not parent_trade:
                order.parentId = 0

        # IMPORTANT: Force transmit=True on modifications. Bracket children may
        # have transmit=False from their initial creation. Leaving it False
        # causes the modification to be accepted but suspends the order locally.
        order.transmit = True

        self.ib.placeOrder(self.contract, order)
        LOGGER.info(
            f"Edited order {order_id}: type={order.orderType}, newPrice={price}, newQuantity={quantity}"
        )
        return {
            "orderId": order_id,
            "orderType": order.orderType,
            "newPrice": price,
            "newQuantity": quantity,
            "status": "modified",
        }

    def edit_order_follow_market(self, order_id: int) -> Dict[str, Any]:
        """
        Convert an existing order to a market order.
        Cancels the existing order and places a new MarketOrder with the same
        action and volume.
        """
        trade = self._find_trade_by_order_id(order_id)
        if not trade:
            raise Exception(f"No active order found with orderId={order_id}")

        action = trade.order.action
        volume = int(trade.order.totalQuantity)

        self.ib.cancelOrder(trade.order)
        LOGGER.info(f"Cancelled order {order_id} to replace with market order")

        mkt_order = MarketOrder(action, volume, tif="DAY")
        new_trade = self.ib.placeOrder(self.contract, mkt_order)
        LOGGER.info(
            f"Placed market {action} order: orderId={new_trade.order.orderId}, "
            f"volume={volume}"
        )
        return {
            "oldOrderId": order_id,
            "newOrderId": new_trade.order.orderId,
            "action": action,
            "volume": volume,
            "status": "converted_to_market",
        }

    # --------------------------------------------------------------------------
    # OCA bracket: place SL + TP given explicit prices
    # --------------------------------------------------------------------------

    def place_oca_bracket(
        self,
        action: str,
        volume: int,
        limit_price: float,
        stop_price: float,
    ) -> Dict[str, Any]:
        """
        Place an OCA group with a take-profit (limit) and stop-loss (stop)
        given explicit prices. Both orders are immediately active.

        Args:
            action: 'SELL' or 'BUY' — the exit direction (opposite of position).
            volume: number of contracts.
            limit_price: take-profit limit price.
            stop_price: stop-loss stop price.

        Returns:
            Dict with order IDs and OCA group name.
        """
        # Check active position
        pos_size = self._get_instrument_position_size("")
        if pos_size == 0:
            raise Exception(
                "No active position found. OCA bracket is not allowed when flat."
            )

        # Check TP/SL relationship and action
        if limit_price == stop_price:
            raise Exception("Limit (TP) and Stop (SL) prices cannot be equal.")
        if limit_price > stop_price and action != "SELL":
            raise Exception(
                "TP is greater than SL: Action must be SELL (closing a long position)."
            )
        if limit_price < stop_price and action != "BUY":
            raise Exception(
                "TP is smaller than SL: Action must be BUY (closing a short position)."
            )

        tp_order = LimitOrder(action, volume, self._round_price(limit_price), tif="DAY")
        sl_order = StopOrder(action, volume, self._round_price(stop_price), tif="DAY")

        # Sanitize order fields that TWS might have populated with localized strings
        for order in [tp_order, sl_order]:
            if getattr(order, "deltaNeutralOrderType", "") == "无":
                order.deltaNeutralOrderType = ""
            if getattr(order, "adjustedOrderType", "") == "无":
                order.adjustedOrderType = ""

        oca_group = f"ibkr_oca_manual_{int(datetime.now(tz=timezone.utc).timestamp())}"
        IB.oneCancelsAll(
            orders=[tp_order, sl_order],
            ocaGroup=oca_group,
            ocaType=1,
        )

        tp_trade = self.ib.placeOrder(self.contract, tp_order)
        sl_trade = self.ib.placeOrder(self.contract, sl_order)

        LOGGER.info(
            f"OCA bracket placed: action={action}, volume={volume}, "
            f"TP orderId={tp_trade.order.orderId} @ {limit_price}, "
            f"SL orderId={sl_trade.order.orderId} @ {stop_price}, "
            f"ocaGroup={oca_group}"
        )
        return {
            "ocaGroup": oca_group,
            "tpOrderId": tp_trade.order.orderId,
            "tpPrice": limit_price,
            "slOrderId": sl_trade.order.orderId,
            "slPrice": stop_price,
            "action": action,
            "volume": volume,
            "status": "placed",
        }

    def place_limit_buy(self, volume: int, price: float) -> Dict[str, Any]:
        """Place a limit buy order via IBKR."""
        order = LimitOrder("BUY", volume, self._round_price(price), tif="DAY")
        trade = self.ib.placeOrder(self.contract, order)
        LOGGER.info(
            f"Placed limit BUY order via IBKR: "
            f"orderId={trade.order.orderId}, price={price}, volume={volume}"
        )
        return {
            "orderId": trade.order.orderId,
            "action": "BUY",
            "orderType": "LMT",
            "totalQuantity": volume,
            "price": price,
            "status": trade.orderStatus.status,
        }

    def place_limit_sell(self, volume: int, price: float) -> Dict[str, Any]:
        """Place a limit sell order via IBKR."""
        order = LimitOrder("SELL", volume, self._round_price(price), tif="DAY")
        trade = self.ib.placeOrder(self.contract, order)
        LOGGER.info(
            f"Placed limit SELL order via IBKR: "
            f"orderId={trade.order.orderId}, price={price}, volume={volume}"
        )
        return {
            "orderId": trade.order.orderId,
            "action": "SELL",
            "orderType": "LMT",
            "totalQuantity": volume,
            "price": price,
            "status": trade.orderStatus.status,
        }

    # --------------------------------------------------------------------------
    # Trade event subscription & order lifecycle tracking
    # --------------------------------------------------------------------------

    def _setup_trade_subscription(self, on_change_callback=None):
        """
        Subscribe to IBKR trade events for order lifecycle tracking.
        Calls on_change_callback(orders_snapshot) whenever an order changes.
        """
        self._on_change_callback = on_change_callback

        self.ib.orderStatusEvent += self._on_order_status
        self.ib.newOrderEvent += self._on_new_order
        self.ib.openOrderEvent += self._on_open_order
        self.ib.errorEvent += self._on_error
        self.ib.execDetailsEvent += self._on_exec_details
        self.ib.positionEvent += self._on_position
        LOGGER.info("Subscribed to IBKR trade events")

    def _on_order_status(self, trade: Trade):
        """Handle order status change events."""
        if trade.contract.conId != self.contract.conId:
            return
        LOGGER.info(
            f"Order status event: orderId={trade.order.orderId}, "
            f"status={trade.orderStatus.status}"
        )
        # We do not perform reactive auto-cancellations here, as IBKR TWS handles
        # parent-child bracket cancellations natively, and transient 'Cancelled' states
        # triggered by warnings (like TIFDAY preset error 10349) would cause premature
        # cancellation of child orders. Explicit cancellations are handled in cancel_order().

        if self._on_change_callback:
            self._on_change_callback(self.get_open_orders())

    def _on_new_order(self, trade: Trade):
        """Handle new order events."""
        if trade.contract.conId != self.contract.conId:
            return
        LOGGER.debug(f"New order event: orderId={trade.order.orderId}")
        if self._on_change_callback:
            self._on_change_callback(self.get_open_orders())

    def _on_position(self, position) -> None:
        """
        Handle position update events.

        positionEvent fires AFTER ib.positions() has been updated with the new
        quantity, so _get_signed_position() will return the correct new value
        here.  This is the correct place to synchronise SL/TP volumes after a
        fill, because execDetailsEvent fires BEFORE the position is refreshed.
        """
        if position.contract.conId != self.contract.conId:
            return
        LOGGER.info(
            f"Position update: conId={position.contract.conId}, "
            f"position={position.position}, avgCost={position.avgCost}"
        )
        try:
            self._sync_sl_tp_volume()
        except Exception as e:
            LOGGER.error(f"Error syncing SL/TP volume on position update: {e}")
        if self._on_change_callback:
            self._on_change_callback(self.get_open_orders())

    def _on_open_order(self, trade: Trade):
        """Handle open order events (e.g. order modification)."""
        if trade.contract.conId != self.contract.conId:
            return
        LOGGER.debug(f"Open order event: orderId={trade.order.orderId}")
        if self._on_change_callback:
            self._on_change_callback(self.get_open_orders())

    def _on_exec_details(self, trade: Trade, _fill: Any):
        """Handle execution details (fills)."""
        if trade.contract.conId != self.contract.conId:
            return

        # NOTE: We intentionally do NOT call _sync_sl_tp_volume here.
        # ib.positions() is NOT yet updated when execDetailsEvent fires — it
        # updates asynchronously via a separate positionEvent.  Calling
        # _sync_sl_tp_volume here would read the OLD position size, causing the
        # SL/TP quantity check (totalQuantity != abs_pos) to always be False and
        # silently skip the update.  Instead we call _sync_sl_tp_volume from
        # _on_position, which fires only after ib.positions() is up to date.

        if self._on_change_callback:
            self._on_change_callback(self.get_open_orders())

    def _on_error(self, reqId: int, _errorCode: int, _errorString: str, contract: Any):
        """Handle errors, which could be order rejections/cancellations."""
        if reqId != -1 and self._on_change_callback:
            # Rejections often cause the order status to change to Cancelled/Inactive,
            # so we trigger an update.
            self._on_change_callback(self.get_open_orders())

    def get_open_orders(self) -> List[Dict[str, Any]]:
        """
        Return active and related done orders for this contract as a list of dicts.
        Used by frontend to render the order lifecycle panel.
        """
        contract_trades = [
            t for t in self.ib.trades() if t.contract.conId == self.contract.conId
        ]

        # Build local orderId to global permId mapping for all contract trades
        order_id_to_perm_id = {}
        for t in contract_trades:
            oid = getattr(t.order, "orderId", 0)
            pid = getattr(t.order, "permId", 0)
            if oid and pid:
                order_id_to_perm_id[oid] = pid

        def get_parent_perm_id(order):
            # 1. Try parentId mapped to permId
            p_id = getattr(order, "parentId", 0)
            if p_id and p_id != IB_UNSET_INT:
                mapped = order_id_to_perm_id.get(p_id)
                if mapped:
                    return mapped
            # 2. Fall back to parentPermId
            parent_perm = getattr(order, "parentPermId", 0)
            if parent_perm and parent_perm != IB_UNSET_INT:
                return parent_perm
            return 0

        # Update cache for all trades
        for t in contract_trades:
            order = t.order
            pid = getattr(order, "permId", 0)
            if not pid:
                continue

            p_perm = get_parent_perm_id(order)
            if p_perm:
                self.perm_id_to_parent_perm_id[pid] = p_perm

            oca = getattr(order, "ocaGroup", "")
            if oca:
                self.perm_id_to_oca_group[pid] = oca

        # Step 1: Find all parent perm IDs
        parent_perm_ids = set()
        for t in contract_trades:
            pid = getattr(t.order, "permId", 0)
            p_perm = self.perm_id_to_parent_perm_id.get(pid, 0)
            if p_perm:
                parent_perm_ids.add(p_perm)

        # Step 2: Group trades
        trade_groups = {}
        for t in contract_trades:
            order = t.order
            pid = getattr(order, "permId", 0)
            p_perm = self.perm_id_to_parent_perm_id.get(pid, 0)
            has_parent = bool(p_perm)
            oca = self.perm_id_to_oca_group.get(pid, "")

            if has_parent:
                group_key = f"parent_{p_perm}"
            elif pid in parent_perm_ids:
                group_key = f"parent_{pid}"
            elif oca:
                group_key = f"oca_{oca}"
            else:
                group_key = f"order_{pid}"

            if group_key not in trade_groups:
                trade_groups[group_key] = []
            trade_groups[group_key].append(t)

        # Step 3: Determine which groups should stay
        result = []
        for group_key, trades in trade_groups.items():
            # If all orders in the group are done, do not show this group
            def is_trade_done(t):
                # ib_async's isDone() covers Filled and Cancelled, but 'Inactive'
                # or 'ApiCancelled' might not be fully covered or linger.
                if t.isDone():
                    return True
                if t.orderStatus.status in ["Inactive", "ApiCancelled", "Cancelled"]:
                    return True
                return False

            if all(is_trade_done(t) for t in trades):
                continue

            # Otherwise, keep all of them
            for t in trades:
                order = t.order
                price = None
                if order.orderType == "STP":
                    price = order.auxPrice
                elif order.orderType == "LMT":
                    price = order.lmtPrice
                elif order.orderType == "STP LMT":
                    price = order.auxPrice

                placed_time = self.order_placed_times.get(order.permId)
                if not placed_time:
                    if t.log:
                        placed_time = t.log[0].time
                    if not placed_time:
                        placed_time = datetime.now(timezone.utc)
                    self.order_placed_times[order.permId] = placed_time

                parent_id_val = self.perm_id_to_parent_perm_id.get(order.permId, 0)
                if parent_id_val == 0:
                    parent_id_val = None

                oca_val = self.perm_id_to_oca_group.get(order.permId, "")
                if not oca_val:
                    oca_val = None

                result.append(
                    {
                        "orderId": order.permId,
                        "action": order.action,
                        "orderType": order.orderType,
                        "totalQuantity": int(order.totalQuantity),
                        "price": price,
                        "status": t.orderStatus.status,
                        "parentId": parent_id_val,
                        "ocaGroup": oca_val,
                        "orderRef": order.orderRef,
                        "isDone": is_trade_done(t),
                        "fulfilled": t.orderStatus.status == "Filled",
                        "placedTime": placed_time.isoformat(),
                    }
                )
        return result

    def cancel_order(self, order_id: int) -> Dict[str, Any]:
        """Cancel a specific order by orderId."""
        trade = self._find_trade_by_order_id(order_id)
        if not trade:
            raise Exception(f"No active order found with orderId={order_id}")
        self.ib.cancelOrder(trade.order)
        trade.orderStatus.status = "Cancelled"
        LOGGER.info(f"Cancelled order {order_id}")
        return {"orderId": order_id, "status": "Cancelled"}

    def get_position_info(self) -> Optional[Dict[str, Any]]:
        """Return the current active position size and average cost for the contract."""
        positions = self.ib.positions()
        for pos in positions:
            if pos.contract.conId == self.contract.conId:
                try:
                    mult = (
                        float(self.contract.multiplier)
                        if self.contract.multiplier
                        else 1.0
                    )
                except (ValueError, TypeError):
                    mult = 1.0
                if mult <= 0:
                    mult = 1.0
                price = pos.avgCost / mult if pos.avgCost else 0.0
                return {"position": int(pos.position), "avgCost": price}
        return None

    # --------------------------------------------------------------------------
    # Override: Avanza-specific account/position methods
    # --------------------------------------------------------------------------

    def _get_accounts_and_positions(self) -> Dict[str, Any]:
        raise NotImplementedError("_get_accounts_and_positions is Avanza-specific")

    def _get_account_balance(self) -> float:
        raise NotImplementedError("_get_account_balance is Avanza-specific")

    def _get_account_positions(self) -> List[Dict[str, Any]]:
        raise NotImplementedError("_get_account_positions is Avanza-specific")

    def _get_instrument_position(self, instrument_id: str) -> Dict[str, Any]:
        raise NotImplementedError("_get_instrument_position is Avanza-specific")
