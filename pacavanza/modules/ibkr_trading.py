import asyncio
import json
import logging
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd
from datetime import datetime, timezone

from ib_async import IB, Future, MarketOrder, StopOrder, LimitOrder, Trade

from .base_trading import BaseAvanzaTrading, PROFIT_LOSS_RATIO

LOGGER = logging.getLogger("ibkr_trading")


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

    def _get_existing_sl_tp_prices(
        self, direction: str
    ) -> Tuple[Optional[float], Optional[float]]:
        """
        Find existing stop-loss and take-profit prices from open orders.

        Args:
            direction: 'long' or 'short' — the current position direction.
                For a long position, SL is a SELL stop and TP is a SELL limit.
                For a short position, SL is a BUY stop and TP is a BUY limit.

        Returns:
            (sl_price, tp_price) — either or both may be None if not found.
        """
        sl_price = None
        tp_price = None
        if direction == "long":
            sl_action, tp_action = "SELL", "SELL"
        else:  # short
            sl_action, tp_action = "BUY", "BUY"

        open_trades = self.ib.openTrades()
        for t in open_trades:
            if t.contract.conId != self.contract.conId or not t.isActive():
                continue
            if t.order.action == sl_action and t.order.orderType == "STP":
                sl_price = t.order.auxPrice  # stop price
            elif t.order.action == tp_action and t.order.orderType == "LMT":
                tp_price = t.order.lmtPrice
        return sl_price, tp_price

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
        parent = StopOrder(action, volume, stop_price)
        parent.orderId = parent_id
        parent.transmit = False

        # Take-profit child: limit order (placed second, don't transmit yet)
        tp_order = LimitOrder(opposite, volume, tp_price)
        tp_order.orderId = tp_id
        tp_order.parentId = parent_id
        tp_order.transmit = False

        # Stop-loss child: stop order (placed last, transmit=True triggers the whole group)
        sl_order = StopOrder(opposite, volume, sl_price)
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

    def _place_standalone_sl_tp(
        self,
        instrument_id: str,
        sl_action: str,
        sl_volume: int,
        sl_price: float,
        tp_action: str,
        tp_volume: int,
        tp_price: float,
    ) -> None:
        """
        Place standalone SL and TP orders linked by OCA group.

        Used for scale-up scenarios where there is no parent entry to attach
        children to — the SL/TP must be immediately active.
        """
        sl_order = StopOrder(sl_action, sl_volume, sl_price)
        tp_order = LimitOrder(tp_action, tp_volume, tp_price)

        oca_group = (
            f"ibkr_oca_{instrument_id}_{int(datetime.now(tz=timezone.utc).timestamp())}"
        )
        IB.oneCancelsAll(
            orders=[sl_order, tp_order],
            ocaGroup=oca_group,
            ocaType=1,
        )

        sl_trade = self.ib.placeOrder(self.contract, sl_order)
        LOGGER.info(
            f"Standalone {sl_action} STOP (SL): orderId={sl_trade.order.orderId}, "
            f"stopPrice={sl_price}, ocaGroup={oca_group}"
        )
        tp_trade = self.ib.placeOrder(self.contract, tp_order)
        LOGGER.info(
            f"Standalone {tp_action} LIMIT (TP): orderId={tp_trade.order.orderId}, "
            f"limitPrice={tp_price}, ocaGroup={oca_group}"
        )

    # --------------------------------------------------------------------------
    # Override: broker-specific order execution hooks (ib_async)
    # Position-aware logic for buy stop and sell stop.
    # --------------------------------------------------------------------------

    def _execute_market_buy_order(
        self, instrument_id: str, info: Dict[str, Any], price: float, volume: int
    ) -> Any:
        """Place a market buy order via IBKR."""
        order = MarketOrder("BUY", volume)
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
        order = MarketOrder("SELL", volume)
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
            LOGGER.info("Buy stop: closing short position (exact match)")
            order = StopOrder("BUY", volume, stop_price)
            trade = self.ib.placeOrder(self.contract, order)
            LOGGER.info(
                f"Placed buy STOP (close short) via IBKR: "
                f"orderId={trade.order.orderId}, stopPrice={stop_price}"
            )
            return

        if signed_pos > 0:
            # Scenario 2: Scale up long — standalone stop + OCA SL/TP
            LOGGER.info("Buy stop: scaling up long position")
            order = StopOrder("BUY", volume, stop_price)
            trade = self.ib.placeOrder(self.contract, order)
            LOGGER.info(
                f"Placed buy STOP (scale up) via IBKR: "
                f"orderId={trade.order.orderId}, stopPrice={stop_price}"
            )
            existing_sl, existing_tp = self._get_existing_sl_tp_prices("long")
            sl = existing_sl if existing_sl is not None else sell_stop_price
            tp = existing_tp if existing_tp is not None else tp_limit
            if existing_sl is None or existing_tp is None:
                LOGGER.warning(
                    "Scale up: no existing SL/TP found, using computed prices"
                )
            self._place_standalone_sl_tp(
                instrument_id=instrument_id,
                sl_action="SELL",
                sl_volume=volume,
                sl_price=sl,
                tp_action="SELL",
                tp_volume=volume,
                tp_price=tp,
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
            close_order = StopOrder("BUY", close_volume, stop_price)
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
            LOGGER.info("Sell stop: closing long position (exact match)")
            order = StopOrder("SELL", volume, stop_price)
            trade = self.ib.placeOrder(self.contract, order)
            LOGGER.info(
                f"Placed sell STOP (close long) via IBKR: "
                f"orderId={trade.order.orderId}, stopPrice={stop_price}"
            )
            return

        if signed_pos < 0:
            # Scenario 2: Scale up short — standalone stop + OCA SL/TP
            LOGGER.info("Sell stop: scaling up short position")
            order = StopOrder("SELL", volume, stop_price)
            trade = self.ib.placeOrder(self.contract, order)
            LOGGER.info(
                f"Placed sell STOP (scale up) via IBKR: "
                f"orderId={trade.order.orderId}, stopPrice={stop_price}"
            )
            existing_sl, existing_tp = self._get_existing_sl_tp_prices("short")
            if existing_sl is not None and existing_tp is not None:
                sl, tp = existing_sl, existing_tp
            else:
                LOGGER.warning(
                    "Scale up: no existing SL/TP found, computing new prices"
                )
                sl, tp = self._compute_short_entry_sl_tp(
                    instrument_id, stop_price, tick
                )
            self._place_standalone_sl_tp(
                instrument_id=instrument_id,
                sl_action="BUY",
                sl_volume=volume,
                sl_price=sl,
                tp_action="BUY",
                tp_volume=volume,
                tp_price=tp,
            )
            return

        # For flat or close+enter, compute SL/TP for the short entry
        sl_price, tp_price = self._compute_short_entry_sl_tp(
            instrument_id, stop_price, tick
        )

        if signed_pos > 0 and signed_pos < volume:
            # Scenario 4: Close long + enter short
            # Two separate orders:
            #   1. Standalone stop to close the existing long position
            #   2. Bracket stop entry for the net new short position
            close_volume = signed_pos
            net_volume = volume - close_volume
            LOGGER.info(
                f"Sell stop: close long ({close_volume}) + "
                f"enter short ({net_volume})"
            )
            # 1. Close existing long
            close_order = StopOrder("SELL", close_volume, stop_price)
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

    def cleanup_residual_orders(self) -> List[Dict[str, Any]]:
        """
        Cancel residual orders that have no corresponding position.
        IBKR version: check positions and cancel orphaned orders.
        """
        position_size = self._get_instrument_position_size("")
        if position_size > 0:
            return []  # have a position, don't clean up

        open_trades = self.ib.openTrades()
        cancelled = []
        for t in open_trades:
            if (
                t.contract.conId == self.contract.conId
                and t.order.orderType in ("STP", "LMT", "STP LMT")
                and t.isActive()
            ):
                LOGGER.info(
                    f"Cancelling residual IBKR order: orderId={t.order.orderId}"
                )
                self.ib.cancelOrder(t.order)
                cancelled.append({"orderId": t.order.orderId})
        return cancelled

    # --------------------------------------------------------------------------
    # Order editing: IBKR implementations
    # --------------------------------------------------------------------------

    def _find_trade_by_order_id(self, order_id: int) -> Optional[Trade]:
        """Find an active Trade object by its orderId."""
        for t in self.ib.openTrades():
            if t.order.orderId == order_id and t.isActive():
                return t
        return None

    def edit_order(self, order_id: int, price: float) -> Dict[str, Any]:
        """
        Change the price of an existing non-market order.
        For STP orders, updates auxPrice. For LMT orders, updates lmtPrice.
        """
        trade = self._find_trade_by_order_id(order_id)
        if not trade:
            raise Exception(f"No active order found with orderId={order_id}")

        order = trade.order
        if order.orderType == "MKT":
            raise Exception("Cannot edit price of a market order")

        if order.orderType == "STP":
            order.auxPrice = price
        elif order.orderType == "LMT":
            order.lmtPrice = price
        elif order.orderType == "STP LMT":
            order.auxPrice = price
        else:
            raise Exception(f"Unsupported order type for edit: {order.orderType}")

        self.ib.placeOrder(self.contract, order)
        LOGGER.info(
            f"Edited order {order_id}: type={order.orderType}, newPrice={price}"
        )
        return {
            "orderId": order_id,
            "orderType": order.orderType,
            "newPrice": price,
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

        mkt_order = MarketOrder(action, volume)
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
        tp_order = LimitOrder(action, volume, limit_price)
        sl_order = StopOrder(action, volume, stop_price)

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
        LOGGER.info("Subscribed to IBKR trade events")

    def _teardown_trade_subscription(self):
        """Unsubscribe from IBKR trade events."""
        try:
            self.ib.orderStatusEvent -= self._on_order_status
            self.ib.newOrderEvent -= self._on_new_order
        except Exception:
            pass

    def _on_order_status(self, trade: Trade):
        """Handle order status change events."""
        if trade.contract.conId != self.contract.conId:
            return
        LOGGER.debug(
            f"Order status event: orderId={trade.order.orderId}, "
            f"status={trade.orderStatus.status}"
        )
        # If parent order is cancelled, ensure all its child/bracket orders are also cancelled
        if trade.orderStatus.status == "Cancelled":
            # 1. If it was a parent order, cancel its children
            for t in list(self.ib.openTrades()):
                if t.contract.conId == self.contract.conId and t.order.parentId == trade.order.orderId:
                    LOGGER.info(
                        f"Auto-cancelling child order {t.order.orderId} "
                        f"because parent {trade.order.orderId} was cancelled"
                    )
                    self.ib.cancelOrder(t.order)

            # 2. If it was a child order, cancel sibling orders sharing the same parentId
            parent_id = trade.order.parentId
            if parent_id:
                for t in list(self.ib.openTrades()):
                    if (
                        t.contract.conId == self.contract.conId
                        and t.order.orderId != trade.order.orderId
                        and t.order.parentId == parent_id
                    ):
                        LOGGER.info(
                            f"Auto-cancelling sibling child order {t.order.orderId} "
                            f"because child {trade.order.orderId} was cancelled"
                        )
                        self.ib.cancelOrder(t.order)

            # 3. If it had an ocaGroup, cancel sibling orders in the same OCA group
            oca_group = trade.order.ocaGroup
            if oca_group:
                for t in list(self.ib.openTrades()):
                    if (
                        t.contract.conId == self.contract.conId
                        and t.order.orderId != trade.order.orderId
                        and t.order.ocaGroup == oca_group
                    ):
                        LOGGER.info(
                            f"Auto-cancelling OCA sibling order {t.order.orderId} "
                            f"because order {trade.order.orderId} was cancelled"
                        )
                        self.ib.cancelOrder(t.order)

        if self._on_change_callback:
            self._on_change_callback(self.get_open_orders())

    def _on_new_order(self, trade: Trade):
        """Handle new order events."""
        if trade.contract.conId != self.contract.conId:
            return
        LOGGER.debug(f"New order event: orderId={trade.order.orderId}")
        if self._on_change_callback:
            self._on_change_callback(self.get_open_orders())

    def get_open_orders(self) -> List[Dict[str, Any]]:
        """
        Return all active orders for this contract as a list of dicts.
        Used by frontend to render the order lifecycle panel.
        """
        result = []
        for t in self.ib.openTrades():
            if t.contract.conId != self.contract.conId:
                continue
            if not t.isActive():
                continue
            order = t.order
            price = None
            if order.orderType == "STP":
                price = order.auxPrice
            elif order.orderType == "LMT":
                price = order.lmtPrice
            elif order.orderType == "STP LMT":
                price = order.auxPrice

            result.append(
                {
                    "orderId": order.orderId,
                    "action": order.action,
                    "orderType": order.orderType,
                    "totalQuantity": int(order.totalQuantity),
                    "price": price,
                    "status": t.orderStatus.status,
                    "parentId": order.parentId if order.parentId else None,
                    "ocaGroup": order.ocaGroup if order.ocaGroup else None,
                }
            )
        return result

    def cancel_order(self, order_id: int) -> Dict[str, Any]:
        """Cancel a specific order by orderId."""
        trade = self._find_trade_by_order_id(order_id)
        if not trade:
            raise Exception(f"No active order found with orderId={order_id}")
        self.ib.cancelOrder(trade.order)
        LOGGER.info(f"Cancelled order {order_id}")

        # 1. Explicitly cancel any child orders of this parent
        for t in list(self.ib.openTrades()):
            if t.contract.conId == self.contract.conId and t.order.parentId == order_id:
                LOGGER.info(f"Cancelling child order {t.order.orderId} of parent {order_id}")
                self.ib.cancelOrder(t.order)

        # 2. If this order is a child, explicitly cancel any siblings (sharing same parentId)
        parent_id = trade.order.parentId
        if parent_id:
            for t in list(self.ib.openTrades()):
                if (
                    t.contract.conId == self.contract.conId
                    and t.order.orderId != order_id
                    and t.order.parentId == parent_id
                ):
                    LOGGER.info(
                        f"Cancelling sibling child order {t.order.orderId} sharing parent {parent_id}"
                    )
                    self.ib.cancelOrder(t.order)

        # 3. If this order has an ocaGroup, explicitly cancel any other orders in the same OCA group
        oca_group = trade.order.ocaGroup
        if oca_group:
            for t in list(self.ib.openTrades()):
                if (
                    t.contract.conId == self.contract.conId
                    and t.order.orderId != order_id
                    and t.order.ocaGroup == oca_group
                ):
                    LOGGER.info(
                        f"Cancelling OCA sibling order {t.order.orderId} in group {oca_group}"
                    )
                    self.ib.cancelOrder(t.order)

        return {"orderId": order_id, "status": "cancel_requested"}

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
