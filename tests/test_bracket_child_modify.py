"""
Unit tests for bracket child order modification and linkage preservation.

Validates that:
1. _place_bracket_stop_entry creates bracket children with parentId linkage
   and WITHOUT IB.oneCancelsAll (avoiding TWS Error 10326).
2. edit_order modifies prices and sets transmit=True while strictly preserving
   the existing parentId and ocaGroup attributes.
"""

import unittest
from unittest.mock import MagicMock, patch
from ib_async import LimitOrder, StopOrder, Trade, Contract, OrderStatus


class MockIbkrTrading:
    """Minimal harness mirroring IbkrTrading methods for test isolation."""

    def __init__(self):
        self.ib = MagicMock()
        self.contract = Contract()
        self.instrument_list = {"omxs306j": {"tick_size": 0.25}}
        self._open_trades = []
        self._placed_orders = []

    def _round_price(self, price: float) -> float:
        tick = 0.25
        return round(round(price / tick) * tick, 4)

    def _place_ib_order(self, order) -> Trade:
        self._placed_orders.append(order)
        trade = Trade(self.contract, order)
        trade.orderStatus = OrderStatus(status="Submitted")
        return trade

    def _get_open_ib_trades(self):
        return [t for t in self._open_trades if t.isActive()]

    def _find_trade_by_order_id(self, order_id: int):
        for t in self._get_open_ib_trades():
            if (
                t.order.orderId == order_id or t.order.permId == order_id
            ) and t.isActive():
                return t
        return None

    def _place_bracket_stop_entry(
        self,
        action: str,
        volume: int,
        stop_price: float,
        sl_price: float,
        tp_price: float,
    ) -> Trade:
        opposite = "SELL" if action == "BUY" else "BUY"

        parent_id = 100
        tp_id = 101
        sl_id = 102

        parent = StopOrder(action, volume, self._round_price(stop_price), tif="DAY")
        parent.orderId = parent_id
        parent.transmit = False

        tp_order = LimitOrder(opposite, volume, self._round_price(tp_price), tif="DAY")
        tp_order.orderId = tp_id
        tp_order.parentId = parent_id
        tp_order.orderRef = "BracketTP"
        tp_order.transmit = False

        sl_order = StopOrder(opposite, volume, self._round_price(sl_price), tif="DAY")
        sl_order.orderId = sl_id
        sl_order.parentId = parent_id
        sl_order.orderRef = "BracketSL"
        sl_order.transmit = True

        parent_trade = self._place_ib_order(parent)
        self._place_ib_order(tp_order)
        self._place_ib_order(sl_order)
        return parent_trade

    def edit_order(self, order_id: int, price=None, quantity=None):
        trade = self._find_trade_by_order_id(order_id)
        if not trade:
            raise Exception(f"No active order found with orderId={order_id}")

        order = trade.order

        if quantity is not None:
            if quantity <= 0:
                raise Exception("Quantity must be greater than 0")
            order.totalQuantity = quantity

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
                raise Exception(f"Unsupported order type: {order.orderType}")

        if getattr(order, "deltaNeutralOrderType", "") == "无":
            order.deltaNeutralOrderType = ""
        if getattr(order, "adjustedOrderType", "") == "无":
            order.adjustedOrderType = ""

        # Preserving parentId and ocaGroup without clearing or rewriting
        order.transmit = True
        self._place_ib_order(order)
        return {
            "orderId": order_id,
            "orderType": order.orderType,
            "newPrice": price,
            "newQuantity": quantity,
            "status": "modified",
        }


class TestBracketChildModify(unittest.TestCase):

    def setUp(self):
        self.trading = MockIbkrTrading()

    def test_place_bracket_stop_entry_attributes(self):
        """Bracket children must have parentId and no conflicting ocaGroup."""
        self.trading._place_bracket_stop_entry(
            action="BUY", volume=1, stop_price=3300.0, sl_price=3280.0, tp_price=3340.0
        )
        orders = self.trading._placed_orders
        self.assertEqual(len(orders), 3)

        parent, tp, sl = orders[0], orders[1], orders[2]

        self.assertEqual(parent.orderId, 100)
        self.assertEqual(parent.transmit, False)
        self.assertEqual(parent.parentId, 0)
        self.assertEqual(getattr(parent, "ocaGroup", ""), "")

        self.assertEqual(tp.orderId, 101)
        self.assertEqual(tp.parentId, 100)
        self.assertEqual(tp.orderRef, "BracketTP")
        self.assertEqual(tp.transmit, False)
        # Crucial: ocaGroup must NOT be populated on bracket placement
        self.assertEqual(getattr(tp, "ocaGroup", ""), "")

        self.assertEqual(sl.orderId, 102)
        self.assertEqual(sl.parentId, 100)
        self.assertEqual(sl.orderRef, "BracketSL")
        self.assertEqual(sl.transmit, True)
        self.assertEqual(getattr(sl, "ocaGroup", ""), "")

    def test_edit_child_when_parent_is_pending(self):
        """Editing child while parent is pending must keep parentId and not set ocaGroup."""
        self.trading._place_bracket_stop_entry(
            action="BUY", volume=1, stop_price=3300.0, sl_price=3280.0, tp_price=3340.0
        )
        parent, tp, sl = self.trading._placed_orders

        # Mock trade objects in active trades
        trade_parent = MagicMock()
        trade_parent.order = parent
        trade_parent.isActive.return_value = True

        trade_tp = MagicMock()
        trade_tp.order = tp
        trade_tp.isActive.return_value = True

        self.trading._open_trades = [trade_parent, trade_tp]

        # Edit TP child
        res = self.trading.edit_order(101, price=3350.0)
        self.assertEqual(res["status"], "modified")
        self.assertEqual(tp.lmtPrice, 3350.0)
        self.assertEqual(tp.transmit, True)
        self.assertEqual(tp.parentId, 100)
        self.assertEqual(getattr(tp, "ocaGroup", ""), "")

    def test_edit_child_when_parent_is_filled(self):
        """Editing child after parent has filled must preserve parentId and not invent ocaGroup."""
        self.trading._place_bracket_stop_entry(
            action="BUY", volume=1, stop_price=3300.0, sl_price=3280.0, tp_price=3340.0
        )
        parent, tp, sl = self.trading._placed_orders

        # Parent is filled (not active)
        trade_parent = MagicMock()
        trade_parent.order = parent
        trade_parent.isActive.return_value = False

        trade_sl = MagicMock()
        trade_sl.order = sl
        trade_sl.isActive.return_value = True

        self.trading._open_trades = [trade_parent, trade_sl]

        # Edit SL child
        res = self.trading.edit_order(102, price=3285.0)
        self.assertEqual(res["status"], "modified")
        self.assertEqual(sl.auxPrice, 3285.0)
        self.assertEqual(sl.transmit, True)
        self.assertEqual(sl.parentId, 100)
        self.assertEqual(getattr(sl, "ocaGroup", ""), "")

    def test_edit_standalone_oca_order(self):
        """Editing standalone OCA order must keep ocaGroup intact."""
        tp_order = LimitOrder("SELL", 1, 3320.0)
        tp_order.orderId = 201
        tp_order.ocaGroup = "ibkr_oca_manual_123456"

        trade_tp = MagicMock()
        trade_tp.order = tp_order
        trade_tp.isActive.return_value = True

        self.trading._open_trades = [trade_tp]

        res = self.trading.edit_order(201, price=3325.0)
        self.assertEqual(res["status"], "modified")
        self.assertEqual(tp_order.lmtPrice, 3325.0)
        self.assertEqual(tp_order.transmit, True)
        self.assertEqual(tp_order.ocaGroup, "ibkr_oca_manual_123456")


if __name__ == "__main__":
    unittest.main()
