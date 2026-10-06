import sys
import unittest
from unittest.mock import MagicMock, patch
from ib_async import Order, Trade, OrderStatus, Contract, LimitOrder, StopOrder

# Import IbkrTrading
from pacavanza.modules.ibkr_trading import IbkrTrading


class TestScaleUpSlTpSync(unittest.TestCase):
    def setUp(self):
        self.mock_ib = MagicMock()
        self.mock_contract = MagicMock(spec=Contract)
        self.mock_contract.conId = 12345
        self.mock_contract.multiplier = "5"
        self.mock_contract.symbol = "MNQ"

        # Patch BaseAvanzaTrading.__init__ and client.reqPositions
        with patch.object(IbkrTrading, "__init__", lambda self, *args, **kwargs: None):
            self.trading = IbkrTrading()
            self.trading.ib = self.mock_ib
            self.trading.contract = self.mock_contract
            self.trading.order_placed_times = {}
            self.trading.perm_id_to_parent_perm_id = {}
            self.trading.perm_id_to_oca_group = {}
            self.trading._pending_entry_fills = []
            self.trading._is_flat = True
            self.trading._last_position = 0
            self.trading._last_avg_cost = 0.0
            self.trading._on_change_callback = None
            self.trading._on_fill_callback = None

        self.trades = []
        self.mock_ib.trades.side_effect = lambda: self.trades
        self.mock_ib.openTrades.side_effect = lambda: [
            t for t in self.trades if t.isActive()
        ]
        self.mock_ib.client.getReqId.side_effect = range(100, 200)

    def _create_trade(
        self,
        order_id,
        action,
        order_type,
        qty,
        price=None,
        aux_price=None,
        parent_id=0,
        oca_group="",
        order_ref="",
        status="Submitted",
    ):
        order = Order()
        order.orderId = order_id
        order.action = action
        order.orderType = order_type
        order.totalQuantity = qty
        if price is not None:
            order.lmtPrice = price
        if aux_price is not None:
            order.auxPrice = aux_price
        order.parentId = parent_id
        order.ocaGroup = oca_group
        order.orderRef = order_ref
        order.transmit = True

        trade = Trade(contract=self.mock_contract, order=order)
        trade.orderStatus = OrderStatus(status=status)
        return trade

    def test_short_bracket_edit_and_scale_up(self):
        """
        User flow reproduction:
        1. Open short position (size 1) via bracket:
           - Parent 59 (SELL STOP @ 100, Filled)
           - Child 60 (BUY LMT @ 90, qty 1, BracketTP)
           - Child 61 (BUY STP @ 110, qty 1, BracketSL)
        2. Position is -1 @ 100.
        3. User edits STP SL price to 115.
           - parentId cleared, ocaGroup preserved, orderRef preserved.
        4. User places scale-up SELL LMT @ 105 (qty 1), orderId=62.
        5. Order 62 fills! Position becomes -2.
        6. _sync_sl_tp_volume:
           - Both TP (60) and SL (61) must be updated to qty=2.
           - TP price stays 90, SL price stays 115.
        7. _process_pending_entry_fills:
           - Detects scale-up, skips Auto-OCA bracket creation.
        """
        # 1. Initial orders
        parent_trade = self._create_trade(
            59, "SELL", "STP", 1, aux_price=100.0, status="Filled"
        )
        tp_trade = self._create_trade(
            60,
            "BUY",
            "LMT",
            1,
            price=90.0,
            parent_id=59,
            oca_group="ibkr_bracket_oca_59",
            order_ref="BracketTP",
        )
        sl_trade = self._create_trade(
            61,
            "BUY",
            "STP",
            1,
            aux_price=110.0,
            parent_id=59,
            oca_group="ibkr_bracket_oca_59",
            order_ref="BracketSL",
        )
        self.trades = [parent_trade, tp_trade, sl_trade]

        # Initial position update (-1 @ 100)
        pos_obj = MagicMock()
        pos_obj.contract = self.mock_contract
        pos_obj.position = -1
        pos_obj.avgCost = 500.0  # 500 / 5 = 100.0
        self.mock_ib.positions.return_value = [pos_obj]

        self.trading._on_position(pos_obj)
        self.assertEqual(self.trading._last_position, -1)
        self.assertEqual(self.trading._last_avg_cost, 100.0)
        self.assertFalse(self.trading._is_flat)

        # 3. User edits SL order 61 in UI to 115.0
        # parentId is preserved to avoid TWS Error 10326 ("OCA group revision is not allowed")
        self.trading.edit_order(61, price=115.0)
        self.assertEqual(sl_trade.order.auxPrice, 115.0)
        self.assertEqual(sl_trade.order.parentId, 59)
        self.assertEqual(sl_trade.order.orderRef, "BracketSL")

        # 4. User places scale-up SELL LMT @ 105 (qty 1), orderId=62
        scaleup_trade = self._create_trade(
            62, "SELL", "LMT", 1, price=105.0, status="Submitted"
        )
        self.trades.append(scaleup_trade)

        # Simulate execution of order 62
        fill_mock = MagicMock()
        fill_mock.execution.price = 105.0
        fill_mock.execution.side = "SLD"
        fill_mock.execution.shares = 1
        scaleup_trade.orderStatus.status = "Filled"

        self.trading._on_exec_details(scaleup_trade, fill_mock)
        self.assertEqual(len(self.trading._pending_entry_fills), 1)
        self.assertEqual(self.trading._pending_entry_fills[0]["price"], 105.0)
        self.assertEqual(self.trading._pending_entry_fills[0]["side"], "sell")

        # 5. Position update arrives: position is now -2 @ avgCost 102.5
        pos_obj.position = -2
        pos_obj.avgCost = 1025.0  # 1025 / 5 = 205 / 2 = 102.5
        self.mock_ib.positions.return_value = [pos_obj]

        with patch.object(self.trading, "place_oca_bracket") as mock_place_oca:
            self.trading._on_position(pos_obj)

            # Verify: NO new Auto-OCA bracket placed!
            mock_place_oca.assert_not_called()

        # 6. Verify volumes updated to 2 for BOTH TP and SL!
        self.assertEqual(tp_trade.order.totalQuantity, 2)
        self.assertEqual(tp_trade.order.lmtPrice, 90.0)  # price unchanged

        self.assertEqual(sl_trade.order.totalQuantity, 2)
        self.assertEqual(sl_trade.order.auxPrice, 115.0)  # price unchanged

        # Pending fills queue must be drained
        self.assertEqual(len(self.trading._pending_entry_fills), 0)
        print("PASS: test_short_bracket_edit_and_scale_up")

    def test_long_scale_up_sync(self):
        """
        Long position scale up:
        1. Long position (size 1) @ 100, TP @ 110, SL @ 90.
        2. Edit TP to 112.
        3. Scale up BUY LMT @ 95 (qty 2) fills, position becomes +3.
        4. Both TP and SL sizes must become 3, prices unchanged, no duplicate OCA.
        """
        parent_trade = self._create_trade(
            70, "BUY", "STP", 1, aux_price=100.0, status="Filled"
        )
        tp_trade = self._create_trade(
            71,
            "SELL",
            "LMT",
            1,
            price=110.0,
            parent_id=70,
            oca_group="ibkr_bracket_oca_70",
            order_ref="BracketTP",
        )
        sl_trade = self._create_trade(
            72,
            "SELL",
            "STP",
            1,
            aux_price=90.0,
            parent_id=70,
            oca_group="ibkr_bracket_oca_70",
            order_ref="BracketSL",
        )
        self.trades = [parent_trade, tp_trade, sl_trade]

        pos_obj = MagicMock()
        pos_obj.contract = self.mock_contract
        pos_obj.position = 1
        pos_obj.avgCost = 500.0  # 100.0
        self.mock_ib.positions.return_value = [pos_obj]

        self.trading._on_position(pos_obj)

        # Edit TP to 112
        self.trading.edit_order(71, price=112.0)
        self.assertEqual(tp_trade.order.parentId, 70)

        # Scale up BUY LMT @ 95 (qty 2)
        scaleup_trade = self._create_trade(
            73, "BUY", "LMT", 2, price=95.0, status="Filled"
        )
        self.trades.append(scaleup_trade)

        fill_mock = MagicMock()
        fill_mock.execution.price = 95.0
        fill_mock.execution.side = "BOT"
        fill_mock.execution.shares = 2
        self.trading._on_exec_details(scaleup_trade, fill_mock)

        pos_obj.position = 3
        pos_obj.avgCost = 1450.0
        self.mock_ib.positions.return_value = [pos_obj]

        with patch.object(self.trading, "place_oca_bracket") as mock_place_oca:
            self.trading._on_position(pos_obj)
            mock_place_oca.assert_not_called()

        self.assertEqual(tp_trade.order.totalQuantity, 3)
        self.assertEqual(tp_trade.order.lmtPrice, 112.0)
        self.assertEqual(sl_trade.order.totalQuantity, 3)
        self.assertEqual(sl_trade.order.auxPrice, 90.0)
        print("PASS: test_long_scale_up_sync")

    def test_open_from_flat_triggers_auto_oca(self):
        """
        Standalone limit order when flat should trigger Auto-OCA bracket.
        """
        entry_trade = self._create_trade(
            80, "BUY", "LMT", 1, price=100.0, status="Filled"
        )
        self.trades = [entry_trade]

        fill_mock = MagicMock()
        fill_mock.execution.price = 100.0
        fill_mock.execution.side = "BOT"
        fill_mock.execution.shares = 1
        self.trading._on_exec_details(entry_trade, fill_mock)

        pos_obj = MagicMock()
        pos_obj.contract = self.mock_contract
        pos_obj.position = 1
        pos_obj.avgCost = 500.0
        self.mock_ib.positions.return_value = [pos_obj]

        with patch.object(self.trading, "place_oca_bracket") as mock_place_oca:
            mock_place_oca.return_value = {"ocaGroup": "test"}
            self.trading._on_position(pos_obj)
            mock_place_oca.assert_called_once()
            args, kwargs = mock_place_oca.call_args
            self.assertEqual(kwargs["action"], "SELL")
            self.assertEqual(kwargs["volume"], 1)
        print("PASS: test_open_from_flat_triggers_auto_oca")


if __name__ == "__main__":
    unittest.main()
