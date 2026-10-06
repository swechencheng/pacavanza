import sys
import unittest
from unittest.mock import MagicMock, patch
from ib_async import Order, Trade, OrderStatus, Contract, LimitOrder, StopOrder

from pacavanza.modules.ibkr_trading import IbkrTrading


class TestBostpBracketGeneration(unittest.TestCase):
    def setUp(self):
        self.mock_ib = MagicMock()
        self.mock_contract = MagicMock(spec=Contract)
        self.mock_contract.conId = 12345
        self.mock_contract.multiplier = "5"
        self.mock_contract.symbol = "MNQ"

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
            self.trading._bostp_info = {}
            self.trading._on_change_callback = None
            self.trading._on_fill_callback = None

        self.trades = []
        self.mock_ib.trades.side_effect = lambda: self.trades
        self.mock_ib.openTrades.side_effect = lambda: [
            t for t in self.trades if t.isActive()
        ]
        self.mock_ib.client.getReqId.side_effect = range(100, 300)

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

    def test_long_bostp_fill_generates_bracket(self):
        """
        Long BO-STP fills:
        - Long BUY STP @ 2600.0 fills
        - Short SELL STP @ 2580.0 is cancelled
        - Position becomes +1 @ 2600.0
        - Must place OCA bracket:
          * Action: SELL
          * SL Stop price: 2580.0 (opposite BO price)
          * TP Limit price: 2600 + 2 * 20 = 2640.0 (2:1 ratio)
        """
        # Place BO-STP OCA
        oca_group = "ibkr_oca_bostp_123"
        long_trade = self._create_trade(
            101,
            "BUY",
            "STP",
            1,
            aux_price=2600.0,
            oca_group=oca_group,
            order_ref="BreakoutStop",
            status="Submitted",
        )
        short_trade = self._create_trade(
            102,
            "SELL",
            "STP",
            1,
            aux_price=2580.0,
            oca_group=oca_group,
            order_ref="BreakoutStop",
            status="Submitted",
        )
        self.trades = [long_trade, short_trade]
        self.trading._bostp_info[oca_group] = {
            "high_price": 2600.0,
            "low_price": 2580.0,
            "volume": 1,
            "longOrderId": 101,
            "shortOrderId": 102,
        }

        # Long order fills @ 2600.0
        long_trade.orderStatus.status = "Filled"
        short_trade.orderStatus.status = "Cancelled"

        fill_mock = MagicMock()
        fill_mock.execution.price = 2600.0
        fill_mock.execution.side = "BOT"
        fill_mock.execution.shares = 1

        self.trading._on_exec_details(long_trade, fill_mock)
        self.assertEqual(len(self.trading._pending_entry_fills), 1)
        self.assertEqual(
            self.trading._pending_entry_fills[0]["orderRef"], "BreakoutStop"
        )

        # Position event arrives: pos = 1 @ 2600
        pos_obj = MagicMock()
        pos_obj.contract = self.mock_contract
        pos_obj.position = 1
        pos_obj.avgCost = 2600.0 * 5
        self.mock_ib.positions.return_value = [pos_obj]

        with patch.object(self.trading, "place_oca_bracket") as mock_place_oca:
            mock_place_oca.return_value = {"ocaGroup": "ibkr_oca_manual_999"}
            self.trading._on_position(pos_obj)

            mock_place_oca.assert_called_once()
            _, kwargs = mock_place_oca.call_args
            self.assertEqual(kwargs["action"], "SELL")
            self.assertEqual(kwargs["volume"], 1)
            self.assertEqual(kwargs["stop_price"], 2580.0)  # SL is short BO price
            self.assertEqual(
                kwargs["limit_price"], 2640.0
            )  # TP is 2:1 ratio (2600 + 40)

        print("PASS: test_long_bostp_fill_generates_bracket")

    def test_short_bostp_fill_generates_bracket(self):
        """
        Short BO-STP fills:
        - Long BUY STP @ 2600.0 cancelled
        - Short SELL STP @ 2580.0 fills
        - Position becomes -2 @ 2580.0
        - Must place OCA bracket:
          * Action: BUY
          * SL Stop price: 2600.0 (opposite BO price)
          * TP Limit price: 2580 - 2 * 20 = 2540.0 (2:1 ratio)
        """
        oca_group = "ibkr_oca_bostp_456"
        long_trade = self._create_trade(
            201,
            "BUY",
            "STP",
            2,
            aux_price=2600.0,
            oca_group=oca_group,
            order_ref="BreakoutStop",
            status="Submitted",
        )
        short_trade = self._create_trade(
            202,
            "SELL",
            "STP",
            2,
            aux_price=2580.0,
            oca_group=oca_group,
            order_ref="BreakoutStop",
            status="Submitted",
        )
        self.trades = [long_trade, short_trade]
        self.trading._bostp_info[oca_group] = {
            "high_price": 2600.0,
            "low_price": 2580.0,
            "volume": 2,
            "longOrderId": 201,
            "shortOrderId": 202,
        }

        # Short order fills @ 2580.0
        short_trade.orderStatus.status = "Filled"
        long_trade.orderStatus.status = "Cancelled"

        fill_mock = MagicMock()
        fill_mock.execution.price = 2580.0
        fill_mock.execution.side = "SLD"
        fill_mock.execution.shares = 2

        self.trading._on_exec_details(short_trade, fill_mock)
        self.assertEqual(len(self.trading._pending_entry_fills), 1)

        pos_obj = MagicMock()
        pos_obj.contract = self.mock_contract
        pos_obj.position = -2
        pos_obj.avgCost = 2580.0 * 5
        self.mock_ib.positions.return_value = [pos_obj]

        with patch.object(self.trading, "place_oca_bracket") as mock_place_oca:
            mock_place_oca.return_value = {"ocaGroup": "ibkr_oca_manual_999"}
            self.trading._on_position(pos_obj)

            mock_place_oca.assert_called_once()
            _, kwargs = mock_place_oca.call_args
            self.assertEqual(kwargs["action"], "BUY")
            self.assertEqual(kwargs["volume"], 2)
            self.assertEqual(kwargs["stop_price"], 2600.0)  # SL is long BO price
            self.assertEqual(
                kwargs["limit_price"], 2540.0
            )  # TP is 2:1 ratio (2580 - 40)

        print("PASS: test_short_bostp_fill_generates_bracket")

    def test_bostp_edit_price_before_fill(self):
        """
        Editing BO-STP price updates _bostp_info and uses updated SL/TP:
        - Initial: Long 2600, Short 2580
        - User edits Short to 2585
        - Long fills @ 2600
        - SL = 2585, Range = 15, TP = 2600 + 30 = 2630
        """
        oca_group = "ibkr_oca_bostp_789"
        long_trade = self._create_trade(
            301,
            "BUY",
            "STP",
            1,
            aux_price=2600.0,
            oca_group=oca_group,
            order_ref="BreakoutStop",
            status="Submitted",
        )
        short_trade = self._create_trade(
            302,
            "SELL",
            "STP",
            1,
            aux_price=2580.0,
            oca_group=oca_group,
            order_ref="BreakoutStop",
            status="Submitted",
        )
        self.trades = [long_trade, short_trade]
        self.trading._bostp_info[oca_group] = {
            "high_price": 2600.0,
            "low_price": 2580.0,
            "volume": 1,
            "longOrderId": 301,
            "shortOrderId": 302,
        }

        # User edits short order to 2585.0
        self.trading.edit_order(302, price=2585.0)
        self.assertEqual(self.trading._bostp_info[oca_group]["low_price"], 2585.0)

        long_trade.orderStatus.status = "Filled"
        short_trade.orderStatus.status = "Cancelled"

        fill_mock = MagicMock()
        fill_mock.execution.price = 2600.0
        fill_mock.execution.side = "BOT"
        fill_mock.execution.shares = 1

        self.trading._on_exec_details(long_trade, fill_mock)

        pos_obj = MagicMock()
        pos_obj.contract = self.mock_contract
        pos_obj.position = 1
        pos_obj.avgCost = 2600.0 * 5
        self.mock_ib.positions.return_value = [pos_obj]

        with patch.object(self.trading, "place_oca_bracket") as mock_place_oca:
            mock_place_oca.return_value = {"ocaGroup": "ibkr_oca_manual_999"}
            self.trading._on_position(pos_obj)

            mock_place_oca.assert_called_once()
            _, kwargs = mock_place_oca.call_args
            self.assertEqual(kwargs["stop_price"], 2585.0)  # Updated low price
            self.assertEqual(kwargs["limit_price"], 2630.0)  # 2600 + 2 * 15

        print("PASS: test_bostp_edit_price_before_fill")


if __name__ == "__main__":
    unittest.main()
