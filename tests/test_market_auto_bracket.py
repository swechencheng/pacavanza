import unittest
from unittest.mock import MagicMock, patch
from pacavanza.modules.ibkr_trading import IbkrTrading, AUTO_OCA_OFFSET
from ib_async import Trade, Order, OrderStatus, Contract


class TestMarketAutoBracket(unittest.TestCase):
    def setUp(self):
        with patch.object(IbkrTrading, "__init__", lambda self, *args, **kwargs: None):
            self.trading = IbkrTrading()
            self.trading.ib = MagicMock()
            self.trading.contract = MagicMock()
            self.trading.contract.conId = 12345
            self.trading.contract.multiplier = "5"
            self.trading.order_placed_times = {}
            self.trading.perm_id_to_parent_perm_id = {}
            self.trading.perm_id_to_oca_group = {}
            self.trading._pending_entry_fills = []
            self.trading._last_position = 0
            self.trading._last_avg_cost = 0.0
            self.trading._is_flat = True
            self.trading._on_change_callback = None
            self.trading._on_fill_callback = None
            self.trading._bostp_info = {}

            self.mock_contract = MagicMock()
            self.mock_contract.conId = 12345

            self.trades = []
            self.trading.ib.trades.side_effect = lambda: self.trades
            self.trading._get_open_ib_trades = lambda: self.trades

    def _create_trade(
        self,
        order_id,
        action,
        order_type,
        quantity,
        price=None,
        aux_price=None,
        parent_id=0,
        oca_group="",
        order_ref="",
        status="Submitted",
    ):
        trade = MagicMock(spec=Trade)
        trade.contract = self.mock_contract
        order = MagicMock(spec=Order)
        order.orderId = order_id
        order.permId = order_id * 1000
        order.action = action
        order.orderType = order_type
        order.totalQuantity = quantity
        order.lmtPrice = price
        order.auxPrice = aux_price
        order.parentId = parent_id
        order.parentPermId = 0
        order.ocaGroup = oca_group
        order.orderRef = order_ref
        trade.order = order

        order_status = MagicMock(spec=OrderStatus)
        order_status.status = status
        trade.orderStatus = order_status
        trade.isDone.return_value = status in ("Filled", "Cancelled")
        trade.isActive.return_value = status not in ("Filled", "Cancelled")
        trade.log = []
        return trade

    def test_market_buy_when_position_event_fires_before_exec_details(self):
        """
        Simulate the exact race condition seen in production:
        1. Market BUY order fills.
        2. positionEvent arrives FIRST with position=1.
        3. Auto-OCA bracket is placed immediately using current_avg_cost.
        4. execDetailsEvent arrives SECOND and does not duplicate the bracket.
        """
        entry_trade = self._create_trade(100, "BUY", "MKT", 1, status="Filled")
        self.trades = [entry_trade]

        pos_obj = MagicMock()
        pos_obj.contract = self.mock_contract
        pos_obj.position = 1
        pos_obj.avgCost = 500.0  # multiplier=5 -> 100.0
        self.trading.ib.positions.return_value = [pos_obj]

        with patch.object(self.trading, "place_oca_bracket") as mock_place_oca:
            mock_place_oca.return_value = {"ocaGroup": "test_oca"}

            # Position event fires first
            self.trading._on_position(pos_obj)

            # Auto-OCA bracket placed immediately on position open
            self.assertEqual(mock_place_oca.call_count, 1)
            args, kwargs = mock_place_oca.call_args
            self.assertEqual(kwargs["action"], "SELL")
            self.assertEqual(kwargs["volume"], 1)
            self.assertEqual(kwargs["limit_price"], 100.0 + AUTO_OCA_OFFSET)
            self.assertEqual(kwargs["stop_price"], 100.0 - AUTO_OCA_OFFSET)

            # 2. When execDetailsEvent arrives, it should not duplicate
            # (assuming bracket trade is now active)
            tp_mock = self._create_trade(
                101, "SELL", "LMT", 1, price=130.0, oca_group="test_oca"
            )
            sl_mock = self._create_trade(
                102, "SELL", "STP", 1, aux_price=70.0, oca_group="test_oca"
            )
            self.trades.extend([tp_mock, sl_mock])

            fill_mock = MagicMock()
            fill_mock.execution.price = 100.0
            fill_mock.execution.side = "BOT"
            fill_mock.execution.shares = 1
            self.trading._on_exec_details(entry_trade, fill_mock)

            self.assertEqual(mock_place_oca.call_count, 1)

    def test_market_buy_when_exec_details_fires_before_position_event(self):
        """
        Simulate execDetailsEvent arriving FIRST (before ib.positions is updated):
        1. execDetails arrives, queues fill.
        2. positionEvent arrives, places Auto-OCA bracket.
        """
        entry_trade = self._create_trade(105, "BUY", "MKT", 1, status="Filled")
        self.trades = [entry_trade]

        # Initially flat in ib.positions
        self.trading.ib.positions.return_value = []

        with patch.object(self.trading, "place_oca_bracket") as mock_place_oca:
            mock_place_oca.return_value = {"ocaGroup": "test_oca"}

            fill_mock = MagicMock()
            fill_mock.execution.price = 100.0
            fill_mock.execution.side = "BOT"
            fill_mock.execution.shares = 1
            self.trading._on_exec_details(entry_trade, fill_mock)

            # Still 0 because position is not yet updated
            self.assertEqual(mock_place_oca.call_count, 0)

            # Now position event fires
            pos_obj = MagicMock()
            pos_obj.contract = self.mock_contract
            pos_obj.position = 1
            pos_obj.avgCost = 500.0
            self.trading.ib.positions.return_value = [pos_obj]

            self.trading._on_position(pos_obj)
            mock_place_oca.assert_called_once()
            args, kwargs = mock_place_oca.call_args
            self.assertEqual(kwargs["action"], "SELL")
            self.assertEqual(kwargs["volume"], 1)
            self.assertEqual(kwargs["limit_price"], 100.0 + AUTO_OCA_OFFSET)
            self.assertEqual(kwargs["stop_price"], 100.0 - AUTO_OCA_OFFSET)

    def test_market_sell_when_position_event_fires_before_exec_details(self):
        """
        Simulate market SELL (short entry from flat):
        1. positionEvent arrives FIRST with position=-1.
        2. Auto-OCA bracket is placed protecting the short position.
        """
        entry_trade = self._create_trade(101, "SELL", "MKT", 1, status="Filled")
        self.trades = [entry_trade]

        pos_obj = MagicMock()
        pos_obj.contract = self.mock_contract
        pos_obj.position = -1
        pos_obj.avgCost = 500.0  # multiplier=5 -> 100.0
        self.trading.ib.positions.return_value = [pos_obj]

        with patch.object(self.trading, "place_oca_bracket") as mock_place_oca:
            mock_place_oca.return_value = {"ocaGroup": "test_oca"}

            self.trading._on_position(pos_obj)

            mock_place_oca.assert_called_once()
            args, kwargs = mock_place_oca.call_args
            self.assertEqual(kwargs["action"], "BUY")
            self.assertEqual(kwargs["volume"], 1)
            self.assertEqual(kwargs["limit_price"], 100.0 - AUTO_OCA_OFFSET)
            self.assertEqual(kwargs["stop_price"], 100.0 + AUTO_OCA_OFFSET)

    def test_done_bostp_group_excluded_from_open_orders(self):
        """
        When both orders in a BreakoutStop OCA group are done (e.g. one Filled, one Cancelled),
        get_open_orders() MUST NOT return them.
        """
        bo_long = self._create_trade(
            201,
            "BUY",
            "STP",
            1,
            aux_price=2600.0,
            oca_group="ibkr_oca_bostp_1",
            order_ref="BreakoutStop",
            status="Filled",
        )
        bo_short = self._create_trade(
            202,
            "SELL",
            "STP",
            1,
            aux_price=2580.0,
            oca_group="ibkr_oca_bostp_1",
            order_ref="BreakoutStop",
            status="Cancelled",
        )
        self.trades = [bo_long, bo_short]

        pos_obj = MagicMock()
        pos_obj.contract = self.mock_contract
        pos_obj.position = 1
        pos_obj.avgCost = 500.0
        self.trading.ib.positions.return_value = [pos_obj]

        orders = self.trading.get_open_orders()
        # The done BO-STP orders must NOT be returned!
        self.assertEqual(len(orders), 0)


if __name__ == "__main__":
    unittest.main()
