"""
Test script for Breakout Stop OCA order placement logic.
Tests validation, order construction, sync protection, and get_open_orders retention.
"""

import logging
from datetime import datetime, timezone
import unittest
from unittest.mock import MagicMock, patch

from ib_async import Future
from pacavanza.modules.ibkr_trading import IbkrTrading


class TestBostpOcaLogic(unittest.TestCase):
    def setUp(self):
        self.ib = MagicMock()
        self.contract = MagicMock(spec=Future)
        self.contract.conId = 12345
        self.logger = logging.getLogger("test")

        self.trading = IbkrTrading(
            ib=self.ib,
            contract=self.contract,
            recent_bars={},
            instrument_list={"omxs306j": {"name": "OMXS306J", "tick_size": 0.25}},
            metadata={"omxs306j": {}},
            logger=self.logger,
        )

    def test_bostp_validation_position_restriction(self):
        # 1. Non-zero position rejection
        with patch.object(self.trading, "_get_signed_position", return_value=1):
            with self.assertRaises(Exception) as ctx:
                self.trading.place_breakout_stop_oca(
                    volume=1, high_price=2600.0, low_price=2580.0
                )
            self.assertIn("only allowed when flat", str(ctx.exception).lower())

    def test_bostp_validation_price_ordering(self):
        # 2. High price <= low price rejection
        with patch.object(self.trading, "_get_signed_position", return_value=0):
            with self.assertRaises(Exception) as ctx:
                self.trading.place_breakout_stop_oca(
                    volume=1, high_price=2580.0, low_price=2600.0
                )
            self.assertIn("strictly greater", str(ctx.exception).lower())

            with self.assertRaises(Exception) as ctx:
                self.trading.place_breakout_stop_oca(
                    volume=1, high_price=2580.0, low_price=2580.0
                )
            self.assertIn("strictly greater", str(ctx.exception).lower())

    def test_bostp_order_formulation(self):
        # 3. Order formulation
        with patch.object(self.trading, "_get_signed_position", return_value=0):
            self.trading.ib.client.getReqId.side_effect = [1001, 1002]
            res = self.trading.place_breakout_stop_oca(
                volume=2, high_price=2600.25, low_price=2580.50
            )

            self.assertEqual(res["volume"], 2)
            self.assertEqual(res["longPrice"], 2600.25)
            self.assertEqual(res["shortPrice"], 2580.50)
            self.assertEqual(self.trading.ib.placeOrder.call_count, 2)

            call1 = self.trading.ib.placeOrder.call_args_list[0]
            call2 = self.trading.ib.placeOrder.call_args_list[1]
            order_buy = call1[0][1]
            order_sell = call2[0][1]

            self.assertEqual(order_buy.action, "BUY")
            self.assertEqual(order_buy.orderType, "STP")
            self.assertEqual(order_buy.auxPrice, 2600.25)
            self.assertEqual(order_buy.totalQuantity, 2)
            self.assertEqual(order_buy.ocaType, 1)
            self.assertEqual(order_buy.orderRef, "BreakoutStop")

            self.assertEqual(order_sell.action, "SELL")
            self.assertEqual(order_sell.orderType, "STP")
            self.assertEqual(order_sell.auxPrice, 2580.50)
            self.assertEqual(order_sell.totalQuantity, 2)
            self.assertEqual(order_sell.ocaType, 1)
            self.assertEqual(order_sell.orderRef, "BreakoutStop")

            self.assertTrue(order_buy.ocaGroup.startswith("ibkr_oca_bostp_"))
            self.assertEqual(order_buy.ocaGroup, order_sell.ocaGroup)

    def test_sync_protection(self):
        # 1. When flat: BreakoutStop entry orders must NOT be cancelled by sync
        trade_bostp = MagicMock()
        trade_bostp.contract = self.trading.contract
        trade_bostp.order = MagicMock(
            orderId=2001,
            action="BUY",
            orderType="STP",
            totalQuantity=1,
            orderRef="BreakoutStop",
            ocaGroup="ibkr_oca_bostp_123",
            parentId=0,
            parentPermId=0,
        )
        trade_bostp.isDone.return_value = False
        trade_bostp.isActive.return_value = True

        trade_orphan = MagicMock()
        trade_orphan.contract = self.trading.contract
        trade_orphan.order = MagicMock(
            orderId=2002,
            action="BUY",
            orderType="LMT",
            totalQuantity=1,
            orderRef="",
            ocaGroup="",
            parentId=0,
            parentPermId=0,
        )
        trade_orphan.isDone.return_value = False
        trade_orphan.isActive.return_value = True

        self.trading.ib.trades.return_value = [trade_bostp, trade_orphan]
        self.trading.ib.openTrades.return_value = [trade_bostp, trade_orphan]

        with patch.object(self.trading, "_get_signed_position", return_value=0):
            self.trading._sync_sl_tp_volume()
            cancelled_ids = [
                call[0][0].orderId for call in self.trading.ib.cancelOrder.call_args_list
            ]
            self.assertIn(2002, cancelled_ids)
            self.assertNotIn(2001, cancelled_ids)

        # 2. When position active (pos=1): residual uncancelled BreakoutStop should be cancelled
        self.trading.ib.cancelOrder.reset_mock()
        trade_residual = MagicMock()
        trade_residual.contract = self.trading.contract
        trade_residual.order = MagicMock(
            orderId=2003,
            action="SELL",
            orderType="STP",
            totalQuantity=1,
            orderRef="BreakoutStop",
            ocaGroup="ibkr_oca_bostp_123",
            parentId=0,
            parentPermId=0,
        )
        trade_residual.isDone.return_value = False
        trade_residual.isActive.return_value = True
        self.trading.ib.trades.return_value = [trade_residual]
        self.trading.ib.openTrades.return_value = [trade_residual]

        with patch.object(self.trading, "_get_signed_position", return_value=1):
            self.trading._sync_sl_tp_volume()
            cancelled_ids = [
                call[0][0].orderId for call in self.trading.ib.cancelOrder.call_args_list
            ]
            self.assertIn(2003, cancelled_ids)

    def test_get_open_orders_omitted_when_all_done(self):
        # When all trades in group are done (Filled + Cancelled), group is omitted from open orders
        trade_filled = MagicMock()
        trade_filled.contract = self.trading.contract
        trade_filled.order = MagicMock(
            permId=3001,
            orderId=3001,
            parentId=0,
            parentPermId=0,
            action="BUY",
            orderType="STP",
            totalQuantity=1,
            auxPrice=2600.0,
            orderRef="BreakoutStop",
            ocaGroup="ibkr_oca_bostp_999",
        )
        trade_filled.orderStatus.status = "Filled"
        trade_filled.isDone.return_value = True
        trade_filled.log = [MagicMock(time=datetime.now(timezone.utc))]

        trade_cancelled = MagicMock()
        trade_cancelled.contract = self.trading.contract
        trade_cancelled.order = MagicMock(
            permId=3002,
            orderId=3002,
            parentId=0,
            parentPermId=0,
            action="SELL",
            orderType="STP",
            totalQuantity=1,
            auxPrice=2580.0,
            orderRef="BreakoutStop",
            ocaGroup="ibkr_oca_bostp_999",
        )
        trade_cancelled.orderStatus.status = "Cancelled"
        trade_cancelled.isDone.return_value = True
        trade_cancelled.log = [MagicMock(time=datetime.now(timezone.utc))]

        self.trading.ib.trades.return_value = [trade_filled, trade_cancelled]

        with patch.object(self.trading, "_get_signed_position", return_value=1):
            orders = self.trading.get_open_orders()
            self.assertEqual(len(orders), 0)

        with patch.object(self.trading, "_get_signed_position", return_value=0):
            orders = self.trading.get_open_orders()
            self.assertEqual(len(orders), 0)

    def test_get_open_orders_zero_permid(self):
        t1 = MagicMock()
        t1.contract = self.trading.contract
        t1.order = MagicMock(
            permId=0,
            orderId=1001,
            parentId=0,
            parentPermId=0,
            action="BUY",
            orderType="STP",
            totalQuantity=1,
            auxPrice=2600.0,
            orderRef="BreakoutStop",
            ocaGroup="ibkr_oca_bostp_999",
        )
        t1.orderStatus.status = "PreSubmitted"
        t1.isDone.return_value = False
        t1.log = [MagicMock(time=datetime.now(timezone.utc))]

        t2 = MagicMock()
        t2.contract = self.trading.contract
        t2.order = MagicMock(
            permId=0,
            orderId=1002,
            parentId=0,
            parentPermId=0,
            action="SELL",
            orderType="STP",
            totalQuantity=1,
            auxPrice=2580.0,
            orderRef="BreakoutStop",
            ocaGroup="ibkr_oca_bostp_999",
        )
        t2.orderStatus.status = "PreSubmitted"
        t2.isDone.return_value = False
        t2.log = [MagicMock(time=datetime.now(timezone.utc))]

        self.trading.ib.trades.return_value = [t1, t2]
        with patch.object(self.trading, "_get_signed_position", return_value=0):
            orders = self.trading.get_open_orders()
            self.assertEqual(len(orders), 2)
            self.assertEqual(orders[0]["orderId"], 1001)
            self.assertEqual(orders[0]["ocaGroup"], "ibkr_oca_bostp_999")
            self.assertEqual(orders[1]["orderId"], 1002)
            self.assertEqual(orders[1]["ocaGroup"], "ibkr_oca_bostp_999")


if __name__ == "__main__":
    unittest.main()
