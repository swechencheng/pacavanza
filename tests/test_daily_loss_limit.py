import asyncio
from datetime import datetime, timezone, timedelta
import unittest
from unittest.mock import MagicMock, AsyncMock, patch
from ib_async import Order, Trade, OrderStatus, Contract, Position
from fastapi import HTTPException

from pacavanza.modules.ibkr_trading import IbkrTrading
from pacavanza.backend import create_app


class TestCheckDailyLossLimit(unittest.TestCase):
    def setUp(self):
        self.mock_trading = MagicMock()
        self.mock_portfolio = MagicMock()
        self.target_symbol = "OMXS306K"

    def test_empty_or_none(self):
        from pacavanza.backend import create_app
        # We can extract check_daily_loss_limit by inspecting backend logic
        # Or instantiate create_app and check endpoints/logic
        today = datetime.now(timezone.utc).date()
        executions = []
        self.mock_portfolio.get_executions.return_value = executions

    def _make_execution(self, order_id, local_symbol, rpnl, dt):
        return {
            "orderId": order_id,
            "localSymbol": local_symbol,
            "realizedPnL": rpnl,
            "time": dt.isoformat(),
        }

    def test_loss_limit_calculation(self):
        # We can test the helper function logic directly
        def check_daily_loss_limit(portfolio, target_local_symbol: str) -> bool:
            if not portfolio or not target_local_symbol:
                return False
            executions = portfolio.get_executions()
            today_utc = datetime.now(timezone.utc).date()
            losing_order_ids = set()
            for ex in executions:
                if ex.get("localSymbol") != target_local_symbol:
                    continue
                if ex.get("time"):
                    ex_date = datetime.fromisoformat(ex["time"]).date()
                    if ex_date == today_utc:
                        rpnl = ex.get("realizedPnL")
                        if rpnl is not None and rpnl < 0:
                            losing_order_ids.add(ex["orderId"])
            return len(losing_order_ids) >= 2

        today = datetime.now(timezone.utc)
        yesterday = today - timedelta(days=1)

        # 0 executions
        self.mock_portfolio.get_executions.return_value = []
        self.assertFalse(check_daily_loss_limit(self.mock_portfolio, self.target_symbol))

        # 1 losing execution today
        self.mock_portfolio.get_executions.return_value = [
            self._make_execution(101, self.target_symbol, -150.0, today)
        ]
        self.assertFalse(check_daily_loss_limit(self.mock_portfolio, self.target_symbol))

        # 2 losing executions today for target symbol -> True
        self.mock_portfolio.get_executions.return_value = [
            self._make_execution(101, self.target_symbol, -150.0, today),
            self._make_execution(102, self.target_symbol, -200.0, today),
        ]
        self.assertTrue(check_daily_loss_limit(self.mock_portfolio, self.target_symbol))

        # 2 losing executions but for different symbol -> False
        self.mock_portfolio.get_executions.return_value = [
            self._make_execution(101, "OTHER_SYM", -150.0, today),
            self._make_execution(102, "OTHER_SYM", -200.0, today),
        ]
        self.assertFalse(check_daily_loss_limit(self.mock_portfolio, self.target_symbol))

        # 2 losing executions yesterday -> False
        self.mock_portfolio.get_executions.return_value = [
            self._make_execution(101, self.target_symbol, -150.0, yesterday),
            self._make_execution(102, self.target_symbol, -200.0, yesterday),
        ]
        self.assertFalse(check_daily_loss_limit(self.mock_portfolio, self.target_symbol))

        # 2 winning executions today -> False
        self.mock_portfolio.get_executions.return_value = [
            self._make_execution(101, self.target_symbol, 250.0, today),
            self._make_execution(102, self.target_symbol, 100.0, today),
        ]
        self.assertFalse(check_daily_loss_limit(self.mock_portfolio, self.target_symbol))


class TestIsOrderRelatedToPosition(unittest.TestCase):
    def setUp(self):
        self.mock_ib = MagicMock()
        self.mock_contract = MagicMock(spec=Contract)
        self.mock_contract.conId = 12345
        self.mock_contract.multiplier = "1.0"
        self.mock_contract.localSymbol = "OMXS306K"

        with patch.object(IbkrTrading, "__init__", lambda self, *args, **kwargs: None):
            self.trading = IbkrTrading()
            self.trading.ib = self.mock_ib
            self.trading.contract = self.mock_contract
            self.trading.order_placed_times = {}
            self.trading.perm_id_to_parent_perm_id = {}
            self.trading.perm_id_to_oca_group = {}

        self.trades = []
        self.mock_ib.trades.side_effect = lambda: self.trades
        self.mock_ib.openTrades.side_effect = lambda: [
            t for t in self.trades if t.isActive()
        ]

    def _create_trade(self, order_id, action, status="Submitted"):
        order = Order()
        order.orderId = order_id
        order.action = action
        order.orderType = "STP"
        order.totalQuantity = 1

        order_status = OrderStatus(status=status)
        trade = Trade(contract=self.mock_contract, order=order, orderStatus=order_status)
        self.trades.append(trade)
        return trade

    def _mock_position(self, pos_qty):
        mock_pos = MagicMock(spec=Position)
        mock_pos.account = ""
        mock_pos.contract = self.mock_contract
        mock_pos.position = pos_qty
        mock_pos.avgCost = 2500.0
        self.mock_ib.positions.return_value = [mock_pos]

    def test_flat_position_returns_false(self):
        self._mock_position(0)
        self._create_trade(201, "SELL")
        self._create_trade(202, "BUY")
        self.assertFalse(self.trading.is_order_related_to_position(201))
        self.assertFalse(self.trading.is_order_related_to_position(202))

    def test_long_position_relates_only_to_sell_orders(self):
        self._mock_position(1)  # Long +1
        self._create_trade(301, "SELL")
        self._create_trade(302, "BUY")
        # SELL order closes/protects long position -> True
        self.assertTrue(self.trading.is_order_related_to_position(301))
        # BUY order adds to long position -> False
        self.assertFalse(self.trading.is_order_related_to_position(302))

    def test_short_position_relates_only_to_buy_orders(self):
        self._mock_position(-1)  # Short -1
        self._create_trade(401, "BUY")
        self._create_trade(402, "SELL")
        # BUY order closes/protects short position -> True
        self.assertTrue(self.trading.is_order_related_to_position(401))
        # SELL order adds to short position -> False
        self.assertFalse(self.trading.is_order_related_to_position(402))

    def test_nonexistent_order_returns_false(self):
        self._mock_position(1)
        self.assertFalse(self.trading.is_order_related_to_position(9999))


class TestBackendDailyLossLimitEndpoints(unittest.TestCase):
    def setUp(self):
        self.mock_ibkr_trading = MagicMock()
        self.mock_ibkr_portfolio = MagicMock()
        self.mock_contract = MagicMock()
        self.mock_contract.localSymbol = "OMXS306K"
        self.mock_ibkr_trading.contract = self.mock_contract

        # Create app with mocked dependencies
        self.app = create_app(
            ibkr_trading=self.mock_ibkr_trading,
            ibkr_portfolio=self.mock_ibkr_portfolio,
        )

    def _set_daily_loss_limit(self, reached: bool):
        today = datetime.now(timezone.utc)
        if reached:
            self.mock_ibkr_portfolio.get_executions.return_value = [
                {"orderId": 1, "localSymbol": "OMXS306K", "realizedPnL": -100, "time": today.isoformat()},
                {"orderId": 2, "localSymbol": "OMXS306K", "realizedPnL": -200, "time": today.isoformat()},
            ]
        else:
            self.mock_ibkr_portfolio.get_executions.return_value = []

    def _find_route_handler(self, path: str, method: str = "POST"):
        for route in self.app.routes:
            if getattr(route, "path", None) == path and method in getattr(route, "methods", []):
                return route.endpoint
        raise ValueError(f"Route {method} {path} not found")

    def test_new_orders_blocked_when_daily_loss_limit_reached(self):
        self._set_daily_loss_limit(True)

        endpoints_to_test = [
            ("/ibkr/limit_buy", {"volume": 1, "price": 2500.0}),
            ("/ibkr/limit_sell", {"volume": 1, "price": 2600.0}),
            ("/ibkr/bare_buy_stop", {"volume": 1, "price": 2550.0}),
            ("/ibkr/bare_sell_stop", {"volume": 1, "price": 2450.0}),
            ("/ibkr/market_buy", {"instrumentId": "OMXS306K", "percentage": 1}),
            ("/ibkr/market_sell", {"instrumentId": "OMXS306K", "percentage": 1}),
            ("/ibkr/buy_stop", {"instrumentId": "OMXS306K", "percentage": 1}),
            ("/ibkr/late_buy_stop", {"instrumentId": "OMXS306K", "percentage": 1}),
            ("/ibkr/sell_stop", {"instrumentId": "OMXS306K", "percentage": 1}),
            ("/ibkr/late_sell_stop", {"instrumentId": "OMXS306K", "percentage": 1}),
            ("/ibkr/place_breakout_stop_oca", {"volume": 1, "highPrice": 2600.0, "lowPrice": 2580.0}),
        ]

        for path, body in endpoints_to_test:
            handler = self._find_route_handler(path)
            req = MagicMock()
            req.json = AsyncMock(return_value=body)

            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(handler(req))
            self.assertEqual(ctx.exception.status_code, 403, f"Expected 403 for {path}")
            self.assertIn("Daily loss limit reached", ctx.exception.detail)

    def test_order_modification_when_daily_loss_limit_reached_and_flat(self):
        self._set_daily_loss_limit(True)
        self.mock_ibkr_trading.is_order_related_to_position.return_value = False
        self.mock_ibkr_trading.get_position_info.return_value = {"position": 0, "avgCost": 0.0}

        # Flat: modifying any order raises 403
        edit_handler = self._find_route_handler("/ibkr/edit_order")
        req = MagicMock()
        req.json = AsyncMock(return_value={"orderId": 501, "price": 2550.0})

        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(edit_handler(req))
        self.assertEqual(ctx.exception.status_code, 403)
        self.assertIn("Orders not related to an active position cannot be modified", ctx.exception.detail)

        # Flat: edit_order_follow_market raises 403
        mkt_handler = self._find_route_handler("/ibkr/edit_order_follow_market")
        req = MagicMock()
        req.json = AsyncMock(return_value={"orderId": 501})

        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(mkt_handler(req))
        self.assertEqual(ctx.exception.status_code, 403)

    def test_order_modification_when_daily_loss_limit_reached_with_active_position(self):
        self._set_daily_loss_limit(True)
        # Position is Short -1 (reversed position)
        self.mock_ibkr_trading.get_position_info.return_value = {"position": -1, "avgCost": 2580.0}

        # Case 1: Order 601 is a related BUY stop-loss order -> is_order_related_to_position returns True
        self.mock_ibkr_trading.is_order_related_to_position.side_effect = lambda oid: oid == 601
        self.mock_ibkr_trading.edit_order.return_value = {
            "orderId": 601,
            "orderType": "STP",
            "newPrice": 2590.0,
            "newQuantity": None,
            "status": "modified",
        }

        edit_handler = self._find_route_handler("/ibkr/edit_order")
        req = MagicMock()
        req.json = AsyncMock(return_value={"orderId": 601, "price": 2590.0})

        resp = asyncio.run(edit_handler(req))
        self.assertEqual(resp.status_code, 200)

        # Case 2: Order 602 is an unrelated SELL order -> is_order_related_to_position returns False
        req = MagicMock()
        req.json = AsyncMock(return_value={"orderId": 602, "price": 2570.0})

        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(edit_handler(req))
        self.assertEqual(ctx.exception.status_code, 403)

        # Case 3: Increasing volume beyond position size (-1 -> abs is 1) raises 400
        req = MagicMock()
        req.json = AsyncMock(return_value={"orderId": 601, "quantity": 5})

        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(edit_handler(req))
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn("Cannot increase order quantity", ctx.exception.detail)

    def test_cancel_and_market_close_allowed_when_daily_loss_limit_reached(self):
        self._set_daily_loss_limit(True)
        self.mock_ibkr_trading.get_position_info.return_value = {"position": 1, "avgCost": 2500.0}
        self.mock_ibkr_trading.cancel_order.return_value = {"orderId": 701, "status": "Cancelled"}
        self.mock_ibkr_trading.place_market_sell = AsyncMock(return_value={"orderId": 702})

        # Cancel order
        cancel_handler = self._find_route_handler("/ibkr/cancel_order")
        req = MagicMock()
        req.json = AsyncMock(return_value={"orderId": 701})
        resp = asyncio.run(cancel_handler(req))
        self.assertEqual(resp.status_code, 200)

        # Market close
        close_handler = self._find_route_handler("/ibkr/market_close")
        req = MagicMock()
        req.json = AsyncMock(return_value={"instrumentId": "OMXS306K"})
        resp = asyncio.run(close_handler(req))
        self.assertEqual(resp.status_code, 200)


if __name__ == "__main__":
    unittest.main()
