"""
ibkr_portfolio.py

Extracts portfolio-level data from Interactive Brokers via ib_async:
- Account summary (NLV, cash, margins, buying power, cushion)
- Portfolio positions with market values and P&L
- Recent executions/fills
- P&L summary (daily/total)

This module wraps an existing IB connection instance and provides
JSON-serializable dicts suitable for the portfolio frontend.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ib_async import IB

LOGGER = logging.getLogger("ibkr_portfolio")


class IbkrPortfolio:
    """
    Read-only portfolio data provider for an existing ib_async IB connection.

    Does NOT own or manage the IB connection — the caller (backend.py) is
    responsible for connecting/disconnecting.
    """

    def __init__(self, ib: IB):
        self.ib = ib
        self._pnl_subscribed = False
        self._pnl_single_subscribed: Dict[int, bool] = {}

        # Subscribe to account and portfolio updates so that
        # ib.accountValues() and ib.portfolio() are populated.
        # This is a streaming subscription — data arrives asynchronously.
        try:
            self.ib.reqAccountUpdates()
            LOGGER.info("Subscribed to IBKR account updates")
        except Exception as e:
            LOGGER.warning(f"Failed to subscribe to account updates: {e}")

    # ------------------------------------------------------------------
    # Account summary
    # ------------------------------------------------------------------

    def get_account_summary(self) -> Dict[str, Any]:
        """
        Return key account metrics from IBKR.

        Uses ib.accountValues() which returns a list of AccountValue objects:
            AccountValue(account, tag, value, currency, modelCode)

        IBKR returns multiple entries per tag — one per currency ("USD",
        "SEK", etc.) plus optionally "BASE" for consolidated values.
        We prefer "BASE" or empty-currency entries; if none exist for a
        tag we fall back to the first currency-specific entry.
        """
        values = self.ib.accountValues()
        if not values:
            return {}

        # Tags we care about (tag -> display label)
        WANTED_TAGS = {
            "NetLiquidation": "netLiquidation",
            "TotalCashValue": "totalCash",
            "BuyingPower": "buyingPower",
            "GrossPositionValue": "grossPositionValue",
            "MaintMarginReq": "maintMargin",
            "AvailableFunds": "availableFunds",
            "ExcessLiquidity": "excessLiquidity",
            "Cushion": "cushion",
            "UnrealizedPnL": "unrealizedPnL",
            "RealizedPnL": "realizedPnL",
            "FullMaintMarginReq": "fullMaintMargin",
            "FullInitMarginReq": "fullInitMargin",
            "InitMarginReq": "initMargin",
            "EquityWithLoanValue": "equityWithLoan",
        }

        # Collect all values per tag, grouped by priority:
        #   priority 0: currency == "" or "BASE" (consolidated)
        #   priority 1: any specific currency (fallback)
        # tag -> { priority: (value_str, currency) }
        tag_values: Dict[str, Dict[int, tuple]] = {}
        account_id = None
        base_currency = None

        for av in values:
            if account_id is None:
                account_id = av.account

            # Detect the account's base currency from NetLiquidation
            if av.tag == "NetLiquidation" and av.currency not in ("", "BASE"):
                if base_currency is None:
                    base_currency = av.currency

            if av.tag not in WANTED_TAGS:
                continue

            if av.tag not in tag_values:
                tag_values[av.tag] = {}

            if av.currency in ("", "BASE"):
                tag_values[av.tag][0] = (av.value, av.currency)
            elif base_currency and av.currency == base_currency:
                # Prefer the base currency over other currencies
                if 1 not in tag_values[av.tag]:
                    tag_values[av.tag][1] = (av.value, av.currency)
            else:
                if 2 not in tag_values[av.tag]:
                    tag_values[av.tag][2] = (av.value, av.currency)

        summary: Dict[str, Any] = {}

        for tag, label in WANTED_TAGS.items():
            entry = tag_values.get(tag)
            if not entry:
                continue
            # Pick best priority: 0 (BASE) > 1 (base_currency) > 2 (any)
            val_str, currency = None, None
            for prio in (0, 1, 2):
                if prio in entry:
                    val_str, currency = entry[prio]
                    break
            if val_str is None:
                continue
            try:
                if tag == "Cushion":
                    summary[label] = float(val_str) * 100  # -> percentage
                else:
                    summary[label] = float(val_str)
            except (ValueError, TypeError):
                summary[label] = val_str

        summary["accountId"] = account_id
        summary["baseCurrency"] = base_currency
        summary["timestamp"] = datetime.now(timezone.utc).isoformat()
        return summary

    # ------------------------------------------------------------------
    # Portfolio positions
    # ------------------------------------------------------------------

    def get_portfolio_positions(self) -> List[Dict[str, Any]]:
        """
        Return all portfolio positions with market values and P&L.

        Uses ib.portfolio() which returns PortfolioItem objects:
            PortfolioItem(contract, position, marketPrice, marketValue,
                          averageCost, unrealizedPNL, realizedPNL, account)
        """
        items = self.ib.portfolio()
        positions = []

        for item in items:
            contract = item.contract
            position = float(item.position)
            market_price = float(item.marketPrice) if item.marketPrice else 0.0
            market_value = float(item.marketValue) if item.marketValue else 0.0
            avg_cost = float(item.averageCost) if item.averageCost else 0.0
            unrealized_pnl = float(item.unrealizedPNL) if item.unrealizedPNL else 0.0
            realized_pnl = float(item.realizedPNL) if item.realizedPNL else 0.0

            # Compute per-unit average cost (for futures, divide by multiplier)
            multiplier = 1.0
            try:
                if contract.multiplier:
                    multiplier = float(contract.multiplier)
                    if multiplier <= 0:
                        multiplier = 1.0
            except (ValueError, TypeError):
                multiplier = 1.0

            avg_price = avg_cost / multiplier if avg_cost else 0.0

            # Compute P&L percentage
            cost_basis = abs(position) * avg_price * multiplier
            pnl_pct = (unrealized_pnl / cost_basis * 100) if cost_basis else 0.0

            positions.append(
                {
                    "conId": contract.conId,
                    "symbol": contract.symbol or "",
                    "localSymbol": contract.localSymbol or "",
                    "secType": contract.secType or "",
                    "exchange": contract.exchange or "",
                    "currency": contract.currency or "",
                    "multiplier": multiplier,
                    "position": position,
                    "marketPrice": round(market_price, 4),
                    "marketValue": round(market_value, 2),
                    "avgCost": round(avg_cost, 2),
                    "avgPrice": round(avg_price, 4),
                    "unrealizedPnL": round(unrealized_pnl, 2),
                    "realizedPnL": round(realized_pnl, 2),
                    "pnlPercent": round(pnl_pct, 2),
                    "account": item.account or "",
                }
            )

        # Also include Cash balances as positions (like IBKR TWS)
        account_values = self.ib.accountValues()
        exchange_rates = {}
        for av in account_values:
            if av.tag == "ExchangeRate" and av.currency not in ("", "BASE"):
                try:
                    exchange_rates[av.currency] = float(av.value)
                except (ValueError, TypeError):
                    pass

        for av in account_values:
            if av.tag == "CashBalance" and av.currency not in ("", "BASE"):
                try:
                    cash_val = float(av.value)
                    if cash_val != 0:
                        rate = exchange_rates.get(av.currency, 1.0)
                        market_value = cash_val * rate
                        positions.append(
                            {
                                "conId": 0,
                                "symbol": av.currency,
                                "localSymbol": f"{av.currency}.CASH",
                                "secType": "CASH",
                                "exchange": "",
                                "currency": av.currency,
                                "multiplier": 1.0,
                                "position": cash_val,
                                "marketPrice": round(rate, 4),
                                "marketValue": round(market_value, 2),
                                "avgCost": 0.0,
                                "avgPrice": 0.0,
                                "unrealizedPnL": 0.0,
                                "realizedPnL": 0.0,
                                "pnlPercent": 0.0,
                                "account": av.account or "",
                            }
                        )
                except (ValueError, TypeError):
                    pass

        # Sort by absolute market value descending
        positions.sort(key=lambda p: abs(p["marketValue"]), reverse=True)
        return positions

    # ------------------------------------------------------------------
    # Open orders
    # ------------------------------------------------------------------

    def get_open_orders(self) -> List[Dict[str, Any]]:
        """
        Return all open/active orders across all contracts.

        Returns a list of dicts with order details suitable for frontend rendering.
        """
        orders = []
        for trade in self.ib.openTrades():
            if not trade.isActive():
                continue

            order = trade.order
            contract = trade.contract

            price = None
            if order.orderType == "STP":
                price = order.auxPrice
            elif order.orderType == "LMT":
                price = order.lmtPrice
            elif order.orderType == "STP LMT":
                price = order.auxPrice
            elif order.orderType == "MKT":
                price = None  # Market orders have no fixed price

            placed_time = None
            if trade.log:
                placed_time = trade.log[0].time
            if not placed_time:
                placed_time = datetime.now(timezone.utc)

            parent_id = getattr(order, "parentId", 0)
            oca_group = getattr(order, "ocaGroup", "")

            orders.append(
                {
                    "orderId": order.orderId,
                    "permId": order.permId,
                    "symbol": contract.symbol or "",
                    "localSymbol": contract.localSymbol or "",
                    "secType": contract.secType or "",
                    "action": order.action,
                    "orderType": order.orderType,
                    "totalQuantity": int(order.totalQuantity),
                    "price": price,
                    "status": trade.orderStatus.status,
                    "parentId": parent_id if parent_id else None,
                    "ocaGroup": oca_group if oca_group else None,
                    "tif": order.tif or "",
                    "placedTime": placed_time.isoformat() if placed_time else None,
                    "filledQuantity": int(trade.orderStatus.filled),
                    "remaining": int(trade.orderStatus.remaining),
                    "avgFillPrice": (
                        float(trade.orderStatus.avgFillPrice)
                        if trade.orderStatus.avgFillPrice
                        else None
                    ),
                }
            )

        return orders

    # ------------------------------------------------------------------
    # Executions / fills
    # ------------------------------------------------------------------

    def get_executions(self) -> List[Dict[str, Any]]:
        """
        Return recent executions from the current IBKR session.

        Uses ib.fills() which returns Fill objects:
            Fill(contract, execution, commissionReport, time)
        """
        fills = self.ib.fills()
        executions = []

        for fill in fills:
            contract = fill.contract
            exec_ = fill.execution
            comm = fill.commissionReport

            executions.append(
                {
                    "execId": exec_.execId,
                    "symbol": contract.symbol or "",
                    "localSymbol": contract.localSymbol or "",
                    "secType": contract.secType or "",
                    "side": exec_.side,  # "BOT" or "SLD"
                    "quantity": int(exec_.shares),
                    "price": float(exec_.price),
                    "time": exec_.time.isoformat() if exec_.time else None,
                    "exchange": exec_.exchange or "",
                    "orderId": exec_.orderId,
                    "commission": (
                        float(comm.commission) if comm and comm.commission else 0.0
                    ),
                    "realizedPnL": (
                        float(comm.realizedPNL) if comm and comm.realizedPNL else None
                    ),
                    "currency": contract.currency or "",
                }
            )

        # Sort by time descending (most recent first)
        executions.sort(key=lambda e: e["time"] or "", reverse=True)
        return executions

    # ------------------------------------------------------------------
    # P&L summary
    # ------------------------------------------------------------------

    def get_pnl_summary(self) -> Dict[str, Any]:
        """
        Return aggregated P&L information.

        Combines data from ib.pnl() (account-level) and positions.
        """
        positions = self.get_portfolio_positions()
        total_unrealized = sum(p["unrealizedPnL"] for p in positions)
        total_realized = sum(p["realizedPnL"] for p in positions)
        total_market_value = sum(p["marketValue"] for p in positions)

        # Count winning/losing positions
        winning = sum(1 for p in positions if p["unrealizedPnL"] > 0)
        losing = sum(1 for p in positions if p["unrealizedPnL"] < 0)
        flat = sum(1 for p in positions if p["unrealizedPnL"] == 0)

        return {
            "totalUnrealizedPnL": round(total_unrealized, 2),
            "totalRealizedPnL": round(total_realized, 2),
            "totalPnL": round(total_unrealized + total_realized, 2),
            "totalMarketValue": round(total_market_value, 2),
            "positionCount": len(positions),
            "winningPositions": winning,
            "losingPositions": losing,
            "flatPositions": flat,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
