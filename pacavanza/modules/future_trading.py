import asyncio
import json
import logging
from typing import Dict, Any, List, Optional

from datetime import datetime, timezone

from .base_trading import BaseAvanzaTrading, LOGGER


class FutureTrading(BaseAvanzaTrading):
    """
    FutureTrading for classic futures (trade-web-push instruments, e.g. OMXS306D).

    Classic futures have only completed trade data — no live buy/sell (bid/ask) quotes.
    This affects:
      - Market buy/sell uses the last completed trade price
      - Buy stop trigger = last completed bar high + tick (tick_coeff is always 1.0)
      - Buy stop limit = same as trigger (no spread to account for)
      - Sell stop trigger = last completed bar low - tick (tick_coeff is always 1.0)
      - Sell stop limit = same as trigger
      - trigger_on_market_maker_quote = False (no market maker for classic futures)
    """

    # --------------------------------------------------------------------------
    # Subclass hook implementations
    # --------------------------------------------------------------------------

    async def _get_last_trade_price(self, instrument_id: str) -> Optional[float]:
        """Get the last completed trade price from metadata or bar close."""
        meta = await self._get_metadata_snapshot(instrument_id)
        for key in ("last_price", "lastPrice", "price"):
            if meta.get(key) is not None:
                try:
                    return float(meta[key])
                except Exception:
                    pass
        lst = await self._get_snapshot(instrument_id)
        if lst:
            return float(lst[-1]["close"])
        return None

    def _compute_buy_stop_trigger_and_limit(
        self,
        high_last: float,
        tick: float,
        tick_coeff: float,
    ) -> tuple:
        """
        Future buy stop:
        - trigger = high_last + tick (tick_coeff is always 1.0 for futures)
        - limit = trigger (same as trigger, no spread)
        - trigger_on_market_maker_quote = False
        """
        stop_price = round(high_last + tick, 2)
        limit_price = stop_price  # same as trigger for futures
        return stop_price, limit_price, False

    def _compute_sell_stop_trigger_and_limit(
        self,
        trigger_price: float,
        tick: float,
        tick_coeff: float,
    ) -> tuple:
        """
        Future sell stop:
        - trigger = trigger_price (already computed by caller)
        - limit = trigger (same as trigger, no spread)
        - trigger_on_market_maker_quote = False
        """
        limit_price = trigger_price  # same as trigger for futures
        return trigger_price, limit_price, False

    def _compute_take_profit_limit(
        self,
        take_profit: float,
        tick: float,
    ) -> tuple:
        """
        Future take-profit:
        - limit = take_profit (same as trigger, no spread to account for)
        - trigger_on_market_maker_quote = False
        """
        limit_price = take_profit
        return limit_price, False
