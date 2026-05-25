from .base_trading import BaseAvanzaTrading


class AvanzaTrading(BaseAvanzaTrading):
    """
    AvanzaTrading for Avanza mini-futures (quote-web-push instruments).

    Mini-futures have live buy/sell (bid/ask) quotes from market makers.
    This affects:
      - Market buy uses last sell (ask) price
      - Market sell uses last buy (bid) price
      - Buy stop trigger is on market maker quote, limit = trigger + tick
      - Sell stop trigger is on market maker quote, limit = trigger - tick
      - Buy stop price includes an extra tick offset (data is buy-side only)
    """

    # --------------------------------------------------------------------------
    # Subclass hook implementations
    # --------------------------------------------------------------------------

    def _compute_buy_stop_trigger_and_limit(
        self,
        high_last: float,
        tick: float,
        tick_coeff: float,
    ) -> tuple:
        """
        Mini-future buy stop:
        - trigger = high_last + tick * tick_coeff + tick (extra tick for buy-side-only data)
        - limit = trigger + tick
        - trigger_on_market_maker_quote = True
        """
        stop_price = high_last + (tick * tick_coeff)
        # Need to add an extra tick to match sell side since the data is only buy side
        stop_price += tick
        stop_price = round(stop_price, 2)
        limit_price = round(stop_price + tick, 2)
        return stop_price, limit_price, True

    def _compute_sell_stop_trigger_and_limit(
        self,
        trigger_price: float,
        tick: float,
        tick_coeff: float,
    ) -> tuple:
        """
        Mini-future sell stop:
        - trigger = trigger_price (already computed by caller)
        - limit = trigger - tick
        - trigger_on_market_maker_quote = True
        """
        limit_price = round(trigger_price - tick, 2)
        return trigger_price, limit_price, True

    def _compute_take_profit_limit(
        self,
        take_profit: float,
        tick: float,
    ) -> tuple:
        """
        Mini-future take-profit:
        - limit = take_profit - 0.01
        - trigger_on_market_maker_quote = True
        """
        limit_price = round(take_profit - 0.01, 2)
        return limit_price, True
