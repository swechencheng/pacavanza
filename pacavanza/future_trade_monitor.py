"""
future_trade_monitor.py

Subscribes to the OMXS30 future real-time stream (Redis channel
`pacavanza:future_updates`), and on every 5-minute bar completion:

  Long position:
    - Counts consecutive bear-bar pullbacks.
    - While any bar creates a new highest-high over the last 20 bars, resets
      the counter to zero (but still increments if the completed bar is a bear).
    - When the counter reaches 3, places a SELL STOP order 1 tick (0.25) below
      the low of the most-recent bear bar.

  Short position:
    - Counts consecutive bull-bar pullbacks.
    - While any bar creates a new lowest-low over the last 20 bars, resets
      the counter to zero (but still increments if the completed bar is a bull).
    - When the counter reaches 3, places a BUY STOP order 1 tick (0.25) above
      the high of the most-recent bull bar.

Order placement uses IbkrTrading (ibkr_trading.py).

The daemon is registered in daemon_controller.py as "future_trade_monitor".
"""

import asyncio
import json
import logging
import signal
import sys
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiohttp
import redis.asyncio as aioredis
from ib_async import IB, ContFuture, StopOrder

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger("future_trade_monitor")

# Redis channel published by future_market_daemon
REDIS_URL = "redis://localhost:6379/0"
REDIS_CHANNEL = "pacavanza:future_updates"

# Backend base URL (same process, loopback)
BACKEND_URL = "http://localhost:8001"

# OMXS30 future tick size
TICK_SIZE = 0.25

# Pullback counter threshold before placing stop order
PULLBACK_TRIGGER_COUNT = 3

# Number of historical bars used for higher-high / lower-low detection
LOOKBACK_BARS = 20

# IBKR connection settings (TWS paper-trading port; adjust for live: 7496)
# Used only for position queries — order management goes through the backend API.
IBKR_HOST = "127.0.0.1"
IBKR_PORT = 7497
IBKR_CLIENT_ID = 88  # unique ID – must not clash with other clients (backend uses 51)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_bear_bar(bar: Dict[str, Any]) -> bool:
    """Return True when the bar closed below its open (bear bar)."""
    return float(bar["close"]) < float(bar["open"])


def _is_bull_bar(bar: Dict[str, Any]) -> bool:
    """Return True when the bar closed above its open (bull bar)."""
    return float(bar["close"]) > float(bar["open"])


def _highest_high(bars: List[Dict[str, Any]], lookback: int) -> float:
    """Return the highest 'high' of the last *lookback* bars."""
    window = bars[-lookback:]
    return max(float(b["high"]) for b in window)


def _lowest_low(bars: List[Dict[str, Any]], lookback: int) -> float:
    """Return the lowest 'low' of the last *lookback* bars."""
    window = bars[-lookback:]
    return min(float(b["low"]) for b in window)


def _last_bear_bar_low(bars: List[Dict[str, Any]]) -> Optional[float]:
    """Return the low of the most-recent bear bar, or None if there is none."""
    for b in reversed(bars):
        if _is_bear_bar(b):
            return float(b["low"])
    return None


def _last_bull_bar_high(bars: List[Dict[str, Any]]) -> Optional[float]:
    """Return the high of the most-recent bull bar, or None if there is none."""
    for b in reversed(bars):
        if _is_bull_bar(b):
            return float(b["high"])
    return None


# ---------------------------------------------------------------------------
# Monitor
# ---------------------------------------------------------------------------


class FutureTradeMonitor:
    """
    Listens to the Redis `pacavanza:future_updates` stream and applies the
    pullback-counter logic described in the module docstring.

    State:
        _completed_bars  – ordered list of finalised 5m bars for the instrument
        _pullback_count  – current pullback bar count (reset on higher-high /
                           lower-low; incremented on bear/bull completion)
    """

    def __init__(
        self,
        redis_url: str = REDIS_URL,
        redis_channel: str = REDIS_CHANNEL,
        backend_url: str = BACKEND_URL,
    ):
        self._redis_url = redis_url
        self._redis_channel = redis_channel
        self._backend_url = backend_url
        self._redis: Optional[aioredis.Redis] = None
        self._http: Optional[aiohttp.ClientSession] = None

        # in-memory bar store, keyed by instrument id
        self._bars: Dict[str, List[Dict[str, Any]]] = {}

        # pullback counter state per instrument
        self._pullback_count: Dict[str, int] = {}

        # track start_time of the last bar we processed to avoid double-counting
        self._last_processed_bar_start: Dict[str, Optional[str]] = {}

        # IBKR connection (used only for position queries)
        self._ib: Optional[IB] = None
        self._contract = None

        # shutdown flag
        self._shutting_down = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    # ------------------------------------------------------------------
    # IBKR helpers
    # ------------------------------------------------------------------

    async def _connect_ibkr(self) -> None:
        """Connect asynchronously to TWS / IB Gateway and qualify the contract."""
        self._ib = IB()
        await self._ib.connectAsync(IBKR_HOST, IBKR_PORT, clientId=IBKR_CLIENT_ID)
        self._contract = ContFuture("OMXS30", "OMS")
        await self._ib.qualifyContractsAsync(self._contract)
        # Ensure position data is streamed
        self._ib.client.reqPositions()
        LOGGER.info(
            f"IBKR connected: contract={self._contract.localSymbol}, "
            f"conId={self._contract.conId}"
        )

    def _get_signed_position(self) -> int:
        """Return the signed position for the OMXS30 future. Positive=long, negative=short."""
        if self._ib is None or self._contract is None:
            return 0
        for pos in self._ib.positions():
            if pos.contract.conId == self._contract.conId:
                return int(pos.position)
        return 0

    async def _get_backend_open_orders(self) -> List[Dict[str, Any]]:
        """
        Fetch all active IBKR orders from the backend's own IB connection
        (clientId=51), which sees the full bracket/OCA order tree.
        Returns an empty list if the backend is unavailable.
        """
        try:
            async with self._http.get(
                f"{self._backend_url}/ibkr/open_orders",
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                if resp.status != 200:
                    LOGGER.warning(f"GET /ibkr/open_orders returned HTTP {resp.status}")
                    return []
                data = await resp.json()
                return data.get("orders", [])
        except Exception as exc:
            LOGGER.warning(f"Failed to fetch open orders from backend: {exc}")
            return []

    def _find_existing_stop_order(self, action: str):
        """
        This method is not used directly any more.
        Use the async version _find_existing_stop_order_async instead.
        """
        raise RuntimeError("Use _find_existing_stop_order_async")

    async def _find_existing_stop_order_async(
        self, action: str
    ) -> Optional[Dict[str, Any]]:
        """
        Find the best-candidate active STOP order for *action* ('SELL' or 'BUY')
        by querying the backend's /ibkr/open_orders endpoint.

        This works for ALL orders the backend's IbkrTrading knows about —
        including OCA/bracket SL children placed by clientId=51 — regardless
        of which IB client placed them.

        When multiple candidates exist we return the *least protective* one
        (lowest SELL STOP price / highest BUY STOP price) so the new tighter
        price can improve it.

        Returns an order dict (with at least 'orderId' and 'price' keys),
        or None if none found.
        """
        orders = await self._get_backend_open_orders()
        candidates = [
            o
            for o in orders
            if o.get("action") == action
            and o.get("orderType") in ("STP", "STP LMT")
            and not o.get("isDone", True)
        ]
        if not candidates:
            return None

        def _price(o):
            return float(o.get("price") or 0)

        # Least protective: lowest SELL STOP (farthest below market) or
        # highest BUY STOP (farthest above market).
        if action == "SELL":
            return min(candidates, key=_price)
        else:
            return max(candidates, key=_price)

    async def _edit_stop_via_backend(self, order_id: int, new_price: float) -> bool:
        """
        Modify an existing stop order's price by calling POST /ibkr/edit_order
        on the backend.

        The backend uses its own IbkrTrading (clientId=51) to modify the order,
        which means:
          - No cross-client permission issues.
          - The backend's _on_order_change callback fires automatically.
          - The frontend receives a WebSocket 'order_update' message immediately.

        Returns True on success.
        """
        rounded = round(round(new_price / TICK_SIZE) * TICK_SIZE, 2)
        payload = {"orderId": order_id, "price": rounded}
        try:
            async with self._http.post(
                f"{self._backend_url}/ibkr/edit_order",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                body = await resp.json()
                if resp.status == 200:
                    LOGGER.info(
                        f"Backend edited orderId={order_id} → stopPrice={rounded}: {body}"
                    )
                    return True
                else:
                    LOGGER.error(
                        f"Backend /ibkr/edit_order failed (HTTP {resp.status}): {body}"
                    )
                    return False
        except Exception as exc:
            LOGGER.error(f"HTTP call to /ibkr/edit_order failed: {exc}")
            return False

    async def _place_stop_via_backend(
        self, action: str, stop_price: float, volume: int
    ) -> bool:
        """
        Place a brand-new standalone STOP order via the backend.
        Uses /ibkr/edit_order would not work for new orders, so we fall back
        to placing it directly through our own IB connection as a standalone
        protective stop.  This only fires when no existing stop was found at all.
        """
        if self._ib is None or self._contract is None:
            LOGGER.warning(f"Cannot place {action} STOP – IBKR not connected.")
            return False
        rounded = round(round(stop_price / TICK_SIZE) * TICK_SIZE, 2)
        order = StopOrder(action, volume, rounded)
        order.orderRef = "PullbackSL"
        trade = self._ib.placeOrder(self._contract, order)
        LOGGER.info(
            f"Placed new {action} STOP via own IB connection: "
            f"orderId={trade.order.orderId}, stopPrice={rounded}, volume={volume}"
        )
        return True

    async def _place_sell_stop(self, stop_price: float) -> None:
        """
        Protect a long position with a SELL STOP at *stop_price*.

        Decision:
          • If an existing SELL STOP is found (via backend) AND the new price
            is higher (nearer to market) → edit the existing order via backend.
          • If found but existing price is already equal or higher → skip.
          • If none found → place a new standalone stop via our own IB connection.
        """
        rounded = round(round(stop_price / TICK_SIZE) * TICK_SIZE, 2)
        existing = await self._find_existing_stop_order_async("SELL")

        if existing is not None:
            existing_price = float(existing.get("price") or 0)
            order_id = int(existing["orderId"])
            if rounded > existing_price:
                LOGGER.info(
                    f"SELL STOP: new {rounded} > existing {existing_price} "
                    f"(orderId={order_id}) – adjusting via backend."
                )
                await self._edit_stop_via_backend(order_id, rounded)
            else:
                LOGGER.info(
                    f"SELL STOP: new {rounded} <= existing {existing_price} "
                    f"(orderId={order_id}) – existing is already nearer; skipping."
                )
            return

        # No existing stop – place fresh
        signed_pos = self._get_signed_position()
        volume = abs(signed_pos) if signed_pos > 0 else 1
        await self._place_stop_via_backend("SELL", rounded, volume)

    async def _place_buy_stop(self, stop_price: float) -> None:
        """
        Protect a short position with a BUY STOP at *stop_price*.

        Decision:
          • If an existing BUY STOP is found (via backend) AND the new price
            is lower (nearer to market) → edit the existing order via backend.
          • If found but existing price is already equal or lower → skip.
          • If none found → place a new standalone stop via our own IB connection.
        """
        rounded = round(round(stop_price / TICK_SIZE) * TICK_SIZE, 2)
        existing = await self._find_existing_stop_order_async("BUY")

        if existing is not None:
            existing_price = float(existing.get("price") or 0)
            order_id = int(existing["orderId"])
            if rounded < existing_price:
                LOGGER.info(
                    f"BUY STOP: new {rounded} < existing {existing_price} "
                    f"(orderId={order_id}) – adjusting via backend."
                )
                await self._edit_stop_via_backend(order_id, rounded)
            else:
                LOGGER.info(
                    f"BUY STOP: new {rounded} >= existing {existing_price} "
                    f"(orderId={order_id}) – existing is already nearer; skipping."
                )
            return

        # No existing stop – place fresh
        signed_pos = self._get_signed_position()
        volume = abs(signed_pos) if signed_pos < 0 else 1
        await self._place_stop_via_backend("BUY", rounded, volume)

    # ------------------------------------------------------------------
    # Bar processing logic
    # ------------------------------------------------------------------

    async def _on_bar_completed(self, instrument_id: str, bar: Dict[str, Any]) -> None:
        """
        Called once per 5m bar completion.

        Implements the pullback-counter rules for both long and short positions.
        All counter mutations are guarded behind current position checks so that
        the monitor is a no-op when flat.
        """
        bars = self._bars.setdefault(instrument_id, [])

        # Append or update the completed bar
        bar_start = bar.get("start_time")
        if bars and bars[-1].get("start_time") == bar_start:
            bars[-1] = bar  # finalise in-place
        else:
            bars.append(bar)

        # Need at least LOOKBACK_BARS bars for meaningful analysis
        if len(bars) < LOOKBACK_BARS:
            LOGGER.debug(
                f"[{instrument_id}] Only {len(bars)} bars – waiting for {LOOKBACK_BARS}."
            )
            return

        signed_pos = self._get_signed_position()
        if signed_pos == 0:
            # Flat – reset counter and do nothing
            self._pullback_count[instrument_id] = 0
            LOGGER.debug(f"[{instrument_id}] Flat position – no action.")
            return

        count = self._pullback_count.get(instrument_id, 0)
        is_bear = _is_bear_bar(bar)
        is_bull = _is_bull_bar(bar)

        if signed_pos > 0:
            # ── Long position ─────────────────────────────────────────
            # Higher-high detection: did this bar's high exceed the previous 20-bar high?
            prev_bars = bars[:-1]  # exclude current bar
            if len(prev_bars) >= LOOKBACK_BARS:
                prior_high = _highest_high(prev_bars, LOOKBACK_BARS)
                if float(bar["high"]) > prior_high:
                    LOGGER.info(
                        f"[{instrument_id}] Long: higher high ({bar['high']} > {prior_high}). "
                        f"Resetting pullback counter."
                    )
                    count = 0

            # Increment counter if bear bar on completion
            if is_bear:
                count += 1
                LOGGER.info(
                    f"[{instrument_id}] Long: bear bar completed. "
                    f"Pullback counter → {count}."
                )

            self._pullback_count[instrument_id] = count

            if count >= PULLBACK_TRIGGER_COUNT:
                low = _last_bear_bar_low(bars)
                if low is not None:
                    stop_price = round(low - TICK_SIZE, 2)
                    LOGGER.info(
                        f"[{instrument_id}] Long: pullback counter reached {count}. "
                        f"Placing SELL STOP at {stop_price} "
                        f"(last bear low={low} – 1 tick)."
                    )
                    await self._place_sell_stop(stop_price)
                    # Reset counter so we don't re-place on every subsequent bar
                    self._pullback_count[instrument_id] = 0
                else:
                    LOGGER.warning(
                        f"[{instrument_id}] Long: pullback counter={count} but no bear bar found."
                    )

        else:
            # ── Short position ────────────────────────────────────────
            # Lower-low detection: did this bar's low break the previous 20-bar low?
            prev_bars = bars[:-1]
            if len(prev_bars) >= LOOKBACK_BARS:
                prior_low = _lowest_low(prev_bars, LOOKBACK_BARS)
                if float(bar["low"]) < prior_low:
                    LOGGER.info(
                        f"[{instrument_id}] Short: lower low ({bar['low']} < {prior_low}). "
                        f"Resetting pullback counter."
                    )
                    count = 0

            # Increment counter if bull bar on completion
            if is_bull:
                count += 1
                LOGGER.info(
                    f"[{instrument_id}] Short: bull bar completed. "
                    f"Pullback counter → {count}."
                )

            self._pullback_count[instrument_id] = count

            if count >= PULLBACK_TRIGGER_COUNT:
                high = _last_bull_bar_high(bars)
                if high is not None:
                    stop_price = round(high + TICK_SIZE, 2)
                    LOGGER.info(
                        f"[{instrument_id}] Short: pullback counter reached {count}. "
                        f"Placing BUY STOP at {stop_price} "
                        f"(last bull high={high} + 1 tick)."
                    )
                    await self._place_buy_stop(stop_price)
                    # Reset counter so we don't re-place on every subsequent bar
                    self._pullback_count[instrument_id] = 0
                else:
                    LOGGER.warning(
                        f"[{instrument_id}] Short: pullback counter={count} but no bull bar found."
                    )

    def _on_bar_update(self, instrument_id: str, bar: Dict[str, Any]) -> None:
        """
        Called on every in-progress bar update (type='update').

        We only update the live bar in our local store here; all business logic
        fires on *completion* (type='completed').
        """
        bars = self._bars.setdefault(instrument_id, [])
        bar_start = bar.get("start_time")
        if bars and bars[-1].get("start_time") == bar_start:
            bars[-1] = bar
        else:
            # New bar started
            bars.append(bar)
            # Keep bounded
            if len(bars) > 1500:
                bars[:] = bars[-1500:]

    # ------------------------------------------------------------------
    # Redis listener
    # ------------------------------------------------------------------

    async def _listen(self) -> None:
        """Subscribe to the Redis channel and dispatch messages."""
        LOGGER.info(f"Connecting to Redis at {self._redis_url} …")
        self._redis = aioredis.from_url(self._redis_url)
        await self._redis.ping()
        LOGGER.info("Redis connected.")

        pubsub = self._redis.pubsub()
        await pubsub.subscribe(REDIS_CHANNEL)
        LOGGER.info(f"Subscribed to Redis channel: {REDIS_CHANNEL}")

        try:
            async for msg in pubsub.listen():
                if self._shutting_down:
                    break
                if not msg or msg.get("type") != "message":
                    continue

                raw = msg["data"]
                try:
                    if isinstance(raw, (bytes, bytearray)):
                        payload = json.loads(raw.decode("utf-8"))
                    else:
                        payload = json.loads(raw)
                except Exception as exc:
                    LOGGER.error(f"Failed to decode Redis message: {exc}")
                    continue

                msg_type = payload.get("type")
                instrument_id = payload.get("instrument")
                bar = payload.get("bar")

                if not instrument_id or not bar:
                    continue

                # Normalise timestamps
                for key in ("start_time", "end_time"):
                    if key in bar and isinstance(bar[key], str):
                        try:
                            dt = datetime.fromisoformat(bar[key])
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                            bar[key] = dt.isoformat()
                        except Exception:
                            pass

                if msg_type == "update":
                    self._on_bar_update(instrument_id, bar)

                elif msg_type == "completed":
                    bar_start = bar.get("start_time")
                    last = self._last_processed_bar_start.get(instrument_id)
                    if bar_start == last:
                        # Already processed this completion (can fire multiple times)
                        continue
                    self._last_processed_bar_start[instrument_id] = bar_start
                    LOGGER.info(
                        f"[{instrument_id}] Bar completed: "
                        f"start={bar_start} O={bar.get('open')} H={bar.get('high')} "
                        f"L={bar.get('low')} C={bar.get('close')}"
                    )
                    try:
                        await self._on_bar_completed(instrument_id, bar)
                    except Exception as exc:
                        LOGGER.exception(
                            f"[{instrument_id}] Error in _on_bar_completed: {exc}"
                        )

        except asyncio.CancelledError:
            LOGGER.info("Redis listener cancelled.")
        finally:
            try:
                await pubsub.unsubscribe(REDIS_CHANNEL)
                await pubsub.aclose()
            except Exception:
                pass
            try:
                await self._redis.aclose()
            except Exception:
                pass
            self._redis = None

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    async def _run_async(self) -> None:
        """Top-level async runner with reconnect loop."""
        # Open a shared aiohttp session for backend HTTP calls
        self._http = aiohttp.ClientSession()

        # Connect IBKR asynchronously (used for position queries only)
        try:
            await self._connect_ibkr()
        except Exception as exc:
            LOGGER.error(
                f"Failed to connect to IBKR – position info unavailable: {exc}"
            )

        try:
            while not self._shutting_down:
                try:
                    await self._listen()
                except Exception as exc:
                    LOGGER.error(f"Redis listener error: {exc}. Reconnecting in 5 s …")
                    if not self._shutting_down:
                        await asyncio.sleep(5)
        finally:
            if self._http is not None:
                await self._http.close()
                self._http = None

    async def _shutdown_async(self, signum) -> None:
        LOGGER.info(f"Signal {signum} received – shutting down.")
        self._shutting_down = True
        if self._http is not None:
            try:
                await self._http.close()
            except Exception:
                pass
            self._http = None
        if self._ib is not None:
            try:
                self._ib.disconnect()
            except Exception:
                pass
        loop = asyncio.get_event_loop()
        for task in asyncio.all_tasks(loop):
            if task is asyncio.current_task():
                continue
            task.cancel()
        loop.stop()

    def run(self) -> None:
        """Synchronous entry point called by daemon_controller via importlib."""
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)

        if threading.current_thread() is threading.main_thread():

            def _schedule_shutdown(s):
                asyncio.run_coroutine_threadsafe(self._shutdown_async(s), loop)

            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, lambda s=sig: _schedule_shutdown(s))
                except Exception:
                    pass

        try:
            loop.run_until_complete(self._run_async())
        except KeyboardInterrupt:
            LOGGER.info("KeyboardInterrupt – stopping.")
        finally:
            try:
                loop.run_until_complete(asyncio.sleep(0.1))
            except Exception:
                pass
            loop.close()


# ---------------------------------------------------------------------------
# Module-level `main()` function expected by daemon_controller._run_module
# ---------------------------------------------------------------------------


def main() -> None:
    monitor = FutureTradeMonitor()
    monitor.run()


if __name__ == "__main__":
    main()
