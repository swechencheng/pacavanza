"""
future_trade_monitor.py

Subscribes to the OMXS30 future real-time stream (Redis channel
`pacavanza:future_updates`), and on every 5-minute bar completion
applies an ABC pivot-pattern trailing stop:

  Long position:
    - A = Pivot Low (initial stop reference)
    - B = Pivot High (swing high after A)
    - C = Pivot Low with C.low > A.low (higher low)
    - Confirmation: a bar on C's right leg closes above B's pivot high
    - Action: move SELL STOP from A.low → C.low

  Short position:
    - A = Pivot High (initial stop reference)
    - B = Pivot Low (swing low after A)
    - C = Pivot High with C.high < A.high (lower high)
    - Confirmation: a bar on C's right leg closes below B's pivot low
    - Action: move BUY STOP from A.high → C.high

  All pivots require at least 2 bars on the left leg and 2 bars on
  the right leg, and are detected on bar close/completion only.
  After a successful stop move, C becomes the new A and the pattern
  chains indefinitely.

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
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import redis.asyncio as aioredis

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger("future_trade_monitor")

from pacavanza.config import REDIS_URL, BACKEND_PORT

# Redis channel published by future_market_daemon
REDIS_CHANNEL = "pacavanza:future_updates"

# Backend base URL (same process, loopback)
BACKEND_URL = f"http://localhost:{BACKEND_PORT}"

# OMXS30 future tick size
TICK_SIZE = 0.25

# Pivot detection: minimum bars on each leg
PIVOT_LEFT = 2
PIVOT_RIGHT = 2


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_bear_bar(bar: Dict[str, Any]) -> bool:
    """Return True when the bar closed below its open (bear bar)."""
    return float(bar["close"]) < float(bar["open"])


def _is_bull_bar(bar: Dict[str, Any]) -> bool:
    """Return True when the bar closed above its open (bull bar)."""
    return float(bar["close"]) > float(bar["open"])


# ---------------------------------------------------------------------------
# Pivot detection helpers
# ---------------------------------------------------------------------------


def _is_pivot_low_at(
    bars: List[Dict[str, Any]],
    idx: int,
    left: int = PIVOT_LEFT,
    right: int = PIVOT_RIGHT,
) -> bool:
    """
    Check whether the bar at *idx* is a pivot low.

    A pivot low requires *left* bars to the left with lows >= pivot low,
    and *right* bars to the right with lows >= pivot low.
    """
    if idx < left or idx + right >= len(bars):
        return False
    pivot_low = float(bars[idx]["low"])
    for i in range(1, left + 1):
        if float(bars[idx - i]["low"]) < pivot_low:
            return False
    for i in range(1, right + 1):
        if float(bars[idx + i]["low"]) < pivot_low:
            return False
    return True


def _is_pivot_high_at(
    bars: List[Dict[str, Any]],
    idx: int,
    left: int = PIVOT_LEFT,
    right: int = PIVOT_RIGHT,
) -> bool:
    """
    Check whether the bar at *idx* is a pivot high.

    A pivot high requires *left* bars to the left with highs <= pivot high,
    and *right* bars to the right with highs <= pivot high.
    """
    if idx < left or idx + right >= len(bars):
        return False
    pivot_high = float(bars[idx]["high"])
    for i in range(1, left + 1):
        if float(bars[idx - i]["high"]) > pivot_high:
            return False
    for i in range(1, right + 1):
        if float(bars[idx + i]["high"]) > pivot_high:
            return False
    return True


def _find_nearest_pivot_low(
    bars: List[Dict[str, Any]],
    left: int = PIVOT_LEFT,
    right: int = PIVOT_RIGHT,
    min_idx: int = 0,
) -> Optional[Tuple[int, float]]:
    """
    Scan backwards through *bars* and return ``(index, low_value)`` for the
    most recent pivot low, or ``None`` if none found.

    The latest confirmable pivot is at index ``len(bars) - 1 - right``
    because it needs *right* bars to its right.

    *min_idx* limits how far back the scan goes (inclusive).
    """
    start = max(left, min_idx)
    for idx in range(len(bars) - 1 - right, start - 1, -1):
        if _is_pivot_low_at(bars, idx, left, right):
            return idx, float(bars[idx]["low"])
    return None


def _find_nearest_pivot_high(
    bars: List[Dict[str, Any]],
    left: int = PIVOT_LEFT,
    right: int = PIVOT_RIGHT,
    min_idx: int = 0,
) -> Optional[Tuple[int, float]]:
    """
    Scan backwards through *bars* and return ``(index, high_value)`` for the
    most recent pivot high, or ``None`` if none found.

    *min_idx* limits how far back the scan goes (inclusive).
    """
    start = max(left, min_idx)
    for idx in range(len(bars) - 1 - right, start - 1, -1):
        if _is_pivot_high_at(bars, idx, left, right):
            return idx, float(bars[idx]["high"])
    return None


# ---------------------------------------------------------------------------
# Monitor
# ---------------------------------------------------------------------------


class FutureTradeMonitor:
    """
    Listens to the Redis `pacavanza:future_updates` stream and applies the
    ABC pivot-pattern trailing stop described in the module docstring.

    State:
        _bars       – ordered list of finalised 5m bars per instrument
        _abc_state  – ABC pattern tracking state per instrument
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

        # ABC pattern state per instrument
        # Each entry tracks the initial anchor: {"A": float, "A_idx": int}
        self._abc_state: Dict[str, Optional[Dict[str, Any]]] = {}

        # track the extreme price since position entry
        self._highest_since_entry: Dict[str, float] = {}
        self._lowest_since_entry: Dict[str, float] = {}
        self._current_direction: Dict[str, int] = {}

        # track start_time of the last bar we processed to avoid double-counting
        self._last_processed_bar_start: Dict[str, Optional[str]] = {}

        # The local symbol of the active contract (e.g. OMXS30F5)
        self._contract_local_symbol: Optional[str] = None

        # shutdown flag
        self._shutting_down = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    # ------------------------------------------------------------------
    # Position helpers
    # ------------------------------------------------------------------

    def _init_contract(self, meta: Dict[str, Any]) -> None:
        """Initialise the active contract local symbol."""
        local_symbol = None
        for key, val in meta.items():
            if isinstance(val, dict) and "name" in val:
                local_symbol = val["name"]
                break

        if local_symbol and local_symbol != self._contract_local_symbol:
            self._contract_local_symbol = local_symbol
            LOGGER.info(f"Active contract identified: {self._contract_local_symbol}")

    async def _get_signed_position(self) -> int:
        """Return the signed position for the active OMXS30 future. Positive=long, negative=short."""
        if not self._contract_local_symbol:
            return 0
        try:
            async with self._http.get(
                f"{self._backend_url}/ibkr/portfolio/positions",
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for pos in data.get("positions", []):
                        if (
                            pos.get("localSymbol", "").upper()
                            == self._contract_local_symbol.upper()
                        ):
                            return int(pos.get("position", 0))
        except Exception as exc:
            LOGGER.warning(f"Failed to fetch positions from backend: {exc}")
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
        Place a brand-new standalone STOP order via the backend endpoint.
        """
        try:
            url = f"{BACKEND_URL}/ibkr/place_stop_order"
            payload = {
                "action": action,
                "volume": volume,
                "stopPrice": stop_price,
                "orderRef": "ABC_TrailingStop",
            }
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as resp:
                    body = await resp.text()
                    if resp.status == 200:
                        LOGGER.info(
                            f"Backend successfully placed {action} STOP: {body}"
                        )
                        return True
                    else:
                        LOGGER.error(
                            f"Backend /ibkr/place_stop_order failed (HTTP {resp.status}): {body}"
                        )
                        return False
        except Exception as exc:
            LOGGER.error(f"HTTP call to /ibkr/place_stop_order failed: {exc}")
            return False

    async def _cancel_all_orders_via_backend(self) -> None:
        """Cancel all active IBKR orders."""
        orders = await self._get_backend_open_orders()
        for order in orders:
            order_id = order.get("orderId")
            if order_id:
                try:
                    url = f"{BACKEND_URL}/ibkr/cancel_order"
                    payload = {"orderId": str(order_id)}
                    async with self._http.post(url, json=payload) as resp:
                        await resp.read()
                        if resp.status == 200:
                            LOGGER.info(f"Successfully cancelled order {order_id}")
                        else:
                            LOGGER.error(
                                f"Failed to cancel order {order_id}: HTTP {resp.status}"
                            )
                except Exception as exc:
                    LOGGER.error(f"Error cancelling order {order_id}: {exc}")

    async def _flatten_position_via_backend(
        self, signed_pos: int, instrument_id: str
    ) -> None:
        """Flatten current position by placing a market order in the opposite direction."""
        if signed_pos == 0:
            return
        action = "market_sell" if signed_pos > 0 else "market_buy"
        volume = abs(signed_pos)
        try:
            url = f"{BACKEND_URL}/ibkr/{action}"
            payload = {
                "instrumentId": instrument_id,
                "percentage": volume,  # Used as number_of_contracts
            }
            async with self._http.post(url, json=payload) as resp:
                body = await resp.text()
                if resp.status == 200:
                    LOGGER.info(
                        f"Successfully flattened position via {action} of {volume} contracts."
                    )
                else:
                    LOGGER.error(
                        f"Failed to flatten position via {action} (HTTP {resp.status}): {body}"
                    )
        except Exception as exc:
            LOGGER.error(f"Error flattening position: {exc}")

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
        signed_pos = await self._get_signed_position()
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
        signed_pos = await self._get_signed_position()
        volume = abs(signed_pos) if signed_pos < 0 else 1
        await self._place_stop_via_backend("BUY", rounded, volume)

    # ------------------------------------------------------------------
    # Bar processing logic
    # ------------------------------------------------------------------

    @staticmethod
    def _today_first_bar_idx(
        bars: List[Dict[str, Any]], tz_name: str = "Europe/Stockholm"
    ) -> int:
        """
        Return the index of the first bar whose ``start_time`` falls on
        today's date in the given timezone.  Falls back to 0 if parsing
        fails or all bars are from earlier days.
        """
        try:
            zone = ZoneInfo(tz_name)
        except Exception:
            zone = ZoneInfo("Europe/Stockholm")
        today = datetime.now(zone).date()

        for i, b in enumerate(bars):
            st = b.get("start_time", "")
            if not st:
                continue
            try:
                dt = datetime.fromisoformat(st)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=zone)
                if dt.astimezone(zone).date() == today:
                    return i
            except Exception:
                continue
        return 0

    def _update_extremes(
        self,
        instrument_id: str,
        bar: Dict[str, Any],
        signed_pos: int,
        is_completed: bool = False,
    ) -> None:
        """
        Track the highest-high and lowest-low since the position was entered.
        """
        current_dir = 1 if signed_pos > 0 else (-1 if signed_pos < 0 else 0)
        last_dir = self._current_direction.get(instrument_id, 0)

        # Reset tracking if direction changed or flattened
        if current_dir != last_dir:
            self._highest_since_entry.pop(instrument_id, None)
            self._lowest_since_entry.pop(instrument_id, None)
            self._abc_state.pop(instrument_id, None)
            self._current_direction[instrument_id] = current_dir

        if current_dir == 0:
            return

        current_high = float(bar["high"])
        current_low = float(bar["low"])

        # Initialize if not set (this is the entry bar)
        if instrument_id not in self._highest_since_entry:
            self._highest_since_entry[instrument_id] = current_high
            self._lowest_since_entry[instrument_id] = current_low

        if current_dir > 0:
            if current_high > self._highest_since_entry[instrument_id]:
                self._highest_since_entry[instrument_id] = current_high

        elif current_dir < 0:
            if current_low < self._lowest_since_entry[instrument_id]:
                self._lowest_since_entry[instrument_id] = current_low

    def _add_or_update_bar(self, instrument_id: str, bar: Dict[str, Any]) -> None:
        """
        Deduplicates and appends bars by start_time.
        """
        bars = self._bars.setdefault(instrument_id, [])
        bar_start = bar.get("start_time")
        if not bar_start:
            return

        for i in range(len(bars) - 1, -1, -1):
            if bars[i].get("start_time") == bar_start:
                bars[i] = bar
                return

        bars.append(bar)
        if len(bars) > 1500:
            excess = len(bars) - 1500
            del bars[:excess]
            state = self._abc_state.get(instrument_id)
            if state and state.get("A_idx") is not None:
                state["A_idx"] -= excess

    async def _on_bar_completed(self, instrument_id: str, bar: Dict[str, Any]) -> None:
        """
        Called once per 5m bar completion.

        Implements the ABC pivot-pattern trailing stop using a stateless backwards scan.
        """
        self._add_or_update_bar(instrument_id, bar)
        bars = self._bars[instrument_id]

        signed_pos = await self._get_signed_position()
        self._update_extremes(instrument_id, bar, signed_pos, is_completed=True)

        if signed_pos == 0:
            LOGGER.debug(f"[{instrument_id}] Flat position – no action.")
            self._abc_state.pop(instrument_id, None)
            return

        is_long = signed_pos > 0
        n = len(bars)

        # ── Ensure we have an A pivot ────────
        if (
            instrument_id not in self._abc_state
            or self._abc_state[instrument_id] is None
        ):
            today_idx = self._today_first_bar_idx(bars)

            if is_long:
                result = _find_nearest_pivot_low(bars, min_idx=today_idx)
                if result:
                    a_idx, a_val = result
                    self._abc_state[instrument_id] = {"A": a_val, "A_idx": a_idx}
                    LOGGER.info(
                        f"[{instrument_id}] Long ABC: A pivot low detected "
                        f"at bar idx {a_idx}, low={a_val}."
                    )
                else:
                    LOGGER.debug(
                        f"[{instrument_id}] Long ABC: no qualifying pivot low found in today's bars."
                    )
            else:
                result = _find_nearest_pivot_high(bars, min_idx=today_idx)
                if result:
                    a_idx, a_val = result
                    self._abc_state[instrument_id] = {"A": a_val, "A_idx": a_idx}
                    LOGGER.info(
                        f"[{instrument_id}] Short ABC: A pivot high detected "
                        f"at bar idx {a_idx}, high={a_val}."
                    )
                else:
                    LOGGER.debug(
                        f"[{instrument_id}] Short ABC: no qualifying pivot high found in today's bars."
                    )
            return

        state = self._abc_state[instrument_id]
        a_val = state["A"]
        a_idx = state["A_idx"]

        # ── Scan backwards for C and B ────────
        if is_long:
            c_result = None
            for idx in range(n - 1 - PIVOT_RIGHT, a_idx, -1):
                if _is_pivot_low_at(bars, idx):
                    if float(bars[idx]["low"]) > a_val:
                        c_result = (idx, float(bars[idx]["low"]))
                        break

            if c_result:
                c_idx, c_val = c_result
                b_result = None
                for idx in range(c_idx - 1, a_idx, -1):
                    if _is_pivot_high_at(bars, idx):
                        b_result = (idx, float(bars[idx]["high"]))
                        break

                if b_result:
                    b_idx, b_val = b_result
                    current_close = float(bars[-1]["close"])
                    if current_close > b_val:
                        LOGGER.info(
                            f"[{instrument_id}] Long ABC CONFIRMED: bar close {current_close} > "
                            f"B={b_val} (idx {b_idx}). Moving SELL STOP to C={c_val} (idx {c_idx})."
                        )
                        stop_price = round(c_val - TICK_SIZE, 2)
                        await self._place_sell_stop(stop_price)

                        state["A"] = c_val
                        state["A_idx"] = c_idx
                        LOGGER.info(
                            f"[{instrument_id}] Long ABC: chaining → new A={c_val} (idx {c_idx})."
                        )
        else:
            c_result = None
            for idx in range(n - 1 - PIVOT_RIGHT, a_idx, -1):
                if _is_pivot_high_at(bars, idx):
                    if float(bars[idx]["high"]) < a_val:
                        c_result = (idx, float(bars[idx]["high"]))
                        break

            if c_result:
                c_idx, c_val = c_result
                b_result = None
                for idx in range(c_idx - 1, a_idx, -1):
                    if _is_pivot_low_at(bars, idx):
                        b_result = (idx, float(bars[idx]["low"]))
                        break

                if b_result:
                    b_idx, b_val = b_result
                    current_close = float(bars[-1]["close"])
                    if current_close < b_val:
                        LOGGER.info(
                            f"[{instrument_id}] Short ABC CONFIRMED: bar close {current_close} < "
                            f"B={b_val} (idx {b_idx}). Moving BUY STOP to C={c_val} (idx {c_idx})."
                        )
                        stop_price = round(c_val + TICK_SIZE, 2)
                        await self._place_buy_stop(stop_price)

                        state["A"] = c_val
                        state["A_idx"] = c_idx
                        LOGGER.info(
                            f"[{instrument_id}] Short ABC: chaining → new A={c_val} (idx {c_idx})."
                        )

    async def _on_bar_update(self, instrument_id: str, bar: Dict[str, Any]) -> None:
        """
        Called on every in-progress bar update (type='update').
        """
        self._add_or_update_bar(instrument_id, bar)

        signed_pos = await self._get_signed_position()
        self._update_extremes(instrument_id, bar, signed_pos)

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
                    await self._on_bar_update(instrument_id, bar)

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

    async def _fetch_metadata(self) -> Dict[str, Any]:
        """Fetch metadata for the active instrument asynchronously."""
        from pacavanza.utils.utils import fetch_active_omxs30_future

        loop = asyncio.get_running_loop()
        try:
            # run_in_executor to avoid blocking the event loop
            data = await loop.run_in_executor(None, fetch_active_omxs30_future)
            return data.get("OMXS30", {})
        except Exception as exc:
            LOGGER.warning(f"Error fetching metadata: {exc}")
            return {}

    def _is_outside_market_hours(self, meta: Dict[str, Any]) -> bool:
        tz_name = meta.get("timezone", "Europe/Stockholm")
        open_str = meta.get("market_open", "09:00")
        close_str = meta.get("market_close", "17:45")

        try:
            zone = ZoneInfo(tz_name)
        except Exception:
            zone = timezone.utc

        try:
            oh, om = (int(x) for x in open_str.split(":"))
            ch, cm = (int(x) for x in close_str.split(":"))
        except Exception:
            return False

        now_local = datetime.now(zone)
        if now_local.weekday() >= 5:
            return True

        t = (now_local.hour, now_local.minute)
        return t < (oh, om) or t >= (ch, cm)

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    async def _run_async(self) -> None:
        """Top-level async runner with reconnect loop."""
        # Open a shared aiohttp session for backend HTTP calls
        self._http = aiohttp.ClientSession()

        async def _eod_flatten_monitor():
            """Monitor time and force close all positions at 17:25 CEST."""
            last_flattened_date = None
            while not self._shutting_down:
                try:
                    meta = await self._fetch_metadata()
                    tz_name = meta.get("timezone", "Europe/Stockholm")
                    try:
                        zone = ZoneInfo(tz_name)
                    except Exception:
                        zone = timezone.utc

                    now_local = datetime.now(zone)
                    # Check if it's 17:25
                    if now_local.hour == 17 and now_local.minute == 25:
                        current_date = now_local.date()
                        if last_flattened_date != current_date:
                            LOGGER.info(
                                "EOD Flattening Triggered: Cancelling all orders and flattening position..."
                            )
                            last_flattened_date = current_date

                            await self._cancel_all_orders_via_backend()
                            signed_pos = await self._get_signed_position()
                            LOGGER.info(
                                f"EOD Flattening: signed_pos={signed_pos}, local_symbol={self._contract_local_symbol}"
                            )
                            if signed_pos != 0 and self._contract_local_symbol:
                                instrument_id = self._contract_local_symbol.lower()
                                await self._flatten_position_via_backend(
                                    signed_pos, instrument_id
                                )
                            else:
                                LOGGER.info(
                                    "EOD Flattening: signed_pos is 0 or local_symbol is missing. No market order placed."
                                )
                except Exception as exc:
                    LOGGER.error(f"Error in EOD flatten monitor: {exc}")

                # Sleep for 30 seconds to avoid high CPU and check frequently enough
                await asyncio.sleep(30)

        # Start the EOD flatten background task
        asyncio.create_task(_eod_flatten_monitor())

        try:
            while not self._shutting_down:
                try:
                    meta = await self._fetch_metadata()
                    self._init_contract(meta)
                except Exception as exc:
                    LOGGER.warning(f"Failed to fetch metadata: {exc}")
                    meta = {}

                if self._is_outside_market_hours(meta):
                    LOGGER.info("Outside trading hours. Sleeping for 1 minute...")
                    if not self._shutting_down:
                        await asyncio.sleep(60)
                    continue

                try:
                    await self._listen()
                except Exception as exc:
                    err_msg = str(exc).lower()
                    if "timeout" in err_msg:
                        LOGGER.info(
                            f"Redis listener idle timeout. Reconnecting in 5 s …"
                        )
                    else:
                        LOGGER.error(
                            f"Redis listener error: {exc}. Reconnecting in 5 s …"
                        )
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
