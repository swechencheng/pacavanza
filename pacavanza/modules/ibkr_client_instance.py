"""
ibkr_client_instance.py

Unified IBKR client instance management (process-local singleton).

Provides:
  - resolve_ibkr_local_symbol()  – quick sync connect/qualify/disconnect
                                    to resolve the active future's localSymbol.
                                    Uses clientId+1 to avoid clashing with
                                    the persistent connection.
  - init_ibkr_async()            – async connect + qualify for use inside
                                    an asyncio event loop (e.g. Uvicorn lifespan).
  - ensure_connected_async()     – reconnect with exponential backoff
                                    (5 s → 10 s → … → 300 s cap).
  - get_ibkr_instance()          – returns the shared (IB, ContFuture) tuple.
  - get_ib() / get_contract()    – convenience accessors.
  - register_reconnect_callback()– hook called after every successful reconnect.
  - disconnect()                 – clean shutdown.

Since daemon_controller.py uses multiprocessing with spawn, each child
process gets its own memory space and therefore its own singleton instance.
"""

import asyncio
import logging
from typing import Callable, List, Optional, Tuple

from ib_async import IB, ContFuture

from pacavanza.config import IBKR_HOST, IBKR_PORT, IBKR_CLIENT_ID

LOGGER = logging.getLogger("ibkr_client")

# ── process-local singleton state ────────────────────────────────────────
_ib: Optional[IB] = None
_contract: Optional[ContFuture] = None
_contract_symbol: str = "OMXS30"
_contract_exchange: str = "OMS"

# ── reconnection state ──────────────────────────────────────────────────
_INITIAL_RETRY_DELAY: float = 5.0
_MAX_RETRY_DELAY: float = 300.0
_retry_delay: float = _INITIAL_RETRY_DELAY

# ── reconnect hooks ─────────────────────────────────────────────────────
_reconnect_callbacks: List[Callable] = []


# ─────────────────────────────────────────────────────────────────────────
# Quick symbol resolution (sync, transient connection)
# ─────────────────────────────────────────────────────────────────────────


def resolve_ibkr_local_symbol(
    symbol: str = "OMXS30",
    exchange: str = "OMS",
    timeout: float = 2.0,
) -> Optional[str]:
    """
    Open a short-lived synchronous IB connection, qualify the continuous
    future contract, extract its ``localSymbol`` (e.g. ``OMXS30F5``), and
    disconnect immediately.

    Uses ``IBKR_CLIENT_ID + 1`` so it never clashes with the persistent
    connection held by the backend.

    Returns the localSymbol string, or *None* on failure.
    """
    ib = IB()
    try:
        ib.connect(IBKR_HOST, IBKR_PORT, clientId=IBKR_CLIENT_ID + 1, timeout=timeout)
        c = ContFuture(symbol, exchange)
        ib.qualifyContracts(c)
        local_symbol = c.localSymbol
        ib.disconnect()
        LOGGER.info(f"Resolved IBKR local symbol: {local_symbol}")
        return local_symbol
    except Exception as e:
        LOGGER.warning(
            f"Failed to resolve IBKR local symbol (fallback to Avanza roll logic): {e}"
        )
        try:
            ib.disconnect()
        except Exception:
            pass
        return None


# ─────────────────────────────────────────────────────────────────────────
# Async persistent connection
# ─────────────────────────────────────────────────────────────────────────


async def init_ibkr_async(
    symbol: str = "OMXS30",
    exchange: str = "OMS",
) -> Tuple[IB, ContFuture]:
    """
    Create (or re-use) the singleton IB instance and connect **async**.

    Must be called from an active asyncio event loop (e.g. inside a
    Uvicorn lifespan context manager).

    Returns ``(ib, contract)``.
    """
    global _ib, _contract, _contract_symbol, _contract_exchange, _retry_delay

    _contract_symbol = symbol
    _contract_exchange = exchange

    if _ib is not None and _ib.isConnected():
        return _ib, _contract

    # Clean up stale instance
    if _ib is not None:
        try:
            _ib.disconnect()
        except Exception:
            pass

    _ib = IB()
    _contract = ContFuture(symbol, exchange)

    await _ib.connectAsync(IBKR_HOST, IBKR_PORT, clientId=IBKR_CLIENT_ID)
    await _ib.qualifyContractsAsync(_contract)
    _retry_delay = _INITIAL_RETRY_DELAY

    LOGGER.info(
        f"IBKR async connected: host={IBKR_HOST}, port={IBKR_PORT}, "
        f"clientId={IBKR_CLIENT_ID}, contract={_contract.localSymbol}"
    )
    return _ib, _contract


# ─────────────────────────────────────────────────────────────────────────
# Reconnection with exponential backoff
# ─────────────────────────────────────────────────────────────────────────


async def ensure_connected_async() -> bool:
    """
    If the connection is alive, return ``True`` immediately.

    Otherwise sleep for the current back-off delay, attempt to reconnect
    and re-qualify the contract.  On success the delay resets to 5 s and
    all registered ``_reconnect_callbacks`` are invoked.  On failure the
    delay doubles (capped at 300 s / 5 min).

    Returns ``True`` on a healthy connection, ``False`` if the reconnect
    attempt failed.
    """
    global _retry_delay

    if _ib is None:
        return False

    if _ib.isConnected():
        return True

    # ── disconnected → attempt reconnect ──
    LOGGER.warning(f"IBKR connection lost, reconnecting in {_retry_delay:.0f}s…")
    await asyncio.sleep(_retry_delay)

    try:
        try:
            _ib.disconnect()
        except Exception:
            pass

        await asyncio.sleep(1)
        await _ib.connectAsync(IBKR_HOST, IBKR_PORT, clientId=IBKR_CLIENT_ID)
        await _ib.qualifyContractsAsync(_contract)

        LOGGER.info("IBKR reconnected successfully.")
        _retry_delay = _INITIAL_RETRY_DELAY

        # invoke reconnect callbacks
        for cb in _reconnect_callbacks:
            try:
                cb()
            except Exception as exc:
                LOGGER.warning(f"Reconnect callback error: {exc}")

        return True
    except Exception as exc:
        LOGGER.debug(f"IBKR reconnect failed: {exc}")
        _retry_delay = min(_retry_delay * 2, _MAX_RETRY_DELAY)
        return False


# ─────────────────────────────────────────────────────────────────────────
# Accessor helpers
# ─────────────────────────────────────────────────────────────────────────


def register_reconnect_callback(callback: Callable) -> None:
    """Register a function to call after every successful reconnect."""
    _reconnect_callbacks.append(callback)


def get_ibkr_instance() -> Tuple[IB, ContFuture]:
    """Return the shared ``(IB, ContFuture)`` tuple.

    Raises ``RuntimeError`` if ``init_ibkr_async()`` has not been called.
    """
    if _ib is None:
        raise RuntimeError("IBKR client not initialised. Call init_ibkr_async() first.")
    return _ib, _contract


def get_ib() -> IB:
    """Return the shared IB instance."""
    return get_ibkr_instance()[0]


def get_contract() -> ContFuture:
    """Return the shared ContFuture contract."""
    return get_ibkr_instance()[1]


def is_connected() -> bool:
    """Return ``True`` when the singleton IB connection is alive."""
    return _ib is not None and _ib.isConnected()


def disconnect() -> None:
    """Disconnect the singleton IB client and clear state."""
    global _ib, _contract
    if _ib is not None:
        try:
            _ib.disconnect()
        except Exception:
            pass
    _ib = None
    _contract = None
    _reconnect_callbacks.clear()
