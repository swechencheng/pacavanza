import asyncio
import json
import logging
from typing import Dict, Any, List, AsyncIterator
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request
from fastapi.responses import JSONResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
import uvicorn
import redis.asyncio as aioredis
from datetime import datetime, timezone
from collections import defaultdict
from pathlib import Path
from contextlib import asynccontextmanager
from .modules.avanza_trading import AvanzaTrading
from .utils.utils import flatten_instrument_list, fetch_active_omxs30_future
from .config import IBKR_PORT

# compute pacavanza package root (pacavanza/)
ROOT = Path(__file__).resolve().parent  # pacavanza/
STATIC_DIR = ROOT / "static"
STATIC_HTML = STATIC_DIR / "chart.html"
STATIC_AVA_HTML = STATIC_DIR / "ava/chart.html"
STATIC_PORTFOLIO_HTML = STATIC_DIR / "portfolio/index.html"

from .indicators.indicators import (
    incremental_ema_update,
)

logging.basicConfig(level=logging.INFO)
logging.getLogger("main").setLevel(logging.INFO)
LOGGER = logging.getLogger("main")


class WebSocketManager:
    def __init__(self):
        self._conns: List[WebSocket] = []
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self._lock:
            self._conns.append(ws)
        LOGGER.info("WebSocket connected (total=%d)", len(self._conns))

    async def disconnect(self, ws: WebSocket):
        async with self._lock:
            if ws in self._conns:
                self._conns.remove(ws)
        LOGGER.info("WebSocket disconnected (total=%d)", len(self._conns))

    async def broadcast(self, message: Dict[str, Any]):
        text = json.dumps(message, default=str)
        async with self._lock:
            conns = list(self._conns)
        for ws in conns:
            try:
                await ws.send_text(text)
            except Exception:
                LOGGER.debug(
                    "Failed to send ws message, removing connection", exc_info=True
                )
                try:
                    await self.disconnect(ws)
                except Exception:
                    pass


def create_app(
    redis_url="redis://localhost:6379/0",
    redis_channels=["pacavanza:ticker_updates", "pacavanza:future_updates"],
    static_html_path: str | Path = STATIC_HTML,
    ibkr_conn=None,
):
    """
    Create FastAPI app. Uses lifespan async context manager to start/stop background
    Redis subscriber task and to close the redis client cleanly (uses aclose()).

    Args:
        ibkr_conn: Optional (ib, contract) tuple from ib_async for IBKR trading.
    """
    # redis client (async)
    redis_client = aioredis.from_url(redis_url)

    manager = WebSocketManager()

    # in-memory per-instrument state (keeps recent history for EMA calculation).
    # For many symbols or long history you might want to persist this or cap size.
    recent_bars: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    # per-instrument locks map and map lock to protect creation
    recent_bars_locks: Dict[str, asyncio.Lock] = {}
    recent_bars_locks_map_lock = asyncio.Lock()

    # metadata and per-instrument locks + map lock
    metadata: Dict[str, Dict[str, Any]] = {}
    metadata_locks: Dict[str, asyncio.Lock] = {}
    metadata_locks_map_lock = asyncio.Lock()

    # ema_state[instrument][length] = last EMA value
    ema_state: Dict[str, Dict[int, float]] = defaultdict(dict)

    # ── Ava mini futures (used by /ava chart) ────────────────────────────────
    # raw_ava_mini_list: hierarchical JSON served to /ava frontend for asset selector
    # instrument_list: flattened dict used internally for trading/history lookups
    raw_ava_mini_list: Dict[str, Any] = {}
    instrument_list: Dict[str, Any] = {}
    try:
        with open(ROOT / "ava_mini_future_list.json", "r", encoding="utf-8") as f:
            raw_list = json.load(f)
            raw_ava_mini_list.update(raw_list)
            instrument_list.update(flatten_instrument_list(raw_list))
    except Exception as e:
        LOGGER.warning("Could not load ava_mini_future_list.json: %s", e)

    # ── Active OMXS30 future (used by / chart only) ───────────────────────────
    # Completely independent of ava_mini_future_list.json.
    # active_future_info: { key, name, orderbookId, timezone, market_open, market_close }
    active_future_info: Dict[str, Any] = {}

    ibkr_local_symbol = None
    if ibkr_conn is not None:
        try:
            from pacavanza.config import IBKR_PORT
            from ib_async import IB

            temp_ib = IB()
            temp_ib.connect("127.0.0.1", IBKR_PORT, clientId=198, timeout=2.0)
            temp_ib.qualifyContracts(ibkr_conn[1])
            ibkr_local_symbol = ibkr_conn[1].localSymbol
            temp_ib.disconnect()
        except Exception as e:
            LOGGER.warning(f"Could not qualify IBKR contract in create_app: {e}")
            try:
                temp_ib.disconnect()
            except:
                pass

    try:
        raw_active = fetch_active_omxs30_future(target_name=ibkr_local_symbol)
        flat_active = flatten_instrument_list(raw_active)
        if flat_active:
            key, meta = next(iter(flat_active.items()))
            active_future_info = {"key": key, **meta}
            # also register in instrument_list so /history and trading endpoints can serve it
            instrument_list.update(flat_active)
            LOGGER.info(f"Loaded active future: {key} ({meta.get('name', '')})")
    except Exception as e:
        LOGGER.warning("Could not fetch active future: %s", e)

    # ── Fill markers persistence ──────────────────────────────────────────
    def get_fills_file_path() -> str:
        """Get the dynamic fills file path based on the active future."""
        name = active_future_info.get("name", "unknown")
        return f"fills_{name}.json"

    def _load_fills() -> List[Dict[str, Any]]:
        """Load persisted fills from disk for the current contract."""
        file_path = get_fills_file_path()
        try:
            with open(file_path, "r") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return []

    def _save_fill(fill_record: Dict[str, Any]) -> None:
        """Append a fill record to the persistent JSON file for the current contract."""
        fills = _load_fills()
        # Deduplicate by execId
        if any(f.get("execId") == fill_record.get("execId") for f in fills):
            return
        fills.append(fill_record)
        file_path = get_fills_file_path()
        with open(file_path, "w") as f:
            json.dump(fills, f, default=str)

    recent_bars = {sid: [] for sid in instrument_list.keys()}

    for sid in recent_bars:
        data_file = f"ohlc_{sid}.json"
        try:
            with open(data_file, "r") as f:
                data = json.load(f)
            for bar in data:
                start = datetime.fromisoformat(bar["start_time"])
                end = datetime.fromisoformat(bar["end_time"])
                if start.tzinfo is None:
                    start = start.replace(tzinfo=timezone.utc)
                else:
                    start = start.astimezone(timezone.utc)
                if end.tzinfo is None:
                    end = end.replace(tzinfo=timezone.utc)
                else:
                    end = end.astimezone(timezone.utc)
                bar["start_time"] = start
                bar["end_time"] = end
                recent_bars[sid].append(bar)
            LOGGER.info(f"[{sid}] Loaded {len(recent_bars[sid])} bars from {data_file}")
        except FileNotFoundError:
            LOGGER.info(f"[{sid}] No previous data file {data_file}.")
        except Exception as e:
            LOGGER.error(f"[{sid}] Failed to load OHLC data: {e}")

    # instantiate AvanzaTrading with references to in-memory state and the lock maps
    try:
        trading = AvanzaTrading(
            recent_bars,
            instrument_list,
            metadata,
            LOGGER,
            bars_locks_map=recent_bars_locks,
            metadata_locks_map=metadata_locks,
            bars_map_lock=recent_bars_locks_map_lock,
            metadata_map_lock=metadata_locks_map_lock,
        )
    except Exception as e:
        LOGGER.warning(f"Failed to initialize AvanzaTrading: {e}")
        trading = None

    # instantiate IbkrTrading (if IBKR connection is available)
    # IbkrTrading gets its OWN recent_bars (not shared with Avanza redis subscriber)
    # so that Avanza SSE price updates don't corrupt IBKR bar data.
    ibkr_trading = None
    ibkr_portfolio = None
    if ibkr_conn is not None:
        try:
            from pacavanza.modules.ibkr_trading import IbkrTrading

            ib, contract = ibkr_conn

            # Build separate bar storage for the active future only
            ibkr_recent_bars: Dict[str, List[Dict[str, Any]]] = {}
            if active_future_info:
                future_key = active_future_info.get("key")
                if future_key:
                    # RTH filter: only keep bars within market hours
                    from zoneinfo import ZoneInfo

                    tz_name = active_future_info.get("timezone", "Europe/Stockholm")
                    market_open_str = active_future_info.get("market_open", "09:00")
                    market_close_str = active_future_info.get("market_close", "17:45")
                    zone = ZoneInfo(tz_name)
                    oh, om = (int(x) for x in market_open_str.split(":"))
                    ch, cm = (int(x) for x in market_close_str.split(":"))

                    def _is_rth(utc_time):
                        """Return True if utc_time is within regular trading hours."""
                        local = utc_time.astimezone(zone)
                        t = (local.hour, local.minute)
                        return (oh, om) <= t < (ch, cm)

                    ibkr_recent_bars[future_key] = []
                    data_file = f"ohlc_{future_key}.json"
                    skipped = 0
                    try:
                        with open(data_file, "r") as f:
                            data = json.load(f)
                        for bar in data:
                            start = datetime.fromisoformat(bar["start_time"])
                            end = datetime.fromisoformat(bar["end_time"])
                            if start.tzinfo is None:
                                start = start.replace(tzinfo=timezone.utc)
                            else:
                                start = start.astimezone(timezone.utc)
                            if end.tzinfo is None:
                                end = end.replace(tzinfo=timezone.utc)
                            else:
                                end = end.astimezone(timezone.utc)
                            bar["start_time"] = start
                            bar["end_time"] = end
                            # Skip bars outside RTH
                            if not _is_rth(start):
                                skipped += 1
                                continue
                            # Skip dirty in-progress bars (duration >> interval)
                            if (end - start).total_seconds() > 600:
                                skipped += 1
                                continue
                            ibkr_recent_bars[future_key].append(bar)
                        LOGGER.info(
                            f"[IBKR] Loaded {len(ibkr_recent_bars[future_key])} "
                            f"RTH bars for {future_key} "
                            f"(filtered {skipped} out-of-hours)"
                        )
                    except FileNotFoundError:
                        LOGGER.info(f"[IBKR] No data file {data_file}")
                    except Exception as e:
                        LOGGER.error(f"[IBKR] Failed to load bars: {e}")

            # Separate locks for IBKR bars
            ibkr_bars_locks: Dict[str, asyncio.Lock] = {}
            ibkr_bars_locks_map_lock = asyncio.Lock()
            ibkr_metadata: Dict[str, Dict[str, Any]] = {}
            ibkr_metadata_locks: Dict[str, asyncio.Lock] = {}
            ibkr_metadata_locks_map_lock = asyncio.Lock()

            # We will initialize IbkrTrading inside the lifespan context manager
            # so it binds to the correct Uvicorn asyncio event loop.
        except Exception as e:
            LOGGER.warning(f"Failed to setup IBKR dependencies: {e}")

    # Background task: subscribe to redis channel and forward events
    async def _redis_subscriber_task():
        LOGGER.info("Starting Redis subscriber task")
        try:
            pubsub = redis_client.pubsub()
            await pubsub.subscribe(*redis_channels)
            LOGGER.info(f"Subscribed to Redis channels: {redis_channels}")

            # Test message to verify Redis is working
            test_msg = {"type": "test", "message": "Redis connection established"}
            for channel in redis_channels:
                await redis_client.publish(channel, json.dumps(test_msg))

            while True:
                msg = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=60.0
                )
                if msg is None:
                    # Timeout reached without messages. Send a ping to keep connection alive.
                    try:
                        await pubsub.ping()
                    except Exception:
                        pass
                    continue

                if msg and msg.get("type") == "message":
                    data = msg["data"]
                    LOGGER.debug(f"Redis subscriber received raw message: {data}")

                    if isinstance(data, (bytes, bytearray)):
                        try:
                            payload = json.loads(data.decode("utf-8"))
                            LOGGER.debug(f"Decoded Redis message: {payload}")
                        except Exception as e:
                            LOGGER.error(
                                f"Invalid JSON from redis (bytes): {e}, data: {data}"
                            )
                            continue
                    elif isinstance(data, str):
                        try:
                            payload = json.loads(data)
                            LOGGER.debug(f"Decoded Redis message: {payload}")
                        except Exception as e:
                            LOGGER.error(
                                f"Invalid JSON from redis (str): {e}, data: {data}"
                            )
                            continue
                    else:
                        LOGGER.warning(f"Unexpected message type: {type(data)}")
                        continue

                    # Handle test message
                    if payload.get("type") == "test":
                        LOGGER.info(
                            f"Redis test message received: {payload.get('message')}"
                        )
                        continue

                    try:
                        await _handle_redis_payload(payload)
                    except Exception as e:
                        LOGGER.exception(f"Error handling redis payload: {e}")

        except asyncio.CancelledError:
            LOGGER.info("Redis subscriber task cancelled (normal shutdown)")
            return  # Do not re-raise — clean exit
        except Exception as e:
            LOGGER.exception(f"Redis subscriber error: {e}")
            # Try to reconnect after delay
            await asyncio.sleep(5)
            # Restart the subscriber task
            asyncio.create_task(_redis_subscriber_task())
        finally:
            try:
                await pubsub.unsubscribe(*redis_channels)
            except Exception:
                pass
            try:
                await pubsub.aclose()
            except Exception:
                pass

    # helper to get/create per-instrument bars lock (used by redis handler)
    async def _get_bars_lock_for(sid: str) -> asyncio.Lock:
        lock = recent_bars_locks.get(sid)
        if lock:
            return lock
        async with recent_bars_locks_map_lock:
            lock = recent_bars_locks.get(sid)
            if not lock:
                lock = asyncio.Lock()
                recent_bars_locks[sid] = lock
            return lock

    # helper to get/create per-instrument metadata lock (used by redis handler)
    async def _get_metadata_lock_for(sid: str) -> asyncio.Lock:
        lock = metadata_locks.get(sid)
        if lock:
            return lock
        async with metadata_locks_map_lock:
            lock = metadata_locks.get(sid)
            if not lock:
                lock = asyncio.Lock()
                metadata_locks[sid] = lock
            return lock

    async def _handle_redis_payload(payload: Dict[str, Any]):
        """
        Process messages from Redis, keep in-memory state for EMA/labels and
        broadcast enriched messages to websocket clients.
        """
        LOGGER.debug("Handling redis payload: %s", payload)
        mtype = payload.get("type")
        sid = payload.get("instrument")
        if not sid:
            return

        # store metadata if provided (use per-instrument lock)
        if payload.get("meta"):
            meta_lock = await _get_metadata_lock_for(sid)
            async with meta_lock:
                metadata[sid] = payload["meta"]

        if mtype == "update":
            # in-progress/current bar updates
            bar = payload.get("bar")
            if not bar:
                return
            # use start_time as canonical timestamp
            ts = datetime.fromisoformat(bar["start_time"])
            # append/replace last bar in recent_bars (keep them bounded)
            # Use per-instrument lock to avoid concurrent mutation while trading reads snapshots
            bars_lock = await _get_bars_lock_for(sid)
            async with bars_lock:
                lst = recent_bars[sid]
                if not lst or lst[-1]["start_time"] != ts:
                    lst.append(
                        {
                            "start_time": ts,
                            "end_time": datetime.fromisoformat(bar["end_time"]),
                            "open": bar["open"],
                            "high": bar["high"],
                            "low": bar["low"],
                            "close": bar["close"],
                            "volume": bar.get("volume", 0),
                        }
                    )
                else:
                    # replace last (update)
                    lst[-1].update(
                        {
                            "open": bar["open"],
                            "high": bar["high"],
                            "low": bar["low"],
                            "close": bar["close"],
                            "volume": bar.get("volume", 0),
                        }
                    )
                # limit history length (e.g. 1500 bars)
                if len(lst) > 1500:
                    lst[:] = lst[-1500:]

            # Also mirror to ibkr_recent_bars if this is the active future
            if ibkr_trading is not None and sid in ibkr_trading.recent_bars:
                ibkr_lst = ibkr_trading.recent_bars[sid]
                if not ibkr_lst or ibkr_lst[-1]["start_time"] != ts:
                    ibkr_lst.append(
                        {
                            "start_time": ts,
                            "end_time": datetime.fromisoformat(bar["end_time"]),
                            "open": bar["open"],
                            "high": bar["high"],
                            "low": bar["low"],
                            "close": bar["close"],
                            "volume": bar.get("volume", 0),
                        }
                    )
                else:
                    ibkr_lst[-1].update(
                        {
                            "open": bar["open"],
                            "high": bar["high"],
                            "low": bar["low"],
                            "close": bar["close"],
                            "volume": bar.get("volume", 0),
                        }
                    )
                if len(ibkr_lst) > 1500:
                    ibkr_lst[:] = ibkr_lst[-1500:]

            # compute incremental EMA updates (fast) - uses the global recent_bars dict; reading latest snapshot is fine
            emas = {}
            for L in (20, 50, 100, 220):
                prev = ema_state[sid].get(L)
                new = incremental_ema_update(
                    prev, bar["close"], L, historical_buffer=recent_bars[sid]
                )
                if new is not None:
                    ema_state[sid][L] = new
                    emas[str(L)] = {"time": ts, "value": new}

            out = {
                "type": "update",
                "instrument": sid,
                "bar": bar,
                "emas": emas,
            }
            await manager.broadcast(out)

        elif mtype == "completed":
            # a completed bar (append to history)
            bar = payload.get("bar")
            if not bar:
                return
            # use start_time as canonical timestamp
            ts = datetime.fromisoformat(bar["start_time"])
            # convert and append under per-instrument lock
            bars_lock = await _get_bars_lock_for(sid)
            async with bars_lock:
                lst = recent_bars[sid]
                for i in range(len(lst) - 1, -1, -1):
                    b = lst[i]
                    start = b["start_time"]
                    if start.tzinfo is None:
                        start = start.replace(tzinfo=timezone.utc)
                    if start == ts:
                        lst[i].update(
                            {
                                "start_time": ts,
                                "end_time": datetime.fromisoformat(bar["end_time"]),
                                "open": bar["open"],
                                "high": bar["high"],
                                "low": bar["low"],
                                "close": bar["close"],
                                "volume": bar.get("volume", 0),
                            }
                        )
                        break
                # limit history length (e.g. 1500 bars)
                if len(lst) > 1500:
                    lst[:] = lst[-1500:]

            # Also mirror completed bar to ibkr_recent_bars
            if ibkr_trading is not None and sid in ibkr_trading.recent_bars:
                ibkr_lst = ibkr_trading.recent_bars[sid]
                found = False
                for i in range(len(ibkr_lst) - 1, -1, -1):
                    b = ibkr_lst[i]
                    bstart = b["start_time"]
                    if bstart.tzinfo is None:
                        bstart = bstart.replace(tzinfo=timezone.utc)
                    if bstart == ts:
                        ibkr_lst[i].update(
                            {
                                "start_time": ts,
                                "end_time": datetime.fromisoformat(bar["end_time"]),
                                "open": bar["open"],
                                "high": bar["high"],
                                "low": bar["low"],
                                "close": bar["close"],
                                "volume": bar.get("volume", 0),
                            }
                        )
                        found = True
                        break
                if not found:
                    ibkr_lst.append(
                        {
                            "start_time": ts,
                            "end_time": datetime.fromisoformat(bar["end_time"]),
                            "open": bar["open"],
                            "high": bar["high"],
                            "low": bar["low"],
                            "close": bar["close"],
                            "volume": bar.get("volume", 0),
                        }
                    )
                if len(ibkr_lst) > 1500:
                    ibkr_lst[:] = ibkr_lst[-1500:]

            # recompute EMAs using incremental update with the finalized close
            emas = {}
            for L in (20, 50, 100, 220):
                prev = ema_state[sid].get(L)
                new = incremental_ema_update(
                    prev, bar["close"], L, historical_buffer=recent_bars[sid]
                )
                if new is not None:
                    ema_state[sid][L] = new
                    emas[str(L)] = {"time": bar["start_time"], "value": new}

            out = {
                "type": "completed",
                "instrument": sid,
                "bar": bar,
                "emas": emas,
            }
            await manager.broadcast(out)

        elif mtype in ("depth", "trade"):
            # Order depth (tape) or trades — pass through to WebSocket clients, no storage
            await manager.broadcast(payload)

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

    # Background task: pump ib_async event loop so openTrades()/events stay current
    async def _ibkr_event_pump():
        """Periodically pump ib_async event loop to process TWS messages."""
        if ibkr_trading is None:
            return
        ib = ibkr_trading.ib
        LOGGER.info("Starting IBKR event pump task")
        retry_delay = 5
        max_delay = 300
        last_conn_state = None
        last_trading_disabled = None
        try:
            while True:
                try:
                    curr_conn_state = ib.isConnected()
                    target_local_symbol = (
                        ibkr_trading.contract.localSymbol
                        if (ibkr_trading and getattr(ibkr_trading, "contract", None))
                        else None
                    )
                    curr_trading_disabled = check_daily_loss_limit(
                        ibkr_portfolio, target_local_symbol
                    )
                    if (
                        curr_conn_state != last_conn_state
                        or curr_trading_disabled != last_trading_disabled
                    ):
                        last_conn_state = curr_conn_state
                        last_trading_disabled = curr_trading_disabled
                        await manager.broadcast(
                            {
                                "type": "ibkr_status",
                                "connected": curr_conn_state,
                                "trading_disabled": curr_trading_disabled,
                            }
                        )

                    if not curr_conn_state:
                        LOGGER.warning(
                            f"IBKR connection lost, reconnecting in {retry_delay}s..."
                        )
                        await asyncio.sleep(retry_delay)
                        try:
                            ib.disconnect()
                        except Exception:
                            pass
                        await asyncio.sleep(1)
                        await ib.connectAsync("127.0.0.1", IBKR_PORT, clientId=51)
                        await ib.qualifyContractsAsync(ibkr_trading.contract)
                        LOGGER.info("IBKR reconnected successfully.")
                        retry_delay = 5  # reset on success
                        if hasattr(ibkr_trading, "_setup_trade_subscription"):
                            ibkr_trading._setup_trade_subscription(
                                on_change_callback=_on_order_change,
                                on_fill_callback=_on_fill,
                            )
                    else:
                        ib.sleep(0)  # process pending IB events without blocking
                except Exception as e:
                    LOGGER.debug(f"IBKR event pump/reconnect error: {e}")
                    if not ib.isConnected():
                        retry_delay = min(retry_delay * 2, max_delay)
                await asyncio.sleep(0.1)  # 100ms cycle
        except asyncio.CancelledError:
            LOGGER.info("IBKR event pump task cancelled (normal shutdown)")

    # Lifespan context manager: start subscriber on startup and close on shutdown
    @asynccontextmanager
    async def lifespan(app) -> AsyncIterator[None]:
        nonlocal ibkr_trading, ibkr_portfolio

        # Suppress noisy asyncio websocket ConnectionClosedError in shielded futures
        loop = asyncio.get_event_loop()

        def custom_exception_handler(loop, context):
            msg = context.get("message", "")
            if "ConnectionClosedError exception in shielded future" in msg:
                return
            exc = context.get("exception")
            if exc and "keepalive ping timeout" in str(exc):
                return
            loop.default_exception_handler(context)

        loop.set_exception_handler(custom_exception_handler)

        # Connect IBKR async inside the correct event loop
        if ibkr_conn is not None:
            ib, contract = ibkr_conn
            try:
                await ib.connectAsync("127.0.0.1", IBKR_PORT, clientId=51)
                await ib.qualifyContractsAsync(contract)
                LOGGER.info("IBKR async connected inside lifespan.")

                ibkr_trading = IbkrTrading(
                    ib=ib,
                    contract=contract,
                    recent_bars=ibkr_recent_bars,
                    instrument_list=instrument_list,
                    metadata=ibkr_metadata,
                    logger=LOGGER,
                    bars_locks_map=ibkr_bars_locks,
                    metadata_locks_map=ibkr_metadata_locks,
                    bars_map_lock=ibkr_bars_locks_map_lock,
                    metadata_map_lock=ibkr_metadata_locks_map_lock,
                )
                if hasattr(ibkr_trading, "_setup_trade_subscription"):
                    ibkr_trading._setup_trade_subscription(
                        on_change_callback=_on_order_change,
                        on_fill_callback=_on_fill,
                    )

                # Create portfolio data provider (read-only, shares the IB conn)
                try:
                    from pacavanza.modules.ibkr_portfolio import IbkrPortfolio

                    ibkr_portfolio = IbkrPortfolio(ib)
                    LOGGER.info("IbkrPortfolio instance created")
                except Exception as e:
                    LOGGER.warning(f"Failed to create IbkrPortfolio: {e}")
            except Exception as e:
                LOGGER.error(f"Failed to connect IBKR async: {e}")

        # start subscriber task in background
        app.state._redis_task = asyncio.create_task(_redis_subscriber_task())
        # start IBKR event pump if connected
        app.state._ibkr_pump_task = asyncio.create_task(_ibkr_event_pump())
        LOGGER.info("FastAPI Redis WS app started (lifespan)")
        try:
            yield
        finally:
            # cancel IBKR event pump
            ibkr_pump = getattr(app.state, "_ibkr_pump_task", None)
            if ibkr_pump:
                ibkr_pump.cancel()
                try:
                    await ibkr_pump
                except asyncio.CancelledError:
                    pass
            # cancel and await subscriber task
            task = getattr(app.state, "_redis_task", None)
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    LOGGER.debug(
                        "Redis subscriber task cancelled cleanly during shutdown"
                    )
                except Exception:
                    LOGGER.exception(
                        "Error awaiting redis subscriber task during shutdown"
                    )
            # close redis client using aclose() to avoid deprecation
            try:
                await redis_client.aclose()
            except Exception:
                pass
            # disconnect IBKR if connected
            if ibkr_trading is not None:
                try:
                    ibkr_trading.ib.disconnect()
                except Exception:
                    pass
            LOGGER.info("FastAPI Redis WS app stopped (lifespan)")

    # create app with lifespan
    app = FastAPI(lifespan=lifespan)

    # MOUNT STATIC FILES - ADD THIS LINE
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    # Expose ava_mini_future_list.json — return the hierarchical JSON so the
    # frontend asset selector shows OMXS30, DAX, NDX … (not the flat instrument keys)
    @app.get("/ava_mini_future_list.json")
    async def get_future_list():
        if not raw_ava_mini_list:
            raise HTTPException(status_code=404, detail="File not found")
        return raw_ava_mini_list

    # Returns the single active OMXS30 future — completely independent of ava_mini_future_list.
    # Response: { "key": "omxs30 jun-2025", "name": "OMXS30 JUN-2025",
    #             "orderbookId": "...", "timezone": "...",
    #             "market_open": "09:00", "market_close": "17:45",
    #             "tick_size": 0.25 }
    @app.get("/active_future")
    async def get_active_future():
        if not active_future_info:
            try:
                raw_active = fetch_active_omxs30_future(target_name=ibkr_local_symbol)
                flat_active = flatten_instrument_list(raw_active)
                if flat_active:
                    key, meta = next(iter(flat_active.items()))
                    active_future_info.clear()
                    active_future_info.update({"key": key, **meta})
                    instrument_list.update(flat_active)
                    LOGGER.info(
                        f"Dynamically loaded active future: {key} ({meta.get('name', '')})"
                    )
            except Exception as e:
                LOGGER.warning("Could not dynamically fetch active future: %s", e)

        if not active_future_info:
            raise HTTPException(status_code=404, detail="No active future loaded")

        # Ensure tick_size has a default fallback
        if "tick_size" not in active_future_info:
            active_future_info["tick_size"] = 0.25
        return active_future_info

    # Useful tiny endpoints to silence noisy probes from browser/devtools
    @app.get("/.well-known/appspecific/com.chrome.devtools.json")
    async def chrome_devtools_probe():
        # return an empty object (200) so devtools probe doesn't show 404
        return JSONResponse({})

    @app.get("/favicon.ico")
    async def favicon():
        # return an empty 204 response (no body, no Content-Length mismatch)
        return Response(status_code=204)

    @app.get("/history/{instrument_id}")
    async def get_history(instrument_id: str, limit: int = 3600):
        """
        Return the most recent completed_ohlc for instrument plus the current live bar.
        This fetches the latest today chart from Avanza, reads existing disk file (preferred),
        merges them, and appends the live bar from memory.
        """
        import asyncio
        from pacavanza.utils.utils import fetch_avanza_chart_history

        info = instrument_list.get(instrument_id, {})
        orderbook_id = info.get("orderbookId")

        avanza_bars = []
        if orderbook_id:
            loop = asyncio.get_running_loop()
            interval_seconds = info.get("interval_seconds", 300)
            tz_name = info.get("timezone", "Europe/Stockholm")
            open_str = info.get("market_open", "09:00")
            close_str = info.get("market_close", "17:45")

            avanza_bars_raw = await loop.run_in_executor(
                None,
                fetch_avanza_chart_history,
                str(orderbook_id),
                interval_seconds,
                tz_name,
                open_str,
                close_str,
            )
            for b in avanza_bars_raw:
                # ISO format to match our JSON
                b["start_time"] = b["start_time"].isoformat()
                b["end_time"] = b["end_time"].isoformat()
                avanza_bars.append(b)

        # Base data from Avanza (might be delayed or incomplete for recent bars)
        merged_dict = {b["start_time"]: b for b in avanza_bars}

        # Override with our perfectly tracked in-memory bars (contains disk history + live updates)
        bars_lock = await _get_bars_lock_for(instrument_id)
        async with bars_lock:
            live_bars = recent_bars.get(instrument_id, [])
            for b in live_bars:
                start_str = (
                    b["start_time"].isoformat()
                    if hasattr(b["start_time"], "isoformat")
                    else b["start_time"]
                )
                end_str = (
                    b["end_time"].isoformat()
                    if hasattr(b["end_time"], "isoformat")
                    else b["end_time"]
                )
                merged_dict[start_str] = {
                    "start_time": start_str,
                    "end_time": end_str,
                    "open": b["open"],
                    "high": b["high"],
                    "low": b["low"],
                    "close": b["close"],
                    "volume": b.get("volume", 0),
                }

        data = [merged_dict[k] for k in sorted(merged_dict.keys())]

        if not data:
            raise HTTPException(status_code=404, detail="no data")

        if limit and len(data) > limit:
            data = data[-limit:]
        return JSONResponse(content=data)

    @app.websocket("/ws")
    async def websocket_endpoint(ws: WebSocket):
        await manager.connect(ws)

        # Send initial IBKR status
        is_ibkr_connected = False
        is_trading_disabled = False
        if ibkr_trading is not None and ibkr_trading.ib is not None:
            is_ibkr_connected = ibkr_trading.ib.isConnected()
            target_local_symbol = (
                ibkr_trading.contract.localSymbol
                if getattr(ibkr_trading, "contract", None)
                else None
            )
            try:
                is_trading_disabled = check_daily_loss_limit(
                    ibkr_portfolio, target_local_symbol
                )
            except Exception:
                pass

        try:
            await ws.send_text(
                json.dumps(
                    {
                        "type": "ibkr_status",
                        "connected": is_ibkr_connected,
                        "trading_disabled": is_trading_disabled,
                    }
                )
            )
        except Exception:
            pass

        try:
            # client may send subscription messages in future; for now we just broadcast all
            while True:
                # keep receive to allow client ping/pong; ignore messages
                _ = await ws.receive_text()
        except WebSocketDisconnect:
            await manager.disconnect(ws)
        except Exception:
            await manager.disconnect(ws)

    # serve a static HTML for convenience if requested
    @app.get("/")
    async def index():
        try:
            with open(str(static_html_path), "r", encoding="utf-8") as f:
                return HTMLResponse(f.read())
        except Exception:
            return JSONResponse({"status": "ok", "note": "Static file not available"})

    @app.post("/client_log")
    async def client_log(req: Request):
        try:
            body = await req.json()
            level = body.get("level", "INFO")
            message = body.get("message", "")
            if level == "ERROR":
                LOGGER.error(f"[Client Console ERROR] {message}")
            elif level == "WARN":
                LOGGER.warning(f"[Client Console WARN] {message}")
            else:
                LOGGER.info(f"[Client Console INFO] {message}")
        except Exception as e:
            LOGGER.error(f"Error handling client log: {e}")
        return JSONResponse({"status": "ok"})

    @app.get("/ava")
    async def ava_index():
        try:
            # Serve the mini trading UI (ava)
            with open(str(STATIC_AVA_HTML), "r", encoding="utf-8") as f:
                return HTMLResponse(f.read())
        except Exception:
            return JSONResponse(
                {"status": "error", "note": "Ava static file not available"}
            )

    # --------------------
    # Trading endpoints (used by trading.js)
    # --------------------

    @app.post("/trade/market_buy")
    async def trade_market_buy(req: Request):
        """
        Place a market buy order, using input parameters: instrumentId, percentage.
        The price should be the current last sell price from the redis data.
        """
        body = await req.json()
        instrument_id = body.get("instrumentId")
        percentage = body.get("percentage")
        if not instrument_id or percentage is None:
            raise HTTPException(
                status_code=400, detail="instrumentId and percentage required"
            )
        # instrumentId should be inside ava_mini_future_list.json.
        if instrument_id not in instrument_list:
            raise HTTPException(
                status_code=400,
                detail="instrumentId not found in ava_mini_future_list.json",
            )
        try:
            order = await trading.place_market_buy(instrument_id, percentage)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content={"status": "ok", "order": order})

    @app.post("/trade/market_sell")
    async def trade_market_sell(req: Request):
        """
        Place a market sell order, using input parameters: instrumentId..
        The price should be the current last buy price from the redis data.
        """
        body = await req.json()
        instrument_id = body.get("instrumentId")
        if not instrument_id:
            raise HTTPException(status_code=400, detail="instrumentId required")
        if instrument_id not in instrument_list:
            raise HTTPException(
                status_code=400,
                detail="instrumentId not found in ava_mini_future_list.json",
            )
        try:
            order = await trading.place_market_sell(instrument_id)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content={"status": "ok", "order": order})

    @app.post("/trade/buy_stop")
    async def trade_buy_stop(req: Request):
        """
        Place a buy stop order:
        Wait for the current bar to complete. Exactly at the 0 second of the new bar, calculate the stop price which should be 1 tick_size above the high of the last completed bar.
        Then immediately place the buy stop order at that stop price.
        Also place a sell stop order at 1 tick_size below the low of the current swing leg. This calculation should use pandas-ta library. This leg should contain only consecutive bull bar(s). So that we have a stop-loss.
        Then use a profit ratio of 2:1 stop-loss to calculate the take-profit price and place a sell limit order:
        take-profit price = high of the current bar + 2 * (high of the current bar - low of the current swing leg) - 1 tick_size.
        The 1 tick_size is to ensure the take-profit order can be filled.
        For now, just log the order details instead of really placing the orders.
        """
        body = await req.json()
        instrument_id = body.get("instrumentId")
        percentage = body.get("percentage")
        if not instrument_id or percentage is None:
            raise HTTPException(
                status_code=400, detail="instrumentId and percentage required"
            )
        if instrument_id not in instrument_list:
            raise HTTPException(
                status_code=400,
                detail="instrumentId not found in ava_mini_future_list.json",
            )
        try:
            res = await trading.schedule_buy_stop(instrument_id, percentage)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content=res)

    @app.post("/trade/late_buy_stop")
    async def trade_late_buy_stop(req: Request):
        """
        Place a late buy stop order:
        Immediately calculate the stop price which should be 1 tick_size above the high of the last completed bar.
        Then immediately place the buy stop order at that stop price.
        Also place a sell stop order at 1 tick_size below the low of the current swing leg (excluding the current bar which is not completed). This calculation should use pandas-ta library. This leg should contain only consecutive bull bar(s). So that we have a stop-loss.
        Then use a profit ratio of 2:1 stop-loss to calculate the take-profit price and place a sell limit order:
        take-profit price = high of the last completed bar + 2 * (high of the last completed bar - low of the current swing leg) - 1 tick_size.
        The 1 tick_size is to ensure the take-profit order can be filled.
        For now, just log the order details instead of really placing the orders.
        """
        body = await req.json()
        instrument_id = body.get("instrumentId")
        percentage = body.get("percentage")
        if not instrument_id or percentage is None:
            raise HTTPException(
                status_code=400, detail="instrumentId and percentage required"
            )
        if instrument_id not in instrument_list:
            raise HTTPException(
                status_code=400,
                detail="instrumentId not found in ava_mini_future_list.json",
            )
        try:
            res = await trading.late_buy_stop(instrument_id, percentage)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content=res)

    @app.post("/trade/sell_stop")
    async def trade_sell_stop(req: Request):
        """
        Place a sell stop order:
        Wait for the current bar to complete. Exactly at the 0 second of the new bar, calculate the stop price which should be 1 tick_size below the low of the last completed bar.
        Then immediately place the sell stop order at that stop price.
        For now, just log the order details instead of really placing the orders.
        """
        body = await req.json()
        instrument_id = body.get("instrumentId")
        if not instrument_id:
            raise HTTPException(status_code=400, detail="instrumentId required")
        if instrument_id not in instrument_list:
            raise HTTPException(
                status_code=400,
                detail="instrumentId not found in ava_mini_future_list.json",
            )
        try:
            res = await trading.schedule_sell_stop(instrument_id)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content=res)

    @app.post("/trade/late_sell_stop")
    async def trade_late_sell_stop(req: Request):
        """
        Place a late sell stop order:
        Immediately calculate the stop price which should be 1 tick_size below the low of the last completed bar.
        Then immediately place the sell stop order at that stop price.
        For now, just log the order details instead of really placing the orders.
        """
        body = await req.json()
        instrument_id = body.get("instrumentId")
        if not instrument_id:
            raise HTTPException(status_code=400, detail="instrumentId required")
        if instrument_id not in instrument_list:
            raise HTTPException(
                status_code=400,
                detail="instrumentId not found in ava_mini_future_list.json",
            )
        try:
            res = await trading.late_sell_stop(instrument_id)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content=res)

    # --------------------
    # Cancellation endpoints
    # --------------------
    @app.post("/trade/cancel_buy_stop")
    async def cancel_buy_stop(req: Request):
        """
        Cancel a scheduled buy_stop for instrument if it exists and hasn't executed yet.
        """
        body = await req.json()
        instrument_id = body.get("instrumentId")
        if not instrument_id:
            raise HTTPException(status_code=400, detail="instrumentId required")
        if instrument_id not in instrument_list:
            raise HTTPException(
                status_code=400,
                detail="instrumentId not found in ava_mini_future_list.json",
            )
        try:
            res = await trading.cancel_buy_stop(instrument_id)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content=res)

    @app.post("/trade/cancel_sell_stop")
    async def cancel_sell_stop(req: Request):
        """
        Cancel a scheduled sell_stop for instrument if it exists and hasn't executed yet.
        """
        body = await req.json()
        instrument_id = body.get("instrumentId")
        if not instrument_id:
            raise HTTPException(status_code=400, detail="instrumentId required")
        if instrument_id not in instrument_list:
            raise HTTPException(
                status_code=400,
                detail="instrumentId not found in ava_mini_future_list.json",
            )
        try:
            res = await trading.cancel_sell_stop(instrument_id)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content=res)

    @app.post("/trade/edit_order")
    async def trade_edit_order(req: Request):
        """
        Place a market buy order, using input parameters: instrumentId, volume.
        The price should be the current last sell price from the redis data.
        """
        body = await req.json()
        order_id = body.get("orderId")
        account_id = body.get("accountId")
        price = body.get("price")
        volume = body.get("volume")
        valid_until = body.get("valid_until")
        if not order_id or not account_id or not price or not volume or not valid_until:
            raise HTTPException(
                status_code=400,
                detail="order_id, account_id, price, volume, and valid_until required",
            )
        if not isinstance(order_id, str):
            raise HTTPException(status_code=400, detail="order_id must be str")
        if not isinstance(account_id, str):
            raise HTTPException(status_code=400, detail="account_id must be str")
        if not isinstance(price, float):
            raise HTTPException(status_code=400, detail="price must be float")
        if not isinstance(volume, int):
            raise HTTPException(status_code=400, detail="volume must be int")
        if not isinstance(valid_until, str):
            raise HTTPException(status_code=400, detail="valid_until must be str")

        try:
            order_ret = trading.edit_order(
                order_id=order_id,
                account_id=account_id,
                price=price,
                volume=volume,
                valid_until=valid_until,
            )
            order_status = order_ret.get("orderRequestStatus")
            if not order_status:
                raise HTTPException(
                    status_code=500, detail=str("No orderRequestStatus")
                )
            if order_status != "SUCCESS":
                raise HTTPException(status_code=500, detail=str(json.dumps(order_ret)))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content={"status": "ok", "order": order_ret})

    @app.post("/trade/edit_order_follow_market")
    async def trade_edit_order_follow_market(req: Request):
        """
        Place a market buy order, using input parameters: instrumentId, volume.
        The price should be the current last sell price from the redis data.
        """
        body = await req.json()
        order_id = body.get("orderId")
        account_id = body.get("accountId")
        if not order_id or not account_id:
            raise HTTPException(
                status_code=400,
                detail="order_id and account_id required",
            )
        if not isinstance(order_id, str):
            raise HTTPException(status_code=400, detail="order_id must be str")
        if not isinstance(account_id, str):
            raise HTTPException(status_code=400, detail="account_id must be str")

        try:
            order_ret = await trading.edit_order_follow_market(
                order_id=order_id,
                account_id=account_id,
            )
            order_status = order_ret.get("orderRequestStatus")
            if not order_status:
                raise HTTPException(
                    status_code=500, detail=str("No orderRequestStatus")
                )
            if order_status != "SUCCESS":
                raise HTTPException(status_code=500, detail=str(json.dumps(order_ret)))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content={"status": "ok", "order": order_ret})

    @app.post("/trade/delete_stop_losses")
    async def delete_stop_losses(req: Request):
        """
        Delete stop losses for a specific instrument.
        """
        body = await req.json()
        instrument_id = body.get("instrumentId")
        if not instrument_id:
            raise HTTPException(status_code=400, detail="instrumentId required")
        if instrument_id not in instrument_list:
            raise HTTPException(
                status_code=400,
                detail="instrumentId not found in ava_mini_future_list.json",
            )
        try:
            trading.delete_stop_losses(instrument_id)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content={"status": "ok"})

    @app.post("/trade/cleanup_residual_sell_stop_losses")
    async def cleanup_residual_sell_stop_losses(req: Request):
        """
        Clean up residual sell stop losses.
        """
        try:
            trading.cleanup_residual_sell_stop_losses()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content={"status": "ok"})

    # ══════════════════════════════════════════════════════════════════════
    # IBKR-specific trading endpoints (prefixed /ibkr/)
    # These use the separate ibkr_trading instance passed to create_app().
    # They do NOT modify any existing /trade/* Avanza endpoints above.
    # ══════════════════════════════════════════════════════════════════════

    def _require_ibkr():
        """Raise 501 if no ibkr_trading instance is available."""
        if ibkr_trading is None:
            raise HTTPException(
                status_code=501,
                detail="IBKR trading not active",
            )

    # ── IBKR stop/market/cancel endpoints (mirror /trade/* but use ibkr_trading) ──

    @app.post("/ibkr/market_buy")
    async def ibkr_market_buy(req: Request):
        """Place a market buy order via IBKR."""
        _require_ibkr()
        body = await req.json()
        instrument_id = body.get("instrumentId")
        percentage = body.get("percentage")
        if not instrument_id:
            raise HTTPException(status_code=400, detail="instrumentId required")
        try:
            num_contracts = int(percentage) if percentage is not None else None
            order = await ibkr_trading.place_market_buy(
                instrument_id, percentage=None, number_of_contracts=num_contracts
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content={"status": "ok", "order": order})

    @app.post("/ibkr/market_sell")
    async def ibkr_market_sell(req: Request):
        """Place a market sell order via IBKR."""
        _require_ibkr()
        body = await req.json()
        instrument_id = body.get("instrumentId")
        percentage = body.get("percentage")
        if not instrument_id:
            raise HTTPException(status_code=400, detail="instrumentId required")
        try:
            num_contracts = int(percentage) if percentage is not None else None
            order = await ibkr_trading.place_market_sell(
                instrument_id, number_of_contracts=num_contracts
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content={"status": "ok", "order": order})

    @app.post("/ibkr/buy_stop")
    async def ibkr_buy_stop(req: Request):
        """Schedule a buy stop order via IBKR."""
        _require_ibkr()
        body = await req.json()
        instrument_id = body.get("instrumentId")
        percentage = body.get("percentage")
        if not instrument_id or percentage is None:
            raise HTTPException(
                status_code=400, detail="instrumentId and percentage required"
            )
        try:
            res = await ibkr_trading.schedule_buy_stop(instrument_id, percentage)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content=res)

    @app.post("/ibkr/late_buy_stop")
    async def ibkr_late_buy_stop(req: Request):
        """Place a late buy stop order immediately via IBKR."""
        _require_ibkr()
        body = await req.json()
        instrument_id = body.get("instrumentId")
        percentage = body.get("percentage")
        if not instrument_id or percentage is None:
            raise HTTPException(
                status_code=400, detail="instrumentId and percentage required"
            )
        try:
            res = await ibkr_trading.late_buy_stop(instrument_id, percentage)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content=res)

    @app.post("/ibkr/sell_stop")
    async def ibkr_sell_stop(req: Request):
        """Schedule a sell stop order via IBKR."""
        _require_ibkr()
        body = await req.json()
        instrument_id = body.get("instrumentId")
        percentage = body.get("percentage")
        if not instrument_id:
            raise HTTPException(status_code=400, detail="instrumentId required")
        try:
            num_contracts = int(percentage) if percentage is not None else None
            res = await ibkr_trading.schedule_sell_stop(
                instrument_id, number_of_contracts=num_contracts
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content=res)

    @app.post("/ibkr/late_sell_stop")
    async def ibkr_late_sell_stop(req: Request):
        """Place a late sell stop order immediately via IBKR."""
        _require_ibkr()
        body = await req.json()
        instrument_id = body.get("instrumentId")
        percentage = body.get("percentage")
        if not instrument_id:
            raise HTTPException(status_code=400, detail="instrumentId required")
        try:
            num_contracts = int(percentage) if percentage is not None else None
            res = await ibkr_trading.late_sell_stop(
                instrument_id, number_of_contracts=num_contracts
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content=res)

    @app.post("/ibkr/cancel_buy_stop")
    async def ibkr_cancel_buy_stop(req: Request):
        """Cancel a scheduled buy_stop via IBKR."""
        _require_ibkr()
        body = await req.json()
        instrument_id = body.get("instrumentId")
        if not instrument_id:
            raise HTTPException(status_code=400, detail="instrumentId required")
        try:
            res = await ibkr_trading.cancel_buy_stop(instrument_id)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content=res)

    @app.post("/ibkr/cancel_sell_stop")
    async def ibkr_cancel_sell_stop(req: Request):
        """Cancel a scheduled sell_stop via IBKR."""
        _require_ibkr()
        body = await req.json()
        instrument_id = body.get("instrumentId")
        if not instrument_id:
            raise HTTPException(status_code=400, detail="instrumentId required")
        try:
            res = await ibkr_trading.cancel_sell_stop(instrument_id)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content=res)

    @app.post("/ibkr/delete_stop_losses")
    async def ibkr_delete_stop_losses(req: Request):
        """Delete stop losses via IBKR."""
        _require_ibkr()
        body = await req.json()
        instrument_id = body.get("instrumentId")
        if not instrument_id:
            raise HTTPException(status_code=400, detail="instrumentId required")
        try:
            ibkr_trading.delete_stop_losses(instrument_id)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content={"status": "ok"})

    # ── IBKR order management endpoints ──

    @app.post("/ibkr/edit_order")
    async def ibkr_edit_order(req: Request):
        """
        Edit the price and/or quantity of an existing IBKR order.
        Accepts: { orderId: int, price: float (optional), quantity: int (optional) }
        """
        _require_ibkr()
        body = await req.json()
        order_id = body.get("orderId")
        price = body.get("price")
        quantity = body.get("quantity")
        if order_id is None:
            raise HTTPException(status_code=400, detail="orderId is required")
        if price is None and quantity is None:
            raise HTTPException(
                status_code=400, detail="Either price or quantity must be provided"
            )
        try:
            p_val = float(price) if price is not None else None
            q_val = int(quantity) if quantity is not None else None
            result = ibkr_trading.edit_order(
                order_id=int(order_id), price=p_val, quantity=q_val
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content={"status": "ok", **result})

    @app.post("/ibkr/edit_order_follow_market")
    async def ibkr_edit_order_follow_market(req: Request):
        """
        Convert an existing IBKR order to a market order.
        Accepts: { orderId: int }
        """
        _require_ibkr()
        body = await req.json()
        order_id = body.get("orderId")
        if order_id is None:
            raise HTTPException(status_code=400, detail="orderId required")
        try:
            result = ibkr_trading.edit_order_follow_market(order_id=int(order_id))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content={"status": "ok", **result})

    @app.post("/ibkr/place_stop_order")
    async def ibkr_place_stop_order(req: Request):
        """
        Place a standalone protective stop order.
        Accepts: { action: str, volume: int, stopPrice: float, orderRef: str }
        """
        _require_ibkr()
        body = await req.json()
        action = body.get("action")
        volume = body.get("volume")
        stop_price = body.get("stopPrice")
        order_ref = body.get("orderRef", "")
        if not action or volume is None or stop_price is None:
            raise HTTPException(
                status_code=400,
                detail="action, volume, and stopPrice required",
            )
        try:
            result = ibkr_trading.place_stop_order(
                action=action,
                volume=int(volume),
                stop_price=float(stop_price),
                order_ref=str(order_ref),
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content={"status": "ok", **result})

    @app.post("/ibkr/place_oca_bracket")
    async def ibkr_place_oca_bracket(req: Request):
        """
        Place an OCA bracket (SL + TP) with explicit prices.
        Accepts: { action: str, volume: int, limitPrice: float, stopPrice: float }
        """
        _require_ibkr()
        body = await req.json()
        action = body.get("action")
        volume = body.get("volume")
        limit_price = body.get("limitPrice")
        stop_price = body.get("stopPrice")
        if not action or volume is None or limit_price is None or stop_price is None:
            raise HTTPException(
                status_code=400,
                detail="action, volume, limitPrice, and stopPrice required",
            )
        try:
            result = ibkr_trading.place_oca_bracket(
                action=action,
                volume=int(volume),
                limit_price=float(limit_price),
                stop_price=float(stop_price),
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content={"status": "ok", **result})

    @app.post("/ibkr/limit_buy")
    async def ibkr_limit_buy(req: Request):
        """Place a limit buy order via IBKR."""
        _require_ibkr()
        body = await req.json()
        volume = body.get("volume")
        price = body.get("price")
        if volume is None or price is None:
            raise HTTPException(status_code=400, detail="volume and price required")
        try:
            order = ibkr_trading.place_limit_buy(int(volume), float(price))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content={"status": "ok", "order": order})

    @app.post("/ibkr/limit_sell")
    async def ibkr_limit_sell(req: Request):
        """Place a limit sell order via IBKR."""
        _require_ibkr()
        body = await req.json()
        volume = body.get("volume")
        price = body.get("price")
        if volume is None or price is None:
            raise HTTPException(status_code=400, detail="volume and price required")
        try:
            order = ibkr_trading.place_limit_sell(int(volume), float(price))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content={"status": "ok", "order": order})

    @app.get("/ibkr/open_orders")
    async def ibkr_open_orders():
        """Return all active orders for the IBKR contract."""
        if ibkr_trading is None:
            return JSONResponse(content={"orders": [], "position": None})
        try:
            orders = ibkr_trading.get_open_orders()
            position = ibkr_trading.get_position_info()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content={"orders": orders, "position": position})

    @app.post("/ibkr/cancel_order")
    async def ibkr_cancel_order(req: Request):
        """
        Cancel a specific IBKR order by orderId.
        Accepts: { orderId: int }
        """
        _require_ibkr()
        body = await req.json()
        order_id = body.get("orderId")
        if order_id is None:
            raise HTTPException(status_code=400, detail="orderId required")
        try:
            result = ibkr_trading.cancel_order(order_id=int(order_id))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content={"status": "ok", **result})

    @app.get("/ibkr/fills")
    async def ibkr_fills():
        """Return all persisted fill records for chart markers."""
        return JSONResponse(content={"fills": _load_fills()})

    # ══════════════════════════════════════════════════════════════════════
    # IBKR Portfolio endpoints (prefixed /ibkr/portfolio/)
    # Read-only portfolio data from the IbkrPortfolio module.
    # ══════════════════════════════════════════════════════════════════════

    @app.get("/ibkr/portfolio/summary")
    async def ibkr_portfolio_summary():
        """Return account summary metrics (NLV, cash, margins, cushion)."""
        if ibkr_portfolio is None:
            return JSONResponse(content={})
        try:
            summary = ibkr_portfolio.get_account_summary()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content=summary)

    @app.get("/ibkr/portfolio/positions")
    async def ibkr_portfolio_positions():
        """Return all portfolio positions with market values and P&L."""
        if ibkr_portfolio is None:
            return JSONResponse(content={"positions": [], "pnl": {}})
        try:
            positions = ibkr_portfolio.get_portfolio_positions()
            pnl = ibkr_portfolio.get_pnl_summary()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content={"positions": positions, "pnl": pnl})

    @app.get("/ibkr/portfolio/orders")
    async def ibkr_portfolio_orders():
        """Return all open/active orders across all contracts."""
        if ibkr_portfolio is None:
            return JSONResponse(content={"orders": []})
        try:
            orders = ibkr_portfolio.get_open_orders()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content={"orders": orders})

    @app.get("/ibkr/portfolio/executions")
    async def ibkr_portfolio_executions():
        """Return recent executions/fills from the current session."""
        if ibkr_portfolio is None:
            return JSONResponse(content={"executions": []})
        try:
            executions = ibkr_portfolio.get_executions()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return JSONResponse(content={"executions": executions})

    @app.get("/portfolio")
    async def portfolio_index():
        """Serve the IBKR portfolio dashboard page."""
        try:
            with open(str(STATIC_PORTFOLIO_HTML), "r", encoding="utf-8") as f:
                return HTMLResponse(f.read())
        except Exception:
            return JSONResponse(
                {"status": "error", "note": "Portfolio static file not available"}
            )

    # Setup trade event subscription to broadcast order changes via WebSocket
    def _on_order_change(orders_snapshot):
        """Broadcast order updates to all connected WebSocket clients."""
        pos_info = None
        if ibkr_trading:
            try:
                pos_info = ibkr_trading.get_position_info()
            except Exception:
                pass

        async def _broadcast():
            await manager.broadcast(
                {
                    "type": "order_update",
                    "orders": orders_snapshot,
                    "position": pos_info,
                }
            )

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_broadcast())
        except RuntimeError:
            pass

    def _on_fill(fill_record):
        """Persist fill to disk and broadcast to WebSocket clients."""
        _save_fill(fill_record)

        async def _broadcast_fill():
            await manager.broadcast(
                {
                    "type": "fill",
                    "fill": fill_record,
                }
            )

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_broadcast_fill())
        except RuntimeError:
            pass

    return app


def main():
    # run as: python -m pacavanza.backend.main

    # Try to create an IbkrTrading instance for the future chart
    ibkr_inst = None
    try:
        from ib_async import IB, ContFuture
        from pacavanza.modules.ibkr_trading import IbkrTrading

        ib = IB()
        # Do NOT connect or qualify here, wait for lifespan context manager!
        contract = ContFuture("OMXS30", "OMS")
        LOGGER.info(
            f"IBKR instance created for {contract.symbol}. Will connect in lifespan."
        )

        # IbkrTrading shares recent_bars/instrument_list with AvanzaTrading
        # but they are populated inside create_app. So we create a minimal
        # instance here and it will be wired up after create_app populates
        # the in-memory state.
        ibkr_inst = (ib, contract)
    except Exception as e:
        LOGGER.warning(f"Could not connect to IBKR: {e}. IBKR endpoints disabled.")

    # Build the app — AvanzaTrading is always created internally.
    # ibkr_trading is created using shared state from inside create_app.
    app = create_app(
        redis_url="redis://localhost:6379/0",
        redis_channels=["pacavanza:ticker_updates", "pacavanza:future_updates"],
        ibkr_conn=ibkr_inst,
    )
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")


if __name__ == "__main__":
    main()
