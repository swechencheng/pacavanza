import asyncio
import json
import logging
from typing import Dict, Any, List, AsyncIterator
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request
from fastapi.responses import JSONResponse, HTMLResponse, Response, FileResponse
from fastapi.staticfiles import StaticFiles
import uvicorn
import redis.asyncio as aioredis
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from pathlib import Path
from contextlib import asynccontextmanager
from .modules.avanza_trading import AvanzaTrading

# compute pacavanza package root (pacavanza/)
ROOT = Path(__file__).resolve().parent  # pacavanza/
STATIC_DIR = ROOT / "static"
STATIC_HTML = STATIC_DIR / "chart.html"

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
    redis_channel="pacavanza:ticker_updates",
    static_html_path: str | Path = STATIC_HTML,
):
    """
    Create FastAPI app. Uses lifespan async context manager to start/stop background
    Redis subscriber task and to close the redis client cleanly (uses aclose()).
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

    # NEW: load instrument_list.json into memory for quick access
    instrument_list: Dict[str, Any] = {}
    try:
        with open(ROOT / "instrument_list.json", "r", encoding="utf-8") as f:
            instrument_list = json.load(f)
    except Exception as e:
        LOGGER.warning("Could not load instrument_list.json: %s", e)

    # instantiate AvanzaTrading with references to in-memory state and the lock maps
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

    # Background task: subscribe to redis channel and forward events
    async def _redis_subscriber_task():
        LOGGER.info("Starting Redis subscriber task")
        try:
            pubsub = redis_client.pubsub()
            await pubsub.subscribe(redis_channel)
            LOGGER.info(f"Subscribed to Redis channel: {redis_channel}")

            # Test message to verify Redis is working
            test_msg = {"type": "test", "message": "Redis connection established"}
            await redis_client.publish(redis_channel, json.dumps(test_msg))

            async for msg in pubsub.listen():
                if msg and msg["type"] == "message":
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
                await pubsub.unsubscribe(redis_channel)
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
                if not lst or lst[-1]["start_time"] != bar["start_time"]:
                    lst.append(
                        {
                            "start_time": datetime.fromisoformat(bar["start_time"]),
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
                # limit history length (e.g. 5000 bars)
                if len(lst) > 5000:
                    lst[:] = lst[-5000:]

                # NEW: prune bars older than one week to save memory
                try:
                    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=7)
                    lst[:] = [
                        b
                        for b in lst
                        if (b["start_time"].astimezone(timezone.utc) >= cutoff)
                    ]
                except Exception:
                    if len(lst) > 5000:
                        lst[:] = lst[-5000:]

            # compute incremental EMA updates (fast) - uses the global recent_bars dict; reading latest snapshot is fine
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
            # convert and append under per-instrument lock
            bars_lock = await _get_bars_lock_for(sid)
            async with bars_lock:
                lst = recent_bars[sid]
                lst.append(
                    {
                        "start_time": datetime.fromisoformat(bar["start_time"]),
                        "end_time": datetime.fromisoformat(bar["end_time"]),
                        "open": bar["open"],
                        "high": bar["high"],
                        "low": bar["low"],
                        "close": bar["close"],
                        "volume": bar.get("volume", 0),
                    }
                )
                if len(lst) > 5000:
                    lst[:] = lst[-5000:]
                # NEW: prune to one week
                try:
                    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=7)
                    lst[:] = [
                        b
                        for b in lst
                        if (b["start_time"].astimezone(timezone.utc) >= cutoff)
                    ]
                except Exception:
                    if len(lst) > 5000:
                        lst[:] = lst[-5000:]
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

    # Lifespan context manager: start subscriber on startup and close on shutdown
    @asynccontextmanager
    async def lifespan(app) -> AsyncIterator[None]:
        # start subscriber task in background
        app.state._redis_task = asyncio.create_task(_redis_subscriber_task())
        LOGGER.info("FastAPI Redis WS app started (lifespan)")
        try:
            yield
        finally:
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
            LOGGER.info("FastAPI Redis WS app stopped (lifespan)")

    # create app with lifespan
    app = FastAPI(lifespan=lifespan)

    # MOUNT STATIC FILES - ADD THIS LINE
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    # Expose instrument_list.json
    @app.get("/instrument_list.json")
    async def get_bar_json():
        return FileResponse(
            ROOT / "instrument_list.json",
            media_type="application/json",
            headers={"Cache-Control": "public, max-age=3600"},
        )

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
    async def get_history(instrument_id: str, limit: int = 500):
        """
        Return the most recent completed_ohlc for instrument.
        This reads existing disk file (preferred) or uses in-memory snapshot if available.
        """
        # first attempt to read disk file (collector is authoritative)
        data_file = f"ohlc_{instrument_id}.json"
        try:
            with open(data_file, "r") as f:
                data = json.load(f)
            # ensure we only return up to limit
            if limit and len(data) > limit:
                data = data[-limit:]
            return JSONResponse(content=data)
        except FileNotFoundError:
            # fallback to in-memory recent_bars
            # Use a per-instrument snapshot under the instrument lock to avoid inconsistent reads
            bars_lock = await _get_bars_lock_for(instrument_id)
            async with bars_lock:
                lst = list(recent_bars.get(instrument_id, []))
            out = []
            for b in lst[-limit:]:
                out.append(
                    {
                        "start_time": b["start_time"].isoformat(),
                        "end_time": b["end_time"].isoformat(),
                        "open": b["open"],
                        "high": b["high"],
                        "low": b["low"],
                        "close": b["close"],
                        "volume": b["volume"],
                    }
                )
            if not out:
                raise HTTPException(status_code=404, detail="no data")
            return JSONResponse(content=out)

    @app.websocket("/ws")
    async def websocket_endpoint(ws: WebSocket):
        await manager.connect(ws)
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

    # --------------------
    # Trading endpoints (used by trading.js)
    # --------------------

    @app.post("/trade/market_buy")
    async def trade_market_buy(req: Request):
        """
        Place a market buy order, using input parameters: instrumentId, volume.
        The price should be the current last sell price from the redis data.
        """
        body = await req.json()
        instrument_id = body.get("instrumentId")
        volume = body.get("volume")
        if not instrument_id or volume is None:
            raise HTTPException(
                status_code=400, detail="instrumentId and volume required"
            )
        # instrumentId should be inside instrument_list.json.
        if instrument_id not in instrument_list:
            raise HTTPException(
                status_code=400, detail="instrumentId not found in instrument_list.json"
            )
        order = await trading.place_market_buy(instrument_id, volume)
        return JSONResponse(content={"status": "ok", "order": order})

    @app.post("/trade/market_sell")
    async def trade_market_sell(req: Request):
        """
        Place a market sell order, using input parameters: instrumentId, volume.
        The price should be the current last buy price from the redis data.
        """
        body = await req.json()
        instrument_id = body.get("instrumentId")
        volume = body.get("volume")
        if not instrument_id or volume is None:
            raise HTTPException(
                status_code=400, detail="instrumentId and volume required"
            )
        if instrument_id not in instrument_list:
            raise HTTPException(
                status_code=400, detail="instrumentId not found in instrument_list.json"
            )
        order = await trading.place_market_sell(instrument_id, volume)
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
        volume = body.get("volume")
        if not instrument_id or volume is None:
            raise HTTPException(
                status_code=400, detail="instrumentId and volume required"
            )
        if instrument_id not in instrument_list:
            raise HTTPException(
                status_code=400, detail="instrumentId not found in instrument_list.json"
            )
        res = await trading.schedule_buy_stop(instrument_id, volume)
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
        volume = body.get("volume")
        if not instrument_id or volume is None:
            raise HTTPException(
                status_code=400, detail="instrumentId and volume required"
            )
        if instrument_id not in instrument_list:
            raise HTTPException(
                status_code=400, detail="instrumentId not found in instrument_list.json"
            )
        res = await trading.late_buy_stop(instrument_id, volume)
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
        volume = body.get("volume")
        if not instrument_id or volume is None:
            raise HTTPException(
                status_code=400, detail="instrumentId and volume required"
            )
        if instrument_id not in instrument_list:
            raise HTTPException(
                status_code=400, detail="instrumentId not found in instrument_list.json"
            )
        res = await trading.schedule_sell_stop(instrument_id, volume)
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
        volume = body.get("volume")
        if not instrument_id or volume is None:
            raise HTTPException(
                status_code=400, detail="instrumentId and volume required"
            )
        if instrument_id not in instrument_list:
            raise HTTPException(
                status_code=400, detail="instrumentId not found in instrument_list.json"
            )
        res = await trading.late_sell_stop(instrument_id, volume)
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
                status_code=400, detail="instrumentId not found in instrument_list.json"
            )
        res = await trading.cancel_buy_stop(instrument_id)
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
                status_code=400, detail="instrumentId not found in instrument_list.json"
            )
        res = await trading.cancel_sell_stop(instrument_id)
        return JSONResponse(content=res)

    return app


def main():
    # run as: python -m pacavanza.backend.main
    app = create_app(
        redis_url="redis://localhost:6379/0", redis_channel="pacavanza:ticker_updates"
    )
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")


if __name__ == "__main__":
    main()
