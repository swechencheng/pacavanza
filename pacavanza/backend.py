import asyncio
import json
import logging
from typing import Dict, Any, List, AsyncIterator
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse, Response, FileResponse
from fastapi.staticfiles import StaticFiles
import uvicorn
import redis.asyncio as aioredis
from datetime import datetime
from collections import defaultdict
from pathlib import Path
from contextlib import asynccontextmanager

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

    # in-memory per-stock state (keeps recent history for EMA calculation).
    # For many symbols or long history you might want to persist this or cap size.
    recent_bars: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    # ema_state[stock][length] = last EMA value
    ema_state: Dict[str, Dict[int, float]] = defaultdict(dict)
    # Optional: map stock -> metadata (timezone, market_open/close) passed from collector in messages
    metadata: Dict[str, Dict[str, Any]] = {}

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
            LOGGER.info("Redis subscriber task cancelled")
            raise
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

    async def _handle_redis_payload(payload: Dict[str, Any]):
        """
        Process messages from Redis, keep in-memory state for EMA/labels and
        broadcast enriched messages to websocket clients.
        """
        LOGGER.debug("Handling redis payload: %s", payload)
        mtype = payload.get("type")
        sid = payload.get("stock")
        if not sid:
            return

        # store metadata if provided
        if payload.get("meta"):
            metadata[sid] = payload["meta"]

        if mtype == "update":
            # in-progress/current bar updates
            bar = payload.get("bar")
            if not bar:
                return
            # use end_time as canonical timestamp
            ts = datetime.fromisoformat(bar["end_time"])
            # append/replace last bar in recent_bars (keep them bounded)
            lst = recent_bars[sid]
            if not lst or lst[-1]["end_time"] != bar["end_time"]:
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

            # compute incremental EMA updates (fast)
            emas = {}
            for L in (20, 50, 100, 220):
                prev = ema_state[sid].get(L)
                new = incremental_ema_update(
                    prev, bar["close"], L, historical_buffer=lst
                )
                if new is not None:
                    ema_state[sid][L] = new
                    emas[str(L)] = {"time": bar["start_time"], "value": new}

            out = {
                "type": "update",
                "stock": sid,
                "bar": bar,
                "emas": emas,
            }
            await manager.broadcast(out)

        elif mtype == "completed":
            # a completed bar (append to history)
            bar = payload.get("bar")
            if not bar:
                return
            # convert and append
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
            # recompute EMAs using incremental update with the finalized close
            emas = {}
            for L in (20, 50, 100, 220):
                prev = ema_state[sid].get(L)
                new = incremental_ema_update(
                    prev, bar["close"], L, historical_buffer=lst
                )
                if new is not None:
                    ema_state[sid][L] = new
                    emas[str(L)] = {"time": bar["start_time"], "value": new}

            out = {
                "type": "completed",
                "stock": sid,
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
                except Exception:
                    pass
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

    # Expose warrant_list.json
    @app.get("/warrant_list.json")
    async def get_bar_json():
        return FileResponse(
            ROOT / "warrant_list.json",
            media_type="application/json",
            headers={"Cache-Control": "public, max-age=3600"}
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

    @app.get("/history/{stock_id}")
    async def get_history(stock_id: str, limit: int = 500):
        """
        Return the most recent completed_ohlc for stock.
        This reads existing disk file (preferred) or uses in-memory snapshot if available.
        """
        # first attempt to read disk file (collector is authoritative)
        data_file = f"ohlc_{stock_id}.json"
        try:
            with open(data_file, "r") as f:
                data = json.load(f)
            # ensure we only return up to limit
            if limit and len(data) > limit:
                data = data[-limit:]
            return JSONResponse(content=data)
        except FileNotFoundError:
            # fallback to in-memory recent_bars
            lst = recent_bars.get(stock_id, [])
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

    return app


def main():
    # run as: python -m pacavanza.backend.main
    app = create_app(
        redis_url="redis://localhost:6379/0", redis_channel="pacavanza:ticker_updates"
    )
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")


if __name__ == "__main__":
    main()
