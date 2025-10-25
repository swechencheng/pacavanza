import asyncio
import random
import sys
import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import copy
import json
import logging
import time

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from flask import Flask
from avanza import Avanza
from avanza_sse_client import AvanzaSSEClient as SSEClient
import talib

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# --- OHLC Storage + lock ---
current_bars = defaultdict(dict)
completed_ohlc = defaultdict(list)
ohlc_lock = threading.Lock()

# --- Interval mapping ---
INTERVAL_MAP = {"10s": 10, "1m": 60, "5m": 300, "15m": 900, "1h": 3600}
MIN_BARS = 180  # minimal number of bars on x-axis

# --- Load secrets and warrant list ---
SECRET = json.load(open("secret.json"))
WARRANT_LIST = json.load(open("warrant_list.json"))

# --- Default values ---
INTERVAL_STR = "5m"  # default
INTERVAL_SECONDS = INTERVAL_MAP.get(INTERVAL_STR)  # default 5m
STOCK_ID = "TEST"
PORT = 8050  # default Dash port
QUOTE_BASE_URL = "https://www.avanza.se/_push/quote-web-push/"

# --- Parse command line args ---
args = sys.argv[1:]
i = 0
while i < len(args):
    if args[i] == "-i" and i + 1 < len(args):
        if args[i + 1] not in INTERVAL_MAP:
            LOGGER.error("Invalid interval. Choose from: 10s, 1m, 5m, 15m, 1h")
            sys.exit(1)
        INTERVAL_STR = args[i + 1]  # store the string
        INTERVAL_SECONDS = INTERVAL_MAP[args[i + 1]]
        i += 2
    elif args[i] == "-p" and i + 1 < len(args):
        try:
            PORT = int(args[i + 1])
        except ValueError:
            LOGGER.error("Port must be an integer")
            sys.exit(1)
        i += 2
    else:
        STOCK_ID = args[i]
        i += 1

LOGGER.info(f"Using STOCK_ID={STOCK_ID}, interval={INTERVAL_SECONDS}s, port={PORT}")

# Global variables to store the latest values
financing_level = None

USE_REAL_DATA = False if STOCK_ID not in WARRANT_LIST.keys() else True

if USE_REAL_DATA:
    WARRANT_ID = WARRANT_LIST.get(STOCK_ID, {}).get("ID")
    if WARRANT_ID is None:
        LOGGER.error("WARRANT_ID not found in warrant_list.json")
        exit(1)

# --- Data persistence ---
DATA_FILE = f"ohlc_{STOCK_ID}.json"
MAX_HISTORY_HOURS = 96


def load_ohlc_from_disk():
    try:
        with open(DATA_FILE, "r") as f:
            data = json.load(f)
        # Convert timestamps back to datetime
        bars = []
        for bar in data:
            bar["start_time"] = datetime.fromisoformat(bar["start_time"])
            bar["end_time"] = datetime.fromisoformat(bar["end_time"])
            bars.append(bar)
        # Keep only recent 96h
        cutoff = datetime.now(timezone.utc) - timedelta(hours=MAX_HISTORY_HOURS)
        bars = [b for b in bars if b["end_time"] >= cutoff]
        completed_ohlc[STOCK_ID] = bars
        LOGGER.info(f"Loaded {len(bars)} bars from {DATA_FILE}")
    except FileNotFoundError:
        LOGGER.info(f"No previous OHLC data found for {STOCK_ID}. Starting fresh.")
    except Exception as e:
        LOGGER.error(f"Failed to load OHLC data: {e}")


def save_ohlc_to_disk():
    try:
        with ohlc_lock:
            bars = copy.deepcopy(completed_ohlc[STOCK_ID])
        data = []
        for b in bars:
            bar = b.copy()
            bar["start_time"] = bar["start_time"].isoformat()
            bar["end_time"] = bar["end_time"].isoformat()
            data.append(bar)
        with open(DATA_FILE, "w") as f:
            json.dump(data, f)
        LOGGER.debug(f"Saved {len(data)} bars to {DATA_FILE}")
    except Exception as e:
        LOGGER.error(f"Failed to save OHLC data: {e}")


# --- Initialize new bar ---
def initialize_new_bar(timestamp, price, interval_sec):
    seconds_since_midnight = (
        timestamp.hour * 3600 + timestamp.minute * 60 + timestamp.second
    )
    floored = (seconds_since_midnight // interval_sec) * interval_sec
    bar_start = timestamp.replace(
        hour=0, minute=0, second=0, microsecond=0
    ) + timedelta(seconds=floored)

    current_bars[STOCK_ID] = {
        "start_time": bar_start,
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "end_time": bar_start + timedelta(seconds=interval_sec),
    }


# --- Update OHLC bar ---
def update_ohlc_bar(price, timestamp):
    with ohlc_lock:
        current_bar = current_bars.get(STOCK_ID)
        if current_bar is None or timestamp >= current_bar["end_time"]:
            if current_bar is not None:
                completed_ohlc[STOCK_ID].append(current_bar.copy())

                # --- cleanup step: keep only last 96h ---
                cutoff = datetime.now(timezone.utc) - timedelta(hours=MAX_HISTORY_HOURS)
                completed_ohlc[STOCK_ID] = [
                    bar for bar in completed_ohlc[STOCK_ID] if bar["end_time"] >= cutoff
                ]

            initialize_new_bar(timestamp, price, INTERVAL_SECONDS)
        else:
            current_bar["high"] = max(current_bar["high"], price)
            current_bar["low"] = min(current_bar["low"], price)
            current_bar["close"] = price


# --- Calculate EMA20 using TA-Lib ---
def calculate_ema20_talib(df):
    """Calculate EMA20 using TA-Lib"""
    if len(df) < 20:
        return []

    # Extract closing prices as numpy array for TA-Lib
    closes = np.array(df["close"].tolist(), dtype=float)

    # Calculate EMA20 using TA-Lib
    ema20 = talib.EMA(closes, timeperiod=20)

    return ema20.tolist() if ema20 is not None else []


# --- Async price generator ---
async def generate_stock_price(start_price=100.0):
    price = round(start_price, 2)
    while True:
        await asyncio.sleep(random.uniform(0.6, 1.8))
        dt = datetime.now(timezone.utc)
        update_ohlc_bar(price, dt)
        change_factor = random.uniform(0.9, 1.111111)
        price = round(price * change_factor, 2)
        price = max(1.00, min(price, 300.00))


async def callback_quote_web_push(id, event, data):
    """
    This runs in the SSE callback from Avanza.
    Defensively handles parsing errors and ensures `dt` is always set.
    An example QUOTE event callback:
    [RdvXmj1XLHFj_AEZKkiSmbcFx] [QUOTE] {'orderbookId': '2026354', 'buyPrice': 201.57, 'sellPrice': 201.63, 'closingPrice': 204.51, 'highestPrice': 201.98, 'lowestPrice': 200.25, 'lastPrice': 201.98, 'totalValueTraded': 21043.55, 'totalVolumeTraded': 105, 'change': -2.53, 'changePercent': -0.0124, 'spreadPercent': 0.0003, 'volumeWeightedAveragePrice': 200.41, 'updated': '2025-10-17T09:54:00.916Z', 'lastPriceUpdated': '2025-10-17T09:54:00.000Z'}
    """
    try:
        # Check if data is a dict or not
        if event != "QUOTE" or not isinstance(data, dict):
            LOGGER.debug(f"[{id}] [{event}] {data}")
            return

        ts = data.get("updated")

        # default fallback timestamp (timezone-aware)
        dt = datetime.now(timezone.utc)

        # format readable timestamp from the message if needed
        if ts:
            try:
                parsed = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f%z")
                dt = parsed.astimezone(timezone.utc)
                milli = dt.microsecond // 1000
                readable_ts = f"{dt.strftime('%Y-%m-%d %H:%M:%S')}.{milli:03d} {dt.strftime('%Z')}"
            except Exception:
                # keep dt as fallback (now in UTC)
                readable_ts = str(ts)
        else:
            readable_ts = "(no timestamp)"

        buy_price = data.get("buyPrice")
        sell_price = data.get("sellPrice")

        # Ensure valid and consistent MM detection
        if buy_price is None or sell_price is None:
            LOGGER.warning(
                f"{readable_ts} - Valid buy/sell price not found (buy={buy_price}, sell={sell_price})"
            )
            return

        LOGGER.info(f"{readable_ts} B: {buy_price:.2f}  S: {sell_price:.2f}")
        # `dt` guaranteed to be defined (UTC)
        update_ohlc_bar(buy_price, dt)

    except Exception as e:
        # Catch *anything* so this callback never bubbles an exception to the websocket loop.
        # Keep the log message small but informative.
        LOGGER.error(f"Exception in callback_quote_web_push: {e!r}")


async def real_market_loop():
    while True:
        avanza = None
        try:
            global financing_level
            avanza = Avanza(
                {
                    "username": SECRET["username"],
                    "password": SECRET["password"],
                    "totpSecret": SECRET["totpSecret"],
                }
            )
            warrant_info = avanza.get_warrant_info(WARRANT_ID)
            underlying_id = warrant_info.get("underlying", {}).get("orderbookId")
            financing_level = warrant_info.get("keyIndicators", {}).get(
                "financingLevel"
            )
            if underlying_id is None:
                LOGGER.warning("Failed to get underlying ID")
            if financing_level is None:
                LOGGER.warning("Failed to get financing level")
                return
            financing_level = float(financing_level)
            LOGGER.info(f"Financing Level: {financing_level}")
            client = SSEClient(avanza, QUOTE_BASE_URL + WARRANT_ID)
            client.add_listener(callback_quote_web_push)
            await client.start()
        except Exception as e:
            LOGGER.error(
                f"Error occurred in real_market_loop: {e}. Reconnecting in 5 seconds..."
            )
            await asyncio.sleep(5)
        finally:
            # if avanza has graceful close/shutdown API, call it here to clean internal tasks
            try:
                if avanza is not None and hasattr(avanza, "close"):
                    await avanza.close()
            except Exception:
                pass
            await asyncio.sleep(0.1)


# --- Periodic saver ---
async def periodic_saver():
    while True:
        await asyncio.sleep(INTERVAL_SECONDS)
        save_ohlc_to_disk()


# --- Background loop ---
def start_background_loop(loop):
    asyncio.set_event_loop(loop)

    def handle_loop_exception(loop, context):
        LOGGER.error("Asyncio loop exception:", context)

    loop.set_exception_handler(handle_loop_exception)

    # Start both price generator / market loop and saver
    if not USE_REAL_DATA:
        tasks = [generate_stock_price()]  # Don't save simulated data
    else:
        tasks = [real_market_loop(), periodic_saver()]

    loop.run_until_complete(asyncio.gather(*tasks))


# --- Dash App ---
logging.getLogger("werkzeug").setLevel(logging.ERROR)
server = Flask(__name__)
app = Dash(__name__, server=server)

app.layout = html.Div(
    [
        # --- Header with realtime clock ---
        html.Div(
            [
                html.Div(
                    id="live-clock",
                    children=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    style={
                        "textAlign": "center",
                        "fontSize": "18px",
                        "color": "blue",
                        "padding": "3px",
                        "width": "99%",
                    },
                ),
                dcc.Interval(
                    id="clock-interval",
                    interval=1000,  # update every second
                    n_intervals=0,
                ),
            ]
        ),
        # --- OHLC chart ---
        dcc.Graph(id="ohlc-chart"),
        dcc.Interval(id="interval-component", interval=100, n_intervals=0),
    ]
)


@app.callback(Output("live-clock", "children"), Input("clock-interval", "n_intervals"))
def update_clock(n):
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@app.callback(
    Output("ohlc-chart", "figure"), Input("interval-component", "n_intervals")
)
def update_chart(n):
    with ohlc_lock:
        completed = list(completed_ohlc[STOCK_ID])
        current = (
            copy.deepcopy(current_bars.get(STOCK_ID))
            if current_bars.get(STOCK_ID)
            else None
        )

    df = pd.DataFrame(completed) if completed else pd.DataFrame()
    if current is not None:
        df = pd.concat([df, pd.DataFrame([current])], ignore_index=True)

    if df.empty:
        return go.Figure()

    # Sort and keep latest MIN_BARS
    df = df.sort_values("start_time").iloc[-MIN_BARS:].reset_index(drop=True)

    # Calculate EMA20 using TA-Lib - only if we have enough bars
    ema20_values = []
    if len(df) >= 20:
        ema20_values = calculate_ema20_talib(df)

    # Convert to local time (optional)
    local_tz = timezone(
        timedelta(seconds=-time.timezone if time.daylight == 0 else -time.altzone)
    )
    df["start_time"] = df["start_time"].dt.tz_convert(local_tz)

    # Sequential index for x-axis
    df["bar_index"] = range(len(df))

    # Build figure
    fig = go.Figure(
        data=[
            go.Candlestick(
                x=df["bar_index"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
                increasing_line_color="green",
                decreasing_line_color="red",
                showlegend=False,
                text=df["start_time"].dt.strftime("%Y-%m-%d %H:%M"),
            )
        ]
    )

    # Add EMA20 line only if we have enough data
    if len(ema20_values) > 0:
        fig.add_trace(
            go.Scatter(
                x=df["bar_index"],
                y=ema20_values,
                mode="lines",
                line=dict(color="blue", width=1.5),
                name="EMA20",
                hoverinfo="y",
            )
        )

    # Proper hover text via update_traces
    fig.update_traces(
        hoverinfo="text",
        hovertext=[
            f"Time: {t}<br>O: {o:.2f}<br>H: {h:.2f}<br>L: {l:.2f}<br>C: {c:.2f}"
            for t, o, h, l, c in zip(
                df["start_time"].dt.strftime("%Y-%m-%d %H:%M"),
                df["open"],
                df["high"],
                df["low"],
                df["close"],
            )
        ],
        selector=dict(type="candlestick"),  # Only apply to candlestick
    )

    # X-axis tick labels = timestamps (no gaps)
    tick_step = max(1, len(df) // 10)
    fig.update_xaxes(
        tickmode="array",
        tickvals=df["bar_index"][::tick_step],
        ticktext=df["start_time"].dt.strftime("%H:%M")[::tick_step],
        title_text="Bars (continuous, skips closed hours)",
    )

    fig.update_layout(
        title=f"{STOCK_ID} ({INTERVAL_STR})",
        xaxis_rangeslider_visible=False,
        template="plotly_dark",
        margin=dict(l=10, r=10, t=30, b=30),
        showlegend=True,
        legend=dict(
            x=0,
            y=1,
            traceorder="normal",
            font=dict(size=10),
        ),
    )
    return fig


if __name__ == "__main__":
    load_ohlc_from_disk()
    new_loop = asyncio.new_event_loop()
    t = threading.Thread(target=start_background_loop, args=(new_loop,), daemon=True)
    t.start()
    app.run(debug=False, use_reloader=False, port=PORT)
