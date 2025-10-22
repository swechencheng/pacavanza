import asyncio
import random
import sys
import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import copy
import json
import logging

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from flask import Flask
from avanza import Avanza
from avanza_sse_client import AvanzaSSEClient as SSEClient

# --- OHLC Storage + lock ---
current_bars = defaultdict(dict)
completed_ohlc = defaultdict(list)
ohlc_lock = threading.Lock()

# --- Interval mapping ---
INTERVAL_MAP = {"10s": 10, "1m": 60, "5m": 300, "15m": 900, "1h": 3600}

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
            print("Invalid interval. Choose from: 10s, 1m, 5m, 15m, 1h")
            sys.exit(1)
        INTERVAL_STR = args[i + 1]  # store the string
        INTERVAL_SECONDS = INTERVAL_MAP[args[i + 1]]
        i += 2
    elif args[i] == "-p" and i + 1 < len(args):
        try:
            PORT = int(args[i + 1])
        except ValueError:
            print("Port must be an integer")
            sys.exit(1)
        i += 2
    else:
        STOCK_ID = args[i]
        i += 1

print(f"Using STOCK_ID={STOCK_ID}, interval={INTERVAL_SECONDS}s, port={PORT}")

# Global variables to store the latest values
financing_level = None

USE_REAL_DATA = False if STOCK_ID not in WARRANT_LIST.keys() else True

if USE_REAL_DATA:
    WARRANT_ID = WARRANT_LIST.get(STOCK_ID, {}).get("ID")
    if WARRANT_ID is None:
        print("WARRANT_ID not found in warrant_list.json")
        exit(1)


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
                cutoff = datetime.now(timezone.utc) - timedelta(hours=96)
                completed_ohlc[STOCK_ID] = [
                    bar for bar in completed_ohlc[STOCK_ID] if bar["end_time"] >= cutoff
                ]

            initialize_new_bar(timestamp, price, INTERVAL_SECONDS)
        else:
            current_bar["high"] = max(current_bar["high"], price)
            current_bar["low"] = min(current_bar["low"], price)
            current_bar["close"] = price


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
            print(f"[{id}] [{event}] {data}")
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
            print(
                f"{readable_ts} - Valid buy/sell price not found (buy={buy_price}, sell={sell_price})"
            )
            return

        print(f"{readable_ts} B: {buy_price:.2f}  S: {sell_price:.2f}")
        # `dt` guaranteed to be defined (UTC)
        update_ohlc_bar(buy_price, dt)

    except Exception as e:
        # Catch *anything* so this callback never bubbles an exception to the websocket loop.
        # Keep the print/log message small but informative.
        print(f"Exception in callback_quote_web_push: {e!r}")


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
                print("Failed to get underlying ID")
            if financing_level is None:
                print("Failed to get financing level")
                return
            financing_level = float(financing_level)
            print(f"Financing Level: {financing_level}")
            client = SSEClient(avanza, QUOTE_BASE_URL + WARRANT_ID)
            client.add_listener(callback_quote_web_push)
            await client.start()
        except Exception as e:
            print(
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


# --- Background loop ---
def start_background_loop(loop):
    asyncio.set_event_loop(loop)

    def handle_loop_exception(loop, context):
        print("Asyncio loop exception:", context)

    loop.set_exception_handler(handle_loop_exception)

    if not USE_REAL_DATA:
        loop.run_until_complete(generate_stock_price())
    else:
        loop.run_until_complete(real_market_loop())


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
        dcc.Interval(id="interval-component", interval=3000, n_intervals=0),
    ]
)


@app.callback(Output("live-clock", "children"), Input("clock-interval", "n_intervals"))
def update_clock(n):
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@app.callback(
    Output("ohlc-chart", "figure"), Input("interval-component", "n_intervals")
)
def update_chart(n):
    MIN_BARS = 90  # minimal number of bars on x-axis
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

    interval_sec = INTERVAL_SECONDS

    if df.empty:
        # create empty placeholder dataframe with 90 bars
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(seconds=interval_sec * MIN_BARS)
        df = pd.DataFrame(
            [
                {
                    "start_time": start_time + timedelta(seconds=i * interval_sec),
                    "open": None,
                    "high": None,
                    "low": None,
                    "close": None,
                }
                for i in range(MIN_BARS)
            ]
        )
    else:
        df["start_time"] = pd.to_datetime(df["start_time"], utc=True)
        df = df.sort_values("start_time")

        # pad at the beginning if fewer than MIN_BARS
        if len(df) < MIN_BARS:
            missing = MIN_BARS - len(df)
            first_time = df["start_time"].iloc[0] - pd.to_timedelta(
                interval_sec * missing, unit="s"
            )

            # ensure the dtype matches the real data
            pad_df = pd.DataFrame(
                {
                    "start_time": [
                        first_time + pd.to_timedelta(interval_sec * i, unit="s")
                        for i in range(missing)
                    ],
                    "open": [np.nan] * missing,
                    "high": [np.nan] * missing,
                    "low": [np.nan] * missing,
                    "close": [np.nan] * missing,
                }
            )

            df = pd.concat([pad_df, df], ignore_index=True)

        # sliding window: keep only the latest MIN_BARS
        df = df.iloc[-MIN_BARS:]

    fig = go.Figure(
        data=[
            go.Candlestick(
                x=df["start_time"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
                increasing_line_color="green",
                decreasing_line_color="red",
                showlegend=False,
            )
        ]
    )
    fig.update_layout(
        title=f"{STOCK_ID} ({INTERVAL_STR})",
        xaxis_rangeslider_visible=False,
        template="plotly_dark",
    )
    return fig


if __name__ == "__main__":
    new_loop = asyncio.new_event_loop()
    t = threading.Thread(target=start_background_loop, args=(new_loop,), daemon=True)
    t.start()
    app.run(debug=False, use_reloader=False, port=PORT)
