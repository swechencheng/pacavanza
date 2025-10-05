import asyncio
import random
import sys
import threading
from collections import defaultdict
from datetime import datetime, timedelta
import copy

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from flask import Flask

# --- OHLC Storage + lock ---
current_bars = defaultdict(dict)
completed_ohlc = defaultdict(list)
ohlc_lock = threading.Lock()

# --- Interval mapping ---
INTERVAL_MAP = {
    "10s": 10,
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600
}

# --- Default values ---
INTERVAL_STR = "5m"  # default
interval_seconds = INTERVAL_MAP.get("5m")  # default 5m
STOCK_ID = "TEST"
PORT = 8050  # default Dash port

# --- Parse command line args ---
args = sys.argv[1:]
i = 0
while i < len(args):
    if args[i] == "-i" and i + 1 < len(args):
        if args[i+1] not in INTERVAL_MAP:
            print("Invalid interval. Choose from: 10s, 1m, 5m, 15m, 1h")
            sys.exit(1)
        INTERVAL_STR = args[i+1]   # store the string
        interval_seconds = INTERVAL_MAP[args[i+1]]
        i += 2
    elif args[i] == "-p" and i + 1 < len(args):
        try:
            PORT = int(args[i+1])
        except ValueError:
            print("Port must be an integer")
            sys.exit(1)
        i += 2
    else:
        STOCK_ID = args[i]
        i += 1

print(f"Using STOCK_ID={STOCK_ID}, interval={interval_seconds}s, port={PORT}")

# --- Initialize new bar ---
def initialize_new_bar(timestamp, price, interval_sec):
    seconds_since_midnight = timestamp.hour * 3600 + timestamp.minute * 60 + timestamp.second
    floored = (seconds_since_midnight // interval_sec) * interval_sec
    bar_start = timestamp.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=floored)

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
            initialize_new_bar(timestamp, price, interval_seconds)
        else:
            current_bar["high"] = max(current_bar["high"], price)
            current_bar["low"] = min(current_bar["low"], price)
            current_bar["close"] = price

# --- Async price generator ---
async def generate_stock_price(start_price=100.0):
    price = round(start_price, 2)
    while True:
        await asyncio.sleep(random.uniform(0.6, 1.8))
        dt = datetime.now()
        update_ohlc_bar(price, dt)
        change_factor = random.uniform(0.9, 1.111111)
        price = round(price * change_factor, 2)
        price = max(1.00, min(price, 300.00))

# --- Background loop ---
def start_background_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_until_complete(generate_stock_price())

# --- Dash App ---
server = Flask(__name__)
app = Dash(__name__, server=server)

app.layout = html.Div([
    dcc.Graph(id="ohlc-chart"),
    dcc.Interval(id="interval-component", interval=3000, n_intervals=0)
])

@app.callback(
    Output("ohlc-chart", "figure"),
    Input("interval-component", "n_intervals")
)
def update_chart(n):
    with ohlc_lock:
        completed = list(completed_ohlc[STOCK_ID])
        current = copy.deepcopy(current_bars.get(STOCK_ID)) if current_bars.get(STOCK_ID) else None

    df = pd.DataFrame(completed) if completed else pd.DataFrame()
    if current is not None:
        df = pd.concat([df, pd.DataFrame([current])], ignore_index=True)

    if df.empty:
        return go.Figure()

    df["start_time"] = pd.to_datetime(df["start_time"])
    df = df.sort_values("start_time")

    fig = go.Figure(
        data=[
            go.Candlestick(
                x=df["start_time"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"]
            )
        ]
    )
    fig.update_layout(title=f"Realtime OHLC: {STOCK_ID} ({INTERVAL_STR})",
                    xaxis_rangeslider_visible=False,
                    template="plotly_dark")
    return fig

if __name__ == "__main__":
    new_loop = asyncio.new_event_loop()
    t = threading.Thread(target=start_background_loop, args=(new_loop,), daemon=True)
    t.start()
    app.run(debug=False, use_reloader=False, port=PORT)
