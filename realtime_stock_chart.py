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
from avanza import Avanza, ChannelType
from websockets.exceptions import ConnectionClosedError

# --- CONFIG: timeouts (seconds) ---
SUBSCRIBE_TIMEOUT = 30   # if subscribe_to_channel doesn't confirm within this, restart
WATCHDOG_TIMEOUT = 60    # if no data for this many seconds, reconnect/re-subscribe

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

# --- Load secrets and warrant list ---
secret = json.load(open("secret.json"))
warrant_list = json.load(open("warrant_list.json"))

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

# Global variables to store the latest values
financing_level = None

USE_REAL_DATA = False if STOCK_ID not in warrant_list.keys() else True

if USE_REAL_DATA:
    WARRANT_ID = warrant_list.get(STOCK_ID, {}).get("ID")
    if WARRANT_ID is None:
        print("WARRANT_ID not found in warrant_list.json")
        exit(1)

# --- Health / coordination primitives (will be created on the event loop) ---
# Note: created inside start_background_loop() so they're bound to the correct loop.
last_data_time = None        # updated by callback_orderdepths() when new data arrives
stop_watchdog = None         # asyncio.Event -> created later
subscription_ready_event = None  # asyncio.Event -> created later

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
    global last_data_time
    with ohlc_lock:
        current_bar = current_bars.get(STOCK_ID)
        if current_bar is None or timestamp >= current_bar["end_time"]:
            if current_bar is not None:
                completed_ohlc[STOCK_ID].append(current_bar.copy())

                # --- cleanup step: keep only last 96h ---
                cutoff = datetime.now(timezone.utc) - timedelta(hours=96)
                completed_ohlc[STOCK_ID] = [
                    bar for bar in completed_ohlc[STOCK_ID]
                    if bar["end_time"] >= cutoff
                ]

            initialize_new_bar(timestamp, price, interval_seconds)
        else:
            current_bar["high"] = max(current_bar["high"], price)
            current_bar["low"] = min(current_bar["low"], price)
            current_bar["close"] = price

    # --- mark we received something (heartbeat for watchdog) ---
    last_data_time = datetime.now(timezone.utc)

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

def callback_orderdepths(data):
    """
    This runs in the websocket callback from Avanza.
    Defensively handles parsing errors and ensures `dt` is always set.
    Any unexpected exception is caught and logged so it won't kill the websocket task.
    """
    try:
        d = data.get("data", {})
        ts = d.get("receivedTime")

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

        levels = d.get("levels", [])
        if not levels:
            print(f"{readable_ts} - No levels data")
            return

        # Track the max volume sides
        max_buy = {"volume": 0, "price": None}
        max_sell = {"volume": 0, "price": None}

        for level in levels:
            buy_side = level.get("buySide", {})
            sell_side = level.get("sellSide", {})

            bv = buy_side.get("volume", 0) or 0
            sv = sell_side.get("volume", 0) or 0

            # price may be string -> try convert defensively
            try:
                bprice = float(buy_side.get("price")) if buy_side.get("price") is not None else None
            except Exception:
                bprice = None

            try:
                sprice = float(sell_side.get("price")) if sell_side.get("price") is not None else None
            except Exception:
                sprice = None

            if bv and bv > max_buy["volume"]:
                max_buy["volume"] = bv
                max_buy["price"] = bprice

            if sv and sv > max_sell["volume"]:
                max_sell["volume"] = sv
                max_sell["price"] = sprice

        # Ensure valid and consistent MM detection
        if (
            max_buy["price"] is None
            or max_sell["price"] is None
            or max_buy["volume"] <= 0
            or max_sell["volume"] <= 0
        ):
            print(f"{readable_ts} - Valid buy/sell price not found (buy={max_buy}, sell={max_sell})")
            return

        if max_buy["volume"] != max_sell["volume"]:
            print(f"{readable_ts} - Volume mismatch: Buy {max_buy['volume']} vs Sell {max_sell['volume']}")
            return

        print(f"{readable_ts} B: {max_buy['price']:.2f}  S: {max_sell['price']:.2f}")
        # `dt` guaranteed to be defined (UTC)
        update_ohlc_bar(max_buy['price'], dt)

    except Exception as exc:
        # Catch *anything* so this callback never bubbles an exception to the websocket loop.
        # Keep the print/log message small but informative.
        print(f"Exception in callback_orderdepths: {exc!r}")

async def subscribe_to_channel(avanza: Avanza):
    """
    Subscribes to the ORDERDEPTHS channel using avanza.subscribe_to_id(...) and then
    runs a lightweight keep-alive loop to allow exceptions to bubble up from the Avanza internals.

    IMPORTANT: sets subscription_ready_event after subscribe_to_id returns so supervisor can continue.
    """
    global financing_level, subscription_ready_event

    warrant_info = avanza.get_warrant_info(WARRANT_ID)
    underlying_id = warrant_info.get('underlying', {}).get('orderbookId')
    financing_level = warrant_info.get('keyIndicators', {}).get('financingLevel')
    if underlying_id is None:
        print("Failed to get underlying ID")
    if financing_level is None:
        print("Failed to get financing level")
        return
    financing_level = float(financing_level)
    print(f"Financing Level: {financing_level}")

    # perform the subscribe call (this usually returns quickly)
    await avanza.subscribe_to_id(
        ChannelType.ORDERDEPTHS,
        WARRANT_ID,
        callback_orderdepths
    )

    # signal the supervisor that subscribe_to_id returned
    try:
        if subscription_ready_event is not None and not subscription_ready_event.is_set():
            subscription_ready_event.set()
    except Exception:
        pass

    # keep this coroutine alive so exceptions inside Avanza internals can surface here
    while True:
        await asyncio.sleep(1)  # keep it alive, but allow exceptions to bubble up

# --- Watchdog & resilient loop ---
async def watchdog_loop():
    """Watchdog that ensures data keeps flowing; reconnects if no data received for too long."""
    global last_data_time, stop_watchdog
    print(f"[watchdog] Started with timeout={WATCHDOG_TIMEOUT}s")
    while True:
        await asyncio.sleep(5)
        if stop_watchdog is not None and stop_watchdog.is_set():
            # somebody already requested restart, just stop
            break
        if last_data_time is None:
            continue
        elapsed = (datetime.now(timezone.utc) - last_data_time).total_seconds()
        if elapsed > WATCHDOG_TIMEOUT:
            print(f"[watchdog] No data for {elapsed:.0f}s, requesting reconnect...")
            # signal the resilient loop to reconnect
            if stop_watchdog is not None:
                stop_watchdog.set()
            break

async def resilient_loop():
    """Main resilient loop that handles Avanza connection and watchdog restarts."""
    global last_data_time, stop_watchdog, subscription_ready_event

    while True:
        # prepare for a new session
        if stop_watchdog is not None:
            try:
                stop_watchdog.clear()
            except Exception:
                # defensive if event not created or already cleared
                pass

        last_data_time = datetime.now(timezone.utc)
        print("[resilient] Starting new Avanza session...")

        avanza = None
        sub_task = None
        wd_task = None

        try:
            avanza = Avanza({
                'username': secret['username'],
                'password': secret['password'],
                'totpSecret': secret['totpSecret']
            })

            # clear and prepare the subscription_ready_event
            if subscription_ready_event is not None:
                try:
                    subscription_ready_event.clear()
                except Exception:
                    pass

            # start the subscription as a background task and supervise it
            sub_task = asyncio.create_task(subscribe_to_channel(avanza))

            # done-callback to explicitly retrieve exception (avoid "Task exception was never retrieved")
            def _sub_done_cb(t: asyncio.Task):
                try:
                    exc = t.exception()
                    if exc:
                        print(f"[subscription task] finished with exception: {exc!r}")
                    else:
                        print("[subscription task] finished (no exception).")
                except asyncio.CancelledError:
                    print("[subscription task] was cancelled.")
                except Exception as e:
                    print(f"[subscription task] done-callback exception: {e!r}")

            sub_task.add_done_callback(_sub_done_cb)

            # wait up to SUBSCRIBE_TIMEOUT for the subscribe_to_id call to return (i.e. subscription_ready_event)
            if subscription_ready_event is None:
                # defensive fallback: if event isn't created, wait briefly for the subscribe coroutine to yield
                try:
                    await asyncio.wait_for(asyncio.sleep(0.1), timeout=SUBSCRIBE_TIMEOUT)
                except asyncio.TimeoutError:
                    pass
            else:
                try:
                    print(f"[resilient] Waiting up to {SUBSCRIBE_TIMEOUT}s for subscription confirmation...")
                    await asyncio.wait_for(subscription_ready_event.wait(), timeout=SUBSCRIBE_TIMEOUT)
                    print("[resilient] Subscription confirmed (subscribe_to_id returned).")
                except asyncio.TimeoutError:
                    print(f"[resilient] subscribe_to_channel did not confirm within {SUBSCRIBE_TIMEOUT}s. Cancelling and restarting...")
                    # cancel subscription and close avanza, then retry loop
                    if sub_task is not None and not sub_task.done():
                        sub_task.cancel()
                        try:
                            await asyncio.wait_for(sub_task, timeout=1.0)
                        except asyncio.CancelledError:
                            pass
                        except Exception:
                            pass
                    # try close avanza if available
                    try:
                        if avanza is not None and hasattr(avanza, "close"):
                            maybe = avanza.close()
                            if asyncio.iscoroutine(maybe):
                                await maybe
                    except Exception:
                        pass
                    await asyncio.sleep(1)
                    continue

            # start watchdog now that subscription is established
            if stop_watchdog is not None:
                wd_task = asyncio.create_task(watchdog_loop())
            else:
                wd_task = asyncio.create_task(watchdog_loop())

            # wait until either: subscription task finishes, or watchdog requests restart
            wait_set = {sub_task}
            if stop_watchdog is not None:
                ev_wait = asyncio.create_task(stop_watchdog.wait())
                wait_set.add(ev_wait)
            else:
                ev_wait = None

            done, pending = await asyncio.wait(wait_set, return_when=asyncio.FIRST_COMPLETED)

            # if subscription finished on its own
            if sub_task in done:
                print("[resilient] Subscription task ended; will restart.")
            # if the watchdog requested restart
            if ev_wait is not None and ev_wait in done:
                print("[resilient] Watchdog requested restart; will restart.")

            # attempt graceful shutdown of subscription
            if sub_task is not None and not sub_task.done():
                try:
                    sub_task.cancel()
                    try:
                        await asyncio.wait_for(sub_task, timeout=1.0)
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        pass
                except Exception:
                    pass

            # try to close avanza (handle sync or async close)
            try:
                if avanza is not None and hasattr(avanza, "close"):
                    maybe = avanza.close()
                    if asyncio.iscoroutine(maybe):
                        await maybe
            except Exception:
                pass

        except asyncio.CancelledError:
            print("[resilient] Loop cancelled gracefully.")
            break
        except (ConnectionClosedError, TimeoutError) as e:
            print(f"[resilient] Connection closed/error: {e!r}. Reconnecting in 5s...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"[resilient] Unexpected error: {e!r}. Reconnecting in 5s...")
            await asyncio.sleep(5)
        finally:
            # cancel watchdog if still running
            try:
                if wd_task is not None and not wd_task.done():
                    wd_task.cancel()
                    try:
                        await wd_task
                    except asyncio.CancelledError:
                        pass
            except Exception:
                pass

            print("[resilient] Restarting session...")
            # short pause to avoid tight restart loop
            await asyncio.sleep(0.1)

# --- Background loop ---
def start_background_loop(loop):
    """Starts the asyncio loop in a dedicated thread."""
    global stop_watchdog, subscription_ready_event

    asyncio.set_event_loop(loop)

    # create loop-local events here (bound to this event loop)
    stop_watchdog = asyncio.Event()
    subscription_ready_event = asyncio.Event()

    def handle_loop_exception(loop, context):
        print("Asyncio loop exception:", context)
        # we just log it; the resilient loop will notice subscription task end or watchdog will trigger

    loop.set_exception_handler(handle_loop_exception)

    try:
        if not USE_REAL_DATA:
            loop.run_until_complete(generate_stock_price())  # simulated mode
        else:
            loop.run_until_complete(resilient_loop())        # real mode
    except asyncio.CancelledError:
        print("[background_loop] Cancelled gracefully.")
    except Exception as e:
        print(f"[background_loop] Unhandled exception: {e!r}")

# --- Dash App ---
logging.getLogger('werkzeug').setLevel(logging.ERROR)
server = Flask(__name__)
app = Dash(__name__, server=server)

app.layout = html.Div([

    # --- Header with realtime clock ---
    html.Div([
        html.Div(
            id="live-clock",
            children=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            style={
                'textAlign': 'center',
                'fontSize': '18px',
                'color': 'blue',
                'padding': '3px',
                'width': '99%'
            }
        ),
        dcc.Interval(
            id="clock-interval",
            interval=1000,  # update every second
            n_intervals=0
        )
    ]),

    # --- OHLC chart ---
    dcc.Graph(id="ohlc-chart"),
    dcc.Interval(id="interval-component", interval=3000, n_intervals=0)
])

@app.callback(
    Output('live-clock', 'children'),
    Input('clock-interval', 'n_intervals')
)
def update_clock(n):
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

@app.callback(
    Output("ohlc-chart", "figure"),
    Input("interval-component", "n_intervals")
)
def update_chart(n):
    MIN_BARS = 90  # minimal number of bars on x-axis
    with ohlc_lock:
        completed = list(completed_ohlc[STOCK_ID])
        current = copy.deepcopy(current_bars.get(STOCK_ID)) if current_bars.get(STOCK_ID) else None

    df = pd.DataFrame(completed) if completed else pd.DataFrame()
    if current is not None:
        df = pd.concat([df, pd.DataFrame([current])], ignore_index=True)

    interval_sec = interval_seconds

    if df.empty:
        # create empty placeholder dataframe with 90 bars
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(seconds=interval_sec * MIN_BARS)
        df = pd.DataFrame([{
            "start_time": start_time + timedelta(seconds=i*interval_sec),
            "open": None,
            "high": None,
            "low": None,
            "close": None
        } for i in range(MIN_BARS)])
    else:
        df["start_time"] = pd.to_datetime(df["start_time"], utc=True)
        df = df.sort_values("start_time")

        # pad at the beginning if fewer than MIN_BARS
        if len(df) < MIN_BARS:
            missing = MIN_BARS - len(df)
            first_time = df["start_time"].iloc[0] - pd.to_timedelta(interval_sec * missing, unit='s')

            # ensure the dtype matches the real data
            pad_df = pd.DataFrame({
                "start_time": [first_time + pd.to_timedelta(interval_sec * i, unit='s') for i in range(missing)],
                "open": [np.nan] * missing,
                "high": [np.nan] * missing,
                "low": [np.nan] * missing,
                "close": [np.nan] * missing
            })

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
                increasing_line_color='green',
                decreasing_line_color='red',
                showlegend=False
            )
        ]
    )
    fig.update_layout(
        title=f"{STOCK_ID} ({INTERVAL_STR})",
        xaxis_rangeslider_visible=False,
        template="plotly_dark"
    )
    return fig

if __name__ == "__main__":
    new_loop = asyncio.new_event_loop()
    t = threading.Thread(target=start_background_loop, args=(new_loop,), daemon=True)
    t.start()
    app.run(debug=False, use_reloader=False, port=PORT)
