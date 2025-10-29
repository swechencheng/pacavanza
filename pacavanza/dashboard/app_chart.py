import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, time
from zoneinfo import ZoneInfo
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from flask import Flask
import json
import logging
import talib

logging.getLogger(__name__).setLevel(logging.INFO)
LOGGER = logging.getLogger(__name__)

MIN_BARS = 180  # Number of bars to display on chart
EMA_PERIOD = 20  # EMA period (used to keep extra history for calculation)
C_CONTADOR = 2  # Interval for bar counter labels


def load_warrant_info(path="./pacavanza/warrant_list.json"):
    with open(path, "r") as f:
        return json.load(f)


def is_trading_hour(
    ts: datetime, tz_name: str, market_open: str, market_close: str
) -> bool:
    """
    Returns True if `ts` is within market hours in local timezone.
    """
    tz = ZoneInfo(tz_name)
    local_ts = ts.astimezone(tz)

    open_h, open_m = map(int, market_open.split(":"))
    close_h, close_m = map(int, market_close.split(":"))
    open_t, close_t = time(open_h, open_m), time(close_h, close_m)

    return open_t <= local_ts.timetz().replace(tzinfo=None) < close_t


class ChartApp:
    """
    ChartApp now accepts either a single StockData or a list of StockData objects.
    It renders one chart per StockData.stock_id and registers one callback per chart.
    """

    def __init__(
        self,
        stock_datas,
        interval_str="5m",
        port=8050,
        warrant_info_path="./pacavanza/warrant_list.json",
    ):
        self.warrant_info = load_warrant_info(warrant_info_path)
        # accept a single StockData or list
        if isinstance(stock_datas, (list, tuple)):
            self.stock_datas = stock_datas
        else:
            self.stock_datas = [stock_datas]

        self.interval_str = interval_str
        self.port = port
        self.server = Flask(__name__)
        self.app = Dash(__name__, server=self.server)
        self._setup_layout()
        self._setup_callbacks()

    def _setup_layout(self):
        # header (clock)
        graphs = []
        for sd in self.stock_datas:
            sid = sd.stock_id
            graphs.append(
                html.Div(
                    [
                        dcc.Graph(id=f"ohlc-chart-{sid}"),
                    ],
                    style={"padding": "9px"},
                )
            )

        self.app.layout = html.Div(
            [
                html.Div(
                    [
                        html.Div(
                            id="live-clock",
                            children=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            style={
                                "textAlign": "center",
                                "fontSize": "18px",
                                "color": "blue",
                                "padding": "6px",
                            },
                        ),
                        dcc.Interval(id="clock-interval", interval=1000, n_intervals=0),
                    ],
                ),
                # charts container
                html.Div(graphs, id="charts-container"),
                # single interval to update all charts
                dcc.Interval(id="interval-component", interval=1000, n_intervals=0),
            ],
            style={"padding": "8px"},
        )

    def _setup_callbacks(self):
        app = self.app

        @app.callback(
            Output("live-clock", "children"), Input("clock-interval", "n_intervals")
        )
        def update_clock(_):
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # For each stock, register a dedicated callback bound to that StockData
        for sd in self.stock_datas:
            sid = sd.stock_id
            chart_id = f"ohlc-chart-{sid}"

            # define a factory to capture sd and sid in defaults
            def make_callback(stock_data, stock_id):
                @app.callback(
                    Output(chart_id, "figure"),
                    Input("interval-component", "n_intervals"),
                )
                def update_chart(
                    _n_intervals, stock_data=stock_data, stock_id=stock_id
                ):
                    # get data
                    completed, current = stock_data.get_dataframes()
                    df = pd.DataFrame(completed)
                    if current:
                        df = pd.concat([df, pd.DataFrame([current])], ignore_index=True)
                    if df.empty:
                        return go.Figure()
                    LOGGER.debug(f"total rows of df after concat: {len(df)}")

                    # ensure start_time is datetime/tz-aware
                    df["start_time"] = pd.to_datetime(df["start_time"], utc=True)

                    # ensure start_time is datetime/tz-aware if it's not already
                    # (assumes stock_data provides tz-aware series; otherwise adapt as needed)
                    df = df.sort_values("start_time").reset_index(drop=True)
                    LOGGER.debug(
                        f"df after sort: rows={len(df)}, start={df['start_time'].iloc[0]}, end={df['start_time'].iloc[-1]}"
                    )

                    # Keep extra rows so EMA can be computed correctly for the last MIN_BARS bars.
                    # We keep at most MIN_BARS + EMA_PERIOD rows (EMA_PERIOD extra history).
                    required_rows = MIN_BARS + EMA_PERIOD
                    if len(df) > required_rows:
                        df = df.iloc[-required_rows:].reset_index(drop=True)
                    LOGGER.debug(f"total rows of df after trim: {len(df)}")

                    # Compute EMA on the kept window if we have enough rows
                    if len(df) >= EMA_PERIOD:
                        df["ema20"] = talib.EMA(df["close"], timeperiod=EMA_PERIOD)
                    # else: no ema column

                    # Now select the rows to DISPLAY: last MIN_BARS (or fewer if not available)
                    display_df = df.iloc[-MIN_BARS:].reset_index(drop=True)
                    LOGGER.debug(
                        f"total rows of display_df after trim: {len(display_df)}"
                    )

                    # convert start_time to local tz for display
                    try:
                        local_tz = datetime.now().astimezone().tzinfo
                        display_df["start_time"] = display_df[
                            "start_time"
                        ].dt.tz_convert(local_tz)
                    except Exception:
                        # if start_time is naive or conversion fails, leave as-is
                        pass

                    display_df["bar_index"] = range(len(display_df))

                    # --- in_session flag ---
                    warrant_info = self.warrant_info.get(stock_id, {})
                    tz_name = warrant_info.get("timezone", "Europe/Stockholm")
                    market_open = warrant_info.get("market_open", "09:00")
                    market_close = warrant_info.get("market_close", "17:30")

                    display_df["in_session"] = display_df["start_time"].apply(
                        lambda ts: is_trading_hour(
                            ts, tz_name, market_open, market_close
                        )
                    )

                    # --- bar counter ---
                    count = 0
                    bar_counter = []
                    for i, row in display_df.iterrows():
                        if not row["in_session"]:
                            bar_counter.append(None)
                            continue

                        # reset if new day or previous bar out of session
                        if (
                            i == 0
                            or display_df.loc[i, "start_time"].date()
                            != display_df.loc[i - 1, "start_time"].date()
                            or not display_df.loc[i - 1, "in_session"]
                        ):
                            count = 0
                        count += 1
                        bar_counter.append(count)

                    display_df["bar_count"] = bar_counter
                    # --- END bar counter ---

                    fig = go.Figure()

                    if "ema20" in display_df and display_df["ema20"].notna().any():
                        fig.add_trace(
                            go.Scatter(
                                x=display_df["bar_index"],
                                y=display_df["ema20"],
                                mode="lines",
                                line=dict(color="yellow", width=1),
                                name=f"EMA{EMA_PERIOD}",
                                hoverinfo="skip",
                            )
                        )

                    fig.add_trace(
                        go.Candlestick(
                            x=display_df["bar_index"],
                            open=display_df["open"],
                            high=display_df["high"],
                            low=display_df["low"],
                            close=display_df["close"],
                            increasing_line_color="green",
                            decreasing_line_color="red",
                            name="MM köp",
                            hoverinfo="text",
                            hovertext=[
                                f"Time: {t}<br>O: {o:.2f}<br>H: {h:.2f}<br>L: {l:.2f}<br>C: {c:.2f}"
                                for t, o, h, l, c in zip(
                                    display_df["start_time"].dt.strftime(
                                        "%Y-%m-%d %H:%M"
                                    ),
                                    display_df["open"],
                                    display_df["high"],
                                    display_df["low"],
                                    display_df["close"],
                                )
                            ],
                        )
                    )

                    latest_open = display_df["open"].iloc[-1]
                    latest_high = display_df["high"].iloc[-1]
                    latest_low = display_df["low"].iloc[-1]
                    latest_close = display_df["close"].iloc[-1]
                    latest_ema = (
                        display_df["ema20"].iloc[-1] if "ema20" in display_df else None
                    )
                    annotation_text = f"O: {latest_open:.2f} H: {latest_high:.2f} L: {latest_low:.2f} C: {latest_close:.2f}"
                    if latest_ema:
                        annotation_text += f" EMA20: {latest_ema:.2f}"

                    fig.add_annotation(
                        text=annotation_text,
                        xref="paper",  # center of the chart (0=left, 1=right)
                        yref="paper",
                        x=0.5,
                        y=1,
                        xanchor="center",
                        yanchor="top",
                        showarrow=False,
                        align="center",
                        font=dict(size=12, color="white"),
                        bgcolor="rgba(0, 0, 0, 0.6)",
                        borderpad=4,
                    )

                    label_indices = display_df[
                        display_df["bar_count"].notna()
                        & (display_df["bar_count"] % C_CONTADOR == 0)
                    ]

                    fig.add_trace(
                        go.Scatter(
                            x=label_indices["bar_index"],
                            y=label_indices["low"] * 0.999,  # slightly below each bar
                            text=label_indices["bar_count"].astype(int).astype(str),
                            mode="text",
                            textposition="bottom center",
                            textfont=dict(size=6, color="orange"),
                            hoverinfo="skip",
                            showlegend=False,
                        )
                    )

                    tick_step = max(1, len(display_df) // 10)
                    fig.update_xaxes(
                        tickmode="array",
                        tickvals=display_df["bar_index"][::tick_step],
                        ticktext=display_df["start_time"].dt.strftime("%H:%M")[
                            ::tick_step
                        ],
                        title_text="Bars (continuous, skips closed hours)",
                    )

                    fig.update_layout(
                        title=f"{stock_data.stock_id} ({self.interval_str})",
                        template="plotly_dark",
                        xaxis_rangeslider_visible=False,
                        margin=dict(l=10, r=10, t=30, b=30),
                        showlegend=True,
                        legend=dict(x=0, y=1, traceorder="normal", font=dict(size=10)),
                    )
                    return fig

                return update_chart

            # actually create the callback function closure
            make_callback(sd, sid)

    def run(self):
        self.app.run(debug=False, use_reloader=False, host="0.0.0.0", port=self.port)
