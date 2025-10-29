import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from flask import Flask
import logging
import talib

logging.getLogger(__name__).setLevel(logging.INFO)
LOGGER = logging.getLogger(__name__)

MIN_BARS = 180  # Number of bars to display on chart
EMA_PERIOD = 20  # EMA period (used to keep extra history for calculation)


class ChartApp:
    """
    ChartApp now accepts either a single StockData or a list of StockData objects.
    It renders one chart per StockData.stock_id and registers one callback per chart.
    """

    def __init__(self, stock_datas, interval_str="5m", port=8050):
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

                    fig = go.Figure(
                        [
                            go.Candlestick(
                                x=display_df["bar_index"],
                                open=display_df["open"],
                                high=display_df["high"],
                                low=display_df["low"],
                                close=display_df["close"],
                                increasing_line_color="green",
                                decreasing_line_color="red",
                                name="MM köp",
                            )
                        ]
                    )

                    if "ema20" in display_df and display_df["ema20"].notna().any():
                        fig.add_trace(
                            go.Scatter(
                                x=display_df["bar_index"],
                                y=display_df["ema20"],
                                mode="lines",
                                line=dict(color="blue"),
                                name=f"EMA{EMA_PERIOD}",
                            )
                        )

                    # hovertext
                    fig.update_traces(
                        hoverinfo="text",
                        hovertext=[
                            f"Time: {t}<br>O: {o:.2f}<br>H: {h:.2f}<br>L: {l:.2f}<br>C: {c:.2f}"
                            for t, o, h, l, c in zip(
                                display_df["start_time"].dt.strftime("%Y-%m-%d %H:%M"),
                                display_df["open"],
                                display_df["high"],
                                display_df["low"],
                                display_df["close"],
                            )
                        ],
                        selector=dict(type="candlestick"),
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
