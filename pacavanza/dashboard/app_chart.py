import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from flask import Flask
import talib

MIN_BARS = 180  # Number of bars to display on chart


class ChartApp:
    def __init__(self, stock_data, interval_str="5m", port=8050):
        self.stock_data = stock_data
        self.interval_str = interval_str
        self.port = port
        self.server = Flask(__name__)
        self.app = Dash(__name__, server=self.server)
        self._setup_layout()
        self._setup_callbacks()

    def _setup_layout(self):
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
                            },
                        ),
                        dcc.Interval(id="clock-interval", interval=1000, n_intervals=0),
                    ]
                ),
                dcc.Graph(id="ohlc-chart"),
                dcc.Interval(id="interval-component", interval=1000, n_intervals=0),
            ]
        )

    def _setup_callbacks(self):
        app = self.app
        stock_data = self.stock_data
        interval_str = self.interval_str

        @app.callback(
            Output("live-clock", "children"), Input("clock-interval", "n_intervals")
        )
        def update_clock(_):
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        @app.callback(
            Output("ohlc-chart", "figure"), Input("interval-component", "n_intervals")
        )
        def update_chart(_):
            completed, current = stock_data.get_dataframes()
            df = pd.DataFrame(completed)
            if current:
                df = pd.concat([df, pd.DataFrame([current])], ignore_index=True)
            if df.empty:
                return go.Figure()

            # Sort and keep latest MIN_BARS
            df = df.sort_values("start_time").iloc[-MIN_BARS:].reset_index(drop=True)

            if len(df) >= 20:
                df["ema20"] = talib.EMA(df["close"], timeperiod=20)

            local_tz = datetime.now().astimezone().tzinfo
            df["start_time"] = df["start_time"].dt.tz_convert(local_tz)
            df["bar_index"] = range(len(df))
            fig = go.Figure(
                [
                    go.Candlestick(
                        x=df["bar_index"],
                        open=df["open"],
                        high=df["high"],
                        low=df["low"],
                        close=df["close"],
                        increasing_line_color="green",
                        decreasing_line_color="red",
                        name="MM köp",
                    )
                ]
            )
            if "ema20" in df:
                fig.add_trace(
                    go.Scatter(
                        x=df["bar_index"],
                        y=df["ema20"],
                        mode="lines",
                        line=dict(color="blue"),
                        name="EMA20",
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
                title=f"{stock_data.stock_id} ({interval_str})",
                template="plotly_dark",
                xaxis_rangeslider_visible=False,
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

    def run(self):
        self.app.run(debug=False, use_reloader=False, host="0.0.0.0", port=self.port)
