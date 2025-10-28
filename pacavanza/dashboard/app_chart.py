import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from flask import Flask
import talib

MIN_BARS = 180  # Number of bars to display on chart
EMA_PERIOD = 20  # EMA period (used to keep extra history for calculation)


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
            # get data
            completed, current = stock_data.get_dataframes()
            df = pd.DataFrame(completed)
            if current:
                df = pd.concat([df, pd.DataFrame([current])], ignore_index=True)
            if df.empty:
                return go.Figure()

            # ensure start_time is datetime/tz-aware if it's not already
            # (assumes stock_data provides tz-aware series; otherwise adapt as needed)
            df = df.sort_values("start_time").reset_index(drop=True)

            # Keep extra rows so EMA can be computed correctly for the last MIN_BARS bars.
            # We keep at most MIN_BARS + EMA_PERIOD rows (EMA_PERIOD extra history).
            required_rows = MIN_BARS + EMA_PERIOD
            if len(df) > required_rows:
                df = df.iloc[-required_rows:].reset_index(drop=True)

            # Compute EMA on the kept window if we have enough rows
            if len(df) >= EMA_PERIOD:
                df["ema20"] = talib.EMA(df["close"], timeperiod=EMA_PERIOD)
            # else: no ema column

            # Now select the rows to DISPLAY: last MIN_BARS (or fewer if not available)
            display_df = df.iloc[-MIN_BARS:].reset_index(drop=True)

            # convert start_time to local tz for display
            try:
                local_tz = datetime.now().astimezone().tzinfo
                display_df["start_time"] = display_df["start_time"].dt.tz_convert(
                    local_tz
                )
            except Exception:
                # if start_time is naive or conversion fails, leave as-is
                pass

            display_df["bar_index"] = range(len(display_df))

            # Build figure using display_df, but EMA values were computed on the extended df
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

            # Only add EMA trace if present and has non-NA values for the displayed rows
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

            # Proper hover text via update_traces (apply to candlestick only)
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

            # X-axis tick labels = timestamps (no gaps)
            tick_step = max(1, len(display_df) // 10)
            fig.update_xaxes(
                tickmode="array",
                tickvals=display_df["bar_index"][::tick_step],
                ticktext=display_df["start_time"].dt.strftime("%H:%M")[::tick_step],
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
