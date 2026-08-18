import os
import json
import logging
import asyncio
import io
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import aiohttp
import pandas as pd
import matplotlib

matplotlib.use("Agg")
import mplfinance as mpf
import redis.asyncio as aioredis

from pacavanza.utils.utils import fetch_active_omxs30_future, flatten_instrument_list
from pacavanza.modules.ibkr_client_instance import resolve_ibkr_local_symbol

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger("trend_bar_alert_daemon")

SECRET_FILE = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../.tg_bot_secret.json")
)

from pacavanza.config import REDIS_URL

CHANNEL = "pacavanza:future_updates"


class TrendBarAlertDaemon:
    def __init__(self):
        ibkr_local_symbol = resolve_ibkr_local_symbol()
        raw_instruments = fetch_active_omxs30_future(target_name=ibkr_local_symbol)
        self.instruments = flatten_instrument_list(raw_instruments)
        self.active_sid = list(self.instruments.keys())[0] if self.instruments else None

        self.history = {sid: [] for sid in self.instruments.keys()}
        self.bot_token = None
        self.chat_id = None
        self.load_secrets()
        self.load_history_from_disk()

    def load_secrets(self):
        try:
            with open(SECRET_FILE, "r") as f:
                data = json.load(f)
                self.bot_token = data.get("telegram_bot_token")
                self.chat_id = data.get("telegram_chat_id")
            LOGGER.info(f"Loaded Telegram secrets from {SECRET_FILE}")
        except Exception as e:
            LOGGER.error(f"Failed to load secrets from {SECRET_FILE}: {e}")

    def load_history_from_disk(self):
        for sid, info in self.instruments.items():
            try:
                with open(f"ohlc_{sid}.json", "r") as f:
                    data = json.load(f)

                tz = ZoneInfo(info.get("timezone", "Europe/Stockholm"))
                today = datetime.now(tz).date()

                todays_bars = []
                for b in data:
                    st = datetime.fromisoformat(b["start_time"]).astimezone(tz)
                    if st.date() == today:
                        todays_bars.append(b)

                todays_bars.sort(key=lambda x: x["start_time"])
                self.history[sid] = todays_bars
                LOGGER.info(
                    f"[{sid}] Loaded {len(todays_bars)} bars for today from disk."
                )
            except FileNotFoundError:
                LOGGER.warning(f"[{sid}] No disk history found (ohlc_{sid}.json).")
            except Exception as e:
                LOGGER.error(f"[{sid}] Error loading disk history: {e}")

    def check_trend_bar(self, sid, completed_bar):
        """
        Check if the completed_bar is a trend bar.
        Returns (is_trend, direction).
        """
        bars_today = self.history[sid]

        info = self.instruments.get(sid, {})
        tz = ZoneInfo(info.get("timezone", "Europe/Stockholm"))
        st = datetime.fromisoformat(completed_bar["start_time"]).astimezone(tz)

        # If hour >= 17, ignore
        if st.hour >= 17:
            return False, None

        bar_index = len(bars_today)
        if bar_index <= 6:
            return False, None

        body_size = abs(completed_bar["close"] - completed_bar["open"])
        bar_size = completed_bar["high"] - completed_bar["low"]

        if bar_size <= 0:
            return False, None

        is_bull = completed_bar["close"] > completed_bar["open"]

        cond1 = body_size >= 0.9 * bar_size
        cond2 = (
            is_bull
            and (body_size >= 0.75 * bar_size)
            and (completed_bar["high"] == completed_bar["close"])
        )
        cond3 = (
            not is_bull
            and (body_size >= 0.75 * bar_size)
            and (completed_bar["low"] == completed_bar["close"])
        )

        if not (cond1 or cond2 or cond3):
            return False, None

        lookback = min(20, bar_index - 1)

        prev_bars = bars_today[:-1]
        recent_history = prev_bars[-lookback:]

        if not recent_history:
            return False, None

        max_prev_body = max([abs(b["close"] - b["open"]) for b in recent_history])
        if body_size > max_prev_body:
            return True, "BULL" if is_bull else "BEAR"

        return False, None

    def _plot_chart(self, df):
        buf = io.BytesIO()
        # Create a candlestick chart and save to the buffer
        mpf.plot(
            df,
            type="candle",
            style="charles",
            savefig=dict(fname=buf, format="png", bbox_inches="tight"),
        )
        buf.seek(0)
        return buf

    async def send_telegram_alert(self, sid, direction):
        if not self.bot_token or not self.chat_id:
            LOGGER.warning("Telegram credentials not loaded, skipping alert.")
            return

        LOGGER.info(f"[{sid}] Generating chart for Telegram alert...")

        bars = self.history[sid]
        df_data = []
        for b in bars:
            st = datetime.fromisoformat(b["start_time"])
            df_data.append(
                {
                    "Date": st,
                    "Open": float(b["open"]),
                    "High": float(b["high"]),
                    "Low": float(b["low"]),
                    "Close": float(b["close"]),
                    "Volume": float(b.get("volume", 0)),
                }
            )

        df = pd.DataFrame(df_data)
        df.set_index("Date", inplace=True)

        buf = await asyncio.to_thread(self._plot_chart, df)

        url = f"https://api.telegram.org/bot{self.bot_token}/sendPhoto"

        form = aiohttp.FormData()
        form.add_field("chat_id", str(self.chat_id))
        form.add_field(
            "caption",
            f"🚨 TREND BAR DETECTED 🚨\n\nInstrument: {sid}\nDirection: {direction}",
        )
        form.add_field("photo", buf, filename="chart.png", content_type="image/png")

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=form) as response:
                    if response.status == 200:
                        LOGGER.info(f"[{sid}] Telegram alert sent successfully.")
                    else:
                        resp_text = await response.text()
                        LOGGER.error(
                            f"[{sid}] Failed to send Telegram alert: {response.status} {resp_text}"
                        )
        except Exception as e:
            LOGGER.exception(f"[{sid}] Error sending Telegram alert: {e}")

    async def run(self):
        redis = aioredis.from_url(REDIS_URL)
        pubsub = redis.pubsub()
        await pubsub.subscribe(CHANNEL)

        LOGGER.info(f"Subscribed to Redis channel: {CHANNEL}")

        try:
            while True:
                try:
                    message = await pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=60.0
                    )
                except Exception as e:
                    # Ignore occasional read timeouts from redis-py
                    if "Timeout" in str(e):
                        continue
                    raise

                if message is None:
                    # Timeout reached without messages. Send a ping to keep connection alive.
                    try:
                        await pubsub.ping()
                    except Exception:
                        pass
                    continue

                if message["type"] == "message":
                    try:
                        data = json.loads(message["data"])
                        if data.get("type") == "completed":
                            sid = data["instrument"]

                            if self.active_sid and sid != self.active_sid:
                                continue

                            bar = data["bar"]

                            info = self.instruments.get(sid, {})
                            tz = ZoneInfo(info.get("timezone", "Europe/Stockholm"))
                            st = datetime.fromisoformat(bar["start_time"]).astimezone(
                                tz
                            )

                            if self.history.get(sid):
                                last_bar_st = datetime.fromisoformat(
                                    self.history[sid][-1]["start_time"]
                                ).astimezone(tz)
                                if st.date() > last_bar_st.date():
                                    LOGGER.info(
                                        f"[{sid}] New day detected. Clearing history."
                                    )
                                    self.history[sid] = []

                            if sid not in self.history:
                                self.history[sid] = []

                            if not any(
                                b["start_time"] == bar["start_time"]
                                for b in self.history[sid]
                            ):
                                self.history[sid].append(bar)

                                is_trend, direction = self.check_trend_bar(sid, bar)
                                if is_trend:
                                    LOGGER.info(
                                        f"[{sid}] Trend bar detected! Direction: {direction}"
                                    )
                                    asyncio.create_task(
                                        self.send_telegram_alert(sid, direction)
                                    )
                    except Exception as e:
                        LOGGER.error(f"Error processing message: {e}")
        finally:
            try:
                await pubsub.unsubscribe(CHANNEL)
            except Exception:
                pass
            await redis.aclose()


def main():
    import sys

    try:
        daemon = TrendBarAlertDaemon()
        asyncio.run(daemon.run())
    except Exception as e:
        LOGGER.exception(f"Fatal error in trend bar daemon: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
