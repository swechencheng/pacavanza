#!/usr/bin/env python3
import subprocess
import time
import logging
from datetime import datetime, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo
import signal
import sys
import os

# Add project directory to Python path
PROJECT_DIR = Path(__file__).parent.resolve()
sys.path.insert(0, str(PROJECT_DIR))

# --- Configuration ---
VENV_PYTHON = PROJECT_DIR / "venv" / "bin" / "python3"
LOG_DIR = PROJECT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
MODULE = "pacavanza.run_dashboard"

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "dashboard_daemon.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# Process registry
processes = {}

# Each entry: (symbol, timezone, start_time, end_time)
SCHEDULES = [
    ("mini-l-omx-ava-468", 9001, "Europe/Stockholm", dtime(9, 0), dtime(17, 30)),
    ("mini-s-omx-ava-1575", 9002, "Europe/Stockholm", dtime(9, 0), dtime(17, 30)),
    ("mini-l-sp500-ava-270", 9011, "America/New_York", dtime(9, 30), dtime(16, 0)),
    ("mini-s-sp500-ava-330", 9012, "America/New_York", dtime(9, 30), dtime(16, 0)),
]


def is_within_schedule(tz_name, start_t, end_t):
    now = datetime.now(ZoneInfo(tz_name))
    if now.weekday() >= 5:  # Skip weekends
        return False
    now_t = now.time()
    return start_t <= now_t <= end_t


def start_process(symbol, port):
    if symbol in processes:
        return
    logging.info(f"Starting {symbol}")
    proc = subprocess.Popen(
        [VENV_PYTHON, "-m", MODULE, "-p", str(port), symbol],
        stdout=open(os.path.join(LOG_DIR, f"{symbol}.log"), "a"),
        stderr=subprocess.STDOUT,
    )
    processes[symbol] = proc


def stop_process(symbol):
    proc = processes.pop(symbol, None)
    if proc and proc.poll() is None:
        logging.info(f"Stopping {symbol}")
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def scheduler_loop():
    while True:
        try:
            for symbol, port, tz, start_t, end_t in SCHEDULES:
                should_run = is_within_schedule(tz, start_t, end_t)
                if should_run and symbol not in processes:
                    start_process(symbol, port)
                elif not should_run and symbol in processes:
                    stop_process(symbol)
        except Exception as e:
            logging.exception(e)
        time.sleep(1)  # check every second


def cleanup(signum=None, frame=None):
    logging.info("Daemon shutting down...")
    for symbol in list(processes.keys()):
        stop_process(symbol)
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, cleanup)
    signal.signal(signal.SIGINT, cleanup)
    scheduler_loop()
