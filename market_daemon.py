#!/usr/bin/env python3
import os
import sys
import time
import datetime
import traceback
from zoneinfo import ZoneInfo
from pathlib import Path
import subprocess

# Add project directory to Python path
PROJECT_DIR = Path(__file__).parent.resolve()
sys.path.insert(0, str(PROJECT_DIR))

# --- Configuration ---
VENV_PYTHON = PROJECT_DIR / "venv" / "bin" / "python3"
LOG_DIR = PROJECT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_RETENTION_DAYS = 4

# --- Market time windows ---
CEST = ZoneInfo("Europe/Stockholm")
ET = ZoneInfo("America/New_York")

SESSIONS = [
    # CEST 09:00–17:30
    {
        "market": "OMX",
        "timezone": CEST,
        "start": datetime.time(9, 0),
        "end": datetime.time(17, 30),
        "commands": [
            ["realtime_stock_chart.py", "-p", "9001", "mini-l-omx-ava-468"],
            ["realtime_stock_chart.py", "-p", "9002", "mini-s-omx-ava-1575"],
        ],
    },
    # US ET 09:30–16:00
    {
        "market": "SP500",
        "timezone": ET,
        "start": datetime.time(9, 30),
        "end": datetime.time(16, 0),
        "commands": [
            ["realtime_stock_chart.py", "-p", "9011", "mini-l-sp500-ava-270"],
            ["realtime_stock_chart.py", "-p", "9012", "mini-s-sp500-ava-330"],
        ],
    },
]

running_processes = {}


def log_message(message):
    """Log message with timestamp"""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"[{timestamp}] {message}"
    print(full_message, flush=True)

    # Also write to a dedicated daemon log
    daemon_log = LOG_DIR / "market_daemon.log"
    with open(daemon_log, "a") as f:
        f.write(full_message + "\n")


def in_session(session):
    try:
        tz = session["timezone"]
        now = datetime.datetime.now(tz)
        if now.weekday() >= 5:  # Saturday (5) or Sunday (6)
            return False
        return session["start"] <= now.time() <= session["end"]
    except Exception as e:
        log_message(f"Error checking session: {e}")
        return False


def rotate_logs():
    try:
        cutoff = time.time() - LOG_RETENTION_DAYS * 86400
        for f in LOG_DIR.glob("*.log"):
            if f.stat().st_mtime < cutoff:
                f.unlink(missing_ok=True)
    except Exception as e:
        log_message(f"Error rotating logs: {e}")


def run_script(script_path, args, log_file):
    """Run script as a subprocess with venv python"""
    try:
        cmd = [str(VENV_PYTHON), str(script_path)] + args

        with open(log_file, "a") as f:
            f.write(f"[{datetime.datetime.now()}] Starting: {' '.join(cmd)}\n")
            f.flush()

            process = subprocess.Popen(
                cmd,
                cwd=PROJECT_DIR,
                stdout=f,
                stderr=subprocess.STDOUT,
                env={**os.environ, "PYTHONPATH": str(PROJECT_DIR)},
            )
            return process
    except Exception as e:
        log_message(f"Error running script {script_path}: {e}")
        return None


def start_market(session):
    market = session["market"]
    log_message(f"Attempting to start market: {market}")

    for cmd in session["commands"]:
        script = PROJECT_DIR / cmd[0]
        if not script.exists():
            log_message(f"ERROR: Script not found: {script}")
            continue

        log_name = LOG_DIR / f"{market}_{cmd[-1]}_{int(time.time())}.log"
        process = run_script(script, cmd[1:], log_name)

        if process:
            running_processes[(market, cmd[-1])] = process
            log_message(f"Started process for {market}:{cmd[-1]} (PID: {process.pid})")
        else:
            log_message(f"Failed to start process for {market}:{cmd[-1]}")


def stop_market(market):
    log_message(f"Stopping all processes for {market}")
    for key in list(running_processes.keys()):
        mkt, name = key
        if mkt == market:
            process = running_processes[key]
            log_message(f"Stopping {mkt}:{name} (PID: {process.pid})")
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
            running_processes.pop(key, None)
    log_message(f"All processes stopped for {market}")


def monitor_loop():
    active_markets = set()

    # Ensure we're in the right directory
    os.chdir(PROJECT_DIR)
    log_message(f"Market daemon started in {PROJECT_DIR}")
    log_message(f"Python path: {sys.executable}")
    log_message(f"Working directory: {os.getcwd()}")

    try:
        while True:
            try:
                rotate_logs()

                # Check each session
                for session in SESSIONS:
                    market = session["market"]
                    active = in_session(session)

                    if active and market not in active_markets:
                        log_message(f"Market {market} session started")
                        start_market(session)
                        active_markets.add(market)
                    elif not active and market in active_markets:
                        log_message(f"Market {market} session ended")
                        stop_market(market)
                        active_markets.remove(market)

                # Check for dead processes
                dead_processes = []
                for key, process in running_processes.items():
                    if process.poll() is not None:
                        dead_processes.append(key)
                        log_message(
                            f"Process {key} died with return code: {process.returncode}"
                        )

                for key in dead_processes:
                    running_processes.pop(key, None)

                time.sleep(60)

            except Exception as e:
                log_message(f"Error in main loop: {e}")
                log_message(traceback.format_exc())
                time.sleep(60)  # Continue after error

    except KeyboardInterrupt:
        log_message("Received interrupt signal")
    except Exception as e:
        log_message(f"Fatal error: {e}")
        log_message(traceback.format_exc())
    finally:
        # Cleanup
        for market in list(active_markets):
            stop_market(market)
        log_message("Daemon exiting.")


if __name__ == "__main__":
    monitor_loop()
