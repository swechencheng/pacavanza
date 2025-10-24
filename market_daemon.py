#!/usr/bin/env python3
import os
import time
import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
import subprocess

# --- Configuration ---
PROJECT_DIR = Path(__file__).parent.resolve()
VENV_PYTHON = PROJECT_DIR / "venv" / "bin" / "python3"
LOG_DIR = PROJECT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_RETENTION_DAYS = 4  # Fixed to match your requirement

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

def in_session(session):
    tz = session["timezone"]
    now = datetime.datetime.now(tz)
    if now.weekday() >= 5:
        return False
    return session["start"] <= now.time() <= session["end"]

def rotate_logs():
    cutoff = time.time() - LOG_RETENTION_DAYS * 86400
    for f in LOG_DIR.glob("*.log"):
        if f.stat().st_mtime < cutoff:
            f.unlink(missing_ok=True)

def run_script(script_path, args, log_file):
    """Run script as a subprocess with venv python"""
    cmd = [str(VENV_PYTHON), str(script_path)] + args
    
    with open(log_file, "a") as f:
        f.write(f"[{datetime.datetime.now()}] Starting: {' '.join(cmd)}\n")
        f.flush()
        
        # Run as subprocess to ensure proper environment isolation
        process = subprocess.Popen(
            cmd,
            cwd=PROJECT_DIR,
            stdout=f,
            stderr=subprocess.STDOUT,
            env={**os.environ, 'PYTHONPATH': str(PROJECT_DIR)}
        )
        
        return process

def start_market(session):
    for cmd in session["commands"]:
        script = PROJECT_DIR / cmd[0]
        log_name = LOG_DIR / f"{session['market']}_{cmd[-1]}_{int(time.time())}.log"
        
        process = run_script(script, cmd[1:], log_name)
        running_processes[(session["market"], cmd[-1])] = process

    print(f"[{datetime.datetime.now()}] Started processes for {session['market']}")

def stop_market(market):
    for key in list(running_processes.keys()):
        mkt, name = key
        if mkt == market:
            process = running_processes[key]
            print(f"[{datetime.datetime.now()}] Stopping {mkt}:{name} (PID: {process.pid})")
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
            running_processes.pop(key, None)
    print(f"[{datetime.datetime.now()}] All processes stopped for {market}")

def monitor_loop():
    active_markets = set()
    os.chdir(PROJECT_DIR)
    print(f"[{datetime.datetime.now()}] Market daemon started.")
    try:
        while True:
            rotate_logs()
            for session in SESSIONS:
                market = session["market"]
                active = in_session(session)
                if active and market not in active_markets:
                    start_market(session)
                    active_markets.add(market)
                elif not active and market in active_markets:
                    stop_market(market)
                    active_markets.remove(market)
            
            # Check if any processes died unexpectedly
            dead_processes = []
            for key, process in running_processes.items():
                if process.poll() is not None:  # Process finished
                    dead_processes.append(key)
                    print(f"[{datetime.datetime.now()}] Process {key} died unexpectedly with return code: {process.returncode}")
            
            for key in dead_processes:
                running_processes.pop(key, None)
            
            time.sleep(60)
    except KeyboardInterrupt:
        for market in list(active_markets):
            stop_market(market)
    print(f"[{datetime.datetime.now()}] Daemon exiting.")

if __name__ == "__main__":
    monitor_loop()
