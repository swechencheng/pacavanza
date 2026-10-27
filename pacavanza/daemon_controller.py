import os

os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"

import time
import sys
import signal
import logging
import importlib
import multiprocessing as mp
from datetime import datetime
from zoneinfo import ZoneInfo
import requests
import argparse

# Set start method to spawn so we avoid macOS fork safety issues
try:
    mp.set_start_method("spawn")
except RuntimeError:
    pass

# Initialize AVANZA in the parent process!
# It will serialize the session to a file for child processes to use.
from pacavanza.modules.avanza_instance import get_avanza

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logging.getLogger("ib_async").setLevel(logging.WARNING)
logger = logging.getLogger("controller")

PROCESS_MAP = {}
PROCESS_CRASH_HISTORY = {}
MAX_CRASH_COUNT = 5
CRASH_WINDOW_SECONDS = 30


def _init_avanza_session():
    """Initialize AVANZA once in the parent process.
    It serializes the session to a file for child processes to use.
    """
    while True:
        try:
            get_avanza()
            break
        except Exception as e:
            logger.warning(f"Failed to initialize Avanza: {e}. Retrying in 30s...")
            time.sleep(30)


def check_crash_loop(name, now=None):
    """
    Record a crash timestamp for process `name`.
    Returns True if crash count exceeds MAX_CRASH_COUNT within CRASH_WINDOW_SECONDS.
    """
    if now is None:
        now = time.time()
    crashes = PROCESS_CRASH_HISTORY.setdefault(name, [])
    crashes.append(now)
    crashes[:] = [t for t in crashes if now - t <= CRASH_WINDOW_SECONDS]
    return len(crashes) >= MAX_CRASH_COUNT


def reset_crash_history(name=None):
    """Reset crash history for testing or manual reset."""
    if name:
        PROCESS_CRASH_HISTORY.pop(name, None)
    else:
        PROCESS_CRASH_HISTORY.clear()


def is_active_trading_window(now=None):
    """Return True if `now` (Stockholm time) is within operating hours (08:00:00 to 23:00:00)."""
    if now is None:
        now = datetime.now(ZoneInfo("Europe/Stockholm"))
    start_time_limit = now.replace(hour=8, minute=0, second=0, microsecond=0)
    stop_time_limit = now.replace(hour=23, minute=0, second=0, microsecond=0)
    return start_time_limit <= now < stop_time_limit


def _run_module(module_name, log_prefix):
    # Reset signal handlers to defaults in child — don't inherit parent's stop_all() handler
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    # Redirect output to log files
    sys.stdout = open(f"/tmp/{log_prefix}.stdout.log", "a")
    sys.stderr = open(f"/tmp/{log_prefix}.stderr.log", "a")
    mod = importlib.import_module(module_name)
    mod.main()


def start_process(name, module_name, log_prefix):
    """
    Start a process if it is not already running.
    Returns True if a new process was started, False otherwise.
    """
    # Check if already running
    if name in PROCESS_MAP:
        if PROCESS_MAP[name].is_alive():
            return False  # Still running
        else:
            exitcode = PROCESS_MAP[name].exitcode
            logger.warning(
                f"{name} died (exit code {exitcode}). Restarting..."
            )
            del PROCESS_MAP[name]

            if check_crash_loop(name):
                logger.critical(
                    f"CRASH LOOP DETECTED for {name} ({MAX_CRASH_COUNT} crashes within {CRASH_WINDOW_SECONDS}s). "
                    f"Exiting daemon controller so launchd can restart with a clean environment."
                )
                stop_all()
                sys.exit(1)

    logger.info(f"Starting {name}...")

    try:
        proc = mp.Process(target=_run_module, args=(module_name, log_prefix), name=name)
        proc.start()
        PROCESS_MAP[name] = proc
        logger.info(f"{name} started with PID {proc.pid}")
        return True
    except Exception as e:
        logger.error(f"Failed to start {name}: {e}")
        if check_crash_loop(name):
            logger.critical(
                f"CRASH LOOP DETECTED for {name} on spawn failure ({MAX_CRASH_COUNT} crashes within {CRASH_WINDOW_SECONDS}s). "
                f"Exiting daemon controller so launchd can restart with a clean environment."
            )
            stop_all()
            sys.exit(1)
        return False


def stop_process(name):
    proc = PROCESS_MAP.get(name)
    if proc:
        if proc.is_alive():
            logger.info(f"Stopping {name} (PID {proc.pid})...")
            proc.terminate()
            proc.join(timeout=5)
            if proc.is_alive():
                logger.warning(f"{name} did not terminate, killing...")
                proc.kill()
                proc.join()
        PROCESS_MAP.pop(name, None)


def stop_all():
    logger.info("Stopping all processes...")
    stop_process("future_market")
    stop_process("trend_bar_alert")
    stop_process("future_trade_monitor")
    stop_process("monitor")
    stop_process("dashboard")
    stop_process("market")


def main():
    parser = argparse.ArgumentParser(description="Pacavanza Daemon Controller")
    parser.add_argument("--real", action="store_true", help="Use real IBKR account")
    parser.add_argument(
        "--enable-ava-mini",
        action="store_true",
        help="Enable Avanza mini options trading daemons",
    )
    args, _ = parser.parse_known_args()

    from pacavanza.config import IBKR_PORT, IBKR_HOST

    logger.info(
        f"Daemon Controller Started. IBKR_HOST: {IBKR_HOST}, IBKR_PORT: {IBKR_PORT}"
    )

    logger.info("Initializing Avanza session...")
    _init_avanza_session()

    def signal_handler(sig, _frame):
        logger.info(f"Received signal {sig}. Shutting down all processes.")
        stop_all()
        sys.exit(0)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    was_active_window = None

    while True:
        try:
            now = datetime.now(ZoneInfo("Europe/Stockholm"))
            is_active_window = is_active_trading_window(now)

            # On the first iteration, initialize was_active_window to the current window
            if was_active_window is None:
                was_active_window = is_active_window

            # Check for morning transition: inactive (overnight) -> active (08:00 morning open)
            if not was_active_window and is_active_window:
                logger.info(
                    "Transitioning to active trading window (08:00). Exiting controller to let launchd "
                    "start a fresh process with updated environment for the new trading day..."
                )
                stop_all()
                sys.exit(0)

            if is_active_window:
                # 1. Dashboard
                start_process("dashboard", "pacavanza.backend", "pacavanza.dashboard")

                # 2. Future Daemon (Always ensure it is running)
                if start_process(
                    "future_market",
                    "pacavanza.future_market_daemon",
                    "pacavanza.future_market_daemon",
                ):
                    time.sleep(1)

                secret_file_path = os.path.abspath(
                    os.path.join(os.path.dirname(__file__), "../.tg_bot_secret.json")
                )
                if os.path.exists(secret_file_path):
                    start_process(
                        "trend_bar_alert",
                        "pacavanza.trend_alert_daemon",
                        "pacavanza.trend_alert_daemon",
                    )

                # 3. Market Daemon (Avanza Mini Futures)
                if args.enable_ava_mini:
                    if start_process(
                        "market",
                        "pacavanza.avanza_market_daemon",
                        "pacavanza.avanza_market_daemon",
                    ):
                        time.sleep(5)

                # 4. Future Trade Monitor (depends on future_market being up)
                start_process(
                    "future_trade_monitor",
                    "pacavanza.future_trade_monitor",
                    "pacavanza.future_trade_monitor",
                )

                # 5. Trading Monitor (Avanza Mini Futures)
                if args.enable_ava_mini:
                    start_process(
                        "monitor",
                        "pacavanza.avanza_trading_monitor",
                        "pacavanza.avanza_trading_monitor",
                    )

            else:
                # Outside active window
                if PROCESS_MAP:  # If any processes are running
                    logger.info(
                        "Outside operating hours (08:00 - 23:00). Stopping services."
                    )
                    stop_all()

            was_active_window = is_active_window
            time.sleep(1)

        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
