import time
import sys
import os
import signal
import logging
import importlib
import multiprocessing as mp
from datetime import datetime
from zoneinfo import ZoneInfo

# Set start method to fork so AVANZA is shared
try:
    mp.set_start_method("fork")
except RuntimeError:
    pass

# Initialize AVANZA once in the parent process!
from pacavanza.modules.avanza_instance import get_avanza

while True:
    try:
        get_avanza()
        break
    except Exception as e:
        print(f"Failed to initialize Avanza: {e}. Retrying in 10s...")
        time.sleep(30)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/tmp/pacavanza.controller.log"),
    ],
)
logger = logging.getLogger("controller")

PROCESS_MAP = {}


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
            logger.warning(
                f"{name} died (exit code {PROCESS_MAP[name].exitcode}). Restarting..."
            )
            del PROCESS_MAP[name]

    logger.info(f"Starting {name}...")

    try:
        proc = mp.Process(target=_run_module, args=(module_name, log_prefix), name=name)
        proc.start()
        PROCESS_MAP[name] = proc
        logger.info(f"{name} started with PID {proc.pid}")
        return True
    except Exception as e:
        logger.error(f"Failed to start {name}: {e}")
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
    stop_process("monitor")
    stop_process("dashboard")
    stop_process("market")


def main():
    logger.info("Daemon Controller Started")

    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}. Shutting down all processes.")
        stop_all()
        sys.exit(0)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    while True:
        try:
            now = datetime.now(ZoneInfo("Europe/Stockholm"))
            # Operating hours: 08:00:00 to 23:00:00
            start_time_limit = now.replace(hour=8, minute=0, second=0, microsecond=0)
            stop_time_limit = now.replace(hour=23, minute=0, second=0, microsecond=0)

            is_active_window = start_time_limit <= now < stop_time_limit

            if is_active_window:
                # 1. Dashboard
                if start_process(
                    "dashboard", "pacavanza.backend", "pacavanza.dashboard"
                ):
                    time.sleep(5)

                # 2. Future Daemon (Always ensure it is running)
                if start_process(
                    "future_market",
                    "pacavanza.future_market_daemon",
                    "pacavanza.future_market_daemon",
                ):
                    time.sleep(5)

                # 3. Market Daemon (Always ensure it is running)
                if start_process(
                    "market",
                    "pacavanza.avanza_market_daemon",
                    "pacavanza.avanza_market_daemon",
                ):
                    time.sleep(5)

                # 4. Trading Monitor
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

            time.sleep(1)

        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
