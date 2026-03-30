import time
import subprocess
import sys
import os
import signal
import logging
from datetime import datetime

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


def get_python_executable():
    return sys.executable


def start_process(name, module_name, log_prefix):
    """
    Start a process if it is not already running.
    Returns True if a new process was started, False otherwise.
    """
    # Check if already running
    if name in PROCESS_MAP:
        if PROCESS_MAP[name].poll() is None:
            return False  # Still running
        else:
            logger.warning(
                f"{name} died (exit code {PROCESS_MAP[name].returncode}). Restarting..."
            )
            # Cleanup dead process info
            del PROCESS_MAP[name]

    logger.info(f"Starting {name}...")

    # Logs
    stdout_log = open(f"/tmp/{log_prefix}.stdout.log", "a")
    stderr_log = open(f"/tmp/{log_prefix}.stderr.log", "a")

    # Run from the root directory of the repository (parent of the package)
    cwd = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    cmd = [get_python_executable(), "-m", module_name]

    try:
        proc = subprocess.Popen(cmd, stdout=stdout_log, stderr=stderr_log, cwd=cwd)
        PROCESS_MAP[name] = proc
        logger.info(f"{name} started with PID {proc.pid}")
        return True
    except Exception as e:
        logger.error(f"Failed to start {name}: {e}")
        return False


def stop_process(name):
    proc = PROCESS_MAP.get(name)
    if proc:
        if proc.poll() is None:
            logger.info(f"Stopping {name} (PID {proc.pid})...")
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                logger.warning(f"{name} did not terminate, killing...")
                proc.kill()
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
            now = datetime.now()
            # Operating hours: 08:00:00 to 23:00:00
            start_time_limit = now.replace(hour=8, minute=0, second=0, microsecond=0)
            stop_time_limit = now.replace(hour=23, minute=0, second=0, microsecond=0)

            is_active_window = start_time_limit <= now < stop_time_limit

            if is_active_window:
                # 1. Market Daemon (Always ensure it is running)
                if start_process(
                    "market",
                    "pacavanza.avanza_market_daemon",
                    "pacavanza.avanza_market_daemon",
                ):
                    time.sleep(30)

                # 2. Future Daemon (Always ensure it is running)
                if start_process(
                    "future_market",
                    "pacavanza.future_market_daemon",
                    "pacavanza.future_market_daemon",
                ):
                    time.sleep(5)

                # 3. Dashboard
                if start_process(
                    "dashboard", "pacavanza.backend", "pacavanza.dashboard"
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
