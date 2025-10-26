import sys
import threading
import logging
import json
from .modules.stock_data import StockData, INTERVAL_MAP
from .data_sources.random_generator import RandomPriceGenerator
from .data_sources.avanza_market import RealMarketData
from .dashboard.app_chart import ChartApp

logging.basicConfig(level=logging.INFO)
logging.getLogger("werkzeug").setLevel(logging.ERROR)
LOGGER = logging.getLogger(__name__)


def parse_args():
    args = sys.argv[1:]
    stock_id = "TEST"
    interval_str = "5m"
    port = 8050
    i = 0
    while i < len(args):
        if args[i] == "-i" and i + 1 < len(args):
            val = args[i + 1]
            if val not in INTERVAL_MAP:
                LOGGER.error(
                    f"Invalid interval '{val}'. Choose from {list(INTERVAL_MAP.keys())}"
                )
                sys.exit(1)
            interval_str = val
            i += 2
        elif args[i] == "-p" and i + 1 < len(args):
            try:
                port = int(args[i + 1])
            except ValueError:
                LOGGER.error("Port must be integer")
                sys.exit(1)
            i += 2
        else:
            stock_id = args[i]
            i += 1
    return stock_id, interval_str, port


def main():
    stock_id, interval_str, port = parse_args()
    interval_seconds = INTERVAL_MAP[interval_str]

    try:
        warrant_list = json.load(open("./pacavanza/warrant_list.json"))
    except FileNotFoundError:
        LOGGER.warning("warrant_list.json not found. Using random data generator.")
        warrant_list = []

    use_real = stock_id in warrant_list
    LOGGER.info(
        f"Using STOCK_ID={stock_id}, interval={interval_seconds}s, port={port}, real={use_real}"
    )

    stock_data = StockData(interval_seconds, stock_id)

    if use_real:
        collector = RealMarketData(stock_data)
        t = threading.Thread(target=collector.run, daemon=True)
        t.start()
    else:
        generator = RandomPriceGenerator(stock_data)
        t = threading.Thread(target=generator.run, daemon=True)
        t.start()

    app = ChartApp(stock_data, interval_str, port)
    app.run()


if __name__ == "__main__":
    main()
