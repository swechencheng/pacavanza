import sys
import threading
import logging
import json
from .modules.stock_data import StockData, INTERVAL_MAP
from .data_sources.random_generator import RandomPriceGenerator
from .data_sources.avanza_market_multi import MultiMarketCollector
from .dashboard.app_chart import ChartApp

logging.basicConfig(level=logging.INFO)
logging.getLogger("werkzeug").setLevel(logging.ERROR)
LOGGER = logging.getLogger(__name__)


def parse_args():
    args = sys.argv[1:]
    stocks = []
    interval_str = "5m"
    port = 8050
    i = 0
    # collect all non-option args as stock ids
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
            stocks.append(args[i])
            i += 1

    if not stocks:
        LOGGER.info("No stock ids provided on command line. Defaulting to TEST.")
        stocks = ["TEST"]
    return stocks, interval_str, port


def main():
    stocks, interval_str, port = parse_args()
    interval_seconds = INTERVAL_MAP[interval_str]

    try:
        warrant_list = json.load(open("./pacavanza/warrant_list.json"))
    except FileNotFoundError:
        LOGGER.warning(
            "warrant_list.json not found. Using random data generator for all stocks."
        )
        warrant_list = {}

    # partition stocks into real (in warrant list) and synthetic
    real_stocks = [s for s in stocks if s in warrant_list]
    synthetic_stocks = [s for s in stocks if s not in warrant_list]

    LOGGER.info(
        f"Starting dashboard for stocks={stocks}, interval={interval_seconds}s, port={port}"
    )
    LOGGER.info(f"Real streams: {real_stocks}, Random generators: {synthetic_stocks}")

    # create StockData objects for all stocks
    stock_datas = {sid: StockData(interval_seconds, sid) for sid in stocks}

    # start MultiMarketCollector for real stocks (single Avanza instance)
    if real_stocks:
        # pass the shared stock_datas mapping so the collector updates the same
        # StockData objects the ChartApp is using (otherwise collector creates
        # its own StockData objects and the charts remain empty).
        collector = MultiMarketCollector(
            real_stocks, interval_seconds, stock_datas=stock_datas
        )
        t = threading.Thread(target=collector.run, daemon=True)
        t.start()

    # start random generators for synthetic stocks
    for sid in synthetic_stocks:
        generator = RandomPriceGenerator(stock_datas[sid])
        t = threading.Thread(target=generator.run, daemon=True)
        t.start()

    # prepare list for ChartApp (order follows passed stocks)
    chart_stock_datas = [stock_datas[sid] for sid in stocks]

    app = ChartApp(chart_stock_datas, interval_str, port)

    try:
        app.run()
    except KeyboardInterrupt:
        LOGGER.info(
            "KeyboardInterrupt received in main (web server). Initiating collector shutdown..."
        )
        if real_stocks:
            try:
                collector.stop(timeout=15.0)
            except Exception as e:
                LOGGER.error(f"Error while stopping collector: {e}")
        raise
    finally:
        # ensure collector stopped on normal exit too
        if real_stocks:
            try:
                collector.stop(timeout=5.0)
            except Exception:
                pass


if __name__ == "__main__":
    main()
