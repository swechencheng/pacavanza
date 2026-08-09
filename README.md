# Pacavanza

Pacavanza is an automated algorithmic trading and monitoring suite designed for two distinct markets: **Futures Trading via Interactive Brokers (IBKR)** and **Mini Futures Trading via Avanza**.

Built with an event-driven async architecture in Python, it separates market data collection, real-time strategy evaluation, and order execution while providing a centralized FastAPI backend for real-time charting and monitoring.

---

## 🌟 Major Features

### 1. OMXS30 Futures (Termin på svenska) Trading through IBKR with live data from Avanza

- **Automated Trade Management**: Semi-auto order placement and advanced tracking for open future positions. See [order handling scenarios](./docs/order_handling_scenarios.md).
- **Auto Contract Rollover**: Implements a built-in roll-window logic that automatically transitions to trading the next back-month contract ~5 days before the expiration of the current front-month contract, ensuring seamless continuity.
- **ABC Pattern Trailing Stops**: Implements an intelligent, stateless backward-scanning algorithm (`future_trade_monitor.py`) to dynamically detect A-B-C pivot fractals in real-time. Once a breakout is confirmed, it automatically trails your stop-loss tight to the most recent 'C' pivot to lock in profits.
- **End of Day (EOD) Auto-Flatten**: Automatically cancels all pending orders and force-closes open positions right before the market closes (configured at 17:25 CEST) to avoid overnight margin requirements and gap risks.

### 2. Avanza Mini Futures Trading

- **Live Market Streaming**: Connects directly to Avanza's Server-Sent Events (SSE) stream (`avanza_sse_client.py`) for ultra-low latency price updates.
- **Automated Execution**: Monitors underlying assets and executes rapid trades on leveraged mini futures (`avanza_trading_monitor.py`).

**Note on Avanza Mini Futures**: These instruments are provided by [Morgan Stanley](https://etp.morganstanley.com/se/sv/). Unlike standard OMXS30 futures, trading Avanza minis does not have full feature support and they are not traded via IBKR. Due to this limitation, the Avanza daemons are disabled by default.

### 3. Centralized Backend & Interactive Dashboard

The FastAPI backend (`backend.py`) acts as the central nervous system, consuming Redis streams to broadcast updates to connected web clients via WebSockets and exposing REST endpoints for trade execution.

The local HTML dashboard (`http://localhost:8001`) offers a rich set of features to improve the manual trading and monitoring experience:

- **Real-Time & Historical Charting**: Uses TradingView's Lightweight Charts to plot live OHLC data. A dropdown selector allows you to instantly pull up charts of historical futures.
- **Advanced Order Management**:
  - **Quick Execution Controls**:
    Trade buttons are designed for fast order placement based on the current market situation. All of them will place a bracketed order with a roughly 2:1 take profit ratio and a stop-loss order. No order confirmation is needed to catch the extreme moves in the market.
    - **Market (B Mkt / S Mkt)**: Instantly buy or sell at the current market price.
    - **Market Close (C Mkt)**: Instantly close your entire open position at the market price.
    - **Auto Stop Entry (B Stp / S Stp)**: Schedules a bracketed stop-entry order for the start of the _next_ 5-minute bar. The entry trigger is dynamically calculated based on the bar's high/low, and it automatically attaches a 2:1 Take-Profit and a Stop-Loss (placed below/above the recent swing leg).
    - **Late Auto Stop (B Late / S Late)**: Instantly places the same auto-calculated bracketed stop-entry order using the _last completed_ 5-minute bar, without waiting for the current bar to close.
    - **Explicit Limit/Stop (Lmt / Stp)**: Place standard limit or stop orders manually by entering a specific price.
  - **Visual Order Lines**: Active orders are displayed as interactive lines on the chart.
  - **OCA Bracket Orders (One-Cancels-All)**: Automatically attach Stop-Loss (SL) and Take-Profit (TP) levels to a position, managed directly from the UI toolbar.
  - **Order Lifecycle Panel**: View all working orders in a list. Easily modify price and quantity inline, or cancel orders with a single click.
- **Position & Market Tape Overlay**:
  - **Position Heads-Up Display**: A persistent on-chart overlay displaying your current Long/Short/Flat position size and average entry price.
  - **Live Order Depth (Level 2)**: A streaming tape panel visualizing bid/ask spread and market depth with dynamic volume bars.
  - **Live Trades Panel**: A real-time scrolling tape of executed market trades.
- **On-Chart Drawing Tools**: Includes a built-in drawing toolbar for plotting trend lines, horizontal support/resistance, rectangles, and Fibonacci retracements.

---

## 🏗 Software Architecture

The system is built on a highly decoupled, async **Event-Driven Architecture** powered by Redis:

1. **Market Collectors (`future_market_daemon.py`, `avanza_market_daemon.py`)**:
   These daemons run independently, fetching live ticker data and bar updates from IBKR and Avanza. They publish this standardized market data directly to Redis channels (e.g., `pacavanza:future_updates`).
2. **Strategy Monitors (`future_trade_monitor.py`, `avanza_trading_monitor.py`)**:
   These scripts subscribe to the Redis data streams. They maintain internal state machines (like tracking highest highs, lowest lows, and pivot points), evaluate entry/exit logic, and issue order commands when conditions are met.
3. **The Backend (`backend.py`)**:
   Acts as the central nervous system. It consumes the same Redis streams to broadcast updates to connected web clients via WebSockets. It also exposes HTTP endpoints that the Strategy Monitors can call to execute trades, ensuring that all API credentials and connection pools (like the IBKR socket) are managed in one place.

---

## 🔌 Dependency on IBKR TWS/GW

For the futures trading capabilities to function, Pacavanza has a **strict dependency** on a running instance of **Interactive Brokers Trader Workstation (TWS)** or **IB Gateway (GW)**.

- **Connection**: The system uses the `ib_async` library to communicate with TWS/GW over a local TCP socket.
- **API Settings**:
  - You must explicitly enable **"Enable ActiveX and Socket Clients"** in your TWS/GW API settings.
  - Ensure the socket port in the application matches your TWS/GW configuration (commonly `7496` or `7497` for TWS, and `4001` or `4002` for IB Gateway).
- **Client IDs**: The application uses specific `clientId`s (e.g., `51`, `42`) to connect to the API. This ensures that the trading monitors and the backend can share the gateway without overriding each other or conflicting with other trading bots you might be running.

---

## 🚀 How to Use Guide

### Prerequisites

#### Python 3.10+

Ensure you have Python 3.10 or higher installed on your system.

#### Redis Server

The event-driven architecture requires a running **Redis** server. By default, the system connects to `localhost` on port **6379**. You can install it via your package manager (e.g., `brew install redis` or `sudo apt-get install redis`) and start it using the `redis-server` command. You can customize the host and port via `config.json`.

#### IBKR TWS or IB Gateway

For futures trading, you must have a running instance of **Interactive Brokers TWS** or **IB Gateway** logged into your paper or live account. Ensure that API connections are enabled as described in the architecture section.

#### Avanza TOTP secret

This is the Time-based One-Time Password secret used for Two-Factor Authentication. For instructions on how to extract this secret from your Avanza account, please refer to the [avanza-api documentation on getting a TOTP secret](https://github.com/Qluxzz/avanza#getting-a-totp-secret).

### 1. Installation

Clone the repository and install the required dependencies:

```bash
git clone git@github.com:swechencheng/pacavanza.git
cd pacavanza
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configuration

#### Avanza Authentication Setup (`secret.json`)

To authenticate with the Avanza API, copy the provided `samples/secret.json.sample` to `secret.json` under repo root folder and fill in your details:

```json
{
  "username": "your_avanza_username",
  "password": "your_avanza_password",
  "totpSecret": "YOUR_TOTP_SECRET_STRING",
  "accountId": "your_avanza_account_id"
}
```

#### System Setup (`config.json`)

You must configure your IBKR connection details, and optionally override the default Redis and Backend ports, by providing a `config.json` file in the root directory. Copy the provided `samples/config.json.sample` to `config.json` under repo root folder and adjust the IP address, port, and your account ID for both live ("real") and "paper" trading environments, e.g.:

```json
{
  "IBKR": {
    "real": {
      "host": "192.168.1.100",
      "port": 4001,
      "account": "UXXXXXXXX",
      "clientId": 41
    },
    "paper": {
      "host": "127.0.0.1",
      "port": 4002,
      "clientId": 42
    }
  },
  "redis": {
    "host": "localhost",
    "port": 6379
  },
  "backend": {
    "port": 8001
  }
}
```

_(You can also review `pacavanza/config.py` to modify the default hardcoded symbols and trade quantities)._

### 3. Start the Infrastructure

Ensure your underlying services are running:

1. Start Redis: `redis-server`
2. Open IBKR TWS/Gateway and verify the API socket is open.

### 4. Launch the System

Use the provided daemon controller to spin up the entire ecosystem seamlessly.

**Start the system:**

```bash
python -m pacavanza.daemon_controller start
```

This will automatically launch the backend API and all the required IBKR futures market data collectors and trade monitors in the background.

To also enable the Avanza mini futures daemons (disabled by default), append the `--enable-ava-mini` flag:

```bash
python -m pacavanza.daemon_controller start --enable-ava-mini
```

_You can now visit `http://localhost:<backend-port>` (default port is `8001`) in your browser to view the dashboard._

_(Use `python -m pacavanza.daemon_controller stop` to gracefully shut down the entire system)._

### 5. Operation

Once running, the system operates autonomously.

- If a future position is opened manually (or by another bot), `future_trade_monitor.py` will automatically detect the position and begin trailing the stop-loss using the ABC pivot pattern.
- At exactly 17:25 CEST, all active future positions will be automatically flattened.

## Known issues

See [known_issues](./docs/known_issues.md).
