# Pacavanza

Pacavanza is an automated algorithmic trading and monitoring suite designed for two distinct markets: **Futures Trading via Interactive Brokers (IBKR)** and **Mini Futures Trading via Avanza**.

Built with an event-driven async architecture in Python, it separates market data collection, real-time strategy evaluation, and order execution while providing a centralized FastAPI backend for real-time charting and monitoring.

---

## 🌟 Major Features

### 1. IBKR OMXS30 Futures (Termin på svenska) Trading

- **Automated Trade Management**: Semi-auto order placement and advanced tracking for open future positions. See [order handling scenarios](./docs/order_handling_scenarios.md).
- **ABC Pattern Trailing Stops**: Implements an intelligent, stateless backward-scanning algorithm (`future_trade_monitor.py`) to dynamically detect A-B-C pivot fractals in real-time. Once a breakout is confirmed, it automatically trails your stop-loss tight to the most recent 'C' pivot to lock in profits.
- **End of Day (EOD) Auto-Flatten**: Automatically cancels all pending orders and force-closes open positions right before the market closes (configured at 17:25 CEST) to avoid overnight margin requirements and gap risks.

### 2. Avanza Mini Futures Trading

- **Live Market Streaming**: Connects directly to Avanza's Server-Sent Events (SSE) stream (`avanza_sse_client.py`) for ultra-low latency price updates.
- **Automated Execution**: Monitors underlying assets and executes rapid trades on leveraged mini futures (`avanza_trading_monitor.py`).

### 3. Centralized Backend & Visualization

- **FastAPI Server**: Hosts REST endpoints for manual trade intervention and querying system state (`backend.py`).
- **WebSockets & Pub/Sub**: Streams live chart data directly to the browser for real-time UI updates.
- **Interactive UI**: Includes local static HTML dashboards for tracking the portfolio and viewing real-time price action overlaid with EMA indicators and active orders.

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

The event-driven architecture requires a running **Redis** server on the default port (**6379**). You can install it via your package manager (e.g., `brew install redis` or `sudo apt-get install redis`) and start it using the `redis-server` command.

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

#### IBKR Setup (`config.json`)

You must configure your IBKR connection details by providing a `config.json` file in the root directory. Copy the provided `samples/config.json.sample` to `config.json` under repo root folder and adjust the IP address, port, and your account ID for both live ("real") and "paper" trading environments, e.g.:

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

This will automatically launch the backend API and all the required market data collectors and trade monitors in the background.

_You can now visit `http://localhost:8001/chart` in your browser to view the dashboard._

_(Use `python -m pacavanza.daemon_controller stop` to gracefully shut down the entire system)._

### 5. Operation

Once running, the system operates autonomously.

- If a future position is opened manually (or by another bot), `future_trade_monitor.py` will automatically detect the position and begin trailing the stop-loss using the ABC pivot pattern.
- At exactly 17:25 CEST, all active future positions will be automatically flattened.

## Known issues

See [known_issues](./docs/known_issues.md).
