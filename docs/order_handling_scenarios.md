# PACavanza Order Handling Scenarios

This document outlines the complete lifecycle of different order types within the PACavanza application, tracing the data flow from user interaction in the GUI to the final order execution on Interactive Brokers (IBKR) via the backend.

## 1. Architecture & Data Flow Overview

1. **User Interaction (Frontend)**: The user configures order parameters (volume, prices) and clicks trading buttons in the UI (`chart.html`).
2. **API Request**: The frontend JavaScript (`future_chart.js`) captures the input and sends a POST request to the corresponding backend endpoint (e.g., `/ibkr/buy_stop`).
3. **Order Logic (Backend)**: The Python backend (`ibkr_trading.py`) interprets the request, queries the current position, and formulates the appropriate `ib_async` order objects (Market, Limit, Stop, Bracket).
4. **IBKR Execution**: The backend calls `ib.placeOrder()` to transmit the orders to Interactive Brokers.
5. **Event Subscription**: The backend listens to IBKR events (`orderStatusEvent`, `execDetailsEvent`, `positionEvent`). When an order executes or its status changes, it updates the local state. When the position updates (`positionEvent`), the `_sync_sl_tp_volume()` synchronizer runs (triggered from `_on_position` rather than `_on_exec_details` because `ib.positions()` updates asynchronously after execution details fire).
6. **WebSocket Broadcast**: The backend broadcasts order updates via WebSockets.
7. **UI / Chart Update**: The frontend receives the WebSocket message, re-renders the "Active Orders" panel, and updates the TradingView-style chart drawings (`drawing_tools.js`) to visually represent the orders.

---

## 2. Scenarios by Order Type

### A. Market Orders (Buy / Sell Market)

Used to enter or exit a position immediately at the best available current price.

- **Trigger**: User sets the volume and clicks `[B] Market` or `[S] Market`.
- **Backend Flow**:
  - Calls `_execute_market_buy_order` or `_execute_market_sell_order`.
  - Creates a `MarketOrder("BUY", volume)` or `MarketOrder("SELL", volume)`.
  - Transmits to IBKR.
- **Lifecycle**:
  - Fills almost instantaneously.
  - The `_on_position` event fires (after `ib.positions()` is updated asynchronously), which triggers `_sync_sl_tp_volume()` to align any existing open stop-loss (SL) / take-profit (TP) order volumes with the new total position.
  - UI briefly shows the order as "Filled" before it disappears from the active order list.

### B. Stop Orders (Position-Aware Entry/Exit)

These are dynamic orders whose behavior heavily depends on the **current position**.

- **Trigger**: User clicks `[B] Stop` or `[S] Stop`.
- **Backend Flow**: The backend queries the current signed position (`_get_signed_position()`). It then executes one of four distinct sub-scenarios:
  - **Scenario B1: Flat (No Position) -> Enter New Position**
    - The backend calculates dynamic Stop Loss (SL) and Take Profit (TP) prices based on recent bar data, swing highs/lows, and a predefined Profit/Loss ratio.
    - Places a **Bracket Order**:
      - Parent Order: Stop Entry (`transmit=False`).
      - Child 1: Take Profit Limit Order (`transmit=False`, linked to parent).
      - Child 2: Stop Loss Stop Order (`transmit=True`, linked to parent, triggers the whole group).
    - **Chart UI**: Renders a `long-position` or `short-position` visual tool on the chart, tying Entry, SL, and TP together.

  - **Scenario B2: Same Direction (Scale Up)**
    - The user is already long and clicks Buy Stop (or short and clicks Sell Stop).
    - Places a **Standalone Stop Order** (`orderRef="ScaleUp"`). No new SL/TP bracket is attached to avoid duplicate exit orders.
    - **Lifecycle**: Once filled, `_sync_sl_tp_volume()` automatically scales up the volume of the original entry's SL/TP orders to match the new larger position.

  - **Scenario B3: Opposite Direction, Exact Match (Close Position)**
    - The user is long N contracts and clicks Sell Stop for N contracts (or short N and clicks Buy Stop for N).
    - Places a **Standalone Stop Order** (`orderRef="CloseOnly"`). If an existing SL order from a prior bracket/OCA is found, the backend adjusts that existing order's price instead of placing a new one, avoiding duplicate stop-loss orders for the same position direction.
    - **Lifecycle**: When this order fills, the position drops to 0. `_sync_sl_tp_volume()` detects the zero position and automatically cancels all remaining active SL/TP orders.

  - **Scenario B4: Opposite Direction, Partial Match (Close + Enter Reverse)**
    - The user is long N contracts and clicks Sell Stop for M contracts (or short N and clicks Buy Stop for M), where M > N.
    - We first check if there is an existing bracket/OCA SL order for the position:
      1. If found, we **modify** the existing SL order price with the new stop price (protecting the existing N contracts), and then place a **New Bracket Order** (M - N contracts) to enter a net position in the opposite direction.
      2. If not found, we place **Two Distinct Orders**:
         - A Standalone Stop Order (N contracts) to close the existing position (`orderRef="CloseOnly"`).
         - A New Bracket Order (M - N contracts) to enter a net position in the opposite direction, complete with new SL and TP children.

### C. Manual OCA Bracket Orders (One-Cancels-All)

Used when a user has an open position and wants to manually attach an SL and TP.

- **Trigger**: User fills the `SL` and `TP` price inputs, selects the Action (Buy/Sell), and clicks the 📎 button (`btn-place-oca`).
- **Backend Flow**:
  - Validates that an active position exists.
  - Creates a Limit Order (for the TP) and a Stop Order (for the SL).
  - Groups them using IBKR's `IB.oneCancelsAll(ocaGroup=..., ocaType=1)`.
- **Lifecycle**:
  - Both orders become immediately active.
  - If the market hits the TP price, the Limit Order fills, and IBKR automatically cancels the Stop Order.
  - Conversely, if the SL is hit, the Stop Order fills, and the Limit Order is canceled.
- **Chart UI**: The frontend detects the `ocaGroup`, groups the two orders, and renders a `long-position` or `short-position` tool on the chart based on the provided prices.

### D. Breakout Stop OCA Orders (BO-STP-OCA)

Used to trade breakouts in either direction when the market is consolidating, automatically entering a position when the breakout occurs while canceling the opposite breakout order.

- **Trigger**: User inputs a higher price for Long (`L`) and a lower price for Short (`S`) in the `BO-STP:` panel, and clicks `⚡ BO` (`btn-place-bostp-oca`).
- **Validation**:
  - **Flat Position Only**: Only allowed when there is **no open position** (`position == 0`). If an active position exists, the order is blocked to keep order handling scenarios predictable and avoid conflicting reversal/scale-up interactions.
  - **Price Relationship**: The Long stop price must be strictly greater than the Short stop price (`highPrice > lowPrice`). The UI provides an interactive swap prompt if inverted.
  - **Volume**: Must be greater than 0.
  - **Market Price Check**: The UI warns the user if the current market price has already exceeded either stop price (which would trigger immediately).
- **Backend Flow**:
  - Validates flat position (`_get_signed_position() == 0`).
  - Creates two `StopOrder` instances:
    - Long: `StopOrder("BUY", volume, highPrice, tif="DAY")`
    - Short: `StopOrder("SELL", volume, lowPrice, tif="DAY")`
  - Tags both orders with `orderRef="BreakoutStop"`.
  - Groups them via IBKR's `IB.oneCancelsAll(orders=[long_order, short_order], ocaGroup=..., ocaType=1)`.
  - Transmits both orders to IBKR.
- **Lifecycle & Background Synchronization**:
  - **Pending State (Flat)**: Both stop orders remain active. The automated synchronizer (`_sync_sl_tp_volume()`) explicitly protects `BreakoutStop` orders while flat so they are not cancelled as orphans.
  - **Execution (Breakout Triggered)**: Once the market breaks out and one stop order fills, IBKR's OCA engine automatically cancels the other stop order.
  - **Automated Bracket Placement (In Position)**:
    - Upon fill, the backend automatically generates and places a new **OCA Bracket Order** (`place_oca_bracket`) to protect the position:
      - **Long BO Fill**: Stop Loss placed at the opposite (Short) BO price; Take Profit Limit placed at a 2:1 profit-to-risk ratio (`entryPrice + 2 * (highPrice - lowPrice)`).
      - **Short BO Fill**: Stop Loss placed at the opposite (Long) BO price; Take Profit Limit placed at a 2:1 profit-to-risk ratio (`entryPrice - 2 * (highPrice - lowPrice)`).
    - In the **Active Orders panel**, the newly placed TP and SL bracket orders appear as active/editable orders. The opposite cancelled breakout order is retained with its status displayed as "Cancelled" and its price/qty inputs disabled.
- **Chart UI**:
  - **Pending**: Renders as **two gray dotted horizontal rays** (`horizontal-ray` with `lineDash: [3, 3]`) at the respective Long and Short stop prices. Each ray can be interactively dragged on the chart to adjust the trigger price via `/ibkr/edit_order`.
  - **Filled**: Automatically transforms into a `long-position` or `short-position` drawing on the chart with anchors at the Entry price, opposite BO Stop Loss price, and 2:1 Take Profit price. The drawing is seamlessly backed by the live exit bracket orders, allowing interactive chart drag-editing of the live TP and SL levels.
  - **Closed**: Once the position is completely closed (position returns to 0), the drawing is automatically removed.

### E. Limit Orders (Buy / Sell Limit)

Used to place a standalone limit order at a specific price.

- **Trigger**: User inputs a price in the `LMT` field and clicks `[B] Limit` or `[S] Limit`.
- **Backend Flow**: Places a standard `LimitOrder` via IBKR.
- **Chart UI**: Renders as a single horizontal dashed line (`horizontal-ray`). It is colored **blue** (`#2196F3`) for Buy Limit orders and **grey** (`#9E9E9E`) for Sell Limit orders.
- **Lifecycle & Execution**:
  - Remains in the order book until the market reaches the specified price, at which point it fills.
  - **Flat Entry (New Position)**: When filled from flat (no open position), the Auto-OCA feature automatically attaches an SL/TP bracket to protect the initial position.
  - **Scale-Up Entry (Existing Position)**: When filled in the same direction as an existing position (e.g., selling limit on a pullback to scale into a short position between average cost and stop price), **no duplicate OCA bracket is created**. Instead, `_sync_sl_tp_volume()` automatically scales up both the existing Take Profit and Stop Loss order quantities to match the new total position size, keeping their original prices unchanged.

---

## 3. Order Management & Modification Scenarios

Once orders are active, users can manage them via the UI or the chart.

### A. Editing Orders via Chart Dragging

- **User Action**: The user drags the Stop Loss, Take Profit, or Entry line of an order directly on the TradingView chart.
- **Execution**: `drawing_tools.js` handles the mouse release event and calculates the new price. `future_chart.js` dispatches an `edit_order` API call. The backend updates the `auxPrice` (for stops) or `lmtPrice` (for limits) and re-transmits the order to IBKR. If the parent entry order has already filled, `edit_order` detaches `parentId` to prevent TWS modification rejection while preserving its `ocaGroup` and `orderRef` so it remains linked in IBKR's OCA engine. Note: Dragging the Entry line will **only** transmit an update if the entry order is still pending (not fulfilled). If the position is already open, dragging the entry line visually moves it, but no entry update is sent to the backend.

### B. Editing Orders via Text Inputs

- **User Action**: The user edits the quantity or price inputs in the "Active Orders" panel and clicks the ✏️ edit button.
- **Execution**: Similar to chart dragging, the backend modifies the live IBKR order with the new parameters.

### C. Converting to Market Order (Chase)

- **User Action**: The user clicks `→MKT` on a pending Stop or Limit order.
- **Execution**: The backend cancels the pending order and immediately issues a `MarketOrder` for the same action and volume.

### D. Canceling Orders

- **User Action**: The user clicks the ❌ button next to an order.
- **Execution**: The backend calls `ib.cancelOrder()`. If the canceled order was part of an OCA bracket, IBKR will typically cancel the grouped counterpart depending on the OCA type.

### E. Mass Deletion of Stop Losses

- **User Action**: The user clicks `❌ SL` in the toolbar.
- **Execution**: The backend iterates through all active trades, identifies orders of type `STP` or `STP LMT`, and cancels them all.

---

## 4. Background Synchronization (`_sync_sl_tp_volume`)

To ensure SL/TP logic remains robust against manual interventions, split executions, or scale-up orders, the backend utilizes an automated synchronizer.

- **Trigger**: Every time an order fills, the position updates (`_on_position` event), which triggers `_sync_sl_tp_volume()`. (Note: the synchronizer is called from `_on_position` rather than `_on_exec_details` because `ib.positions()` updates asynchronously after execution details fire — calling it from `_on_exec_details` would read the old position size.)
- **Logic**:
  1. Queries the absolute current position size (`abs(position)`).
  2. Iterates over all open child/exit orders (identified by `ocaGroup`, `parentId`, bracket `orderRef`, or active opposite-direction closing orders).
  3. **If flat (position = 0)**: Cancels all active SL/TP orders to prevent ghost entries (while protecting pending `BreakoutStop` or entry parent orders).
  4. **If active (position != 0, Long or Short)**: Identifies SL/TP orders in the wrong direction and cancels them. Updates the volume (`totalQuantity`) of both the Take Profit and Stop Loss orders to perfectly match the current position size without modifying their prices. Transmits the update to IBKR.
