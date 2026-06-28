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
    - Places **Two Distinct Orders**:
      1. A Standalone Stop Order (N contracts) to close the existing position (`orderRef="CloseOnly"`).
      2. A new Bracket Order (M - N contracts) to enter a net position in the opposite direction, complete with new SL and TP children.

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

### D. Limit Orders (Buy / Sell Limit)

Used to place a standalone limit order at a specific price.

- **Trigger**: User inputs a price in the `LMT` field and clicks `[B] Limit` or `[S] Limit`.
- **Backend Flow**: Places a standard `LimitOrder` via IBKR.
- **Chart UI**: Renders as a single horizontal dashed line (`horizontal-ray`). It is colored **blue** (`#2196F3`) for Buy Limit orders and **grey** (`#9E9E9E`) for Sell Limit orders.
- **Lifecycle**: Remains in the order book until the market reaches the specified price, at which point it fills.

---

## 3. Order Management & Modification Scenarios

Once orders are active, users can manage them via the UI or the chart.

### A. Editing Orders via Chart Dragging

- **User Action**: The user drags the Stop Loss, Take Profit, or Entry line of an order directly on the TradingView chart.
- **Execution**: `drawing_tools.js` handles the mouse release event and calculates the new price. `future_chart.js` dispatches an `edit_order` API call. The backend updates the `auxPrice` (for stops) or `lmtPrice` (for limits) and re-transmits the order to IBKR. Note: Dragging the Entry line will **only** transmit an update if the entry order is still pending (not fulfilled). If the position is already open, dragging the entry line visually moves it, but no entry update is sent to the backend.

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

To ensure SL/TP logic remains robust against manual interventions or split executions, the backend utilizes an automated synchronizer.

- **Trigger**: Every time an order fills, the position updates (`_on_position` event), which triggers `_sync_sl_tp_volume()`. (Note: the synchronizer is called from `_on_position` rather than `_on_exec_details` because `ib.positions()` updates asynchronously after execution details fire — calling it from `_on_exec_details` would read the old position size.)
- **Logic**:
  1. Queries the absolute current position size.
  2. Iterates over all open child orders (identified by `parentId` or `ocaGroup`).
  3. **If flat (position = 0)**: Cancels all active SL/TP orders to prevent ghost entries.
  4. **If active (position > 0)**: Identifies SL/TP orders in the wrong direction and cancels them. Updates the volume (`totalQuantity`) of the correct SL/TP orders to perfectly match the current position size. Transmits the update to IBKR.
