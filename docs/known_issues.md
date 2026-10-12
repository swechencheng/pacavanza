# Known Issues and Benign Errors

This document explains some known, benign errors that may appear in the application logs, particularly those related to IBKR integrations and order execution.

## IBKR Error 10148 & Error 202 during Order Fills

**Log Output:**

```
ib_async.wrapper - ERROR - Error 10148, reqId 629: OrderId 629 that needs to be cancelled cannot be cancelled, state: Filled.
ib_async.wrapper - ERROR - Error 202, reqId 628: Order Canceled - reason:
```

### Explanation

These two "ERROR" lines from `ib_async` are completely benign. They occur due to a race condition when the daemon actively trails stop-loss orders in a fast-moving market.

Here is the exact sequence of events that triggers these logs:

1. **The Trail/Modify Race:** When `future_trade_monitor.py` attempts to update/trail a stop-loss price, it sends a modify request to IBKR. Under the hood, modifying an order in IBKR involves sending a cancel-and-replace request.
2. **Error 10148 (`cannot be cancelled, state: Filled`):** If the market hits the stop price and fills the order at the exact millisecond the modify request is sent, IBKR's server receives the modify (cancel) request but realizes it is too late. It replies with Error 10148: _"I can't modify/cancel this order, it literally just got filled!"_
3. **Error 202 (`Order Canceled`):** Because the stop-loss order was successfully filled, IBKR's native OCA (One-Cancels-All) logic automatically cancels the remaining leg of the bracket (the Take-Profit limit order). `ib_async` logs IBKR's standard cancellation notification (Code 202) as an `ERROR`, even though it is just an informational alert confirming that the OCA group worked correctly.

### Resolution

No action is required. The system is functioning as intended: the stop-loss successfully executes and closes the position, and the take-profit order is correctly cleaned up by IBKR. These logs simply indicate that the market hit the stop-loss at the exact moment the daemon attempted to trail it.

---

## Avanza vs Direct Exchange Feed (TradingView) Volume Differences & RVOL Dynamics

### 1. Opening Auction (09:00) Volume Discrepancy

**Observation:**
The first 5-minute bar (09:00 - 09:05) on TradingView (`OMXNORDIC:FXS301!`) often shows a significantly higher relative volume ratio than the corresponding bar on Pacavanza via Avanza.

**Explanation:**

- **Opening Uncrossing Auction:** TradingView connects directly to the official Nasdaq Stockholm exchange feed, which bundles the entire market opening uncrossing batch at 09:00:00 into the first bar's volume. On TradingView, this auction print can cause bar 1 volume to be 6× or more higher than subsequent bars.
- **Avanza Retail Feed:** Avanza's chart history API aggregates continuous contract trades and may report opening auction prints differently. While bar 1 is still the highest volume bar of the session on Avanza, its relative multiple over bar 2 is typically ~2.4× rather than 6×.

### 2. Cumulative RVOL Dilution Effect ("Declining Highs")

**Explanation:**
When calculating **Relative Volume at Time (Cumulative)**:
$$\text{RVol}(t) = \frac{\sum_{i=1}^t V_{\text{today}, i}}{\frac{1}{N} \sum_{d=1}^N \sum_{i=1}^t V_{d, i}}$$

If the market opens with an outsized volume burst on bar 1 ($V_{\text{today}, 1} \gg \overline{V}_{\text{past}, 1}$), the ratio $\text{RVol}(1)$ starts at its highest point of the day. As the session progresses and subsequent bars trade at normal historical volumes, adding roughly equal quantities to both numerator and denominator naturally dilutes the initial surge:
$$\frac{A + \Delta}{B + \Delta} < \frac{A}{B} \quad \text{for } A > B \text{ and } \Delta > 0$$
This dilution creates the classic downward-sloping trend of relative volume highs across the session.

### 3. Historical Lookback Truncation (Resolved)

**Previous Symptom:**
Pacavanza's afternoon RVOL unexpectedly surged higher throughout the day, creating a new daily peak late in the afternoon instead of diluting downwards.

**Root Cause:**
OMXS30 futures trade ~105 five-minute bars per day. The backend previously maintained a hardcoded history limit of 1500 bars, which only covered ~14 trading days. When calculating a 20-day historical average, the oldest 5 trading days (often high-volume contract expiration sessions) were dropped. This artificially depressed the historical denominator for afternoon hours by ~20%, causing normal afternoon volume to produce an artificially inflated ratio (e.g. 1.41).

**Resolution:**
The bar retention limit has been expanded to `MAX_RECENT_BARS = 3600` (covering ~34 trading days), ensuring the full 20-day lookback window is always available with accurate historical denominators.

### 4. SSE Trade Stream Deduplication

**Explanation:**
Avanza's SSE trade feed sends an `initialSubscription` dump of recent fulfilled trades upon connecting or reconnecting. To prevent historical trades from artificially inflating the current forming bar's volume:

- `future_market_daemon.py` filters out any trades tagged with `initialSubscription` or `cancelled`.
- Trades are tracked per `tradeId` using an incremental volume delta ($\Delta = V_{\text{new}} - V_{\text{prev}}$), ensuring partial execution updates only accumulate their newly executed contracts.
- Completed bars are periodically cross-referenced and reconciled with Avanza's official chart history (`fetch_avanza_chart_history`) to maintain authoritative volume integrity.
