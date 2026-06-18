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
