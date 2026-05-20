/**
 * future_chart.js — OMXS30 continuous future chart (single panel).
 * Extends PACChartApp from chart_core.js.
 *
 * Trading controls call /ibkr/* endpoints (separate from Avanza /trade/*).
 */
class FutureChartApp extends PACChartApp {
  constructor() {
    super();
    window.PAC_TRADING_STATE = { futureInstrument: null };
    this.chartFuture = this.createChartManager("chart-future", { toolTipId: "chart-ohlc-info-future" });
    this.instrumentMapFlat = null;
    this._orderPollTimer = null;
    this._orderDrawingIds = [];

    // Global error trackers for remote backend logging
    window.addEventListener("error", (e) => {
      this._remoteLog("ERROR", `Unhandled: ${e.message} at ${e.filename}:${e.lineno}`);
    });
    window.addEventListener("unhandledrejection", (e) => {
      this._remoteLog("ERROR", `Unhandled rejection: ${e.reason}`);
    });
    this._remoteLog("INFO", "FutureChartApp constructor initialized");
  }

  _remoteLog(level, message) {
    console.log(`[${level}] ${message}`);
    fetch("/client_log", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ level, message })
    }).catch(() => { });
  }

  getStatusChartManager() { return this.chartFuture; }

  // ── Initialization ────────────────────────────────────────────────
  async initialize() {
    const res = await fetch("/active_future");
    if (!res.ok) throw new Error("active_future fetch failed: " + res.status);
    const info = await res.json();

    const futureKey = info.key;
    if (!futureKey) {
      this._error("No active future returned from backend!");
      document.getElementById("status").textContent = "No future found";
      return;
    }

    this.instrumentMapFlat = {
      [futureKey]: {
        name: info.name,
        orderbookId: info.orderbookId,
        timezone: info.timezone,
        market_open: info.market_open,
        market_close: info.market_close,
      },
    };

    window.PAC_TRADING_STATE.futureInstrument = futureKey;
    const labelEl = document.getElementById("label-future");
    const originalText = info.name || futureKey;
    labelEl.textContent = originalText;

    const linkEl = labelEl.closest("a");
    if (linkEl) {
      linkEl.addEventListener("mouseenter", () => {
        labelEl.textContent = "Go to AVA Mini's";
      });
      linkEl.addEventListener("mouseleave", () => {
        labelEl.textContent = originalText;
      });
    } else {
      labelEl.addEventListener("mouseenter", () => {
        labelEl.textContent = "Go to AVA Mini's";
      });
      labelEl.addEventListener("mouseleave", () => {
        labelEl.textContent = originalText;
      });
    }

    await this.loadHistoryForInstrument(this.chartFuture, futureKey, this.instrumentMapFlat);
    this.ensureMarketCountdown(this.chartFuture.groupingState);
  }

  // ── WebSocket message handler ─────────────────────────────────────
  handleWsMessage(msg) {
    // Handle order update broadcasts from backend
    if (msg.type === "order_update" && msg.orders) {
      this._renderOrders(msg.orders);
      return;
    }

    if (msg.instrument !== window.PAC_TRADING_STATE.futureInstrument || !msg.bar) return;

    const t = this.isoToLWTime(msg.bar.start_time);
    const candleData = { time: t, open: +msg.bar.open, high: +msg.bar.high, low: +msg.bar.low, close: +msg.bar.close };

    if (msg.type === "update") {
      this._applyBarUpdate(this.chartFuture, candleData);
      this.startCountdownForBar(t + this.INTERVAL_SECONDS, this.chartFuture.groupingState);
    } else if (msg.type === "completed") {
      this._applyBarCompleted(this.chartFuture, candleData);
      this.clearCountdown(true);
      this.ensureMarketCountdown(this.chartFuture.groupingState);
    }
    this._applyEMAFromMsg(this.chartFuture, msg);
  }

  // ── Helper: call API endpoint ─────────────────────────────────────
  async _callApi(endpoint, payload, method = "POST", alertOnError = false) {
    try {
      const opts = { method, headers: { "Content-Type": "application/json" } };
      if (method !== "GET") opts.body = JSON.stringify(payload);
      const res = await fetch(endpoint, opts);
      const text = await res.text();
      if (!res.ok) {
        console.error("[ibkr] Failed", res.status, text);
        let errMsg = text;
        try {
          const parsed = JSON.parse(text);
          if (parsed && parsed.detail) {
            errMsg = parsed.detail;
          }
        } catch (_) { }
        if (alertOnError) {
          alert(`Error: ${errMsg}`);
        }
        return null;
      }
      return JSON.parse(text);
    } catch (e) {
      console.error("[ibkr] Request error", e);
      if (alertOnError) {
        alert(`Request Error: ${e.message}`);
      }
      return null;
    }
  }

  // ── Trading Controls (IBKR via /ibkr/* endpoints) ─────────────────
  _initTradingControls() {
    const instrument = () => window.PAC_TRADING_STATE.futureInstrument;
    const contracts = () => parseInt(document.getElementById("order-contracts-future").value) || 1;

    // Bind stop/market buttons — these call existing /trade/* endpoints
    // (which the backend routes to IbkrTrading when that's the active trading instance)
    const bind = (id, endpoint, needsContracts) => {
      const el = document.getElementById(id);
      if (!el) return;
      el.addEventListener("click", async () => {
        const iid = instrument();
        if (!iid) return;
        const payload = { instrumentId: iid };
        if (needsContracts) payload.percentage = contracts(); // percentage repurposed as contracts
        const res = await this._callApi(endpoint, payload);
        if (res) {
          console.log("[ibkr] OK:", res);
          this._refreshOrders();
        }
      });
    };

    bind("btn-buy-stop-future", "/ibkr/buy_stop", true);
    bind("btn-cancel-buy-stop-future", "/ibkr/cancel_buy_stop", false);
    bind("btn-sell-stop-future", "/ibkr/sell_stop", true);
    bind("btn-cancel-sell-stop-future", "/ibkr/cancel_sell_stop", false);
    bind("btn-late-buy-stop-future", "/ibkr/late_buy_stop", true);
    bind("btn-late-sell-stop-future", "/ibkr/late_sell_stop", true);
    bind("btn-delete-stop-losses-future", "/ibkr/delete_stop_losses", false);
    bind("btn-market-buy-future", "/ibkr/market_buy", true);
    bind("btn-market-sell-future", "/ibkr/market_sell", true);

    // OCA Bracket button — calls new /ibkr/ endpoint
    const ocaBtn = document.getElementById("btn-place-oca");
    if (ocaBtn) {
      ocaBtn.addEventListener("click", async () => {
        const action = document.getElementById("oca-action").value;
        const volume = contracts();
        const limitPrice = parseFloat(document.getElementById("oca-limit-price").value);
        const stopPrice = parseFloat(document.getElementById("oca-stop-price").value);
        if (isNaN(limitPrice) || isNaN(stopPrice)) {
          alert("OCA: Both Limit (TP) and Stop (SL) prices are required.");
          return;
        }
        if (limitPrice === stopPrice) {
          alert("OCA: Limit (TP) and Stop (SL) prices cannot be equal.");
          return;
        }
        if (limitPrice > stopPrice && action !== "SELL") {
          alert("TP is greater than SL: Action must be S (SELL) to close a long position.");
          return;
        }
        if (limitPrice < stopPrice && action !== "BUY") {
          alert("TP is smaller than SL: Action must be B (BUY) to close a short position.");
          return;
        }
        const res = await this._callApi("/ibkr/place_oca_bracket", {
          action, volume, limitPrice, stopPrice,
        }, "POST", true);
        if (res) {
          console.log("[ibkr] OCA placed:", res);
          this._refreshOrders();
        }
      });
    }

    // Buy Limit button
    const buyLimitBtn = document.getElementById("btn-buy-limit-future");
    if (buyLimitBtn) {
      buyLimitBtn.addEventListener("click", async () => {
        const volume = contracts();
        const price = parseFloat(document.getElementById("limit-order-price").value);
        if (isNaN(price)) {
          alert("Limit Order: Price is required.");
          return;
        }
        const res = await this._callApi("/ibkr/limit_buy", {
          volume, price
        }, "POST", true);
        if (res) {
          console.log("[ibkr] Limit Buy placed:", res);
          this._refreshOrders();
        }
      });
    }

    // Sell Limit button
    const sellLimitBtn = document.getElementById("btn-sell-limit-future");
    if (sellLimitBtn) {
      sellLimitBtn.addEventListener("click", async () => {
        const volume = contracts();
        const price = parseFloat(document.getElementById("limit-order-price").value);
        if (isNaN(price)) {
          alert("Limit Order: Price is required.");
          return;
        }
        const res = await this._callApi("/ibkr/limit_sell", {
          volume, price
        }, "POST", true);
        if (res) {
          console.log("[ibkr] Limit Sell placed:", res);
          this._refreshOrders();
        }
      });
    }

    // Refresh orders button
    const refreshBtn = document.getElementById("btn-refresh-orders");
    if (refreshBtn) refreshBtn.addEventListener("click", () => this._refreshOrders());

    // Initial load + periodic polling
    this._refreshOrders();
    this._orderPollTimer = setInterval(() => this._refreshOrders(), 5000);
  }

  // ── Order Lifecycle Panel ─────────────────────────────────────────
  async _refreshOrders() {
    const res = await this._callApi("/ibkr/open_orders", null, "GET");
    if (res && res.orders) this._renderOrders(res.orders);
  }

  _renderOrders(orders) {
    this._updateOrderDrawings(orders);

    const container = document.getElementById("order-list");
    if (!container) return;

    if (!orders || orders.length === 0) {
      container.innerHTML = '<div class="order-empty">No active orders</div>';
      return;
    }

    container.innerHTML = orders.map(o => {
      const actionCls = o.action === "BUY" ? "tag-buy" : "tag-sell";
      const priceVal = o.price != null ? o.price : "";
      const isMkt = o.orderType === "MKT";
      const parentInfo = o.parentId ? `<span style="color:#555;">P:${o.parentId}</span>` : "";
      const ocaInfo = o.ocaGroup ? `<span style="color:#555;">OCA</span>` : "";

      const isFulfilled = !!o.fulfilled;
      const isDone = !!o.isDone;
      const fulfilledCls = isFulfilled ? "order-row-fulfilled" : "";
      const purpleStyle = isFulfilled ? 'style="color: #b388ff !important;"' : "";

      return `<div class="order-row ${fulfilledCls}" data-order-id="${o.orderId}" ${purpleStyle}>
        <span class="tag ${actionCls}">${o.action}</span>
        <span class="tag tag-type">${o.orderType}</span>
        <span style="color:#888;">×${o.totalQuantity}</span>
        ${isMkt ? '<span style="color:#ff9900;">MKT</span>' :
          `<input type="number" class="order-price-input" value="${priceVal}" step="0.25" data-oid="${o.orderId}" ${isFulfilled ? "disabled" : ""} ${purpleStyle} />`}
        <span style="color:#555; ${isFulfilled ? "color: #b388ff !important;" : ""}">${o.status}</span>
        ${parentInfo}${ocaInfo}
        <span style="flex:1;"></span>
        ${(!isMkt && !isFulfilled && !isDone) ? `<button class="order-btn" onclick="_futureApp._editOrderPrice(${o.orderId}, this)">✏️</button>` : ""}
        ${(!isMkt && !isFulfilled && !isDone) ? `<button class="order-btn order-btn-market" onclick="_futureApp._toMarket(${o.orderId})">→MKT</button>` : ""}
        ${!isDone ? `<button class="order-btn order-btn-danger" onclick="_futureApp._cancelOrder(${o.orderId})">❌</button>` : ""}
      </div>`;
    }).join("");
  }

  _updateOrderDrawings(orders) {
    const cm = this.chartFuture;
    if (!cm || !cm.drawingManager || !cm.toolRegistry) return;

    this._remoteLog("INFO", `_updateOrderDrawings starting. Total orders: ${orders ? orders.length : 0}. Chart data size: ${cm.data ? cm.data.size : 0}`);

    // 1. Clear previous drawings
    if (this._orderDrawingIds) {
      for (const id of this._orderDrawingIds) {
        try {
          cm.drawingManager.removeDrawing(id);
        } catch (e) {
          this._remoteLog("WARN", `Failed to remove drawing ${id}: ${e.message}`);
        }
      }
    }
    this._orderDrawingIds = [];

    // If no orders, or chart history is not loaded yet (no data), we don't draw anything
    if (!orders || orders.length === 0 || cm.data.size === 0) {
      this._remoteLog("INFO", `Skipping drawing sync: orders empty or cm.data empty.`);
      return;
    }

    const getBarTimeForOrder = (placedTimeStr) => {
      if (!placedTimeStr) return null;
      const placedUnix = this.isoToLWTime(placedTimeStr);
      const times = Array.from(cm.data.keys()).sort((a, b) => a - b);
      if (times.length === 0) return null;
      let orderBarTime = null;
      for (let i = times.length - 1; i >= 0; i--) {
        if (times[i] <= placedUnix) {
          orderBarTime = times[i];
          break;
        }
      }
      if (orderBarTime === null) {
        orderBarTime = times[0];
      }
      return orderBarTime;
    };

    // Filter orders
    const activeOrders = orders.filter(o => !o.isDone && !o.fulfilled);
    this._remoteLog("INFO", `Active orders for drawings: ${JSON.stringify(activeOrders)}`);

    // Track which order IDs are processed as part of a drawing group to avoid double rendering
    const processedOrderIds = new Set();

    // ── 1. Stop order with children (bracket) ──
    const parentIdToChildren = {};
    activeOrders.forEach(o => {
      if (o.parentId) {
        if (!parentIdToChildren[o.parentId]) {
          parentIdToChildren[o.parentId] = [];
        }
        parentIdToChildren[o.parentId].push(o);
      }
    });

    activeOrders.forEach(parent => {
      // Must be a parent order (no parentId)
      if (parent.parentId) return;
      const children = parentIdToChildren[parent.orderId] || [];
      if (children.length === 0) return;

      this._remoteLog("INFO", `Parent ${parent.orderId} has children: ${JSON.stringify(children)}`);

      // Find children: TP (LMT) and SL (STP / STP LMT)
      const tpChild = children.find(c => c.orderType === "LMT");
      const slChild = children.find(c => c.orderType === "STP" || c.orderType === "STP LMT");

      if (tpChild && slChild) {
        const orderBarTime = getBarTimeForOrder(parent.placedTime);
        if (orderBarTime !== null) {
          const times = Array.from(cm.data.keys()).sort((a, b) => a - b);
          const startIndex = times.indexOf(orderBarTime);
          let endBarTime;
          if (startIndex !== -1 && startIndex + 20 < times.length) {
            endBarTime = times[startIndex + 20];
          } else {
            endBarTime = orderBarTime + 20 * this.INTERVAL_SECONDS;
          }

          const toolType = parent.action === "BUY" ? "long-position" : "short-position";
          const id = `order-bracket-${parent.orderId}`;
          const anchors = [
            { time: orderBarTime, price: parent.price },
            { time: endBarTime, price: slChild.price },
            { time: endBarTime, price: tpChild.price }
          ];
          const style = {
            lineColor: parent.action === "BUY" ? "#26A69A" : "#EF5350",
            lineWidth: 1.5
          };
          const opts = {
            showPrices: true,
            showPercentage: true,
            showRiskReward: true
          };

          this._remoteLog("INFO", `Attempting to create bracket drawing ${toolType} for parent ${parent.orderId} with anchors: ${JSON.stringify(anchors)}`);
          try {
            const drawing = cm.toolRegistry.createDrawing(toolType, id, anchors, style, opts);
            if (drawing) {
              cm.drawingManager.addDrawing(drawing);
              this._orderDrawingIds.push(id);
              this._remoteLog("INFO", `Successfully added bracket drawing ${id}`);
            } else {
              this._remoteLog("WARN", `createDrawing returned null for bracket ${toolType}`);
            }
          } catch (err) {
            this._remoteLog("ERROR", `Failed to create bracket drawing ${toolType}: ${err.message}\nStack: ${err.stack}`);
          }

          processedOrderIds.add(parent.orderId);
          processedOrderIds.add(tpChild.orderId);
          processedOrderIds.add(slChild.orderId);
        }
      } else {
        this._remoteLog("INFO", `Parent ${parent.orderId} does not have both TP (LMT) and SL (STP). Found TP: ${!!tpChild}, SL: ${!!slChild}`);
      }
    });

    // ── 2. OCA orders ──
    const ocaGroupToOrders = {};
    activeOrders.forEach(o => {
      if (o.ocaGroup && !processedOrderIds.has(o.orderId)) {
        if (!ocaGroupToOrders[o.ocaGroup]) {
          ocaGroupToOrders[o.ocaGroup] = [];
        }
        ocaGroupToOrders[o.ocaGroup].push(o);
      }
    });

    for (const ocaGroupId in ocaGroupToOrders) {
      const ocaOrders = ocaGroupToOrders[ocaGroupId];
      if (ocaOrders.length >= 2) {
        const prices = ocaOrders.map(o => o.price).filter(p => p != null);
        if (prices.length === 0) continue;
        const upperPrice = Math.max(...prices);
        const lowerPrice = Math.min(...prices);

        const times = ocaOrders.map(o => o.placedTime).filter(t => t != null);
        const earliestPlacedTime = times.length > 0 ? times.reduce((a, b) => a < b ? a : b) : null;
        const leftBarTime = getBarTimeForOrder(earliestPlacedTime);

        if (leftBarTime !== null) {
          const allTimes = Array.from(cm.data.keys()).sort((a, b) => a - b);
          const leftIndex = allTimes.indexOf(leftBarTime);
          let rightBarTime;
          if (leftIndex !== -1 && leftIndex + 20 < allTimes.length) {
            rightBarTime = allTimes[leftIndex + 20];
          } else {
            rightBarTime = leftBarTime + 20 * this.INTERVAL_SECONDS;
          }

          const id = `order-oca-${ocaGroupId}`;
          const anchors = [
            { time: leftBarTime, price: upperPrice },
            { time: rightBarTime, price: lowerPrice }
          ];
          const style = {
            lineColor: '#ff9800',
            lineWidth: 1.5,
            fillColor: 'rgba(255, 152, 0, 0.1)'
          };
          const opts = {
            filled: true,
            showDimensions: false
          };

          this._remoteLog("INFO", `Attempting to create OCA drawing rectangle for group ${ocaGroupId} with anchors: ${JSON.stringify(anchors)}`);
          try {
            const drawing = cm.toolRegistry.createDrawing('rectangle', id, anchors, style, opts);
            if (drawing) {
              cm.drawingManager.addDrawing(drawing);
              this._orderDrawingIds.push(id);
              this._remoteLog("INFO", `Successfully added OCA drawing ${id}`);
            } else {
              this._remoteLog("WARN", `createDrawing returned null for OCA rectangle`);
            }
          } catch (err) {
            this._remoteLog("ERROR", `Failed to create OCA drawing rectangle: ${err.message}\nStack: ${err.stack}`);
          }

          ocaOrders.forEach(o => processedOrderIds.add(o.orderId));
        }
      }
    }

    // ── 3. Individual limit orders ──
    activeOrders.forEach(o => {
      if (processedOrderIds.has(o.orderId)) return;
      if (o.orderType === "LMT") {
        const orderBarTime = getBarTimeForOrder(o.placedTime);
        if (orderBarTime !== null) {
          const isBuy = o.action === "BUY";
          const id = `order-limit-${o.orderId}`;
          const anchors = [{ time: orderBarTime, price: o.price }];
          const style = {
            lineColor: isBuy ? '#2196F3' : '#9E9E9E',
            lineWidth: 1.5,
            lineDash: [6, 4]
          };
          const opts = {
            showPrice: true
          };

          this._remoteLog("INFO", `Attempting to create limit drawing horizontal-ray for order ${o.orderId} with anchors: ${JSON.stringify(anchors)}`);
          try {
            const drawing = cm.toolRegistry.createDrawing('horizontal-ray', id, anchors, style, opts);
            if (drawing) {
              cm.drawingManager.addDrawing(drawing);
              this._orderDrawingIds.push(id);
              this._remoteLog("INFO", `Successfully added limit drawing ${id}`);
            } else {
              this._remoteLog("WARN", `createDrawing returned null for limit horizontal-ray`);
            }
          } catch (err) {
            this._remoteLog("ERROR", `Failed to create limit drawing horizontal-ray: ${err.message}\nStack: ${err.stack}`);
          }
          processedOrderIds.add(o.orderId);
        }
      }
    });
  }

  async _editOrderPrice(orderId, btn) {
    const row = btn.closest(".order-row");
    const input = row.querySelector(`.order-price-input[data-oid="${orderId}"]`);
    if (!input) return;
    const price = parseFloat(input.value);
    if (!price || isNaN(price)) return;
    const res = await this._callApi("/ibkr/edit_order", { orderId, price });
    if (res) {
      console.log("[ibkr] Edited:", res);
      this._refreshOrders();
    }
  }

  async _toMarket(orderId) {
    const res = await this._callApi("/ibkr/edit_order_follow_market", { orderId });
    if (res) {
      console.log("[ibkr] Converted to market:", res);
      this._refreshOrders();
    }
  }

  async _cancelOrder(orderId) {
    const res = await this._callApi("/ibkr/cancel_order", { orderId });
    if (res) {
      console.log("[ibkr] Cancelled:", res);
      this._refreshOrders();
    }
  }
}

const _futureApp = new FutureChartApp();
_futureApp.run().then(() => {
  _futureApp._initTradingControls();
  // Mount drawing toolbar onto the future chart container
  if (typeof DrawingToolbar !== 'undefined') {
    const dt = new DrawingToolbar(_futureApp.chartFuture);
    dt.mount(document.getElementById('chart-future'));
  }
});
