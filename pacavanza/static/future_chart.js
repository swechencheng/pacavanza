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
    this._currentPosition = null;

    // Global error trackers for remote backend logging
    window.addEventListener("error", (e) => {
      this._remoteLog("ERROR", `Unhandled: ${e.message} at ${e.filename}:${e.lineno}`);
    });
    window.addEventListener("unhandledrejection", (e) => {
      this._remoteLog("ERROR", `Unhandled rejection: ${e.reason}`);
    });
    this._pendingDrawingUpdate = null;
    this._lastOrders = [];
    this._isSnapping = false;
    this.tickSize = 0.25;

    if (this.chartFuture.drawingManager) {
      this.chartFuture.drawingManager.on("drawing:updated", (event) => this._handleDrawingUpdated(event));
      this.chartFuture.drawingManager.on("drawing:selected", (event) => {
        this._remoteLog("DEBUG", `Drawing selected: ${event.drawingId}`);
      });
      this.chartFuture.drawingManager.on("drawing:deselected", (event) => {
        this._remoteLog("DEBUG", `Drawing deselected: ${event.drawingId}`);
        this._pendingDrawingUpdate = null;
      });
      window.addEventListener("mouseup", (e) => this._handleMouseUp(e));
      const container = document.getElementById("chart-future");
      if (container) {
        container.addEventListener("mouseup", (e) => this._handleMouseUp(e));
      }
      window.addEventListener("keydown", (e) => {
        if (e.key === "Escape") {
          const selected = this.chartFuture.drawingManager.getSelectedDrawing();
          if (selected && selected.id && selected.id.startsWith("order-")) {
            this._remoteLog("DEBUG", `Escape pressed. Deselecting drawing ${selected.id}`);
            this.chartFuture.drawingManager.deselectAll();
            this._refreshOrders();
          }
        }
      });
    }

    this._remoteLog("INFO", "FutureChartApp constructor initialized");
  }

  _remoteLog(level, message) {
    console.log(`[${level}] ${message}`);
    const levelUpper = (level || "").toUpperCase();
    if (levelUpper === "DEBUG") {
      return;
    }
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

    this.tickSize = parseFloat(info.tick_size) || parseFloat(info.tickSize) || 0.25;
    this._remoteLog("INFO", `Set tickSize to ${this.tickSize} from active_future`);

    const inputsToUpdate = ["oca-stop-price", "oca-limit-price", "limit-order-price"];
    inputsToUpdate.forEach(id => {
      const el = document.getElementById(id);
      if (el) {
        el.setAttribute("step", this.tickSize);
      }
    });

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
      if (msg.position !== undefined) {
        this._currentPosition = msg.position;
        this._updatePositionOverlay();
      }
      this._renderOrders(msg.orders);
      return;
    }

    // Handle order depth (tape) updates
    if (msg.type === "depth") {
      this._renderDepth(msg);
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

  // ── Order Depth (Tape) Rendering ───────────────────────────────────
  _renderDepth(msg) {
    const content = document.getElementById("tape-content");
    const updatedEl = document.getElementById("tape-updated");
    if (!content) return;

    const levels = msg.levels;
    if (!levels || levels.length === 0) {
      content.innerHTML = '<div class="tape-empty">No depth data</div>';
      return;
    }

    let maxVol = 0;
    for (const level of levels) {
      if (level.buyVolume && level.buyVolume > maxVol) maxVol = level.buyVolume;
      if (level.sellVolume && level.sellVolume > maxVol) maxVol = level.sellVolume;
    }
    maxVol = Math.max(maxVol, 1);

    let html = '<table class="tape-table"><thead><tr>';
    html += '<th>Vol</th><th>Bid</th><th>Ask</th><th>Vol</th>';
    html += '</tr></thead>';

    for (const level of levels) {
      const bp = level.buyPrice != null ? level.buyPrice.toFixed(2) : '—';
      const bv = level.buyVolume != null ? level.buyVolume : '—';
      const sp = level.sellPrice != null ? level.sellPrice.toFixed(2) : '—';
      const sv = level.sellVolume != null ? level.sellVolume : '—';

      const buyPct = level.buyVolume ? (level.buyVolume / maxVol) * 100 : 0;
      const sellPct = level.sellVolume ? (level.sellVolume / maxVol) * 100 : 0;

      html += `<tbody>`;
      html += `<tr>`;
      html += `<td class="tape-bid-vol">${bv}</td>`;
      html += `<td class="tape-bid-price">${bp}</td>`;
      html += `<td class="tape-ask-price">${sp}</td>`;
      html += `<td class="tape-ask-vol">${sv}</td>`;
      html += `</tr>`;
      html += `<tr class="tape-bar-row">`;
      html += `<td colspan="2"><div class="tape-bar-container buy"><div class="tape-bar" style="width: ${buyPct}%"></div></div></td>`;
      html += `<td colspan="2"><div class="tape-bar-container sell"><div class="tape-bar" style="width: ${sellPct}%"></div></div></td>`;
      html += `</tr>`;
      html += `</tbody>`;
    }

    html += '</table>';
    content.innerHTML = html;

    // Update timestamp
    if (updatedEl && msg.updated) {
      try {
        const d = new Date(msg.updated);
        updatedEl.textContent = d.toLocaleTimeString('sv-SE', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
      } catch (e) {
        updatedEl.textContent = msg.updated;
      }
    }
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
    // Pause if user is currently editing an input field
    const activeEl = document.activeElement;
    if (activeEl && (activeEl.classList.contains("order-qty-input") || activeEl.classList.contains("order-price-input"))) {
      return; // Skip polling so we don't overwrite their input before they click save
    }

    const res = await this._callApi("/ibkr/open_orders", null, "GET");
    if (res) {
      if (res.position !== undefined) {
        this._currentPosition = res.position;
        this._updatePositionOverlay();
      }
      if (res.orders) {
        this._renderOrders(res.orders);
      }
    }
  }

  _renderOrders(orders) {
    this._lastOrders = orders;
    this._updateOrderDrawings(orders);

    const container = document.getElementById("order-list");
    if (!container) return;

    // Pause HTML re-rendering if user is currently editing an input field
    const activeEl = document.activeElement;
    if (activeEl && (activeEl.classList.contains("order-qty-input") || activeEl.classList.contains("order-price-input"))) {
      return; // Skip overwriting HTML so we don't blur their input before they click save
    }

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
        ${isFulfilled || isDone ? `<span style="color:#888; ${purpleStyle}">×${o.totalQuantity}</span>` :
          `×<input type="number" class="order-qty-input" value="${o.totalQuantity}" step="1" min="1" data-oid="${o.orderId}" />`}
        ${isMkt ? '<span style="color:#ff9900;">MKT</span>' :
          `<input type="number" class="order-price-input" value="${priceVal}" step="${this.tickSize || 0.25}" data-oid="${o.orderId}" ${isFulfilled ? "disabled" : ""} ${purpleStyle} />`}
        <span style="color:#555; ${isFulfilled ? "color: #b388ff !important;" : ""}">${o.status}</span>
        ${parentInfo}${ocaInfo}
        <span style="flex:1;"></span>
        ${(!isFulfilled && !isDone) ? `<button class="order-btn" onclick="_futureApp._editOrder(${o.orderId}, this)">✏️</button>` : ""}
        ${(!isMkt && !isFulfilled && !isDone) ? `<button class="order-btn order-btn-market" onclick="_futureApp._toMarket(${o.orderId})">→MKT</button>` : ""}
        ${!isDone ? `<button class="order-btn order-btn-danger" onclick="_futureApp._cancelOrder(${o.orderId})">❌</button>` : ""}
      </div>`;
    }).join("");
  }

  _updateOrderDrawings(orders) {
    const cm = this.chartFuture;
    if (!cm || !cm.drawingManager || !cm.toolRegistry) return;

    // Check if any order drawing is currently selected or being modified
    const selected = cm.drawingManager.getSelectedDrawing();
    if (selected && selected.id && selected.id.startsWith("order-")) {
      this._remoteLog("DEBUG", `Skipping _updateOrderDrawings because drawing ${selected.id} is currently selected/being edited.`);
      return;
    }

    if (this._pendingDrawingUpdate) {
      this._remoteLog("DEBUG", "Skipping _updateOrderDrawings because there is a pending drawing update.");
      return;
    }

    if (orders && orders.length > 0) {
      this._remoteLog("DEBUG", `_updateOrderDrawings starting. Total orders: ${orders.length}. Chart data size: ${cm.data ? cm.data.size : 0}`);
    }

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
      this._remoteLog("DEBUG", `Skipping drawing sync: orders empty or cm.data empty.`);
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

    // Filter active orders for standalone drawings and OCA drawings
    const activeOrders = orders.filter(o => !o.isDone && !o.fulfilled);
    this._remoteLog("DEBUG", `Active orders for drawings: ${JSON.stringify(activeOrders)}`);

    // Track which order IDs are processed as part of a drawing group to avoid double rendering
    const processedOrderIds = new Set();

    // ── 1. Stop order with children (bracket) ──
    // Map parentId to children using the full orders array to catch active children of fulfilled parents
    const parentIdToChildren = {};
    orders.forEach(o => {
      if (o.parentId) {
        if (!parentIdToChildren[o.parentId]) {
          parentIdToChildren[o.parentId] = [];
        }
        parentIdToChildren[o.parentId].push(o);
      }
    });

    // Find brackets: iterate over all parents in the full orders array
    orders.forEach(parent => {
      if (parent.parentId) return; // Must be a parent order
      const children = parentIdToChildren[parent.orderId] || [];
      if (children.length === 0) return;

      this._remoteLog("DEBUG", `Parent ${parent.orderId} has children: ${JSON.stringify(children)}`);

      // Find children: TP (LMT) and SL (STP / STP LMT)
      const tpChild = children.find(c => c.orderType === "LMT");
      const slChild = children.find(c => c.orderType === "STP" || c.orderType === "STP LMT");

      if (tpChild && slChild) {
        // The bracket persists only if BOTH TP and SL children are still active (not done)
        if (!tpChild.isDone && !slChild.isDone) {
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

            this._remoteLog("DEBUG", `Attempting to create bracket drawing ${toolType} for parent ${parent.orderId} with anchors: ${JSON.stringify(anchors)}`);
            try {
              const drawing = cm.toolRegistry.createDrawing(toolType, id, anchors, style, opts);
              if (drawing) {
                PACChartApp.patchPositionDrawing(drawing, toolType);
                cm.drawingManager.addDrawing(drawing);
                this._orderDrawingIds.push(id);
                this._remoteLog("DEBUG", `Successfully added bracket drawing ${id}`);
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
        }
      } else {
        this._remoteLog("DEBUG", `Parent ${parent.orderId} does not have both TP (LMT) and SL (STP). Found TP: ${!!tpChild}, SL: ${!!slChild}`);
      }
    });

    // ── 2. OCA orders ──
    const ocaGroupToAllOrders = {};
    orders.forEach(o => {
      if (o.ocaGroup) {
        if (!ocaGroupToAllOrders[o.ocaGroup]) {
          ocaGroupToAllOrders[o.ocaGroup] = [];
        }
        ocaGroupToAllOrders[o.ocaGroup].push(o);
      }
    });

    for (const ocaGroupId in ocaGroupToAllOrders) {
      const ocaOrders = ocaGroupToAllOrders[ocaGroupId];

      // Only draw if at least two orders in the OCA group are active (not done)
      // and not already processed by bracket logic
      const activeOcaOrders = ocaOrders.filter(o => !o.isDone && !processedOrderIds.has(o.orderId));
      if (activeOcaOrders.length < 2) continue;

      const prices = ocaOrders.map(o => o.price).filter(p => p != null);

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

        // Determine if long or short position drawing should be used
        let isLong = true;
        let entryPrice = null;

        // Try using the current active position
        if (this._currentPosition && this._currentPosition.position !== 0) {
          isLong = this._currentPosition.position > 0;
          entryPrice = this._currentPosition.avgCost;
        } else {
          // Fallback to OCA orders action (which closes the position)
          // If action is SELL, the position is Long (isLong = true)
          // If action is BUY, the position is Short (isLong = false)
          const firstOca = ocaOrders[0];
          isLong = (firstOca.action === "SELL");

          // Fallback entry price: last bar's close price or midpoint
          const lastTime = allTimes[allTimes.length - 1];
          entryPrice = lastTime ? cm.data.get(lastTime).close : (upperPrice + lowerPrice) / 2;
        }

        // Long position: TP is upperPrice, SL is lowerPrice
        // Short position: TP is lowerPrice, SL is upperPrice
        const slPrice = isLong ? lowerPrice : upperPrice;
        const tpPrice = isLong ? upperPrice : lowerPrice;

        const toolType = isLong ? "long-position" : "short-position";
        const id = `order-oca-${ocaGroupId}`;
        const anchors = [
          { time: leftBarTime, price: entryPrice },
          { time: rightBarTime, price: slPrice },
          { time: rightBarTime, price: tpPrice }
        ];

        const style = {
          lineColor: isLong ? "#26A69A" : "#EF5350",
          lineWidth: 1.5
        };
        const opts = {
          showPrices: true,
          showPercentage: true,
          showRiskReward: true
        };

        this._remoteLog("DEBUG", `Attempting to create OCA position drawing ${toolType} for group ${ocaGroupId} with anchors: ${JSON.stringify(anchors)}`);
        try {
          const drawing = cm.toolRegistry.createDrawing(toolType, id, anchors, style, opts);
          if (drawing) {
            PACChartApp.patchPositionDrawing(drawing, toolType);
            cm.drawingManager.addDrawing(drawing);
            this._orderDrawingIds.push(id);
            this._remoteLog("DEBUG", `Successfully added OCA position drawing ${id}`);
          } else {
            this._remoteLog("WARN", `createDrawing returned null for OCA position ${toolType}`);
          }
        } catch (err) {
          this._remoteLog("ERROR", `Failed to create OCA position drawing ${toolType}: ${err.message}\nStack: ${err.stack}`);
        }

        // Mark all OCA orders in the group as processed
        ocaOrders.forEach(o => processedOrderIds.add(o.orderId));
      }
    }

    // ── 3. Individual limit and stop orders ──
    activeOrders.forEach(o => {
      if (processedOrderIds.has(o.orderId)) return;
      // Skip drawing if the order is exclusively for closing or scaling up
      if (o.orderRef === "CloseOnly" || o.orderRef === "ScaleUp") return;

      if (o.orderType === "LMT" || o.orderType === "STP" || o.orderType === "STP LMT") {
        const orderBarTime = getBarTimeForOrder(o.placedTime);
        if (orderBarTime !== null) {
          const isBuy = o.action === "BUY";
          const id = `order-standalone-${o.orderId}`;
          const anchors = [{ time: orderBarTime, price: o.price }];
          const style = {
            lineColor: isBuy ? '#2196F3' : '#9E9E9E',
            lineWidth: 1.5,
            lineDash: [6, 4]
          };
          const opts = {
            showPrice: true
          };

          this._remoteLog("DEBUG", `Attempting to create limit drawing horizontal-ray for order ${o.orderId} with anchors: ${JSON.stringify(anchors)}`);
          try {
            const drawing = cm.toolRegistry.createDrawing('horizontal-ray', id, anchors, style, opts);
            if (drawing) {
              cm.drawingManager.addDrawing(drawing);
              this._orderDrawingIds.push(id);
              this._remoteLog("DEBUG", `Successfully added limit drawing ${id}`);
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

  // ── Position Info Overlay ─────────────────────────────────────────
  _updatePositionOverlay() {
    const container = document.getElementById('chart-future');
    if (!container) return;

    let overlay = container.querySelector('.pac-position-overlay');
    if (!overlay) {
      overlay = document.createElement('div');
      overlay.className = 'pac-position-overlay';
      container.appendChild(overlay);
    }

    const pos = this._currentPosition;
    if (!pos || pos.position === 0) {
      overlay.innerHTML = `<span class="pos-label pos-flat">FLAT</span>`;
      return;
    }

    const isLong = pos.position > 0;
    const sideClass = isLong ? 'pos-long' : 'pos-short';
    const sideLabel = isLong ? 'LONG' : 'SHORT';
    const size = Math.abs(pos.position);
    const avgPrice = pos.avgCost != null ? pos.avgCost.toFixed(2) : '—';

    overlay.innerHTML = [
      `<span class="pos-label ${sideClass}">${sideLabel}</span>`,
      `<span class="pos-size ${sideClass}">×${size}</span>`,
      `<span class="pos-sep">│</span>`,
      `<span class="pos-avg">Avg ${avgPrice}</span>`,
    ].join('');
  }

  async _editOrder(orderId, btn) {
    const row = btn.closest(".order-row");

    // Read price if input exists
    const priceInput = row.querySelector(`.order-price-input[data-oid="${orderId}"]`);
    const price = priceInput ? parseFloat(priceInput.value) : null;
    if (priceInput) priceInput.blur();

    // Read quantity if input exists
    const qtyInput = row.querySelector(`.order-qty-input[data-oid="${orderId}"]`);
    const qty = qtyInput ? parseInt(qtyInput.value, 10) : null;
    if (qtyInput) qtyInput.blur();

    const payload = { orderId };
    if (price !== null && !isNaN(price)) {
      payload.price = price;
    }
    if (qty !== null && !isNaN(qty)) {
      payload.quantity = qty;
    }

    if (payload.price === undefined && payload.quantity === undefined) return;

    const res = await this._callApi("/ibkr/edit_order", payload);
    if (res) {
      console.log("[ibkr] Edited order:", res);
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

  _handleDrawingUpdated(event) {
    const drawing = event.drawing;
    if (!drawing || !drawing.anchors) return;

    const id = event.drawingId;
    const isOrderDrawing = id.startsWith("order-bracket-") || id.startsWith("order-oca-") || id.startsWith("order-standalone-");
    if (!isOrderDrawing) return;

    this._remoteLog("DEBUG", `_handleDrawingUpdated called for ${id}. anchors: ${JSON.stringify(drawing.anchors.map(a => a.price))}`);

    if (this._isSnapping) return;

    // Tick size snapping
    const tickSize = this.tickSize || 0.25;
    let changed = false;
    drawing.anchors.forEach((anchor, index) => {
      const roundedPrice = Math.round(anchor.price / tickSize) * tickSize;
      if (Math.abs(anchor.price - roundedPrice) > 1e-9) {
        this._isSnapping = true;
        try {
          drawing.updateAnchor(index, { time: anchor.time, price: roundedPrice });
        } finally {
          this._isSnapping = false;
        }
        changed = true;
      }
    });

    // Track pending update for mouseup release
    this._pendingDrawingUpdate = {
      drawingId: id,
      anchors: drawing.anchors.map(a => ({ time: a.time, price: a.price }))
    };
  }

  _handleMouseUp(e) {
    this._remoteLog("DEBUG", `_handleMouseUp event triggered on target: ${e.target ? e.target.tagName : 'unknown'} id: ${e.target ? e.target.id : 'none'}. Pending update exists: ${!!this._pendingDrawingUpdate}`);
    if (this._pendingDrawingUpdate) {
      const { drawingId, anchors } = this._pendingDrawingUpdate;
      this._pendingDrawingUpdate = null; // Clear immediately to prevent duplicate requests

      this._remoteLog("DEBUG", `Mouse released. Processing pending update for drawing ${drawingId} with anchors: ${JSON.stringify(anchors)}`);
      this._submitDrawingUpdates(drawingId, anchors);
    }
  }

  async _submitDrawingUpdates(drawingId, anchors) {
    const orders = this._lastOrders || [];
    const updates = []; // Array of { orderId, price } to submit

    if (drawingId.startsWith("order-bracket-")) {
      const parentOrderId = parseInt(drawingId.substring("order-bracket-".length), 10);
      const parent = orders.find(o => o.orderId === parentOrderId);
      if (!parent) return;

      // Map parentId to children
      const parentIdToChildren = {};
      orders.forEach(o => {
        if (o.parentId) {
          if (!parentIdToChildren[o.parentId]) parentIdToChildren[o.parentId] = [];
          parentIdToChildren[o.parentId].push(o);
        }
      });
      const children = parentIdToChildren[parentOrderId] || [];
      const tpChild = children.find(c => c.orderType === "LMT");
      const slChild = children.find(c => c.orderType === "STP" || c.orderType === "STP LMT");

      // Check which anchors have changed compared to original order prices
      // anchors[0] -> Parent Entry price
      if (anchors[0] && !parent.isDone && !parent.fulfilled && Math.abs(parent.price - anchors[0].price) > 1e-9) {
        updates.push({ orderId: parent.orderId, price: anchors[0].price });
      }
      // anchors[1] -> SL price
      if (anchors[1] && slChild && !slChild.isDone && Math.abs(slChild.price - anchors[1].price) > 1e-9) {
        updates.push({ orderId: slChild.orderId, price: anchors[1].price });
      }
      // anchors[2] -> TP price
      if (anchors[2] && tpChild && !tpChild.isDone && Math.abs(tpChild.price - anchors[2].price) > 1e-9) {
        updates.push({ orderId: tpChild.orderId, price: anchors[2].price });
      }
    } else if (drawingId.startsWith("order-oca-")) {
      const ocaGroupId = drawingId.substring("order-oca-".length);
      const ocaOrders = orders.filter(o => o.ocaGroup === ocaGroupId);
      if (ocaOrders.length === 0) return;

      const tpChild = ocaOrders.find(o => o.orderType === "LMT");
      const slChild = ocaOrders.find(o => o.orderType === "STP" || o.orderType === "STP LMT");

      // anchors[1] -> SL price (Stop)
      if (anchors[1] && slChild && !slChild.isDone && Math.abs(slChild.price - anchors[1].price) > 1e-9) {
        updates.push({ orderId: slChild.orderId, price: anchors[1].price });
      }
      // anchors[2] -> TP price (Limit)
      if (anchors[2] && tpChild && !tpChild.isDone && Math.abs(tpChild.price - anchors[2].price) > 1e-9) {
        updates.push({ orderId: tpChild.orderId, price: anchors[2].price });
      }
    } else if (drawingId.startsWith("order-standalone-")) {
      const orderId = parseInt(drawingId.substring("order-standalone-".length), 10);
      const order = orders.find(o => o.orderId === orderId);
      if (!order) return;

      // anchors[0] -> Limit Price
      if (anchors[0] && !order.isDone && Math.abs(order.price - anchors[0].price) > 1e-9) {
        updates.push({ orderId: order.orderId, price: anchors[0].price });
      }
    }

    if (updates.length === 0) {
      this._remoteLog("DEBUG", "No order prices changed, skipping update.");
      if (this.chartFuture && this.chartFuture.drawingManager) {
        this.chartFuture.drawingManager.deselectAll();
      }
      this._refreshOrders();
      return;
    }

    this._remoteLog("DEBUG", `Submitting order updates from drawing: ${JSON.stringify(updates)}`);
    for (const update of updates) {
      try {
        const res = await this._callApi("/ibkr/edit_order", update);
        if (res) {
          this._remoteLog("DEBUG", `Successfully updated order ${update.orderId} to price ${update.price}`);
        }
      } catch (err) {
        this._remoteLog("ERROR", `Failed to update order ${update.orderId}: ${err.message}`);
      }
    }

    if (this.chartFuture && this.chartFuture.drawingManager) {
      this.chartFuture.drawingManager.deselectAll();
    }
    this._refreshOrders();
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
