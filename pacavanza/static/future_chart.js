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
