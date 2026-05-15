/**
 * future_chart.js — OMXS30 continuous future chart (single panel).
 * Extends PACChartApp from chart_core.js.
 */
class FutureChartApp extends PACChartApp {
  constructor() {
    super();
    window.PAC_TRADING_STATE = { futureInstrument: null };
    this.chartFuture = this.createChartManager("chart-future", { toolTipId: "chart-ohlc-info-future" });
    this.instrumentMapFlat = null;
  }

  getStatusChartManager() { return this.chartFuture; }

  // ── Initialization ────────────────────────────────────────────────
  async initialize() {
    // Backend is the single source of truth for the active OMXS30 future.
    // /active_future returns: { key, name, orderbookId, timezone, market_open, market_close }
    const res = await fetch("/active_future");
    if (!res.ok) throw new Error("active_future fetch failed: " + res.status);
    const info = await res.json();

    const futureKey = info.key;
    if (!futureKey) {
      this._error("No active future returned from backend!");
      document.getElementById("status").textContent = "No future found";
      return;
    }

    // Build a minimal flat map so chart_core's setSessionConfig / loadHistoryForInstrument work
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
    document.getElementById("label-future").textContent = info.name || futureKey;

    await this.loadHistoryForInstrument(this.chartFuture, futureKey, this.instrumentMapFlat);
    this.ensureMarketCountdown(this.chartFuture.groupingState);
  }

  // ── WebSocket message handler ─────────────────────────────────────
  handleWsMessage(msg) {
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

  // ── Trading Controls (stubs — real logic will be in future trading.js) ──
  _initTradingControls() {
    const instrument = () => window.PAC_TRADING_STATE.futureInstrument;
    const pct = () => parseFloat(document.getElementById("order-percentage-future").value) || 15;

    const stub = (label) => () => this._log(`[stub] ${label} — instrument: ${instrument()}, pct: ${pct()}%`);

    const bindings = {
      "btn-buy-stop-future": stub("Buy Stop"),
      "btn-cancel-buy-stop-future": stub("Cancel Buy Stop"),
      "btn-sell-stop-future": stub("Sell Stop"),
      "btn-cancel-sell-stop-future": stub("Cancel Sell Stop"),
      "btn-late-buy-stop-future": stub("Late Buy Stop"),
      "btn-late-sell-stop-future": stub("Late Sell Stop"),
      "btn-delete-stop-losses-future": stub("Delete Stop Losses"),
      "btn-market-buy-future": stub("Market Buy"),
      "btn-market-sell-future": stub("Market Sell"),
    };

    for (const [id, handler] of Object.entries(bindings)) {
      const el = document.getElementById(id);
      if (el) el.addEventListener("click", handler);
    }
  }
}

const _futureApp = new FutureChartApp();
_futureApp.run().then(() => _futureApp._initTradingControls());
