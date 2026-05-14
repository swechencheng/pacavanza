/**
 * chart.js — Ava mini-futures chart (long + short dual panels).
 * Extends PACChartApp from chart_core.js.
 */
class AvaChartApp extends PACChartApp {
  constructor() {
    super();
    window.PAC_TRADING_STATE = { longInstrument: null, shortInstrument: null };
    this.chartLong  = this.createChartManager("chart-long",  { toolTipId: "chart-ohlc-info-long" });
    this.chartShort = this.createChartManager("chart-short", { toolTipId: "chart-ohlc-info-short" });
    this.instrumentMapFlat = null;
    this.hierarchicalMap   = null;
  }

  getStatusChartManager() { return this.chartLong; }

  // ── Initialization ────────────────────────────────────────────────
  async initialize() {
    await this._loadInstrumentJSON();
    const sel    = document.getElementById("asset");
    const assets = Object.keys(this.hierarchicalMap);
    sel.innerHTML = "";
    assets.forEach((a) => {
      const opt = document.createElement("option");
      opt.value = opt.textContent = a;
      sel.appendChild(opt);
    });
    if (assets.length > 0) {
      sel.value = assets[0];
      await this._onAssetChanged(assets[0]);
    }
    sel.addEventListener("change", () => this._onAssetChanged(sel.value));
  }

  async _loadInstrumentJSON() {
    if (this.instrumentMapFlat && this.hierarchicalMap) return;
    const res = await fetch("/ava_mini_future_list.json");
    const data = await res.json();
    this.hierarchicalMap   = data;
    this.instrumentMapFlat = this.flattenInstruments(data);
  }

  async _onAssetChanged(assetKey) {
    const asset = this.hierarchicalMap[assetKey];
    if (!asset) return;

    let longKey = null, shortKey = null;
    for (const k in asset) {
      const v = asset[k];
      if (typeof v !== "object" || !v.orderbookId) continue;
      const lk = k.toLowerCase();
      if (lk.includes("mini-l") || lk.includes("bull") || (v.name && v.name.includes("MINI L"))) longKey  = k;
      else if (lk.includes("mini-s") || lk.includes("bear") || (v.name && v.name.includes("MINI S"))) shortKey = k;
    }

    window.PAC_TRADING_STATE.longInstrument  = longKey;
    window.PAC_TRADING_STATE.shortInstrument = shortKey;

    document.getElementById("label-long").textContent  = longKey  ? this.instrumentMapFlat[longKey].name  : "N/A";
    document.getElementById("label-short").textContent = shortKey ? this.instrumentMapFlat[shortKey].name : "N/A";

    const p = [];
    if (longKey)  p.push(this.loadHistoryForInstrument(this.chartLong,  longKey,  this.instrumentMapFlat));
    if (shortKey) p.push(this.loadHistoryForInstrument(this.chartShort, shortKey, this.instrumentMapFlat));
    await Promise.all(p);
    this.ensureMarketCountdown(this.chartLong.groupingState);
  }

  // ── WebSocket message handler ─────────────────────────────────────
  handleWsMessage(msg) {
    let cm = null;
    if (msg.instrument === window.PAC_TRADING_STATE.longInstrument)  cm = this.chartLong;
    else if (msg.instrument === window.PAC_TRADING_STATE.shortInstrument) cm = this.chartShort;
    if (!cm || !msg.bar) return;

    const t = this.isoToLWTime(msg.bar.start_time);
    const candleData = { time: t, open: +msg.bar.open, high: +msg.bar.high, low: +msg.bar.low, close: +msg.bar.close };

    if (msg.type === "update") {
      this._applyBarUpdate(cm, candleData);
      if (msg.instrument === window.PAC_TRADING_STATE.longInstrument) {
        this.startCountdownForBar(t + this.INTERVAL_SECONDS, cm.groupingState);
      }
    } else if (msg.type === "completed") {
      this._applyBarCompleted(cm, candleData);
      if (msg.instrument === window.PAC_TRADING_STATE.longInstrument) {
        this.clearCountdown(true);
        this.ensureMarketCountdown(cm.groupingState);
      }
    }
    this._applyEMAFromMsg(cm, msg);
  }
}

new AvaChartApp().run();
