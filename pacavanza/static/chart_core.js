/**
 * chart_core.js — Shared base class for PACavanza chart applications.
 * Subclasses override: getStatusChartManager(), initialize(), handleWsMessage()
 */
class PACChartApp {
  constructor() {
    this.INTERVAL_SECONDS = 5 * 60;
    this._countdownTimerId = null;
    this._countdownEndTime = null;
  }

  // ── Logging ───────────────────────────────────────────────────────
  _log(...a) { console.log("[pac-chart]", ...a); }
  _warn(...a) { console.warn("[pac-chart]", ...a); }
  _error(...a) { console.error("[pac-chart]", ...a); }

  // ── Chart Manager Factory ─────────────────────────────────────────
  createChartManager(containerId, options = {}) {
    const container = document.getElementById(containerId);
    if (!container) return null;

    const chart = LightweightCharts.createChart(container, {
      width: container.clientWidth,
      height: container.clientHeight,
      layout: { textColor: "#d1d4dc", background: { type: "Solid", color: "#000000ff" } },
      grid: { vertLines: { color: "transparent" }, horzLines: { color: "transparent" } },
      rightPriceScale: { scaleMargins: { top: 0.2, bottom: 0.2 } },
      timeScale: { timeVisible: true, secondsVisible: false, barSpacing: 6, minBarSpacing: 3 },
    });

    const candleSeries = chart.addSeries(LightweightCharts.CandlestickSeries, {
      upColor: "#4caf50", downColor: "#f44336",
      borderDownColor: "#f44336", borderUpColor: "#4caf50",
      wickDownColor: "#f44336", wickUpColor: "#4caf50",
    });
    const ema20Series = chart.addSeries(LightweightCharts.LineSeries, { color: "#6bebffff", lineWidth: 1 });
    const state = {
      chart,
      series: { candle: candleSeries, ema20: ema20Series },
      drawingManager: null,
      toolRegistry: null,
      autoRays: { yh: null, yl: null, th: null, tl: null },
      data: new Map(),
      ema20Data: new Map(),
      lastBarTime: null,
      lastEMAValue: null,
      lastEMATime: null,
      highLowState: {
        currentDay: null,
        currentHigh: -Infinity, currentHighTime: null,
        currentLow: Infinity, currentLowTime: null,
        yesterdayHigh: null, yesterdayHighTime: null,
        yesterdayLow: null, yesterdayLowTime: null
      },
      groupingState: { sessionTZ: null, sessionOpenMinutes: null, sessionCloseMinutes: null, count: 0, bar_group_count: 0, currentDay: null },
      barGroupMap: new Map(),
      currentInstrument: null,
      toolTipElement: document.getElementById(options.toolTipId),
    };

    chart.subscribeCrosshairMove((param) => {
      const tt = state.toolTipElement;
      if (!tt) return;
      const empty = [`<span style="color:#ddd">O:-</span>`, `<span style="color:#4caf50">H:-</span>`,
        `<span style="color:#f44336">L:-</span>`, `<span style="color:#ddd">C:-</span>`,
        `<span style="color:#ff9900">Bar -</span>`].join(" ");
      if (!param.point || !param.time || param.point.x < 0 || param.point.y < 0) { tt.innerHTML = empty; return; }
      const cData = param.seriesData.get(candleSeries);
      if (!cData || cData.open === undefined) { tt.innerHTML = empty; return; }
      const { open, high, low, close } = cData;
      const bg = state.barGroupMap.get(cData.time);
      tt.innerHTML = [
        `<span style="color:#ddd">O:${open.toFixed(2)}</span>`,
        `<span style="color:#4caf50">H:${high.toFixed(2)}</span>`,
        `<span style="color:#f44336">L:${low.toFixed(2)}</span>`,
        `<span style="color:#ddd">C:${close.toFixed(2)}</span>`,
        `<span style="color:#ff9900">Bar ${bg !== undefined ? bg : "-"}</span>`,
      ].join(" ");
    });

    new ResizeObserver(() => {
      if (container) chart.applyOptions({ width: container.clientWidth, height: container.clientHeight });
    }).observe(container);

    if (window.LightweightChartsDrawing) {
      state.drawingManager = new window.LightweightChartsDrawing.DrawingManager();
      state.drawingManager.attach(chart, candleSeries, container);
      state.toolRegistry = window.LightweightChartsDrawing.getToolRegistry();
    }

    return state;
  }

  // ── Time Helpers ──────────────────────────────────────────────────
  isoToLWTime(iso) { return Math.floor(Date.parse(iso) / 1000); }

  getLocalParts(unixSeconds, timeZone) {
    const parts = new Intl.DateTimeFormat("en-GB", {
      timeZone, hour12: false,
      year: "numeric", month: "2-digit", day: "2-digit",
      hour: "2-digit", minute: "2-digit", second: "2-digit",
    }).formatToParts(new Date(unixSeconds * 1000));
    const map = {};
    parts.forEach((p) => (map[p.type] = p.value));
    return {
      year: parseInt(map.year, 10), month: parseInt(map.month, 10), day: parseInt(map.day, 10),
      hour: parseInt(map.hour, 10), minute: parseInt(map.minute, 10), second: parseInt(map.second, 10),
      ymd: `${map.year}-${map.month}-${map.day}`,
    };
  }

  hhmmToMinutes(hhmm) {
    if (!hhmm) return null;
    const [h, m] = hhmm.split(":").map((s) => parseInt(s, 10));
    return h * 60 + (isNaN(m) ? 0 : m);
  }

  isTradingHoursForBar(unixSeconds, gState) {
    if (!gState.sessionTZ || gState.sessionOpenMinutes == null || gState.sessionCloseMinutes == null) return false;
    const { hour, minute } = this.getLocalParts(unixSeconds, gState.sessionTZ);
    const mins = hour * 60 + minute;
    return mins >= gState.sessionOpenMinutes && mins < gState.sessionCloseMinutes;
  }

  // ── Countdown ─────────────────────────────────────────────────────
  formatMMSS(sec) {
    if (sec < 0) sec = 0;
    return `${Math.floor(sec / 60)}:${String(sec % 60).padStart(2, "0")}`;
  }

  _updateCountdownDisplay() {
    if (!this._countdownEndTime) return;
    const rem = this._countdownEndTime - Math.floor(Date.now() / 1000);
    const el = document.getElementById("status");
    if (!el) return;
    if (rem <= 0) {
      el.textContent = "0:00";
      this.clearCountdown();
      this.ensureMarketCountdown(this.getStatusChartManager().groupingState);
      return;
    }
    el.textContent = this.formatMMSS(rem);
  }

  startCountdownForBar(endUnixSeconds, sessionConfig) {
    if (!sessionConfig || !sessionConfig.sessionTZ) return;
    const now = Math.floor(Date.now() / 1000);
    if (!this.isTradingHoursForBar(now, sessionConfig)) {
      document.getElementById("status").textContent = "Market closed";
      return;
    }
    let end = Number(endUnixSeconds);
    if (!Number.isFinite(end) || end <= now) end = now + this.INTERVAL_SECONDS;
    if (this._countdownEndTime === end && this._countdownTimerId) return;
    this.clearCountdown(false);
    this._countdownEndTime = end;
    this._updateCountdownDisplay();
    this._countdownTimerId = setInterval(() => this._updateCountdownDisplay(), 1000);
  }

  clearCountdown(setMarketMessage = true) {
    if (this._countdownTimerId) { clearInterval(this._countdownTimerId); this._countdownTimerId = null; }
    this._countdownEndTime = null;
    if (setMarketMessage) {
      const el = document.getElementById("status");
      const gs = this.getStatusChartManager().groupingState;
      if (gs.sessionTZ) {
        el.textContent = this.isTradingHoursForBar(Math.floor(Date.now() / 1000), gs) ? "Market open" : "Market closed";
      } else {
        el.textContent = "Disconnected";
      }
    }
  }

  ensureMarketCountdown(groupingState) {
    if (!groupingState || !groupingState.sessionTZ) return;
    const now = Math.floor(Date.now() / 1000);
    if (!this.isTradingHoursForBar(now, groupingState)) { this.clearCountdown(true); return; }
    const barStart = Math.floor(now / this.INTERVAL_SECONDS) * this.INTERVAL_SECONDS;
    this.startCountdownForBar(barStart + this.INTERVAL_SECONDS, groupingState);
  }

  // ── EMA ───────────────────────────────────────────────────────────
  calculateHistoricalEMA20(bars) {
    if (bars.length < 20) return [];
    const k = 2 / 21;
    let ema = bars.slice(0, 20).reduce((s, b) => s + b.close, 0) / 20;
    const result = [{ time: bars[19].time, value: ema }];
    for (let i = 20; i < bars.length; i++) {
      ema = (bars[i].close - ema) * k + ema;
      result.push({ time: bars[i].time, value: ema });
    }
    return result;
  }

  updateEMA20Incremental(cm, newClose, newTime) {
    if (cm.ema20Data.size === 0) return;
    const k = 2 / 21;
    const keys = Array.from(cm.ema20Data.keys()).sort((a, b) => a - b);
    let prevEMA = null;
    for (let i = keys.length - 1; i >= 0; i--) {
      if (keys[i] < newTime) { prevEMA = cm.ema20Data.get(keys[i]); break; }
    }
    if (prevEMA === null) {
      if (cm.lastEMAValue !== null && cm.lastEMATime !== null && cm.lastEMATime < newTime) prevEMA = cm.lastEMAValue;
      else return;
    }
    const newEMA = (newClose - prevEMA) * k + prevEMA;
    cm.ema20Data.set(newTime, newEMA);
    cm.lastEMAValue = newEMA;
    cm.lastEMATime = newTime;
    cm.series.ema20.update({ time: newTime, value: newEMA });
  }

  // ── High/Low (Drawing Rays) ──────────────────────────────────────
  _setRay(cm, key, time, price, color, lineStyle) {
    if (!cm.drawingManager || !cm.toolRegistry) return;

    if (cm.autoRays[key]) {
      cm.drawingManager.removeDrawing(cm.autoRays[key]);
      cm.autoRays[key] = null;
    }
    if (price === null || price === -Infinity || price === Infinity || time === null) return;

    const id = `auto-ray-${key}`;
    const anchors = [{ time, price }];
    const style = { lineColor: color, lineWidth: 1, lineStyle };
    const opts = {};

    const drawing = cm.toolRegistry.createDrawing('horizontal-ray', id, anchors, style, opts);
    if (drawing) {
      // Patch renderer to suppress the anchor dot and direction arrow.
      // The library renderer calls ctx.fill() twice after the line:
      //   1) filled circle at anchor (arc + fill)
      //   2) filled arrowhead triangle (moveTo/lineTo + closePath + fill)
      // We intercept by wrapping paneViews so our custom renderer skips those.
      const origPaneViews = drawing.paneViews.bind(drawing);
      drawing.paneViews = () => {
        const views = origPaneViews();
        return views.map(v => {
          const origRenderer = v.renderer.bind(v);
          return {
            zOrder: v.zOrder.bind(v),
            renderer: () => {
              const r = origRenderer();
              if (!r || !r.draw) return r;
              const origDraw = r.draw.bind(r);
              return {
                draw: (target) => {
                  target.useBitmapCoordinateSpace((scope) => {
                    const ctx = scope.context;
                    // Suppress all fill() calls (dot + arrow) while keeping stroke (the line)
                    const realFill = ctx.fill.bind(ctx);
                    ctx.fill = () => { }; // no-op: skip dot and arrowhead fills
                    // Call original drawImpl via the scope
                    if (r.drawImpl) {
                      r.drawImpl(scope);
                    }
                    ctx.fill = realFill;
                  });
                }
              };
            }
          };
        });
      };
      cm.drawingManager.addDrawing(drawing);
      cm.autoRays[key] = id;
    }
  }

  _syncHighLowRays(cm) {
    const hl = cm.highLowState;
    // Yesterday high/low — dotted, purple
    this._setRay(cm, 'yh', hl.yesterdayHighTime, hl.yesterdayHigh, '#8929ffff', LightweightCharts.LineStyle.Dotted);
    this._setRay(cm, 'yl', hl.yesterdayLowTime, hl.yesterdayLow, '#8929ffff', LightweightCharts.LineStyle.Dotted);
    // Today high/low — dashed, orange
    this._setRay(cm, 'th', hl.currentHighTime, hl.currentHigh, '#c04d00ff', LightweightCharts.LineStyle.Dashed);
    this._setRay(cm, 'tl', hl.currentLowTime, hl.currentLow, '#c04d00ff', LightweightCharts.LineStyle.Dashed);
  }

  calculateHistoryHighLow(cm, bars) {
    if (!bars || bars.length === 0 || !cm.groupingState.sessionTZ) return;
    let scanDay = null, scanHigh = -Infinity, scanLow = Infinity, prevH = null, prevL = null;
    let scanHighTime = null, scanLowTime = null, prevHTime = null, prevLTime = null;
    for (const bar of bars) {
      const { ymd } = this.getLocalParts(bar.time, cm.groupingState.sessionTZ);
      if (ymd !== scanDay) {
        if (scanDay !== null) {
          prevH = scanHigh; prevL = scanLow;
          prevHTime = scanHighTime; prevLTime = scanLowTime;
        }
        scanDay = ymd;
        scanHigh = -Infinity; scanLow = Infinity;
        scanHighTime = null; scanLowTime = null;
      }
      if (bar.high > scanHigh) { scanHigh = bar.high; scanHighTime = bar.time; }
      if (bar.low < scanLow) { scanLow = bar.low; scanLowTime = bar.time; }
    }
    cm.highLowState = {
      currentDay: scanDay,
      currentHigh: scanHigh, currentHighTime: scanHighTime,
      currentLow: scanLow, currentLowTime: scanLowTime,
      yesterdayHigh: prevH, yesterdayHighTime: prevHTime,
      yesterdayLow: prevL, yesterdayLowTime: prevLTime
    };
    this._syncHighLowRays(cm);
  }

  updateHighLowIncremental(cm, bar) {
    if (!cm.groupingState.sessionTZ) return;
    const { ymd } = this.getLocalParts(bar.time, cm.groupingState.sessionTZ);
    let changed = false;
    if (ymd !== cm.highLowState.currentDay) {
      if (cm.highLowState.currentDay !== null) {
        cm.highLowState.yesterdayHigh = cm.highLowState.currentHigh;
        cm.highLowState.yesterdayHighTime = cm.highLowState.currentHighTime;
        cm.highLowState.yesterdayLow = cm.highLowState.currentLow;
        cm.highLowState.yesterdayLowTime = cm.highLowState.currentLowTime;
      }
      cm.highLowState.currentDay = ymd;
      cm.highLowState.currentHigh = -Infinity;
      cm.highLowState.currentHighTime = null;
      cm.highLowState.currentLow = Infinity;
      cm.highLowState.currentLowTime = null;
      changed = true;
    }
    if (bar.high > cm.highLowState.currentHigh) {
      cm.highLowState.currentHigh = bar.high;
      cm.highLowState.currentHighTime = bar.time;
      changed = true;
    }
    if (bar.low < cm.highLowState.currentLow) {
      cm.highLowState.currentLow = bar.low;
      cm.highLowState.currentLowTime = bar.time;
      changed = true;
    }
    if (changed) this._syncHighLowRays(cm);
  }

  // ── Grouping ──────────────────────────────────────────────────────
  processBarForGrouping(cm, barObj) {
    if (!cm.groupingState.sessionTZ || !barObj || !barObj.time) return;
    const { ymd, hour, minute, second } = this.getLocalParts(barObj.time, cm.groupingState.sessionTZ);
    if (cm.groupingState.currentDay !== ymd) {
      cm.groupingState.currentDay = ymd;
      cm.groupingState.count = 0;
      cm.groupingState.bar_group_count = 0;
    }
    const isOpen = hour * 60 + minute === cm.groupingState.sessionOpenMinutes && second === 0;
    if (isOpen) {
      cm.groupingState.count = cm.groupingState.bar_group_count = 1;
    } else if (this.isTradingHoursForBar(barObj.time, cm.groupingState)) {
      cm.groupingState.count += 1;
      cm.groupingState.bar_group_count += 1;
    }
    cm.barGroupMap.set(barObj.time, cm.groupingState.bar_group_count);
  }

  // ── Data Loading ──────────────────────────────────────────────────
  async fetchHistory(instrument) {
    const res = await fetch(`/history/${encodeURIComponent(instrument)}?limit=3600`);
    if (!res.ok) throw new Error("history fetch failed: " + res.status);
    return res.json();
  }

  clearChartData(cm) {
    cm.data.clear(); cm.lastBarTime = null;
    cm.ema20Data.clear(); cm.lastEMAValue = null; cm.lastEMATime = null;
    cm.series.candle.setData([]); cm.series.ema20.setData([]);
    if (cm.drawingManager) {
      for (const key of ['yh', 'yl', 'th', 'tl']) {
        if (cm.autoRays[key]) {
          cm.drawingManager.removeDrawing(cm.autoRays[key]);
          cm.autoRays[key] = null;
        }
      }
    }
    cm.highLowState = {
      currentDay: null,
      currentHigh: -Infinity, currentHighTime: null,
      currentLow: Infinity, currentLowTime: null,
      yesterdayHigh: null, yesterdayHighTime: null,
      yesterdayLow: null, yesterdayLowTime: null
    };
    cm.barGroupMap.clear();
    cm.groupingState.count = cm.groupingState.bar_group_count = 0;
    cm.groupingState.currentDay = null;
  }

  setSessionConfig(cm, instrumentId, map) {
    const conf = map[instrumentId];
    if (!conf) { cm.groupingState.sessionTZ = null; return; }
    cm.groupingState.sessionTZ = conf.timezone || null;
    cm.groupingState.sessionOpenMinutes = this.hhmmToMinutes(conf.market_open);
    cm.groupingState.sessionCloseMinutes = this.hhmmToMinutes(conf.market_close);
  }

  async loadHistoryForInstrument(cm, instrumentId, instrumentMap) {
    this.clearChartData(cm);
    cm.currentInstrument = instrumentId;
    this.setSessionConfig(cm, instrumentId, instrumentMap);

    const bars = await this.fetchHistory(instrumentId);
    const barData = bars
      .map((b) => ({ time: this.isoToLWTime(b.start_time), open: +b.open, high: +b.high, low: +b.low, close: +b.close }))
      .filter((b) => b.open && !isNaN(b.open));

    cm.data.clear(); cm.ema20Data.clear();
    barData.forEach((b) => cm.data.set(b.time, b));
    const sorted = Array.from(cm.data.values()).sort((a, b) => a.time - b.time);

    if (sorted.length > 0) {
      cm.series.candle.setData(sorted);
      cm.lastBarTime = sorted[sorted.length - 1].time;
      if (sorted.length >= 20) {
        const histEMA = this.calculateHistoricalEMA20(sorted);
        histEMA.forEach((e) => cm.ema20Data.set(e.time, e.value));
        cm.series.ema20.setData(histEMA);
        const last = histEMA[histEMA.length - 1];
        if (last) { cm.lastEMAValue = last.value; cm.lastEMATime = last.time; }
      }
      this.calculateHistoryHighLow(cm, sorted);
      sorted.forEach((b) => this.processBarForGrouping(cm, b));
    }
  }

  flattenInstruments(data) {
    const flat = {};
    for (const assetKey in data) {
      const assetData = data[assetKey];
      const common = {};
      if (assetData.timezone) common.timezone = assetData.timezone;
      if (assetData.market_open) common.market_open = assetData.market_open;
      if (assetData.market_close) common.market_close = assetData.market_close;
      for (const childKey in assetData) {
        const val = assetData[childKey];
        if (typeof val === "object" && val !== null && val.orderbookId) {
          flat[childKey] = Object.assign({}, common, val);
        }
      }
    }
    return flat;
  }

  // ── Helpers for WS message handling ──────────────────────────────
  _applyBarUpdate(cm, candleData) {
    const t = candleData.time;
    if (cm.lastBarTime === null || t >= cm.lastBarTime) {
      cm.data.set(t, candleData);
      cm.series.candle.update(candleData);
      this.updateEMA20Incremental(cm, candleData.close, t);
      this.updateHighLowIncremental(cm, candleData);
      if (t > cm.lastBarTime) {
        this.processBarForGrouping(cm, candleData);
        cm.lastBarTime = t;
      }
    }
  }

  _applyBarCompleted(cm, candleData) {
    cm.data.set(candleData.time, candleData);
    cm.series.candle.update(candleData);
    this.updateEMA20Incremental(cm, candleData.close, candleData.time);
    this.updateHighLowIncremental(cm, candleData);
    if (cm.lastBarTime === null || candleData.time > cm.lastBarTime) cm.lastBarTime = candleData.time;
  }

  _applyEMAFromMsg(cm, msg) {
    if (msg.emas && msg.emas["20"]) {
      const e = msg.emas["20"];
      if (e && e.value && cm.ema20Data.size === 0) {
        const et = this.isoToLWTime(e.time);
        const ev = parseFloat(e.value);
        cm.ema20Data.set(et, ev);
        cm.lastEMAValue = ev; cm.lastEMATime = et;
        cm.series.ema20.update({ time: et, value: ev });
      }
    }
  }

  // ── WebSocket ─────────────────────────────────────────────────────
  setupWS() {
    const ws = new WebSocket(`ws://${window.location.host}/ws`);
    ws.onopen = () => { this._log("WS connected"); this.ensureMarketCountdown(this.getStatusChartManager().groupingState); };
    ws.onclose = () => setTimeout(() => this.setupWS(), 1000);
    ws.onmessage = (evt) => {
      try { this.handleWsMessage(JSON.parse(evt.data)); }
      catch (e) { this._error("WS Msg error", e); }
    };
  }

  // ── Abstract (must override) ──────────────────────────────────────
  getStatusChartManager() { throw new Error("getStatusChartManager() not implemented"); }
  async initialize() { throw new Error("initialize() not implemented"); }
  handleWsMessage(msg) { throw new Error("handleWsMessage() not implemented"); }

  // ── Entry Point ───────────────────────────────────────────────────
  async run() {
    try {
      if (typeof LightweightCharts === "undefined") {
        this._error("LightweightCharts global not found.");
        document.getElementById("status").textContent = "Chart lib N/A";
        return;
      }
      await this.initialize();
      this.setupWS();
    } catch (e) {
      this._error("Chart init failed:", e);
      document.getElementById("status").textContent = "Chart init failed";
    }
  }
}
