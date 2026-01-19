(() => {
  const log = (...a) => console.log("[chart]", ...a);
  const warn = (...a) => console.warn("[chart]", ...a);
  const error = (...a) => console.error("[chart]", ...a);

  // Initialize global state for sharing with trading.js
  window.PAC_TRADING_STATE = {
    longInstrument: null,
    shortInstrument: null,
  };

  if (typeof LightweightCharts === "undefined") {
    error("LightweightCharts global not found.");
    document.getElementById("status").textContent = "Chart lib N/A";
    return;
  }

  try {
    // -----------------------------------------------------------
    // 1) CHART MANAGER FACTORY
    // -----------------------------------------------------------
    // Creates a self-contained chart instance for a specific DOM element ID
    function createChartManager(containerId, options = {}) {
      const container = document.getElementById(containerId);
      if (!container) return null;

      const chart = LightweightCharts.createChart(container, {
        width: container.clientWidth,
        height: container.clientHeight,
        layout: {
          textColor: "#d1d4dc",
          background: { type: "Solid", color: "#000000ff" },
        },
        grid: {
          vertLines: { color: "transparent" },
          horzLines: { color: "transparent" },
        },
        rightPriceScale: { scaleMargins: { top: 0.2, bottom: 0.2 } },
        timeScale: {
          timeVisible: true,
          secondsVisible: false,
          barSpacing: 6,
          minBarSpacing: 3,
        },
      });

      // Series creation
      const candleSeries = chart.addSeries(
        LightweightCharts.CandlestickSeries,
        {
          upColor: "#4caf50",
          downColor: "#f44336",
          borderDownColor: "#f44336",
          borderUpColor: "#4caf50",
          wickDownColor: "#f44336",
          wickUpColor: "#4caf50",
        }
      );

      const ema20Series = chart.addSeries(LightweightCharts.LineSeries, {
        color: "#6bebffff",
        lineWidth: 1,
      });

      const yesterdayHighSeries = chart.addSeries(
        LightweightCharts.LineSeries,
        {
          color: "#8929ffff",
          lineWidth: 1,
          lineStyle: LightweightCharts.LineStyle.Dashed,
          title: "Y-H",
        }
      );
      const yesterdayLowSeries = chart.addSeries(LightweightCharts.LineSeries, {
        color: "#8929ffff",
        lineWidth: 1,
        lineStyle: LightweightCharts.LineStyle.Dashed,
        title: "Y-L",
      });

      const todayHighSeries = chart.addSeries(LightweightCharts.LineSeries, {
        color: "#c04d00ff",
        lineWidth: 1,
        title: "T-H",
      });
      const todayLowSeries = chart.addSeries(LightweightCharts.LineSeries, {
        color: "#c04d00ff",
        lineWidth: 1,
        title: "T-L",
      });

      // Data state
      const state = {
        chart,
        series: {
          candle: candleSeries,
          ema20: ema20Series,
          yh: yesterdayHighSeries,
          yl: yesterdayLowSeries,
          th: todayHighSeries,
          tl: todayLowSeries,
        },
        data: new Map(), // time -> candle
        ema20Data: new Map(),
        lastBarTime: null,
        lastEMAValue: null,
        lastEMATime: null,
        highLowState: {
          currentDay: null,
          currentHigh: -Infinity,
          currentLow: Infinity,
          yesterdayHigh: null,
          yesterdayLow: null,
        },
        groupingState: {
          // kept for compatibility if needed, though unused for logic here
          sessionTZ: null,
          sessionOpenMinutes: null,
          sessionCloseMinutes: null,
          count: 0,
          bar_group_count: 0,
          currentDay: null,
          tf: "5",
        },
        barGroupMap: new Map(),
        currentInstrument: null,
        toolTipElement: document.getElementById(options.toolTipId),

        // Methods
        resize: () => {
          if (container) {
            chart.applyOptions({
              width: container.clientWidth,
              height: container.clientHeight,
            });
          }
        },
      };

      // Tooltip handling
      chart.subscribeCrosshairMove((param) => {
        const tt = state.toolTipElement;
        if (!tt) return;

        tt.innerHTML = [
          `<span style="color: #ddd;">O: -</span>`,
          `<span style="color: #4caf50;">H: -</span>`,
          `<span style="color: #f44336;">L: -</span>`,
          `<span style="color: #ddd;">C: -</span>`,
          `<span style="color: #ff9900;">Bar -</span>`,
        ].join(" ");

        if (
          !param.point ||
          !param.time ||
          param.point.x < 0 ||
          param.point.y < 0
        ) {
          return;
        }

        const cData = param.seriesData.get(candleSeries);
        if (!cData) return;

        const { open, high, low, close } = cData;
        if (open === undefined) return;

        // Bar group
        const barGroup = state.barGroupMap.get(cData.time);
        const parts = [
          `<span style="color: #ddd;">O: ${open.toFixed(2)}</span>`,
          `<span style="color: #4caf50;">H: ${high.toFixed(2)}</span>`,
          `<span style="color: #f44336;">L: ${low.toFixed(2)}</span>`,
          `<span style="color: #ddd;">C: ${close.toFixed(2)}</span>`,
        ];
        if (barGroup !== undefined) {
          parts.push(`<span style="color: #ff9900;">Bar ${barGroup}</span>`);
        } else {
          parts.push(`<span style="color: #ff9900;">Bar -</span>`);
        }
        tt.innerHTML = parts.join(" ");
      });

      // Use ResizeObserver for robust responsiveness (handles orientation, flex changes)
      const observer = new ResizeObserver(() => {
        if (container) {
          chart.applyOptions({
            width: container.clientWidth,
            height: container.clientHeight,
          });
        }
      });
      observer.observe(container);

      return state;
    }

    // CREATE CHART INSTANCES
    const chartLong = createChartManager("chart-long", {
      toolTipId: "chart-ohlc-info-long",
    });
    const chartShort = createChartManager("chart-short", {
      toolTipId: "chart-ohlc-info-short",
    });

    // -----------------------------------------------------------
    // 2) SHARED LOGIC / HELPERS
    // -----------------------------------------------------------
    const INTERVAL_SECONDS = 5 * 60;

    // Countdown state (shared or per chart? usually one market clock)
    // We can display countdown on status line
    let countdownTimerId = null;
    let countdownEndTime = null;

    function formatMMSS(sec) {
      if (sec < 0) sec = 0;
      const m = Math.floor(sec / 60);
      const s = sec % 60;
      return `${m}:${String(s).padStart(2, "0")}`;
    }

    function updateCountdownDisplay() {
      if (!countdownEndTime) return;
      const now = Math.floor(Date.now() / 1000);
      let rem = countdownEndTime - now;
      const statusEl = document.getElementById("status");
      if (!statusEl) return;

      if (rem <= 0) {
        statusEl.textContent = "0:00";
        clearCountdown();
        // Try to restart if market is open (using Long chart config as master)
        ensureMarketCountdown(chartLong.groupingState);
        return;
      }
      statusEl.textContent = formatMMSS(rem);
    }

    function startCountdownForBar(endUnixSeconds, sessionConfig) {
      if (!sessionConfig || !sessionConfig.sessionTZ) return;
      const now = Math.floor(Date.now() / 1000);
      // check trading hours
      if (!isTradingHoursForBar(now, sessionConfig)) {
        document.getElementById("status").textContent = "Market closed";
        return;
      }

      let end = Number(endUnixSeconds);
      if (!Number.isFinite(end) || end <= now) {
        end = now + INTERVAL_SECONDS;
      }

      if (countdownEndTime && countdownEndTime === end && countdownTimerId)
        return;

      clearCountdown(false);
      countdownEndTime = end;
      updateCountdownDisplay();
      countdownTimerId = setInterval(updateCountdownDisplay, 1000);
    }

    function clearCountdown(setMarketMessage = true) {
      if (countdownTimerId) {
        clearInterval(countdownTimerId);
        countdownTimerId = null;
      }
      countdownEndTime = null;
      if (setMarketMessage) {
        const statusEl = document.getElementById("status");
        // Use chartLong session config as canonical
        if (chartLong.groupingState.sessionTZ) {
          const now = Math.floor(Date.now() / 1000);
          if (isTradingHoursForBar(now, chartLong.groupingState)) {
            statusEl.textContent = "Market open";
          } else {
            statusEl.textContent = "Market closed";
          }
        } else {
          statusEl.textContent = "Disconnected";
        }
      }
    }

    function ensureMarketCountdown(groupingState) {
      if (!groupingState || !groupingState.sessionTZ) return;
      const now = Math.floor(Date.now() / 1000);
      if (!isTradingHoursForBar(now, groupingState)) {
        clearCountdown(true);
        return;
      }
      // Align to 5m
      const barStart = Math.floor(now / INTERVAL_SECONDS) * INTERVAL_SECONDS;
      const barEnd = barStart + INTERVAL_SECONDS;
      startCountdownForBar(barEnd, groupingState);
    }

    function isoToLWTime(iso) {
      const ms = Date.parse(iso);
      return Math.floor(ms / 1000);
    }

    function getLocalParts(unixSeconds, timeZone) {
      const d = new Date(unixSeconds * 1000);
      const parts = new Intl.DateTimeFormat("en-GB", {
        timeZone,
        hour12: false,
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      }).formatToParts(d);
      const map = {};
      parts.forEach((p) => (map[p.type] = p.value));
      return {
        year: parseInt(map.year, 10),
        month: parseInt(map.month, 10),
        day: parseInt(map.day, 10),
        hour: parseInt(map.hour, 10),
        minute: parseInt(map.minute, 10),
        second: parseInt(map.second, 10),
        ymd: `${map.year}-${map.month}-${map.day}`,
      };
    }

    function hhmmToMinutes(hhmm) {
      if (!hhmm) return null;
      const [h, m] = hhmm.split(":").map((s) => parseInt(s, 10));
      return h * 60 + (isNaN(m) ? 0 : m);
    }

    function isTradingHoursForBar(unixSeconds, gState) {
      if (
        !gState.sessionTZ ||
        gState.sessionOpenMinutes == null ||
        gState.sessionCloseMinutes == null
      )
        return false;
      const parts = getLocalParts(unixSeconds, gState.sessionTZ);
      const minutes = parts.hour * 60 + parts.minute;
      return (
        minutes >= gState.sessionOpenMinutes &&
        minutes < gState.sessionCloseMinutes
      );
    }

    // -----------------------------------------------------------
    // 3) LOGIC FOR CALCULATIONS (EMA, HighLow, Grouping) - Adapted for Instance
    // -----------------------------------------------------------

    function calculateHistoricalEMA20(bars) {
      if (bars.length < 20) return [];
      const emaValues = [];
      const multiplier = 2 / 21;
      let sum = 0;
      for (let i = 0; i < 20; i++) sum += bars[i].close;
      let ema = sum / 20;
      emaValues.push({ time: bars[19].time, value: ema });
      for (let i = 20; i < bars.length; i++) {
        ema = (bars[i].close - ema) * multiplier + ema;
        emaValues.push({ time: bars[i].time, value: ema });
      }
      return emaValues;
    }

    function updateEMA20Incremental(cm, newClose, newTime) {
      // cm = chart manager instance
      if (cm.ema20Data.size === 0) return;
      const multiplier = 2 / 21;

      // get prev EMA
      const keys = Array.from(cm.ema20Data.keys()).sort((a, b) => a - b);
      let prevEMA = null;
      for (let i = keys.length - 1; i >= 0; i--) {
        if (keys[i] < newTime) {
          prevEMA = cm.ema20Data.get(keys[i]);
          break;
        }
      }

      if (prevEMA === null) {
        if (
          cm.lastEMAValue !== null &&
          cm.lastEMATime !== null &&
          cm.lastEMATime < newTime
        ) {
          prevEMA = cm.lastEMAValue;
        } else {
          return;
        }
      }

      const newEMA = (newClose - prevEMA) * multiplier + prevEMA;
      cm.ema20Data.set(newTime, newEMA);
      cm.lastEMAValue = newEMA;
      cm.lastEMATime = newTime;
      cm.series.ema20.update({ time: newTime, value: newEMA });
    }

    function calculateHistoryHighLow(cm, bars) {
      if (!bars || bars.length === 0 || !cm.groupingState.sessionTZ) return;

      const yhData = [],
        ylData = [],
        thData = [],
        tlData = [];
      let scanDay = null,
        scanHigh = -Infinity,
        scanLow = Infinity;
      let prevDayHigh = null,
        prevDayLow = null;

      for (const bar of bars) {
        const parts = getLocalParts(bar.time, cm.groupingState.sessionTZ);
        const dayKey = parts.ymd;

        if (dayKey !== scanDay) {
          if (scanDay !== null) {
            prevDayHigh = scanHigh;
            prevDayLow = scanLow;
          }
          scanDay = dayKey;
          scanHigh = -Infinity;
          scanLow = Infinity;
        }
        if (bar.high > scanHigh) scanHigh = bar.high;
        if (bar.low < scanLow) scanLow = bar.low;

        if (prevDayHigh !== null) {
          yhData.push({ time: bar.time, value: prevDayHigh });
          ylData.push({ time: bar.time, value: prevDayLow });
        }
        thData.push({ time: bar.time, value: scanHigh });
        tlData.push({ time: bar.time, value: scanLow });
      }

      cm.series.yh.setData(yhData);
      cm.series.yl.setData(ylData);
      cm.series.th.setData(thData);
      cm.series.tl.setData(tlData);

      cm.highLowState = {
        currentDay: scanDay,
        currentHigh: scanHigh,
        currentLow: scanLow,
        yesterdayHigh: prevDayHigh,
        yesterdayLow: prevDayLow,
      };
    }

    function updateHighLowIncremental(cm, bar) {
      if (!cm.groupingState.sessionTZ) return;
      const parts = getLocalParts(bar.time, cm.groupingState.sessionTZ);
      const dayKey = parts.ymd;

      if (dayKey !== cm.highLowState.currentDay) {
        if (cm.highLowState.currentDay !== null) {
          cm.highLowState.yesterdayHigh = cm.highLowState.currentHigh;
          cm.highLowState.yesterdayLow = cm.highLowState.currentLow;
        }
        cm.highLowState.currentDay = dayKey;
        cm.highLowState.currentHigh = -Infinity;
        cm.highLowState.currentLow = Infinity;
      }

      if (bar.high > cm.highLowState.currentHigh)
        cm.highLowState.currentHigh = bar.high;
      if (bar.low < cm.highLowState.currentLow)
        cm.highLowState.currentLow = bar.low;

      if (cm.highLowState.yesterdayHigh !== null) {
        cm.series.yh.update({
          time: bar.time,
          value: cm.highLowState.yesterdayHigh,
        });
        cm.series.yl.update({
          time: bar.time,
          value: cm.highLowState.yesterdayLow,
        });
      }
      cm.series.th.update({
        time: bar.time,
        value: cm.highLowState.currentHigh,
      });
      cm.series.tl.update({
        time: bar.time,
        value: cm.highLowState.currentLow,
      });
    }

    function processBarForGrouping(cm, barObj) {
      if (!cm.groupingState.sessionTZ) return;
      if (!barObj || !barObj.time) return;

      const parts = getLocalParts(barObj.time, cm.groupingState.sessionTZ);
      const dayKey = parts.ymd;

      if (cm.groupingState.currentDay !== dayKey) {
        cm.groupingState.currentDay = dayKey;
        cm.groupingState.count = 0;
        cm.groupingState.bar_group_count = 0;
      }

      const isOpenBar =
        parts.hour * 60 + parts.minute ===
          cm.groupingState.sessionOpenMinutes && parts.second === 0;

      if (isOpenBar) {
        cm.groupingState.count = 1;
        cm.groupingState.bar_group_count = 1;
        cm.barGroupMap.set(barObj.time, cm.groupingState.bar_group_count);
      } else if (isTradingHoursForBar(barObj.time, cm.groupingState)) {
        cm.groupingState.count += 1;
        cm.groupingState.bar_group_count += 1;
        cm.barGroupMap.set(barObj.time, cm.groupingState.bar_group_count);
      }
    }

    // -----------------------------------------------------------
    // 4) FETCH & LOAD LOGIC
    // -----------------------------------------------------------

    async function fetchHistory(instrument) {
      const res = await fetch(
        `/history/${encodeURIComponent(instrument)}?limit=3600`
      );
      if (!res.ok) throw new Error("history fetch failed: " + res.status);
      return await res.json();
    }

    function clearChartData(cm) {
      cm.data.clear();
      cm.lastBarTime = null;
      cm.ema20Data.clear();
      cm.lastEMAValue = null;
      cm.lastEMATime = null;

      cm.series.candle.setData([]);
      cm.series.ema20.setData([]);
      cm.series.yh.setData([]);
      cm.series.yl.setData([]);
      cm.series.th.setData([]);
      cm.series.tl.setData([]);

      cm.highLowState = {
        currentDay: null,
        currentHigh: -Infinity,
        currentLow: Infinity,
        yesterdayHigh: null,
        yesterdayLow: null,
      };
      cm.barGroupMap.clear();
      cm.groupingState.count = 0;
      cm.groupingState.bar_group_count = 0;
      cm.groupingState.currentDay = null;
    }

    // Config session based on instrument metadata
    function setSessionConfig(cm, instrumentId, map) {
      const conf = map[instrumentId];
      if (!conf) {
        cm.groupingState.sessionTZ = null;
        return;
      }
      // Simple map inherited properties
      cm.groupingState.sessionTZ = conf.timezone || null;
      cm.groupingState.sessionOpenMinutes = hhmmToMinutes(conf.market_open);
      cm.groupingState.sessionCloseMinutes = hhmmToMinutes(conf.market_close);
    }

    async function loadHistoryForInstrument(cm, instrumentId, instrumentMap) {
      if (cm.currentInstrument !== instrumentId) {
        clearChartData(cm);
        cm.currentInstrument = instrumentId;
        // Update labels/titles if needed
        // (Optional: update UI label like "Long: InstrumentName")
      }

      setSessionConfig(cm, instrumentId, instrumentMap);

      const bars = await fetchHistory(instrumentId);
      const barData = bars
        .map((b) => ({
          time: isoToLWTime(b.start_time),
          open: parseFloat(b.open),
          high: parseFloat(b.high),
          low: parseFloat(b.low),
          close: parseFloat(b.close),
        }))
        .filter((b) => b.open && !isNaN(b.open));

      cm.data.clear();
      cm.ema20Data.clear();
      barData.forEach((b) => cm.data.set(b.time, b));

      const sortedData = Array.from(cm.data.values()).sort(
        (a, b) => a.time - b.time
      );

      if (sortedData.length > 0) {
        cm.series.candle.setData(sortedData);
        cm.lastBarTime = sortedData[sortedData.length - 1].time;

        // EMA
        if (sortedData.length >= 20) {
          const histEMA = calculateHistoricalEMA20(sortedData);
          histEMA.forEach((e) => cm.ema20Data.set(e.time, e.value));
          cm.series.ema20.setData(histEMA);
          const last = histEMA[histEMA.length - 1];
          if (last) {
            cm.lastEMAValue = last.value;
            cm.lastEMATime = last.time;
          }
        }

        // High/Low
        calculateHistoryHighLow(cm, sortedData);

        // Grouping
        cm.barGroupMap.clear();
        cm.groupingState.count = 0;
        cm.groupingState.bar_group_count = 0;
        cm.groupingState.currentDay = null;
        sortedData.forEach((b) => processBarForGrouping(cm, b));
      }
    }

    // -----------------------------------------------------------
    // 5) SELECTOR LOGIC (ASSETS)
    // -----------------------------------------------------------
    let instrumentMapFlat = null;
    let hierarchicalMap = null;

    async function loadInstrumentJSON() {
      if (instrumentMapFlat && hierarchicalMap) return;
      const res = await fetch("/instrument_list.json");
      const data = await res.json();
      hierarchicalMap = data;
      instrumentMapFlat = flattenInstruments(data); // Re-use flatten helper logic
      return data;
    }

    function flattenInstruments(data) {
      // duplicate of previous logic
      const flat = {};
      for (const assetKey in data) {
        const assetData = data[assetKey];
        const common = {};
        if (assetData.timezone) common.timezone = assetData.timezone;
        if (assetData.market_open) common.market_open = assetData.market_open;
        if (assetData.market_close)
          common.market_close = assetData.market_close;

        for (const childKey in assetData) {
          const val = assetData[childKey];
          if (typeof val === "object" && val !== null && val.orderbookId) {
            const merged = Object.assign({}, val);
            for (const k in common) {
              if (!merged[k]) merged[k] = common[k];
            }
            flat[childKey] = merged;
          }
        }
      }
      return flat;
    }

    async function onAssetChanged(assetKey) {
      if (!hierarchicalMap) return;
      const asset = hierarchicalMap[assetKey];
      if (!asset) return;

      // Identify Long vs Short
      // Heuristic: Key contains "mini-l" or "mini-s"
      // Or iterate keys and check name?
      let longKey = null;
      let shortKey = null;

      for (const k in asset) {
        const v = asset[k];
        if (typeof v === "object" && v.orderbookId) {
          const lowerK = k.toLowerCase();
          if (
            lowerK.includes("mini-l") ||
            lowerK.includes("bull") ||
            (v.name && v.name.includes("MINI L"))
          ) {
            longKey = k;
          } else if (
            lowerK.includes("mini-s") ||
            lowerK.includes("bear") ||
            (v.name && v.name.includes("MINI S"))
          ) {
            shortKey = k;
          }
        }
      }

      // Update Global State
      window.PAC_TRADING_STATE.longInstrument = longKey;
      window.PAC_TRADING_STATE.shortInstrument = shortKey;

      // Update Labels
      const lName = longKey ? instrumentMapFlat[longKey].name : "N/A";
      const sName = shortKey ? instrumentMapFlat[shortKey].name : "N/A";
      document.getElementById("label-long").textContent = lName;
      document.getElementById("label-short").textContent = sName;

      // Load History
      const p = [];
      if (longKey)
        p.push(loadHistoryForInstrument(chartLong, longKey, instrumentMapFlat));
      if (shortKey)
        p.push(
          loadHistoryForInstrument(chartShort, shortKey, instrumentMapFlat)
        );

      await Promise.all(p);

      // Ensure countdown (uses Long as master)
      ensureMarketCountdown(chartLong.groupingState);
    }

    async function setupSelectors() {
      await loadInstrumentJSON();
      const sel = document.getElementById("asset");
      const assets = Object.keys(hierarchicalMap);

      sel.innerHTML = "";
      assets.forEach((a) => {
        const opt = document.createElement("option");
        opt.value = a;
        opt.textContent = a;
        sel.appendChild(opt);
      });

      if (assets.length > 0) {
        sel.value = assets[0];
        onAssetChanged(assets[0]);
      }

      sel.addEventListener("change", () => {
        onAssetChanged(sel.value);
      });
    }

    // -----------------------------------------------------------
    // 6) WEBSOCKET
    // -----------------------------------------------------------
    function setupWS() {
      const wsUrl = `ws://${window.location.host}/ws`;
      const ws = new WebSocket(wsUrl);
      ws.onopen = () => {
        log("WS connected");
        ensureMarketCountdown(chartLong.groupingState);
      };
      ws.onclose = () => {
        setTimeout(setupWS, 1000);
      };
      ws.onmessage = (evt) => {
        try {
          const msg = JSON.parse(evt.data);
          let targetChart = null;

          if (msg.instrument === window.PAC_TRADING_STATE.longInstrument)
            targetChart = chartLong;
          else if (msg.instrument === window.PAC_TRADING_STATE.shortInstrument)
            targetChart = chartShort;

          if (!targetChart) return;

          const b = msg.bar;
          if (!b) return;
          const t = isoToLWTime(b.start_time);

          const candleData = {
            time: t,
            open: parseFloat(b.open),
            high: parseFloat(b.high),
            low: parseFloat(b.low),
            close: parseFloat(b.close),
          };

          if (msg.type === "update") {
            if (
              targetChart.lastBarTime === null ||
              t >= targetChart.lastBarTime
            ) {
              targetChart.data.set(t, candleData);
              targetChart.series.candle.update(candleData);
              updateEMA20Incremental(targetChart, candleData.close, t);
              updateHighLowIncremental(targetChart, candleData);

              // Update grouping if new bar
              if (t > targetChart.lastBarTime) {
                processBarForGrouping(targetChart, candleData);
                targetChart.lastBarTime = t;
              }

              // Countdown (only if it's master i.e. long?) or both?
              // Usually we only show one countdown.
              if (msg.instrument === window.PAC_TRADING_STATE.longInstrument) {
                const barEnd = t + INTERVAL_SECONDS; // simplify
                startCountdownForBar(barEnd, targetChart.groupingState);
              }
            }
          } else if (msg.type === "completed") {
            // Similar logic to integrate completed bar
            targetChart.data.set(t, candleData);
            targetChart.series.candle.update(candleData);
            updateEMA20Incremental(targetChart, candleData.close, t);
            updateHighLowIncremental(targetChart, candleData);

            if (targetChart.lastBarTime === null || t > targetChart.lastBarTime)
              targetChart.lastBarTime = t;

            if (msg.instrument === window.PAC_TRADING_STATE.longInstrument) {
              clearCountdown(true);
              ensureMarketCountdown(targetChart.groupingState);
            }
          }

          // EMA from backend
          if (msg.emas && msg.emas["20"]) {
            const e = msg.emas["20"];
            if (e && e.value) {
              const et = isoToLWTime(e.time);
              const ev = parseFloat(e.value);
              if (targetChart.ema20Data.size === 0) {
                targetChart.ema20Data.set(et, ev);
                targetChart.lastEMAValue = ev;
                targetChart.lastEMATime = et;
                targetChart.series.ema20.update({ time: et, value: ev });
              }
              // ... other validation logic omitted for brevity, keeping simple trust for now
            }
          }
        } catch (e) {
          error("WS Msg error", e);
        }
      };
    }

    // -----------------------------------------------------------
    // INIT
    // -----------------------------------------------------------
    (async () => {
      await setupSelectors();
      setupWS();
    })();
  } catch (e) {
    error("Chart init failed:", e);
    document.getElementById("status").textContent = "Chart init failed";
  }
})();
