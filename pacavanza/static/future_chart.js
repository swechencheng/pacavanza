(() => {
  const log = (...a) => console.log("[future-chart]", ...a);
  const warn = (...a) => console.warn("[future-chart]", ...a);
  const error = (...a) => console.error("[future-chart]", ...a);

  window.PAC_TRADING_STATE = {
    futureInstrument: null,
  };

  if (typeof LightweightCharts === "undefined") {
    error("LightweightCharts global not found.");
    document.getElementById("status").textContent = "Chart lib N/A";
    return;
  }

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

    const state = {
      chart,
      series: { candle: candleSeries },
      data: new Map(),
      lastBarTime: null,
      currentInstrument: null,
      toolTipElement: document.getElementById(options.toolTipId),
      resize: () => {
        if (container) {
          chart.applyOptions({
            width: container.clientWidth,
            height: container.clientHeight,
          });
        }
      },
    };

    chart.subscribeCrosshairMove((param) => {
      const tt = state.toolTipElement;
      if (!tt) return;

      tt.innerHTML = [
        `<span style="color: #ddd;">O: -</span>`,
        `<span style="color: #4caf50;">H: -</span>`,
        `<span style="color: #f44336;">L: -</span>`,
        `<span style="color: #ddd;">C: -</span>`
      ].join(" ");

      if (!param.point || !param.time || param.point.x < 0 || param.point.y < 0) {
        return;
      }

      const cData = param.seriesData.get(candleSeries);
      if (!cData) return;

      const { open, high, low, close } = cData;
      if (open === undefined) return;

      tt.innerHTML = [
        `<span style="color: #ddd;">O: ${open.toFixed(2)}</span>`,
        `<span style="color: #4caf50;">H: ${high.toFixed(2)}</span>`,
        `<span style="color: #f44336;">L: ${low.toFixed(2)}</span>`,
        `<span style="color: #ddd;">C: ${close.toFixed(2)}</span>`
      ].join(" ");
    });

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

  const chartFuture = createChartManager("chart-future", {
    toolTipId: "chart-ohlc-info-future",
  });

  function isoToLWTime(iso) {
    const ms = Date.parse(iso);
    return Math.floor(ms / 1000);
  }

  async function fetchHistory(instrument) {
    const res = await fetch(`/history/${encodeURIComponent(instrument)}?limit=3600`);
    if (!res.ok) throw new Error("history fetch failed: " + res.status);
    return await res.json();
  }

  async function loadHistoryForInstrument(cm, instrumentId) {
    cm.currentInstrument = instrumentId;
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
    barData.forEach((b) => cm.data.set(b.time, b));

    const sortedData = Array.from(cm.data.values()).sort((a, b) => a.time - b.time);
    if (sortedData.length > 0) {
      cm.series.candle.setData(sortedData);
      cm.lastBarTime = sortedData[sortedData.length - 1].time;
    }
  }

  async function loadInstrumentJSON() {
    const res = await fetch("/ava_mini_future_list.json");
    const flatMap = await res.json();
    return flatMap;
  }

  async function initializeFuture() {
    const flatMap = await loadInstrumentJSON();
    let futureKey = null;

    // Find the future (should be the one that isn't a mini and has a recent orderbook)
    // We can just find the one whose key doesn't start with "mini"
    for (const k in flatMap) {
      if (!k.toLowerCase().includes("mini")) {
        futureKey = k;
        break;
      }
    }

    if (!futureKey) {
      error("No active future found in instrument list!");
      return;
    }

    window.PAC_TRADING_STATE.futureInstrument = futureKey;
    document.getElementById("label-future").textContent = flatMap[futureKey].name;

    await loadHistoryForInstrument(chartFuture, futureKey);
  }

  function setupWS() {
    const wsUrl = `ws://${window.location.host}/ws`;
    const ws = new WebSocket(wsUrl);
    ws.onopen = () => {
      log("WS connected");
      document.getElementById("status").textContent = "Connected";
    };
    ws.onclose = () => {
      document.getElementById("status").textContent = "Disconnected";
      setTimeout(setupWS, 1000);
    };
    ws.onmessage = (evt) => {
      try {
        const msg = JSON.parse(evt.data);
        if (msg.instrument !== window.PAC_TRADING_STATE.futureInstrument) return;

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
          if (chartFuture.lastBarTime === null || t >= chartFuture.lastBarTime) {
            chartFuture.data.set(t, candleData);
            chartFuture.series.candle.update(candleData);
            if (t > chartFuture.lastBarTime) {
              chartFuture.lastBarTime = t;
            }
          }
        }
      } catch (e) {
        error("WS Parse Error", e);
      }
    };
  }

  initializeFuture().then(() => {
    setupWS();
  });

})();
