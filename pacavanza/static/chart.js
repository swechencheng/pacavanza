// chart.js
(() => {
  const log = (...a) => console.log("[chart]", ...a);
  const warn = (...a) => console.warn("[chart]", ...a);
  const error = (...a) => console.error("[chart]", ...a);
  const infoEl = document.getElementById("info");
  function setInfo(t) {
    if (infoEl) infoEl.textContent = t;
  }

  // confirm library loaded
  if (typeof LightweightCharts === "undefined") {
    error(
      "LightweightCharts global not found. Check that the exact CDN script loaded."
    );
    document.getElementById("status").textContent = "Chart lib not loaded";
    setInfo("Expected LightweightCharts global not found; check Network tab.");
    return;
  }
  setInfo("Loaded lightweight-charts@5.0.9");

  try {
    // create chart per v5 docs
    const chart = LightweightCharts.createChart(
      document.getElementById("chart"),
      {
        width: document.getElementById("chart").clientWidth,
        height: document.getElementById("chart").clientHeight,
        layout: {
          textColor: "#d1d4dc",
          background: { type: "Solid", color: "#0b1220" },
        },
        rightPriceScale: { scaleMargins: { top: 0.2, bottom: 0.2 } },
        timeScale: { timeVisible: true, secondsVisible: false },
      }
    );

    // handle resize
    window.addEventListener("resize", () => {
      chart.applyOptions({
        width: document.getElementById("chart").clientWidth,
        height: document.getElementById("chart").clientHeight,
      });
    });

    // ********** v5 series creation (correct API) **********
    // add series by passing the series class (CandlestickSeries, LineSeries, ...)
    const candleSeries = chart.addSeries(LightweightCharts.CandlestickSeries);
    const ema20Series = chart.addSeries(LightweightCharts.LineSeries);
    const ema50Series = chart.addSeries(LightweightCharts.LineSeries);
    const ema100Series = chart.addSeries(LightweightCharts.LineSeries);
    const ema220Series = chart.addSeries(LightweightCharts.LineSeries);

    // ********** OHLC TOOLTIP FOR CONTROLS LINE **********
    // Get the tooltip element
    const toolTip = document.getElementById("chart-tooltip");

    // Subscribe to crosshair movements
    chart.subscribeCrosshairMove((param) => {
      // Default tooltip content when not hovering over a data point
      toolTip.innerHTML = [
        `<span style="color: #ddd;">O: -</span>`,
        `<span style="color: #4caf50;">H: -</span>`,
        `<span style="color: #f44336;">L: -</span>`,
        `<span style="color: #ddd;">C: -</span>`,
      ].join(" ");

      // Check if the crosshair is over a data point
      if (
        param.point === undefined ||
        !param.time ||
        param.point.x < 0 ||
        param.point.y < 0
      ) {
        return;
      } else {
        // Get the candlestick data at the hovered time
        const candleData = param.seriesData.get(candleSeries);

        // Check if data exists
        if (candleData === undefined) {
          return;
        }

        // Extract OHLC values
        const open = candleData.open;
        const high = candleData.high;
        const low = candleData.low;
        const close = candleData.close;

        // Check if we have valid data to display
        if (
          open === undefined ||
          high === undefined ||
          low === undefined ||
          close === undefined
        ) {
          return;
        }

        // Create the tooltip content - SIMPLE TEXT FOR CONTROLS LINE
        toolTip.innerHTML = [
          `<span style="color: #ddd;">O: ${open.toFixed(2)}</span>`,
          `<span style="color: #4caf50;">H: ${high.toFixed(2)}</span>`,
          `<span style="color: #f44336;">L: ${low.toFixed(2)}</span>`,
          `<span style="color: #ddd;">C: ${close.toFixed(2)}</span>`,
        ].join(" ");
      }
    });

    // create a series-markers plugin instance once (re-use, don't recreate for every tick)
    let seriesMarkersApi = null;
    if (typeof LightweightCharts.createSeriesMarkers === "function") {
      seriesMarkersApi = LightweightCharts.createSeriesMarkers(
        candleSeries,
        []
      );
    }

    function isoToLWTime(iso) {
      const d = new Date(iso);
      return Math.floor(d.getTime() / 1000);
    }

    async function fetchHistory(stock) {
      const res = await fetch(
        `/history/${encodeURIComponent(stock)}?limit=500`
      );
      if (!res.ok) throw new Error("history fetch failed: " + res.status);
      return await res.json();
    }

    // helper to load history and setData on the candlestick series
    async function loadHistoryFor(stock) {
      document.getElementById("status").textContent = "Loading history...";
      try {
        const bars = await fetchHistory(stock);
        const barData = bars.map((b) => ({
          time: isoToLWTime(b.end_time),
          open: b.open,
          high: b.high,
          low: b.low,
          close: b.close,
        }));
        candleSeries.setData(barData);
        document.getElementById("status").textContent = "History loaded.";
        setInfo(`History loaded: ${barData.length} bars`);
        log("History loaded for", stock, barData.length);
        return barData.length;
      } catch (err) {
        error("History load failed:", err);
        document.getElementById("status").textContent =
          "Failed to load history — see console";
        setInfo("History fetch failed; check /history/<stock> endpoint");
        throw err;
      }
    }

    // Load button still works manually
    document.getElementById("load").addEventListener("click", async () => {
      const stock = document.getElementById("stock").value;
      await loadHistoryFor(stock).catch(() => {
        /* already handled above */
      });
    });

    // WebSocket for streaming updates (updates candlestick points + EMAs + markers)
    function setupWS() {
      try {
        const wsUrl = `ws://${window.location.host}/ws`;
        log("Connecting websocket to", wsUrl);
        const ws = new WebSocket(wsUrl);

        ws.onopen = () => {
          log("WS connected");
          document.getElementById("status").textContent = "WS connected";
        };
        ws.onclose = (ev) => {
          warn("WS closed", ev);
          document.getElementById("status").textContent =
            "WS closed — retrying...";
          setTimeout(setupWS, 1000);
        };
        ws.onerror = (e) => {
          error("WS error", e);
        };
        ws.onmessage = (evt) => {
          try {
            const msg = JSON.parse(evt.data);
            if (!msg || (msg.type !== "update" && msg.type !== "completed"))
              return;
            const b = msg.bar;
            if (!b) return;

            // time is ISO string -> unix seconds
            const t = isoToLWTime(b.end_time);
            // update candlestick series (update expects a single point or setData for bulk)
            candleSeries.update({
              time: t,
              open: b.open,
              high: b.high,
              low: b.low,
              close: b.close,
            });

            // EMAs: server sends incremental EMA values in msg.emas keyed by length
            if (msg.emas) {
              if (msg.emas["20"])
                ema20Series.update({
                  time: isoToLWTime(msg.emas["20"].time),
                  value: msg.emas["20"].value,
                });
              if (msg.emas["50"])
                ema50Series.update({
                  time: isoToLWTime(msg.emas["50"].time),
                  value: msg.emas["50"].value,
                });
              if (msg.emas["100"])
                ema100Series.update({
                  time: isoToLWTime(msg.emas["100"].time),
                  value: msg.emas["100"].value,
                });
              if (msg.emas["220"])
                ema220Series.update({
                  time: isoToLWTime(msg.emas["220"].time),
                  value: msg.emas["220"].value,
                });
            }

            // label — use seriesMarkersApi.setMarkers([...]) (faster than recreating)
            if (
              msg.label &&
              seriesMarkersApi &&
              typeof seriesMarkersApi.setMarkers === "function"
            ) {
              const marker = {
                time: t,
                position: "belowBar",
                color: "orange",
                shape: "square",
                text: String(msg.label.text || ""),
              };
              seriesMarkersApi.setMarkers([marker]);
            }
          } catch (e) {
            error("Failed processing WS message:", e, evt.data);
          }
        };
      } catch (e) {
        error("Failed to setup WS:", e);
      }
    }

    // ------------- AUTO-LOAD on first page open -------------
    // Immediately load history for the currently selected stock (no button click).
    // This happens once during initialization.
    (async () => {
      const initialStock = document.getElementById("stock").value;
      try {
        await loadHistoryFor(initialStock);
      } catch (e) {
        // already logged in loadHistoryFor; continue to setup WS regardless so
        // live ticks can still arrive and update the chart.
      } finally {
        // After attempting history load, start the websocket to receive streaming updates.
        setupWS();
        // Also, reset tooltip to default state
        toolTip.innerHTML = [
          `<span style="color: #ddd;">O: -</span>`,
          `<span style="color: #4caf50;">H: -</span>`,
          `<span style="color: #f44336;">L: -</span>`,
          `<span style="color: #ddd;">C: -</span>`,
        ].join(" ");
      }
    })();
    // ---------------- end auto-load ----------------
  } catch (e) {
    error("Chart init failed:", e);
    document.getElementById("status").textContent =
      "Chart init failed — see console";
    setInfo(
      "Chart init error. Confirm the exact library build is the UMD standalone one."
    );
  }
})();
