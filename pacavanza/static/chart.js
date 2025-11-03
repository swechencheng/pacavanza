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
        timeScale: {
          timeVisible: true,
          secondsVisible: false,
          barSpacing: 6,
          minBarSpacing: 3,
        },
      }
    );

    // handle resize
    window.addEventListener("resize", () => {
      chart.applyOptions({
        width: document.getElementById("chart").clientWidth,
        height: document.getElementById("chart").clientHeight,
      });
    });

    // ********** v5 series creation **********
    const candleSeries = chart.addSeries(LightweightCharts.CandlestickSeries, {
      upColor: "#4caf50",
      downColor: "#f44336",
      borderDownColor: "#f44336",
      borderUpColor: "#4caf50",
      wickDownColor: "#f44336",
      wickUpColor: "#4caf50",
    });

    const ema20Series = chart.addSeries(LightweightCharts.LineSeries, {
      color: "#FF6B6B",
      lineWidth: 1,
    });
    const ema50Series = chart.addSeries(LightweightCharts.LineSeries, {
      color: "#4ECDC4",
      lineWidth: 1,
    });
    const ema100Series = chart.addSeries(LightweightCharts.LineSeries, {
      color: "#45B7D1",
      lineWidth: 1,
    });
    const ema220Series = chart.addSeries(LightweightCharts.LineSeries, {
      color: "#FFA07A",
      lineWidth: 1,
    });

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

    // Track current data to avoid duplicates and handle updates properly
    let currentData = new Map(); // time -> candle data
    let lastBarTime = null;
    let currentStock = null; // Track the currently displayed stock

    function isoToLWTime(iso) {
      const d = new Date(iso);
      // Convert to local time by creating a UTC timestamp that represents the local time
      return (
        Date.UTC(
          d.getFullYear(),
          d.getMonth(),
          d.getDate(),
          d.getHours(),
          d.getMinutes(),
          d.getSeconds(),
          d.getMilliseconds()
        ) / 1000
      );
    }

    async function fetchHistory(stock) {
      const res = await fetch(
        `/history/${encodeURIComponent(stock)}?limit=500`
      );
      if (!res.ok) throw new Error("history fetch failed: " + res.status);
      return await res.json();
    }

    // Clear all chart data
    function clearChartData() {
      currentData.clear();
      lastBarTime = null;
      candleSeries.setData([]);
      ema20Series.setData([]);
      ema50Series.setData([]);
      ema100Series.setData([]);
      ema220Series.setData([]);

      // Clear markers
      if (
        seriesMarkersApi &&
        typeof seriesMarkersApi.setMarkers === "function"
      ) {
        seriesMarkersApi.setMarkers([]);
      }
    }

    // helper to load history and setData on the candlestick series
    async function loadHistoryFor(stock) {
      document.getElementById("status").textContent = "Loading history...";
      try {
        // Clear previous data if switching stocks
        if (currentStock !== stock) {
          clearChartData();
          currentStock = stock;
        }

        const bars = await fetchHistory(stock);
        const barData = bars
          .map((b) => {
            const time = isoToLWTime(b.start_time);
            return {
              time: time,
              open: parseFloat(b.open),
              high: parseFloat(b.high),
              low: parseFloat(b.low),
              close: parseFloat(b.close),
            };
          })
          .filter(
            (bar) =>
              bar.open &&
              bar.high &&
              bar.low &&
              bar.close &&
              !isNaN(bar.open) &&
              !isNaN(bar.high) &&
              !isNaN(bar.low) &&
              !isNaN(bar.close)
          );

        // Clear current data and repopulate
        currentData.clear();
        barData.forEach((bar) => {
          currentData.set(bar.time, bar);
        });

        // Sort by time and set data
        const sortedData = Array.from(currentData.values()).sort(
          (a, b) => a.time - b.time
        );

        if (sortedData.length > 0) {
          candleSeries.setData(sortedData);
          lastBarTime = sortedData[sortedData.length - 1].time;
          log(
            `Initial lastBarTime set to: ${lastBarTime}`
          );
        }

        document.getElementById("status").textContent = "History loaded.";
        setInfo(`History loaded: ${barData.length} bars for ${stock}`);
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

    // Load button - load new stock data
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

            // CRITICAL FIX: Only process messages for the currently displayed stock
            if (!msg || msg.stock !== currentStock) {
              // Log for debugging (optional)
              if (msg && msg.stock) {
                log(
                  `Ignoring message for stock: ${msg.stock}, current: ${currentStock}`
                );
              }
              return;
            }

            if (msg.type !== "update" && msg.type !== "completed") {
              return;
            }

            const b = msg.bar;
            if (!b) return;

            // time is ISO string -> unix seconds
            const t = isoToLWTime(b.start_time);

            // Validate data to prevent "Value is null" errors
            const open = parseFloat(b.open);
            const high = parseFloat(b.high);
            const low = parseFloat(b.low);
            const close = parseFloat(b.close);

            if (isNaN(open) || isNaN(high) || isNaN(low) || isNaN(close)) {
              error("Invalid bar data received:", b);
              return;
            }

            const candleData = {
              time: t,
              open: open,
              high: high,
              low: low,
              close: close,
            };

            // Handle different message types
            if (msg.type === "update") {
              // For update messages, only update if this is the current or newer bar
              if (lastBarTime === null || t >= lastBarTime) {
                currentData.set(t, candleData);
                try {
                  candleSeries.update(candleData);
                  if (t > lastBarTime) {
                    lastBarTime = t;
                    log(
                      `Updated lastBarTime to: ${lastBarTime}`
                    );
                  }
                } catch (e) {
                  if (
                    e.message &&
                    e.message.includes("Cannot update oldest data")
                  ) {
                    log(
                      `Skipping update for historical bar at ${t}`
                    );
                  } else {
                    throw e;
                  }
                }
              } else {
                log(
                  `Skipping update for historical bar at ${t} , current lastBarTime: ${lastBarTime}`
                );
              }
            } else if (msg.type === "completed") {
              // For completed messages, we need to be more careful
              // Only update if this is a new bar or the current bar
              if (lastBarTime === null || t >= lastBarTime) {
                currentData.set(t, candleData);
                try {
                  candleSeries.update(candleData);
                  if (t > lastBarTime) {
                    lastBarTime = t;
                    log(
                      `Completed bar - updated lastBarTime to: ${lastBarTime}`
                    );
                  } else {
                    log(`Completed current bar at ${t}`);
                  }
                } catch (e) {
                  if (
                    e.message &&
                    e.message.includes("Cannot update oldest data")
                  ) {
                    // For completed bars that are in history, we might need to replace the data
                    log(
                      `Completed bar is historical, replacing data set for ${t}`
                    );
                    // Remove the old bar and add the new one
                    currentData.delete(t);
                    currentData.set(t, candleData);
                    // Recreate the entire dataset
                    const sortedData = Array.from(currentData.values()).sort(
                      (a, b) => a.time - b.time
                    );
                    candleSeries.setData(sortedData);
                  } else {
                    throw e;
                  }
                }
              } else {
                log(
                  `Skipping completed historical bar at ${t}, current lastBarTime: ${lastBarTime}`
                );
              }
            }

            // Update EMAs with proper validation
            // EMAs: server sends incremental EMA values in msg.emas keyed by length
            if (msg.emas) {
              Object.keys(msg.emas).forEach((emaLength) => {
                const emaData = msg.emas[emaLength];
                if (emaData && emaData.value !== undefined && emaData.time) {
                  const emaTime = isoToLWTime(emaData.time);
                  const emaValue = parseFloat(emaData.value);

                  if (!isNaN(emaValue)) {
                    const emaPoint = { time: emaTime, value: emaValue };

                    try {
                      // Update the appropriate EMA series
                      switch (emaLength) {
                        case "20":
                          ema20Series.update(emaPoint);
                          break;
                        case "50":
                          ema50Series.update(emaPoint);
                          break;
                        case "100":
                          ema100Series.update(emaPoint);
                          break;
                        case "220":
                          ema220Series.update(emaPoint);
                          break;
                      }
                    } catch (e) {
                      if (
                        e.message &&
                        e.message.includes("Cannot update oldest data")
                      ) {
                        log(
                          `Skipping EMA update for historical time ${emaTime}`
                        );
                      } else {
                        throw e;
                      }
                    }
                  }
                }
              });
            }

            // Update label markers
            if (
              msg.label &&
              seriesMarkersApi &&
              typeof seriesMarkersApi.setMarkers === "function"
            ) {
              const markerTime = isoToLWTime(msg.label.time);
              const marker = {
                time: markerTime,
                position: "belowBar",
                color: "orange",
                shape: "square",
                text: String(msg.label.text || ""),
              };
              try {
                // Keep existing markers and add new one
                const currentMarkers = seriesMarkersApi.getMarkers() || [];
                const newMarkers = [
                  ...currentMarkers.filter((m) => m.time !== markerTime),
                  marker,
                ];
                seriesMarkersApi.setMarkers(newMarkers);
              } catch (e) {
                log(`Failed to set marker: ${e.message}`);
              }
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
      currentStock = initialStock; // Set the initial stock
      try {
        await loadHistoryFor(initialStock);
      } catch (e) {
        // already logged in loadHistoryFor; continue to setup WS regardless so
        // live ticks can still arrive and update the chart.
      } finally {
        // After attempting history load, start the websocket to receive streaming updates.
        setupWS();
        // Also, reset tooltip to default state
        if (toolTip) {
          toolTip.innerHTML = [
            `<span style="color: #ddd;">O: -</span>`,
            `<span style="color: #4caf50;">H: -</span>`,
            `<span style="color: #f44336;">L: -</span>`,
            `<span style="color: #ddd;">C: -</span>`,
          ].join(" ");
        }
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
