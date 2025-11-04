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
        grid: {
          vertLines: { color: "#727272ff" },
          horzLines: { color: "#727272ff" },
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

    // ********** ONLY EMA20 REMAINS - OTHER EMAS REMOVED **********
    const ema20Series = chart.addSeries(LightweightCharts.LineSeries, {
      color: "#6bebffff",
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

    // Track current data to avoid duplicates and handle updates properly
    let currentData = new Map(); // time -> candle data
    let lastBarTime = null;
    let currentStock = null; // Track the currently displayed stock
    let ema20Data = new Map(); // time -> ema20 value for incremental updates

    // keep a quick cache of the latest EMA and its time to avoid sorting each tick
    let lastEMAValue = null;
    let lastEMATime = null;

    function isoToLWTime(iso) {
      const ms = Date.parse(iso);
      // Convert to unix seconds (floor to avoid fractional seconds differences)
      return Math.floor(ms / 1000);
    }

    // ********** EMA20 CALCULATION FUNCTIONS **********
    function calculateHistoricalEMA20(bars) {
      if (bars.length < 20) return [];

      const emaValues = [];
      const multiplier = 2 / (20 + 1);

      // Calculate SMA for first 20 periods
      let sum = 0;
      for (let i = 0; i < 20; i++) {
        sum += bars[i].close;
      }
      let ema = sum / 20;
      emaValues.push({ time: bars[19].time, value: ema });

      // Calculate EMA for remaining periods
      for (let i = 20; i < bars.length; i++) {
        ema = (bars[i].close - ema) * multiplier + ema;
        emaValues.push({ time: bars[i].time, value: ema });
      }

      return emaValues;
    }

    function updateEMA20Incremental(newClose, newTime) {
      // If we don't have any historical EMA base, we should not try to invent one.
      // The EMA requires a prior EMA (usually from historical calculation).
      if (ema20Data.size === 0) {
        // No historical EMA to base from — skip incremental EMA until we have history.
        return;
      }

      const multiplier = 2 / (20 + 1);

      // Find the most recent EMA time strictly less than newTime (previous bar)
      // Note: we intentionally require strictly less-than so we use the EMA of the previous bar.
      const keys = Array.from(ema20Data.keys()).sort((a, b) => a - b);
      let prevEMA = null;
      for (let i = keys.length - 1; i >= 0; i--) {
        if (keys[i] < newTime) {
          prevEMA = ema20Data.get(keys[i]);
          break;
        }
      }

      // If we failed to find a strictly-less key, try safe fallback:
      // use lastEMAValue only if it exists and its time is strictly less than newTime.
      if (prevEMA === null) {
        if (
          lastEMAValue !== null &&
          lastEMATime !== null &&
          lastEMATime < newTime
        ) {
          prevEMA = lastEMAValue;
        } else {
          // Cannot compute an incremental EMA safely (no proper prior EMA) — skip.
          return;
        }
      }

      const newEMA = (newClose - prevEMA) * multiplier + prevEMA;

      ema20Data.set(newTime, newEMA);
      // Update cached last EMA/time
      lastEMAValue = newEMA;
      lastEMATime = newTime;

      ema20Series.update({ time: newTime, value: newEMA });
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
      ema20Data.clear(); // Clear EMA20 data when switching stocks
      lastEMAValue = null;
      lastEMATime = null;
      candleSeries.setData([]);
      ema20Series.setData([]); // Only clear EMA20 series
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
        ema20Data.clear(); // Clear EMA20 data for new stock
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
          log(`Initial lastBarTime set to: ${lastBarTime}`);

          // ********** CALCULATE HISTORICAL EMA20 **********
          if (sortedData.length >= 20) {
            const historicalEMA20 = calculateHistoricalEMA20(sortedData);
            historicalEMA20.forEach((ema) => {
              ema20Data.set(ema.time, ema.value);
            });
            ema20Series.setData(historicalEMA20);

            // cache last EMA/time for incremental updates (avoid sorting each tick)
            const lastEmaPoint = historicalEMA20[historicalEMA20.length - 1];
            if (lastEmaPoint) {
              lastEMAValue = lastEmaPoint.value;
              lastEMATime = lastEmaPoint.time;
            }

            log(
              `Calculated EMA20 for ${historicalEMA20.length} historical bars`
            );
          }
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

    // WebSocket for streaming updates (updates candlestick points + EMAs)
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

                  // ********** INCREMENTAL EMA20 UPDATE FOR REAL-TIME DATA **********
                  updateEMA20Incremental(close, t);

                  if (t > lastBarTime) {
                    lastBarTime = t;
                    log(`Updated lastBarTime to: ${lastBarTime}`);
                  }
                } catch (e) {
                  if (
                    e.message &&
                    e.message.includes("Cannot update oldest data")
                  ) {
                    log(`Skipping update for historical bar at ${t}`);
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

                  // ********** INCREMENTAL EMA20 UPDATE FOR COMPLETED BARS **********
                  updateEMA20Incremental(close, t);

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

                    // ********** RECALCULATE EMA20 WHEN REPLACING HISTORICAL DATA **********
                    if (sortedData.length >= 20) {
                      const historicalEMA20 =
                        calculateHistoricalEMA20(sortedData);
                      ema20Data.clear();
                      historicalEMA20.forEach((ema) => {
                        ema20Data.set(ema.time, ema.value);
                      });
                      ema20Series.setData(historicalEMA20);

                      // update cached last EMA/time after recalculation
                      const lastEmaPoint =
                        historicalEMA20[historicalEMA20.length - 1];
                      if (lastEmaPoint) {
                        lastEMAValue = lastEmaPoint.value;
                        lastEMATime = lastEmaPoint.time;
                      }
                    }
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
              // ********** ONLY UPDATE EMA20 FROM BACKEND (AS BACKUP) **********
              if (msg.emas["20"]) {
                const emaData = msg.emas["20"];
                if (emaData && emaData.value !== undefined && emaData.time) {
                  const emaTime = isoToLWTime(emaData.time);
                  const emaValue = parseFloat(emaData.value);

                  if (!isNaN(emaValue)) {
                    const emaPoint = { time: emaTime, value: emaValue };

                    try {
                      // If we have no local EMA history, accept backend EMA (bootstrap).
                      if (ema20Data.size === 0) {
                        ema20Data.set(emaTime, emaValue);
                        lastEMAValue = emaValue;
                        lastEMATime = emaTime;
                        ema20Series.update(emaPoint);
                      } else {
                        // Otherwise, be conservative: only accept backend EMA if it's strictly newer
                        // and not wildly different from our local cached EMA to avoid sudden jumps.
                        if (lastEMATime === null || emaTime > lastEMATime) {
                          const localCompare = lastEMAValue || emaValue;
                          const diff = Math.abs(emaValue - localCompare);
                          // 5% tolerance threshold — adjust if necessary
                          if (diff / (Math.abs(localCompare) || 1) < 0.05) {
                            ema20Data.set(emaTime, emaValue);
                            lastEMAValue = emaValue;
                            lastEMATime = emaTime;
                            ema20Series.update(emaPoint);
                          } else {
                            log(
                              `Ignoring backend EMA (time ${emaTime}) due to large deviation (${(
                                diff / (Math.abs(localCompare) || 1)
                              ).toFixed(3)}) from local EMA`
                            );
                          }
                        } else {
                          // ignore equal/older backend EMA to avoid overwriting historical/local EMA
                          log(
                            `Ignoring backend EMA for time ${emaTime} (not newer than lastEMATime ${lastEMATime})`
                          );
                        }
                      }
                    } catch (e) {
                      if (
                        e.message &&
                        e.message.includes("Cannot update oldest data")
                      ) {
                        log(
                          `Skipping EMA20 update for historical time ${emaTime}`
                        );
                      } else {
                        throw e;
                      }
                    }
                  }
                }
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
