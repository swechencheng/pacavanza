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
        `<span style="color: #ff9900;">Bar -</span>`,
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

        // Determine bar group number (if we have one) and append to tooltip
        // Use candleData.time as key (should be unix seconds)
        const barGroup = barGroupMap.get(candleData.time);

        // Create the tooltip content - SIMPLE TEXT FOR CONTROLS LINE
        const parts = [
          `<span style="color: #ddd;">O: ${open.toFixed(2)}</span>`,
          `<span style="color: #4caf50;">H: ${high.toFixed(2)}</span>`,
          `<span style="color: #f44336;">L: ${low.toFixed(2)}</span>`,
          `<span style="color: #ddd;">C: ${close.toFixed(2)}</span>`,
        ];

        // Always show Bar, use '-' if we don't have the number
        if (barGroup !== undefined) {
          parts.push(`<span style="color: #ff9900;">Bar ${barGroup}</span>`);
        } else {
          parts.push(`<span style="color: #ff9900;">Bar -</span>`);
        }

        toolTip.innerHTML = parts.join(" ");
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
      // reset grouping map/state
      barGroupMap.clear();
      groupingState.count = 0;
      groupingState.bar_group_count = 0;
      groupingState.currentDay = null;
    }

    // ----------------- BEGIN: session & grouping helpers -----------------
    // small helper: parse "HH:MM" string => minutes since midnight
    function hhmmToMinutes(hhmm) {
      if (!hhmm) return null;
      const [h, m] = hhmm.split(":").map((s) => parseInt(s, 10));
      return h * 60 + (isNaN(m) ? 0 : m);
    }

    // Use Intl to convert unix seconds to local session date/time parts
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

    // grouping state per stock (no markers)
    const groupingState = {
      sessionTZ: null,
      sessionOpenMinutes: null,
      sessionCloseMinutes: null,
      count: 0, // minute count during current session
      bar_group_count: 0, // group number
      currentDay: null, // y-m-d
      tf: "5", // DEFAULT to 5m
    };

    // map for quick tooltip lookup: time -> bar_group_count
    const barGroupMap = new Map();

    // load warrant_list.json (user requested this is exposed at /warrant_list.json)
    let warrantMap = null;
    async function loadWarrantJSON() {
      try {
        const res = await fetch("/warrant_list.json");
        if (!res.ok)
          throw new Error("warrant_list.json fetch failed: " + res.status);
        const data = await res.json();
        log("Loaded warrant JSON from /warrant_list.json");
        return data;
      } catch (e) {
        warn(
          "Could not load /warrant_list.json; session grouping will be disabled for this stock."
        );
        return null;
      }
    }

    // configure session from warrant entry
    function setStockSessionConfigFromWarrant(stockId) {
      if (!warrantMap) return;
      const conf = warrantMap[stockId];
      if (!conf) {
        warn("No warrant entry for", stockId);
        groupingState.sessionTZ = null;
        groupingState.sessionOpenMinutes = null;
        groupingState.sessionCloseMinutes = null;
        return;
      }
      groupingState.sessionTZ = conf.timezone || null;
      groupingState.sessionOpenMinutes = hhmmToMinutes(conf.market_open);
      groupingState.sessionCloseMinutes = hhmmToMinutes(conf.market_close);
      log(
        "Session config:",
        stockId,
        groupingState.sessionTZ,
        groupingState.sessionOpenMinutes,
        groupingState.sessionCloseMinutes
      );
      // Reset counts but keep barGroupMap (we only clear barGroupMap on dataset switch)
      groupingState.count = 0;
      groupingState.bar_group_count = 0;
      groupingState.currentDay = null;
    }

    // check if this bar (unixSeconds) is inside trading hours (sessionTZ required)
    function isTradingHoursForBar(unixSeconds) {
      if (
        !groupingState.sessionTZ ||
        groupingState.sessionOpenMinutes == null ||
        groupingState.sessionCloseMinutes == null
      )
        return false;
      const parts = getLocalParts(unixSeconds, groupingState.sessionTZ);
      const minutes = parts.hour * 60 + parts.minute;
      return (
        minutes >= groupingState.sessionOpenMinutes &&
        minutes < groupingState.sessionCloseMinutes
      );
    }

    // main grouping function; NOTE: this will create entries in barGroupMap (no markers)
    function processBarForGrouping(barObj) {
      if (!groupingState.sessionTZ) return;
      if (!barObj || !barObj.time) return;

      const parts = getLocalParts(barObj.time, groupingState.sessionTZ);
      const dayKey = parts.ymd;

      // new day detection -> reset counts for that market day
      if (groupingState.currentDay !== dayKey) {
        groupingState.currentDay = dayKey;
        groupingState.count = 0;
        groupingState.bar_group_count = 0;
      }

      // if this bar is market open exactly, reset as Pine does
      const isOpenBar =
        parts.hour * 60 + parts.minute === groupingState.sessionOpenMinutes &&
        parts.second === 0; // optional: require exact second 0 if data has seconds

      if (isOpenBar) {
        groupingState.count = 1;
        groupingState.bar_group_count = 1;
        // For open bar we may want to register a group number — follow your Pine behaviour
        barGroupMap.set(barObj.time, groupingState.bar_group_count);
      } else if (isTradingHoursForBar(barObj.time)) {
        if (groupingState.tf === "1") {
          groupingState.count += 1;
          // For 1m timeframe, count every 5 bars => label when count % 5 == 1
          if (groupingState.count % 5 === 1) {
            groupingState.bar_group_count += 1;
          }
          // assign bar_group_count to every bar (do not skip)
          barGroupMap.set(barObj.time, groupingState.bar_group_count);
        } else {
          // For 5m timeframe, use regular count
          groupingState.count += 1;
          groupingState.bar_group_count += 1;
          // assign bar_group_count to every bar (do not skip every 2 bars)
          barGroupMap.set(barObj.time, groupingState.bar_group_count);
        }
      }
    }
    // ----------------- END: session & grouping helpers -----------------

    // helper to load history and setData on the candlestick series
    async function loadHistoryFor(stock) {
      document.getElementById("status").textContent = "Loading history...";
      try {
        // Clear previous data if switching stocks
        if (currentStock !== stock) {
          clearChartData();
          currentStock = stock;
        }

        // ensure warrant JSON loaded BEFORE configuring session & grouping
        if (!warrantMap) {
          warrantMap = await loadWarrantJSON();
        }
        // configure session params for this stock (may clear grouping)
        setStockSessionConfigFromWarrant(stock);

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

          // ------------------ grouping for history (added) ------------------
          // We use 5m by default (ignore Pine interval checking)
          groupingState.tf = "5";

          // Process grouping for entire history (one-time). This will populate barGroupMap
          barGroupMap.clear();
          groupingState.count = 0;
          groupingState.bar_group_count = 0;
          groupingState.currentDay = null;
          sortedData.forEach((bar) => {
            // Only grouping for bars that fall inside trading session for the currentStock
            processBarForGrouping(bar);
          });
          // ---------------- end grouping for history ------------------
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

                  // NOTE: Do NOT run grouping/marker creation on 'update' messages.
                  // That caused multiple markers on a still-open last bar.
                  // Grouping (and marker creation) will run for history and completed messages only.

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

                  // Run grouping now for completed bars (this will populate barGroupMap once)
                  processBarForGrouping(candleData);

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

                    // ********** RECALCULATE GROUPING WHEN REPLACING HISTORICAL DATA **********
                    // Recompute grouping across whole sortedData and repopulate barGroupMap
                    barGroupMap.clear();
                    groupingState.count = 0;
                    groupingState.bar_group_count = 0;
                    groupingState.currentDay = null;
                    sortedData.forEach((bar) => processBarForGrouping(bar));
                    // ---------------- end recalc grouping ------------------
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
        // load warrant JSON early so session config exists before history grouping
        warrantMap = await loadWarrantJSON();
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
            `<span style="color: #ff9900;">Bar -</span>`,
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
