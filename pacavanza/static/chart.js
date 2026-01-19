(() => {
  const log = (...a) => console.log("[chart]", ...a);
  const warn = (...a) => console.warn("[chart]", ...a);
  const error = (...a) => console.error("[chart]", ...a);
  const infoEl = document.getElementById("info");

  // confirm library loaded
  if (typeof LightweightCharts === "undefined") {
    error(
      "LightweightCharts global not found. Check that the exact CDN script loaded."
    );
    document.getElementById("status").textContent = "Chart lib N/A";
    return;
  }

  try {
    // create chart per v5 docs
    const chart = LightweightCharts.createChart(
      document.getElementById("chart"),
      {
        width: document.getElementById("chart").clientWidth,
        height: document.getElementById("chart").clientHeight,
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

    // ********** HIGH/LOW LINES **********
    // Dashed Blue for Yesterday
    const yesterdayHighSeries = chart.addSeries(LightweightCharts.LineSeries, {
      color: "#8929ffff",
      lineWidth: 1,
      lineStyle: LightweightCharts.LineStyle.Dashed,
      title: "Y-H",
    });
    const yesterdayLowSeries = chart.addSeries(LightweightCharts.LineSeries, {
      color: "#8929ffff",
      lineWidth: 1,
      lineStyle: LightweightCharts.LineStyle.Dashed,
      title: "Y-L",
    });

    // Solid Blue for Today
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

    // ********** OHLC TOOLTIP FOR CONTROLS LINE **********
    // Get the tooltip element
    const toolTip = document.getElementById("chart-ohlc-info");

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
    let currentInstrument = null; // Track the currently displayed instrument
    let ema20Data = new Map(); // time -> ema20 value for incremental updates

    // keep a quick cache of the latest EMA and its time to avoid sorting each tick
    let lastEMAValue = null;
    let lastEMATime = null;

    // High/Low State
    const highLowState = {
      currentDay: null, // YYYY-MM-DD
      currentHigh: -Infinity,
      currentLow: Infinity,
      yesterdayHigh: null,
      yesterdayLow: null,
    };

    // default bar interval: 5m (in seconds) — use this to compute countdown if end_time missing
    const INTERVAL_SECONDS = 5 * 60;

    // countdown timer state for status display
    let countdownTimerId = null;
    let countdownEndTime = null; // unix seconds (seconds)

    // Format mm:ss
    function formatMMSS(sec) {
      if (sec < 0) sec = 0;
      const m = Math.floor(sec / 60);
      const s = sec % 60;
      return `${m}:${String(s).padStart(2, "0")}`;
    }

    // update status element with remaining time
    function updateCountdownDisplay() {
      if (!countdownEndTime) return;
      const now = Math.floor(Date.now() / 1000);
      let rem = countdownEndTime - now;

      if (rem <= 0) {
        // final tick
        document.getElementById("status").textContent = formatMMSS(0);
        clearCountdown(); // will set fallback message
        // after finalizing a bar we *try* to ensure the next bar countdown starts if market still open
        ensureMarketCountdown();
        return;
      }
      document.getElementById("status").textContent = formatMMSS(rem);
    }

    // start countdown for a given end time (unix seconds)
    function startCountdownForBar(endUnixSeconds) {
      // If session timezone unknown, don't start countdown
      if (!groupingState.sessionTZ) {
        log("startCountdownForBar: no sessionTZ — skipping countdown");
        document.getElementById("status").textContent = "NO TZ";
        return;
      }

      const now = Math.floor(Date.now() / 1000);

      // only start if market is open now for the session
      if (!isTradingHoursForBar(now)) {
        log("startCountdownForBar: market closed now — not starting countdown");
        clearCountdown();
        document.getElementById("status").textContent = "Market closed";
        return;
      }

      // Defensive: if provided end time is not a number or already in the past,
      // fall back to start + INTERVAL_SECONDS so countdown runs reliably.
      let end = Number(endUnixSeconds);
      if (!Number.isFinite(end) || end <= now) {
        // assume 5m interval from now (endUnixSeconds might be missing or stale)
        end = now + INTERVAL_SECONDS;
      }

      // guard: if same end time already running, no-op
      if (countdownEndTime && countdownEndTime === end && countdownTimerId) {
        // already running
        return;
      }

      // set end time and (re)start timer
      clearCountdown(false); // clear any existing timer (don't override status text here)
      countdownEndTime = end;

      // // Debug log to help confirm timer started
      // log(
      //   "Starting countdown for bar, endUnix:",
      //   countdownEndTime,
      //   "now:",
      //   now
      // );

      // immediately update then schedule per-second ticks
      updateCountdownDisplay();
      countdownTimerId = setInterval(updateCountdownDisplay, 1000);
    }

    // clear countdown and optionally set a message
    function clearCountdown(setMarketMessage = true) {
      if (countdownTimerId) {
        clearInterval(countdownTimerId);
        countdownTimerId = null;
      }
      countdownEndTime = null;
      // If requested, set status based on market open/closed (but avoid overwriting countdown).
      if (setMarketMessage) {
        const now = Math.floor(Date.now() / 1000);
        if (groupingState.sessionTZ && isTradingHoursForBar(now)) {
          // keep neutral message while open — but ensureMarketCountdown will restart countdown
          document.getElementById("status").textContent = "Market open";
        } else if (groupingState.sessionTZ) {
          document.getElementById("status").textContent = "Market closed";
        } else {
          // fallback
          document.getElementById("status").textContent = "NO TZ";
        }
      }
    }

    // Ensures a countdown is running while market is open.
    // Computes the "current bar end" using lastBarTime if available (and <= now),
    // otherwise aligns current time to nearest interval boundary.
    function ensureMarketCountdown() {
      if (!groupingState.sessionTZ) {
        // no session info: just show NO TZ
        document.getElementById("status").textContent = "NO TZ";
        return;
      }
      const now = Math.floor(Date.now() / 1000);
      if (!isTradingHoursForBar(now)) {
        // market closed -> stop any countdown and show closed
        clearCountdown(true);
        document.getElementById("status").textContent = "Market closed";
        return;
      }

      // Prefer lastBarTime when it's recent (<= now)
      let barStart = null;
      if (lastBarTime && lastBarTime <= now) {
        barStart = lastBarTime;
      } else {
        // align to interval boundary (floor)
        barStart = Math.floor(now / INTERVAL_SECONDS) * INTERVAL_SECONDS;
      }

      const barEnd = barStart + INTERVAL_SECONDS;
      // Ensure end is in the future; otherwise shift forward by one interval
      if (barEnd <= now) {
        const shiftedEnd =
          Math.floor(now / INTERVAL_SECONDS) * INTERVAL_SECONDS +
          INTERVAL_SECONDS;
        startCountdownForBar(shiftedEnd);
      } else {
        startCountdownForBar(barEnd);
      }
    }

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

    // ********** HIGH/LOW CALCULATION FUNCTIONS **********
    function calculateHistoryHighLow(bars) {
      if (!bars || bars.length === 0 || !groupingState.sessionTZ) return;

      const yhData = [];
      const ylData = [];
      const thData = [];
      const tlData = [];

      // We need to group bars by day first to calculate historical dailies
      // But we also need to output sliding values for every bar.
      // Approach: linear scan
      let currentDay = null;
      let dailyHigh = -Infinity;
      let dailyLow = Infinity;

      // Store completed days stats: dayKey -> {high, low}
      const dayStats = new Map();

      // Pass 1: Identify days and their ranges (to get "yesterday" for history)
      // A bit tricky for "yesterday" if we do one pass.
      // Actually, for "Yesterday's High", on Day X, we need Day X-1's High.
      // We can maintain `prevDayHigh` and `prevDayLow` as we switch days.

      let prevDayHigh = null;
      let prevDayLow = null;

      // State for current linear scan
      let scanDay = null;
      let scanHigh = -Infinity;
      let scanLow = Infinity;

      for (const bar of bars) {
        const parts = getLocalParts(bar.time, groupingState.sessionTZ);
        const dayKey = parts.ymd;

        if (dayKey !== scanDay) {
          // New day detected
          if (scanDay !== null) {
            // Finish previous day
            prevDayHigh = scanHigh;
            prevDayLow = scanLow;
          }
          scanDay = dayKey;
          scanHigh = -Infinity;
          scanLow = Infinity;
        }

        // Update current day stats
        if (bar.high > scanHigh) scanHigh = bar.high;
        if (bar.low < scanLow) scanLow = bar.low;

        // Push data points
        // Yesterday's lines (Dashed)
        if (prevDayHigh !== null) {
          yhData.push({ time: bar.time, value: prevDayHigh });
          ylData.push({ time: bar.time, value: prevDayLow });
        }

        // Today's lines (Solid) - Running high/low
        thData.push({ time: bar.time, value: scanHigh });
        tlData.push({ time: bar.time, value: scanLow });
      }

      // Update series
      yesterdayHighSeries.setData(yhData);
      yesterdayLowSeries.setData(ylData);
      todayHighSeries.setData(thData);
      todayLowSeries.setData(tlData);

      // Update state for incremental updates
      highLowState.currentDay = scanDay;
      highLowState.currentHigh = scanHigh;
      highLowState.currentLow = scanLow;
      highLowState.yesterdayHigh = prevDayHigh;
      highLowState.yesterdayLow = prevDayLow;
    }

    function updateHighLowIncremental(bar) {
      if (!groupingState.sessionTZ) return;

      const parts = getLocalParts(bar.time, groupingState.sessionTZ);
      const dayKey = parts.ymd;

      // Check for new day
      if (dayKey !== highLowState.currentDay) {
        // Close out old day
        if (highLowState.currentDay !== null) {
          highLowState.yesterdayHigh = highLowState.currentHigh;
          highLowState.yesterdayLow = highLowState.currentLow;
        }
        // Init new day
        highLowState.currentDay = dayKey;
        highLowState.currentHigh = -Infinity;
        highLowState.currentLow = Infinity;
      }

      // Update current day running stats
      // Note: bar can be an update (same time) or new bar (new time).
      // For proper "Running High/Low", we need to distinguish finalized bars vs updates?
      // Actually tracking "Running High of Session" is monotonic increasing (for High)
      // unless we are correcting a bad tick.
      // But we just receive `bar.high` and `bar.low`.
      // If this is a new tick for SAME bar, we might need to be careful if we processed it before?
      // Logic: simplified -> Global session High is max(currentSessionHigh, bar.high).
      // We don't support "downgrading" a high if a trade is cancelled, but that's rare.

      // Wait, if we receive an update for a bar, that bar's high might increase.
      // If we move to a NEW bar, we continue accumulating.
      // The issue is if we process the SAME bar multiple times, max() is safe.

      if (bar.high > highLowState.currentHigh)
        highLowState.currentHigh = bar.high;
      if (bar.low < highLowState.currentLow) highLowState.currentLow = bar.low;

      // Update Series
      if (highLowState.yesterdayHigh !== null) {
        yesterdayHighSeries.update({
          time: bar.time,
          value: highLowState.yesterdayHigh,
        });
        yesterdayLowSeries.update({
          time: bar.time,
          value: highLowState.yesterdayLow,
        });
      }

      todayHighSeries.update({
        time: bar.time,
        value: highLowState.currentHigh,
      });
      todayLowSeries.update({ time: bar.time, value: highLowState.currentLow });
    }

    async function fetchHistory(instrument) {
      const res = await fetch(
        `/history/${encodeURIComponent(instrument)}?limit=3600`
      );
      if (!res.ok) throw new Error("history fetch failed: " + res.status);
      return await res.json();
    }

    // Clear all chart data
    function clearChartData() {
      currentData.clear();
      lastBarTime = null;
      ema20Data.clear(); // Clear EMA20 data when switching instruments
      lastEMAValue = null;
      lastEMATime = null;
      candleSeries.setData([]);
      ema20Series.setData([]); // Only clear EMA20 series
      yesterdayHighSeries.setData([]);
      yesterdayLowSeries.setData([]);
      todayHighSeries.setData([]);
      todayLowSeries.setData([]);
      // Reset High/Low State
      highLowState.currentDay = null;
      highLowState.currentHigh = -Infinity;
      highLowState.currentLow = Infinity;
      highLowState.yesterdayHigh = null;
      highLowState.yesterdayLow = null;
      // reset grouping map/state
      barGroupMap.clear();
      groupingState.count = 0;
      groupingState.bar_group_count = 0;
      groupingState.currentDay = null;
      // stop any countdown
      clearCountdown(true);
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

    // grouping state per instrument (no markers)
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

    // load instrument_list.json (user requested this is exposed at /instrument_list.json)
    let instrumentMap = null; // flattened map: ID -> info
    let hierarchicalMap = null; // raw nested map: Asset -> info

    function flattenInstruments(data) {
      const flat = {};
      for (const assetKey in data) {
        const assetData = data[assetKey];
        const common = {};
        // Inherit metadata if present
        if (assetData.timezone) common.timezone = assetData.timezone;
        if (assetData.market_open) common.market_open = assetData.market_open;
        if (assetData.market_close)
          common.market_close = assetData.market_close;

        for (const childKey in assetData) {
          const val = assetData[childKey];
          // Simple check for instrument dict
          if (typeof val === "object" && val !== null && val.orderbookId) {
            const merged = Object.assign({}, val);
            // merge default metadata
            for (const k in common) {
              if (!merged[k]) merged[k] = common[k];
            }
            flat[childKey] = merged;
          }
        }
      }
      return flat;
    }

    // ---------------- NEW: populate selectors with hierarchical data ------------
    async function setupSelectors() {
      const assetSel = document.getElementById("asset");
      const instrSel = document.getElementById("instrument");
      if (!assetSel || !instrSel) return;

      try {
        if (!hierarchicalMap || !instrumentMap) {
          await loadInstrumentJSON();
        }

        const assets = Object.keys(hierarchicalMap);
        if (assets.length === 0) {
          const opt = document.createElement("option");
          opt.textContent = "(no assets)";
          assetSel.appendChild(opt);
          return;
        }

        // Helper to populate instrument select based on current asset
        function updateInstrumentOptions(assetKey) {
          instrSel.innerHTML = "";
          const assetData = hierarchicalMap[assetKey];
          if (!assetData) return;

          // Filter child keys that are present in our flattened map (i.e. legitimate instruments)
          // or perform the same check as flattenInstruments
          const instrKeys = Object.keys(assetData).filter((k) => {
            const val = assetData[k];
            return typeof val === "object" && val !== null && val.orderbookId;
          });

          instrKeys.forEach((k) => {
            const opt = document.createElement("option");
            opt.value = k;
            // Use name if available, else key
            opt.textContent = assetData[k].name || k;
            instrSel.appendChild(opt);
          });

          if (instrKeys.length > 0) {
            instrSel.value = instrKeys[0];
          }
        }

        // Populate Assets
        assetSel.innerHTML = "";
        assets.forEach((a) => {
          const opt = document.createElement("option");
          opt.value = a;
          opt.textContent = a;
          assetSel.appendChild(opt);
        });

        // Set initial state
        if (assets.length > 0) {
          assetSel.value = assets[0];
          updateInstrumentOptions(assets[0]);
        }

        // Event Listeners
        assetSel.addEventListener("change", () => {
          updateInstrumentOptions(assetSel.value);
          // Trigger load for the new first instrument
          const newInstr = instrSel.value;
          if (newInstr) loadHistoryFor(newInstr);
        });

        instrSel.addEventListener("change", () => {
          const newInstr = instrSel.value;
          if (newInstr) loadHistoryFor(newInstr);
        });
      } catch (e) {
        warn("setupSelectors failed:", e);
      }
    }
    // -------------------------------------------------------------------------------

    async function loadInstrumentJSON() {
      try {
        if (instrumentMap && hierarchicalMap)
          return { flat: instrumentMap, hierarchical: hierarchicalMap };
        const res = await fetch("/instrument_list.json");
        if (!res.ok)
          throw new Error("instrument_list.json fetch failed: " + res.status);
        const data = await res.json();
        log("Loaded instrument JSON from /instrument_list.json");
        hierarchicalMap = data;
        instrumentMap = flattenInstruments(data);
        return { flat: instrumentMap, hierarchical: hierarchicalMap };
      } catch (e) {
        warn(
          "Could not load /instrument_list.json; session grouping will be disabled for this instrument."
        );
        return null;
      }
    }

    // configure session from instrument entry
    function setInstrumentSessionConfigFromInstrument(instrumentId) {
      if (!instrumentMap) return;
      const conf = instrumentMap[instrumentId];
      if (!conf) {
        warn("No instrument entry for", instrumentId);
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
        instrumentId,
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

    // ----------------- NEW: recompute grouping for a single session day -------------
    // This helper computes bar_group_count for all bars in `barsForDay` (chronological)
    // and returns a Map(time -> group). It uses identical grouping rules but with
    // local counters so it doesn't depend on global groupingState being up-to-date.
    function computeGroupingForBars(
      barsForDay,
      sessionTZ,
      sessionOpenMin,
      sessionCloseMin,
      tf = "5"
    ) {
      const map = new Map();
      let count = 0;
      let bar_group_count = 0;
      // We expect barsForDay to be chronological (sorted by time)
      for (const bar of barsForDay) {
        const parts = getLocalParts(bar.time, sessionTZ);
        const isOpenBar =
          parts.hour * 60 + parts.minute === sessionOpenMin &&
          parts.second === 0;
        if (isOpenBar) {
          count = 1;
          bar_group_count = 1;
          map.set(bar.time, bar_group_count);
          continue;
        }
        const minutes = parts.hour * 60 + parts.minute;
        if (minutes >= sessionOpenMin && minutes < sessionCloseMin) {
          if (tf === "1") {
            count += 1;
            if (count % 5 === 1) {
              bar_group_count += 1;
            }
            map.set(bar.time, bar_group_count);
          } else {
            count += 1;
            bar_group_count += 1;
            map.set(bar.time, bar_group_count);
          }
        } else {
          // outside trading hours: don't set group (no entry)
        }
      }
      return map;
    }

    // Recompute grouping for the day of unixSeconds 't' using data from currentData
    // Only processes bars for the same ymd (session tz) as t, which keeps it fast.
    function recomputeGroupingForDayOf(t) {
      if (!groupingState.sessionTZ) return;
      const parts = getLocalParts(t, groupingState.sessionTZ);
      const dayKey = parts.ymd;
      // collect bars from currentData that fall into this day (in session timezone)
      const allBars = Array.from(currentData.values()).sort(
        (a, b) => a.time - b.time
      );
      const barsForDay = [];
      for (const b of allBars) {
        const p = getLocalParts(b.time, groupingState.sessionTZ);
        if (p.ymd === dayKey) barsForDay.push(b);
      }
      if (barsForDay.length === 0) return;
      // compute new grouping map for that day
      const newMap = computeGroupingForBars(
        barsForDay,
        groupingState.sessionTZ,
        groupingState.sessionOpenMinutes,
        groupingState.sessionCloseMinutes,
        groupingState.tf
      );
      // merge into global barGroupMap (overwrite entries for that day)
      for (const [time, grp] of newMap.entries()) {
        barGroupMap.set(time, grp);
      }
      // also update global groupingState so subsequent incremental processing continues from latest counts:
      // find last entry from newMap to update groupingState.currentDay/count/bar_group_count
      const times = Array.from(newMap.keys()).sort((a, b) => a - b);
      if (times.length > 0) {
        const lastTime = times[times.length - 1];
        groupingState.currentDay = dayKey;
        groupingState.bar_group_count = newMap.get(lastTime);
        // compute count: number of bars in session processed so far (approximate)
        // Count bars within session up to lastTime
        let c = 0;
        for (const b of barsForDay) {
          const p = getLocalParts(b.time, groupingState.sessionTZ);
          const minutes = p.hour * 60 + p.minute;
          if (
            minutes >= groupingState.sessionOpenMinutes &&
            minutes < groupingState.sessionCloseMinutes
          ) {
            c += 1;
            if (b.time === lastTime) break;
          }
        }
        groupingState.count = c;
      }
    }
    // ----------------- END recompute helper --------------------------------------

    // grouping state helpers end
    // ----------------- END: session & grouping helpers -----------------

    // helper to load history and setData on the candlestick series
    async function loadHistoryFor(instrument) {
      document.getElementById("status").textContent = "Loading history...";
      try {
        // Clear previous data if switching instruments
        if (currentInstrument !== instrument) {
          clearChartData();
          currentInstrument = instrument;
        }

        // ensure instrument JSON loaded BEFORE configuring session & grouping
        if (!instrumentMap) {
          instrumentMap = await loadInstrumentJSON();
        }
        // configure session params for this instrument (may clear grouping)
        setInstrumentSessionConfigFromInstrument(instrument);

        const bars = await fetchHistory(instrument);
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
        ema20Data.clear(); // Clear EMA20 data for new instrument
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

          // ********** CALCULATE HIGH/LOW LINES **********
          calculateHistoryHighLow(sortedData);

          // ------------------ grouping for history (added) ------------------
          // We use 5m by default (ignore Pine interval checking)
          groupingState.tf = "5";

          // Process grouping for entire history (one-time). This will populate barGroupMap
          barGroupMap.clear();
          groupingState.count = 0;
          groupingState.bar_group_count = 0;
          groupingState.currentDay = null;
          sortedData.forEach((bar) => {
            // Only grouping for bars that fall inside trading session for the currentInstrument
            processBarForGrouping(bar);
          });
          // ---------------- end grouping for history ------------------
        }

        // After loading history, ensure countdown if market open (countdown is default display when open)
        ensureMarketCountdown();

        log("History loaded for", instrument, barData.length);
        return barData.length;
      } catch (err) {
        error("History load failed:", err);
        document.getElementById("status").textContent =
          "Failed to load history";
        throw err;
      }
    }

    // NEW: auto-reload handlers are now inside setupSelectors
    // Removed old independent event listener

    // WebSocket for streaming updates (updates candlestick points + EMAs)
    function setupWS() {
      try {
        const wsUrl = `ws://${window.location.host}/ws`;
        log("Connecting websocket to", wsUrl);
        const ws = new WebSocket(wsUrl);

        ws.onopen = () => {
          log("WS connected");
          // If market open, ensure countdown runs; otherwise show connected
          ensureMarketCountdown();
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

            // CRITICAL FIX: Only process messages for the currently displayed instrument
            if (!msg || msg.instrument !== currentInstrument) {
              // Log for debugging (optional)
              if (msg && msg.instrument) {
                // log(
                //   `Ignoring message for instrument: ${msg.instrument}, current: ${currentInstrument}`
                // );
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

                  // ********** INCREMENTAL HIGH/LOW UPDATE **********
                  updateHighLowIncremental(candleData);

                  // NOTE: Do NOT run grouping/marker creation on 'update' messages.
                  // That caused multiple markers on a still-open last bar.
                  // Grouping will run for history and completed messages only.

                  // --- START countdown logic ---
                  // Determine bar end time: prefer provided end_time if available
                  let barEndUnix = null;
                  if (b.end_time) {
                    try {
                      barEndUnix = isoToLWTime(b.end_time);
                    } catch (err) {
                      barEndUnix = t + INTERVAL_SECONDS;
                    }
                  } else {
                    barEndUnix = t + INTERVAL_SECONDS;
                  }

                  // Start countdown only if market is open (and we have sessionTZ)
                  const now = Math.floor(Date.now() / 1000);
                  if (groupingState.sessionTZ && isTradingHoursForBar(now)) {
                    // prefer the provided end time but ensure fallback to ensureMarketCountdown logic if needed
                    startCountdownForBar(barEndUnix);
                  } else if (groupingState.sessionTZ) {
                    clearCountdown(true);
                    document.getElementById("status").textContent =
                      "Market closed";
                  }
                  // --- END countdown logic ---

                  if (t > lastBarTime) {
                    // process grouping for this *new* bar so bar_group_count advances
                    try {
                      processBarForGrouping(candleData);
                    } catch (err) {
                      log("processBarForGrouping failed for update:", err);
                    }

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
              // If the frontend already has identical data for this completed bar, skip processing
              const existing = currentData.get(t);
              if (
                existing &&
                existing.open === candleData.open &&
                existing.high === candleData.high &&
                existing.low === candleData.low &&
                existing.close === candleData.close
              ) {
                // log(`Ignoring Duplicate completed bar for ${t}`);
                // still update lastBarTime if necessary
                if (lastBarTime === null || t > lastBarTime) lastBarTime = t;
                // completed -> stop countdown (bar finalized)
                clearCountdown(true);
                // after a completed bar, ensure the next bar countdown is started if market still open
                ensureMarketCountdown();
                return;
              }

              // ----- CHANGED: always integrate completed bars (do NOT skip) -----
              // We will attempt a lightweight update(), and on API refusal we rebuild the dataset.
              currentData.set(t, candleData);
              try {
                // Try to update the series in-place (fast path)
                candleSeries.update(candleData);

                // ********** INCREMENTAL EMA20 UPDATE FOR COMPLETED BARS **********
                updateEMA20Incremental(close, t);

                // ********** INCREMENTAL HIGH/LOW UPDATE (Completed) **********
                updateHighLowIncremental(candleData);

                // Recompute grouping for that day's session using currentData snapshot.
                // This fills barGroupMap for any completed bars that were missed.
                recomputeGroupingForDayOf(t);

                // completed -> stop countdown (bar finalized)
                clearCountdown(true);
                // after integrating, ensure next bar countdown (if market still open)
                ensureMarketCountdown();

                // Update lastBarTime if this is actually the newest bar
                if (lastBarTime === null || t > lastBarTime) {
                  lastBarTime = t;
                  log(`Completed bar - updated lastBarTime to: ${lastBarTime}`);
                } else {
                  log(`Integrated completed historical bar at ${t}`);
                }
              } catch (e) {
                // If the chart refuses to update an older datapoint we rebuild the dataset (fallback).
                if (
                  e.message &&
                  e.message.includes("Cannot update oldest data")
                ) {
                  log(
                    `Completed bar is historical (chart refused in-place update), replacing data set for ${t}`
                  );
                  // Ensure the currentData contains the completed bar
                  currentData.delete(t);
                  currentData.set(t, candleData);
                  // Recreate the entire dataset (sorted) and setData
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

                  // ********** RECALCULATE HIGH/LOW ON HISTORICAL REPLACE **********
                  calculateHistoryHighLow(sortedData);

                  // ********** RECALCULATE GROUPING WHEN REPLACING HISTORICAL DATA **********
                  // Recompute grouping across whole sortedData and repopulate barGroupMap
                  barGroupMap.clear();
                  groupingState.count = 0;
                  groupingState.bar_group_count = 0;
                  groupingState.currentDay = null;
                  sortedData.forEach((bar) => processBarForGrouping(bar));
                  // ---------------- end recalc grouping ------------------

                  // completed -> stop countdown (we've rebuilt)
                  clearCountdown(true);
                  // ensure next bar countdown if market open
                  ensureMarketCountdown();
                } else {
                  throw e;
                }
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
                          // log(
                          //   `Ignoring backend EMA for time ${emaTime} (not newer than lastEMATime ${lastEMATime})`
                          // );
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
    // Immediately load history for the currently selected instrument (no button click).
    // This happens once during initialization.
    (async () => {
      // Setup UI with Asset -> Instrument cascading
      await setupSelectors();

      const initialInstrument = document.getElementById("instrument").value;
      if (initialInstrument) {
        currentInstrument = initialInstrument;
        try {
          // Already loaded in setupSelectors, but ensure logic consistency
          // if we need to call loadHistoryFor explicitly
          await loadHistoryFor(initialInstrument);
        } catch (e) {
          // log
        }
      } else {
        warn("No initial instrument found.");
      }

      // Finally start WS
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
      // ensure countdown display state matches market/open after WS start
      ensureMarketCountdown();
    })();
    // ---------------- end auto-load ----------------
  } catch (e) {
    error("Chart init failed:", e);
    document.getElementById("status").textContent = "Chart init failed";
  }
})();
