// Assumes DOM elements and styling are provided by chart.html.

(async function () {
  const log = (...a) => console.log("[trade]", ...a);
  const warn = (...a) => console.warn("[trade]", ...a);
  const error = (...a) => console.error("[trade]", ...a);

  // Global state provided by chart.js
  // window.PAC_TRADING_STATE = { longInstrument: "...", shortInstrument: "..." }

  const infoEl = document.getElementById("trade-info");

  function showInfo(txt, timeout = 4000) {
    if (!infoEl) return;
    infoEl.textContent = txt;
    if (timeout) {
      setTimeout(() => {
        if (infoEl.textContent === txt) infoEl.textContent = "";
      }, timeout);
    }
  }

  async function callTrade(endpoint, payload) {
    try {
      const res = await fetch(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const text = await res.text();
      if (!res.ok) {
        let parsed = text;
        try {
          parsed = JSON.parse(text);
        } catch (e) {}
        showInfo("Trade failed: " + (res.status || ""));
        error("Trade failed", res.status, parsed);
        return null;
      }
      const j = JSON.parse(text);
      const summary = j.order || j.orders || j.note || j;
      showInfo("OK: " + JSON.stringify(summary));
      log("OK: " + JSON.stringify(summary));
      return j;
    } catch (e) {
      error("Trade request failed", e);
      showInfo("Trade request failed");
      return null;
    }
  }

  function getSideState(side) {
    if (!window.PAC_TRADING_STATE) {
      error("PAC_TRADING_STATE unavailable");
      return null;
    }
    if (side === "long")
      return { instrumentId: window.PAC_TRADING_STATE.longInstrument };
    if (side === "short")
      return { instrumentId: window.PAC_TRADING_STATE.shortInstrument };
    return null;
  }

  function getInstrumentAndPercentage(side) {
    const state = getSideState(side);
    if (!state || !state.instrumentId) {
      showInfo(`No ${side} instrument selected`);
      return null;
    }

    const pctInput = document.getElementById(`order-percentage-${side}`);
    const percentage = pctInput ? parseFloat(pctInput.value) : null;

    if (!percentage || Number.isNaN(percentage) || percentage <= 0) {
      error("Set percentage > 0");
      showInfo("Set percentage > 0");
      return null;
    }
    return { instrumentId: state.instrumentId, percentage };
  }

  // Generic binder for a specific side ("long" or "short")
  function bindSide(side) {
    const s = side; // capture closure

    const actions = [
      {
        id: `btn-market-buy-${s}`,
        endpoint: "/trade/market_buy",
        needsPct: true,
      },
      {
        id: `btn-market-sell-${s}`,
        endpoint: "/trade/market_sell",
        needsPct: false,
      },
      { id: `btn-buy-stop-${s}`, endpoint: "/trade/buy_stop", needsPct: true },
      {
        id: `btn-cancel-buy-stop-${s}`,
        endpoint: "/trade/cancel_buy_stop",
        needsPct: false,
      },
      {
        id: `btn-late-buy-stop-${s}`,
        endpoint: "/trade/late_buy_stop",
        needsPct: true,
      },
      {
        id: `btn-sell-stop-${s}`,
        endpoint: "/trade/sell_stop",
        needsPct: false,
      }, // Wait, sell stop usually needs no params? Old code: { instrumentId }
      {
        id: `btn-cancel-sell-stop-${s}`,
        endpoint: "/trade/cancel_sell_stop",
        needsPct: false,
      },
      {
        id: `btn-late-sell-stop-${s}`,
        endpoint: "/trade/late_sell_stop",
        needsPct: false,
      },
      {
        id: `btn-delete-stop-losses-${s}`,
        endpoint: "/trade/delete_stop_losses",
        needsPct: false,
      },
    ];

    actions.forEach((act) => {
      const btn = document.getElementById(act.id);
      if (btn) {
        btn.addEventListener("click", async () => {
          const state = getSideState(s);
          if (!state || !state.instrumentId) {
            showInfo(`No ${s} instrument`);
            return;
          }

          const payload = { instrumentId: state.instrumentId };

          if (act.needsPct) {
            const p = getInstrumentAndPercentage(s);
            if (!p) return;
            payload.percentage = p.percentage;
          }

          await callTrade(act.endpoint, payload);
        });
      }
    });
  }

  // Bind controls for both sides
  bindSide("long");
  bindSide("short");
})();
