// Assumes DOM elements and styling are provided by chart.html.

(async function () {
  const log = (...a) => console.log("[trade]", ...a);
  const warn = (...a) => console.warn("[trade]", ...a);
  const error = (...a) => console.error("[trade]", ...a);

  const instrumentSelect = document.getElementById("instrument");
  const pctInput = document.getElementById("order-percentage");
  const infoEl = document.getElementById("trade-info");

  const btnMarketBuy = document.getElementById("btn-market-buy");
  const btnMarketSell = document.getElementById("btn-market-sell");
  const btnBuyStop = document.getElementById("btn-buy-stop");
  const btnCancelBuyStop = document.getElementById("btn-cancel-buy-stop");
  const btnLateBuyStop = document.getElementById("btn-late-buy-stop");
  const btnSellStop = document.getElementById("btn-sell-stop");
  const btnCancelSellStop = document.getElementById("btn-cancel-sell-stop");
  const btnLateSellStop = document.getElementById("btn-late-sell-stop");
  const btnDeleteStopLosses = document.getElementById("btn-delete-stop-losses");

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

  // button handlers (do minimal validation)
  function getInstrumentAndPercentage() {
    const instrumentId = instrumentSelect ? instrumentSelect.value : null;
    const percentage = pctInput ? parseFloat(pctInput.value) : null;
    if (!instrumentId) {
      error("Select instrument");
      showInfo("Select instrument");
      return null;
    }
    if (!percentage || Number.isNaN(percentage) || percentage <= 0) {
      error("Set percentage > 0");
      showInfo("Set percentage > 0");
      return null;
    }
    return { instrumentId, percentage };
  }

  if (btnMarketBuy) {
    btnMarketBuy.addEventListener("click", async () => {
      const p = getInstrumentAndPercentage();
      if (!p) return;
      await callTrade("/trade/market_buy", {
        instrumentId: p.instrumentId,
        percentage: p.percentage,
      });
    });
  }

  if (btnMarketSell) {
    btnMarketSell.addEventListener("click", async () => {
      const p = getInstrumentAndPercentage();
      if (!p) return;
      await callTrade("/trade/market_sell", {
        instrumentId: p.instrumentId,
      });
    });
  }

  if (btnBuyStop) {
    btnBuyStop.addEventListener("click", async () => {
      const p = getInstrumentAndPercentage();
      if (!p) return;
      // scheduled: server waits for next completed bar
      await callTrade("/trade/buy_stop", {
        instrumentId: p.instrumentId,
        percentage: p.percentage,
      });
    });
  }

  if (btnCancelBuyStop) {
    btnCancelBuyStop.addEventListener("click", async () => {
      const p = getInstrumentAndPercentage();
      if (!p) return;
      // scheduled: server waits for next completed bar
      await callTrade("/trade/cancel_buy_stop", {
        instrumentId: p.instrumentId,
      });
    });
  }

  if (btnLateBuyStop) {
    btnLateBuyStop.addEventListener("click", async () => {
      const p = getInstrumentAndPercentage();
      if (!p) return;
      await callTrade("/trade/late_buy_stop", {
        instrumentId: p.instrumentId,
        percentage: p.percentage,
      });
    });
  }

  if (btnSellStop) {
    btnSellStop.addEventListener("click", async () => {
      const p = getInstrumentAndPercentage();
      if (!p) return;
      await callTrade("/trade/sell_stop", {
        instrumentId: p.instrumentId,
      });
    });
  }

  if (btnCancelSellStop) {
    btnCancelSellStop.addEventListener("click", async () => {
      const p = getInstrumentAndPercentage();
      if (!p) return;
      await callTrade("/trade/cancel_sell_stop", {
        instrumentId: p.instrumentId,
      });
    });
  }

  if (btnLateSellStop) {
    btnLateSellStop.addEventListener("click", async () => {
      const p = getInstrumentAndPercentage();
      if (!p) return;
      await callTrade("/trade/late_sell_stop", {
        instrumentId: p.instrumentId,
      });
    });
  }

  if (btnDeleteStopLosses) {
    btnDeleteStopLosses.addEventListener("click", async () => {
      const p = getInstrumentAndPercentage();
      if (!p) return;
      await callTrade("/trade/delete_stop_losses", {
        instrumentId: p.instrumentId,
      });
    });
  }
})();
