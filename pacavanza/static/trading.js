// trading.js (slim)
// Assumes DOM elements and styling are provided by chart.html.
// Responsibilities:
// - load warrant_list.json for defaults
// - set order volume default when stock selection changes
// - handle button clicks to call trade endpoints and show results in #trade-info

(async function () {
  // load warrant list (for order_volume defaults)
  let warrantList = {};
  try {
    const r = await fetch("/warrant_list.json");
    if (r.ok) warrantList = await r.json();
  } catch (e) {
    console.warn("Could not load warrant_list.json", e);
  }

  const stockSelect = document.getElementById("stock");
  const volInput = document.getElementById("order-volume");
  const infoEl = document.getElementById("trade-info");

  const btnMarketBuy = document.getElementById("btn-market-buy");
  const btnMarketSell = document.getElementById("btn-market-sell");
  const btnBuyStop = document.getElementById("btn-buy-stop");
  const btnLateBuyStop = document.getElementById("btn-late-buy-stop");
  const btnSellStop = document.getElementById("btn-sell-stop");
  const btnLateSellStop = document.getElementById("btn-late-sell-stop");

  function showInfo(txt, timeout = 4000) {
    if (!infoEl) return;
    infoEl.textContent = txt;
    if (timeout) {
      setTimeout(() => {
        if (infoEl.textContent === txt) infoEl.textContent = "";
      }, timeout);
    }
  }

  // default volume update based on selected instrument
  function updateVolumeDefault() {
    if (!stockSelect || !volInput) return;
    const inst = stockSelect.value;
    if (!inst) return;
    const info = warrantList[inst];
    if (info && info.order_volume !== undefined) {
      volInput.value = info.order_volume;
    }
  }

  if (stockSelect) {
    stockSelect.addEventListener("change", updateVolumeDefault);
    // in case options already set
    updateVolumeDefault();
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
        console.error("Trade failed", res.status, parsed);
        return null;
      }
      const j = JSON.parse(text);
      const summary = j.order || j.orders || j.note || j;
      showInfo("OK: " + JSON.stringify(summary));
      return j;
    } catch (e) {
      console.error("Trade request failed", e);
      showInfo("Trade request failed");
      return null;
    }
  }

  // button handlers (do minimal validation)
  function getInstrumentAndVolume() {
    const instrumentId = stockSelect ? stockSelect.value : null;
    const volume = volInput ? parseFloat(volInput.value) : null;
    if (!instrumentId) {
      showInfo("Select instrument");
      return null;
    }
    if (!volume || Number.isNaN(volume) || volume <= 0) {
      showInfo("Set volume > 0");
      return null;
    }
    return { instrumentId, volume };
  }

  if (btnMarketBuy) {
    btnMarketBuy.addEventListener("click", async () => {
      const p = getInstrumentAndVolume();
      if (!p) return;
      await callTrade("/trade/market_buy", {
        instrumentId: p.instrumentId,
        volume: p.volume,
      });
    });
  }

  if (btnMarketSell) {
    btnMarketSell.addEventListener("click", async () => {
      const p = getInstrumentAndVolume();
      if (!p) return;
      await callTrade("/trade/market_sell", {
        instrumentId: p.instrumentId,
        volume: p.volume,
      });
    });
  }

  if (btnBuyStop) {
    btnBuyStop.addEventListener("click", async () => {
      const p = getInstrumentAndVolume();
      if (!p) return;
      // scheduled: server waits for next completed bar
      await callTrade("/trade/buy_stop", {
        instrumentId: p.instrumentId,
        volume: p.volume,
      });
    });
  }

  if (btnLateBuyStop) {
    btnLateBuyStop.addEventListener("click", async () => {
      const p = getInstrumentAndVolume();
      if (!p) return;
      await callTrade("/trade/late_buy_stop", {
        instrumentId: p.instrumentId,
        volume: p.volume,
      });
    });
  }

  if (btnSellStop) {
    btnSellStop.addEventListener("click", async () => {
      const p = getInstrumentAndVolume();
      if (!p) return;
      await callTrade("/trade/sell_stop", {
        instrumentId: p.instrumentId,
        volume: p.volume,
      });
    });
  }

  if (btnLateSellStop) {
    btnLateSellStop.addEventListener("click", async () => {
      const p = getInstrumentAndVolume();
      if (!p) return;
      await callTrade("/trade/late_sell_stop", {
        instrumentId: p.instrumentId,
        volume: p.volume,
      });
    });
  }
})();
