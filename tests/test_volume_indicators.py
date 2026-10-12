import unittest
from datetime import datetime, timezone
from pacavanza.indicators.indicators import (
    calculate_volume_sma,
    calculate_rvol,
    incremental_rvol_update,
    calculate_rvol_tod,
    calculate_cumulative_rvol,
    incremental_cumulative_rvol_update,
)


class TestVolumeIndicators(unittest.TestCase):
    def test_calculate_volume_sma(self):
        # Empty list
        self.assertEqual(calculate_volume_sma([]), [])

        # Small list (< 20 items)
        vols = [100.0, 200.0, 300.0]
        sma = calculate_volume_sma(vols, length=20)
        self.assertEqual(len(sma), 3)
        self.assertAlmostEqual(sma[0], 100.0)
        self.assertAlmostEqual(sma[1], 150.0)
        self.assertAlmostEqual(sma[2], 200.0)

        # Exact 20 items with constant volume
        vols_const = [500.0] * 25
        sma_const = calculate_volume_sma(vols_const, length=20)
        self.assertEqual(len(sma_const), 25)
        self.assertAlmostEqual(sma_const[-1], 500.0)

    def test_calculate_rvol(self):
        # Empty list
        self.assertEqual(calculate_rvol([]), [])

        # First bar defaults to 1.0
        self.assertEqual(calculate_rvol([500.0]), [1.0])

        # Normal volume series: prior 20 bars average 100, current bar has 200 -> RVOL = 2.0
        prior = [100.0] * 20
        vols = prior + [200.0]
        rvol = calculate_rvol(vols, length=20)
        self.assertEqual(len(rvol), 21)
        self.assertAlmostEqual(rvol[-1], 2.0)

        # 1.5x breakout volume
        vols_breakout = prior + [150.0]
        rvol_breakout = calculate_rvol(vols_breakout, length=20)
        self.assertAlmostEqual(rvol_breakout[-1], 1.5)

        # Subdued volume (0.5x)
        vols_subdued = prior + [50.0]
        rvol_subdued = calculate_rvol(vols_subdued, length=20)
        self.assertAlmostEqual(rvol_subdued[-1], 0.5)

    def test_incremental_rvol_update(self):
        # Empty prior returns 1.0
        self.assertEqual(incremental_rvol_update(100.0, []), 1.0)

        # Prior average is 100, current volume is 250 -> 2.5x
        prev = [100.0] * 20
        self.assertAlmostEqual(incremental_rvol_update(250.0, prev, length=20), 2.5)

        # Zero prior volume handles gracefully
        self.assertEqual(incremental_rvol_update(100.0, [0.0, 0.0]), 1.0)

    def test_calculate_rvol_tod(self):
        # Test time of day volume calculation across multiple days
        bars = [
            # Day 1: 09:00 (100), 09:05 (200)
            {"start_time": "2026-10-01T07:00:00+00:00", "volume": 100.0},
            {"start_time": "2026-10-01T07:05:00+00:00", "volume": 200.0},
            # Day 2: 09:00 (300), 09:05 (200)
            {"start_time": "2026-10-02T07:00:00+00:00", "volume": 300.0},
            {"start_time": "2026-10-02T07:05:00+00:00", "volume": 200.0},
        ]
        # Average for 09:00 is (100 + 300) / 2 = 200.0
        # Average for 09:05 is (200 + 200) / 2 = 200.0
        tod_rvol = calculate_rvol_tod(bars, timezone_str="Europe/Stockholm")
        self.assertEqual(len(tod_rvol), 4)
        # Day 1 09:00 has 100 / 200 = 0.5
        self.assertAlmostEqual(tod_rvol[0], 0.5)
        # Day 2 09:00 has 300 / 200 = 1.5
        self.assertAlmostEqual(tod_rvol[2], 1.5)
        # Day 1 09:05 has 200 / 200 = 1.0
        self.assertAlmostEqual(tod_rvol[1], 1.0)

    def test_calculate_cumulative_rvol(self):
        # Test cumulative volume at time (TradingView Rel vol anchor=1D mode=Cumulative)
        bars = [
            # Day 1: 09:00 (100, cum=100), 09:05 (200, cum=300)
            {"start_time": "2026-10-01T07:00:00+00:00", "volume": 100.0},
            {"start_time": "2026-10-01T07:05:00+00:00", "volume": 200.0},
            # Day 2: 09:00 (200, cum=200), 09:05 (400, cum=600)
            {"start_time": "2026-10-02T07:00:00+00:00", "volume": 200.0},
            {"start_time": "2026-10-02T07:05:00+00:00", "volume": 400.0},
        ]
        # Day 1 has no prior days -> default 1.0
        # Day 2 09:00 has cum=200, Day 1 09:00 has cum=100 -> ratio = 2.0
        # Day 2 09:05 has cum=600, Day 1 09:05 has cum=300 -> ratio = 2.0
        cum_rvol = calculate_cumulative_rvol(
            bars, length_days=20, timezone_str="Europe/Stockholm"
        )
        self.assertEqual(len(cum_rvol), 4)
        self.assertAlmostEqual(cum_rvol[0], 1.0)
        self.assertAlmostEqual(cum_rvol[1], 1.0)
        self.assertAlmostEqual(cum_rvol[2], 2.0)
        self.assertAlmostEqual(cum_rvol[3], 2.0)

    def test_incremental_cumulative_rvol_update(self):
        prior_map = {
            "2026-10-01": {"09:00": 100.0, "09:05": 300.0},
            "2026-10-02": {"09:00": 200.0, "09:05": 500.0},
        }
        # Average prior at 09:00 is (100 + 200) / 2 = 150.0
        # Today cumulative at 09:00 is 300.0 -> ratio = 2.0
        ratio = incremental_cumulative_rvol_update(
            300.0, "09:00", prior_map, length_days=20
        )
        self.assertAlmostEqual(ratio, 2.0)

    def test_instrument_data_trade_volume_aggregation(self):
        from pacavanza.modules.instrument_data import InstrumentData

        id_data = InstrumentData(interval_seconds=300, instrument_id="OMXS306J")

        # Trade 1 at 09:01:00
        t1 = datetime(2026, 10, 12, 7, 1, 0, tzinfo=timezone.utc)
        id_data.add_trade(price=3250.0, volume=5, timestamp=t1)

        bar = id_data.current_bars["OMXS306J"]
        self.assertEqual(bar["open"], 3250.0)
        self.assertEqual(bar["high"], 3250.0)
        self.assertEqual(bar["low"], 3250.0)
        self.assertEqual(bar["close"], 3250.0)
        self.assertEqual(bar["volume"], 5)

        # Trade 2 at 09:02:30 (higher price, vol 10)
        t2 = datetime(2026, 10, 12, 7, 2, 30, tzinfo=timezone.utc)
        id_data.add_trade(price=3255.0, volume=10, timestamp=t2)
        self.assertEqual(bar["high"], 3255.0)
        self.assertEqual(bar["low"], 3250.0)
        self.assertEqual(bar["close"], 3255.0)
        self.assertEqual(bar["volume"], 15)

        # Trade 3 at 09:04:00 (lower price, vol 3)
        t3 = datetime(2026, 10, 12, 7, 4, 0, tzinfo=timezone.utc)
        id_data.add_trade(price=3248.0, volume=3, timestamp=t3)
        self.assertEqual(bar["high"], 3255.0)
        self.assertEqual(bar["low"], 3248.0)
        self.assertEqual(bar["close"], 3248.0)
        self.assertEqual(bar["volume"], 18)

        # Trade 4 at 09:05:01 (next 5m bar starts!)
        t4 = datetime(2026, 10, 12, 7, 5, 1, tzinfo=timezone.utc)
        id_data.add_trade(price=3252.0, volume=7, timestamp=t4)

        # Previous bar should be archived into completed_ohlc with its full volume 18
        completed = id_data.completed_ohlc["OMXS306J"]
        self.assertEqual(len(completed), 1)
        self.assertEqual(completed[0]["volume"], 18)
        self.assertEqual(completed[0]["high"], 3255.0)
        self.assertEqual(completed[0]["low"], 3248.0)

        # New current bar should have volume 7
        new_bar = id_data.current_bars["OMXS306J"]
        self.assertEqual(new_bar["open"], 3252.0)
        self.assertEqual(new_bar["volume"], 7)

    def test_history_reconciliation_merging(self):
        from pacavanza.modules.instrument_data import InstrumentData

        id_data = InstrumentData(interval_seconds=300, instrument_id="OMXS306J")
        t1 = datetime(2026, 10, 12, 7, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 10, 12, 7, 5, 0, tzinfo=timezone.utc)

        # Bar completed with partial/missing volume (e.g. 10)
        id_data.completed_ohlc["OMXS306J"].append(
            {
                "start_time": t1,
                "end_time": t2,
                "open": 3250.0,
                "high": 3255.0,
                "low": 3248.0,
                "close": 3252.0,
                "volume": 10,
            }
        )

        # Official Avanza history returns official volume 45
        official_bars = [
            {
                "start_time": t1,
                "end_time": t2,
                "open": 3250.0,
                "high": 3256.0,
                "low": 3247.5,
                "close": 3252.0,
                "volume": 45.0,
            }
        ]

        # Reconcile logic
        with id_data.lock:
            local_bars = id_data.completed_ohlc["OMXS306J"]
            merged = {b["start_time"]: b for b in local_bars}
            for b in official_bars:
                merged[b["start_time"]] = b
            id_data.completed_ohlc["OMXS306J"] = sorted(
                merged.values(), key=lambda x: x["start_time"]
            )

        self.assertEqual(len(id_data.completed_ohlc["OMXS306J"]), 1)
        self.assertEqual(id_data.completed_ohlc["OMXS306J"][0]["volume"], 45.0)
        self.assertEqual(id_data.completed_ohlc["OMXS306J"][0]["high"], 3256.0)

    def test_trade_sse_deduplication_and_incremental_volume(self):
        """
        Verify that multi-stage fill updates for the same tradeId (e.g. 2 -> 3 -> 4)
        only contribute incremental volume (total 4), not the sum (2 + 3 + 4 = 9),
        and that initialSubscription backlog and cancelled trades are ignored.
        """
        from pacavanza.modules.instrument_data import InstrumentData

        id_data = InstrumentData(interval_seconds=300, instrument_id="OMXS306J")
        seen_vols = {}

        def process_trade(trade_dict):
            if trade_dict.get("initialSubscription") is True:
                return 0
            if trade_dict.get("cancelled") is True:
                return 0
            trade_id = str(trade_dict.get("tradeId", ""))
            raw_vol = float(trade_dict.get("volume", 0))
            if trade_id:
                prev_vol = seen_vols.get(trade_id, 0.0)
                delta_vol = raw_vol - prev_vol
                if delta_vol <= 0:
                    return 0
                seen_vols[trade_id] = raw_vol
            else:
                delta_vol = raw_vol
            vol = int(delta_vol)
            if vol > 0:
                id_data.add_trade(
                    price=float(trade_dict["price"]),
                    volume=vol,
                    timestamp=datetime(2026, 10, 12, 7, 1, 0, tzinfo=timezone.utc),
                )
            return vol

        # Stage 1: Initial subscription replay - MUST BE IGNORED
        v0 = process_trade(
            {
                "tradeId": "T1",
                "price": 3250.0,
                "volume": 10.0,
                "initialSubscription": True,
            }
        )
        self.assertEqual(v0, 0)
        self.assertNotIn("OMXS306J", id_data.current_bars)

        # Stage 2: Trade T2 fills in 3 messages: 2 -> 3 -> 4
        v1 = process_trade({"tradeId": "T2", "price": 3251.0, "volume": 2.0})
        self.assertEqual(v1, 2)
        v2 = process_trade({"tradeId": "T2", "price": 3251.0, "volume": 3.0})
        self.assertEqual(v2, 1)  # 3 - 2 = 1
        v3 = process_trade({"tradeId": "T2", "price": 3251.0, "volume": 4.0})
        self.assertEqual(v3, 1)  # 4 - 3 = 1

        # Duplicate message replay of 4.0
        v4 = process_trade({"tradeId": "T2", "price": 3251.0, "volume": 4.0})
        self.assertEqual(v4, 0)

        # Cancelled trade
        v5 = process_trade(
            {"tradeId": "T3", "price": 3250.0, "volume": 5.0, "cancelled": True}
        )
        self.assertEqual(v5, 0)

        # Another distinct trade T4 with 5.0
        v6 = process_trade({"tradeId": "T4", "price": 3252.0, "volume": 5.0})
        self.assertEqual(v6, 5)

        # Total bar volume should be 4 (from T2) + 5 (from T4) = 9
        current_bar = id_data.current_bars["OMXS306J"]
        self.assertEqual(current_bar["volume"], 9)


if __name__ == "__main__":
    unittest.main()
