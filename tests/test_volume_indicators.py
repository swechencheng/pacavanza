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


if __name__ == "__main__":
    unittest.main()
