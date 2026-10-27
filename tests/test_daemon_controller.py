import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
from zoneinfo import ZoneInfo
import time

from pacavanza.daemon_controller import (
    check_crash_loop,
    reset_crash_history,
    is_active_trading_window,
    start_process,
    MAX_CRASH_COUNT,
    CRASH_WINDOW_SECONDS,
    PROCESS_MAP,
)


class TestDaemonController(unittest.TestCase):
    def setUp(self):
        reset_crash_history()
        PROCESS_MAP.clear()

    def tearDown(self):
        reset_crash_history()
        PROCESS_MAP.clear()

    def test_check_crash_loop_under_threshold(self):
        base_time = 1000.0
        for i in range(MAX_CRASH_COUNT - 1):
            triggered = check_crash_loop("test_proc", now=base_time + i)
            self.assertFalse(triggered)

    def test_check_crash_loop_reaches_threshold(self):
        base_time = 1000.0
        for i in range(MAX_CRASH_COUNT - 1):
            check_crash_loop("test_proc", now=base_time + i)
        
        # 5th crash triggers threshold
        triggered = check_crash_loop("test_proc", now=base_time + MAX_CRASH_COUNT - 1)
        self.assertTrue(triggered)

    def test_check_crash_loop_sliding_window_expiration(self):
        # 4 crashes at t=1000
        for _ in range(4):
            check_crash_loop("test_proc", now=1000.0)

        # 5th crash occurs after CRASH_WINDOW_SECONDS (e.g. t = 1000 + CRASH_WINDOW_SECONDS + 5)
        # Old crashes should expire, so count resets to 1 -> False
        triggered = check_crash_loop("test_proc", now=1000.0 + CRASH_WINDOW_SECONDS + 5.0)
        self.assertFalse(triggered)

    def test_reset_crash_history(self):
        check_crash_loop("proc_a", now=1000.0)
        check_crash_loop("proc_b", now=1000.0)
        
        reset_crash_history("proc_a")
        self.assertFalse(check_crash_loop("proc_a", now=1001.0))
        
        reset_crash_history()
        self.assertFalse(check_crash_loop("proc_b", now=1001.0))

    def test_is_active_trading_window(self):
        tz = ZoneInfo("Europe/Stockholm")
        # Pre-market (07:59:59) -> Inactive
        pre_market = datetime(2026, 10, 27, 7, 59, 59, tzinfo=tz)
        self.assertFalse(is_active_trading_window(pre_market))

        # Market open sharp (08:00:00) -> Active
        market_open = datetime(2026, 10, 27, 8, 0, 0, tzinfo=tz)
        self.assertTrue(is_active_trading_window(market_open))

        # Mid-day (14:30:00) -> Active
        mid_day = datetime(2026, 10, 27, 14, 30, 0, tzinfo=tz)
        self.assertTrue(is_active_trading_window(mid_day))

        # Right before close (22:59:59) -> Active
        before_close = datetime(2026, 10, 27, 22, 59, 59, tzinfo=tz)
        self.assertTrue(is_active_trading_window(before_close))

        # Close sharp (23:00:00) -> Inactive
        market_close = datetime(2026, 10, 27, 23, 0, 0, tzinfo=tz)
        self.assertFalse(is_active_trading_window(market_close))

        # Night (02:00:00) -> Inactive
        night = datetime(2026, 10, 27, 2, 0, 0, tzinfo=tz)
        self.assertFalse(is_active_trading_window(night))

    @patch("pacavanza.daemon_controller.stop_all")
    @patch("multiprocessing.Process")
    def test_start_process_triggers_crash_loop_exit(self, mock_process_cls, mock_stop_all):
        mock_proc = MagicMock()
        mock_proc.is_alive.return_value = False
        mock_proc.exitcode = 1
        mock_process_cls.return_value = mock_proc

        # Pre-seed 4 crashes
        for _ in range(MAX_CRASH_COUNT - 1):
            check_crash_loop("dashboard")

        # Set dead process in PROCESS_MAP
        PROCESS_MAP["dashboard"] = mock_proc

        # 5th crash should raise SystemExit(1)
        with self.assertRaises(SystemExit) as ctx:
            start_process("dashboard", "pacavanza.backend", "pacavanza.dashboard")

        self.assertEqual(ctx.exception.code, 1)
        mock_stop_all.assert_called_once()


if __name__ == "__main__":
    unittest.main()
