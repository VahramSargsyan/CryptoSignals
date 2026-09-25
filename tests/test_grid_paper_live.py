import unittest

from scripts.run_grid_paper_live import PROFILES, _config, _notification_text


class GridPaperLiveTests(unittest.TestCase):
    def test_profiles_keep_full_reinvestment_and_no_runner(self):
        base = _config("CONTROL_BASE")
        wide = _config("CANDIDATE_WIDE")

        self.assertEqual(base.micro_exit_sublevels, 1)
        self.assertEqual(base.mid_recovery_sublevels, 10)
        self.assertEqual(wide.micro_exit_sublevels, 6)
        self.assertEqual(wide.mid_recovery_sublevels, 18)

        for cfg in (base, wide):
            self.assertEqual(cfg.profit_reinvest_fraction, 1.0)
            self.assertEqual(cfg.runner_fraction, 0.0)
            self.assertEqual(cfg.rolling_range.lookback_candles, 1095)
            self.assertEqual(cfg.rolling_range.refresh_candles, 30)

    def test_notification_is_compact(self):
        payload = {
            "latest_closed_candle": "2026-09-26T00:00:00+00:00",
            "completed_paper_candles": 1,
            "milestone": None,
            "portfolio": [
                {
                    "profile": "CONTROL_BASE",
                    "return": 0.01,
                    "today_buys": 2,
                    "today_sells": 1,
                }
            ],
        }
        text = _notification_text(payload)
        self.assertIn("CONTROL_BASE", text)
        self.assertIn("PAPER ONLY", text)
        self.assertLess(len(text), 1000)


if __name__ == "__main__":
    unittest.main()
