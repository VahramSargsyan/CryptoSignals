import unittest
from types import SimpleNamespace

import pandas as pd

from scripts.run_grid_paper_live import (
    PROFILES,
    _config,
    _notification_text,
    _profile_engine,
    _profile_metrics,
    _scale_single_layer_frame,
)


class GridPaperLiveTests(unittest.TestCase):
    def test_profiles_keep_full_reinvestment_and_no_runner(self):
        base = _config("CONTROL_BASE")
        wide = _config("CANDIDATE_WIDE")
        micro_only = _config("MICRO_ONLY_WIDE")
        mid_only = _config("MID_ONLY_WIDE")

        self.assertEqual(base.micro_exit_sublevels, 1)
        self.assertEqual(base.mid_recovery_sublevels, 10)
        self.assertEqual(wide.micro_exit_sublevels, 6)
        self.assertEqual(wide.mid_recovery_sublevels, 18)
        self.assertEqual(micro_only.micro_exit_sublevels, 6)
        self.assertEqual(micro_only.mid_recovery_sublevels, 18)
        self.assertEqual(mid_only.micro_exit_sublevels, 6)
        self.assertEqual(mid_only.mid_recovery_sublevels, 18)

        self.assertEqual(PROFILES["CONTROL_BASE"]["layer"], "BOTH")
        self.assertEqual(PROFILES["CANDIDATE_WIDE"]["layer"], "BOTH")
        self.assertEqual(PROFILES["MICRO_ONLY_WIDE"]["layer"], "MICRO")
        self.assertEqual(PROFILES["MID_ONLY_WIDE"]["layer"], "MID")
        self.assertEqual(PROFILES["MID_OSS_ATR50_TRAIL7"]["layer"], "MID")
        self.assertEqual(_profile_engine("MID_ONLY_WIDE"), "CANONICAL")
        self.assertEqual(_profile_engine("MID_OSS_ATR50_TRAIL7"), "OSS_FORWARD_CANDIDATE")
        self.assertEqual(PROFILES["MID_OSS_ATR50_TRAIL7"]["atr_regrid_threshold"], 0.50)
        self.assertEqual(PROFILES["MID_OSS_ATR50_TRAIL7"]["exit_retracement"], 0.07)

        for cfg in (base, wide, micro_only, mid_only):
            self.assertEqual(cfg.profit_reinvest_fraction, 1.0)
            self.assertEqual(cfg.runner_fraction, 0.0)
            self.assertEqual(cfg.rolling_range.lookback_candles, 1095)
            self.assertEqual(cfg.rolling_range.refresh_candles, 30)

    def test_single_layer_projection_uses_same_total_normalized_capital(self):
        result = SimpleNamespace(
            summary={
                "total_final_equity": 2200.0,
                "total_return": 0.10,
                "max_drawdown": 0.12,
                "open_micro_lots_end": 3,
                "open_mid_lots_end": 2,
                "closed_trade_count": 9,
                "micro_initial_capital": 1000.0,
                "mid_initial_capital": 1000.0,
                "micro_final_equity": 1250.0,
                "mid_final_equity": 950.0,
                "micro_total_return": 0.25,
                "mid_total_return": -0.05,
            },
            equity_curve=pd.DataFrame(
                {
                    "micro_equity": [1000.0, 900.0, 1250.0],
                    "mid_equity": [1000.0, 800.0, 950.0],
                }
            ),
            trades=pd.DataFrame(
                {
                    "layer": ["MICRO", "MICRO", "MID"],
                    "invested_cash": [10.0, 20.0, 30.0],
                }
            ),
        )

        micro = _profile_metrics("MICRO_ONLY_WIDE", result)
        mid = _profile_metrics("MID_ONLY_WIDE", result)

        self.assertEqual(micro["equity"], 2500.0)
        self.assertEqual(micro["return"], 0.25)
        self.assertAlmostEqual(micro["max_drawdown"], 0.10)
        self.assertEqual(micro["open_micro_lots"], 3)
        self.assertEqual(micro["open_mid_lots"], 0)
        self.assertEqual(micro["closed_trade_count"], 2)

        self.assertEqual(mid["equity"], 1900.0)
        self.assertEqual(mid["return"], -0.05)
        self.assertAlmostEqual(mid["max_drawdown"], 0.20)
        self.assertEqual(mid["open_micro_lots"], 0)
        self.assertEqual(mid["open_mid_lots"], 2)
        self.assertEqual(mid["closed_trade_count"], 1)

    def test_single_layer_event_ledger_filters_and_scales(self):
        events = pd.DataFrame(
            {
                "layer": ["MICRO", "MID"],
                "event_type": ["BUY", "SELL"],
                "units": [2.0, 3.0],
                "cash_value": [100.0, 200.0],
                "fill_price": [50.0, 60.0],
            }
        )

        micro = _scale_single_layer_frame("MICRO_ONLY_WIDE", events)
        mid = _scale_single_layer_frame("MID_ONLY_WIDE", events)

        self.assertEqual(list(micro["layer"]), ["MICRO"])
        self.assertEqual(float(micro.iloc[0]["units"]), 4.0)
        self.assertEqual(float(micro.iloc[0]["cash_value"]), 200.0)
        self.assertEqual(float(micro.iloc[0]["fill_price"]), 50.0)

        self.assertEqual(list(mid["layer"]), ["MID"])
        self.assertEqual(float(mid.iloc[0]["units"]), 6.0)
        self.assertEqual(float(mid.iloc[0]["cash_value"]), 400.0)
        self.assertEqual(float(mid.iloc[0]["fill_price"]), 60.0)

    def test_notification_lists_only_profiles_with_today_signals(self):
        payload = {
            "latest_closed_candle": "2026-09-26T00:00:00+00:00",
            "completed_paper_candles": 1,
            "milestone": None,
            "rows": [
                {
                    "profile": "MICRO_ONLY_WIDE",
                    "symbol": "LINKUSDT",
                    "today_events": 2,
                    "today_buys": 2,
                    "today_sells": 0,
                },
                {
                    "profile": "MID_ONLY_WIDE",
                    "symbol": "LINKUSDT",
                    "today_events": 0,
                    "today_buys": 0,
                    "today_sells": 0,
                },
            ],
            "portfolio": [
                {
                    "profile": "MICRO_ONLY_WIDE",
                    "return": 0.01,
                    "today_buys": 2,
                    "today_sells": 0,
                },
                {
                    "profile": "MID_ONLY_WIDE",
                    "return": 0.02,
                    "today_buys": 0,
                    "today_sells": 0,
                },
            ],
        }
        text = _notification_text(payload)
        self.assertIn("MICRO_ONLY_WIDE LINKUSDT: BUY 2, SELL 0", text)
        self.assertIn("MICRO_ONLY_WIDE: +1.00%", text)
        self.assertNotIn("MID_ONLY_WIDE:", text)
        self.assertIn("PAPER ONLY", text)
        self.assertLess(len(text), 1000)


if __name__ == "__main__":
    unittest.main()
