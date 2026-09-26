import unittest

import numpy as np
import pandas as pd

from scripts.research_grid_oss_harvest import (
    EVALUATION_START as RESEARCH_START,
    atr_gated_hl_schedule,
    run_mid,
)
from strategies.crypto.link_level_grid.oss_forward_candidate import (
    OssMidCandidateConfig,
    run_oss_mid_candidate,
)


def fixture_frame() -> tuple[pd.DataFrame, pd.Timestamp]:
    prehistory_days = 1095
    timestamps = pd.date_range("2023-01-01", periods=prehistory_days + 4, freq="D", tz="UTC")
    rows = []
    for ts in timestamps[:prehistory_days]:
        rows.append(
            {
                "timestamp": ts,
                "open": 100.0,
                "high": 110.0,
                "low": 90.0,
                "close": 100.0,
                "volume": 1000.0,
            }
        )

    evaluation = [
        (100.0, 101.0, 99.0, 100.0),
        (103.0, 106.0, 100.0, 105.0),
        (104.0, 106.5, 98.0, 99.0),
        (99.0, 100.0, 97.0, 98.0),
    ]
    for ts, (open_, high, low, close) in zip(timestamps[prehistory_days:], evaluation):
        rows.append(
            {
                "timestamp": ts,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": 1000.0,
            }
        )
    return pd.DataFrame(rows), timestamps[prehistory_days]


class OssForwardCandidateTests(unittest.TestCase):
    def test_candidate_is_deterministic(self):
        frame, start = fixture_frame()
        cfg = OssMidCandidateConfig()
        first = run_oss_mid_candidate(frame, evaluation_start=start, config=cfg)
        second = run_oss_mid_candidate(frame, evaluation_start=start, config=cfg)
        self.assertEqual(first.summary, second.summary)
        pd.testing.assert_frame_equal(first.events, second.events)
        pd.testing.assert_frame_equal(first.equity_curve, second.equity_curve)

    def test_initial_range_uses_only_prehistory(self):
        frame, start = fixture_frame()
        result = run_oss_mid_candidate(frame, evaluation_start=start)
        first_range = result.range_history.iloc[0]
        self.assertEqual(float(first_range["high"]), 110.0)
        self.assertEqual(float(first_range["low"]), 90.0)
        self.assertEqual(first_range["reason"], "INITIAL")

    def test_target_touch_arms_before_later_trailing_sell(self):
        frame, start = fixture_frame()
        result = run_oss_mid_candidate(frame, evaluation_start=start)
        events = result.events[result.events["slot_id"] == 8].copy()
        arm = events[events["event_type"] == "ARM_EXIT"]
        sell = events[events["event_type"] == "SELL"]
        self.assertEqual(len(arm), 1)
        self.assertEqual(len(sell), 1)
        self.assertLess(
            pd.Timestamp(arm.iloc[0]["timestamp"]),
            pd.Timestamp(sell.iloc[0]["timestamp"]),
        )


    def test_forward_engine_matches_research_harness_candidate(self):
        timestamps = pd.date_range("2020-09-25", periods=2191, freq="D", tz="UTC")
        x = np.arange(len(timestamps), dtype=float)
        close = 100.0 + 0.018 * x + 18.0 * np.sin(x / 31.0) + 6.0 * np.sin(x / 7.0)
        open_ = close * (1.0 + 0.002 * np.sin(x / 5.0))
        frame = pd.DataFrame(
            {
                "timestamp": timestamps,
                "open": open_,
                "high": np.maximum(open_, close) * 1.025,
                "low": np.minimum(open_, close) * 0.975,
                "close": close,
                "volume": np.full(len(timestamps), 1000.0),
            }
        )

        research = run_mid(
            frame,
            schedule=atr_gated_hl_schedule(frame, atr_regrid_threshold=0.50),
            exit_retracement=0.07,
        )
        forward = run_oss_mid_candidate(
            frame,
            evaluation_start=RESEARCH_START,
            config=OssMidCandidateConfig(
                initial_capital=2000.0,
                atr_regrid_threshold=0.50,
                regrid_cooldown_candles=60,
                exit_retracement=0.07,
            ),
        )
        self.assertAlmostEqual(forward.summary["total_return"], research.total_return, places=12)
        self.assertAlmostEqual(forward.summary["max_drawdown"], research.max_drawdown, places=12)
        self.assertEqual(forward.summary["closed_trade_count"], research.closed_trades)

    def test_profile_uses_2000_normalized_capital(self):
        frame, start = fixture_frame()
        result = run_oss_mid_candidate(frame, evaluation_start=start)
        self.assertEqual(result.summary["initial_capital"], 2000.0)
        self.assertTrue(result.summary["real_orders"] is False)


if __name__ == "__main__":
    unittest.main()
