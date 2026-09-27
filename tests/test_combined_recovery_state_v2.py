from __future__ import annotations

import unittest

import pandas as pd

from scripts.research_combined_recovery_state_v2 import (
    BTC_CANDIDATES,
    btc_bull_state_map,
    build_state_gate_map,
)


class CombinedRecoveryStateV2Tests(unittest.TestCase):
    def test_btc_candidates_remain_frozen(self):
        self.assertEqual(BTC_CANDIDATES, ((25, 100), (30, 100), (12, 100)))

    def test_btc_state_persists_after_crossover(self):
        dates = pd.date_range("2024-01-01", periods=8, freq="D", tz="UTC")
        panel = pd.DataFrame({
            "timestamp": dates,
            "BTC_close": [10, 9, 8, 7, 8, 9, 10, 11],
        })
        state = btc_bull_state_map(panel, 2, 4)
        true_dates = [d for d in dates if state[d]]
        self.assertGreaterEqual(len(true_dates), 2)
        self.assertTrue(state[true_dates[-1]])

    def test_later_breadth_recovery_can_exit_without_new_crossover(self):
        dates = pd.date_range("2024-01-01", periods=3, freq="D", tz="UTC")
        panel = pd.DataFrame({
            "timestamp": dates,
            "breadth_sma200": [2, 3, 4],
            "m2_expansion_all": [True, True, True],
        })
        btc_state = {dates[0]: True, dates[1]: True, dates[2]: True}
        gate, diag = build_state_gate_map(panel, btc_state)

        self.assertFalse(gate[dates[0]])
        self.assertFalse(gate[dates[1]])
        self.assertTrue(gate[dates[2]])
        self.assertEqual(diag["blocked_by_breadth_after_m2_pass"], 2)
        self.assertEqual(diag["all_recovery_state_days"], 1)

    def test_m2_can_still_block_bullish_btc_and_breadth(self):
        dates = pd.date_range("2024-01-01", periods=1, freq="D", tz="UTC")
        panel = pd.DataFrame({
            "timestamp": dates,
            "breadth_sma200": [5],
            "m2_expansion_all": [False],
        })
        gate, diag = build_state_gate_map(panel, {dates[0]: True})
        self.assertFalse(gate[dates[0]])
        self.assertEqual(diag["blocked_by_m2"], 1)


if __name__ == "__main__":
    unittest.main()
