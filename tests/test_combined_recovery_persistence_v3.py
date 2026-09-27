from __future__ import annotations

import unittest

import pandas as pd

from scripts.research_combined_recovery_persistence_v3 import (
    BTC_CANDIDATES,
    CONFIRM_DAYS,
    run_persistent_reentry_backtest,
)


class CombinedRecoveryPersistenceV3Tests(unittest.TestCase):
    def test_candidates_and_confirmation_remain_frozen(self):
        self.assertEqual(BTC_CANDIDATES, ((25,100),(30,100),(12,100)))
        self.assertEqual(CONFIRM_DAYS, 3)

    def test_three_good_closes_are_counted_only_after_cash_entry(self):
        dates = pd.date_range("2026-01-01", periods=9, freq="D", tz="UTC")
        panel = pd.DataFrame({
            "timestamp": dates,
            "breadth_sma200": [2,2,2,4,4,4,4,4,4],
            "ATOM_open": [10.0]*9,
            "ATOM_close": [10.0]*9,
        })
        # Recovery state is already true before CASH entry.
        # Entry executes Jan4 open after Jan1-3 stress closes.
        # V3 must still wait for Jan4, Jan5, Jan6 closes and exit Jan7 open.
        recovery = {d: True for d in dates}
        result = run_persistent_reentry_backtest(
            panel,
            {},
            recovery,
            start=dates[0],
            end=dates[-1],
            start_asset="ATOM",
        )
        self.assertEqual(result["cash_entries"], 1)
        self.assertEqual(result["btc_reentry_exits"], 1)
        self.assertEqual(result["median_cash_wait_days"], 3.0)
        self.assertEqual(result["cash_recovery_streak_completions"], 1)

    def test_recovery_streak_resets_when_any_state_breaks(self):
        dates = pd.date_range("2026-01-01", periods=11, freq="D", tz="UTC")
        panel = pd.DataFrame({
            "timestamp": dates,
            "breadth_sma200": [2,2,2,4,4,4,4,4,4,4,4],
            "ATOM_open": [10.0]*11,
            "ATOM_close": [10.0]*11,
        })
        recovery = {
            dates[0]: True,
            dates[1]: True,
            dates[2]: True,
            dates[3]: True,
            dates[4]: True,
            dates[5]: False,
            dates[6]: True,
            dates[7]: True,
            dates[8]: True,
            dates[9]: True,
            dates[10]: True,
        }
        result = run_persistent_reentry_backtest(
            panel,
            {},
            recovery,
            start=dates[0],
            end=dates[-1],
            start_asset="ATOM",
        )
        self.assertEqual(result["cash_recovery_streak_resets"], 1)
        self.assertEqual(result["btc_reentry_exits"], 1)
        # Jan4/Jan5 good, Jan6 reset, Jan7-Jan9 good -> Jan10 open exit.
        self.assertEqual(result["median_cash_wait_days"], 6.0)


if __name__ == "__main__":
    unittest.main()
