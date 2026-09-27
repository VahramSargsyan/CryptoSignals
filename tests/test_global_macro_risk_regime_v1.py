from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from scripts.research_global_macro_risk_regime_v1 import (
    availability_date,
    build_crypto_stress_episodes,
    rolling_zscore,
)


class GlobalMacroRiskRegimeV1Tests(unittest.TestCase):
    def test_confirmation_and_next_open_execution(self):
        dates = pd.date_range("2026-01-01", periods=12, freq="D")
        breadth = [6, 3, 3, 3, 2, 4, 5, 5, 5, 6, 6, 6]
        daily = pd.DataFrame({"date": dates, "breadth": breadth})

        episodes = build_crypto_stress_episodes(daily)

        self.assertEqual(len(episodes), 1)
        row = episodes.iloc[0]
        self.assertEqual(pd.Timestamp(row["entry_signal_date"]), pd.Timestamp("2026-01-04"))
        self.assertEqual(pd.Timestamp(row["entry_execution_date"]), pd.Timestamp("2026-01-05"))
        self.assertEqual(pd.Timestamp(row["exit_signal_date"]), pd.Timestamp("2026-01-09"))
        self.assertEqual(pd.Timestamp(row["exit_execution_date"]), pd.Timestamp("2026-01-10"))

    def test_defense_does_not_exit_on_two_recovery_closes(self):
        dates = pd.date_range("2026-02-01", periods=10, freq="D")
        breadth = [3, 3, 3, 2, 5, 5, 4, 5, 5, 4]
        daily = pd.DataFrame({"date": dates, "breadth": breadth})

        episodes = build_crypto_stress_episodes(daily)

        self.assertEqual(len(episodes), 1)
        self.assertTrue(pd.isna(episodes.iloc[0]["exit_execution_date"]))

    def test_rolling_zscore_is_causal(self):
        s1 = pd.Series(np.arange(1.0, 21.0))
        s2 = s1.copy()
        z1 = rolling_zscore(s1, window=5, min_periods=3)
        s2.iloc[-1] = 10_000.0
        z2 = rolling_zscore(s2, window=5, min_periods=3)

        pd.testing.assert_series_equal(z1.iloc[:-1], z2.iloc[:-1])

    def test_nfci_style_availability_lag(self):
        dates = pd.Series(pd.to_datetime(["2026-01-02", "2026-01-09"]))
        shifted = availability_date(dates, 5)
        self.assertEqual(shifted.iloc[0], pd.Timestamp("2026-01-07"))
        self.assertEqual(shifted.iloc[1], pd.Timestamp("2026-01-14"))


if __name__ == "__main__":
    unittest.main()
