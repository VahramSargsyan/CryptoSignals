from __future__ import annotations

import unittest

import pandas as pd

from scripts.research_cash_defense_destination_ablation_v1 import (
    complete_windows,
    run_cash_backtest,
)


class CashDefenseDestinationAblationV1Tests(unittest.TestCase):
    def _panel(self) -> pd.DataFrame:
        dates = pd.date_range("2026-01-01", periods=8, freq="D", tz="UTC")
        return pd.DataFrame(
            {
                "timestamp": dates,
                "ATOM_open": [10.0] * 8,
                "ATOM_close": [10.0] * 8,
                "breadth_sma200": [2, 2, 2, 2, 6, 6, 6, 6],
                "lowest_vol_asset": ["TRX"] * 8,
            }
        )

    def test_cash_uses_same_three_day_entry_exit_confirmation(self):
        daily: list[dict] = []
        episodes: list[dict] = []
        result = run_cash_backtest(
            self._panel(),
            {},
            start=pd.Timestamp("2026-01-01", tz="UTC"),
            end=pd.Timestamp("2026-01-08", tz="UTC"),
            start_asset="ATOM",
            episode_log=episodes,
            daily_log=daily,
        )
        self.assertEqual(result.defensive_entries, 1)
        self.assertEqual(result.defensive_transitions, 2)
        self.assertEqual(result.defensive_days, 4)
        self.assertEqual(episodes[0]["action"], "ENTER")
        self.assertEqual(episodes[0]["date"][:10], "2026-01-04")
        self.assertEqual(episodes[1]["action"], "EXIT")
        self.assertEqual(episodes[1]["date"][:10], "2026-01-08")

    def test_cash_equity_is_flat_between_entry_and_exit(self):
        daily: list[dict] = []
        run_cash_backtest(
            self._panel(),
            {},
            start=pd.Timestamp("2026-01-01", tz="UTC"),
            end=pd.Timestamp("2026-01-08", tz="UTC"),
            start_asset="ATOM",
            daily_log=daily,
        )
        x = pd.DataFrame(daily)
        defensive = x[x["defensive"]]
        self.assertEqual(set(defensive["actual_asset"]), {"CASH_PROXY"})
        self.assertEqual(defensive["equity"].nunique(), 1)

    def test_cash_only_loses_two_transition_costs_in_flat_market(self):
        result = run_cash_backtest(
            self._panel(),
            {},
            start=pd.Timestamp("2026-01-01", tz="UTC"),
            end=pd.Timestamp("2026-01-08", tz="UTC"),
            start_asset="ATOM",
        )
        expected = (1.0 - 0.001) ** 2 - 1.0
        self.assertAlmostEqual(result.total_return, expected, places=12)

    def test_complete_windows_drop_terminal_partial_remainder(self):
        windows = complete_windows(
            pd.Timestamp("2026-01-01", tz="UTC"),
            pd.Timestamp("2026-05-10", tz="UTC"),
            60,
        )
        self.assertEqual(len(windows), 2)
        self.assertEqual(windows[0][0], pd.Timestamp("2026-01-01", tz="UTC"))
        self.assertEqual(windows[0][1], pd.Timestamp("2026-03-01", tz="UTC"))
        self.assertEqual(windows[1][0], pd.Timestamp("2026-03-02", tz="UTC"))
        self.assertEqual(windows[1][1], pd.Timestamp("2026-04-30", tz="UTC"))


if __name__ == "__main__":
    unittest.main()
