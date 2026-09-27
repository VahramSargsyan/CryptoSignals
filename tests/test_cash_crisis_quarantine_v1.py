from __future__ import annotations

import unittest

import pandas as pd

from scripts.research_cash_crisis_quarantine_v1 import run_cash_quarantine_backtest


class CashCrisisQuarantineV1Tests(unittest.TestCase):
    def _panel(self, periods: int = 14) -> pd.DataFrame:
        dates = pd.date_range("2026-01-01", periods=periods, freq="D", tz="UTC")
        return pd.DataFrame(
            {
                "timestamp": dates,
                "ATOM_open": [10.0] * periods,
                "ATOM_close": [10.0] * periods,
                "breadth_sma200": [2] * periods,
                "lowest_vol_asset": ["TRX"] * periods,
            }
        )

    def test_fixed_three_day_quarantine_exits_on_fourth_open(self):
        episodes: list[dict] = []
        result = run_cash_quarantine_backtest(
            self._panel(),
            {},
            start=pd.Timestamp("2026-01-01", tz="UTC"),
            end=pd.Timestamp("2026-01-14", tz="UTC"),
            start_asset="ATOM",
            quarantine_days=3,
            episode_log=episodes,
        )
        # First crisis entry: three low closes Jan1-Jan3 -> cash Jan4 open.
        self.assertEqual(episodes[0]["action"], "ENTER")
        self.assertEqual(episodes[0]["date"][:10], "2026-01-04")
        # Cash days Jan4-Jan6 -> re-entry Jan7 open.
        self.assertEqual(episodes[1]["action"], "EXIT")
        self.assertEqual(episodes[1]["date"][:10], "2026-01-07")
        self.assertGreaterEqual(result.defensive_days, 3)

    def test_crisis_streak_restarts_after_cash_exit(self):
        episodes: list[dict] = []
        run_cash_quarantine_backtest(
            self._panel(),
            {},
            start=pd.Timestamp("2026-01-01", tz="UTC"),
            end=pd.Timestamp("2026-01-14", tz="UTC"),
            start_asset="ATOM",
            quarantine_days=3,
            episode_log=episodes,
        )
        # Exit Jan7. Fresh low closes Jan7-Jan9 are required;
        # earliest second entry is Jan10 open, not Jan8.
        enter_dates = [x["date"][:10] for x in episodes if x["action"] == "ENTER"]
        self.assertGreaterEqual(len(enter_dates), 2)
        self.assertEqual(enter_dates[1], "2026-01-10")

    def test_cash_equity_flat_during_quarantine(self):
        daily: list[dict] = []
        run_cash_quarantine_backtest(
            self._panel(),
            {},
            start=pd.Timestamp("2026-01-01", tz="UTC"),
            end=pd.Timestamp("2026-01-14", tz="UTC"),
            start_asset="ATOM",
            quarantine_days=3,
            daily_log=daily,
        )
        x = pd.DataFrame(daily)
        first_cash = x[x["in_cash"]].iloc[:3]
        self.assertEqual(set(first_cash["actual_asset"]), {"CASH_PROXY"})
        self.assertEqual(first_cash["equity"].nunique(), 1)

    def test_transition_cost_applies_on_cash_round_trip(self):
        # Use changing breadth so only one episode occurs.
        panel = self._panel(periods=9)
        panel["breadth_sma200"] = [2, 2, 2, 2, 2, 2, 8, 8, 8]
        result = run_cash_quarantine_backtest(
            panel,
            {},
            start=pd.Timestamp("2026-01-01", tz="UTC"),
            end=pd.Timestamp("2026-01-09", tz="UTC"),
            start_asset="ATOM",
            quarantine_days=3,
        )
        expected = (1.0 - 0.001) ** 2 - 1.0
        self.assertAlmostEqual(result.total_return, expected, places=12)


if __name__ == "__main__":
    unittest.main()
