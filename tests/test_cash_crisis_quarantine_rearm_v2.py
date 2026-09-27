from __future__ import annotations

import unittest

import pandas as pd

from scripts.research_cash_crisis_quarantine_rearm_v2 import (
    run_cash_quarantine_rearm_backtest,
)


class CashCrisisQuarantineRearmV2Tests(unittest.TestCase):
    def _panel(self, breadth: list[int]) -> pd.DataFrame:
        periods = len(breadth)
        dates = pd.date_range("2026-01-01", periods=periods, freq="D", tz="UTC")
        return pd.DataFrame(
            {
                "timestamp": dates,
                "ATOM_open": [10.0] * periods,
                "ATOM_close": [10.0] * periods,
                "breadth_sma200": breadth,
                "lowest_vol_asset": ["TRX"] * periods,
            }
        )

    def test_persistent_stress_does_not_retrigger_after_cash_exit(self):
        panel = self._panel([2] * 16)
        episodes: list[dict] = []
        r = run_cash_quarantine_rearm_backtest(
            panel,
            {},
            start=panel.iloc[0]["timestamp"],
            end=panel.iloc[-1]["timestamp"],
            start_asset="ATOM",
            quarantine_days=3,
            episode_log=episodes,
        )
        enters = [x for x in episodes if x["action"] == "ENTER"]
        exits = [x for x in episodes if x["action"] == "EXIT"]
        rearms = [x for x in episodes if x["action"] == "REARM"]
        self.assertEqual(len(enters), 1)
        self.assertEqual(len(exits), 1)
        self.assertEqual(len(rearms), 0)
        self.assertEqual(r.cash_entries, 1)

    def test_rearm_requires_three_recovery_closes_then_fresh_three_low_closes(self):
        # Jan1-3 low -> cash Jan4.
        # Cash Jan4-6 -> exit Jan7.
        # Jan7-9 high -> REARM after Jan9 close.
        # Jan10-12 low -> second cash entry Jan13 open.
        breadth = [2,2,2,2,2,2,6,6,6,2,2,2,2,2]
        panel = self._panel(breadth)
        episodes: list[dict] = []
        r = run_cash_quarantine_rearm_backtest(
            panel,
            {},
            start=panel.iloc[0]["timestamp"],
            end=panel.iloc[-1]["timestamp"],
            start_asset="ATOM",
            quarantine_days=3,
            episode_log=episodes,
        )
        enters = [x["date"][:10] for x in episodes if x["action"] == "ENTER"]
        rearms = [x["date"][:10] for x in episodes if x["action"] == "REARM"]
        self.assertEqual(enters, ["2026-01-04", "2026-01-13"])
        self.assertEqual(rearms, ["2026-01-09"])
        self.assertEqual(r.rearm_count, 1)

    def test_cash_duration_is_exact(self):
        breadth = [2,2,2,2,2,2,6,6,6,6]
        panel = self._panel(breadth)
        daily: list[dict] = []
        run_cash_quarantine_rearm_backtest(
            panel,
            {},
            start=panel.iloc[0]["timestamp"],
            end=panel.iloc[-1]["timestamp"],
            start_asset="ATOM",
            quarantine_days=3,
            daily_log=daily,
        )
        x = pd.DataFrame(daily)
        first_cash = x[x["state"] == "CASH"].iloc[:3]
        self.assertEqual(len(first_cash), 3)
        self.assertEqual(first_cash.iloc[0]["date"][:10], "2026-01-04")
        self.assertEqual(first_cash.iloc[-1]["date"][:10], "2026-01-06")
        self.assertEqual(first_cash["equity"].nunique(), 1)

    def test_post_cash_disarmed_follows_crypto_not_cash(self):
        breadth = [2,2,2,2,2,2,2,2,2,2]
        panel = self._panel(breadth)
        daily: list[dict] = []
        run_cash_quarantine_rearm_backtest(
            panel,
            {},
            start=panel.iloc[0]["timestamp"],
            end=panel.iloc[-1]["timestamp"],
            start_asset="ATOM",
            quarantine_days=3,
            daily_log=daily,
        )
        x = pd.DataFrame(daily)
        disarmed = x[x["state"] == "POST_CASH_DISARMED"]
        self.assertFalse(disarmed.empty)
        self.assertTrue((disarmed["actual_asset"] == "ATOM").all())


if __name__ == "__main__":
    unittest.main()
