from __future__ import annotations

import unittest

import pandas as pd

from scripts.research_cash_shadow_sma25_50_reentry_v1 import (
    run_cash_sma_reentry_backtest,
)
from scripts.research_relative_rotation_graph_intelligence import PairSignal


class CashShadowSma2550ReentryV1Tests(unittest.TestCase):
    def _panel(self, breadth: list[int]) -> pd.DataFrame:
        n = len(breadth)
        dates = pd.date_range("2026-01-01", periods=n, freq="D", tz="UTC")
        data = {
            "timestamp": dates,
            "breadth_sma200": breadth,
            "lowest_vol_asset": ["TRX"] * n,
        }
        for asset in ("ATOM", "BNB"):
            data[f"{asset}_open"] = [10.0] * n
            data[f"{asset}_close"] = [10.0] * n
            data[f"{asset}_sma25_50_cross_up"] = [False] * n
        return pd.DataFrame(data)

    def test_no_cross_means_cash_stays_active(self):
        panel = self._panel([2] * 10)
        episodes: list[dict] = []
        r = run_cash_sma_reentry_backtest(
            panel,
            {},
            start=panel.iloc[0]["timestamp"],
            end=panel.iloc[-1]["timestamp"],
            start_asset="ATOM",
            episode_log=episodes,
        )
        enters = [x for x in episodes if x["action"] == "ENTER_CASH"]
        exits = [x for x in episodes if x["action"] == "EXIT_CASH_ON_SMA25_50_CROSS"]
        self.assertEqual(len(enters), 1)
        self.assertEqual(len(exits), 0)
        self.assertTrue(r.unresolved_cash_end)

    def test_cross_on_shadow_exits_cash_next_open(self):
        panel = self._panel([2] * 10)
        # Jan1-3 low -> cash Jan4 open.
        # Bullish cross on Jan6 close -> crypto Jan7 open.
        panel.loc[panel["timestamp"] == pd.Timestamp("2026-01-06", tz="UTC"), "ATOM_sma25_50_cross_up"] = True
        episodes: list[dict] = []
        r = run_cash_sma_reentry_backtest(
            panel,
            {},
            start=panel.iloc[0]["timestamp"],
            end=panel.iloc[-1]["timestamp"],
            start_asset="ATOM",
            episode_log=episodes,
        )
        exits = [x for x in episodes if x["action"] == "EXIT_CASH_ON_SMA25_50_CROSS"]
        self.assertEqual(len(exits), 1)
        self.assertEqual(exits[0]["date"][:10], "2026-01-07")
        self.assertEqual(exits[0]["actual_asset"], "ATOM")
        self.assertEqual(exits[0]["cash_wait_days"], 3)
        self.assertFalse(r.unresolved_cash_end)

    def test_router_transition_and_cross_reenter_directly_into_new_shadow(self):
        panel = self._panel([2] * 11)
        jan6 = pd.Timestamp("2026-01-06", tz="UTC")
        panel.loc[panel["timestamp"] == jan6, "BNB_sma25_50_cross_up"] = True
        signals = {
            jan6: [
                PairSignal(
                    date=jan6,
                    from_asset="ATOM",
                    to_asset="BNB",
                    strength=0.2,
                    pair="ATOM/BNB",
                )
            ]
        }
        episodes: list[dict] = []
        r = run_cash_sma_reentry_backtest(
            panel,
            signals,
            start=panel.iloc[0]["timestamp"],
            end=panel.iloc[-1]["timestamp"],
            start_asset="ATOM",
            episode_log=episodes,
        )
        exits = [x for x in episodes if x["action"] == "EXIT_CASH_ON_SMA25_50_CROSS"]
        self.assertEqual(len(exits), 1)
        self.assertEqual(exits[0]["date"][:10], "2026-01-07")
        self.assertEqual(exits[0]["actual_asset"], "BNB")
        self.assertEqual(exits[0]["shadow_asset"], "BNB")
        # Actual transitions: ATOM->CASH, CASH->BNB. No CASH->ATOM->BNB.
        self.assertEqual(r.actual_transitions, 2)

    def test_no_second_cash_entry_before_recovery_rearm(self):
        panel = self._panel([2,2,2,2,2,2,2,2,2,2,2,2,2,2])
        panel.loc[panel["timestamp"] == pd.Timestamp("2026-01-06", tz="UTC"), "ATOM_sma25_50_cross_up"] = True
        episodes: list[dict] = []
        r = run_cash_sma_reentry_backtest(
            panel,
            {},
            start=panel.iloc[0]["timestamp"],
            end=panel.iloc[-1]["timestamp"],
            start_asset="ATOM",
            episode_log=episodes,
        )
        enters = [x for x in episodes if x["action"] == "ENTER_CASH"]
        self.assertEqual(len(enters), 1)
        self.assertEqual(r.cash_entries, 1)
        self.assertEqual(r.rearm_count, 0)

    def test_rearm_then_fresh_crisis_can_enter_cash_again(self):
        breadth = [2,2,2,2,2,2,6,6,6,2,2,2,2,2]
        panel = self._panel(breadth)
        panel.loc[panel["timestamp"] == pd.Timestamp("2026-01-06", tz="UTC"), "ATOM_sma25_50_cross_up"] = True
        episodes: list[dict] = []
        r = run_cash_sma_reentry_backtest(
            panel,
            {},
            start=panel.iloc[0]["timestamp"],
            end=panel.iloc[-1]["timestamp"],
            start_asset="ATOM",
            episode_log=episodes,
        )
        enters = [x["date"][:10] for x in episodes if x["action"] == "ENTER_CASH"]
        rearms = [x["date"][:10] for x in episodes if x["action"] == "REARM_AFTER_RECOVERY"]
        self.assertEqual(enters, ["2026-01-04", "2026-01-13"])
        self.assertEqual(rearms, ["2026-01-09"])
        self.assertEqual(r.rearm_count, 1)


if __name__ == "__main__":
    unittest.main()
