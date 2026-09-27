from __future__ import annotations

import unittest

import pandas as pd

from scripts.research_one_trx_defense_per_crisis_v4 import (
    run_one_trx_per_crisis_backtest,
)
from scripts.research_relative_rotation_graph_intelligence import ASSETS, PairSignal


class OneTrxDefensePerCrisisV4Tests(unittest.TestCase):
    def _panel(self, breadth: list[int]) -> pd.DataFrame:
        n = len(breadth)
        dates = pd.date_range("2026-01-01", periods=n, freq="D", tz="UTC")
        data = {
            "timestamp": dates,
            "breadth_sma200": breadth,
            "lowest_vol_asset": ["TRX"] * n,
        }
        for asset in ASSETS:
            data[f"{asset}_open"] = [10.0] * n
            data[f"{asset}_close"] = [10.0] * n
        return pd.DataFrame(data)

    def test_direct_trx_to_new_shadow_target(self):
        # Jan1-3 low -> TRX defense Jan4.
        # Signal on Jan5: shadow ATOM -> BNB.
        # Jan6 open must execute directly TRX -> BNB.
        panel = self._panel([2,2,2,2,2,2,2,2,6,6,6,6])
        jan5 = pd.Timestamp("2026-01-05", tz="UTC")
        signals = {
            jan5: [
                PairSignal(
                    date=jan5,
                    from_asset="ATOM",
                    to_asset="BNB",
                    strength=0.2,
                    pair="ATOM/BNB",
                )
            ]
        }
        episodes: list[dict] = []
        daily: list[dict] = []
        r = run_one_trx_per_crisis_backtest(
            panel,
            signals,
            start=panel.iloc[0]["timestamp"],
            end=panel.iloc[-1]["timestamp"],
            start_asset="ATOM",
            episode_log=episodes,
            daily_log=daily,
        )

        exits = [x for x in episodes if x["action"] == "EXIT_TRX_ON_ROUTER_SIGNAL"]
        self.assertEqual(len(exits), 1)
        self.assertEqual(exits[0]["date"][:10], "2026-01-06")
        self.assertEqual(exits[0]["actual_asset"], "BNB")
        self.assertEqual(exits[0]["shadow_asset"], "BNB")
        self.assertEqual(r.actual_transitions, 2)

        day6 = pd.DataFrame(daily)
        day6 = day6[day6["date"].str.startswith("2026-01-06")].iloc[0]
        self.assertEqual(day6["actual_asset"], "BNB")
        self.assertEqual(day6["state"], "POST_TRX_DISARMED")

    def test_no_second_trx_entry_before_recovery_rearm(self):
        panel = self._panel([2,2,2,2,2,2,2,2,2,2,2,2,2,2,2])
        jan5 = pd.Timestamp("2026-01-05", tz="UTC")
        signals = {
            jan5: [
                PairSignal(
                    date=jan5,
                    from_asset="ATOM",
                    to_asset="BNB",
                    strength=0.2,
                    pair="ATOM/BNB",
                )
            ]
        }
        episodes: list[dict] = []
        r = run_one_trx_per_crisis_backtest(
            panel,
            signals,
            start=panel.iloc[0]["timestamp"],
            end=panel.iloc[-1]["timestamp"],
            start_asset="ATOM",
            episode_log=episodes,
        )
        enters = [x for x in episodes if x["action"] == "ENTER_TRX_DEFENSE"]
        self.assertEqual(len(enters), 1)
        self.assertEqual(r.defensive_entries, 1)
        self.assertEqual(r.rearm_count, 0)

    def test_rearm_then_fresh_crisis_can_enter_trx_again(self):
        # Entry Jan4; exit to BNB Jan6.
        # Recovery closes Jan7-Jan9 -> rearm after Jan9.
        # Fresh low closes Jan10-Jan12 -> second TRX entry Jan13.
        breadth = [2,2,2,2,2,2,6,6,6,2,2,2,2,2,2]
        panel = self._panel(breadth)
        jan5 = pd.Timestamp("2026-01-05", tz="UTC")
        signals = {
            jan5: [
                PairSignal(
                    date=jan5,
                    from_asset="ATOM",
                    to_asset="BNB",
                    strength=0.2,
                    pair="ATOM/BNB",
                )
            ]
        }
        episodes: list[dict] = []
        r = run_one_trx_per_crisis_backtest(
            panel,
            signals,
            start=panel.iloc[0]["timestamp"],
            end=panel.iloc[-1]["timestamp"],
            start_asset="ATOM",
            episode_log=episodes,
        )
        enters = [x["date"][:10] for x in episodes if x["action"] == "ENTER_TRX_DEFENSE"]
        rearms = [x["date"][:10] for x in episodes if x["action"] == "REARM_AFTER_RECOVERY"]
        self.assertEqual(enters, ["2026-01-04", "2026-01-13"])
        self.assertEqual(rearms, ["2026-01-09"])
        self.assertEqual(r.rearm_count, 1)

    def test_pre_entry_pending_router_signal_does_not_count_as_exit(self):
        # Signal generated on the same close that completes the entry streak.
        # It updates shadow at Jan4 open, but defense also starts Jan4.
        # It must not instantly exit defense because signal predates entry.
        panel = self._panel([2,2,2,2,2,2,2,2])
        jan3 = pd.Timestamp("2026-01-03", tz="UTC")
        signals = {
            jan3: [
                PairSignal(
                    date=jan3,
                    from_asset="ATOM",
                    to_asset="BNB",
                    strength=0.2,
                    pair="ATOM/BNB",
                )
            ]
        }
        episodes: list[dict] = []
        r = run_one_trx_per_crisis_backtest(
            panel,
            signals,
            start=panel.iloc[0]["timestamp"],
            end=panel.iloc[-1]["timestamp"],
            start_asset="ATOM",
            episode_log=episodes,
        )
        exits = [x for x in episodes if x["action"] == "EXIT_TRX_ON_ROUTER_SIGNAL"]
        self.assertEqual(exits, [])
        self.assertEqual(r.defensive_entries, 1)


if __name__ == "__main__":
    unittest.main()
