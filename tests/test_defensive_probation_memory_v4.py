from __future__ import annotations

import unittest

import pandas as pd

from scripts.research_defensive_probation_memory_v4 import (
    run_probation_memory_backtest,
)
from scripts.research_relative_rotation_graph_intelligence import PairSignal


class DefensiveProbationMemoryV4Tests(unittest.TestCase):
    def _panel(self, days: int = 22, breadth: int = 1) -> pd.DataFrame:
        dates = pd.date_range("2025-01-01", periods=days, freq="D", tz="UTC")
        rows = []
        for ts in dates:
            row = {
                "timestamp": ts,
                "breadth_sma200": breadth,
                "lowest_vol_asset": "TRX",
            }
            for asset in ("ATOM", "TWT", "AAVE", "TRX"):
                row[f"{asset}_open"] = 1.0
                row[f"{asset}_close"] = 1.0
            rows.append(row)
        return pd.DataFrame(rows)

    def test_failed_probe_returns_to_trx_and_keeps_shadow_memory(self):
        panel = self._panel()
        d = list(panel["timestamp"])
        signals = {
            d[4]: [
                PairSignal(
                    date=d[4],
                    from_asset="ATOM",
                    to_asset="TWT",
                    strength=0.2,
                    pair="ATOM/TWT",
                )
            ]
        }
        episodes = []
        result = run_probation_memory_backtest(
            panel,
            signals,
            start=d[0],
            end=d[-1],
            start_asset="ATOM",
            episode_log=episodes,
        )
        actions = [x["action"] for x in episodes]
        self.assertIn("PROBE_START", actions)
        self.assertIn("PROBE_FAIL_FALLBACK", actions)
        fallback = [x for x in episodes if x["action"] == "PROBE_FAIL_FALLBACK"][0]
        self.assertEqual(fallback["actual_asset"], "TRX")
        self.assertEqual(fallback["shadow_asset"], "TWT")
        self.assertEqual(result.probes_failed, 1)

    def test_shadow_transitions_during_probation_do_not_reset_14_day_clock(self):
        panel = self._panel()
        d = list(panel["timestamp"])
        signals = {
            d[4]: [
                PairSignal(
                    date=d[4],
                    from_asset="ATOM",
                    to_asset="TWT",
                    strength=0.2,
                    pair="ATOM/TWT",
                )
            ],
            d[8]: [
                PairSignal(
                    date=d[8],
                    from_asset="TWT",
                    to_asset="AAVE",
                    strength=0.2,
                    pair="TWT/AAVE",
                )
            ],
        }
        episodes = []
        result = run_probation_memory_backtest(
            panel,
            signals,
            start=d[0],
            end=d[-1],
            start_asset="ATOM",
            episode_log=episodes,
        )
        fallback = [x for x in episodes if x["action"] == "PROBE_FAIL_FALLBACK"][0]
        self.assertEqual(fallback["shadow_asset"], "AAVE")
        self.assertEqual(fallback["actual_asset"], "TRX")
        self.assertEqual(result.probes_failed, 1)
        self.assertEqual(result.probation_days, 14)

    def test_broad_recovery_during_probation_marks_success_without_forced_trade(self):
        panel = self._panel()
        d = list(panel["timestamp"])
        panel.loc[panel["timestamp"].isin([d[9], d[10], d[11]]), "breadth_sma200"] = 5
        signals = {
            d[4]: [
                PairSignal(
                    date=d[4],
                    from_asset="ATOM",
                    to_asset="TWT",
                    strength=0.2,
                    pair="ATOM/TWT",
                )
            ]
        }
        episodes = []
        result = run_probation_memory_backtest(
            panel,
            signals,
            start=d[0],
            end=d[-1],
            start_asset="ATOM",
            episode_log=episodes,
        )
        actions = [x["action"] for x in episodes]
        self.assertIn("PROBE_SUCCESS_BROAD_RECOVERY", actions)
        self.assertNotIn("PROBE_FAIL_FALLBACK", actions)
        self.assertEqual(result.probes_succeeded, 1)
        self.assertEqual(result.probes_failed, 0)


if __name__ == "__main__":
    unittest.main()
