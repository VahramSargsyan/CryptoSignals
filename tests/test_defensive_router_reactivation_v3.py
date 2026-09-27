from __future__ import annotations

import unittest

import pandas as pd

from scripts.research_defensive_router_reactivation_v3 import (
    run_router_reactivation_backtest,
)
from scripts.research_relative_rotation_graph_intelligence import PairSignal


class DefensiveRouterReactivationV3Tests(unittest.TestCase):
    def _panel(self):
        dates = pd.date_range("2025-01-01", periods=5, freq="D", tz="UTC")
        rows = []
        for i, ts in enumerate(dates):
            rows.append(
                {
                    "timestamp": ts,
                    "ATOM_open": 1.0,
                    "ATOM_close": 1.0,
                    "TWT_open": 1.0,
                    "TWT_close": 1.0,
                    "TRX_open": 1.0,
                    "TRX_close": 1.0,
                    "breadth_sma200": 1,
                    "lowest_vol_asset": "TRX",
                }
            )
        return pd.DataFrame(rows)

    def test_new_router_transition_after_entry_exits_defense(self):
        panel = self._panel()
        d = list(panel["timestamp"])
        signals = {
            d[3]: [
                PairSignal(
                    date=d[3],
                    from_asset="ATOM",
                    to_asset="TWT",
                    strength=0.2,
                    pair="ATOM/TWT",
                )
            ]
        }
        episodes = []
        result = run_router_reactivation_backtest(
            panel,
            signals,
            start=d[0],
            end=d[-1],
            start_asset="ATOM",
            episode_log=episodes,
        )
        self.assertEqual(result.defensive_transitions, 2)
        self.assertEqual(episodes[-1]["action"], "EXIT_ROUTER_REACTIVATION")
        self.assertEqual(episodes[-1]["shadow_asset"], "TWT")

    def test_transition_pending_before_entry_does_not_immediately_exit(self):
        panel = self._panel()
        d = list(panel["timestamp"])
        signals = {
            d[2]: [
                PairSignal(
                    date=d[2],
                    from_asset="ATOM",
                    to_asset="TWT",
                    strength=0.2,
                    pair="ATOM/TWT",
                )
            ]
        }
        episodes = []
        result = run_router_reactivation_backtest(
            panel,
            signals,
            start=d[0],
            end=d[-1],
            start_asset="ATOM",
            episode_log=episodes,
        )
        self.assertEqual(result.defensive_transitions, 1)
        self.assertEqual([x["action"] for x in episodes], ["ENTER"])


if __name__ == "__main__":
    unittest.main()
