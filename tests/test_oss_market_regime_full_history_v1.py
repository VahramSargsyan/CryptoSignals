from __future__ import annotations

import unittest

import pandas as pd

from scripts.research_oss_market_regime_full_history_v1 import (
    build_episode_evidence,
    lead_window_summary,
)


class OssMarketRegimeFullHistoryV1Tests(unittest.TestCase):
    def test_lead_windows_are_nested_and_preregistered(self):
        leads = pd.DataFrame(
            [
                {
                    "signal_date": pd.Timestamp("2026-01-10"),
                    "found": True,
                    "lead_days": 0,
                    "within_0d": True,
                    "within_7d": True,
                    "within_14d": True,
                    "within_30d": True,
                },
                {
                    "signal_date": pd.Timestamp("2026-02-10"),
                    "found": True,
                    "lead_days": 10,
                    "within_0d": False,
                    "within_7d": False,
                    "within_14d": True,
                    "within_30d": True,
                },
                {
                    "signal_date": pd.Timestamp("2026-03-10"),
                    "found": False,
                    "lead_days": None,
                    "within_0d": False,
                    "within_7d": False,
                    "within_14d": False,
                    "within_30d": False,
                },
            ]
        )
        s = lead_window_summary(leads)
        self.assertEqual(s["signals"], 3)
        self.assertEqual(s["within_0d_count"], 1)
        self.assertEqual(s["within_7d_count"], 1)
        self.assertEqual(s["within_14d_count"], 2)
        self.assertEqual(s["within_30d_count"], 2)
        self.assertAlmostEqual(s["median_lead_days_when_found"], 5.0)

    def test_episode_evidence_uses_latest_preceding_target_regime(self):
        dates = pd.date_range("2026-01-01", periods=12, freq="D")
        daily = pd.DataFrame(
            {
                "date": dates,
                "market_regime": [
                    "MIXED",
                    "BROAD_RISK_OFF",
                    "MIXED",
                    "BROAD_RISK_OFF",
                    "MIXED",
                    "MIXED",
                    "BROAD_RISK_ON",
                    "MIXED",
                    "BROAD_RISK_ON",
                    "MIXED",
                    "MIXED",
                    "MIXED",
                ],
                "market_regime_confidence": ["low"] * 12,
                "crypto_mode": ["NORMAL"] * 4 + ["DEFENSIVE"] * 6 + ["NORMAL"] * 2,
            }
        )
        episodes = pd.DataFrame(
            [
                {
                    "entry_signal_date": pd.Timestamp("2026-01-04"),
                    "entry_execution_date": pd.Timestamp("2026-01-05"),
                    "entry_breadth": 2,
                    "exit_signal_date": pd.Timestamp("2026-01-10"),
                    "exit_execution_date": pd.Timestamp("2026-01-11"),
                    "exit_breadth": 6,
                }
            ]
        )
        episode_table, entries, exits, _ = build_episode_evidence(daily, episodes)
        self.assertEqual(int(entries.iloc[0]["lead_days"]), 0)
        self.assertEqual(int(exits.iloc[0]["lead_days"]), 1)
        self.assertEqual(episode_table.iloc[0]["entry_market_regime"], "BROAD_RISK_OFF")
        self.assertEqual(episode_table.iloc[0]["exit_market_regime"], "MIXED")


if __name__ == "__main__":
    unittest.main()
