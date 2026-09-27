from __future__ import annotations

import unittest

import pandas as pd

from scripts.research_official_fed_net_liquidity_vs_crypto_stress_v1 import (
    build_net_liquidity,
    sign_stats,
)


class OfficialFedNetLiquidityVsCryptoStressV1Tests(unittest.TestCase):
    def test_net_liquidity_is_same_date_and_available_friday(self):
        h41 = pd.DataFrame(
            {
                "observation_date": pd.to_datetime(["2026-01-07", "2026-01-14"]),
                "fed_total_assets_usd_mn": [6600000.0, 6610000.0],
                "tga_usd_mn": [700000.0, 710000.0],
            }
        )
        rrp = pd.DataFrame(
            {
                "observation_date": pd.to_datetime(["2026-01-07", "2026-01-14"]),
                "rrp_usd_mn": [380000.0, 390000.0],
            }
        )
        out = build_net_liquidity(h41, rrp)
        self.assertEqual(len(out), 2)
        self.assertEqual(out.iloc[0]["net_liquidity_usd_mn"], 5520000.0)
        self.assertEqual(
            out.iloc[0]["availability_date"], pd.Timestamp("2026-01-09")
        )

    def test_same_date_contract_drops_missing_rrp_week(self):
        h41 = pd.DataFrame(
            {
                "observation_date": pd.to_datetime(["2026-01-07", "2026-01-14"]),
                "fed_total_assets_usd_mn": [1.0, 2.0],
                "tga_usd_mn": [0.1, 0.2],
            }
        )
        rrp = pd.DataFrame(
            {
                "observation_date": pd.to_datetime(["2026-01-07"]),
                "rrp_usd_mn": [0.05],
            }
        )
        out = build_net_liquidity(h41, rrp)
        self.assertEqual(list(out["observation_date"]), [pd.Timestamp("2026-01-07")])

    def test_sign_stats_uses_state_conditioned_baselines(self):
        daily = pd.DataFrame(
            {
                "crypto_mode": ["NORMAL", "NORMAL", "DEFENSIVE", "DEFENSIVE"],
                "net_liquidity_change_4w_usd_mn": [-1.0, 1.0, 1.0, -1.0],
                "net_liquidity_change_13w_usd_mn": [-1.0, 1.0, 1.0, -1.0],
                "net_liquidity_change_26w_usd_mn": [-1.0, 1.0, 1.0, -1.0],
            }
        )
        snapshots = pd.DataFrame(
            {
                "event_type": ["ENTRY_SIGNAL", "EXIT_SIGNAL"],
                "net_liquidity_change_4w_usd_mn": [-1.0, 1.0],
                "net_liquidity_change_13w_usd_mn": [-1.0, 1.0],
                "net_liquidity_change_26w_usd_mn": [-1.0, 1.0],
            }
        )
        stats = sign_stats(daily, snapshots)
        self.assertEqual(stats["entry"]["4w"]["event_rate"], 1.0)
        self.assertEqual(stats["entry"]["4w"]["normal_baseline_rate"], 0.5)
        self.assertEqual(stats["exit"]["4w"]["event_rate"], 1.0)
        self.assertEqual(stats["exit"]["4w"]["defensive_baseline_rate"], 0.5)


if __name__ == "__main__":
    unittest.main()
