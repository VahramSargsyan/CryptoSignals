from __future__ import annotations

import unittest

import pandas as pd

from scripts.research_official_m2_vs_crypto_stress_v1 import (
    align_m2_to_crypto,
    build_causal_m2,
    release_map,
)


class OfficialM2VsCryptoStressV1Tests(unittest.TestCase):
    def test_release_map_uses_actual_known_dates(self):
        x = release_map()
        row = x[x["observation_month_end"] == pd.Timestamp("2022-12-31")].iloc[0]
        self.assertEqual(row["release_date"], pd.Timestamp("2023-01-24"))
        self.assertEqual(row["available_date"], pd.Timestamp("2023-01-25"))

        row2 = x[x["observation_month_end"] == pd.Timestamp("2024-11-30")].iloc[0]
        self.assertEqual(row2["release_date"], pd.Timestamp("2024-12-26"))
        self.assertEqual(row2["available_date"], pd.Timestamp("2024-12-27"))

    def test_builds_3_6_12_month_changes(self):
        dates = pd.date_range("2021-01-31", periods=24, freq="ME")
        raw = pd.DataFrame({
            "period": dates.strftime("%Y-%m-%d"),
            "m2_value": [100.0 + i for i in range(24)],
            "period_date": dates,
        })
        x = build_causal_m2(raw)
        self.assertIn("m2_change_3m", x.columns)
        self.assertIn("m2_change_6m", x.columns)
        self.assertIn("m2_change_12m", x.columns)
        self.assertAlmostEqual(
            float(x.iloc[12]["m2_change_12m"]),
            112.0 / 100.0 - 1.0,
            places=12,
        )

    def test_asof_does_not_expose_m2_before_availability(self):
        crypto = pd.DataFrame({
            "date": pd.to_datetime(["2023-01-24", "2023-01-25", "2023-01-26"]),
            "breadth": [4, 4, 4],
            "crypto_mode": ["NORMAL", "NORMAL", "NORMAL"],
        })
        m2 = pd.DataFrame({
            "observation_month_end": [pd.Timestamp("2022-12-31")],
            "release_date": [pd.Timestamp("2023-01-24")],
            "available_date": [pd.Timestamp("2023-01-25")],
            "m2_value": [21000.0],
            "m2_change_3m": [0.01],
            "m2_change_6m": [0.02],
            "m2_change_12m": [0.03],
        })
        x = align_m2_to_crypto(crypto, m2)
        self.assertTrue(pd.isna(x.iloc[0]["m2_value"]))
        self.assertEqual(float(x.iloc[1]["m2_value"]), 21000.0)


if __name__ == "__main__":
    unittest.main()
