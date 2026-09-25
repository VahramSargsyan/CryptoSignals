from pathlib import Path
import unittest

import pandas as pd

from strategies.crypto.vahram_original.strategy import (
    LEGACY_SOURCE_COMMIT_SHA,
    STRATEGY_ID,
    apply_legacy_indicators,
    scan_strength_signals,
)

FIXTURE = Path(__file__).parent / "fixtures" / "vahram_original_v1_saga_2025_03_03_2025_05_02.csv"


class VahramOriginalV1RegressionTests(unittest.TestCase):
    def setUp(self):
        self.frame = pd.read_csv(FIXTURE)
        self.frame["Date"] = pd.to_datetime(self.frame["Date"])

    def test_identity_is_frozen(self):
        self.assertEqual(STRATEGY_ID, "VAHRAM_ORIGINAL_V1")
        self.assertEqual(LEGACY_SOURCE_COMMIT_SHA, "e363ffd8a4d29492bb6e685181770c39ca3047d7")

    def test_historical_strength_signals_match_frozen_fixture(self):
        signals = scan_strength_signals(self.frame, symbol="SAGAUSDT")
        actual = signals[["Date", "Signal", "Strength", "Action", "Change %"]].to_dict("records")
        expected = [
            {"Date": "2025-04-02", "Signal": "BUY", "Strength": 67.2, "Action": "Enter", "Change %": 50},
            {"Date": "2025-04-25", "Signal": "SELL", "Strength": 74.9, "Action": "Reduce", "Change %": 50},
            {"Date": "2025-04-26", "Signal": "SELL", "Strength": 87.3, "Action": "Exit", "Change %": 100},
            {"Date": "2025-04-28", "Signal": "SELL", "Strength": 66.7, "Action": "Reduce", "Change %": 50},
        ]
        self.assertEqual(actual, expected)

    def test_historical_stochrsi_formula_is_close_range_normalization(self):
        enriched = apply_legacy_indicators(self.frame)
        row = enriched.loc[enriched["Date"] == pd.Timestamp("2025-04-02")].iloc[0]
        self.assertAlmostEqual(row["StochRSI"], 0.30609121518212457, places=12)
        self.assertAlmostEqual(row["Lower"], 0.2750359217028319, places=12)
        self.assertAlmostEqual(row["Upper"], 0.43823407829716804, places=12)


if __name__ == "__main__":
    unittest.main()
