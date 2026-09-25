from pathlib import Path
import unittest

import pandas as pd

from core.indicators.technical import (
    bollinger_bands,
    build_standard_features,
    candle_body_strength,
    rsi_wilder,
    true_stoch_rsi,
    vahram_close_range_oscillator,
)

FIXTURE = Path(__file__).parent / "fixtures" / "vahram_original_v1_saga_2025_03_03_2025_05_02.csv"


class SharedIndicatorTests(unittest.TestCase):
    def test_vahram_close_range_matches_frozen_baseline(self):
        frame = pd.read_csv(FIXTURE)
        value = vahram_close_range_oscillator(frame["Close"])
        row = frame.index[frame["Date"] == "2025-04-02"][0]
        self.assertAlmostEqual(value.iloc[row], 0.30609121518212457, places=12)

    def test_bollinger_matches_frozen_baseline_at_known_row(self):
        frame = pd.read_csv(FIXTURE)
        bands = bollinger_bands(frame["Close"])
        row = frame.index[frame["Date"] == "2025-04-02"][0]
        self.assertAlmostEqual(bands.iloc[row]["bb_lower"], 0.2750359217028319, places=12)
        self.assertAlmostEqual(bands.iloc[row]["bb_upper"], 0.43823407829716804, places=12)

    def test_wilder_rsi_uses_sma_seed_then_recursive_smoothing(self):
        close = pd.Series([1, 2, 3, 2, 4, 5], dtype="float64")
        rsi = rsi_wilder(close, window=3)
        self.assertAlmostEqual(rsi.iloc[3], 66.66666666666667, places=12)
        self.assertAlmostEqual(rsi.iloc[4], 83.33333333333333, places=12)

    def test_true_stoch_rsi_is_based_on_rsi_not_close_range(self):
        close = pd.Series(
            [1, 2, 3, 2, 4, 5, 4, 6, 5, 7, 6, 8, 7, 9, 8, 10, 9, 11, 10, 12],
            dtype="float64",
        )
        result = true_stoch_rsi(
            close,
            rsi_window=3,
            stoch_window=3,
            smooth_k=2,
            smooth_d=2,
        )
        expected_raw = (
            (result["rsi"] - result["rsi"].rolling(3).min())
            / (result["rsi"].rolling(3).max() - result["rsi"].rolling(3).min())
        ) * 100.0
        pd.testing.assert_series_equal(
            result["stoch_rsi_raw"],
            expected_raw.rename("stoch_rsi_raw"),
        )
        legacy = vahram_close_range_oscillator(close, window=3, smooth=2)
        comparable = result["stoch_rsi_k"].notna() & legacy.notna()
        self.assertTrue((result.loc[comparable, "stoch_rsi_k"] != legacy.loc[comparable]).any())

    def test_candle_body_strength_handles_zero_range(self):
        strength = candle_body_strength(
            pd.Series([10, 10]),
            pd.Series([12, 10]),
            pd.Series([8, 10]),
            pd.Series([11, 10]),
        )
        self.assertAlmostEqual(strength.iloc[0], 0.25)
        self.assertEqual(strength.iloc[1], 0.0)

    def test_standard_feature_builder_keeps_distinct_legacy_and_true_stochrsi(self):
        frame = pd.read_csv(FIXTURE).rename(
            columns={
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
            }
        )
        features = build_standard_features(frame)
        self.assertIn("vahram_close_range", features.columns)
        self.assertIn("stoch_rsi_k", features.columns)
        self.assertIn("rsi", features.columns)
        self.assertFalse(features["vahram_close_range"].equals(features["stoch_rsi_k"]))


if __name__ == "__main__":
    unittest.main()
