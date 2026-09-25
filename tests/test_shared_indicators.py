from pathlib import Path
import unittest

import pandas as pd

from core.indicators.technical import (
    bollinger_bands,
    build_standard_features,
    candle_body_strength,
    ema_sma_seed,
    macd,
    moving_average_trend_features,
    rsi_wilder,
    simple_moving_average,
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

    def test_ema_uses_sma_seed_then_recursive_smoothing(self):
        values = pd.Series([1, 2, 3, 4, 5], dtype="float64")
        result = ema_sma_seed(values, window=3)
        self.assertTrue(pd.isna(result.iloc[0]))
        self.assertTrue(pd.isna(result.iloc[1]))
        self.assertAlmostEqual(result.iloc[2], 2.0, places=12)
        self.assertAlmostEqual(result.iloc[3], 3.0, places=12)
        self.assertAlmostEqual(result.iloc[4], 4.0, places=12)

    def test_macd_uses_standard_12_26_9_shape_and_identity(self):
        close = pd.Series(range(1, 80), dtype="float64")
        result = macd(close)
        self.assertEqual(
            list(result.columns),
            ["ema_fast", "ema_slow", "macd", "macd_signal", "macd_hist"],
        )
        self.assertTrue(result["macd"].iloc[:25].isna().all())
        self.assertTrue(pd.notna(result["macd"].iloc[25]))
        first_signal = result["macd_signal"].first_valid_index()
        self.assertEqual(first_signal, 33)
        comparable = result["macd_signal"].notna()
        self.assertTrue(
            ((result.loc[comparable, "macd"] - result.loc[comparable, "macd_signal"])
             == result.loc[comparable, "macd_hist"]).all()
        )

    def test_simple_moving_average_uses_exact_window(self):
        values = pd.Series([1, 2, 3, 4, 5], dtype="float64")
        result = simple_moving_average(values, window=3, name="sma_3")
        self.assertTrue(pd.isna(result.iloc[0]))
        self.assertTrue(pd.isna(result.iloc[1]))
        self.assertAlmostEqual(result.iloc[2], 2.0, places=12)
        self.assertAlmostEqual(result.iloc[4], 4.0, places=12)
        self.assertEqual(result.name, "sma_3")

    def test_ma_50_100_200_features_capture_price_and_stack_state(self):
        close = pd.Series(range(1, 251), dtype="float64")
        result = moving_average_trend_features(close)
        self.assertIn("sma_50", result.columns)
        self.assertIn("sma_100", result.columns)
        self.assertIn("sma_200", result.columns)
        self.assertTrue(pd.isna(result["sma_200"].iloc[198]))
        self.assertTrue(pd.notna(result["sma_200"].iloc[199]))
        self.assertTrue(bool(result["close_above_sma_50"].iloc[-1]))
        self.assertTrue(bool(result["close_above_sma_100"].iloc[-1]))
        self.assertTrue(bool(result["close_above_sma_200"].iloc[-1]))
        self.assertTrue(bool(result["ma_bull_stack"].iloc[-1]))
        self.assertFalse(bool(result["ma_bear_stack"].iloc[-1]))

    def test_ma_trend_windows_must_be_strictly_ordered(self):
        with self.assertRaises(ValueError):
            moving_average_trend_features(pd.Series(range(300)), fast_window=50, medium_window=50, slow_window=200)

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
        self.assertIn("macd", features.columns)
        self.assertIn("macd_signal", features.columns)
        self.assertIn("sma_50", features.columns)
        self.assertIn("sma_100", features.columns)
        self.assertIn("sma_200", features.columns)
        self.assertIn("ma_bull_stack", features.columns)
        self.assertIn("ma_bear_stack", features.columns)
        self.assertFalse(features["vahram_close_range"].equals(features["stoch_rsi_k"]))


if __name__ == "__main__":
    unittest.main()
