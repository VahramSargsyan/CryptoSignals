import unittest
from unittest.mock import patch

import pandas as pd

from strategies.crypto.weighted_multi_signal_v2.strategy import (
    CORE_BASE_STRENGTH,
    MA_ALIGNED_STRENGTH,
    MIN_CANDLE_BODY_STRENGTH,
    STRATEGY_ID,
    _core_direction,
    _ma_context,
    generate_weighted_multi_signal_v2_outputs,
)


class WeightedMultiSignalV2Tests(unittest.TestCase):
    def test_frozen_v2_constants(self):
        self.assertEqual(CORE_BASE_STRENGTH, 80.0)
        self.assertEqual(MA_ALIGNED_STRENGTH, 100.0)
        self.assertEqual(MIN_CANDLE_BODY_STRENGTH, 0.5)

    def test_buy_requires_all_three_core_confirmations(self):
        row = pd.Series(
            {
                "open": 100.0,
                "close": 110.0,
                "volume": 150.0,
                "volume_ma": 100.0,
                "candle_body_strength": 0.7,
                "macd": 2.0,
                "macd_signal": 1.0,
                "stoch_rsi_k": 60.0,
                "stoch_rsi_d": 40.0,
            }
        )
        self.assertEqual(_core_direction(row), "BUY")

        row["volume"] = 90.0
        self.assertIsNone(_core_direction(row))

    def test_sell_requires_all_three_core_confirmations(self):
        row = pd.Series(
            {
                "open": 110.0,
                "close": 90.0,
                "volume": 150.0,
                "volume_ma": 100.0,
                "candle_body_strength": 0.8,
                "macd": -2.0,
                "macd_signal": -1.0,
                "stoch_rsi_k": 30.0,
                "stoch_rsi_d": 50.0,
            }
        )
        self.assertEqual(_core_direction(row), "SELL")

        row["stoch_rsi_k"] = 70.0
        self.assertIsNone(_core_direction(row))

    def test_ma_context_never_creates_core_signal(self):
        row = pd.Series(
            {
                "close": 120.0,
                "sma_200": 100.0,
                "ma_bull_stack": True,
                "ma_bear_stack": False,
            }
        )
        self.assertEqual(_ma_context(row, "BUY"), "MA_CONTEXT_ALIGNED")

    @patch("strategies.crypto.weighted_multi_signal_v2.strategy.build_standard_features")
    def test_generator_uses_ma_only_for_strength_context(self, mocked_features):
        candles = pd.DataFrame(
            [
                {"timestamp": "2026-01-01T00:00:00Z", "open": 100, "high": 112, "low": 99, "close": 110, "volume": 150},
                {"timestamp": "2026-01-02T00:00:00Z", "open": 100, "high": 112, "low": 99, "close": 110, "volume": 150},
            ]
        )
        mocked_features.return_value = pd.DataFrame(
            [
                {
                    "sma_200": 90,
                    "ma_bull_stack": True,
                    "ma_bear_stack": False,
                    "macd": 2,
                    "macd_signal": 1,
                    "stoch_rsi_k": 60,
                    "stoch_rsi_d": 40,
                    "volume_ma": 100,
                    "candle_body_strength": 0.8,
                },
                {
                    "sma_200": 120,
                    "ma_bull_stack": False,
                    "ma_bear_stack": False,
                    "macd": 2,
                    "macd_signal": 1,
                    "stoch_rsi_k": 60,
                    "stoch_rsi_d": 40,
                    "volume_ma": 100,
                    "candle_body_strength": 0.8,
                },
            ]
        )

        outputs = generate_weighted_multi_signal_v2_outputs(
            candles,
            symbol="BTCUSDT",
            timeframe="1D",
            source_commit_sha="test-sha",
            run_id="RUN-TEST",
        )

        self.assertEqual([item.signal for item in outputs], ["BUY", "BUY"])
        self.assertEqual([item.strength for item in outputs], [100.0, 80.0])
        self.assertTrue(all(item.strategy_id == STRATEGY_ID for item in outputs))


if __name__ == "__main__":
    unittest.main()
