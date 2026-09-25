import unittest
from unittest.mock import patch

import pandas as pd

from strategies.crypto.weighted_multi_signal.strategy import (
    BLOCK_WEIGHT,
    MIN_TRIGGERED_BLOCKS,
    SIGNAL_THRESHOLD,
    STRATEGY_ID,
    _signal_from_score,
    generate_weighted_multi_signal_outputs,
    score_weighted_row,
)


class WeightedMultiSignalV1Tests(unittest.TestCase):
    def test_frozen_v1_thresholds_are_equal_weighted(self):
        self.assertEqual(BLOCK_WEIGHT, 20.0)
        self.assertEqual(SIGNAL_THRESHOLD, 60.0)
        self.assertEqual(MIN_TRIGGERED_BLOCKS, 3)

    def test_bullish_three_block_threshold_fires_buy(self):
        row = pd.Series(
            {
                "open": 100.0,
                "close": 110.0,
                "volume": 120.0,
                "volume_ma": 100.0,
                "candle_body_strength": 0.7,
                "sma_200": 90.0,
                "ma_bull_stack": True,
                "ma_bear_stack": False,
                "macd": 2.0,
                "macd_signal": 1.0,
                "stoch_rsi_k": 40.0,
                "stoch_rsi_d": 60.0,
                "bb_lower": 80.0,
                "bb_upper": 130.0,
            }
        )
        score = score_weighted_row(row)
        self.assertEqual(score.bullish_score, 60.0)
        self.assertEqual(set(score.bullish_blocks), {"ma_trend", "macd", "volume_candle"})
        self.assertEqual(score.bearish_score, 20.0)
        self.assertEqual(_signal_from_score(score), "BUY")

    def test_bearish_three_block_threshold_fires_sell(self):
        row = pd.Series(
            {
                "open": 110.0,
                "close": 90.0,
                "volume": 150.0,
                "volume_ma": 100.0,
                "candle_body_strength": 0.8,
                "sma_200": 100.0,
                "ma_bull_stack": False,
                "ma_bear_stack": True,
                "macd": -2.0,
                "macd_signal": -1.0,
                "stoch_rsi_k": 70.0,
                "stoch_rsi_d": 40.0,
                "bb_lower": 70.0,
                "bb_upper": 130.0,
            }
        )
        score = score_weighted_row(row)
        self.assertEqual(score.bearish_score, 60.0)
        self.assertEqual(set(score.bearish_blocks), {"ma_trend", "macd", "volume_candle"})
        self.assertEqual(score.bullish_score, 20.0)
        self.assertEqual(_signal_from_score(score), "SELL")

    def test_two_blocks_do_not_create_signal(self):
        row = pd.Series(
            {
                "open": 100.0,
                "close": 101.0,
                "volume": 90.0,
                "volume_ma": 100.0,
                "candle_body_strength": 0.2,
                "sma_200": 90.0,
                "ma_bull_stack": True,
                "ma_bear_stack": False,
                "macd": 2.0,
                "macd_signal": 1.0,
                "stoch_rsi_k": 50.0,
                "stoch_rsi_d": 50.0,
                "bb_lower": 80.0,
                "bb_upper": 120.0,
            }
        )
        score = score_weighted_row(row)
        self.assertEqual(score.bullish_score, 40.0)
        self.assertIsNone(_signal_from_score(score))

    @patch("strategies.crypto.weighted_multi_signal.strategy.build_standard_features")
    def test_generator_emits_contract_outputs_only_when_threshold_met(self, mocked_features):
        candles = pd.DataFrame(
            [
                {"timestamp": "2025-01-01T00:00:00Z", "open": 100, "high": 112, "low": 99, "close": 110, "volume": 150},
                {"timestamp": "2025-01-02T00:00:00Z", "open": 110, "high": 111, "low": 88, "close": 90, "volume": 160},
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
                    "stoch_rsi_k": 50,
                    "stoch_rsi_d": 50,
                    "bb_lower": 80,
                    "bb_upper": 130,
                    "volume_ma": 100,
                    "candle_body_strength": 0.8,
                },
                {
                    "sma_200": 100,
                    "ma_bull_stack": False,
                    "ma_bear_stack": True,
                    "macd": -2,
                    "macd_signal": -1,
                    "stoch_rsi_k": 50,
                    "stoch_rsi_d": 50,
                    "bb_lower": 70,
                    "bb_upper": 130,
                    "volume_ma": 100,
                    "candle_body_strength": 0.8,
                },
            ]
        )

        outputs = generate_weighted_multi_signal_outputs(
            candles,
            symbol="BTCUSDT",
            timeframe="1D",
            source_commit_sha="test-sha",
            run_id="RUN-TEST",
        )

        self.assertEqual([item.signal for item in outputs], ["BUY", "SELL"])
        self.assertTrue(all(item.strategy_id == STRATEGY_ID for item in outputs))
        self.assertTrue(all(item.strength == 60.0 for item in outputs))
        self.assertTrue(all("triggered_blocks_3" in item.reasons for item in outputs))


if __name__ == "__main__":
    unittest.main()
