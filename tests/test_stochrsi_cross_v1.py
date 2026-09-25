import unittest
from unittest.mock import patch

import pandas as pd

from strategies.crypto.stochrsi_cross.strategy import (
    DEFAULT_RSI_WINDOW,
    DEFAULT_SMOOTH_D,
    DEFAULT_SMOOTH_K,
    DEFAULT_STOCH_WINDOW,
    STRATEGY_ID,
    _cross_signal,
    generate_stochrsi_cross_outputs,
)


class StochRsiCrossV1Tests(unittest.TestCase):
    def test_strict_cross_predicate_matches_source_semantics(self):
        self.assertEqual(_cross_signal(10, 20, 30, 20), "BUY")
        self.assertEqual(_cross_signal(30, 20, 10, 20), "SELL")
        self.assertIsNone(_cross_signal(20, 20, 30, 20))
        self.assertIsNone(_cross_signal(10, 20, 20, 20))
        self.assertIsNone(_cross_signal(float("nan"), 20, 30, 20))

    def test_default_parameters_match_reviewed_oss_candidate(self):
        self.assertEqual(
            (DEFAULT_RSI_WINDOW, DEFAULT_STOCH_WINDOW, DEFAULT_SMOOTH_K, DEFAULT_SMOOTH_D),
            (14, 14, 10, 3),
        )

    @patch("strategies.crypto.stochrsi_cross.strategy.true_stoch_rsi")
    def test_generator_emits_only_cross_events_with_project_identity(self, mocked_stoch):
        candles = pd.DataFrame(
            [
                {"timestamp": "2025-01-01T00:00:00Z", "open": 1, "high": 2, "low": 0.5, "close": 1.0, "volume": 10},
                {"timestamp": "2025-01-02T00:00:00Z", "open": 1, "high": 2, "low": 0.5, "close": 1.1, "volume": 10},
                {"timestamp": "2025-01-03T00:00:00Z", "open": 1, "high": 2, "low": 0.5, "close": 1.2, "volume": 10},
                {"timestamp": "2025-01-04T00:00:00Z", "open": 1, "high": 2, "low": 0.5, "close": 1.1, "volume": 10},
            ]
        )
        mocked_stoch.return_value = pd.DataFrame(
            {
                "rsi": [50, 50, 50, 50],
                "stoch_rsi_raw": [20, 40, 60, 40],
                "stoch_rsi_k": [10, 30, 35, 15],
                "stoch_rsi_d": [20, 20, 25, 25],
            }
        )

        outputs = generate_stochrsi_cross_outputs(
            candles,
            symbol="BTCUSDT",
            timeframe="1D",
            source_commit_sha="test-sha",
            run_id="RUN-TEST",
        )

        self.assertEqual(
            [(item.timestamp.strftime("%Y-%m-%d"), item.signal) for item in outputs],
            [("2025-01-02", "BUY"), ("2025-01-04", "SELL")],
        )
        self.assertTrue(all(item.strategy_id == STRATEGY_ID for item in outputs))
        self.assertTrue(all(item.strategy_version == "1.0.0" for item in outputs))
        self.assertTrue(all(item.strength == 100.0 for item in outputs))
        mocked_stoch.assert_called_once()
        self.assertEqual(mocked_stoch.call_args.kwargs["rsi_window"], 14)
        self.assertEqual(mocked_stoch.call_args.kwargs["stoch_window"], 14)
        self.assertEqual(mocked_stoch.call_args.kwargs["smooth_k"], 10)
        self.assertEqual(mocked_stoch.call_args.kwargs["smooth_d"], 3)


if __name__ == "__main__":
    unittest.main()
