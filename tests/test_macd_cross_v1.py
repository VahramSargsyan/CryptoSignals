import unittest
from unittest.mock import patch

import pandas as pd

from strategies.crypto.macd_cross.strategy import (
    DEFAULT_FAST_WINDOW,
    DEFAULT_SIGNAL_WINDOW,
    DEFAULT_SLOW_WINDOW,
    STRATEGY_ID,
    _cross_signal,
    generate_macd_cross_outputs,
)


class MacdCrossV1Tests(unittest.TestCase):
    def test_strict_cross_predicate_matches_reviewed_source_semantics(self):
        self.assertEqual(_cross_signal(-2, -1, 1, 0), "BUY")
        self.assertEqual(_cross_signal(2, 1, -1, 0), "SELL")
        self.assertIsNone(_cross_signal(1, 1, 2, 1))
        self.assertIsNone(_cross_signal(-1, 0, 0, 0))
        self.assertIsNone(_cross_signal(float("nan"), 0, 1, 0))

    def test_default_parameters_match_reviewed_oss_candidate(self):
        self.assertEqual(
            (DEFAULT_FAST_WINDOW, DEFAULT_SLOW_WINDOW, DEFAULT_SIGNAL_WINDOW),
            (12, 26, 9),
        )

    @patch("strategies.crypto.macd_cross.strategy.macd")
    def test_generator_emits_only_cross_events_with_project_identity(self, mocked_macd):
        candles = pd.DataFrame(
            [
                {"timestamp": "2025-01-01T00:00:00Z", "open": 1, "high": 2, "low": 0.5, "close": 1.0, "volume": 10},
                {"timestamp": "2025-01-02T00:00:00Z", "open": 1, "high": 2, "low": 0.5, "close": 1.1, "volume": 10},
                {"timestamp": "2025-01-03T00:00:00Z", "open": 1, "high": 2, "low": 0.5, "close": 1.2, "volume": 10},
                {"timestamp": "2025-01-04T00:00:00Z", "open": 1, "high": 2, "low": 0.5, "close": 1.1, "volume": 10},
            ]
        )
        mocked_macd.return_value = pd.DataFrame(
            {
                "ema_fast": [1, 1, 1, 1],
                "ema_slow": [1, 1, 1, 1],
                "macd": [-2, 1, 2, -1],
                "macd_signal": [-1, 0, 1, 0],
                "macd_hist": [-1, 1, 1, -1],
            }
        )

        outputs = generate_macd_cross_outputs(
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
        mocked_macd.assert_called_once()
        self.assertEqual(mocked_macd.call_args.kwargs["fast_window"], 12)
        self.assertEqual(mocked_macd.call_args.kwargs["slow_window"], 26)
        self.assertEqual(mocked_macd.call_args.kwargs["signal_window"], 9)


if __name__ == "__main__":
    unittest.main()
