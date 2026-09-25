import unittest

import pandas as pd

from core.backtest.event_study import run_event_study, summarize_event_study
from core.contracts.strategy import make_strategy_output


class EventStudyTests(unittest.TestCase):
    def setUp(self):
        self.candles = pd.DataFrame(
            [
                {"timestamp": "2026-01-01T00:00:00Z", "open": 100, "close": 100},
                {"timestamp": "2026-01-02T00:00:00Z", "open": 110, "close": 121},
                {"timestamp": "2026-01-03T00:00:00Z", "open": 120, "close": 100},
                {"timestamp": "2026-01-04T00:00:00Z", "open": 90, "close": 99},
                {"timestamp": "2026-01-05T00:00:00Z", "open": 100, "close": 110},
            ]
        )

    def _output(self, signal="BUY", timestamp="2026-01-01T00:00:00Z"):
        return make_strategy_output(
            strategy_id="TEST_STRATEGY",
            strategy_version="1.0.0",
            symbol="BTCUSDT",
            timeframe="1D",
            timestamp=timestamp,
            signal=signal,
            strength=70,
            reasons=("test",),
            source_commit_sha="abc",
        )

    def test_event_study_uses_next_candle_open_not_signal_close(self):
        result = run_event_study(
            self.candles,
            [self._output("BUY")],
            horizons=(1, 3),
        )
        one = result.loc[result["horizon_candles"] == 1].iloc[0]
        three = result.loc[result["horizon_candles"] == 3].iloc[0]

        self.assertEqual(one["entry_timestamp"], pd.Timestamp("2026-01-02T00:00:00Z"))
        self.assertEqual(one["entry_open"], 110.0)
        self.assertEqual(one["target_timestamp"], pd.Timestamp("2026-01-02T00:00:00Z"))
        self.assertAlmostEqual(one["directional_return"], 0.10)

        self.assertAlmostEqual(three["raw_return"], -0.10)
        self.assertAlmostEqual(three["directional_return"], -0.10)

    def test_sell_direction_is_inverted(self):
        result = run_event_study(
            self.candles,
            [self._output("SELL")],
            horizons=(1,),
        )
        row = result.iloc[0]
        self.assertAlmostEqual(row["raw_return"], 0.10)
        self.assertAlmostEqual(row["directional_return"], -0.10)

    def test_unavailable_future_horizon_is_not_invented(self):
        output = self._output("BUY", timestamp="2026-01-04T00:00:00Z")
        result = run_event_study(self.candles, [output], horizons=(1, 3, 7, 14))
        self.assertEqual(result["horizon_candles"].tolist(), [1])

    def test_summary_reports_count_win_rate_average_and_median(self):
        buy = self._output("BUY")
        result = run_event_study(self.candles, [buy], horizons=(1, 3))
        summary = summarize_event_study(result)
        self.assertEqual(summary["observation_count"].tolist(), [1, 1])
        self.assertEqual(summary["win_rate"].tolist(), [1.0, 0.0])
        self.assertAlmostEqual(summary.iloc[0]["average_directional_return"], 0.10)
        self.assertAlmostEqual(summary.iloc[1]["median_directional_return"], -0.10)


if __name__ == "__main__":
    unittest.main()
