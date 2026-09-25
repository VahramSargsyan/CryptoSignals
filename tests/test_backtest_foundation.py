import unittest

import pandas as pd

from core.backtest.contracts import BacktestRunManifest, ExecutionPolicy
from core.backtest.execution import next_candle_open_fill


class BacktestFoundationTests(unittest.TestCase):
    def setUp(self):
        self.candles = pd.DataFrame(
            [
                {"timestamp": pd.Timestamp("2026-01-01T00:00:00Z"), "open": 100.0, "high": 110.0, "low": 95.0, "close": 105.0, "volume": 1000},
                {"timestamp": pd.Timestamp("2026-01-02T00:00:00Z"), "open": 108.0, "high": 115.0, "low": 106.0, "close": 112.0, "volume": 1200},
            ]
        )

    def test_default_execution_is_next_candle_open(self):
        policy = ExecutionPolicy(fee_bps=10, slippage_bps=5)
        fill = next_candle_open_fill(self.candles, signal_position=0, side="BUY", policy=policy)
        self.assertEqual(fill.signal_timestamp, pd.Timestamp("2026-01-01T00:00:00Z"))
        self.assertEqual(fill.entry_timestamp, pd.Timestamp("2026-01-02T00:00:00Z"))
        self.assertEqual(fill.raw_open_price, 108.0)
        self.assertAlmostEqual(fill.execution_price, 108.054, places=9)

    def test_last_candle_cannot_execute_without_future_data(self):
        with self.assertRaises(IndexError):
            next_candle_open_fill(self.candles, signal_position=1, side="BUY", policy=ExecutionPolicy())

    def test_manifest_run_id_is_deterministic_and_records_cost_assumptions(self):
        manifest = BacktestRunManifest(
            strategy_id="VAHRAM_ORIGINAL_V1",
            strategy_version="1.0.0",
            source_commit_sha="abc123",
            dataset_id="BINANCE:SAGAUSDT:1D:test",
            symbol="SAGAUSDT",
            timeframe="1D",
            period_start="2025-01-01",
            period_end="2025-12-31",
            execution=ExecutionPolicy(fee_bps=10, slippage_bps=5),
            parameters={"window": 20},
        )
        clone = BacktestRunManifest(**{**manifest.__dict__})
        self.assertEqual(manifest.run_id, clone.run_id)
        self.assertIn('"entry_rule":"NEXT_CANDLE_OPEN"', manifest.canonical_payload())
        self.assertIn('"fee_bps":10', manifest.canonical_payload())
        self.assertIn('"slippage_bps":5', manifest.canonical_payload())


if __name__ == "__main__":
    unittest.main()
