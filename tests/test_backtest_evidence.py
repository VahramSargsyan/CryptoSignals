import json
from pathlib import Path
import tempfile
import unittest

import pandas as pd

from core.backtest.contracts import BacktestRunManifest, ExecutionPolicy
from core.backtest.trading import ENGINE_NAME, LongOnlyTradingPolicy, run_long_only_backtest
from core.contracts.strategy import make_strategy_output
from core.evidence.backtest_record import build_backtest_evidence, write_backtest_evidence


class BacktestEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.candles = pd.DataFrame(
            [
                {"timestamp": "2026-01-01T00:00:00Z", "open": 100, "close": 100},
                {"timestamp": "2026-01-02T00:00:00Z", "open": 110, "close": 120},
                {"timestamp": "2026-01-03T00:00:00Z", "open": 120, "close": 115},
            ]
        )
        self.policy = LongOnlyTradingPolicy()
        self.manifest = BacktestRunManifest(
            strategy_id="TEST",
            strategy_version="1.0.0",
            source_commit_sha="abc123",
            dataset_id="DATASET-1",
            symbol="BTCUSDT",
            timeframe="1D",
            period_start="2026-01-01",
            period_end="2026-01-03",
            execution=ExecutionPolicy(fee_bps=10, slippage_bps=5),
            parameters={"example": 1},
            validation_type="RESEARCH",
            engine_name=ENGINE_NAME,
            engine_config=self.policy.to_config(),
        )
        self.output = make_strategy_output(
            strategy_id="TEST",
            strategy_version="1.0.0",
            symbol="BTCUSDT",
            timeframe="1D",
            timestamp="2026-01-01T00:00:00Z",
            signal="BUY",
            strength=70,
            reasons=("test",),
            source_commit_sha="abc123",
            run_id=self.manifest.run_id,
        )

    def test_evidence_contains_manifest_metrics_policy_and_trades(self):
        result = run_long_only_backtest(
            self.candles,
            [self.output],
            manifest=self.manifest,
            policy=self.policy,
        )
        evidence = build_backtest_evidence(
            manifest=self.manifest,
            policy=self.policy,
            result=result,
            dataset_quality={"missing_candles": 0},
            created_at="2026-09-25T00:00:00Z",
        )
        self.assertEqual(evidence["run_id"], self.manifest.run_id)
        self.assertEqual(evidence["manifest"]["source_commit_sha"], "abc123")
        self.assertEqual(evidence["manifest"]["dataset_id"], "DATASET-1")
        self.assertEqual(evidence["manifest"]["engine_name"], ENGINE_NAME)
        self.assertEqual(evidence["trading_policy"], self.policy.to_config())
        self.assertEqual(evidence["trade_count"], 1)
        self.assertIn("max_drawdown", evidence["metrics"])
        self.assertEqual(len(evidence["trades"]), 1)

    def test_evidence_json_round_trip(self):
        result = run_long_only_backtest(
            self.candles,
            [self.output],
            manifest=self.manifest,
            policy=self.policy,
        )
        evidence = build_backtest_evidence(
            manifest=self.manifest,
            policy=self.policy,
            result=result,
            created_at="2026-09-25T00:00:00Z",
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / (self.manifest.run_id + ".json")
            write_backtest_evidence(path, evidence)
            loaded = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(loaded["run_id"], self.manifest.run_id)
        self.assertEqual(loaded["trade_count"], 1)

    def test_non_finite_evidence_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bad.json"
            with self.assertRaises(ValueError):
                write_backtest_evidence(path, {"bad": float("nan")})
            with self.assertRaises(ValueError):
                write_backtest_evidence(path, {"bad": float("inf")})


if __name__ == "__main__":
    unittest.main()
