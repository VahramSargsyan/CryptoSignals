import unittest

import pandas as pd

from core.backtest.contracts import BacktestRunManifest, ExecutionPolicy
from core.backtest.trading import (
    ENGINE_NAME,
    END_POLICY_FINAL_CLOSE,
    LongOnlyTradingPolicy,
    run_long_only_backtest,
)
from core.contracts.strategy import make_strategy_output


class TradingBacktestTests(unittest.TestCase):
    def setUp(self):
        self.candles = pd.DataFrame(
            [
                {"timestamp": "2026-01-01T00:00:00Z", "open": 100, "close": 100},
                {"timestamp": "2026-01-02T00:00:00Z", "open": 110, "close": 120},
                {"timestamp": "2026-01-03T00:00:00Z", "open": 130, "close": 125},
                {"timestamp": "2026-01-04T00:00:00Z", "open": 90, "close": 95},
                {"timestamp": "2026-01-05T00:00:00Z", "open": 100, "close": 110},
            ]
        )
        self.policy = LongOnlyTradingPolicy()
        self.manifest = BacktestRunManifest(
            strategy_id="TEST",
            strategy_version="1.0.0",
            source_commit_sha="abc",
            dataset_id="TEST-DATASET",
            symbol="BTCUSDT",
            timeframe="1D",
            period_start="2026-01-01",
            period_end="2026-01-05",
            execution=ExecutionPolicy(fee_bps=0, slippage_bps=0),
            validation_type="RESEARCH",
            engine_name=ENGINE_NAME,
            engine_config=self.policy.to_config(),
        )

    def _output(self, timestamp, signal, strength=70):
        return make_strategy_output(
            strategy_id="TEST",
            strategy_version="1.0.0",
            symbol="BTCUSDT",
            timeframe="1D",
            timestamp=timestamp,
            signal=signal,
            strength=strength,
            reasons=("test",),
            source_commit_sha="abc",
            run_id=self.manifest.run_id,
        )

    def test_buy_and_sell_execute_at_next_candle_open(self):
        outputs = [
            self._output("2026-01-01T00:00:00Z", "BUY"),
            self._output("2026-01-03T00:00:00Z", "SELL"),
        ]
        result = run_long_only_backtest(
            self.candles,
            outputs,
            manifest=self.manifest,
            policy=self.policy,
        )
        trade = result.trades.iloc[0]
        self.assertEqual(trade["entry_timestamp"], pd.Timestamp("2026-01-02T00:00:00Z"))
        self.assertEqual(trade["exit_timestamp"], pd.Timestamp("2026-01-04T00:00:00Z"))
        self.assertEqual(trade["entry_price"], 110.0)
        self.assertEqual(trade["exit_price"], 90.0)
        self.assertEqual(trade["holding_candles"], 2)
        self.assertAlmostEqual(trade["net_return"], (90 / 110) - 1)

    def test_required_metrics_include_intratrade_drawdown_and_benchmark(self):
        outputs = [
            self._output("2026-01-01T00:00:00Z", "BUY"),
            self._output("2026-01-03T00:00:00Z", "SELL"),
        ]
        result = run_long_only_backtest(
            self.candles,
            outputs,
            manifest=self.manifest,
            policy=self.policy,
        )
        metrics = result.metrics
        self.assertEqual(metrics.trade_count, 1)
        self.assertEqual(metrics.win_rate, 0.0)
        self.assertEqual(metrics.profit_factor, 0.0)
        self.assertAlmostEqual(metrics.total_return, (90 / 110) - 1)
        self.assertAlmostEqual(metrics.benchmark_return, 0.10)
        self.assertAlmostEqual(metrics.average_holding_period, 2.0)
        self.assertAlmostEqual(metrics.exposure, 0.4)
        self.assertAlmostEqual(metrics.max_drawdown, 0.28)

    def test_costs_reduce_return_and_execution_prices_include_slippage(self):
        policy = LongOnlyTradingPolicy()
        manifest = BacktestRunManifest(
            strategy_id="TEST",
            strategy_version="1.0.0",
            source_commit_sha="abc",
            dataset_id="TEST-DATASET",
            symbol="BTCUSDT",
            timeframe="1D",
            period_start="2026-01-01",
            period_end="2026-01-05",
            execution=ExecutionPolicy(fee_bps=10, slippage_bps=5),
            engine_name=ENGINE_NAME,
            engine_config=policy.to_config(),
        )
        outputs = [
            make_strategy_output(
                strategy_id="TEST",
                strategy_version="1.0.0",
                symbol="BTCUSDT",
                timeframe="1D",
                timestamp="2026-01-01T00:00:00Z",
                signal="BUY",
                strength=70,
                reasons=("test",),
                source_commit_sha="abc",
                run_id=manifest.run_id,
            ),
            make_strategy_output(
                strategy_id="TEST",
                strategy_version="1.0.0",
                symbol="BTCUSDT",
                timeframe="1D",
                timestamp="2026-01-03T00:00:00Z",
                signal="SELL",
                strength=70,
                reasons=("test",),
                source_commit_sha="abc",
                run_id=manifest.run_id,
            ),
        ]
        result = run_long_only_backtest(
            self.candles,
            outputs,
            manifest=manifest,
            policy=policy,
        )
        trade = result.trades.iloc[0]
        self.assertAlmostEqual(trade["entry_price"], 110.055)
        self.assertAlmostEqual(trade["exit_price"], 89.955)
        self.assertLess(trade["net_return"], trade["gross_return"])

    def test_open_position_is_explicitly_liquidated_at_final_close(self):
        result = run_long_only_backtest(
            self.candles,
            [self._output("2026-01-01T00:00:00Z", "BUY")],
            manifest=self.manifest,
            policy=self.policy,
        )
        trade = result.trades.iloc[0]
        self.assertEqual(trade["exit_reason"], END_POLICY_FINAL_CLOSE)
        self.assertEqual(trade["exit_timestamp"], pd.Timestamp("2026-01-05T00:00:00Z"))
        self.assertEqual(trade["exit_price"], 110.0)
        self.assertEqual(trade["holding_candles"], 4)
        self.assertAlmostEqual(result.metrics.total_return, 0.0)
        self.assertAlmostEqual(result.metrics.exposure, 0.8)

    def test_manifest_must_encode_exact_engine_policy(self):
        wrong = BacktestRunManifest(
            strategy_id="TEST",
            strategy_version="1.0.0",
            source_commit_sha="abc",
            dataset_id="TEST-DATASET",
            symbol="BTCUSDT",
            timeframe="1D",
            period_start="2026-01-01",
            period_end="2026-01-05",
            execution=ExecutionPolicy(fee_bps=0, slippage_bps=0),
            engine_name=ENGINE_NAME,
            engine_config={},
        )
        with self.assertRaises(ValueError):
            run_long_only_backtest(
                self.candles,
                [],
                manifest=wrong,
                policy=self.policy,
            )

    def test_outputs_are_bound_to_exact_run_and_source_commit(self):
        wrong_run = make_strategy_output(
            strategy_id="TEST",
            strategy_version="1.0.0",
            symbol="BTCUSDT",
            timeframe="1D",
            timestamp="2026-01-01T00:00:00Z",
            signal="BUY",
            strength=70,
            reasons=("test",),
            source_commit_sha="abc",
            run_id="RUN-WRONG",
        )
        with self.assertRaises(ValueError):
            run_long_only_backtest(
                self.candles,
                [wrong_run],
                manifest=self.manifest,
                policy=self.policy,
            )

        wrong_sha = make_strategy_output(
            strategy_id="TEST",
            strategy_version="1.0.0",
            symbol="BTCUSDT",
            timeframe="1D",
            timestamp="2026-01-01T00:00:00Z",
            signal="BUY",
            strength=70,
            reasons=("test",),
            source_commit_sha="different",
            run_id=self.manifest.run_id,
        )
        with self.assertRaises(ValueError):
            run_long_only_backtest(
                self.candles,
                [wrong_sha],
                manifest=self.manifest,
                policy=self.policy,
            )

    def test_manifest_rejects_non_finite_reproducibility_fields(self):
        manifest = BacktestRunManifest(
            strategy_id="TEST",
            strategy_version="1.0.0",
            source_commit_sha="abc",
            dataset_id="TEST-DATASET",
            symbol="BTCUSDT",
            timeframe="1D",
            period_start="2026-01-01",
            period_end="2026-01-05",
            parameters={"bad": float("nan")},
            engine_name=ENGINE_NAME,
            engine_config=self.policy.to_config(),
        )
        with self.assertRaises(ValueError):
            _ = manifest.run_id


if __name__ == "__main__":
    unittest.main()
