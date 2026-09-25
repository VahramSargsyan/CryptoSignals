import unittest

import pandas as pd

from core.backtest.contracts import BacktestRunManifest, ExecutionPolicy
from core.backtest.trading_short import (
    ENGINE_NAME,
    ZERO_EQUITY_LIQUIDATION,
    ShortOnlyTradingPolicy,
    run_short_only_backtest,
)
from core.contracts.strategy import make_strategy_output


class ShortOnlyTradingBacktestTests(unittest.TestCase):
    def setUp(self):
        self.candles = pd.DataFrame(
            [
                {"timestamp": "2026-01-01T00:00:00Z", "open": 100, "high": 105, "close": 100},
                {"timestamp": "2026-01-02T00:00:00Z", "open": 110, "high": 115, "close": 105},
                {"timestamp": "2026-01-03T00:00:00Z", "open": 100, "high": 105, "close": 95},
                {"timestamp": "2026-01-04T00:00:00Z", "open": 90, "high": 95, "close": 92},
                {"timestamp": "2026-01-05T00:00:00Z", "open": 95, "high": 100, "close": 96},
            ]
        )
        self.policy = ShortOnlyTradingPolicy()
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

    def test_sell_opens_short_and_buy_covers_at_next_open(self):
        result = run_short_only_backtest(
            self.candles,
            [
                self._output("2026-01-01T00:00:00Z", "SELL"),
                self._output("2026-01-03T00:00:00Z", "BUY"),
            ],
            manifest=self.manifest,
            policy=self.policy,
        )
        trade = result.trades.iloc[0]
        self.assertEqual(trade["entry_timestamp"], pd.Timestamp("2026-01-02T00:00:00Z"))
        self.assertEqual(trade["exit_timestamp"], pd.Timestamp("2026-01-04T00:00:00Z"))
        self.assertEqual(trade["entry_price"], 110.0)
        self.assertEqual(trade["exit_price"], 90.0)
        self.assertAlmostEqual(trade["gross_return"], 1 - (90 / 110))
        self.assertAlmostEqual(trade["net_return"], 1 - (90 / 110))
        self.assertGreater(result.metrics.total_return, 0)

    def test_costs_and_slippage_are_adverse_to_short(self):
        policy = ShortOnlyTradingPolicy()
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
                signal="SELL",
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
                signal="BUY",
                strength=70,
                reasons=("test",),
                source_commit_sha="abc",
                run_id=manifest.run_id,
            ),
        ]
        result = run_short_only_backtest(
            self.candles,
            outputs,
            manifest=manifest,
            policy=policy,
        )
        trade = result.trades.iloc[0]
        self.assertAlmostEqual(trade["entry_price"], 110 * 0.9995)
        self.assertAlmostEqual(trade["exit_price"], 90 * 1.0005)
        self.assertLess(trade["net_return"], trade["gross_return"])

    def test_intraday_high_can_zero_equity_and_stop_future_trading(self):
        candles = pd.DataFrame(
            [
                {"timestamp": "2026-01-01T00:00:00Z", "open": 100, "high": 100, "close": 100},
                {"timestamp": "2026-01-02T00:00:00Z", "open": 100, "high": 110, "close": 105},
                {"timestamp": "2026-01-03T00:00:00Z", "open": 105, "high": 220, "close": 150},
                {"timestamp": "2026-01-04T00:00:00Z", "open": 150, "high": 160, "close": 140},
                {"timestamp": "2026-01-05T00:00:00Z", "open": 140, "high": 145, "close": 130},
            ]
        )
        outputs = [
            self._output("2026-01-01T00:00:00Z", "SELL"),
            self._output("2026-01-03T00:00:00Z", "BUY"),
            self._output("2026-01-04T00:00:00Z", "SELL"),
        ]
        result = run_short_only_backtest(
            candles,
            outputs,
            manifest=self.manifest,
            policy=self.policy,
        )
        self.assertEqual(len(result.trades), 1)
        trade = result.trades.iloc[0]
        self.assertEqual(trade["exit_reason"], ZERO_EQUITY_LIQUIDATION)
        self.assertEqual(trade["net_return"], -1.0)
        self.assertEqual(result.metrics.total_return, -1.0)
        self.assertEqual(result.metrics.max_drawdown, 1.0)

    def test_short_benchmark_rewards_market_decline(self):
        result = run_short_only_backtest(
            self.candles,
            [],
            manifest=self.manifest,
            policy=self.policy,
        )
        self.assertGreater(result.metrics.benchmark_return, 0)

    def test_manifest_must_encode_short_policy(self):
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
            run_short_only_backtest(
                self.candles,
                [],
                manifest=wrong,
                policy=self.policy,
            )


if __name__ == "__main__":
    unittest.main()
