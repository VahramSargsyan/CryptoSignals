from pathlib import Path
import unittest

import pandas as pd

from core.data.candles import load_csv_dataset
from core.backtest.trading import ENGINE_NAME as LONG_ENGINE_NAME
from core.backtest.trading_short import ENGINE_NAME as SHORT_ENGINE_NAME
from core.research.comparison import compare_strategies_on_dataset

FIXTURE = Path(__file__).parent / "fixtures" / "vahram_original_v1_saga_2025_03_03_2025_05_02.csv"


class StrategyComparisonTests(unittest.TestCase):
    def test_v1_and_v2_share_dataset_and_engine_but_keep_distinct_identity(self):
        dataset = load_csv_dataset(
            FIXTURE,
            symbol="SAGAUSDT",
            timeframe="1D",
            source="BINANCE",
        )
        comparison = compare_strategies_on_dataset(
            dataset,
            source_commit_sha="accepted-test-sha",
            validation_type="LOCAL_FIXTURE_SMOKE",
            fee_bps=10,
            slippage_bps=5,
        )

        summary = comparison["summary"]
        self.assertEqual(len(summary), 2)
        self.assertEqual(set(summary["dataset_id"]), {dataset.dataset_id})
        self.assertEqual(
            set(summary["strategy_id"]),
            {"VAHRAM_ORIGINAL_V1", "VAHRAM_TRUE_STOCHRSI_V2"},
        )
        self.assertEqual(len(set(summary["run_id"])), 2)

        for strategy_id, result in comparison["strategies"].items():
            self.assertEqual(result["manifest"].dataset_id, dataset.dataset_id)
            self.assertEqual(result["manifest"].engine_name, LONG_ENGINE_NAME)
            self.assertEqual(
                result["evidence"]["trading_policy"]["position_mode"],
                "LONG_ONLY_FULL_EQUITY",
            )
            self.assertEqual(result["manifest"].source_commit_sha, "accepted-test-sha")
            self.assertEqual(result["evidence"]["manifest"]["dataset_id"], dataset.dataset_id)
            self.assertEqual(result["evidence"]["manifest"]["strategy_id"], strategy_id)
            self.assertEqual(result["evidence"]["run_id"], result["manifest"].run_id)

    def test_explicit_candidate_selection_does_not_change_default_v1_v2_set(self):
        dataset = load_csv_dataset(
            FIXTURE,
            symbol="SAGAUSDT",
            timeframe="1D",
            source="BINANCE",
        )
        comparison = compare_strategies_on_dataset(
            dataset,
            source_commit_sha="candidate-test-sha",
            validation_type="LOCAL_CANDIDATE_SMOKE",
            strategy_ids=("STOCHRSI_CROSS_V1",),
        )

        summary = comparison["summary"]
        self.assertEqual(len(summary), 1)
        self.assertEqual(list(summary["strategy_id"]), ["STOCHRSI_CROSS_V1"])
        result = comparison["strategies"]["STOCHRSI_CROSS_V1"]
        self.assertEqual(
            result["manifest"].parameters,
            {
                "rsi_window": 14,
                "stoch_window": 14,
                "smooth_k": 10,
                "smooth_d": 3,
                "signal_logic": "STRICT_K_D_CROSS",
            },
        )

        default = compare_strategies_on_dataset(
            dataset,
            source_commit_sha="accepted-test-sha",
            validation_type="LOCAL_FIXTURE_SMOKE",
        )
        self.assertEqual(
            set(default["summary"]["strategy_id"]),
            {"VAHRAM_ORIGINAL_V1", "VAHRAM_TRUE_STOCHRSI_V2"},
        )

    def test_macd_candidate_can_be_selected_without_expanding_defaults(self):
        dataset = load_csv_dataset(
            FIXTURE,
            symbol="SAGAUSDT",
            timeframe="1D",
            source="BINANCE",
        )
        comparison = compare_strategies_on_dataset(
            dataset,
            source_commit_sha="macd-test-sha",
            validation_type="LOCAL_CANDIDATE_SMOKE",
            strategy_ids=("MACD_CROSS_V1",),
        )
        self.assertEqual(list(comparison["summary"]["strategy_id"]), ["MACD_CROSS_V1"])
        result = comparison["strategies"]["MACD_CROSS_V1"]
        self.assertEqual(
            result["manifest"].parameters,
            {
                "fast_window": 12,
                "slow_window": 26,
                "signal_window": 9,
                "signal_logic": "STRICT_MACD_SIGNAL_CROSS",
            },
        )

        default = compare_strategies_on_dataset(
            dataset,
            source_commit_sha="default-test-sha",
            validation_type="LOCAL_FIXTURE_SMOKE",
        )
        self.assertEqual(
            set(default["summary"]["strategy_id"]),
            {"VAHRAM_ORIGINAL_V1", "VAHRAM_TRUE_STOCHRSI_V2"},
        )

    def test_weighted_candidate_can_be_selected_without_expanding_defaults(self):
        dataset = load_csv_dataset(
            FIXTURE,
            symbol="SAGAUSDT",
            timeframe="1D",
            source="BINANCE",
        )
        comparison = compare_strategies_on_dataset(
            dataset,
            source_commit_sha="weighted-test-sha",
            validation_type="LOCAL_CANDIDATE_SMOKE",
            strategy_ids=("WEIGHTED_MULTI_SIGNAL_V1",),
        )
        self.assertEqual(list(comparison["summary"]["strategy_id"]), ["WEIGHTED_MULTI_SIGNAL_V1"])
        result = comparison["strategies"]["WEIGHTED_MULTI_SIGNAL_V1"]
        self.assertEqual(
            result["manifest"].parameters,
            {
                "block_weight": 20.0,
                "signal_threshold": 60.0,
                "min_triggered_blocks": 3,
                "blocks": [
                    "ma_trend",
                    "macd",
                    "stochrsi",
                    "bollinger_reversion",
                    "volume_candle",
                ],
            },
        )

        default = compare_strategies_on_dataset(
            dataset,
            source_commit_sha="default-test-sha",
            validation_type="LOCAL_FIXTURE_SMOKE",
        )
        self.assertEqual(
            set(default["summary"]["strategy_id"]),
            {"VAHRAM_ORIGINAL_V1", "VAHRAM_TRUE_STOCHRSI_V2"},
        )

    def test_weighted_v2_candidate_can_be_selected_without_expanding_defaults(self):
        dataset = load_csv_dataset(
            FIXTURE,
            symbol="SAGAUSDT",
            timeframe="1D",
            source="BINANCE",
        )
        comparison = compare_strategies_on_dataset(
            dataset,
            source_commit_sha="weighted-v2-test-sha",
            validation_type="LOCAL_CANDIDATE_SMOKE",
            strategy_ids=("WEIGHTED_MULTI_SIGNAL_V2",),
        )
        self.assertEqual(list(comparison["summary"]["strategy_id"]), ["WEIGHTED_MULTI_SIGNAL_V2"])
        result = comparison["strategies"]["WEIGHTED_MULTI_SIGNAL_V2"]
        self.assertEqual(
            result["manifest"].parameters,
            {
                "core_blocks": ["macd", "stochrsi", "volume_candle"],
                "core_base_strength": 80.0,
                "ma_aligned_strength": 100.0,
                "min_candle_body_strength": 0.5,
                "ma_role": "CONTEXT_ONLY",
                "bollinger_role": "NOT_USED",
            },
        )

        default = compare_strategies_on_dataset(
            dataset,
            source_commit_sha="default-test-sha",
            validation_type="LOCAL_FIXTURE_SMOKE",
        )
        self.assertEqual(
            set(default["summary"]["strategy_id"]),
            {"VAHRAM_ORIGINAL_V1", "VAHRAM_TRUE_STOCHRSI_V2"},
        )

    def test_evaluation_start_uses_prior_candles_only_as_warmup(self):
        dataset = load_csv_dataset(
            FIXTURE,
            symbol="SAGAUSDT",
            timeframe="1D",
            source="BINANCE",
        )
        boundary = pd.Timestamp("2025-04-10T00:00:00Z")
        comparison = compare_strategies_on_dataset(
            dataset,
            source_commit_sha="warmup-test-sha",
            validation_type="LOCAL_WARMUP_SMOKE",
            fee_bps=0,
            slippage_bps=0,
            strategy_ids=("STOCHRSI_CROSS_V1",),
            evaluation_start=boundary,
        )

        result = comparison["strategies"]["STOCHRSI_CROSS_V1"]
        evaluation = dataset.candles.loc[
            pd.to_datetime(dataset.candles["timestamp"], utc=True) >= boundary
        ].reset_index(drop=True)

        self.assertLess(
            pd.Timestamp(comparison["data_start"]),
            pd.Timestamp(comparison["evaluation_start"]),
        )
        self.assertEqual(
            pd.Timestamp(result["manifest"].period_start),
            pd.Timestamp(evaluation.iloc[0]["timestamp"]),
        )
        self.assertTrue(
            all(pd.Timestamp(output.timestamp) >= boundary for output in result["outputs"])
        )
        self.assertTrue(
            (pd.to_datetime(result["trading"].equity_curve["timestamp"], utc=True) >= boundary).all()
        )

        expected_benchmark = (
            float(evaluation.iloc[-1]["close"]) / float(evaluation.iloc[0]["open"])
        ) - 1.0
        self.assertAlmostEqual(
            result["trading"].metrics.benchmark_return,
            expected_benchmark,
            places=12,
        )

    def test_evaluation_start_after_dataset_is_rejected(self):
        dataset = load_csv_dataset(
            FIXTURE,
            symbol="SAGAUSDT",
            timeframe="1D",
            source="BINANCE",
        )
        with self.assertRaisesRegex(ValueError, "leaves no candles"):
            compare_strategies_on_dataset(
                dataset,
                source_commit_sha="warmup-test-sha",
                strategy_ids=("STOCHRSI_CROSS_V1",),
                evaluation_start="2030-01-01T00:00:00Z",
            )

    def test_explicit_short_mode_uses_separate_engine_without_changing_strategy(self):
        dataset = load_csv_dataset(
            FIXTURE,
            symbol="SAGAUSDT",
            timeframe="1D",
            source="BINANCE",
        )
        comparison = compare_strategies_on_dataset(
            dataset,
            source_commit_sha="short-test-sha",
            validation_type="LOCAL_SHORT_ENGINE_SMOKE",
            strategy_ids=("WEIGHTED_MULTI_SIGNAL_V2",),
            position_mode="SHORT_ONLY",
        )

        result = comparison["strategies"]["WEIGHTED_MULTI_SIGNAL_V2"]
        self.assertEqual(comparison["position_mode"], "SHORT_ONLY")
        self.assertEqual(result["manifest"].engine_name, SHORT_ENGINE_NAME)
        self.assertEqual(
            result["evidence"]["trading_policy"]["position_mode"],
            "SHORT_ONLY_FULL_EQUITY_1X_NOTIONAL",
        )
        self.assertEqual(
            result["evidence"]["trading_policy"]["entry_signal"],
            "SELL",
        )
        self.assertEqual(
            result["evidence"]["trading_policy"]["exit_signal"],
            "BUY",
        )

    def test_unsupported_position_mode_is_rejected(self):
        dataset = load_csv_dataset(
            FIXTURE,
            symbol="SAGAUSDT",
            timeframe="1D",
            source="BINANCE",
        )
        with self.assertRaisesRegex(ValueError, "Unsupported position_mode"):
            compare_strategies_on_dataset(
                dataset,
                source_commit_sha="test",
                position_mode="BOTH",
            )

    def test_unknown_candidate_is_rejected_before_execution(self):
        dataset = load_csv_dataset(
            FIXTURE,
            symbol="SAGAUSDT",
            timeframe="1D",
            source="BINANCE",
        )
        with self.assertRaisesRegex(ValueError, "Unknown strategy_ids"):
            compare_strategies_on_dataset(
                dataset,
                source_commit_sha="test",
                strategy_ids=("NOT_A_STRATEGY",),
            )

    def test_critical_dataset_quality_blocks_formal_comparison(self):
        dataset = load_csv_dataset(
            FIXTURE,
            symbol="SAGAUSDT",
            timeframe="1D",
            source="BINANCE",
        )
        broken = dataset.candles.drop(index=20).reset_index(drop=True)
        from core.data.candles import HistoricalDataset, build_dataset_id, validate_candles

        quality = validate_candles(broken, symbol="SAGAUSDT", timeframe="1D")
        bad_dataset = HistoricalDataset(
            symbol="SAGAUSDT",
            timeframe="1D",
            source="BINANCE",
            dataset_id=build_dataset_id(broken, "BINANCE", "SAGAUSDT", "1D"),
            candles=broken,
            quality=quality,
        )
        self.assertTrue(quality.has_critical_issues)
        with self.assertRaises(ValueError):
            compare_strategies_on_dataset(
                bad_dataset,
                source_commit_sha="accepted-test-sha",
            )


if __name__ == "__main__":
    unittest.main()
