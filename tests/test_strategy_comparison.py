from pathlib import Path
import unittest

from core.data.candles import load_csv_dataset
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
            self.assertEqual(result["manifest"].source_commit_sha, "accepted-test-sha")
            self.assertEqual(result["evidence"]["manifest"]["dataset_id"], dataset.dataset_id)
            self.assertEqual(result["evidence"]["manifest"]["strategy_id"], strategy_id)
            self.assertEqual(result["evidence"]["run_id"], result["manifest"].run_id)

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
