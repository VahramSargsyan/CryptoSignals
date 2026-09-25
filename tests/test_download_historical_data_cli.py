import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from core.data.candles import CandleQualityReport, HistoricalDataset
from integrations.binance.historical import (
    BinanceDownloadMetadata,
    BinanceDownloadResult,
)
from scripts.download_historical_data import (
    default_closed_candle_cutoff,
    resolve_download_window,
    write_dataset_bundle,
)


class HistoricalDataCliTests(unittest.TestCase):
    def test_daily_default_cutoff_is_current_daily_boundary(self):
        cutoff = default_closed_candle_cutoff(
            "1D",
            now="2026-09-25T15:33:00Z",
        )
        self.assertEqual(cutoff, pd.Timestamp("2026-09-25T00:00:00Z"))

    def test_three_year_window_uses_cutoff_as_anchor(self):
        start, cutoff = resolve_download_window(
            timeframe="1D",
            years=3,
            end="2026-09-25T00:00:00Z",
        )
        self.assertEqual(start, pd.Timestamp("2023-09-25T00:00:00Z"))
        self.assertEqual(cutoff, pd.Timestamp("2026-09-25T00:00:00Z"))

    def test_bundle_writes_canonical_csv_and_manifest(self):
        candles = pd.DataFrame(
            [
                {
                    "timestamp": pd.Timestamp("2026-09-23T00:00:00Z"),
                    "open": 10.0,
                    "high": 12.0,
                    "low": 9.0,
                    "close": 11.0,
                    "volume": 100.0,
                },
                {
                    "timestamp": pd.Timestamp("2026-09-24T00:00:00Z"),
                    "open": 11.0,
                    "high": 13.0,
                    "low": 10.0,
                    "close": 12.0,
                    "volume": 120.0,
                },
            ]
        )
        quality = CandleQualityReport(
            symbol="LINKUSDT",
            timeframe="1D",
            total_rows=2,
            start_timestamp="2026-09-23T00:00:00+00:00",
            end_timestamp="2026-09-24T00:00:00+00:00",
            duplicate_timestamps=0,
            out_of_order_rows=0,
            missing_candles=0,
            off_grid_timestamps=0,
            invalid_ohlc_rows=0,
            null_cells=0,
            negative_volume_rows=0,
        )
        dataset = HistoricalDataset(
            symbol="LINKUSDT",
            timeframe="1D",
            source="BINANCE",
            dataset_id="BINANCE:LINKUSDT:1D:test1234",
            candles=candles,
            quality=quality,
        )
        metadata = BinanceDownloadMetadata(
            symbol="LINKUSDT",
            timeframe="1D",
            requested_start="2023-09-25T00:00:00+00:00",
            requested_end="2026-09-25T00:00:00+00:00",
            actual_start="2026-09-23T00:00:00+00:00",
            actual_end="2026-09-24T00:00:00+00:00",
            listing_truncated=True,
            raw_rows=2,
            closed_rows=2,
            status="OK",
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            csv_path, manifest_path = write_dataset_bundle(
                BinanceDownloadResult(metadata=metadata, dataset=dataset),
                output_root=root / "data",
                manifest_root=root / "evidence",
                retrieved_at="2026-09-25T12:00:00Z",
            )

            self.assertTrue(csv_path.exists())
            self.assertTrue(manifest_path.exists())
            written = pd.read_csv(csv_path)
            self.assertEqual(
                list(written.columns),
                ["timestamp", "open", "high", "low", "close", "volume"],
            )
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["dataset_id"], dataset.dataset_id)
            self.assertFalse(manifest["quality"]["has_critical_issues"])
            self.assertEqual(
                manifest["download"]["requested_start"],
                "2023-09-25T00:00:00+00:00",
            )
            self.assertEqual(
                manifest["files"]["candles_csv"],
                csv_path.as_posix(),
            )


if __name__ == "__main__":
    unittest.main()
