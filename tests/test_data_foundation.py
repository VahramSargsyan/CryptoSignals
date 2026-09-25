import unittest

import pandas as pd

from core.data.candles import (
    build_dataset_id,
    keep_closed_candles,
    normalize_candles,
    validate_candles,
)


class DataFoundationTests(unittest.TestCase):
    def test_normalizes_legacy_columns_and_stable_dataset_id(self):
        raw = pd.DataFrame(
            [
                {"Date": "2026-01-01", "Open": 10, "High": 12, "Low": 9, "Close": 11, "Volume": 100},
                {"Date": "2026-01-02", "Open": 11, "High": 13, "Low": 10, "Close": 12, "Volume": 120},
            ]
        )
        normalized = normalize_candles(raw)
        self.assertEqual(list(normalized.columns), ["timestamp", "open", "high", "low", "close", "volume"])
        first = build_dataset_id(normalized, "BINANCE", "BTCUSDT", "1D")
        second = build_dataset_id(normalized.copy(), "BINANCE", "BTCUSDT", "1D")
        self.assertEqual(first, second)

    def test_quality_report_detects_gap_duplicate_order_and_bad_ohlc(self):
        frame = pd.DataFrame(
            [
                {"timestamp": "2026-01-02", "open": 10, "high": 12, "low": 9, "close": 11, "volume": 100},
                {"timestamp": "2026-01-01", "open": 10, "high": 9, "low": 8, "close": 11, "volume": 100},
                {"timestamp": "2026-01-01", "open": 10, "high": 12, "low": 9, "close": 11, "volume": -1},
                {"timestamp": "2026-01-04", "open": 11, "high": 13, "low": 10, "close": 12, "volume": 120},
            ]
        )
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        report = validate_candles(frame, symbol="TEST", timeframe="1D")
        self.assertEqual(report.duplicate_timestamps, 1)
        self.assertEqual(report.out_of_order_rows, 1)
        self.assertEqual(report.missing_candles, 1)
        self.assertEqual(report.invalid_ohlc_rows, 1)
        self.assertEqual(report.negative_volume_rows, 1)
        self.assertTrue(report.has_critical_issues)

    def test_closed_candle_filter(self):
        frame = pd.DataFrame(
            [
                {"timestamp": "2026-01-01T00:00:00Z", "open": 10, "high": 12, "low": 9, "close": 11, "volume": 100},
                {"timestamp": "2026-01-02T00:00:00Z", "open": 11, "high": 13, "low": 10, "close": 12, "volume": 120},
            ]
        )
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        filtered = keep_closed_candles(frame, timeframe="1D", as_of=pd.Timestamp("2026-01-02T12:00:00Z"))
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered.iloc[0]["timestamp"], pd.Timestamp("2026-01-01T00:00:00Z"))


if __name__ == "__main__":
    unittest.main()
