import unittest

import pandas as pd

from integrations.binance.historical import download_historical_dataset


def kline(timestamp, open_, high, low, close, volume):
    return [
        int(pd.Timestamp(timestamp).timestamp() * 1000),
        str(open_),
        str(high),
        str(low),
        str(close),
        str(volume),
        0,
        "0",
        0,
        "0",
        "0",
        "0",
    ]


class FakeClient:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def get_historical_klines(self, symbol, interval, start_str, end_str=None):
        self.calls.append((symbol, interval, start_str, end_str))
        return self.rows


class BinanceHistoricalAdapterTests(unittest.TestCase):
    def test_listing_start_is_recorded_not_treated_as_missing_prelisting_data(self):
        client = FakeClient(
            [
                kline("2024-04-09T00:00:00Z", 1, 2, 0.5, 1.5, 100),
                kline("2024-04-10T00:00:00Z", 1.5, 2, 1, 1.8, 120),
            ]
        )
        result = download_historical_dataset(
            client,
            symbol="SAGAUSDT",
            start="2021-01-01T00:00:00Z",
            timeframe="1D",
            as_of="2024-04-11T00:00:00Z",
        )
        self.assertEqual(result.metadata.status, "OK")
        self.assertTrue(result.metadata.listing_truncated)
        self.assertEqual(result.metadata.actual_start, "2024-04-09T00:00:00+00:00")
        self.assertEqual(result.dataset.quality.missing_candles, 0)
        self.assertEqual(result.dataset.quality.off_grid_timestamps, 0)
        self.assertEqual(client.calls[0][0:2], ("SAGAUSDT", "1d"))

    def test_open_candle_is_excluded(self):
        client = FakeClient(
            [
                kline("2026-09-24T00:00:00Z", 1, 2, 0.5, 1.5, 100),
                kline("2026-09-25T00:00:00Z", 1.5, 2, 1, 1.8, 120),
            ]
        )
        result = download_historical_dataset(
            client,
            symbol="BTCUSDT",
            start="2026-09-24T00:00:00Z",
            timeframe="1D",
            as_of="2026-09-25T12:00:00Z",
        )
        self.assertEqual(result.metadata.raw_rows, 2)
        self.assertEqual(result.metadata.closed_rows, 1)
        self.assertEqual(len(result.dataset.candles), 1)
        self.assertEqual(
            result.dataset.candles.iloc[0]["timestamp"],
            pd.Timestamp("2026-09-24T00:00:00Z"),
        )

    def test_no_data_is_explicit(self):
        result = download_historical_dataset(
            FakeClient([]),
            symbol="UNKNOWNUSDT",
            start="2021-01-01T00:00:00Z",
            timeframe="1D",
            as_of="2026-09-25T00:00:00Z",
        )
        self.assertEqual(result.metadata.status, "NO_DATA")
        self.assertIsNone(result.dataset)

    def test_invalid_requested_period_is_rejected_before_network_call(self):
        client = FakeClient([])
        with self.assertRaises(ValueError):
            download_historical_dataset(
                client,
                symbol="BTCUSDT",
                start="2026-01-02T00:00:00Z",
                end="2026-01-01T00:00:00Z",
            )
        self.assertEqual(client.calls, [])


if __name__ == "__main__":
    unittest.main()
