import unittest

import pandas as pd

from core.data.candles import HistoricalDataset, validate_candles
from core.regime.crypto import CryptoRegimeConfig, build_crypto_regime


def make_dataset(symbol, closes, volumes=None, start="2025-01-01"):
    index = pd.date_range(start=start, periods=len(closes), freq="D", tz="UTC")
    volumes = volumes or [100 + i for i in range(len(closes))]
    frame = pd.DataFrame(
        {
            "timestamp": index,
            "open": closes,
            "high": [value * 1.02 for value in closes],
            "low": [value * 0.98 for value in closes],
            "close": closes,
            "volume": volumes,
        }
    )
    quality = validate_candles(frame, symbol=symbol, timeframe="1D")
    return HistoricalDataset(
        symbol=symbol,
        timeframe="1D",
        source="TEST",
        dataset_id="TEST:" + symbol,
        candles=frame,
        quality=quality,
    )


def small_config(minimum_assets=2):
    return CryptoRegimeConfig(
        trend_fast_window=3,
        trend_slow_window=5,
        breadth_window=3,
        volume_window=3,
        volatility_window=2,
        volatility_baseline_window=3,
        minimum_breadth_assets=minimum_assets,
        breadth_bull_threshold=0.60,
        breadth_bear_threshold=0.40,
        volume_high_threshold=0.60,
        volume_low_threshold=0.40,
        volatility_high_multiple=1.25,
        volatility_low_multiple=0.80,
    )


class CryptoRegimeTests(unittest.TestCase):
    def test_trending_universe_builds_expected_directional_labels(self):
        datasets = {
            "BTCUSDT": make_dataset(
                "BTCUSDT",
                [100, 102, 101, 105, 107, 110, 112, 115, 117, 120],
                [100, 100, 100, 110, 120, 130, 140, 150, 160, 170],
            ),
            "ETHUSDT": make_dataset(
                "ETHUSDT",
                [50, 51, 52, 53, 55, 57, 59, 61, 63, 65],
                [80, 80, 80, 90, 100, 110, 120, 130, 140, 150],
            ),
        }
        regime = build_crypto_regime(datasets, config=small_config())
        last = regime.iloc[-1]
        self.assertEqual(last["btc_trend"], "BTC_BULL")
        self.assertEqual(last["breadth_regime"], "BREADTH_STRONG")
        self.assertEqual(last["broad_crypto_trend"], "BROAD_BULL")
        self.assertEqual(last["volume_regime"], "VOLUME_HIGH")
        self.assertEqual(last["breadth_assets"], 2)
        self.assertEqual(last["volume_assets"], 2)
        self.assertTrue(last["regime_id"].startswith("BTC_BULL|BROAD_BULL|"))

    def test_future_data_does_not_change_past_regime(self):
        base_btc = [100, 102, 101, 105, 107, 110, 112, 115, 117, 120]
        base_eth = [50, 51, 52, 53, 55, 57, 59, 61, 63, 65]
        datasets = {
            "BTCUSDT": make_dataset("BTCUSDT", base_btc),
            "ETHUSDT": make_dataset("ETHUSDT", base_eth),
        }
        altered = {
            "BTCUSDT": make_dataset("BTCUSDT", base_btc[:8] + [10, 5]),
            "ETHUSDT": make_dataset("ETHUSDT", base_eth[:8] + [5, 2]),
        }
        first = build_crypto_regime(datasets, config=small_config())
        second = build_crypto_regime(altered, config=small_config())
        timestamp = pd.Timestamp("2025-01-08T00:00:00Z")
        first_row = first.loc[first["timestamp"] == timestamp].iloc[0]
        second_row = second.loc[second["timestamp"] == timestamp].iloc[0]
        for column in (
            "btc_trend",
            "broad_crypto_trend",
            "volatility_regime",
            "breadth_regime",
            "volume_regime",
            "regime_id",
        ):
            self.assertEqual(first_row[column], second_row[column])

    def test_late_listing_is_not_counted_before_it_has_enough_history(self):
        btc = make_dataset("BTCUSDT", [100, 101, 102, 103, 104, 105, 106, 107])
        eth = make_dataset(
            "ETHUSDT",
            [50, 51, 52],
            start="2025-01-06",
        )
        regime = build_crypto_regime(
            {"BTCUSDT": btc, "ETHUSDT": eth},
            config=small_config(minimum_assets=2),
        )
        early = regime.loc[regime["timestamp"] == pd.Timestamp("2025-01-05T00:00:00Z")].iloc[0]
        late = regime.loc[regime["timestamp"] == pd.Timestamp("2025-01-08T00:00:00Z")].iloc[0]
        self.assertEqual(early["breadth_assets"], 1)
        self.assertEqual(early["breadth_regime"], "UNKNOWN")
        self.assertEqual(late["breadth_assets"], 2)
        self.assertNotEqual(late["breadth_regime"], "UNKNOWN")

    def test_benchmark_is_required(self):
        with self.assertRaises(ValueError):
            build_crypto_regime(
                {"ETHUSDT": make_dataset("ETHUSDT", [1, 2, 3, 4, 5])},
                config=small_config(minimum_assets=1),
            )


if __name__ == "__main__":
    unittest.main()
