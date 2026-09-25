import unittest

import pandas as pd

from core.research.regime_analysis import (
    attach_regime_to_events,
    attach_regime_to_trades,
    summarize_events_by_regime,
    summarize_trades_by_regime,
)


class RegimeAnalysisTests(unittest.TestCase):
    def setUp(self):
        self.regime = pd.DataFrame(
            [
                {
                    "timestamp": "2025-01-01T00:00:00Z",
                    "btc_trend": "BTC_BULL",
                    "broad_crypto_trend": "BROAD_BULL",
                    "volatility_regime": "VOL_LOW",
                    "breadth_regime": "BREADTH_STRONG",
                    "volume_regime": "VOLUME_HIGH",
                    "regime_id": "BTC_BULL|BROAD_BULL|VOL_LOW|VOLUME_HIGH",
                },
                {
                    "timestamp": "2025-01-02T00:00:00Z",
                    "btc_trend": "BTC_BEAR",
                    "broad_crypto_trend": "BROAD_BEAR",
                    "volatility_regime": "VOL_HIGH",
                    "breadth_regime": "BREADTH_WEAK",
                    "volume_regime": "VOLUME_LOW",
                    "regime_id": "BTC_BEAR|BROAD_BEAR|VOL_HIGH|VOLUME_LOW",
                },
            ]
        )

    def test_events_use_signal_close_regime(self):
        events = pd.DataFrame(
            [
                {
                    "strategy_id": "S1",
                    "strategy_version": "1.0.0",
                    "symbol": "ETHUSDT",
                    "timeframe": "1D",
                    "signal": "BUY",
                    "signal_timestamp": "2025-01-01T00:00:00Z",
                    "horizon_candles": 3,
                    "directional_return": 0.10,
                },
                {
                    "strategy_id": "S1",
                    "strategy_version": "1.0.0",
                    "symbol": "ETHUSDT",
                    "timeframe": "1D",
                    "signal": "BUY",
                    "signal_timestamp": "2025-01-02T00:00:00Z",
                    "horizon_candles": 3,
                    "directional_return": -0.05,
                },
            ]
        )
        attached = attach_regime_to_events(events, self.regime)
        self.assertEqual(attached.iloc[0]["btc_trend"], "BTC_BULL")
        self.assertEqual(attached.iloc[1]["btc_trend"], "BTC_BEAR")

        summary = summarize_events_by_regime(attached, dimensions=("btc_trend",))
        bull = summary.loc[summary["regime_value"] == "BTC_BULL"].iloc[0]
        bear = summary.loc[summary["regime_value"] == "BTC_BEAR"].iloc[0]
        self.assertEqual(bull["observation_count"], 1)
        self.assertEqual(bull["win_rate"], 1.0)
        self.assertEqual(bear["win_rate"], 0.0)

    def test_trades_are_classified_by_entry_signal_regime(self):
        trades = pd.DataFrame(
            [
                {
                    "strategy_id": "S1",
                    "strategy_version": "1.0.0",
                    "symbol": "ETHUSDT",
                    "timeframe": "1D",
                    "entry_signal_timestamp": "2025-01-01T00:00:00Z",
                    "net_return": 0.20,
                },
                {
                    "strategy_id": "S1",
                    "strategy_version": "1.0.0",
                    "symbol": "ETHUSDT",
                    "timeframe": "1D",
                    "entry_signal_timestamp": "2025-01-02T00:00:00Z",
                    "net_return": -0.10,
                },
            ]
        )
        attached = attach_regime_to_trades(trades, self.regime)
        summary = summarize_trades_by_regime(attached, dimensions=("btc_trend",))
        bull = summary.loc[summary["regime_value"] == "BTC_BULL"].iloc[0]
        bear = summary.loc[summary["regime_value"] == "BTC_BEAR"].iloc[0]
        self.assertEqual(bull["trade_count"], 1)
        self.assertAlmostEqual(bull["average_return"], 0.20)
        self.assertEqual(bear["win_rate"], 0.0)
        self.assertEqual(bear["profit_factor"], 0.0)


if __name__ == "__main__":
    unittest.main()
