import unittest

import numpy as np
import pandas as pd

from scripts import research_grid_oss_harvest as oss


def fixture_frame(days=1250):
    timestamps = pd.date_range("2020-09-25", periods=days, freq="D", tz="UTC")
    x = np.arange(days, dtype=float)
    close = 100.0 + 0.02 * x + 8.0 * np.sin(x / 17.0)
    open_ = close * (1.0 + 0.001 * np.sin(x / 9.0))
    high = np.maximum(open_, close) * 1.02
    low = np.minimum(open_, close) * 0.98
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": np.full(days, 1000.0),
        }
    )


class OssHarvestTests(unittest.TestCase):
    def test_causal_range_uses_only_prior_1095_candles(self):
        frame = fixture_frame()
        schedule = oss.causal_hl_schedule(frame)
        first = int(frame.index[frame["timestamp"] >= oss.EVALUATION_START][0])
        history = frame.iloc[first - 1095:first]
        self.assertEqual(
            schedule[first],
            (float(history["high"].max()), float(history["low"].min())),
        )

    def test_atr_gated_schedule_does_not_change_past_when_future_changes(self):
        frame = fixture_frame()
        original = oss.atr_gated_hl_schedule(frame, atr_regrid_threshold=0.5)
        cutoff = 1160
        mutated = frame.copy()
        mutated.loc[cutoff + 1 :, ["open", "high", "low", "close"]] *= 4.0
        changed = oss.atr_gated_hl_schedule(mutated, atr_regrid_threshold=0.5)
        self.assertEqual(original[: cutoff + 1], changed[: cutoff + 1])

    def test_mid_engine_is_deterministic(self):
        frame = fixture_frame()
        self.assertEqual(oss.run_mid(frame), oss.run_mid(frame))

    def test_external_port_is_deterministic_and_finite(self):
        frame = fixture_frame()
        result = oss.run_jordan_dynamic_grid_port(frame)
        self.assertEqual(result, oss.run_jordan_dynamic_grid_port(frame))
        self.assertTrue(np.isfinite(result.total_return))
        self.assertTrue(np.isfinite(result.max_drawdown))
        self.assertGreaterEqual(result.closed_trades, 0)


if __name__ == "__main__":
    unittest.main()
