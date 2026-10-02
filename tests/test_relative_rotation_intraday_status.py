from __future__ import annotations

import unittest

import pandas as pd

from scripts.run_relative_rotation_intraday_status import (
    LOOKBACK_HOURS,
    build_intraday_pair_states,
)


class RelativeRotationIntradayStatusTests(unittest.TestCase):
    def test_h1_snapshot_uses_180_days_of_hourly_observations(self):
        timestamps = pd.date_range(
            "2026-01-01T00:00:00Z",
            periods=LOOKBACK_HOURS,
            freq="h",
        )
        frame = pd.DataFrame(
            {
                "timestamp": timestamps,
                "A_close": [1.0] * LOOKBACK_HOURS,
                "B_close": [1.0] * (LOOKBACK_HOURS - 1) + [1.20],
            }
        )

        states = build_intraday_pair_states(
            frame,
            assets=("A", "B"),
            lookback_hours=LOOKBACK_HOURS,
            arm_threshold=0.15,
        )

        self.assertEqual(len(states), 1)
        state = states[0]
        self.assertEqual(state["pair"], "A/B")
        self.assertAlmostEqual(state["median"], 1.0)
        self.assertAlmostEqual(state["deviation"], 0.20)
        self.assertEqual(state["mode"], "HIGH")
        self.assertEqual(state["from_asset"], "B")
        self.assertEqual(state["to_asset"], "A")


if __name__ == "__main__":
    unittest.main()
