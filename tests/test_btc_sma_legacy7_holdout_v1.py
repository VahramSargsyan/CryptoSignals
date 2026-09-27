from __future__ import annotations

import unittest

from scripts.research_btc_sma_legacy7_holdout_v1 import (
    FROZEN_BTC_CANDIDATES,
    LEGACY_ASSETS,
    ENTER_BREADTH_MAX,
    EXIT_BREADTH_MIN,
)


class BtcSmaLegacy7HoldoutV1Tests(unittest.TestCase):
    def test_universe_is_exactly_legacy7_without_pepe(self):
        self.assertEqual(
            LEGACY_ASSETS,
            ("ATOM", "TWT", "BNB", "SOL", "TRX", "AAVE", "LINK"),
        )
        self.assertNotIn("PEPE", LEGACY_ASSETS)

    def test_btc_candidates_are_frozen_development_top3(self):
        self.assertEqual(
            FROZEN_BTC_CANDIDATES,
            ((25, 100), (30, 100), (12, 100)),
        )

    def test_breadth_thresholds_are_not_retuned_for_seven_assets(self):
        self.assertEqual(ENTER_BREADTH_MAX, 3)
        self.assertEqual(EXIT_BREADTH_MIN, 5)


if __name__ == "__main__":
    unittest.main()
