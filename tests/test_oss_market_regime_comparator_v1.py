from __future__ import annotations

import unittest
import pandas as pd

from scripts.research_oss_market_regime_comparator_v1 import (
    classify_external_regime,
    market_availability_date,
)


class OssMarketRegimeComparatorV1Tests(unittest.TestCase):
    def test_broad_risk_off(self):
        row = {
            "SPY_return_5d": -0.03,
            "sector_positive_count_5d": 2,
            "sector_negative_count_5d": 9,
            "sector_dispersion_5d": 0.03,
            "gld_vs_spy_5d": 0.02,
            "ief_vs_spy_5d": 0.01,
            "tlt_vs_spy_5d": -0.01,
            "hyg_vs_lqd_5d": -0.02,
            "vix_return_5d": 0.15,
            "defensive_spread_5d": 0.03,
            "defensive_outperform_count": 3,
            "qqq_vs_spy_5d": -0.01,
            "iwm_vs_spy_5d": -0.02,
            "rsp_vs_spy_5d": -0.01,
        }
        regime, confidence, reasons = classify_external_regime(row)
        self.assertEqual(regime, "BROAD_RISK_OFF")
        self.assertEqual(confidence, "high")
        self.assertGreaterEqual(len(reasons), 3)

    def test_broad_risk_on(self):
        row = {
            "SPY_return_5d": 0.025,
            "sector_positive_count_5d": 9,
            "sector_negative_count_5d": 2,
            "sector_dispersion_5d": 0.01,
            "gld_vs_spy_5d": -0.01,
            "ief_vs_spy_5d": -0.01,
            "tlt_vs_spy_5d": -0.01,
            "hyg_vs_lqd_5d": 0.01,
            "vix_return_5d": -0.10,
            "defensive_spread_5d": -0.01,
            "defensive_outperform_count": 0,
            "qqq_vs_spy_5d": 0.01,
            "iwm_vs_spy_5d": 0.01,
            "rsp_vs_spy_5d": 0.005,
        }
        regime, confidence, _ = classify_external_regime(row)
        self.assertEqual(regime, "BROAD_RISK_ON")
        self.assertEqual(confidence, "high")

    def test_risk_off_has_priority_over_defensive_rotation(self):
        row = {
            "SPY_return_5d": -0.02,
            "sector_positive_count_5d": 3,
            "sector_negative_count_5d": 8,
            "sector_dispersion_5d": 0.03,
            "gld_vs_spy_5d": 0.01,
            "ief_vs_spy_5d": 0.01,
            "tlt_vs_spy_5d": 0.0,
            "hyg_vs_lqd_5d": -0.01,
            "vix_return_5d": 0.05,
            "defensive_spread_5d": 0.02,
            "defensive_outperform_count": 3,
            "qqq_vs_spy_5d": -0.01,
            "iwm_vs_spy_5d": -0.01,
            "rsp_vs_spy_5d": -0.01,
        }
        self.assertEqual(classify_external_regime(row)[0], "BROAD_RISK_OFF")

    def test_us_session_is_available_next_calendar_day(self):
        self.assertEqual(
            market_availability_date(pd.Timestamp("2026-09-25")),
            pd.Timestamp("2026-09-26"),
        )


if __name__ == "__main__":
    unittest.main()
