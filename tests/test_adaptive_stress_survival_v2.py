from __future__ import annotations

import unittest

from scripts.research_adaptive_stress_survival_v2 import (
    duration_support_status,
    fit_survival_forecast,
)


class AdaptiveStressSurvivalV2Tests(unittest.TestCase):
    def test_support_flag_turns_oos_above_historical_max(self):
        status, max_duration = duration_support_status(31, [10, 20, 30])
        self.assertEqual(status, "OUT_OF_DURATION_SUPPORT")
        self.assertEqual(max_duration, 30)

    def test_support_flag_stays_in_support_at_historical_max(self):
        status, max_duration = duration_support_status(30, [10, 20, 30])
        self.assertEqual(status, "IN_DURATION_SUPPORT")
        self.assertEqual(max_duration, 30)

    def test_km_refuses_to_invent_hazard_beyond_completed_support(self):
        forecast = fit_survival_forecast(
            [10, 20, 30],
            current_age_days=40,
        )
        self.assertEqual(
            forecast.support_status,
            "OUT_OF_DURATION_SUPPORT",
        )
        self.assertIsNone(forecast.km_median_remaining)
        self.assertTrue(
            all(value == 0.0 for value in forecast.km_probabilities.values())
        )

    def test_weibull_tail_is_finite_but_remains_flagged_oos(self):
        forecast = fit_survival_forecast(
            [10, 20, 30, 45],
            current_age_days=60,
        )
        self.assertEqual(
            forecast.support_status,
            "OUT_OF_DURATION_SUPPORT",
        )
        self.assertIsNotNone(forecast.weibull_median_remaining)
        self.assertGreaterEqual(forecast.weibull_median_remaining, 0.0)
        self.assertTrue(
            all(0.0 <= p <= 1.0 for p in forecast.weibull_probabilities.values())
        )


if __name__ == "__main__":
    unittest.main()
