from __future__ import annotations

import unittest

from scripts.research_defensive_shadow_reentry_v2 import update_shadow_recovery_streak


class DefensiveShadowReentryTests(unittest.TestCase):
    def test_streak_increments_for_same_target_above_sma(self):
        target, streak = update_shadow_recovery_streak(
            previous_target="ATOM",
            current_target="ATOM",
            target_above_sma200=True,
            streak=2,
        )
        self.assertEqual(target, "ATOM")
        self.assertEqual(streak, 3)

    def test_streak_resets_when_target_changes(self):
        target, streak = update_shadow_recovery_streak(
            previous_target="ATOM",
            current_target="LINK",
            target_above_sma200=True,
            streak=2,
        )
        self.assertEqual(target, "LINK")
        self.assertEqual(streak, 1)

    def test_streak_resets_when_target_falls_below_sma(self):
        target, streak = update_shadow_recovery_streak(
            previous_target="ATOM",
            current_target="ATOM",
            target_above_sma200=False,
            streak=2,
        )
        self.assertEqual(target, "ATOM")
        self.assertEqual(streak, 0)


if __name__ == "__main__":
    unittest.main()
