from __future__ import annotations

import unittest

import pandas as pd

from scripts.research_adaptive_stress_duration_v1 import (
    Episode,
    extract_stress_episodes,
    predict_state,
)


class AdaptiveStressDurationV1Tests(unittest.TestCase):
    def test_episode_starts_on_third_low_and_ends_on_third_high(self):
        breadth = [6, 3, 2, 1, 2, 4, 5, 6, 5, 4]
        dates = pd.date_range("2025-01-01", periods=len(breadth), freq="D", tz="UTC")
        panel = pd.DataFrame(
            {
                "timestamp": dates,
                "breadth_sma200": breadth,
                "duration_features_valid": True,
            }
        )
        episodes = extract_stress_episodes(panel)
        self.assertEqual(len(episodes), 1)
        ep = episodes[0]
        self.assertTrue(ep.completed)
        self.assertEqual(ep.start, dates[3])
        self.assertEqual(ep.end, dates[8])
        self.assertEqual(ep.duration_days, 5)
        self.assertEqual(int(ep.states.iloc[0]["remaining_stress_days"]), 5)
        self.assertEqual(int(ep.states.iloc[-1]["remaining_stress_days"]), 0)

    def test_censored_episode_has_no_remaining_label(self):
        breadth = [6, 3, 2, 1, 2, 2]
        dates = pd.date_range("2025-01-01", periods=len(breadth), freq="D", tz="UTC")
        panel = pd.DataFrame(
            {
                "timestamp": dates,
                "breadth_sma200": breadth,
                "duration_features_valid": True,
            }
        )
        episodes = extract_stress_episodes(panel)
        self.assertEqual(len(episodes), 1)
        self.assertFalse(episodes[0].completed)
        self.assertIsNone(episodes[0].end)
        self.assertIsNone(episodes[0].duration_days)

    def _episode(self, episode_id: str, start: str, values: list[tuple]) -> Episode:
        dates = pd.date_range(start, periods=len(values), freq="D", tz="UTC")
        rows = []
        duration = len(values) - 1
        for i, (breadth, delta, gap, dispersion, vol) in enumerate(values):
            rows.append(
                {
                    "timestamp": dates[i],
                    "breadth_sma200": breadth,
                    "episode_age_days": i,
                    "breadth_delta_7d": delta,
                    "median_sma200_gap": gap,
                    "dispersion_sma200_gap": dispersion,
                    "median_vol30": vol,
                    "remaining_stress_days": duration - i,
                    "episode_duration_days": duration,
                    "episode_id": episode_id,
                }
            )
        frame = pd.DataFrame(rows)
        return Episode(
            episode_id=episode_id,
            start=dates[0],
            end=dates[-1],
            completed=True,
            duration_days=duration,
            states=frame,
        )

    def test_each_training_episode_contributes_only_one_analog(self):
        ep1 = self._episode(
            "A",
            "2024-01-01",
            [(1, 0, -0.2, 0.1, 0.04), (2, 1, -0.1, 0.08, 0.03)],
        )
        ep2 = self._episode(
            "B",
            "2024-03-01",
            [(0, -1, -0.3, 0.12, 0.05), (1, 0, -0.2, 0.1, 0.04)],
        )
        test = pd.Series(
            {
                "breadth_sma200": 1,
                "episode_age_days": 1,
                "breadth_delta_7d": 0,
                "median_sma200_gap": -0.2,
                "dispersion_sma200_gap": 0.1,
                "median_vol30": 0.04,
            }
        )
        pred = predict_state(test, [ep1, ep2])
        self.assertEqual(pred["analog_count"], 2)
        self.assertEqual(set(pred["analog_details"]["episode_id"]), {"A", "B"})


if __name__ == "__main__":
    unittest.main()
