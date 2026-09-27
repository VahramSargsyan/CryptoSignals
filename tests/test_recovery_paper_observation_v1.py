from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from scripts.recovery_paper_observation_v1 import (
    JOURNAL_COLUMNS,
    _episode_training_frame,
    _market_status,
    append_observation,
    latest_fully_closed_d1,
    resolve_history,
)
from scripts.research_adaptive_stress_duration_v1 import Episode


def _episode(
    episode_id: str,
    start: str,
    duration: int,
    breadth: list[int] | None = None,
) -> Episode:
    start_ts = pd.Timestamp(start, tz="UTC")
    dates = pd.date_range(start_ts, periods=duration + 1, freq="D")
    if breadth is None:
        breadth = [2] * max(1, duration - 1) + [5, 5]
        breadth = breadth[: duration + 1]
        if len(breadth) < duration + 1:
            breadth += [5] * (duration + 1 - len(breadth))

    states = pd.DataFrame(
        {
            "timestamp": dates,
            "breadth_sma200": breadth,
            "median_sma200_gap": [
                -0.20 + (0.20 * i / max(duration, 1))
                for i in range(duration + 1)
            ],
            "remaining_stress_days": [
                duration - i for i in range(duration + 1)
            ],
            "episode_age_days": list(range(duration + 1)),
        }
    )
    end_ts = dates[-1]
    return Episode(
        episode_id=episode_id,
        start=start_ts,
        end=end_ts,
        completed=True,
        duration_days=duration,
        states=states,
    )


class RecoveryPaperObservationV1Tests(unittest.TestCase):
    def test_latest_fully_closed_d1_uses_previous_utc_day(self):
        now = pd.Timestamp("2026-09-27T08:30:00Z")
        self.assertEqual(
            latest_fully_closed_d1(now),
            pd.Timestamp("2026-09-26T00:00:00Z"),
        )

    def test_training_frame_excludes_confirmed_recovery_close(self):
        episode = _episode("E1", "2025-01-01", 4)
        frame = _episode_training_frame(episode, horizon=7)

        self.assertEqual(len(frame), 4)
        self.assertNotIn(0, frame["remaining_stress_days"].astype(int).tolist())
        self.assertAlmostEqual(float(frame["sample_weight"].sum()), 1.0)

    def test_append_observation_is_idempotent_by_candle(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "observations.csv"
            row = {column: None for column in JOURNAL_COLUMNS}
            row.update(
                {
                    "observation_candle": "2026-09-26T00:00:00+00:00",
                    "generated_at": "2026-09-27T08:30:00+00:00",
                    "model_id": "RECOVERY_PAPER_OBSERVATION_V1",
                    "source_commit_sha": "abc",
                    "market_status": "NORMAL",
                    "breadth_sma200": 4,
                    "median_sma200_gap": 0.01,
                    "breadth_delta_7d": 1,
                    "paper_only": True,
                }
            )

            first, appended_first = append_observation(path, row)
            second, appended_second = append_observation(path, row)

            self.assertTrue(appended_first)
            self.assertFalse(appended_second)
            self.assertEqual(len(first), 1)
            self.assertEqual(len(second), 1)

    def test_resolved_history_keeps_prediction_and_adds_realized_outcome(self):
        episode = _episode("E1", "2026-01-01", 10)
        observation_candle = pd.Timestamp("2026-01-05T00:00:00Z")
        journal = pd.DataFrame(
            [
                {
                    "observation_candle": observation_candle.isoformat(),
                    "stress_start": episode.start.isoformat(),
                    "state_p_recovery_le_7d": 0.8,
                    "state_p_recovery_le_14d": 0.9,
                }
            ]
        )

        resolved = resolve_history(journal, [episode])
        row = resolved.iloc[0]

        self.assertTrue(bool(row["resolved"]))
        self.assertEqual(int(row["actual_remaining_days"]), 6)
        self.assertEqual(
            row["actual_recovery_candle"],
            episode.end.isoformat(),
        )
        self.assertAlmostEqual(float(row["state_brier_7d"]), 0.04)
        self.assertAlmostEqual(float(row["state_brier_14d"]), 0.01)

    def test_market_status_reports_two_close_recovery_confirmation(self):
        active = Episode(
            episode_id="E1",
            start=pd.Timestamp("2026-01-01T00:00:00Z"),
            end=None,
            completed=False,
            duration_days=None,
            states=pd.DataFrame(
                {
                    "timestamp": pd.date_range(
                        "2026-01-01",
                        periods=4,
                        freq="D",
                        tz="UTC",
                    ),
                    "breadth_sma200": [2, 4, 5, 5],
                }
            ),
        )
        features = pd.DataFrame(
            {
                "duration_features_valid": [True, True, True, True],
                "breadth_sma200": [2, 4, 5, 5],
            }
        )

        status, _, recovery_streak = _market_status(features, active)
        self.assertEqual(status, "RECOVERY_CONFIRMATION_2_OF_3")
        self.assertEqual(recovery_streak, 2)


if __name__ == "__main__":
    unittest.main()
