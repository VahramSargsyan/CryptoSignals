from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts.apply_relative_rotation_telegram_operation import (
    mark_morning_missed,
    set_position,
)


class RelativeRotationTelegramOperationTests(unittest.TestCase):
    def _fixture(self):
        temp = tempfile.TemporaryDirectory()
        root = Path(temp.name)
        config = root / "config.json"
        control = root / "control.json"
        log = root / "log.md"
        report = root / "report.json"

        config.write_text(
            json.dumps(
                {
                    "held_asset": "ATOM",
                    "target_assets": ["TRX", "ALGO", "FIL"],
                    "sunset_assets": ["ATOM", "LINK"],
                    "position_books": [
                        {"book_id": "BOOK_1", "held_asset": "ATOM"},
                        {
                            "book_id": "BOOK_2",
                            "held_asset": "TRX",
                            "quantity": 3950.7453,
                        },
                    ],
                }
            ),
            encoding="utf-8",
        )
        control.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "timezone": "Asia/Yerevan",
                    "morning_slot": "04:03",
                    "evening_slot": "22:30",
                    "evening_requires_explicit_missed_morning": True,
                    "missed_morning_signals": [],
                }
            ),
            encoding="utf-8",
        )
        log.write_text(
            """# Relative Rotation — Real Manual Rotation Log

### BOOK_1 — primary branch

Current held asset:

`ATOM`

### BOOK_2 — secondary rotation branch

Current held asset:

`TRX`

Current tracked quantity:

`3950.7453 TRX`

## Recording rule

fixture
""",
            encoding="utf-8",
        )
        report.write_text(
            json.dumps(
                {
                    "notification_candidates": [
                        {
                            "event_id": "BOOK_2|CONFIRMED|2026-10-01T00:00:00+00:00|TRX|FIL|TRX/FIL",
                            "book_id": "BOOK_2",
                            "book": {"held_asset": "TRX", "quantity": 3950.7453},
                            "event": {
                                "date": "2026-10-01T00:00:00+00:00",
                                "event": "CONFIRMED",
                                "pair": "TRX/FIL",
                                "from_asset": "TRX",
                                "to_asset": "FIL",
                                "max_dislocation": 0.25,
                                "reversal_from_extreme": 0.04,
                            },
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        return temp, config, control, log, report

    def test_mark_morning_missed_arms_same_day_evening_for_current_confirmed(self):
        temp, _config, control, _log, report = self._fixture()
        self.addCleanup(temp.cleanup)

        result = mark_morning_missed(
            control_path=control,
            report_path=report,
            book_id="BOOK_2",
            confirmed_at="2026-10-01T06:35:00+00:00",
            telegram_update_id="101",
        )
        self.assertTrue(result["changed"])
        payload = json.loads(control.read_text(encoding="utf-8"))
        entry = payload["missed_morning_signals"][0]
        self.assertEqual(entry["book_id"], "BOOK_2")
        self.assertEqual(entry["armed_local_date"], "2026-10-01")
        self.assertEqual(entry["from_asset"], "TRX")
        self.assertEqual(entry["to_asset"], "FIL")

    def test_mark_morning_missed_is_idempotent_for_same_update(self):
        temp, _config, control, _log, report = self._fixture()
        self.addCleanup(temp.cleanup)

        kwargs = dict(
            control_path=control,
            report_path=report,
            book_id="BOOK_2",
            confirmed_at="2026-10-01T06:35:00+00:00",
            telegram_update_id="102",
        )
        first = mark_morning_missed(**kwargs)
        second = mark_morning_missed(**kwargs)
        self.assertTrue(first["changed"])
        self.assertFalse(second["changed"])

    def test_set_position_updates_book_and_clears_evening_arm(self):
        temp, config, control, log, _report = self._fixture()
        self.addCleanup(temp.cleanup)
        payload = json.loads(control.read_text(encoding="utf-8"))
        payload["missed_morning_signals"] = [
            {
                "book_id": "BOOK_2",
                "event_id": "x",
                "armed_local_date": "2026-10-01",
            }
        ]
        control.write_text(json.dumps(payload), encoding="utf-8")

        result = set_position(
            config_path=config,
            control_path=control,
            log_path=log,
            book_id="BOOK_2",
            asset="FIL",
            quantity="777.5",
            confirmed_at="2026-10-01T18:40:00+00:00",
            telegram_update_id="103",
        )
        self.assertTrue(result["changed"])
        cfg = json.loads(config.read_text(encoding="utf-8"))
        self.assertEqual(cfg["position_books"][1]["held_asset"], "FIL")
        self.assertEqual(cfg["position_books"][1]["quantity"], 777.5)
        self.assertEqual(cfg["held_asset"], "ATOM")

        state = json.loads(control.read_text(encoding="utf-8"))
        self.assertEqual(state["missed_morning_signals"], [])
        text = log.read_text(encoding="utf-8")
        self.assertIn("Current held asset:\n\n`FIL`", text)
        self.assertIn("Current tracked quantity:\n\n`777.5 FIL`", text)
        self.assertIn("telegram position update id: 103.", text)
        self.assertIn("does not invent a CONFIRMED strategy event", text)

    def test_set_position_rejects_unknown_asset(self):
        temp, config, control, log, _report = self._fixture()
        self.addCleanup(temp.cleanup)
        with self.assertRaisesRegex(ValueError, "outside configured"):
            set_position(
                config_path=config,
                control_path=control,
                log_path=log,
                book_id="BOOK_2",
                asset="DOGE",
                quantity="10",
                confirmed_at="2026-10-01T18:40:00+00:00",
                telegram_update_id="104",
            )


if __name__ == "__main__":
    unittest.main()
