from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts.apply_relative_rotation_manual_execution import apply_execution


class RelativeRotationExecutionControlTests(unittest.TestCase):
    def _fixture(self, *, book1_asset="ATOM", book1_qty=None, book2_asset="ALGO", book2_qty=10723.76037691):
        temp = tempfile.TemporaryDirectory()
        root = Path(temp.name)
        config_path = root / "config.json"
        log_path = root / "log.md"
        report_path = root / "report.json"

        books = [
            {"book_id": "BOOK_1", "held_asset": book1_asset},
            {"book_id": "BOOK_2", "held_asset": book2_asset},
        ]
        if book1_qty is not None:
            books[0]["quantity"] = book1_qty
        if book2_qty is not None:
            books[1]["quantity"] = book2_qty

        config = {
            "held_asset": book1_asset,
            "target_assets": ["TWT", "PEPE", "BNB", "TRX", "AAVE", "AVAX", "FIL", "ALGO", "XRP", "HBAR"],
            "position_books": books,
        }
        config_path.write_text(json.dumps(config), encoding="utf-8")
        log_path.write_text(
            """# Relative Rotation — Real Manual Rotation Log

### BOOK_1 — primary branch

Current held asset:

`ATOM`

Starting-state provenance:
- fixture.

### BOOK_2 — secondary rotation branch

Current held asset:

`ALGO`

Current tracked quantity:

`10723.76037691 ALGO`

## Recording rule

fixture
""",
            encoding="utf-8",
        )
        report = {
            "notification_candidates": [
                {
                    "event_id": "old",
                    "book_id": "BOOK_2",
                    "book": {"held_asset": "ALGO", "quantity": 10723.76037691},
                    "event": {
                        "date": "2026-09-27T00:00:00+00:00",
                        "event": "CONFIRMED",
                        "pair": "ALGO/HBAR",
                        "from_asset": "ALGO",
                        "to_asset": "HBAR",
                        "max_dislocation": 0.40,
                        "deviation": 0.30,
                        "reversal_from_extreme": 0.04,
                    },
                },
                {
                    "event_id": "latest-weak",
                    "book_id": "BOOK_2",
                    "book": {"held_asset": "ALGO", "quantity": 10723.76037691},
                    "event": {
                        "date": "2026-09-28T00:00:00+00:00",
                        "event": "CONFIRMED",
                        "pair": "ALGO/FIL",
                        "from_asset": "ALGO",
                        "to_asset": "FIL",
                        "max_dislocation": 0.20,
                        "deviation": 0.17,
                        "reversal_from_extreme": 0.035,
                    },
                },
                {
                    "event_id": "latest-strong",
                    "book_id": "BOOK_2",
                    "book": {"held_asset": "ALGO", "quantity": 10723.76037691},
                    "event": {
                        "date": "2026-09-28T00:00:00+00:00",
                        "event": "CONFIRMED",
                        "pair": "ALGO/AVAX",
                        "from_asset": "ALGO",
                        "to_asset": "AVAX",
                        "max_dislocation": 0.30,
                        "deviation": 0.24,
                        "reversal_from_extreme": 0.04,
                    },
                },
            ]
        }
        report_path.write_text(json.dumps(report), encoding="utf-8")
        return temp, config_path, log_path, report_path

    def test_applies_latest_strongest_confirmed_and_updates_book2(self):
        temp, config_path, log_path, report_path = self._fixture()
        self.addCleanup(temp.cleanup)

        result = apply_execution(
            config_path=config_path,
            log_path=log_path,
            report_path=report_path,
            book_id="BOOK_2",
            signal_date="20260928",
            from_asset="ALGO",
            to_asset="AVAX",
            sent_quantity="10723.76037691",
            received_quantity="456.789",
            confirmed_at="2026-09-30T01:30:00+00:00",
            telegram_update_id="12345",
        )

        self.assertTrue(result["changed"])
        config = json.loads(config_path.read_text(encoding="utf-8"))
        self.assertEqual(config["held_asset"], "ATOM")
        self.assertEqual(config["position_books"][1]["held_asset"], "AVAX")
        self.assertEqual(config["position_books"][1]["quantity"], 456.789)

        log = log_path.read_text(encoding="utf-8")
        self.assertIn("Current held asset:\n\n`AVAX`", log)
        self.assertIn("Current tracked quantity:\n\n`456.789 AVAX`", log)
        self.assertIn("telegram update id: 12345.", log)

    def test_rejects_noncanonical_destination(self):
        temp, config_path, log_path, report_path = self._fixture()
        self.addCleanup(temp.cleanup)

        with self.assertRaisesRegex(ValueError, "canonical route"):
            apply_execution(
                config_path=config_path,
                log_path=log_path,
                report_path=report_path,
                book_id="BOOK_2",
                signal_date="20260928",
                from_asset="ALGO",
                to_asset="FIL",
                sent_quantity="10723.76037691",
                received_quantity="500",
                confirmed_at="2026-09-30T01:30:00+00:00",
                telegram_update_id="12346",
            )

    def test_rejects_unresolved_route_conflict_without_canonical_override(self):
        temp, config_path, log_path, report_path = self._fixture()
        self.addCleanup(temp.cleanup)

        report = json.loads(report_path.read_text(encoding="utf-8"))
        strong = next(
            item
            for item in report["notification_candidates"]
            if item["event_id"] == "latest-strong"
        )
        strong["route_conflicts"] = [
            {
                "severity": "ROUTE_CONFLICT_WARNING",
                "competing_candidate": {
                    "from_asset": "ALGO",
                    "to_asset": "TRX",
                    "max_dislocation": 0.70,
                },
                "destination_relation": {
                    "from_asset": "AVAX",
                    "to_asset": "TRX",
                    "event": "ARMED",
                    "max_dislocation": 0.40,
                },
            }
        ]
        report_path.write_text(json.dumps(report), encoding="utf-8")

        with self.assertRaisesRegex(ValueError, "ROUTE_CONFLICT"):
            apply_execution(
                config_path=config_path,
                log_path=log_path,
                report_path=report_path,
                book_id="BOOK_2",
                signal_date="20260928",
                from_asset="ALGO",
                to_asset="AVAX",
                sent_quantity="10723.76037691",
                received_quantity="456.789",
                confirmed_at="2026-09-30T01:30:00+00:00",
                telegram_update_id="999",
            )

    def test_applies_destination_dominance_override_route(self):
        temp, config_path, log_path, report_path = self._fixture()
        self.addCleanup(temp.cleanup)

        report = json.loads(report_path.read_text(encoding="utf-8"))
        strong = next(
            item
            for item in report["notification_candidates"]
            if item["event_id"] == "latest-strong"
        )
        baseline = dict(strong["event"])
        relation = {
            "from_asset": "AVAX",
            "to_asset": "TRX",
            "event": "ARMED",
            "max_dislocation": 0.40,
        }
        strong["route_conflicts"] = [
            {
                "severity": "ROUTE_CONFLICT_WARNING",
                "source_asset": "ALGO",
                "primary": baseline,
                "competing_candidate": {
                    "from_asset": "ALGO",
                    "to_asset": "TRX",
                    "pair": "ALGO/TRX",
                    "event": "ARMED",
                    "max_dislocation": 0.70,
                    "reversal_from_extreme": 0.01,
                },
                "destination_relation": relation,
            }
        ]
        strong["event"] = {
            **baseline,
            "pair": "ALGO/TRX",
            "to_asset": "TRX",
            "max_dislocation": 0.70,
            "reversal_from_extreme": 0.01,
            "route_override": True,
            "route_override_rule": "DESTINATION_DOMINANCE_MIN_1_5X_V2",
            "route_override_trigger": baseline,
            "competing_original_state": "ARMED",
            "destination_relation": relation,
        }
        strong["route_override"] = strong["route_conflicts"][0]
        report_path.write_text(json.dumps(report), encoding="utf-8")

        result = apply_execution(
            config_path=config_path,
            log_path=log_path,
            report_path=report_path,
            book_id="BOOK_2",
            signal_date="20260928",
            from_asset="ALGO",
            to_asset="TRX",
            sent_quantity="10723.76037691",
            received_quantity="9999",
            confirmed_at="2026-09-30T19:10:00+00:00",
            telegram_update_id="1001",
        )

        self.assertTrue(result["changed"])
        config = json.loads(config_path.read_text(encoding="utf-8"))
        self.assertEqual(config["position_books"][1]["held_asset"], "TRX")
        log = log_path.read_text(encoding="utf-8")
        self.assertIn("route selection: DESTINATION_DOMINANCE_MIN_1_5X_V2", log)
        self.assertIn("baseline confirmed trigger: ALGO -> AVAX", log)
        self.assertIn("destination relation: AVAX -> TRX", log)

    def test_duplicate_telegram_update_is_idempotent(self):
        temp, config_path, log_path, report_path = self._fixture()
        self.addCleanup(temp.cleanup)

        kwargs = dict(
            config_path=config_path,
            log_path=log_path,
            report_path=report_path,
            book_id="BOOK_2",
            signal_date="20260928",
            from_asset="ALGO",
            to_asset="AVAX",
            sent_quantity="10723.76037691",
            received_quantity="456.789",
            confirmed_at="2026-09-30T01:30:00+00:00",
            telegram_update_id="777",
        )
        first = apply_execution(**kwargs)
        second = apply_execution(**kwargs)

        self.assertTrue(first["changed"])
        self.assertFalse(second["changed"])
        self.assertEqual(second["reason"], "ALREADY_APPLIED")


if __name__ == "__main__":
    unittest.main()
