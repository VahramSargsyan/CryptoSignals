from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import send_relative_rotation_telegram_menu as menu


class RelativeRotationTelegramMenuTests(unittest.TestCase):
    def _config(self) -> dict:
        return {
            "position_books": [
                {"book_id": "BOOK_1", "held_asset": "ATOM"},
                {
                    "book_id": "BOOK_2",
                    "held_asset": "TRX",
                    "quantity": 3950.7453,
                },
            ]
        }

    def _report(self) -> dict:
        confirmed = {
            "date": "2026-10-01T00:00:00+00:00",
            "event": "CONFIRMED",
            "pair": "TRX/FIL",
            "from_asset": "TRX",
            "to_asset": "FIL",
            "max_dislocation": 0.25,
            "reversal_from_extreme": 0.04,
        }
        armed = {
            "date": "2026-10-01T00:00:00+00:00",
            "event": "ARMED",
            "pair": "ATOM/AVAX",
            "from_asset": "ATOM",
            "to_asset": "AVAX",
            "max_dislocation": 0.17,
            "reversal_from_extreme": 0.0,
        }
        return {
            "latest_closed_candle": "2026-10-01T00:00:00+00:00",
            "book_events": {
                "BOOK_1": {
                    "book": {"book_id": "BOOK_1", "held_asset": "ATOM"},
                    "events": {
                        "primary_confirmed": None,
                        "confirmed": [],
                        "armed": [armed],
                    },
                    "route_conflicts": [],
                },
                "BOOK_2": {
                    "book": {
                        "book_id": "BOOK_2",
                        "held_asset": "TRX",
                        "quantity": 3950.7453,
                    },
                    "events": {
                        "primary_confirmed": confirmed,
                        "confirmed": [confirmed],
                        "armed": [],
                    },
                    "route_conflicts": [],
                },
            },
            "route_conflicts": {},
        }

    def test_positions_text_and_nested_edit_buttons(self):
        text = menu.build_positions_text(self._config())
        self.assertIn("📊 Мои позиции", text)
        self.assertIn("BOOK_1", text)
        self.assertIn("ATOM — количество пока не записано", text)
        self.assertIn("3950.7453 TRX", text)

        with mock.patch.dict(
            os.environ, {"RR_TELEGRAM_CONTROL_ENABLED": "true"}, clear=False
        ):
            markup = menu._position_edit_markup(self._config())

        self.assertEqual(
            markup["inline_keyboard"][1][0]["callback_data"], "rrp|BOOK_2"
        )

    def test_status_shows_armed_and_confirmed(self):
        text = menu.build_status_text(self._report())
        self.assertIn("📡 Relative Rotation — текущий статус", text)
        self.assertIn("BOOK_1 — ATOM", text)
        self.assertIn("⚠️ ARM / PREWATCH", text)
        self.assertIn("ATOM -> AVAX", text)
        self.assertIn("BOOK_2 — 3950.75 TRX", text)
        self.assertIn("🚨 CONFIRMED", text)
        self.assertIn("TRX -> FIL", text)

    def test_safe_current_confirmed_and_action_buttons(self):
        report = self._report()
        items = menu._safe_current_confirmed(report)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["book_id"], "BOOK_2")

        with mock.patch.dict(
            os.environ, {"RR_TELEGRAM_CONTROL_ENABLED": "true"}, clear=False
        ):
            markup = menu._action_markup(
                items, include_done=True, include_missed=True
            )

        row = markup["inline_keyboard"][0]
        self.assertTrue(row[0]["callback_data"].startswith("rrd|BOOK_2|TRX|FIL|"))
        self.assertEqual(row[1]["callback_data"], "rrl|BOOK_2")

    def test_unresolved_route_conflict_suppresses_actions(self):
        report = self._report()
        details = report["book_events"]["BOOK_2"]
        details["route_conflicts"] = [
            {
                "severity": "ROUTE_CONFLICT_WARNING",
                "competing_candidate": {"from_asset": "TRX", "to_asset": "HBAR"},
            }
        ]
        items = menu._safe_current_confirmed(report)
        self.assertEqual(items, [])

    def test_positions_action_does_not_require_market_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.json"
            config_path.write_text(json.dumps(self._config()), encoding="utf-8")

            with mock.patch.dict(
                os.environ, {"RR_TELEGRAM_CONTROL_ENABLED": "true"}, clear=False
            ), mock.patch.object(menu, "_send_telegram") as send:
                menu.send_menu_action(
                    action="POSITIONS",
                    config_path=config_path,
                    report_path=None,
                )

            send.assert_called_once()
            self.assertIn("📊 Мои позиции", send.call_args.args[0])

    def test_status_action_sends_current_confirmed_buttons(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.json"
            report_path = Path(tmp) / "report.json"
            config_path.write_text(json.dumps(self._config()), encoding="utf-8")
            report_path.write_text(json.dumps(self._report()), encoding="utf-8")

            with mock.patch.dict(
                os.environ, {"RR_TELEGRAM_CONTROL_ENABLED": "true"}, clear=False
            ), mock.patch.object(menu, "_send_telegram") as send:
                menu.send_menu_action(
                    action="STATUS",
                    config_path=config_path,
                    report_path=report_path,
                )

            send.assert_called_once()
            self.assertIn("текущий статус", send.call_args.args[0])
            markup = send.call_args.kwargs["reply_markup"]
            self.assertEqual(len(markup["inline_keyboard"][0]), 2)


if __name__ == "__main__":
    unittest.main()
