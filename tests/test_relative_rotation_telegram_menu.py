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
            "target_assets": ["TWT", "PEPE", "BNB", "TRX", "AAVE", "AVAX", "FIL", "ALGO", "XRP", "HBAR"],
            "latest_pair_states": [
                {
                    "pair": "AVAX/ATOM",
                    "mode": "HIGH",
                    "from_asset": "ATOM",
                    "to_asset": "AVAX",
                    "deviation": 0.12,
                    "max_dislocation": 0.18,
                    "reversal_from_extreme": 0.01,
                },
                {
                    "pair": "FIL/ATOM",
                    "mode": "NONE",
                    "from_asset": None,
                    "to_asset": None,
                    "deviation": 0.08,
                    "max_dislocation": 0.0,
                    "reversal_from_extreme": None,
                },
                {
                    "pair": "TRX/FIL",
                    "mode": "NONE",
                    "from_asset": None,
                    "to_asset": None,
                    "deviation": -0.11,
                    "max_dislocation": 0.0,
                    "reversal_from_extreme": None,
                },
                {
                    "pair": "TRX/HBAR",
                    "mode": "HIGH",
                    "from_asset": "HBAR",
                    "to_asset": "TRX",
                    "deviation": 0.16,
                    "max_dislocation": 0.19,
                    "reversal_from_extreme": 0.01,
                },
            ],
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

    def test_status_shows_full_signed_rotation_board(self):
        text = menu.build_status_text(self._report())
        self.assertIn("📡 Relative Rotation — полная текущая ротация", text)
        self.assertIn("BOOK_1 — текущая позиция: ATOM", text)
        self.assertIn("AVAX: +12.00%", text)
        self.assertIn("⚠️ ARM", text)
        self.assertIn("FIL: +8.00%", text)
        self.assertIn("до ARM 7.00 п.п.", text)
        self.assertIn("BOOK_2 — текущая позиция: 3950.75 TRX", text)
        self.assertIn("FIL: +11.00%", text)
        self.assertIn("🚨 CONFIRMED", text)
        self.assertIn("HBAR: -16.00%", text)
        self.assertIn("↩️ ARM обратно", text)
        self.assertIn("минус = сейчас сильнее", text)
        self.assertIn("НЕ доходность", text)

    def test_h1_status_is_explicitly_intraday_and_informational(self):
        report = self._report()
        report["report_kind"] = "H1_INTRADAY_STATUS"
        report["lookback_observations"] = 4320
        report["latest_closed_candle_end_yerevan"] = "2026-10-02T13:00:00+04:00"
        report["book_events"]["BOOK_2"]["events"] = {
            "primary_confirmed": None,
            "confirmed": [],
            "armed": [],
        }

        text = menu.build_status_text(report)

        self.assertIn("текущая H1-ротация", text)
        self.assertIn("2026-10-02T13:00:00+04:00", text)
        self.assertIn("4320 H1-наблюдений", text)
        self.assertIn("официальные ARM/CONFIRMED", text)
        self.assertIn("кнопки исполнения намеренно отключены", text)

    def test_directional_rotation_value_is_signed_from_book_perspective(self):
        row = {"pair": "TRX/FIL", "deviation": 0.10}
        self.assertAlmostEqual(
            menu._directional_rotation_value(row, "FIL", "TRX"),
            0.10,
        )
        self.assertAlmostEqual(
            menu._directional_rotation_value(row, "TRX", "FIL"),
            -0.10,
        )

    def test_status_lists_every_target_except_current_asset(self):
        report = self._report()
        rows = menu._book_rotation_rows(
            report,
            report["book_events"]["BOOK_2"],
        )
        targets = {row["target"] for row in rows}
        self.assertEqual(
            targets,
            {"TWT", "PEPE", "BNB", "AAVE", "AVAX", "FIL", "ALGO", "XRP", "HBAR"},
        )

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

    def test_status_action_never_exposes_execution_buttons(self):
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
            self.assertIn("полная текущая ротация", send.call_args.args[0])
            self.assertIsNone(send.call_args.kwargs["reply_markup"])


if __name__ == "__main__":
    unittest.main()
