from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import send_relative_rotation_report as sender


class RelativeRotationTelegramSenderTests(unittest.TestCase):
    def _files(self, report: dict):
        temp = tempfile.TemporaryDirectory()
        root = Path(temp.name)
        report_path = root / "report.json"
        notification_path = root / "notification.txt"
        report_path.write_text(json.dumps(report), encoding="utf-8")
        notification_path.write_text("legacy text", encoding="utf-8")
        return temp, report_path, notification_path

    def test_sender_skips_when_policy_says_no_notify(self):
        temp, report_path, notification_path = self._files(
            {
                "should_notify": False,
                "telegram_text_ru": "should not send",
            }
        )
        self.addCleanup(temp.cleanup)

        with mock.patch.object(sender, "_send_telegram") as send:
            rc = sender.main(
                [
                    "--report-json",
                    str(report_path),
                    "--notification-text",
                    str(notification_path),
                ]
            )

        self.assertEqual(rc, 0)
        send.assert_not_called()

    def test_sender_sends_when_policy_allows(self):
        temp, report_path, notification_path = self._files(
            {
                "should_notify": True,
                "telegram_text_ru": "send me",
            }
        )
        self.addCleanup(temp.cleanup)

        with mock.patch.dict(
            "os.environ",
            {
                "TELEGRAM_BOT_TOKEN": "fake-token",
                "TELEGRAM_CHAT_ID": "fake-chat",
            },
            clear=False,
        ), mock.patch.object(sender, "_send_telegram") as send:
            rc = sender.main(
                [
                    "--report-json",
                    str(report_path),
                    "--notification-text",
                    str(notification_path),
                ]
            )

        self.assertEqual(rc, 0)
        send.assert_called_once_with("send me")


    def test_sender_replays_unsent_candidate_and_persists_event_id(self):
        candidate = {
            "event_id": "BOOK_2|CONFIRMED|2026-09-27T00:00:00+00:00|LINK|HBAR|HBAR/LINK",
            "book_id": "BOOK_2",
            "book": {"held_asset": "LINK", "quantity": 100},
            "event": {
                "date": "2026-09-27T00:00:00+00:00",
                "event": "CONFIRMED",
                "pair": "HBAR/LINK",
                "from_asset": "LINK",
                "to_asset": "HBAR",
                "max_dislocation": 0.3840,
                "reversal_from_extreme": 0.0330,
            },
        }
        temp, report_path, notification_path = self._files(
            {
                "should_notify": False,
                "latest_closed_candle": "2026-09-28T00:00:00+00:00",
                "notification_candidates": [candidate],
                "telegram_text_ru": "fallback",
            }
        )
        self.addCleanup(temp.cleanup)
        state_path = Path(temp.name) / "state.json"

        with mock.patch.dict(
            "os.environ",
            {
                "TELEGRAM_BOT_TOKEN": "fake-token",
                "TELEGRAM_CHAT_ID": "fake-chat",
            },
            clear=False,
        ), mock.patch.object(sender, "_send_telegram") as send:
            rc = sender.main(
                [
                    "--report-json",
                    str(report_path),
                    "--notification-text",
                    str(notification_path),
                    "--state-file",
                    str(state_path),
                ]
            )

        self.assertEqual(rc, 0)
        text = send.call_args.args[0]
        self.assertIn("BOOK_2", text)
        self.assertIn("100 LINK", text)
        self.assertIn("LINK -> HBAR", text)
        state = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertIn(candidate["event_id"], state["sent_event_ids"])

    def test_sender_deduplicates_previously_sent_candidate(self):
        candidate = {
            "event_id": "BOOK_2|CONFIRMED|2026-09-27T00:00:00+00:00|LINK|HBAR|HBAR/LINK",
            "book_id": "BOOK_2",
            "book": {"held_asset": "LINK", "quantity": 100},
            "event": {
                "date": "2026-09-27T00:00:00+00:00",
                "event": "CONFIRMED",
                "pair": "HBAR/LINK",
                "from_asset": "LINK",
                "to_asset": "HBAR",
                "max_dislocation": 0.3840,
                "reversal_from_extreme": 0.0330,
            },
        }
        temp, report_path, notification_path = self._files(
            {
                "should_notify": False,
                "latest_closed_candle": "2026-09-28T00:00:00+00:00",
                "notification_candidates": [candidate],
                "telegram_text_ru": "fallback",
            }
        )
        self.addCleanup(temp.cleanup)
        state_path = Path(temp.name) / "state.json"
        state_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "sent_event_ids": [candidate["event_id"]],
                }
            ),
            encoding="utf-8",
        )

        with mock.patch.object(sender, "_send_telegram") as send:
            rc = sender.main(
                [
                    "--report-json",
                    str(report_path),
                    "--notification-text",
                    str(notification_path),
                    "--state-file",
                    str(state_path),
                ]
            )

        self.assertEqual(rc, 0)
        send.assert_not_called()

    def test_missing_secrets_does_not_mark_candidate_sent(self):
        candidate = {
            "event_id": "BOOK_1|ARMED|2026-09-28T00:00:00+00:00|ATOM|AVAX|ATOM/AVAX",
            "book_id": "BOOK_1",
            "book": {"held_asset": "ATOM", "quantity": None},
            "event": {
                "date": "2026-09-28T00:00:00+00:00",
                "event": "ARMED",
                "pair": "ATOM/AVAX",
                "from_asset": "ATOM",
                "to_asset": "AVAX",
                "max_dislocation": 0.16,
                "reversal_from_extreme": 0.0,
            },
        }
        temp, report_path, notification_path = self._files(
            {
                "should_notify": True,
                "latest_closed_candle": "2026-09-28T00:00:00+00:00",
                "notification_candidates": [candidate],
                "telegram_text_ru": "fallback",
            }
        )
        self.addCleanup(temp.cleanup)
        state_path = Path(temp.name) / "state.json"

        with mock.patch.dict(
            "os.environ",
            {"TELEGRAM_BOT_TOKEN": "", "TELEGRAM_CHAT_ID": ""},
            clear=False,
        ), mock.patch.object(sender, "_send_telegram") as send:
            rc = sender.main(
                [
                    "--report-json",
                    str(report_path),
                    "--notification-text",
                    str(notification_path),
                    "--state-file",
                    str(state_path),
                ]
            )

        self.assertEqual(rc, 0)
        send.assert_not_called()
        state = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertNotIn(candidate["event_id"], state["sent_event_ids"])


    def test_evening_reminder_repeats_same_candle_confirmed_after_morning_send(self):
        candidate = {
            "event_id": "BOOK_2|CONFIRMED|2026-09-28T00:00:00+00:00|ALGO|FIL|ALGO/FIL",
            "book_id": "BOOK_2",
            "book": {"held_asset": "ALGO", "quantity": 10723.76037691},
            "event": {
                "date": "2026-09-28T00:00:00+00:00",
                "event": "CONFIRMED",
                "pair": "ALGO/FIL",
                "from_asset": "ALGO",
                "to_asset": "FIL",
                "max_dislocation": 0.20,
                "reversal_from_extreme": 0.035,
            },
        }
        temp, report_path, notification_path = self._files(
            {
                "should_notify": False,
                "latest_closed_candle": "2026-09-28T00:00:00+00:00",
                "notification_candidates": [candidate],
                "telegram_text_ru": "fallback",
            }
        )
        self.addCleanup(temp.cleanup)
        state_path = Path(temp.name) / "state.json"
        state_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "sent_event_ids": [candidate["event_id"]],
                }
            ),
            encoding="utf-8",
        )

        with mock.patch.dict(
            "os.environ",
            {
                "TELEGRAM_BOT_TOKEN": "fake-token",
                "TELEGRAM_CHAT_ID": "fake-chat",
            },
            clear=False,
        ), mock.patch.object(sender, "_send_telegram") as send:
            rc = sender.main(
                [
                    "--report-json",
                    str(report_path),
                    "--notification-text",
                    str(notification_path),
                    "--state-file",
                    str(state_path),
                    "--evening-confirmed-reminder",
                ]
            )

        self.assertEqual(rc, 0)
        send.assert_called_once()
        text = send.call_args.args[0]
        self.assertIn("вечернее напоминание 22:30 Ереван", text)
        self.assertIn("ALGO -> FIL", text)
        state = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertIn(candidate["event_id"], state["sent_event_ids"])
        self.assertIn(
            "EVENING_REMINDER|" + candidate["event_id"],
            state["sent_event_ids"],
        )

    def test_evening_reminder_ignores_armed_and_stale_confirmed(self):
        report = {
            "should_notify": True,
            "latest_closed_candle": "2026-09-28T00:00:00+00:00",
            "notification_candidates": [
                {
                    "event_id": "BOOK_2|ARMED|2026-09-28T00:00:00+00:00|ALGO|FIL|ALGO/FIL",
                    "book_id": "BOOK_2",
                    "book": {"held_asset": "ALGO", "quantity": 10723.76037691},
                    "event": {
                        "date": "2026-09-28T00:00:00+00:00",
                        "event": "ARMED",
                        "pair": "ALGO/FIL",
                        "from_asset": "ALGO",
                        "to_asset": "FIL",
                        "max_dislocation": 0.16,
                        "reversal_from_extreme": 0.0,
                    },
                },
                {
                    "event_id": "BOOK_2|CONFIRMED|2026-09-27T00:00:00+00:00|ALGO|HBAR|ALGO/HBAR",
                    "book_id": "BOOK_2",
                    "book": {"held_asset": "ALGO", "quantity": 10723.76037691},
                    "event": {
                        "date": "2026-09-27T00:00:00+00:00",
                        "event": "CONFIRMED",
                        "pair": "ALGO/HBAR",
                        "from_asset": "ALGO",
                        "to_asset": "HBAR",
                        "max_dislocation": 0.25,
                        "reversal_from_extreme": 0.04,
                    },
                },
            ],
            "telegram_text_ru": "fallback",
        }
        temp, report_path, notification_path = self._files(report)
        self.addCleanup(temp.cleanup)
        state_path = Path(temp.name) / "state.json"

        with mock.patch.object(sender, "_send_telegram") as send:
            rc = sender.main(
                [
                    "--report-json",
                    str(report_path),
                    "--notification-text",
                    str(notification_path),
                    "--state-file",
                    str(state_path),
                    "--evening-confirmed-reminder",
                ]
            )

        self.assertEqual(rc, 0)
        send.assert_not_called()

    def test_evening_reminder_is_deduplicated_separately(self):
        candidate = {
            "event_id": "BOOK_1|CONFIRMED|2026-09-28T00:00:00+00:00|ATOM|AVAX|ATOM/AVAX",
            "book_id": "BOOK_1",
            "book": {"held_asset": "ATOM", "quantity": None},
            "event": {
                "date": "2026-09-28T00:00:00+00:00",
                "event": "CONFIRMED",
                "pair": "ATOM/AVAX",
                "from_asset": "ATOM",
                "to_asset": "AVAX",
                "max_dislocation": 0.21,
                "reversal_from_extreme": 0.034,
            },
        }
        temp, report_path, notification_path = self._files(
            {
                "should_notify": False,
                "latest_closed_candle": "2026-09-28T00:00:00+00:00",
                "notification_candidates": [candidate],
                "telegram_text_ru": "fallback",
            }
        )
        self.addCleanup(temp.cleanup)
        state_path = Path(temp.name) / "state.json"
        state_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "sent_event_ids": [
                        candidate["event_id"],
                        "EVENING_REMINDER|" + candidate["event_id"],
                    ],
                }
            ),
            encoding="utf-8",
        )

        with mock.patch.object(sender, "_send_telegram") as send:
            rc = sender.main(
                [
                    "--report-json",
                    str(report_path),
                    "--notification-text",
                    str(notification_path),
                    "--state-file",
                    str(state_path),
                    "--evening-confirmed-reminder",
                ]
            )

        self.assertEqual(rc, 0)
        send.assert_not_called()


if __name__ == "__main__":
    unittest.main()
