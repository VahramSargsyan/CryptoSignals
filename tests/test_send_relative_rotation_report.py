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

    def test_sender_morning_snapshot_sends_when_policy_has_no_signal(self):
        temp, report_path, notification_path = self._files(
            {
                "should_notify": False,
                "telegram_text_ru": "morning rotation snapshot",
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
                    "--morning-rotation-snapshot",
                ]
            )

        self.assertEqual(rc, 0)
        send.assert_called_once_with("morning rotation snapshot")

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
                "latest_close_prices_usdt": {
                    "ALGO": 0.15,
                    "FIL": 3.0,
                },
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


    def test_evening_reminder_repeats_confirmed_after_initial_send(self):
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
        report = {
            "should_notify": False,
            "latest_closed_candle": "2026-09-28T00:00:00+00:00",
            "latest_close_prices_usdt": {"ALGO": 0.15, "FIL": 3.0},
            "notification_candidates": [candidate],
            "telegram_text_ru": "fallback",
        }
        temp, report_path, notification_path = self._files(report)
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
        self.assertIn("вечернее напоминание об НЕИСПОЛНЕННОЙ ротации", text)
        self.assertIn("ALGO -> FIL", text)
        self.assertIn("Цена закрытия: ALGO $0.15; FIL $3; 1 ALGO = 0.05 FIL.", text)
        state = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertIn(candidate["event_id"], state["sent_event_ids"])
        self.assertIn(
            "EVENING_PENDING|2026-09-28T00:00:00+00:00|" + candidate["event_id"],
            state["sent_event_ids"],
        )

    def test_evening_reminder_replays_latest_stale_confirmed_and_ignores_armed(self):
        confirmed = {
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
        }
        armed = {
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
        }
        temp, report_path, notification_path = self._files(
            {
                "should_notify": True,
                "latest_closed_candle": "2026-09-28T00:00:00+00:00",
                "notification_candidates": [armed, confirmed],
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
                    "--evening-confirmed-reminder",
                ]
            )

        self.assertEqual(rc, 0)
        send.assert_called_once()
        text = send.call_args.args[0]
        self.assertIn("ALGO -> HBAR", text)
        self.assertNotIn("ALGO -> FIL", text)

    def test_evening_retry_is_deduplicated_per_closed_candle(self):
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
        latest = "2026-09-28T00:00:00+00:00"
        reminder_id = f"EVENING_PENDING|{latest}|{candidate['event_id']}"
        temp, report_path, notification_path = self._files(
            {
                "should_notify": False,
                "latest_closed_candle": latest,
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
                    "sent_event_ids": [candidate["event_id"], reminder_id],
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

    def test_next_closed_candle_can_remind_again_if_still_unexecuted(self):
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
        previous = "EVENING_PENDING|2026-09-28T00:00:00+00:00|" + candidate["event_id"]
        latest = "2026-09-29T00:00:00+00:00"
        temp, report_path, notification_path = self._files(
            {
                "should_notify": False,
                "latest_closed_candle": latest,
                "notification_candidates": [candidate],
                "telegram_text_ru": "fallback",
            }
        )
        self.addCleanup(temp.cleanup)
        state_path = Path(temp.name) / "state.json"
        state_path.write_text(
            json.dumps({"schema_version": 1, "sent_event_ids": [previous]}),
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
        state = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertIn(
            "EVENING_PENDING|2026-09-29T00:00:00+00:00|" + candidate["event_id"],
            state["sent_event_ids"],
        )

    def test_morning_snapshot_repeats_unresolved_confirmed_after_base_event_sent(self):
        candidate = {
            "event_id": "BOOK_2|CONFIRMED|2026-09-28T00:00:00+00:00|LINK|ALGO|ALGO/LINK",
            "book_id": "BOOK_2",
            "book": {"held_asset": "LINK", "quantity": 100},
            "event": {
                "date": "2026-09-28T00:00:00+00:00",
                "event": "CONFIRMED",
                "pair": "ALGO/LINK",
                "from_asset": "LINK",
                "to_asset": "ALGO",
                "max_dislocation": 0.340071,
                "reversal_from_extreme": 0.050643,
            },
        }
        latest = "2026-09-29T00:00:00+00:00"
        temp, report_path, notification_path = self._files(
            {
                "should_notify": False,
                "latest_closed_candle": latest,
                "notification_candidates": [candidate],
                "telegram_text_ru": "ordinary morning snapshot",
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
                    "--morning-rotation-snapshot",
                ]
            )

        self.assertEqual(rc, 0)
        send.assert_called_once()
        text = send.call_args.args[0]
        self.assertIn("повтор НЕИСПОЛНЕННОЙ ротации", text)
        self.assertIn("LINK -> ALGO", text)
        state = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertIn(
            "MORNING_PENDING|2026-09-29T00:00:00+00:00|" + candidate["event_id"],
            state["sent_event_ids"],
        )

    def test_latest_confirmed_by_book_prefers_latest_date_then_strongest(self):
        rows = [
            {
                "event_id": "old",
                "book_id": "BOOK_1",
                "book": {"held_asset": "ATOM"},
                "event": {
                    "date": "2026-09-27T00:00:00+00:00",
                    "event": "CONFIRMED",
                    "from_asset": "ATOM",
                    "to_asset": "FIL",
                    "pair": "ATOM/FIL",
                    "max_dislocation": 0.50,
                },
            },
            {
                "event_id": "new-weaker",
                "book_id": "BOOK_1",
                "book": {"held_asset": "ATOM"},
                "event": {
                    "date": "2026-09-28T00:00:00+00:00",
                    "event": "CONFIRMED",
                    "from_asset": "ATOM",
                    "to_asset": "AVAX",
                    "pair": "ATOM/AVAX",
                    "max_dislocation": 0.20,
                },
            },
            {
                "event_id": "new-stronger",
                "book_id": "BOOK_1",
                "book": {"held_asset": "ATOM"},
                "event": {
                    "date": "2026-09-28T00:00:00+00:00",
                    "event": "CONFIRMED",
                    "from_asset": "ATOM",
                    "to_asset": "TWT",
                    "pair": "ATOM/TWT",
                    "max_dislocation": 0.30,
                },
            },
        ]
        selected = sender._latest_confirmed_by_book(
            {"notification_candidates": rows}
        )
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["event_id"], "new-stronger")


if __name__ == "__main__":
    unittest.main()
