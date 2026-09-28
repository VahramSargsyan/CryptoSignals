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


if __name__ == "__main__":
    unittest.main()
