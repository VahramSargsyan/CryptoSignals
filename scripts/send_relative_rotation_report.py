from __future__ import annotations

import argparse
import json
import os
import urllib.parse
import urllib.request
from pathlib import Path


def _send_telegram(text: str) -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID are not configured")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode(
        {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": "true",
        }
    ).encode("utf-8")
    request = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(request, timeout=20) as response:
        if response.status >= 300:
            raise RuntimeError(f"Telegram HTTP {response.status}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Send Relative Rotation paper-live Telegram alert.")
    parser.add_argument("--report-json", type=Path, required=True)
    parser.add_argument("--notification-text", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = json.loads(args.report_json.read_text(encoding="utf-8"))

    legacy_text = args.notification_text.read_text(encoding="utf-8").strip()
    text = str(report.get("telegram_text_ru") or legacy_text).strip()
    if not text:
        raise RuntimeError("notification text is empty")

    if not os.environ.get("TELEGRAM_BOT_TOKEN", "").strip() or not os.environ.get("TELEGRAM_CHAT_ID", "").strip():
        print("notification=SKIPPED_MISSING_TELEGRAM_SECRETS")
        return 0

    _send_telegram(text)
    print("telegram=SENT")
    print("notification=DAILY_HEARTBEAT_OR_SIGNAL")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
