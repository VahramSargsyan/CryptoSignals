from __future__ import annotations

import argparse
import json
import os
import smtplib
import ssl
import urllib.parse
import urllib.request
from email.message import EmailMessage
from pathlib import Path


def _send_telegram(text: str) -> bool:
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        print("telegram=SKIPPED_MISSING_SECRETS")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode(
        {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": "true",
        }
    ).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=20) as response:
        if response.status >= 300:
            raise RuntimeError(f"Telegram HTTP {response.status}")
    print("telegram=SENT")
    return True


def _send_email(subject: str, text: str) -> bool:
    host = os.environ.get("SMTP_HOST", "").strip()
    port_text = os.environ.get("SMTP_PORT", "").strip()
    username = os.environ.get("SMTP_USERNAME", "").strip()
    password = os.environ.get("SMTP_PASSWORD", "")
    sender = os.environ.get("REPORT_EMAIL_FROM", "").strip()
    recipient = os.environ.get("REPORT_EMAIL_TO", "").strip()

    if not host or not recipient or not sender:
        print("email=SKIPPED_MISSING_SECRETS")
        return False

    port = int(port_text or "587")
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient
    msg.set_content(text)

    context = ssl.create_default_context()
    if port == 465:
        with smtplib.SMTP_SSL(host, port, context=context, timeout=20) as smtp:
            if username:
                smtp.login(username, password)
            smtp.send_message(msg)
    else:
        with smtplib.SMTP(host, port, timeout=20) as smtp:
            smtp.ehlo()
            smtp.starttls(context=context)
            smtp.ehlo()
            if username:
                smtp.login(username, password)
            smtp.send_message(msg)

    print("email=SENT")
    return True


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Send optional grid paper-live notifications.")
    parser.add_argument("--report-json", type=Path, required=True)
    parser.add_argument("--notification-text", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = json.loads(args.report_json.read_text(encoding="utf-8"))
    text = args.notification_text.read_text(encoding="utf-8").strip()

    if not report.get("should_notify", False):
        print("notification=SKIPPED_POLICY")
        return 0

    subject_bits = ["Grid Paper Live"]
    if report.get("milestone"):
        subject_bits.append(str(report["milestone"]))
    elif report.get("latest_closed_candle"):
        subject_bits.append(str(report["latest_closed_candle"])[:10])
    subject = " — ".join(subject_bits)

    configured = 0
    failures: list[str] = []

    telegram_configured = bool(
        os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
        and os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    )
    if telegram_configured:
        configured += 1
        try:
            _send_telegram(text)
        except Exception as exc:
            failures.append(f"telegram: {exc}")
            print(f"telegram=FAILED {exc}")
    else:
        print("telegram=SKIPPED_MISSING_SECRETS")

    email_configured = bool(
        os.environ.get("SMTP_HOST", "").strip()
        and os.environ.get("REPORT_EMAIL_FROM", "").strip()
        and os.environ.get("REPORT_EMAIL_TO", "").strip()
    )
    if email_configured:
        configured += 1
        try:
            _send_email(subject, text)
        except Exception as exc:
            failures.append(f"email: {exc}")
            print(f"email=FAILED {exc}")
    else:
        print("email=SKIPPED_MISSING_SECRETS")

    if configured == 0:
        print("notification=NO_CHANNEL_CONFIGURED")
        return 0
    if failures:
        raise RuntimeError("; ".join(failures))

    print("notification=SENT")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
