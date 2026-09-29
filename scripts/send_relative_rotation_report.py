from __future__ import annotations

import argparse
import json
import os
import urllib.parse
import urllib.request
from pathlib import Path

STATE_SCHEMA_VERSION = 1
MAX_SENT_EVENT_IDS = 500


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


def _load_state(path: Path | None) -> dict:
    if path is None or not path.exists():
        return {"schema_version": STATE_SCHEMA_VERSION, "sent_event_ids": []}

    payload = json.loads(path.read_text(encoding="utf-8"))
    ids = [str(value) for value in payload.get("sent_event_ids", []) if str(value).strip()]
    return {
        "schema_version": STATE_SCHEMA_VERSION,
        "sent_event_ids": ids[-MAX_SENT_EVENT_IDS:],
    }


def _save_state(path: Path | None, state: dict) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": STATE_SCHEMA_VERSION,
        "sent_event_ids": [str(value) for value in state.get("sent_event_ids", [])][-MAX_SENT_EVENT_IDS:],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _book_position_ru(book: dict) -> str:
    asset = str(book.get("held_asset") or "")
    quantity = book.get("quantity")
    if quantity is None:
        return asset
    return f"{float(quantity):g} {asset}"


def _event_line_ru(event: dict) -> str:
    event_type = str(event.get("event") or "")
    prefix = "🚨 CONFIRMED" if event_type == "CONFIRMED" else "⚠️ ARM / PREWATCH"
    max_dislocation = event.get("max_dislocation")
    reversal = event.get("reversal_from_extreme")
    max_text = "n/a" if max_dislocation is None else f"{float(max_dislocation) * 100:.2f}%"
    rev_text = "n/a" if reversal is None else f"{float(reversal) * 100:.2f}%"
    return (
        f"{prefix}: {event.get('from_asset')} -> {event.get('to_asset')} "
        f"({event.get('pair')}; отклонение {max_text}; разворот {rev_text})"
    )


def _format_usdt_price(value: float) -> str:
    if value >= 100:
        digits = 2
    elif value >= 1:
        digits = 4
    elif value >= 0.01:
        digits = 6
    elif value >= 0.0001:
        digits = 8
    else:
        digits = 10
    text = f"{value:.{digits}f}".rstrip("0").rstrip(".")
    return f"${text}"


def _format_relative_units(value: float) -> str:
    if value >= 1000:
        return f"{value:,.2f}".replace(",", " ").rstrip("0").rstrip(".")
    if value >= 1:
        return f"{value:.4f}".rstrip("0").rstrip(".")
    if value >= 0.01:
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return f"{value:.8f}".rstrip("0").rstrip(".")


def _current_event_price_line_ru(report: dict, event: dict) -> str | None:
    if str(event.get("date") or "") != str(report.get("latest_closed_candle") or ""):
        return None

    prices = report.get("latest_close_prices_usdt", {})
    from_asset = str(event.get("from_asset") or "")
    to_asset = str(event.get("to_asset") or "")
    from_price = prices.get(from_asset)
    to_price = prices.get(to_asset)
    if from_price is None or to_price is None:
        return None

    from_price = float(from_price)
    to_price = float(to_price)
    rate = from_price / to_price
    return (
        f"Цена закрытия: {from_asset} {_format_usdt_price(from_price)}; "
        f"{to_asset} {_format_usdt_price(to_price)}; "
        f"1 {from_asset} = {_format_relative_units(rate)} {to_asset}."
    )


def _pending_candidates(report: dict, state: dict) -> list[dict]:
    sent = set(str(value) for value in state.get("sent_event_ids", []))
    pending = []
    for item in report.get("notification_candidates", []):
        event_id = str(item.get("event_id") or "")
        if not event_id or event_id in sent:
            continue
        pending.append(item)
    return pending


def _evening_reminder_id(item: dict) -> str:
    return f"EVENING_REMINDER|{item.get('event_id') or ''}"


def _evening_confirmed_candidates(report: dict, state: dict) -> list[dict]:
    """Return same-candle CONFIRMED events not yet repeated by the evening reminder."""
    latest = str(report.get("latest_closed_candle") or "")
    sent = set(str(value) for value in state.get("sent_event_ids", []))
    pending = []
    for item in report.get("notification_candidates", []):
        event = item.get("event", {})
        if event.get("event") != "CONFIRMED":
            continue
        if str(event.get("date") or "") != latest:
            continue
        reminder_id = _evening_reminder_id(item)
        if not item.get("event_id") or reminder_id in sent:
            continue
        pending.append(item)
    return pending


def build_evening_reminder_ru(report: dict, pending: list[dict]) -> str:
    lines = [
        "🌙 Relative Rotation — вечернее напоминание 22:30 Ереван",
        f"Утренний сигнал рассчитан по закрытой D1-свече: {report.get('latest_closed_candle')}",
        "Это НЕ новый сигнал: повторяется только сегодняшний CONFIRMED для текущей позиции.",
    ]

    for item in pending:
        book = item.get("book", {})
        event = item.get("event", {})
        lines.extend(
            [
                "",
                f"{item.get('book_id')} — текущая позиция: {_book_position_ru(book)}",
                _event_line_ru(event),
            ]
        )
        price_line = _current_event_price_line_ru(report, event)
        if price_line:
            lines.append(price_line)
        lines.append("Если утром не успел исполнить, это вечерний fallback перед поздним окном исполнения.")

    lines.extend(
        [
            "",
            "Если сделка уже выполнена — НЕ повторяй её; обнови позицию/реальный лог.",
            "БУМАЖНЫЙ/РУЧНОЙ РЕЖИМ — реальные ордера не отправляются.",
        ]
    )
    return "\n".join(lines)


def build_pending_notification_ru(report: dict, pending: list[dict]) -> str:
    lines = [
        "Relative Rotation — новое/восстановленное уведомление",
        f"Последняя закрытая свеча: {report.get('latest_closed_candle')}",
    ]

    latest = str(report.get("latest_closed_candle") or "")
    for item in pending:
        book = item.get("book", {})
        event = item.get("event", {})
        event_date = str(event.get("date") or "")
        is_stale = bool(latest and event_date and event_date != latest)

        lines.extend(
            [
                "",
                f"{item.get('book_id')} — текущая позиция: {_book_position_ru(book)}",
                f"Свеча события: {event_date}",
            ]
        )

        if is_stale:
            lines.append("↩️ ВОССТАНОВЛЕННОЕ ПРОПУЩЕННОЕ СОБЫТИЕ — не считать текущей командой.")
        lines.append(_event_line_ru(event))
        price_line = _current_event_price_line_ru(report, event)
        if price_line:
            lines.append(price_line)

        if event.get("event") == "CONFIRMED":
            if is_stale:
                lines.append("Это исторически пропущенный подтверждённый сигнал; смотри текущую последнюю свечу отдельно.")
            else:
                lines.append("Сигнал модели подтверждён на последней закрытой свече; исполнение остаётся ручным.")
        else:
            lines.append("Это текущий PREWATCH: ротации пока нет, ждём подтверждение 3%.")

    lines.extend(
        [
            "",
            "БУМАЖНЫЙ/РУЧНОЙ РЕЖИМ — реальные ордера не отправляются.",
        ]
    )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Send Relative Rotation paper-live Telegram alert.")
    parser.add_argument("--report-json", type=Path, required=True)
    parser.add_argument("--notification-text", type=Path, required=True)
    parser.add_argument("--state-file", type=Path)
    parser.add_argument(
        "--morning-rotation-snapshot",
        action="store_true",
        help=(
            "At the morning scheduled run, send the current Relative Rotation "
            "snapshot even when there is no new ARMED/CONFIRMED event."
        ),
    )
    parser.add_argument(
        "--evening-confirmed-reminder",
        action="store_true",
        help=(
            "At the 22:30 Yerevan scheduled run, repeat only same-candle "
            "CONFIRMED events for currently configured live books."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = json.loads(args.report_json.read_text(encoding="utf-8"))
    state = _load_state(args.state_file)
    pending = _pending_candidates(report, state)

    legacy_text = args.notification_text.read_text(encoding="utf-8").strip()
    fallback_text = str(report.get("telegram_text_ru") or legacy_text).strip()

    if args.evening_confirmed_reminder:
        evening_pending = _evening_confirmed_candidates(report, state)
        if not evening_pending:
            print("notification=SKIPPED_EVENING_NO_CONFIRMED")
            _save_state(args.state_file, state)
            return 0
        text = build_evening_reminder_ru(report, evening_pending).strip()
    elif pending:
        text = build_pending_notification_ru(report, pending).strip()
    elif bool(report.get("should_notify", False)) or args.morning_rotation_snapshot:
        text = fallback_text
    else:
        print("notification=SKIPPED_POLICY")
        _save_state(args.state_file, state)
        return 0

    if not text:
        raise RuntimeError("notification text is empty")

    if not os.environ.get("TELEGRAM_BOT_TOKEN", "").strip() or not os.environ.get("TELEGRAM_CHAT_ID", "").strip():
        print("notification=SKIPPED_MISSING_TELEGRAM_SECRETS")
        _save_state(args.state_file, state)
        return 0

    _send_telegram(text)

    if args.evening_confirmed_reminder:
        sent_ids = [str(value) for value in state.get("sent_event_ids", [])]
        sent_ids.extend(_evening_reminder_id(item) for item in evening_pending)
        state["sent_event_ids"] = sent_ids[-MAX_SENT_EVENT_IDS:]
        _save_state(args.state_file, state)
        print(f"notification_events_sent={len(evening_pending)}")
        print("notification=EVENING_CONFIRMED_REMINDER")
    elif pending:
        sent_ids = [str(value) for value in state.get("sent_event_ids", [])]
        sent_ids.extend(str(item["event_id"]) for item in pending)
        state["sent_event_ids"] = sent_ids[-MAX_SENT_EVENT_IDS:]
        _save_state(args.state_file, state)
        print(f"notification_events_sent={len(pending)}")
        print("notification=REPLAY_OR_NEW_SIGNAL")
    else:
        _save_state(args.state_file, state)
        if args.morning_rotation_snapshot and not bool(report.get("should_notify", False)):
            print("notification=MORNING_ROTATION_SNAPSHOT")
        else:
            print("notification=SIGNAL_OR_FORCE_NOTIFY")

    print("telegram=SENT")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
