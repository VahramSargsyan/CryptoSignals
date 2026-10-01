from __future__ import annotations

import argparse
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

STATE_SCHEMA_VERSION = 1
MAX_SENT_EVENT_IDS = 500
PRIMARY_EXECUTION_SLOT_YEREVAN = "10:30"
FALLBACK_EXECUTION_WINDOW_YEREVAN = "22:30"
YEREVAN = ZoneInfo("Asia/Yerevan")


def _send_telegram(text: str, *, reply_markup: dict | None = None) -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID are not configured")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup

    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
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


def _load_notification_control(path: Path | None) -> dict:
    if path is None or not path.exists():
        return {
            "schema_version": 1,
            "timezone": "Asia/Yerevan",
            "morning_slot": PRIMARY_EXECUTION_SLOT_YEREVAN,
            "evening_slot": FALLBACK_EXECUTION_WINDOW_YEREVAN,
            "evening_requires_explicit_missed_morning": True,
            "missed_morning_signals": [],
        }
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload.setdefault("missed_morning_signals", [])
    return payload


def _current_yerevan_date() -> str:
    return datetime.now(YEREVAN).date().isoformat()


def _is_evening_armed(item: dict, control: dict, *, local_date: str | None = None) -> bool:
    local_date = local_date or _current_yerevan_date()
    event = item.get("event", {})
    event_id = str(item.get("event_id") or "")
    book_id = str(item.get("book_id") or "").upper()
    for entry in control.get("missed_morning_signals", []):
        if str(entry.get("book_id") or "").upper() != book_id:
            continue
        if str(entry.get("event_id") or "") != event_id:
            continue
        if str(entry.get("armed_local_date") or "") != local_date:
            continue
        if str(entry.get("from_asset") or "").upper() != str(event.get("from_asset") or "").upper():
            continue
        if str(entry.get("to_asset") or "").upper() != str(event.get("to_asset") or "").upper():
            continue
        return True
    return False


def _book_position_ru(book: dict) -> str:
    asset = str(book.get("held_asset") or "")
    quantity = book.get("quantity")
    if quantity is None:
        return asset
    return f"{float(quantity):g} {asset}"


def _event_line_ru(event: dict) -> str:
    event_type = str(event.get("event") or "")
    if event.get("route_override"):
        prefix = "🚨 CONFIRMED / AUTO ROUTE"
    else:
        prefix = "🚨 CONFIRMED" if event_type == "CONFIRMED" else "⚠️ ARM / PREWATCH"
    max_dislocation = event.get("max_dislocation")
    reversal = event.get("reversal_from_extreme")
    max_text = "n/a" if max_dislocation is None else f"{float(max_dislocation) * 100:.2f}%"
    rev_text = "n/a" if reversal is None else f"{float(reversal) * 100:.2f}%"
    return (
        f"{prefix}: {event.get('from_asset')} -> {event.get('to_asset')} "
        f"({event.get('pair')}; отклонение {max_text}; разворот {rev_text})"
    )


def _pct(value) -> str:
    return "n/a" if value is None else f"{float(value) * 100:.2f}%"


def _route_conflict_lines_ru(item: dict) -> list[str]:
    conflicts = item.get("route_conflicts") or []
    if not conflicts:
        return []
    effective = item.get("event", {})
    lines = ["🔀 DESTINATION DOMINANCE — стратегия автоматически заменила маршрут."]
    for conflict in conflicts:
        primary = conflict.get("primary", {})
        competing = conflict.get("competing_candidate", {})
        relation = conflict.get("destination_relation", {})
        lines.extend(
            [
                (
                    f"{conflict.get('severity')}: основной {primary.get('from_asset')} -> "
                    f"{primary.get('to_asset')} CONFIRMED."
                ),
                (
                    f"Более сильный кандидат: {competing.get('from_asset')} -> "
                    f"{competing.get('to_asset')} (отклонение {_pct(competing.get('max_dislocation'))}; "
                    f"разворот {_pct(competing.get('reversal_from_extreme'))}; "
                    f"статус {competing.get('event')})."
                ),
                (
                    f"Между назначениями: {relation.get('from_asset')} -> "
                    f"{relation.get('to_asset')} (отклонение {_pct(relation.get('max_dislocation'))}; "
                    f"разворот {_pct(relation.get('reversal_from_extreme'))}; "
                    f"статус {relation.get('event')})."
                ),
                (
                    "Возможен промежуточный маршрут: "
                    + " -> ".join(conflict.get("possible_intermediate_path", []))
                    + "."
                ),
            ]
        )
    lines.extend(
        [
            (
                f"Исполняемый маршрут: {effective.get('from_asset')} -> "
                f"{effective.get('to_asset')}."
            ),
            "One-click относится уже к этому эффективному маршруту, а не к исходному промежуточному назначению.",
            "⚠️ FORWARD WATCH: rolling-окна исторического теста были нестабильны; результат override нужно отслеживать отдельно.",
        ]
    )
    return lines


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


def _latest_confirmed_by_book(report: dict) -> list[dict]:
    """Return the latest strongest unresolved CONFIRMED candidate per live book.

    notification_candidates are already restricted by the monitor to routes whose
    from_asset equals the book's currently configured held_asset. Therefore an
    executed trade disappears from this set as soon as the book is updated.
    """
    grouped: dict[str, list[dict]] = {}
    for item in report.get("notification_candidates", []):
        event = item.get("event", {})
        if event.get("event") != "CONFIRMED":
            continue
        event_id = str(item.get("event_id") or "")
        book_id = str(item.get("book_id") or "")
        if not event_id or not book_id:
            continue
        grouped.setdefault(book_id, []).append(item)

    selected: list[dict] = []
    for book_id in sorted(grouped):
        rows = grouped[book_id]
        latest_date = max(str(row.get("event", {}).get("date") or "") for row in rows)
        same_date = [
            row
            for row in rows
            if str(row.get("event", {}).get("date") or "") == latest_date
        ]
        same_date.sort(
            key=lambda row: (
                -float(row.get("event", {}).get("max_dislocation") or 0.0),
                str(row.get("event", {}).get("to_asset") or ""),
                str(row.get("event", {}).get("pair") or ""),
            )
        )
        if same_date:
            selected.append(same_date[0])
    return selected


def _telegram_control_enabled() -> bool:
    return os.environ.get("RR_TELEGRAM_CONTROL_ENABLED", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _callback_quantity(book: dict) -> str:
    quantity = book.get("quantity")
    if quantity is None:
        return "-"
    text = f"{float(quantity):.12g}"
    return text if len(text) <= 20 else "-"


def _callback_date_token(event: dict) -> str:
    raw = str(event.get("date") or "")
    if len(raw) < 10:
        return ""
    return raw[:10].replace("-", "")


def _execution_reply_markup(
    items: list[dict], *, include_missed: bool = True
) -> dict | None:
    if not _telegram_control_enabled():
        return None

    rows = []
    for item in items:
        event = item.get("event", {})
        if event.get("event") != "CONFIRMED":
            continue
        if item.get("route_conflicts") and not event.get("route_override"):
            continue
        book = item.get("book", {})
        book_id = str(item.get("book_id") or "")
        from_asset = str(event.get("from_asset") or "").upper()
        to_asset = str(event.get("to_asset") or "").upper()
        date_token = _callback_date_token(event)
        quantity = _callback_quantity(book)
        if not book_id or not from_asset or not to_asset or not date_token:
            continue

        done_data = "|".join(
            ["rrd", book_id, from_asset, to_asset, date_token, quantity]
        )
        if len(done_data.encode("utf-8")) > 64:
            done_data = "|".join(
                ["rrd", book_id, from_asset, to_asset, date_token, "-"]
            )
        if len(done_data.encode("utf-8")) > 64:
            continue

        row = [
            {
                "text": f"✅ Выполнено {book_id}",
                "callback_data": done_data,
            }
        ]
        if include_missed:
            later_data = f"rrl|{book_id}"
            row.append(
                {
                    "text": f"⏰ Пропустил утром {book_id}",
                    "callback_data": later_data,
                }
            )
        rows.append(row)

    return {"inline_keyboard": rows} if rows else None


def _execution_reminder_id(kind: str, report: dict, item: dict) -> str:
    slot = str(report.get("latest_closed_candle") or "")
    return f"{kind.upper()}_PENDING|{slot}|{item.get('event_id') or ''}"


def _pending_execution_candidates(
    report: dict,
    state: dict,
    *,
    kind: str,
    control: dict | None = None,
    local_date: str | None = None,
) -> list[dict]:
    """Repeat unresolved CONFIRMED routes once per reminder kind.

    Evening reminders are fail-closed: they are eligible only when the user
    explicitly marked that morning signal as missed for the same Yerevan date.
    """
    sent = set(str(value) for value in state.get("sent_event_ids", []))
    pending = []
    for item in _latest_confirmed_by_book(report):
        if kind.upper() == "EVENING":
            if control is None or not _is_evening_armed(
                item, control, local_date=local_date
            ):
                continue
        reminder_id = _execution_reminder_id(kind, report, item)
        if reminder_id in sent:
            continue
        pending.append(item)
    return pending


def _evening_confirmed_candidates(
    report: dict, state: dict, control: dict | None = None
) -> list[dict]:
    """Compatibility wrapper for the explicitly armed evening reminder."""
    return _pending_execution_candidates(
        report, state, kind="EVENING", control=control
    )


def build_execution_reminder_ru(report: dict, pending: list[dict], *, kind: str) -> str:
    is_evening = kind.upper() == "EVENING"
    header = (
        "🌙 Relative Rotation — вечернее напоминание об НЕИСПОЛНЕННОЙ ротации"
        if is_evening
        else "⏰ Relative Rotation — повтор НЕИСПОЛНЕННОЙ ротации"
    )
    lines = [
        header,
        f"Последняя закрытая D1-свеча: {report.get('latest_closed_candle')}",
        "Это не новый сигнал. Напоминание остаётся активным, пока BOOK всё ещё держит исходный актив.",
        (
            f"Правило времени: утренний слот {PRIMARY_EXECUTION_SLOT_YEREVAN} и "
            f"вечерний слот {FALLBACK_EXECUTION_WINDOW_YEREVAN} по Еревану."
        ),
        (
            "Вечерний этап не запускается автоматически: сначала в Telegram нужно "
            "отметить, что утренний сигнал пропущен. Перед вечером CONFIRMED проверяется заново."
        ),
    ]

    for item in pending:
        book = item.get("book", {})
        event = item.get("event", {})
        event_date = str(event.get("date") or "")
        lines.extend(
            [
                "",
                f"{item.get('book_id')} — текущая позиция: {_book_position_ru(book)}",
                f"CONFIRMED-свеча: {event_date}",
                _event_line_ru(event),
            ]
        )
        price_line = _current_event_price_line_ru(report, event)
        if price_line:
            lines.append(price_line)
        lines.extend(_route_conflict_lines_ru(item))
        if is_evening:
            lines.append(
                f"Вечерний этап был явно включён после отметки пропущенного утра; "
                f"текущий слот {FALLBACK_EXECUTION_WINDOW_YEREVAN} по Еревану."
            )
        else:
            lines.append(
                f"Это утренний слот {PRIMARY_EXECUTION_SLOT_YEREVAN}. Если не успел — "
                "нажми «⏰ Пропустил утром»; без этой отметки вечернего сообщения не будет."
            )

    lines.extend(
        [
            "",
            "Если сделка уже выполнена — НЕ повторяй её; зафиксируй сделку и обнови held_asset/quantity.",
            "После смены held_asset это напоминание прекращается автоматически.",
            "БУМАЖНЫЙ/РУЧНОЙ РЕЖИМ — реальные ордера не отправляются.",
        ]
    )
    return "\n".join(lines)


def build_evening_reminder_ru(report: dict, pending: list[dict]) -> str:
    return build_execution_reminder_ru(report, pending, kind="EVENING")


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
        lines.extend(_route_conflict_lines_ru(item))

        if event.get("event") == "CONFIRMED":
            if is_stale:
                lines.append("Это исторически пропущенный подтверждённый сигнал; смотри текущую последнюю свечу отдельно.")
            else:
                lines.append("Сигнал модели подтверждён на последней закрытой свече; исполнение остаётся ручным.")
                lines.append(
                    f"Утренний слот: {PRIMARY_EXECUTION_SLOT_YEREVAN} по Еревану. "
                    f"Если пропущен, отметь это в Telegram; только тогда будет вечерний этап "
                    f"в {FALLBACK_EXECUTION_WINDOW_YEREVAN}."
                )
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
    parser.add_argument("--notification-control-file", type=Path)
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
            "At the evening scheduled/retry runs, repeat the latest unresolved "
            "CONFIRMED route for each currently configured live book."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = json.loads(args.report_json.read_text(encoding="utf-8"))
    state = _load_state(args.state_file)
    notification_control = _load_notification_control(
        args.notification_control_file
    )
    pending = _pending_candidates(report, state)

    legacy_text = args.notification_text.read_text(encoding="utf-8").strip()
    fallback_text = str(report.get("telegram_text_ru") or legacy_text).strip()

    morning_pending: list[dict] = []
    evening_pending: list[dict] = []
    reply_markup: dict | None = None

    if args.evening_confirmed_reminder:
        evening_pending = _pending_execution_candidates(
            report,
            state,
            kind="EVENING",
            control=notification_control,
        )
        if not evening_pending:
            print("notification=SKIPPED_EVENING_NO_PENDING_EXECUTION")
            _save_state(args.state_file, state)
            return 0
        text = build_execution_reminder_ru(report, evening_pending, kind="EVENING").strip()
        reply_markup = _execution_reply_markup(
            evening_pending, include_missed=False
        )
    elif pending:
        text = build_pending_notification_ru(report, pending).strip()
        confirmed_pending = _latest_confirmed_by_book(
            {"notification_candidates": pending}
        )
        reply_markup = _execution_reply_markup(confirmed_pending)
    elif args.morning_rotation_snapshot:
        morning_pending = _pending_execution_candidates(report, state, kind="MORNING")
        if morning_pending:
            text = build_execution_reminder_ru(report, morning_pending, kind="MORNING").strip()
            reply_markup = _execution_reply_markup(morning_pending)
        else:
            text = fallback_text
    elif bool(report.get("should_notify", False)):
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

    if reply_markup:
        _send_telegram(text, reply_markup=reply_markup)
    else:
        _send_telegram(text)

    if args.evening_confirmed_reminder:
        sent_ids = [str(value) for value in state.get("sent_event_ids", [])]
        sent_ids.extend(
            _execution_reminder_id("EVENING", report, item)
            for item in evening_pending
        )
        state["sent_event_ids"] = sent_ids[-MAX_SENT_EVENT_IDS:]
        _save_state(args.state_file, state)
        print(f"notification_events_sent={len(evening_pending)}")
        print("notification=EVENING_PENDING_EXECUTION_REMINDER")
    elif pending:
        sent_ids = [str(value) for value in state.get("sent_event_ids", [])]
        sent_ids.extend(str(item["event_id"]) for item in pending)
        state["sent_event_ids"] = sent_ids[-MAX_SENT_EVENT_IDS:]
        _save_state(args.state_file, state)
        print(f"notification_events_sent={len(pending)}")
        print("notification=REPLAY_OR_NEW_SIGNAL")
    else:
        if args.morning_rotation_snapshot and morning_pending:
            sent_ids = [str(value) for value in state.get("sent_event_ids", [])]
            sent_ids.extend(
                _execution_reminder_id("MORNING", report, item)
                for item in morning_pending
            )
            state["sent_event_ids"] = sent_ids[-MAX_SENT_EVENT_IDS:]
            _save_state(args.state_file, state)
            print(f"notification_events_sent={len(morning_pending)}")
            print("notification=MORNING_PENDING_EXECUTION_REMINDER")
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
