from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts.send_relative_rotation_report import (
    _book_position_ru,
    _callback_date_token,
    _callback_quantity,
    _event_line_ru,
    _send_telegram,
    _telegram_control_enabled,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "config" / "relative_rotation_paper_live_v1.json"
ACTIONS = ("POSITIONS", "STATUS", "EXECUTION", "MISSED")


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_current_confirmed(report: dict) -> list[dict]:
    """Return current primary CONFIRMED candidates that are safe to expose as actions."""
    result: list[dict] = []
    for book_id, details in sorted((report.get("book_events") or {}).items()):
        book = details.get("book") or {}
        events = details.get("events") or {}
        event = events.get("primary_confirmed")
        if not event:
            continue

        conflicts = details.get("route_conflicts") or (
            report.get("route_conflicts", {}).get(book_id, [])
        )
        if conflicts and not event.get("route_override"):
            # Match the existing notification button safety rule: do not expose an
            # action button while destination routing is unresolved.
            continue

        result.append(
            {
                "book_id": str(book_id),
                "book": book,
                "event": event,
                "route_conflicts": conflicts,
            }
        )
    return result


def _done_callback(item: dict) -> str | None:
    event = item.get("event", {})
    book = item.get("book", {})
    book_id = str(item.get("book_id") or "")
    from_asset = str(event.get("from_asset") or "").upper()
    to_asset = str(event.get("to_asset") or "").upper()
    date_token = _callback_date_token(event)
    quantity = _callback_quantity(book)
    if not book_id or not from_asset or not to_asset or not date_token:
        return None
    value = "|".join(["rrd", book_id, from_asset, to_asset, date_token, quantity])
    if len(value.encode("utf-8")) > 64:
        value = "|".join(["rrd", book_id, from_asset, to_asset, date_token, "-"])
    if len(value.encode("utf-8")) > 64:
        return None
    return value


def _action_markup(
    items: list[dict],
    *,
    include_done: bool,
    include_missed: bool,
) -> dict | None:
    if not _telegram_control_enabled():
        return None

    rows = []
    for item in items:
        book_id = str(item.get("book_id") or "")
        row = []
        if include_done:
            done = _done_callback(item)
            if done:
                row.append(
                    {
                        "text": f"✅ Выполнено {book_id}",
                        "callback_data": done,
                    }
                )
        if include_missed and book_id:
            row.append(
                {
                    "text": f"⏰ Пропустил утром {book_id}",
                    "callback_data": f"rrl|{book_id}",
                }
            )
        if row:
            rows.append(row)
    return {"inline_keyboard": rows} if rows else None


def _position_edit_markup(config: dict) -> dict | None:
    if not _telegram_control_enabled():
        return None
    rows = []
    for book in config.get("position_books", []):
        book_id = str(book.get("book_id") or "").upper()
        if not book_id:
            continue
        rows.append(
            [
                {
                    "text": f"✏️ Исправить {book_id}",
                    "callback_data": f"rrp|{book_id}",
                }
            ]
        )
    return {"inline_keyboard": rows} if rows else None


def build_positions_text(config: dict) -> str:
    lines = ["📊 Мои позиции", ""]
    books = config.get("position_books", [])
    if not books:
        return "📊 Мои позиции\n\nПозиции не настроены."

    for index, book in enumerate(books):
        if index:
            lines.append("")
        book_id = str(book.get("book_id") or f"BOOK_{index + 1}")
        asset = str(book.get("held_asset") or "")
        quantity = book.get("quantity")
        lines.append(book_id)
        if quantity is None:
            lines.append(f"{asset} — количество пока не записано")
        else:
            lines.append(f"{float(quantity):.12g} {asset}")

    lines.extend(
        [
            "",
            "Это каноническое состояние position_books в GitHub.",
            "Если реальный баланс отличается, используй кнопку «Исправить» ниже.",
        ]
    )
    return "\n".join(lines)


def _pct(value) -> str:
    return "n/a" if value is None else f"{float(value) * 100:.2f}%"


def build_status_text(report: dict) -> str:
    latest = report.get("latest_closed_candle")
    lines = [
        "📡 Relative Rotation — текущий статус",
        f"Последняя закрытая D1-свеча: {latest}",
    ]

    book_events = report.get("book_events") or {}
    for book_id, details in sorted(book_events.items()):
        book = details.get("book") or {}
        events = details.get("events") or {}
        primary = events.get("primary_confirmed")
        armed = list(events.get("armed") or [])

        lines.extend(["", f"{book_id} — {_book_position_ru(book)}"])
        if primary:
            lines.append("🚨 CONFIRMED")
            lines.append(_event_line_ru(primary))
        elif armed:
            strongest = sorted(
                armed,
                key=lambda event: -float(event.get("max_dislocation") or 0.0),
            )[0]
            lines.append("⚠️ ARM / PREWATCH")
            lines.append(_event_line_ru(strongest))
        else:
            lines.append("Нет текущего CONFIRMED / ARM.")

    if not book_events:
        lines.append("")
        lines.append("Нет данных по position_books в текущем отчёте.")

    lines.extend(
        [
            "",
            "Кнопки исполнения показываются только для текущего безопасного CONFIRMED.",
            "Реальные ордера бот не отправляет.",
        ]
    )
    return "\n".join(lines)


def build_execution_text(report: dict, items: list[dict]) -> str:
    if not items:
        return (
            "✅ Выполнил ротацию\n\n"
            "Сейчас нет актуальной безопасной CONFIRMED-ротации для текущих BOOK. "
            "Ничего записывать не нужно."
        )

    lines = ["✅ Выполнил ротацию", "", "Актуальные CONFIRMED:"]
    for item in items:
        event = item["event"]
        lines.extend(
            [
                "",
                f"{item['book_id']} — {_book_position_ru(item.get('book', {}))}",
                _event_line_ru(event),
            ]
        )
    lines.extend(
        [
            "",
            "Нажми «✅ Выполнено» только после фактического обмена.",
            "После этого бот спросит, сколько нового актива реально получено.",
        ]
    )
    return "\n".join(lines)


def build_missed_text(report: dict, items: list[dict]) -> str:
    if not items:
        return (
            "⏰ Пропустил сигнал\n\n"
            "Сейчас нет актуальной безопасной CONFIRMED-ротации, которую можно "
            "отметить как пропущенную."
        )

    lines = [
        "⏰ Пропустил сигнал",
        "",
        "Выбери BOOK, утренний CONFIRMED которого ты не успел исполнить:",
    ]
    for item in items:
        lines.extend(
            [
                "",
                f"{item['book_id']} — {_book_position_ru(item.get('book', {}))}",
                _event_line_ru(item["event"]),
            ]
        )
    lines.extend(
        [
            "",
            "После выбора GitHub ещё раз проверит сигнал и только затем включит вечерний этап 22:30.",
        ]
    )
    return "\n".join(lines)


def send_menu_action(
    *,
    action: str,
    config_path: Path,
    report_path: Path | None,
) -> None:
    action = str(action).upper()
    if action not in ACTIONS:
        raise ValueError(f"Unsupported menu action: {action}")

    config = _read_json(config_path)

    if action == "POSITIONS":
        _send_telegram(
            build_positions_text(config),
            reply_markup=_position_edit_markup(config),
        )
        return

    if report_path is None:
        raise ValueError(f"--report-json is required for {action}")
    report = _read_json(report_path)
    items = _safe_current_confirmed(report)

    if action == "STATUS":
        _send_telegram(
            build_status_text(report),
            reply_markup=_action_markup(
                items,
                include_done=True,
                include_missed=True,
            ),
        )
    elif action == "EXECUTION":
        _send_telegram(
            build_execution_text(report, items),
            reply_markup=_action_markup(
                items,
                include_done=True,
                include_missed=False,
            ),
        )
    else:
        _send_telegram(
            build_missed_text(report, items),
            reply_markup=_action_markup(
                items,
                include_done=False,
                include_missed=True,
            ),
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Send an on-demand Relative Rotation Telegram menu response."
    )
    parser.add_argument("--action", required=True, choices=ACTIONS)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--report-json", type=Path)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    send_menu_action(
        action=args.action,
        config_path=args.config,
        report_path=args.report_json,
    )
    print(f"telegram_menu_action={args.action}")
    print("telegram=SENT")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
