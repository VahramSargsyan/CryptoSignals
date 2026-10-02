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


def _signed_pct(value) -> str:
    if value is None:
        return "n/a"
    return f"{float(value) * 100:+.2f}%"


def _pair_assets(pair: str) -> tuple[str, str] | None:
    parts = str(pair or "").upper().split("/", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        return None
    return parts[0], parts[1]


def _directional_rotation_value(row: dict, source: str, target: str) -> float | None:
    """Return signed current dislocation in the SOURCE -> TARGET direction.

    Pair monitor stores deviation as RIGHT/LEFT versus its 180D median.
    Positive output here means the current displacement points toward rotating
    out of SOURCE and into TARGET under the frozen mean-reversion semantics.
    Negative output means the same pair currently points in the opposite direction.
    """
    pair_assets = _pair_assets(str(row.get("pair") or ""))
    deviation = row.get("deviation")
    if pair_assets is None or deviation is None:
        return None

    left, right = pair_assets
    source = str(source or "").upper()
    target = str(target or "").upper()
    deviation = float(deviation)

    if source == right and target == left:
        return deviation
    if source == left and target == right:
        return -deviation
    return None


def _book_rotation_rows(report: dict, details: dict) -> list[dict]:
    book = details.get("book") or {}
    source = str(book.get("held_asset") or "").upper()
    targets = [
        str(asset).upper()
        for asset in report.get("target_assets", [])
        if str(asset or "").strip() and str(asset).upper() != source
    ]
    confirmed_by_target = {
        str(event.get("to_asset") or "").upper(): event
        for event in (details.get("events") or {}).get("confirmed", [])
        if str(event.get("from_asset") or "").upper() == source
    }

    state_by_target: dict[str, dict] = {}
    for row in report.get("latest_pair_states", []):
        for target in targets:
            value = _directional_rotation_value(row, source, target)
            if value is None:
                continue
            state_by_target[target] = {
                "row": row,
                "value": value,
            }

    result = []
    for target in targets:
        state_info = state_by_target.get(target, {})
        state_row = state_info.get("row") or {}
        value = state_info.get("value")
        confirmed = confirmed_by_target.get(target)

        intraday = report.get("report_kind") == "H1_INTRADAY_STATUS"
        mode = str(state_row.get("mode") or "NONE").upper()
        prospective_from = str(state_row.get("from_asset") or "").upper()
        prospective_to = str(state_row.get("to_asset") or "").upper()

        if intraday:
            if value is not None and float(value) >= 0.15:
                status = "⚠️ ARM-зона"
            elif value is not None and float(value) <= -0.15:
                status = "↩️ ARM-зона обратно"
            else:
                status = "• NONE"
            confirmed = None
        elif confirmed is not None:
            status = "🚨 CONFIRMED"
        elif (
            mode in {"HIGH", "LOW"}
            and prospective_from == source
            and prospective_to == target
        ):
            status = "⚠️ ARM"
        elif (
            mode in {"HIGH", "LOW"}
            and prospective_from == target
            and prospective_to == source
        ):
            status = "↩️ ARM обратно"
        else:
            status = "• NONE"

        result.append(
            {
                "target": target,
                "value": value,
                "status": status,
                "state": state_row,
                "confirmed": confirmed,
                "intraday": intraday,
            }
        )

    return sorted(
        result,
        key=lambda item: (
            item["value"] is None,
            -(float(item["value"]) if item["value"] is not None else -999.0),
            item["target"],
        ),
    )


def _rotation_row_text(index: int, item: dict) -> str:
    target = item["target"]
    value = item.get("value")
    status = item.get("status")
    state = item.get("state") or {}
    confirmed = item.get("confirmed")

    parts = [f"{index}. {target}: {_signed_pct(value)}", status]

    if item.get("intraday"):
        if value is not None and float(value) >= 0.15:
            parts.append(f"выше ARM на {(float(value) - 0.15) * 100:.2f} п.п.")
        elif value is not None and float(value) <= -0.15:
            parts.append(f"обратное превышение ARM на {(-float(value) - 0.15) * 100:.2f} п.п.")
        elif value is not None:
            parts.append(f"до ARM {(0.15 - float(value)) * 100:.2f} п.п.")
    elif confirmed is not None:
        parts.append(
            f"max {_pct(confirmed.get('max_dislocation'))}; "
            f"разворот {_pct(confirmed.get('reversal_from_extreme'))}"
        )
    elif status == "⚠️ ARM":
        parts.append(
            f"max {_pct(state.get('max_dislocation'))}; "
            f"разворот {_pct(state.get('reversal_from_extreme'))}/3.00%"
        )
    elif status == "↩️ ARM обратно":
        parts.append(
            f"max {_pct(state.get('max_dislocation'))}; "
            f"разворот {_pct(state.get('reversal_from_extreme'))}/3.00%"
        )
    elif value is not None:
        gap = 0.15 - float(value)
        parts.append(f"до ARM {gap * 100:.2f} п.п.")

    return " | ".join(parts)


def build_status_text(report: dict) -> str:
    intraday = report.get("report_kind") == "H1_INTRADAY_STATUS"
    if intraday:
        latest = (
            report.get("latest_closed_candle_end_yerevan")
            or report.get("latest_closed_candle_end")
            or report.get("latest_closed_candle")
        )
        lines = [
            "📡 Relative Rotation — текущая H1-ротация",
            f"Последняя закрытая H1-свеча: {latest}",
            "",
            "Расчёт использует 180 дней закрытых часовых свечей "
            f"({report.get('lookback_observations', 4320)} H1-наблюдений).",
            "Рейтинг ниже показывает текущую внутридневную относительную ротацию "
            "из актива каждого BOOK в TARGET-активы.",
        ]
    else:
        latest = report.get("latest_closed_candle")
        lines = [
            "📡 Relative Rotation — полная текущая ротация",
            f"Последняя закрытая D1-свеча: {latest}",
            "",
            "Рейтинг ниже показывает ВСЕ направления из текущего актива BOOK "
            "в TARGET-активы, а не только сигналы.",
        ]

    book_events = report.get("book_events") or {}
    for book_id, details in sorted(book_events.items()):
        book = details.get("book") or {}
        rows = _book_rotation_rows(report, details)

        lines.extend(
            [
                "",
                f"{book_id} — текущая позиция: {_book_position_ru(book)}",
                "От сильнейшего текущего направления к слабейшему:",
            ]
        )
        if rows:
            for index, item in enumerate(rows, start=1):
                lines.append(_rotation_row_text(index, item))
        else:
            lines.append("Нет доступных pair-state данных для TARGET-направлений.")

    if not book_events:
        lines.append("")
        lines.append("Нет данных по position_books в текущем отчёте.")

    lines.extend(
        [
            "",
            "Как читать %: плюс = текущая относительная позиция пары уже направлена "
            "от удерживаемого актива к указанному TARGET; минус = сейчас сильнее "
            "обратное направление.",
            "Это отклонение отношения цен от 180-дневной медианы, НЕ доходность и "
            "не самостоятельный сигнал.",
            (
                "H1 здесь показывает только текущую ARM-зону. Официальные ARM/CONFIRMED "
                "и исполнение стратегии остаются по D1."
                if intraday
                else "ARM начинается при +15%; CONFIRMED требует последующего разворота 3%."
            ),
            "",
            (
                "Для H1-снимка кнопки исполнения намеренно отключены."
                if intraday
                else "Кнопки исполнения показываются только для текущего безопасного CONFIRMED."
            ),
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
            reply_markup=None,
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
