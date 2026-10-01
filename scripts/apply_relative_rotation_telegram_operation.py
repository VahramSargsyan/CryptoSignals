from __future__ import annotations

import argparse
import json
import math
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from scripts.apply_relative_rotation_manual_execution import (
    latest_confirmed,
    read_json,
    update_summary,
    write_json,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "config" / "relative_rotation_paper_live_v1.json"
DEFAULT_CONTROL = ROOT / "config" / "relative_rotation_notification_control_v1.json"
DEFAULT_LOG = ROOT / "research" / "relative_rotation" / "REAL_ROTATION_LOG.md"
YEREVAN = ZoneInfo("Asia/Yerevan")


def _positive(value, name: str) -> float:
    number = float(str(value).replace(",", "."))
    if not math.isfinite(number) or number <= 0:
        raise ValueError(f"{name} must be positive")
    return number


def _normalize_book(value: str) -> str:
    book = str(value or "").strip().upper()
    if book and not re.fullmatch(r"BOOK_[1-9][0-9]*", book):
        raise ValueError("book_id must look like BOOK_1")
    return book


def _normalize_asset(value: str) -> str:
    asset = str(value or "").strip().upper()
    if not re.fullmatch(r"[A-Z0-9]{2,12}", asset):
        raise ValueError("asset is invalid")
    return asset


def _parse_timestamp(value: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise ValueError("confirmed_at is required")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        raise ValueError("confirmed_at must include timezone")
    return parsed


def _load_control(path: Path) -> dict:
    if not path.exists():
        return {
            "schema_version": 1,
            "timezone": "Asia/Yerevan",
            "morning_slot": "10:30",
            "evening_slot": "22:30",
            "evening_requires_explicit_missed_morning": True,
            "missed_morning_signals": [],
        }
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload.setdefault("schema_version", 1)
    payload.setdefault("timezone", "Asia/Yerevan")
    payload.setdefault("morning_slot", "10:30")
    payload.setdefault("evening_slot", "22:30")
    payload.setdefault("evening_requires_explicit_missed_morning", True)
    payload.setdefault("missed_morning_signals", [])
    return payload


def _write_control(path: Path, payload: dict) -> None:
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def _resolve_latest_confirmed(report: dict, requested_book: str) -> tuple[str, dict]:
    requested_book = _normalize_book(requested_book)
    if requested_book:
        return requested_book, latest_confirmed(report, requested_book)

    books = sorted(
        {
            str(item.get("book_id") or "").upper()
            for item in report.get("notification_candidates", [])
            if item.get("event", {}).get("event") == "CONFIRMED"
            and str(item.get("book_id") or "").strip()
        }
    )
    resolved = []
    for book_id in books:
        try:
            resolved.append((book_id, latest_confirmed(report, book_id)))
        except ValueError:
            continue
    if not resolved:
        raise ValueError("no unresolved CONFIRMED candidate")
    if len(resolved) > 1:
        raise ValueError("multiple books have unresolved CONFIRMED candidates; specify BOOK_1 or BOOK_2")
    return resolved[0]


def mark_morning_missed(
    *,
    control_path: Path,
    report_path: Path,
    book_id: str,
    confirmed_at: str,
    telegram_update_id: str,
) -> dict:
    report = read_json(report_path)
    control = _load_control(control_path)
    book_id, item = _resolve_latest_confirmed(report, book_id)
    event = item.get("event", {})
    event_id = str(item.get("event_id") or "").strip()
    if not event_id:
        raise ValueError(f"{book_id}: confirmed candidate has no event_id")

    timestamp = _parse_timestamp(confirmed_at)
    local_date = timestamp.astimezone(YEREVAN).date().isoformat()
    update_id = str(telegram_update_id or "").strip()

    entries = list(control.get("missed_morning_signals", []))
    if update_id and any(
        str(entry.get("telegram_update_id") or "") == update_id for entry in entries
    ):
        return {
            "changed": False,
            "reason": "ALREADY_APPLIED",
            "book_id": book_id,
            "event_id": event_id,
            "armed_local_date": local_date,
        }

    new_entry = {
        "book_id": book_id,
        "event_id": event_id,
        "signal_date": str(event.get("date") or ""),
        "from_asset": str(event.get("from_asset") or "").upper(),
        "to_asset": str(event.get("to_asset") or "").upper(),
        "armed_local_date": local_date,
        "armed_at": timestamp.isoformat(),
        "telegram_update_id": update_id,
    }

    existing = next(
        (
            entry
            for entry in entries
            if str(entry.get("book_id") or "").upper() == book_id
            and str(entry.get("event_id") or "") == event_id
            and str(entry.get("armed_local_date") or "") == local_date
        ),
        None,
    )
    if existing is not None:
        return {
            "changed": False,
            "reason": "ALREADY_ARMED_TODAY",
            "book_id": book_id,
            "event_id": event_id,
            "armed_local_date": local_date,
        }

    entries = [
        entry
        for entry in entries
        if str(entry.get("book_id") or "").upper() != book_id
    ]
    entries.append(new_entry)
    control["missed_morning_signals"] = entries
    _write_control(control_path, control)
    return {
        "changed": True,
        "reason": "MORNING_MISSED_ARMED",
        "book_id": book_id,
        "event_id": event_id,
        "from_asset": new_entry["from_asset"],
        "to_asset": new_entry["to_asset"],
        "armed_local_date": local_date,
    }


def _position_log_entry(
    *,
    operation_id: str,
    book_id: str,
    previous_asset: str,
    previous_quantity,
    asset: str,
    quantity: float,
    confirmed_at: str,
    telegram_update_id: str,
) -> str:
    previous_quantity_text = (
        "not recorded"
        if previous_quantity is None
        else f"{float(previous_quantity):.12g} {previous_asset}"
    )
    return f"""
### {operation_id} — TELEGRAM POSITION SYNC

Book:

`{book_id}`

User-declared current position:

- previous tracked state: `{previous_quantity_text}`;
- current asset: `{asset}`;
- current quantity: `{quantity:.12g} {asset}`;
- Telegram confirmation timestamp: `{confirmed_at}`;
- telegram position update id: {telegram_update_id}.

Evidence boundary:

- this is an explicit user-declared current-position synchronization;
- it does not create an exchange order;
- it does not invent a CONFIRMED strategy event, fee, slippage, route, or trade ID;
- if the asset changed, the repository records the new real holding but does not reconstruct missing execution details.
""".strip()


def set_position(
    *,
    config_path: Path,
    control_path: Path,
    log_path: Path,
    book_id: str,
    asset: str,
    quantity,
    confirmed_at: str,
    telegram_update_id: str,
) -> dict:
    book_id = _normalize_book(book_id)
    if not book_id:
        raise ValueError("book_id is required for SET_POSITION")
    asset = _normalize_asset(asset)
    quantity_value = _positive(quantity, "quantity")
    timestamp = _parse_timestamp(confirmed_at)
    update_id = str(telegram_update_id or "").strip()

    config = read_json(config_path)
    control = _load_control(control_path)
    text = log_path.read_text(encoding="utf-8")

    marker = f"telegram position update id: {update_id}."
    if update_id and marker in text:
        return {
            "changed": False,
            "reason": "ALREADY_APPLIED",
            "book_id": book_id,
            "asset": asset,
            "quantity": quantity_value,
        }

    books = config.get("position_books", [])
    book = next(
        (
            item
            for item in books
            if str(item.get("book_id") or "").upper() == book_id
        ),
        None,
    )
    if book is None:
        raise ValueError(f"Unknown book_id: {book_id}")

    allowed = {
        str(value).upper()
        for value in (
            list(config.get("target_assets", []))
            + list(config.get("sunset_assets", []))
            + [book.get("held_asset")]
        )
        if str(value or "").strip()
    }
    if asset not in allowed:
        raise ValueError(
            f"{book_id}: asset {asset} is outside configured target/sunset/current assets"
        )

    previous_asset = str(book.get("held_asset") or "").upper()
    previous_quantity = book.get("quantity")

    book["held_asset"] = asset
    book["quantity"] = quantity_value
    book["quantity_source"] = (
        f"TELEGRAM_MANUAL_POSITION_SYNC_{timestamp.astimezone(YEREVAN).date().isoformat()}"
    )
    if book is books[0]:
        config["held_asset"] = asset

    text = update_summary(text, book_id, asset, quantity_value)
    day = timestamp.astimezone(YEREVAN).date().strftime("%Y%m%d")
    suffix = update_id or timestamp.astimezone(YEREVAN).strftime("%H%M%S")
    operation_id = f"POS-{book_id.replace('_', '')}-{day}-TG{suffix}"
    text = (
        text.rstrip()
        + "\n\n"
        + _position_log_entry(
            operation_id=operation_id,
            book_id=book_id,
            previous_asset=previous_asset,
            previous_quantity=previous_quantity,
            asset=asset,
            quantity=quantity_value,
            confirmed_at=timestamp.isoformat(),
            telegram_update_id=update_id,
        )
        + "\n"
    )

    control["missed_morning_signals"] = [
        entry
        for entry in control.get("missed_morning_signals", [])
        if str(entry.get("book_id") or "").upper() != book_id
    ]

    write_json(config_path, config)
    _write_control(control_path, control)
    log_path.write_text(text, encoding="utf-8")
    return {
        "changed": True,
        "reason": "POSITION_SYNCED",
        "operation_id": operation_id,
        "book_id": book_id,
        "previous_asset": previous_asset,
        "asset": asset,
        "quantity": quantity_value,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--action",
        required=True,
        choices=["MARK_MORNING_MISSED", "SET_POSITION"],
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--control", type=Path, default=DEFAULT_CONTROL)
    parser.add_argument("--log", type=Path, default=DEFAULT_LOG)
    parser.add_argument("--report-json", type=Path)
    parser.add_argument("--book-id", default="")
    parser.add_argument("--asset", default="")
    parser.add_argument("--quantity", default="")
    parser.add_argument("--confirmed-at", required=True)
    parser.add_argument("--telegram-update-id", default="")
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    if args.action == "MARK_MORNING_MISSED":
        if args.report_json is None:
            raise ValueError("--report-json is required for MARK_MORNING_MISSED")
        result = mark_morning_missed(
            control_path=args.control,
            report_path=args.report_json,
            book_id=args.book_id,
            confirmed_at=args.confirmed_at,
            telegram_update_id=args.telegram_update_id,
        )
    else:
        result = set_position(
            config_path=args.config,
            control_path=args.control,
            log_path=args.log,
            book_id=args.book_id,
            asset=args.asset,
            quantity=args.quantity,
            confirmed_at=args.confirmed_at,
            telegram_update_id=args.telegram_update_id,
        )

    for key, value in result.items():
        print(f"{key}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
