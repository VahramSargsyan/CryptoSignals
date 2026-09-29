from __future__ import annotations

import argparse
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "config" / "relative_rotation_paper_live_v1.json"
DEFAULT_LOG = ROOT / "research" / "relative_rotation" / "REAL_ROTATION_LOG.md"


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict) -> None:
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def signal_iso(token: str) -> str:
    if not re.fullmatch(r"20\d{6}", token):
        raise ValueError("signal_date must be YYYYMMDD")
    dt = datetime(int(token[:4]), int(token[4:6]), int(token[6:8]), tzinfo=timezone.utc)
    return dt.isoformat()


def positive(value, name: str) -> float:
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise ValueError(f"{name} must be positive")
    return number


def latest_confirmed(report: dict, book_id: str) -> dict:
    rows = [
        item
        for item in report.get("notification_candidates", [])
        if str(item.get("book_id") or "").upper() == book_id
        and item.get("event", {}).get("event") == "CONFIRMED"
    ]
    if not rows:
        raise ValueError(f"{book_id}: no unresolved CONFIRMED candidate")
    latest_date = max(str(item["event"].get("date") or "") for item in rows)
    rows = [item for item in rows if str(item["event"].get("date") or "") == latest_date]
    rows.sort(
        key=lambda item: (
            -float(item["event"].get("max_dislocation") or 0.0),
            str(item["event"].get("to_asset") or ""),
            str(item["event"].get("pair") or ""),
        )
    )
    return rows[0]


def update_summary(text: str, book_id: str, asset: str, quantity: float) -> str:
    heading = re.search(rf"^### {re.escape(book_id)}\b.*$", text, flags=re.MULTILINE)
    if not heading:
        raise ValueError(f"{book_id}: summary heading not found")
    next_heading = re.search(
        r"^### BOOK_\d+\b|^## Recording rule\b",
        text[heading.end():],
        flags=re.MULTILINE,
    )
    end = heading.end() + next_heading.start() if next_heading else len(text)
    section = text[heading.start():end]

    held = re.compile(r"(Current held asset:\s*\n\s*`)[^`]+(`)", re.MULTILINE)
    if not held.search(section):
        raise ValueError(f"{book_id}: current held asset block not found")
    section = held.sub(rf"\g<1>{asset}\g<2>", section, count=1)

    quantity_block = f"Current tracked quantity:\n\n`{quantity:.12g} {asset}`"
    tracked = re.compile(r"Current tracked quantity:\s*\n\s*`[^`]+`", re.MULTILINE)
    if tracked.search(section):
        section = tracked.sub(quantity_block, section, count=1)
    else:
        match = held.search(section)
        section = section[: match.end()] + "\n\n" + quantity_block + section[match.end():]

    return text[: heading.start()] + section + text[end:]


def pct(value) -> str:
    return "n/a" if value is None else f"{float(value) * 100:.12g}%"


def log_entry(rotation_id, book_id, event, confirmed_at, sent, received, update_id) -> str:
    source = str(event["from_asset"]).upper()
    target = str(event["to_asset"]).upper()
    ratio = received / sent
    deviation = event.get("deviation")
    deviation_line = (
        f"- deviation from 180d median: {pct(deviation)};\n" if deviation is not None else ""
    )
    return f"""
### {rotation_id} - {source} -> {target}

Book: {book_id}

Signal evidence:

- signal closed candle: {event.get('date')};
- signal state: CONFIRMED;
- model pair: {event.get('pair')};
- route: {source} -> {target};
- maximum dislocation: {pct(event.get('max_dislocation'))};
{deviation_line}- reversal from post-ARM extreme: {pct(event.get('reversal_from_extreme'))};
- strategy threshold: 15% ARM / 3% reversal confirmation.

Manual execution confirmation:

- source: Telegram control bridge;
- Telegram confirmation timestamp: {confirmed_at};
- quantity sent: {sent:.12g} {source};
- quantity received: {received:.12g} {target};
- effective aggregate cross ratio: 1 {source} = {ratio:.12g} {target};
- fee: not supplied;
- slippage: not independently measured;
- exchange order/trade ID: not supplied;
- exact exchange execution timestamp is not independently verified;
- telegram update id: {update_id}.

Position after execution:

- configured held asset: {target};
- configured quantity: {received:.12g} {target};
- execution remains manual-only; no exchange API order was placed;
- GitHub re-validated this as the latest strongest unresolved CONFIRMED route.
"""


def apply_execution(
    *,
    config_path: Path,
    log_path: Path,
    report_path: Path,
    book_id: str,
    signal_date: str,
    from_asset: str,
    to_asset: str,
    sent_quantity,
    received_quantity,
    confirmed_at: str,
    telegram_update_id: str,
) -> dict:
    book_id = book_id.upper()
    from_asset = from_asset.upper()
    to_asset = to_asset.upper()
    sent = positive(sent_quantity, "sent_quantity")
    received = positive(received_quantity, "received_quantity")

    config = read_json(config_path)
    report = read_json(report_path)
    text = log_path.read_text(encoding="utf-8")

    marker = f"telegram update id: {telegram_update_id}."
    if telegram_update_id and marker in text:
        return {"changed": False, "reason": "ALREADY_APPLIED", "book_id": book_id}

    books = config.get("position_books", [])
    book = next(
        (item for item in books if str(item.get("book_id") or "").upper() == book_id),
        None,
    )
    if book is None:
        raise ValueError(f"Unknown book_id: {book_id}")
    current = str(book.get("held_asset") or "").upper()
    if current != from_asset:
        raise ValueError(f"{book_id}: current asset is {current}, not {from_asset}")
    if to_asset not in {str(x).upper() for x in config.get("target_assets", [])}:
        raise ValueError(f"{book_id}: destination {to_asset} is outside TARGET")

    configured_quantity = book.get("quantity")
    if configured_quantity is not None and not math.isclose(
        float(configured_quantity), sent, rel_tol=1e-8, abs_tol=1e-8
    ):
        raise ValueError(
            f"{book_id}: sent quantity {sent} does not match configured {configured_quantity}"
        )

    item = latest_confirmed(report, book_id)
    event = item["event"]
    if str(event.get("date") or "") != signal_iso(signal_date):
        raise ValueError(f"{book_id}: signal date is no longer canonical")
    if str(event.get("from_asset") or "").upper() != from_asset:
        raise ValueError(f"{book_id}: source asset mismatch")
    if str(event.get("to_asset") or "").upper() != to_asset:
        raise ValueError(
            f"{book_id}: canonical route is {event.get('from_asset')} -> {event.get('to_asset')}"
        )

    day = confirmed_at[:10].replace("-", "")
    rotation_id = f"ROT-{book_id.replace('_', '')}-{day}-TG{telegram_update_id}"

    book["held_asset"] = to_asset
    book["quantity"] = received
    book["quantity_source"] = f"TELEGRAM_USER_CONFIRMED_MANUAL_ROTATION_{confirmed_at[:10]}"
    if book is books[0]:
        config["held_asset"] = to_asset

    text = update_summary(text, book_id, to_asset, received)
    text = text.rstrip() + "\n\n" + log_entry(
        rotation_id, book_id, event, confirmed_at, sent, received, telegram_update_id
    ).strip() + "\n"

    write_json(config_path, config)
    log_path.write_text(text, encoding="utf-8")
    return {
        "changed": True,
        "reason": "APPLIED",
        "rotation_id": rotation_id,
        "book_id": book_id,
        "from_asset": from_asset,
        "to_asset": to_asset,
        "sent_quantity": sent,
        "received_quantity": received,
    }


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    p.add_argument("--log", type=Path, default=DEFAULT_LOG)
    p.add_argument("--report-json", type=Path, required=True)
    p.add_argument("--book-id", required=True)
    p.add_argument("--signal-date", required=True)
    p.add_argument("--from-asset", required=True)
    p.add_argument("--to-asset", required=True)
    p.add_argument("--sent-quantity", required=True)
    p.add_argument("--received-quantity", required=True)
    p.add_argument("--confirmed-at", required=True)
    p.add_argument("--telegram-update-id", default="")
    return p


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    result = apply_execution(
        config_path=args.config,
        log_path=args.log,
        report_path=args.report_json,
        book_id=args.book_id,
        signal_date=args.signal_date,
        from_asset=args.from_asset,
        to_asset=args.to_asset,
        sent_quantity=args.sent_quantity,
        received_quantity=args.received_quantity,
        confirmed_at=args.confirmed_at,
        telegram_update_id=args.telegram_update_id,
    )
    for key, value in result.items():
        print(f"{key}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
