from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path

import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import (
    ARM_THRESHOLD,
    ASSETS,
    DEFENSIVE_CONFIRM_DAYS,
    DEFENSIVE_ENTER_BREADTH,
    DEFENSIVE_EXIT_BREADTH,
    DEFENSIVE_SMA_LOOKBACK,
    DEFENSIVE_VOL_LOOKBACK,
    LOOKBACK,
    REVERSAL,
    TARGET_ASSETS,
    SUNSET_ASSETS,
    build_pair_monitor,
    choose_held_events,
    choose_destination_dominance_override,
    evaluate_defensive_mode,
    find_route_conflicts,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = REPO_ROOT / "config" / "relative_rotation_paper_live_v1.json"
DEFAULT_HISTORY_START = "2023-05-05T00:00:00Z"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "paper_artifacts" / "relative_rotation_paper_live_v1"
NOTIFICATION_REPLAY_DAYS = 7
EXECUTION_TIMEZONE = "Asia/Yerevan"
PRIMARY_EXECUTION_SLOT = "04:20"
FALLBACK_EXECUTION_WINDOW = "23:00–24:00"
SYMBOLS = {asset: f"{asset}USDT" for asset in ASSETS}


def _utc(value: str | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _cutoff(now: str | pd.Timestamp | None = None) -> pd.Timestamp:
    current = pd.Timestamp.now(tz="UTC") if now is None else _utc(now)
    return current.floor("D")


def _source_commit() -> str:
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO_ROOT,
        text=True,
    ).strip()


def _read_config(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    held_asset = str(payload.get("held_asset", "")).upper()
    if held_asset not in ASSETS:
        raise ValueError(f"held_asset must be one of {ASSETS}; got {held_asset!r}")

    target_assets = tuple(str(value).upper() for value in payload.get("target_assets", TARGET_ASSETS))
    sunset_assets = tuple(str(value).upper() for value in payload.get("sunset_assets", SUNSET_ASSETS))
    unknown = sorted((set(target_assets) | set(sunset_assets)).difference(ASSETS))
    if unknown:
        raise ValueError(f"target/sunset assets outside monitor universe: {unknown}")
    overlap = sorted(set(target_assets).intersection(sunset_assets))
    if overlap:
        raise ValueError(f"target_assets and sunset_assets must be disjoint; overlap={overlap}")
    if not target_assets:
        raise ValueError("target_assets cannot be empty")

    monitor_start = _utc(payload.get("monitor_start", "2026-09-27T00:00:00Z"))
    watch_assets = []
    for value in payload.get("watch_assets", []):
        asset = str(value).upper()
        if asset not in ASSETS:
            raise ValueError(f"watch_asset must be one of {ASSETS}; got {asset!r}")
        if asset not in watch_assets:
            watch_assets.append(asset)

    raw_books = payload.get("position_books")
    position_books = []
    if raw_books:
        seen_book_ids = set()
        for index, raw in enumerate(raw_books, start=1):
            book_id = str(raw.get("book_id") or f"BOOK_{index}").strip().upper()
            if not book_id:
                raise ValueError("position book_id cannot be empty")
            if book_id in seen_book_ids:
                raise ValueError(f"duplicate position book_id: {book_id}")
            seen_book_ids.add(book_id)

            asset = str(raw.get("held_asset", "")).upper()
            if asset not in ASSETS:
                raise ValueError(f"{book_id} held_asset must be one of {ASSETS}; got {asset!r}")

            quantity = raw.get("quantity")
            if quantity is not None:
                quantity = float(quantity)
                if quantity <= 0:
                    raise ValueError(f"{book_id} quantity must be positive")

            initial_quantity = raw.get("initial_quantity")
            if initial_quantity is not None:
                initial_quantity = float(initial_quantity)
                if initial_quantity <= 0:
                    raise ValueError(f"{book_id} initial_quantity must be positive")

            tracking_start = _utc(raw.get("tracking_start", monitor_start))
            position_books.append(
                {
                    **raw,
                    "book_id": book_id,
                    "label": str(raw.get("label") or book_id),
                    "held_asset": asset,
                    "quantity": quantity,
                    "initial_quantity": initial_quantity,
                    "tracking_start": tracking_start,
                }
            )
    else:
        position_books = [
            {
                "book_id": "BOOK_1",
                "label": "LEGACY_PRIMARY",
                "held_asset": held_asset,
                "quantity": None,
                "initial_quantity": None,
                "tracking_start": monitor_start,
                "quantity_source": "LEGACY_HELD_ASSET_ALIAS",
            }
        ]

    if position_books[0]["held_asset"] != held_asset:
        raise ValueError(
            "legacy held_asset must match the first position book held_asset "
            f"({held_asset} != {position_books[0]['held_asset']})"
        )

    return {
        **payload,
        "held_asset": held_asset,
        "position_books": position_books,
        "target_assets": target_assets,
        "sunset_assets": sunset_assets,
        "watch_assets": watch_assets,
        "migration_mode": bool(payload.get("migration_mode", False)),
        "defensive_overlay_enabled": bool(payload.get("defensive_overlay_enabled", True)),
        "forward_validation_enabled": bool(payload.get("forward_validation_enabled", False)),
        "forward_validation_start": _utc(payload["forward_validation_start"]) if payload.get("forward_validation_start") else None,
        "universe_version": str(payload.get("universe_version", "")),
        "monitor_start": monitor_start,
    }


def _book_payload(book: dict) -> dict:
    return {
        **book,
        "tracking_start": book["tracking_start"].isoformat(),
    }


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, default=str, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def download_panel(*, start: pd.Timestamp, cutoff: pd.Timestamp) -> tuple[pd.DataFrame, dict]:
    client = BinanceSpotRestClient()
    pieces: list[pd.DataFrame] = []
    metadata: dict[str, dict] = {}

    for asset in ASSETS:
        result = download_historical_dataset(
            client,
            symbol=SYMBOLS[asset],
            start=start,
            end=cutoff,
            timeframe="1D",
            as_of=cutoff,
        )
        if result.dataset is None:
            raise RuntimeError(f"{asset}: no closed daily dataset ({result.metadata.status})")
        if result.dataset.quality.has_critical_issues:
            raise RuntimeError(f"{asset}: critical data quality: {result.dataset.quality}")

        frame = result.dataset.candles[["timestamp", "close"]].copy()
        frame = frame.rename(columns={"close": f"{asset}_close"})
        pieces.append(frame)
        metadata[asset] = {
            "symbol": SYMBOLS[asset],
            "dataset_id": result.dataset.dataset_id,
            "rows": len(frame),
            "actual_start": result.metadata.actual_start,
            "actual_end": result.metadata.actual_end,
            "listing_truncated": result.metadata.listing_truncated,
            "status": result.metadata.status,
        }

    panel = pieces[0]
    for frame in pieces[1:]:
        panel = panel.merge(frame, on="timestamp", how="inner", validate="one_to_one")
    panel = panel.sort_values("timestamp", kind="stable").reset_index(drop=True)
    if panel.empty:
        raise RuntimeError(f"Common {len(ASSETS)}-asset panel is empty")

    latest = pd.Timestamp(panel.iloc[-1]["timestamp"])
    metadata["panel"] = {
        "rows": len(panel),
        "start": pd.Timestamp(panel.iloc[0]["timestamp"]).isoformat(),
        "end": latest.isoformat(),
    }
    return panel, metadata


def _pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100:+.2f}%"


def _format_usdt_price(value: float | None) -> str:
    if value is None:
        return "n/a"
    price = float(value)
    if price >= 100:
        digits = 2
    elif price >= 1:
        digits = 4
    elif price >= 0.01:
        digits = 6
    elif price >= 0.0001:
        digits = 8
    else:
        digits = 10
    text = f"{price:.{digits}f}".rstrip("0").rstrip(".")
    return f"${text}"


def _format_relative_units(value: float | None) -> str:
    if value is None:
        return "n/a"
    units = float(value)
    if units >= 1000:
        return f"{units:,.2f}".replace(",", " ").rstrip("0").rstrip(".")
    if units >= 1:
        return f"{units:.4f}".rstrip("0").rstrip(".")
    if units >= 0.01:
        return f"{units:.6f}".rstrip("0").rstrip(".")
    return f"{units:.8f}".rstrip("0").rstrip(".")


def _price_line_ru(payload: dict, from_asset: str, to_asset: str) -> str | None:
    prices = payload.get("latest_close_prices_usdt", {})
    from_price = prices.get(from_asset)
    to_price = prices.get(to_asset)
    if from_price is None or to_price is None:
        return None

    rate = float(from_price) / float(to_price)
    return (
        f"Цена закрытия: {from_asset} {_format_usdt_price(float(from_price))}; "
        f"{to_asset} {_format_usdt_price(float(to_price))}; "
        f"1 {from_asset} = {_format_relative_units(rate)} {to_asset}."
    )


def _event_line(event: dict) -> str:
    prefix = "AUTO ROUTE: " if event.get("route_override") else ""
    return (
        f"{prefix}{event['from_asset']} -> {event['to_asset']} "
        f"({event['pair']}; dislocation {_pct(event.get('max_dislocation'))}; "
        f"reversal {_pct(event.get('reversal_from_extreme'))})"
    )


def _event_line_ru(event: dict) -> str:
    prefix = "AUTO ROUTE: " if event.get("route_override") else ""
    return (
        f"{prefix}{event['from_asset']} -> {event['to_asset']} "
        f"({event['pair']}; отклонение {_pct(event.get('max_dislocation'))}; "
        f"разворот от экстремума {_pct(event.get('reversal_from_extreme'))})"
    )


def _route_conflict_lines_ru(conflicts: list[dict]) -> list[str]:
    if not conflicts:
        return []
    lines = [
        "🔀 DESTINATION DOMINANCE — маршрут автоматически переопределён.",
    ]
    for conflict in conflicts:
        primary = conflict.get("primary", {})
        competing = conflict.get("competing_candidate", {})
        relation = conflict.get("destination_relation", {})
        severity = str(conflict.get("severity") or "ROUTE_CONFLICT_WARNING")
        lines.extend(
            [
                f"{severity}: основной CONFIRMED {primary.get('from_asset')} -> {primary.get('to_asset')}.",
                (
                    f"Более сильный кандидат: {competing.get('from_asset')} -> {competing.get('to_asset')} "
                    f"(отклонение {_pct(competing.get('max_dislocation'))}; "
                    f"разворот {_pct(competing.get('reversal_from_extreme'))}; "
                    f"статус {competing.get('event')})."
                ),
                (
                    f"Связь между назначениями: {relation.get('from_asset')} -> {relation.get('to_asset')} "
                    f"(отклонение {_pct(relation.get('max_dislocation'))}; "
                    f"разворот {_pct(relation.get('reversal_from_extreme'))}; "
                    f"статус {relation.get('event')})."
                ),
                (
                    "Возможен промежуточный маршрут: "
                    + " -> ".join(conflict.get("possible_intermediate_path", []))
                    + "."
                ),
                (
                    "Прямой альтернативный маршрут под наблюдением: "
                    + " -> ".join(conflict.get("direct_alternative", []))
                    + "."
                ),
            ]
        )
    chosen = conflicts[0]
    chosen_source = chosen.get("source_asset") or chosen.get("primary", {}).get("from_asset")
    chosen_to = chosen.get("competing_candidate", {}).get("to_asset")
    lines.extend(
        [
            f"Исполняемый маршрут стратегии: {chosen_source} -> {chosen_to}.",
            "Основание: основной CONFIRMED + same-source кандидат минимум 1.5× сильнее + реальная связь между назначениями.",
            "⚠️ FORWARD WATCH: исторически правило улучшило агрегатные результаты, но rolling-окна были нестабильны; это место нужно отслеживать отдельно.",
        ]
    )
    return lines


def _route_conflict_lines_en(conflicts: list[dict]) -> list[str]:
    if not conflicts:
        return []
    lines = ["DESTINATION DOMINANCE — route automatically overridden."]
    for conflict in conflicts:
        primary = conflict.get("primary", {})
        competing = conflict.get("competing_candidate", {})
        relation = conflict.get("destination_relation", {})
        lines.extend(
            [
                f"{conflict.get('severity')}: primary {primary.get('from_asset')} -> {primary.get('to_asset')}.",
                (
                    f"Stronger competing candidate: {competing.get('from_asset')} -> {competing.get('to_asset')} "
                    f"(max dislocation {_pct(competing.get('max_dislocation'))}; "
                    f"reversal {_pct(competing.get('reversal_from_extreme'))}; "
                    f"state {competing.get('event')})."
                ),
                (
                    f"Destination relation: {relation.get('from_asset')} -> {relation.get('to_asset')} "
                    f"(max dislocation {_pct(relation.get('max_dislocation'))}; "
                    f"reversal {_pct(relation.get('reversal_from_extreme'))}; "
                    f"state {relation.get('event')})."
                ),
            ]
        )
    chosen = conflicts[0]
    chosen_source = chosen.get("source_asset") or chosen.get("primary", {}).get("from_asset")
    lines.append(
        f"Effective strategy route: {chosen_source} -> "
        f"{chosen.get('competing_candidate', {}).get('to_asset')}."
    )
    lines.append(
        "Forward watch: 1.5x threshold was selected after historical testing; aggregate history improved, but rolling-window behavior remains a forward-monitoring risk."
    )
    return lines


def _notification_event_id(book_id: str, event: dict) -> str:
    """Stable notification identity for cross-run duplicate suppression."""
    return "|".join(
        [
            str(book_id).upper(),
            str(event.get("event") or "").upper(),
            str(event.get("date") or ""),
            str(event.get("from_asset") or "").upper(),
            str(event.get("to_asset") or "").upper(),
            str(event.get("pair") or ""),
        ]
    )


def build_notification_candidates(
    events: list[dict],
    *,
    position_books: list[dict],
    target_assets: tuple[str, ...] | list[str],
    latest: pd.Timestamp,
    monitor_start: pd.Timestamp,
    replay_days: int = NOTIFICATION_REPLAY_DAYS,
) -> list[dict]:
    """Build a bounded replay window for live-book ARMED/CONFIRMED alerts.

    Notification recovery deliberately starts from monitor_start rather than the
    book tracking_start. This lets a newly registered live book recover a signal
    that was already present when the book was registered, without changing any
    forward-evidence semantics.
    """
    if replay_days < 1:
        raise ValueError("replay_days must be >= 1")

    latest = _utc(latest)
    monitor_start = _utc(monitor_start)
    floor = max(monitor_start, latest - pd.Timedelta(days=replay_days))
    allowed_to = {str(asset).upper() for asset in target_assets}
    candidates: list[dict] = []

    for book in position_books:
        book_id = str(book["book_id"]).upper()
        held_asset = str(book["held_asset"]).upper()
        for event in events:
            if event.get("event") not in {"ARMED", "CONFIRMED"}:
                continue
            if str(event.get("from_asset") or "").upper() != held_asset:
                continue
            if str(event.get("to_asset") or "").upper() not in allowed_to:
                continue

            event_ts = _utc(str(event["date"]))
            if event_ts < floor or event_ts > latest:
                continue

            # CONFIRMED events are replayable because missing one can strand an
            # actionable manual signal. ARMED/PREWATCH is transient; only keep
            # it when it belongs to the latest closed candle so stale prewatch
            # does not get delivered days later.
            if event.get("event") == "ARMED" and event_ts != latest:
                continue

            candidates.append(
                {
                    "event_id": _notification_event_id(book_id, event),
                    "book_id": book_id,
                    "book": _book_payload(book),
                    "event": dict(event),
                }
            )

    event_rank = {"ARMED": 0, "CONFIRMED": 1}
    candidates.sort(
        key=lambda item: (
            str(item["event"].get("date") or ""),
            str(item["book_id"]),
            event_rank.get(str(item["event"].get("event") or ""), 9),
            str(item["event"].get("to_asset") or ""),
            str(item["event"].get("pair") or ""),
        )
    )
    return candidates


def build_notification(payload: dict) -> str:
    latest = payload["latest_closed_candle"]
    watch_events = payload.get("watch_events", {})
    defensive = payload["defensive"]
    latest_defensive_events = payload["latest_defensive_events"]

    raw_book_events = payload.get("book_events")
    if raw_book_events:
        book_events = raw_book_events
    else:
        book_events = {
            "BOOK_1": {
                "book": {
                    "book_id": "BOOK_1",
                    "held_asset": payload["held_asset"],
                    "quantity": None,
                },
                "events": payload["held_events"],
            }
        }

    lines = [
        "Relative Rotation Paper Live v1",
        f"Closed candle: {latest}",
        f"Target universe: {', '.join(payload.get('target_assets', []))}",
        f"Sunset/exit-only: {', '.join(payload.get('sunset_assets', []))}",
    ]

    for book_id, details in book_events.items():
        book = details["book"]
        quantity = book.get("quantity")
        position = book["held_asset"] if quantity is None else f"{float(quantity):g} {book['held_asset']}"
        selected = details["events"]
        lines.append(f"{book_id} held: {position}")

        primary = selected.get("primary_confirmed")
        if primary:
            lines.extend(
                [
                    f"{book_id} ROTATION CONFIRMED",
                    _event_line(primary),
                    "Historical model action only — manual approval required.",
                ]
            )
            extra = [event for event in selected.get("confirmed", []) if event is not primary]
            if extra:
                lines.append(f"Other {book_id} confirmed outbound candidates:")
                lines.extend(f"- {_event_line(event)}" for event in extra)
            conflicts = details.get("route_conflicts") or payload.get("route_conflicts", {}).get(book_id, [])
            if conflicts:
                lines.extend(_route_conflict_lines_en(conflicts))
            armed = selected.get("armed", [])
            if armed:
                lines.append(f"Other {book_id} ARMED / PREWATCH candidates (not confirmed):")
                lines.extend(f"- {_event_line(event)}" for event in armed)
        elif selected.get("armed"):
            lines.append(f"{book_id} ARMED / PREWATCH")
            lines.extend(f"- {_event_line(event)}" for event in selected["armed"])
            lines.append("15% threshold reached; no rotation until 3% reversal confirms.")
        elif payload.get("force_notify"):
            lines.append(f"{book_id}: no confirmed rotation on this candle.")

    for asset, watched in watch_events.items():
        primary_watch = watched.get("primary_confirmed")
        if primary_watch:
            lines.extend(
                [
                    f"{asset} WATCH — EXIT CONFIRMED",
                    _event_line(primary_watch),
                    "Independent sunset watch only — not a live-book position signal.",
                ]
            )
            extra_watch = [event for event in watched.get("confirmed", []) if event is not primary_watch]
            if extra_watch:
                lines.append(f"Other {asset} confirmed outbound candidates:")
                lines.extend(f"- {_event_line(event)}" for event in extra_watch)
            armed_watch = watched.get("armed", [])
            if armed_watch:
                lines.append(f"Other {asset} ARMED / PREWATCH candidates (not confirmed):")
                lines.extend(f"- {_event_line(event)}" for event in armed_watch)
        elif watched.get("armed"):
            lines.append(f"{asset} WATCH — ARMED / PREWATCH")
            lines.extend(f"- {_event_line(event)}" for event in watched["armed"])
            lines.append("15% threshold reached; wait for 3% reversal confirmation.")

    if latest_defensive_events:
        for event in latest_defensive_events:
            if event["event"] == "DEFENSIVE_ENTER":
                lines.append(
                    f"DEFENSIVE ENTER candidate: breadth {event['breadth']}/{len(payload.get('target_assets', []))}; "
                    f"low-vol token {event['defensive_asset']}."
                )
            elif event["event"] == "DEFENSIVE_EXIT":
                lines.append(
                    f"DEFENSIVE EXIT candidate: breadth {event['breadth']}/{len(payload.get('target_assets', []))}; "
                    "return-to-shadow routing remains manual."
                )

    if payload.get("defensive_overlay_enabled", True):
        lines.append(
            "Defensive status: "
            + (f"ON ({defensive['defensive_asset']})" if defensive["active"] else "OFF")
            + f"; breadth={defensive.get('breadth')}/{len(payload.get('target_assets', []))}"
        )
    else:
        lines.append("Defensive overlay: DISABLED during target migration.")
    lines.append("PAPER/MANUAL ONLY — no exchange orders, no API trading keys.")
    return "\n".join(lines) + "\n"


def _observation_candidates(
    payload: dict,
    asset: str,
    threshold: float = 0.10,
    allowed_to_assets: tuple[str, ...] | list[str] | None = None,
) -> list[dict]:
    """Return current outbound pair dislocations for an asset at/above alert threshold.

    This is notification-only. It does not change the frozen 15% ARM threshold.
    """
    candidates: list[dict] = []
    allowed_to = None if allowed_to_assets is None else {str(value).upper() for value in allowed_to_assets}
    for row in payload.get("latest_pair_states", []):
        pair = str(row.get("pair") or "")
        deviation = row.get("deviation")
        if not pair or deviation is None:
            continue

        left, right = pair.split("/", 1)
        deviation = float(deviation)
        mode = str(row.get("mode") or "NONE")

        # A previously armed pair remains operationally relevant until its
        # reversal confirms, even if the current deviation falls below 10%.
        if mode == "NONE" and abs(deviation) < threshold:
            continue

        if mode == "HIGH":
            from_asset, to_asset = right, left
        elif mode == "LOW":
            from_asset, to_asset = left, right
        elif deviation > 0:
            from_asset, to_asset = right, left
        else:
            from_asset, to_asset = left, right

        if from_asset != asset:
            continue
        if allowed_to is not None and to_asset not in allowed_to:
            continue

        max_dislocation = row.get("max_dislocation")
        candidates.append(
            {
                "pair": pair,
                "from_asset": from_asset,
                "to_asset": to_asset,
                "dislocation": abs(deviation),
                "max_dislocation": (
                    abs(float(max_dislocation))
                    if max_dislocation is not None
                    else abs(deviation)
                ),
                "mode": mode,
                "reversal_from_extreme": row.get("reversal_from_extreme"),
            }
        )

    return sorted(
        candidates,
        key=lambda x: (
            0 if x["mode"] != "NONE" else 1,
            -float(x["max_dislocation"]),
            str(x["to_asset"]),
            str(x["pair"]),
        ),
    )


def _book_position_ru(book: dict) -> str:
    quantity = book.get("quantity")
    asset = str(book.get("held_asset") or "")
    if quantity is None:
        return asset
    value = float(quantity)
    quantity_text = str(int(value)) if value.is_integer() else f"{value:g}"
    return f"{quantity_text} {asset}"


def build_notification_ru(payload: dict) -> str:
    latest = payload["latest_closed_candle"]
    watch_events = payload.get("watch_events", {})
    defensive = payload["defensive"]
    latest_defensive_events = payload["latest_defensive_events"]

    raw_book_events = payload.get("book_events")
    legacy_mode = not bool(raw_book_events)
    if raw_book_events:
        book_events = raw_book_events
    else:
        held = payload["held_asset"]
        book_events = {
            "BOOK_1": {
                "book": {
                    "book_id": "BOOK_1",
                    "label": "LEGACY_PRIMARY",
                    "held_asset": held,
                    "quantity": None,
                },
                "events": payload["held_events"],
            }
        }

    per_book_observations = {
        book_id: _observation_candidates(
            payload,
            details["book"]["held_asset"],
            threshold=0.10,
            allowed_to_assets=payload.get("target_assets"),
        )
        for book_id, details in book_events.items()
    }

    book_has_signal = any(
        details["events"].get("primary_confirmed")
        or details["events"].get("armed")
        or per_book_observations.get(book_id)
        for book_id, details in book_events.items()
    )
    watch_has_signal = any(
        watched.get("primary_confirmed") or watched.get("armed")
        for watched in watch_events.values()
    )
    detailed = bool(book_has_signal or watch_has_signal or latest_defensive_events)

    if not detailed:
        if legacy_mode:
            held = payload["held_asset"]
            snapshot = _observation_candidates(
                payload,
                held,
                threshold=0.0,
                allowed_to_assets=payload.get("target_assets"),
            )
            lines = [
                "Relative Rotation — утренний снимок",
                f"Закрытая свеча: {latest}",
                f"Текущий актив: {held}",
                "Ближайшие потенциальные ротации:",
            ]
            if snapshot:
                for candidate in snapshot[:3]:
                    gap = max(0.0, ARM_THRESHOLD - candidate["dislocation"])
                    lines.append(
                        f"- {held} -> {candidate['to_asset']}: "
                        f"{candidate['dislocation'] * 100:.2f}% отклонение; "
                        f"до ARM 15%: {gap * 100:.2f} п.п."
                    )
                    price_line = _price_line_ru(payload, held, candidate["to_asset"])
                    if price_line:
                        lines.append(f"  {price_line}")
            else:
                lines.append("- Нет исходящих TARGET-кандидатов в текущем направлении относительной силы.")
            lines.append(
                "Проценты — отклонение отношения цен от 180-дневной медианы; "
                "это ещё не CONFIRMED."
            )
            return "\n".join(lines)

        lines = [
            "Relative Rotation — утренний снимок",
            f"Закрытая свеча: {latest}",
            "Текущие ветки и ближайшие потенциальные ротации:",
        ]
        for book_id, details in book_events.items():
            book = details["book"]
            snapshot = _observation_candidates(
                payload,
                book["held_asset"],
                threshold=0.0,
                allowed_to_assets=payload.get("target_assets"),
            )
            lines.append(f"{book_id} — {_book_position_ru(book)}")
            if snapshot:
                for candidate in snapshot[:3]:
                    gap = max(0.0, ARM_THRESHOLD - candidate["dislocation"])
                    lines.append(
                        f"- {book['held_asset']} -> {candidate['to_asset']}: "
                        f"{candidate['dislocation'] * 100:.2f}% отклонение; "
                        f"до ARM 15%: {gap * 100:.2f} п.п."
                    )
                    price_line = _price_line_ru(payload, book["held_asset"], candidate["to_asset"])
                    if price_line:
                        lines.append(f"  {price_line}")
            else:
                lines.append("- Нет исходящих TARGET-кандидатов в текущем направлении относительной силы.")
        lines.append(
            "Проценты — отклонение отношения цен от 180-дневной медианы; "
            "это ещё не CONFIRMED."
        )
        return "\n".join(lines)

    lines = [
        "Relative Rotation — бумажный монитор v1",
        f"Закрытая свеча: {latest}",
    ]
    if not legacy_mode:
        books_summary = "; ".join(
            f"{book_id}={_book_position_ru(details['book'])}"
            for book_id, details in book_events.items()
        )
        lines.append(f"Реальные ветки: {books_summary}")
    else:
        lines.append(f"Текущий актив: {payload['held_asset']}")

    for book_id, details in book_events.items():
        book = details["book"]
        selected = details["events"]
        observations = per_book_observations.get(book_id, [])
        primary = selected.get("primary_confirmed")
        armed = selected.get("armed", [])

        if not legacy_mode:
            lines.append("")
            lines.append(f"{book_id} — текущая позиция: {_book_position_ru(book)}")

        if primary:
            heading = (
                "🚨 РОТАЦИЯ ПОДТВЕРЖДЕНА"
                if legacy_mode
                else f"🚨 {book_id} — РОТАЦИЯ ПОДТВЕРЖДЕНА"
            )
            lines.extend(
                [
                    heading,
                    _event_line_ru(primary),
                    *(
                        [_price_line_ru(payload, primary["from_asset"], primary["to_asset"])]
                        if _price_line_ru(payload, primary["from_asset"], primary["to_asset"])
                        else []
                    ),
                    (
                        "Сигнал модели — требуется ручное подтверждение."
                        if legacy_mode
                        else f"Сигнал относится к {book_id}; требуется ручное подтверждение исполнения."
                    ),
                    (
                        f"Правило времени: основной слот {PRIMARY_EXECUTION_SLOT} по Еревану; "
                        f"если пропущен — не догоняем сигнал днём, резервное окно {FALLBACK_EXECUTION_WINDOW}."
                    ),
                ]
            )
            conflicts = details.get("route_conflicts") or payload.get("route_conflicts", {}).get(book_id, [])
            if conflicts:
                lines.extend(_route_conflict_lines_ru(conflicts))
            extra = [event for event in selected.get("confirmed", []) if event is not primary]
            if extra:
                lines.append("Другие подтверждённые кандидаты на выход:")
                for event in extra:
                    lines.append(f"- {_event_line_ru(event)}")
                    price_line = _price_line_ru(payload, event["from_asset"], event["to_asset"])
                    if price_line:
                        lines.append(f"  {price_line}")
            if armed:
                lines.append("Другие ARM / PREWATCH по этой ветке (ещё НЕ подтверждены):")
                for event in armed:
                    lines.append(f"- {_event_line_ru(event)}")
                    price_line = _price_line_ru(payload, event["from_asset"], event["to_asset"])
                    if price_line:
                        lines.append(f"  {price_line}")
            continue

        if armed:
            heading = (
                "⚠️ ARM 15% — ЖДЁМ ПОДТВЕРЖДЕНИЯ"
                if legacy_mode
                else f"⚠️ {book_id} — ARM 15% / PREWATCH"
            )
            lines.append(heading)
            for event in armed:
                lines.append(f"- {_event_line_ru(event)}")
                price_line = _price_line_ru(payload, event["from_asset"], event["to_asset"])
                if price_line:
                    lines.append(f"  {price_line}")
            lines.append("Порог 15% достигнут; ротации пока нет. Ждём разворот от экстремума минимум 3%.")
            continue

        if observations:
            strongest = observations[0]
            if strongest["mode"] == "NONE":
                lines.extend(
                    [
                        (
                            "👀 НАБЛЮДЕНИЕ 10%+"
                            if legacy_mode
                            else f"👀 {book_id} — НАБЛЮДЕНИЕ 10%+"
                        ),
                        (
                            f"Расхождение между {book['held_asset']} и {strongest['to_asset']} составляет "
                            f"{strongest['dislocation'] * 100:.2f}%."
                        ),
                        (
                            f"Возможное направление при дальнейшем подтверждении: "
                            f"{book['held_asset']} -> {strongest['to_asset']}."
                        ),
                        *(
                            [_price_line_ru(payload, book["held_asset"], strongest["to_asset"])]
                            if _price_line_ru(payload, book["held_asset"], strongest["to_asset"])
                            else []
                        ),
                        "Порог наблюдения 10% достигнут. Торгового сигнала пока нет; ARM включается с 15%.",
                    ]
                )
            else:
                lines.extend(
                    [
                        (
                            "⚠️ ARM 15% АКТИВЕН"
                            if legacy_mode
                            else f"⚠️ {book_id} — ARM 15% АКТИВЕН"
                        ),
                        (
                            f"Пара {book['held_asset']}/{strongest['to_asset']}: текущее расхождение "
                            f"{strongest['dislocation'] * 100:.2f}%, максимум после ARM "
                            f"{strongest['max_dislocation'] * 100:.2f}%."
                        ),
                        f"Ожидаем направление {book['held_asset']} -> {strongest['to_asset']} после подтверждения.",
                        *(
                            [_price_line_ru(payload, book["held_asset"], strongest["to_asset"])]
                            if _price_line_ru(payload, book["held_asset"], strongest["to_asset"])
                            else []
                        ),
                        "Ждём разворот от экстремума минимум 3%.",
                    ]
                )

            if len(observations) > 1:
                lines.append("Другие наблюдаемые пары:")
                for event in observations[1:4]:
                    suffix = " (ARM активен)" if event["mode"] != "NONE" else ""
                    lines.append(
                        f"- {book['held_asset']}/{event['to_asset']}: "
                        f"{event['dislocation'] * 100:.2f}%{suffix}"
                    )
                    price_line = _price_line_ru(payload, book["held_asset"], event["to_asset"])
                    if price_line:
                        lines.append(f"  {price_line}")
        elif not legacy_mode:
            lines.append("Новых ARMED/CONFIRMED событий по этой ветке нет.")

    for asset, watched in watch_events.items():
        primary_watch = watched.get("primary_confirmed")
        if primary_watch:
            lines.extend(
                [
                    f"🚨 WATCH {asset} — ВЫХОД ПОДТВЕРЖДЁН ДЛЯ {asset}",
                    _event_line_ru(primary_watch),
                    *(
                        [_price_line_ru(payload, primary_watch["from_asset"], primary_watch["to_asset"])]
                        if _price_line_ru(payload, primary_watch["from_asset"], primary_watch["to_asset"])
                        else []
                    ),
                ]
            )
            if legacy_mode:
                lines.append(
                    f"Это НЕ сигнал для текущего актива {payload['held_asset']}; это независимый sunset-watch."
                )
            else:
                lines.append("Это независимый sunset-watch, не сигнал ни для одной активной BOOK-ветки.")
            lines.append("Только ручная проверка — наблюдение не размещает ордера.")
            extra_watch = [event for event in watched.get("confirmed", []) if event is not primary_watch]
            if extra_watch:
                lines.append(f"Другие подтверждённые кандидаты на выход для {asset}:")
                for event in extra_watch:
                    lines.append(f"- {_event_line_ru(event)}")
                    price_line = _price_line_ru(payload, event["from_asset"], event["to_asset"])
                    if price_line:
                        lines.append(f"  {price_line}")
            armed_watch = watched.get("armed", [])
            if armed_watch:
                lines.append(f"Другие {asset} ARM / PREWATCH (ещё НЕ подтверждены):")
                for event in armed_watch:
                    lines.append(f"- {_event_line_ru(event)}")
                    price_line = _price_line_ru(payload, event["from_asset"], event["to_asset"])
                    if price_line:
                        lines.append(f"  {price_line}")
        elif watched.get("armed"):
            lines.append(f"⚠️ WATCH {asset} — ARM / PREWATCH, ЭТО НЕ СИГНАЛ НА ОБМЕН")
            for event in watched["armed"]:
                lines.append(f"- {_event_line_ru(event)}")
                price_line = _price_line_ru(payload, event["from_asset"], event["to_asset"])
                if price_line:
                    lines.append(f"  {price_line}")
            if legacy_mode:
                lines.append(
                    f"Текущий актив {payload['held_asset']} не меняется. "
                    f"Для {asset} ждём разворот от экстремума минимум 3%."
                )
            else:
                lines.append(
                    f"Активные BOOK-ветки не меняются. Для {asset} ждём разворот от экстремума минимум 3%."
                )

    for event in latest_defensive_events:
        if event["event"] == "DEFENSIVE_ENTER":
            lines.append(
                f"Кандидат на ВХОД В ЗАЩИТНЫЙ РЕЖИМ: ширина рынка "
                f"{event['breadth']}/{len(payload.get('target_assets', []))}; токен с низкой волатильностью "
                f"{event['defensive_asset']}."
            )
        elif event["event"] == "DEFENSIVE_EXIT":
            lines.append(
                f"Кандидат на ВЫХОД ИЗ ЗАЩИТНОГО РЕЖИМА: ширина рынка "
                f"{event['breadth']}/{len(payload.get('target_assets', []))}; возврат по shadow-routing остаётся ручным."
            )

    if payload.get("defensive_overlay_enabled", True):
        status = (
            f"ВКЛ ({defensive['defensive_asset']})"
            if defensive["active"]
            else "ВЫКЛ"
        )
        lines.append(
            f"Защитный режим: {status}; ширина рынка={defensive.get('breadth')}/{len(payload.get('target_assets', []))}"
        )
    else:
        lines.append("Защитный overlay отключён на время миграции.")
    lines.append("БУМАЖНЫЙ/РУЧНОЙ РЕЖИМ — реальные ордера не отправляются.")
    return "\n".join(lines) + "\n"


def build_report_markdown(payload: dict) -> str:
    held_events = payload["held_events"]
    defensive = payload["defensive"]
    lines = [
        "# Relative Rotation Paper Live v1",
        "",
        f"Generated: {payload['generated_at']}",
        f"Source commit: `{payload['source_commit_sha']}`",
        f"Latest closed candle: **{payload['latest_closed_candle']}**",
        f"Current configured held asset: **{payload['held_asset']}**",
        f"Target universe: **{', '.join(payload.get('target_assets', []))}**",
        f"Sunset / exit-only assets: **{', '.join(payload.get('sunset_assets', []))}**",
        f"Persistent watch assets: **{', '.join(payload.get('watch_assets', [])) or '-'}**",
        f"Monitor start: **{payload['monitor_start']}**",
        f"Status: **{payload['status']}**",
        f"Universe version: **{payload.get('universe_version') or '-'}**",
        f"Forward validation start: **{payload.get('forward_validation_start') or '-'}**",
        "",
        "## Live position books",
        "",
    ]

    for book in payload.get("position_books", []):
        quantity = book.get("quantity")
        quantity_text = "-" if quantity is None else f"{float(quantity):g}"
        lines.append(
            f"- **{book['book_id']}**: held **{book['held_asset']}**; quantity **{quantity_text}**; "
            f"tracking start **{book.get('tracking_start') or '-'}**"
        )

    lines.extend(
        [
        "",
        "## Latest per-book events",
        "",
        ]
    )
    for book_id, details in payload.get("book_events", {}).items():
        book = details["book"]
        events = details["events"]
        lines.append(f"### {book_id} — {book['held_asset']}")
        lines.append("")
        if events["confirmed"]:
            lines.append("CONFIRMED:")
            for event in events["confirmed"]:
                lines.append(f"- {_event_line(event)}")
        if events["armed"]:
            lines.append("ARMED / PREWATCH:")
            for event in events["armed"]:
                lines.append(f"- {_event_line(event)}")
        if not events["confirmed"] and not events["armed"]:
            lines.append("- No new latest-candle ARMED/CONFIRMED event.")
        lines.append("")

    lines.extend(
        [
        "## Frozen relative-rotation engine",
        "",
        f"- Monitor union: {', '.join(ASSETS)}",
        f"- Monitor pair graph: {len(ASSETS)} assets / {len(ASSETS) * (len(ASSETS) - 1) // 2} undirected pairs",
        f"- Active TARGET graph: {len(payload.get('target_assets', []))} assets / {len(payload.get('target_assets', [])) * (len(payload.get('target_assets', [])) - 1) // 2} undirected pairs",
        "- Migration guard: every actionable destination must belong to TARGET; sunset re-entry is blocked",
        f"- Rolling median: {LOOKBACK} closed daily candles",
        f"- ARM threshold: {ARM_THRESHOLD:.0%}",
        f"- Reversal confirmation: {REVERSAL:.0%}",
        "- Router conflict rule: strongest confirmed max dislocation",
        "- Execution: MANUAL ONLY; monitor never places an order",
        "",
        "## Legacy BOOK_1 compatibility view",
        "",
    ]
    )

    if held_events["confirmed"]:
        lines.append("### CONFIRMED")
        lines.append("")
        for event in held_events["confirmed"]:
            lines.append(f"- {_event_line(event)}")
    elif held_events["armed"]:
        lines.append("### ARMED / PREWATCH")
        lines.append("")
        for event in held_events["armed"]:
            lines.append(f"- {_event_line(event)}")
    else:
        lines.append("No new ARMED or CONFIRMED event from the configured held asset on the latest closed candle.")

    watch_events = payload.get("watch_events", {})
    if watch_events:
        lines.extend(["", "## Persistent watch-asset events", ""])
        for asset, watched in watch_events.items():
            if watched["confirmed"]:
                lines.append(f"### {asset} — CONFIRMED")
                lines.append("")
                for event in watched["confirmed"]:
                    lines.append(f"- {_event_line(event)}")
            elif watched["armed"]:
                lines.append(f"### {asset} — ARMED / PREWATCH")
                lines.append("")
                for event in watched["armed"]:
                    lines.append(f"- {_event_line(event)}")
            else:
                lines.append(f"- {asset}: no new latest-candle ARMED/CONFIRMED outbound event.")

    lines.extend(
        [
            "",
            "## Active pair states from held asset",
            "",
            "| Pair | Mode | Prospective route | Deviation | Max dislocation | Reversal from extreme | Armed at |",
            "|---|---|---|---:|---:|---:|---|",
        ]
    )
    for row in payload["held_pair_states"]:
        route = (
            f"{row['from_asset']} -> {row['to_asset']}"
            if row.get("from_asset") and row.get("to_asset")
            else "-"
        )
        lines.append(
            f"| {row['pair']} | {row['mode']} | {route} | {_pct(row.get('deviation'))} | "
            f"{_pct(row.get('max_dislocation'))} | {_pct(row.get('reversal_from_extreme'))} | "
            f"{row.get('armed_at') or '-'} |"
        )

    lines.extend(
        [
            "",
            "## Defensive low-vol research overlay",
            "",
            f"- Status: **{defensive['status']}**",
            f"- Current mode: **{'ON' if defensive['active'] else 'OFF'}**",
            f"- Defensive asset: **{defensive.get('defensive_asset') or '-'}**",
            f"- Current breadth above SMA{DEFENSIVE_SMA_LOOKBACK}: **{defensive.get('breadth')}/{len(payload.get('target_assets', []))}**",
            f"- Enter: breadth <= {DEFENSIVE_ENTER_BREADTH} for {DEFENSIVE_CONFIRM_DAYS} closed days",
            f"- Exit: breadth >= {DEFENSIVE_EXIT_BREADTH} for {DEFENSIVE_CONFIRM_DAYS} closed days",
            f"- Defensive token: lowest {DEFENSIVE_VOL_LOOKBACK}-day realized close-to-close volatility at entry",
            "- This overlay is reported as research evidence only; it does not override manual approval.",
            "",
            "## Notification policy",
            "",
            "Telegram is requested for a new latest-candle ARMED/CONFIRMED event from any configured live book "
            "only when the destination belongs to TARGET, "
            "a new TARGET-bound ARMED/CONFIRMED outbound event from any persistent sunset watch asset, "
            "a latest-candle defensive ENTER/EXIT event, or an explicit force-notify run.",
            "",
            "## Safety boundary",
            "",
            "No broker/exchange API keys are used. No order placement code exists in this monitor. "
            "A real swap must be manually approved and executed by Vahram, then that book's held asset and quantity must be updated.",
            "",
        ]
    )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Daily paper-live monitor for the configured relative-rotation graph.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--history-start", default=DEFAULT_HISTORY_START)
    parser.add_argument("--cutoff")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--force-notify", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = _read_config(args.config)
    cutoff = _utc(args.cutoff) if args.cutoff else _cutoff()
    history_start = _utc(args.history_start)
    if cutoff <= history_start:
        raise ValueError("cutoff must be after history-start")

    panel, data_metadata = download_panel(start=history_start, cutoff=cutoff)
    latest = pd.Timestamp(panel.iloc[-1]["timestamp"])
    latest_row = panel.iloc[-1]
    latest_close_prices_usdt = {
        asset: float(latest_row[f"{asset}_close"])
        for asset in ASSETS
    }
    events, pair_states = build_pair_monitor(panel)

    book_events = {}
    route_conflicts = {}
    for book in config["position_books"]:
        selected = choose_held_events(
            events,
            held_asset=book["held_asset"],
            latest_date=latest,
            allowed_to_assets=config["target_assets"],
        )
        baseline_primary = selected.get("primary_confirmed")
        conflicts = find_route_conflicts(
            events,
            pair_states,
            primary_confirmed=baseline_primary,
            latest_date=latest,
            allowed_to_assets=config["target_assets"],
        )
        effective_primary, route_override = choose_destination_dominance_override(
            baseline_primary,
            conflicts,
        )
        selected["baseline_primary_confirmed"] = baseline_primary
        selected["primary_confirmed"] = effective_primary
        selected["route_override"] = route_override
        route_conflicts[book["book_id"]] = conflicts
        book_events[book["book_id"]] = {
            "book": _book_payload(book),
            "events": selected,
            "route_conflicts": conflicts,
            "route_override": route_override,
        }

    primary_book_id = config["position_books"][0]["book_id"]
    held_events = book_events[primary_book_id]["events"]

    held_assets = {book["held_asset"] for book in config["position_books"]}
    independent_watch_assets = [
        asset for asset in config["watch_assets"] if asset not in held_assets
    ]
    watch_events = {
        asset: choose_held_events(
            events,
            held_asset=asset,
            latest_date=latest,
            allowed_to_assets=config["target_assets"],
        )
        for asset in independent_watch_assets
    }
    if config["defensive_overlay_enabled"]:
        defensive_events, defensive, defensive_diagnostics = evaluate_defensive_mode(
            panel,
            assets=config["target_assets"],
        )
    else:
        defensive_events = []
        defensive = {
            "status": "DISABLED_DURING_MIGRATION",
            "active": False,
            "defensive_asset": None,
            "breadth": None,
            "low_streak": 0,
            "high_streak": 0,
        }
        defensive_diagnostics = pd.DataFrame()

    latest_iso = latest.isoformat()
    latest_defensive_events = [event for event in defensive_events if event["date"] == latest_iso]
    target_set = set(config["target_assets"])
    book_pair_states = {}
    for book in config["position_books"]:
        rows = []
        for row in pair_states:
            parts = row["pair"].split("/")
            if book["held_asset"] not in parts:
                continue
            other = parts[1] if parts[0] == book["held_asset"] else parts[0]
            if other in target_set:
                rows.append(row)
        book_pair_states[book["book_id"]] = rows

    held_pair_states = book_pair_states[primary_book_id]

    notification_candidates = build_notification_candidates(
        events,
        position_books=config["position_books"],
        target_assets=config["target_assets"],
        latest=latest,
        monitor_start=config["monitor_start"],
    )
    for item in notification_candidates:
        item["route_conflicts"] = []
        item["route_override"] = None

    for book_id, details in book_events.items():
        selected = details["events"]
        baseline = selected.get("baseline_primary_confirmed")
        effective = selected.get("primary_confirmed")
        override = selected.get("route_override")
        if baseline is None or effective is None:
            continue
        for item in notification_candidates:
            event = item.get("event", {})
            if str(item.get("book_id") or "") != str(book_id):
                continue
            if event.get("event") != "CONFIRMED":
                continue
            if str(event.get("date") or "") != str(baseline.get("date") or ""):
                continue
            if str(event.get("pair") or "") != str(baseline.get("pair") or ""):
                continue
            item["route_conflicts"] = details.get("route_conflicts", [])
            item["route_override"] = override
            if override:
                item["baseline_event"] = dict(event)
                item["event"] = dict(effective)
                item["event_id"] = _notification_event_id(book_id, item["event"])
            break

    # If the override target was also independently CONFIRMED on the same candle,
    # keep one canonical notification candidate and prefer the explicit override.
    deduped_candidates = {}
    for item in notification_candidates:
        key = str(item.get("event_id") or "")
        existing = deduped_candidates.get(key)
        if existing is None or (item.get("route_override") and not existing.get("route_override")):
            deduped_candidates[key] = item
    notification_candidates = list(deduped_candidates.values())
    notification_candidates.sort(
        key=lambda item: (
            str(item.get("event", {}).get("date") or ""),
            str(item.get("book_id") or ""),
            str(item.get("event", {}).get("event") or ""),
            str(item.get("event", {}).get("to_asset") or ""),
        )
    )

    after_monitor_start = latest >= config["monitor_start"]
    book_signal = any(
        details["events"]["armed"] or details["events"]["confirmed"]
        for details in book_events.values()
    )
    watch_signal = any(
        watched["armed"] or watched["confirmed"]
        for watched in watch_events.values()
    )
    new_signal = bool(book_signal or watch_signal or latest_defensive_events)
    replay_signal = bool(notification_candidates)
    should_notify = bool(
        args.force_notify
        or (after_monitor_start and (new_signal or replay_signal))
    )

    generated_at = pd.Timestamp.now(tz="UTC")
    run_id = generated_at.strftime("%Y%m%dT%H%M%SZ")
    run_dir = args.output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "schema_version": 1,
        "strategy": config.get("strategy", "RELATIVE_ROTATION_TARGET_U10_FORWARD_V1"),
        "status": "FROZEN_U10_FORWARD_MULTIBOOK_PAPER_LIVE / DESTINATION_DOMINANCE_AUTO_ROUTE / MANUAL_EXECUTION_ONLY",
        "universe_version": config.get("universe_version", ""),
        "forward_validation_enabled": config.get("forward_validation_enabled", False),
        "forward_validation_start": (
            config["forward_validation_start"].isoformat()
            if config.get("forward_validation_start") is not None
            else None
        ),
        "generated_at": generated_at.isoformat(),
        "source_commit_sha": _source_commit(),
        "held_asset": config["held_asset"],
        "position_books": [_book_payload(book) for book in config["position_books"]],
        "book_events": book_events,
        "book_pair_states": book_pair_states,
        "migration_mode": config["migration_mode"],
        "defensive_overlay_enabled": config["defensive_overlay_enabled"],
        "target_assets": list(config["target_assets"]),
        "sunset_assets": list(config["sunset_assets"]),
        "watch_assets": config["watch_assets"],
        "monitor_start": config["monitor_start"].isoformat(),
        "latest_closed_candle": latest_iso,
        "latest_close_prices_usdt": latest_close_prices_usdt,
        "force_notify": bool(args.force_notify),
        "should_notify": should_notify,
        "notification_replay_days": NOTIFICATION_REPLAY_DAYS,
        "execution_timing_policy": {
            "timezone": EXECUTION_TIMEZONE,
            "primary_slot": PRIMARY_EXECUTION_SLOT,
            "fallback_window": FALLBACK_EXECUTION_WINDOW,
            "midday_chase": False,
            "manual_execution_only": True,
        },
        "notification_candidates": notification_candidates,
        "route_conflicts": route_conflicts,
        "notification_reason": {
            "after_monitor_start": after_monitor_start,
            "held_armed": len(held_events["armed"]),
            "held_confirmed": len(held_events["confirmed"]),
            "book_events": {
                book_id: {
                    "held_asset": details["book"]["held_asset"],
                    "armed": len(details["events"]["armed"]),
                    "confirmed": len(details["events"]["confirmed"]),
                }
                for book_id, details in book_events.items()
            },
            "watch_events": {
                asset: {
                    "armed": len(watched["armed"]),
                    "confirmed": len(watched["confirmed"]),
                }
                for asset, watched in watch_events.items()
            },
            "defensive_events": len(latest_defensive_events),
            "replay_candidates": len(notification_candidates),
            "force_notify": bool(args.force_notify),
        },
        "frozen_parameters": {
            "lookback": LOOKBACK,
            "arm_threshold": ARM_THRESHOLD,
            "reversal": REVERSAL,
            "assets": list(ASSETS),
            "pair_count": len(ASSETS) * (len(ASSETS) - 1) // 2,
            "monitor_assets": list(ASSETS),
            "monitor_pair_count": len(ASSETS) * (len(ASSETS) - 1) // 2,
            "target_assets": list(config["target_assets"]),
            "target_pair_count": len(config["target_assets"]) * (len(config["target_assets"]) - 1) // 2,
            "sunset_assets": list(config["sunset_assets"]),
            "destination_guard": "TARGET_ONLY",
            "route_conflict_guard": "DESTINATION_DOMINANCE_MIN_1_5X_V2 / AUTO_ROUTE_OVERRIDE / FORWARD_WATCH_REQUIRED",
            "execution_timing_policy": "04:20 YEREVAN PRIMARY / 23:00-24:00 YEREVAN FALLBACK / NO MIDDAY CHASE",
        },
        "held_events": held_events,
        "book_events": book_events,
        "watch_events": watch_events,
        "held_pair_states": held_pair_states,
        "book_pair_states": book_pair_states,
        "latest_pair_states": pair_states,
        "defensive": defensive,
        "latest_defensive_events": latest_defensive_events,
        "data": data_metadata,
    }

    payload["telegram_text_ru"] = build_notification_ru(payload)

    _write_json(run_dir / "report.json", payload)
    (run_dir / "report.md").write_text(build_report_markdown(payload), encoding="utf-8")
    (run_dir / "notification.txt").write_text(build_notification(payload), encoding="utf-8")
    defensive_diagnostics.to_csv(run_dir / "defensive_diagnostics.csv", index=False)
    pd.DataFrame(events).to_csv(run_dir / "pair_events.csv", index=False)
    pd.DataFrame(pair_states).to_csv(run_dir / "pair_states.csv", index=False)

    print(f"run_dir={run_dir}")
    print(f"latest_closed_candle={latest_iso}")
    print(f"held_asset={config['held_asset']}")
    for book_id, details in book_events.items():
        book = details["book"]
        print(
            f"book_{book_id}_held={book['held_asset']};"
            f"quantity={book.get('quantity')};"
            f"armed={len(details['events']['armed'])};"
            f"confirmed={len(details['events']['confirmed'])}"
        )
    print(f"target_assets={','.join(config['target_assets'])}")
    print(f"sunset_assets={','.join(config['sunset_assets'])}")
    print(f"held_armed={len(held_events['armed'])}")
    print(f"held_confirmed={len(held_events['confirmed'])}")
    for asset, watched in watch_events.items():
        print(f"watch_{asset}_armed={len(watched['armed'])}")
        print(f"watch_{asset}_confirmed={len(watched['confirmed'])}")
    print(f"defensive_active={defensive['active']}")
    print(f"defensive_breadth={defensive.get('breadth')}")
    print(f"notification_candidates={len(notification_candidates)}")
    for book_id, conflicts in route_conflicts.items():
        print(f"book_{book_id}_route_conflicts={len(conflicts)}")
    print(f"should_notify={should_notify}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
