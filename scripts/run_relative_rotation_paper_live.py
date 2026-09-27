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
    build_pair_monitor,
    choose_held_events,
    evaluate_defensive_mode,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = REPO_ROOT / "config" / "relative_rotation_paper_live_v1.json"
DEFAULT_HISTORY_START = "2023-05-05T00:00:00Z"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "paper_artifacts" / "relative_rotation_paper_live_v1"
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
    monitor_start = _utc(payload.get("monitor_start", "2026-09-27T00:00:00Z"))
    watch_assets = []
    for value in payload.get("watch_assets", []):
        asset = str(value).upper()
        if asset not in ASSETS:
            raise ValueError(f"watch_asset must be one of {ASSETS}; got {asset!r}")
        if asset not in watch_assets:
            watch_assets.append(asset)
    return {
        **payload,
        "held_asset": held_asset,
        "watch_assets": watch_assets,
        "monitor_start": monitor_start,
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


def _event_line(event: dict) -> str:
    return (
        f"{event['from_asset']} -> {event['to_asset']} "
        f"({event['pair']}; dislocation {_pct(event.get('max_dislocation'))}; "
        f"reversal {_pct(event.get('reversal_from_extreme'))})"
    )


def build_notification(payload: dict) -> str:
    held = payload["held_asset"]
    latest = payload["latest_closed_candle"]
    selected = payload["held_events"]
    watch_events = payload.get("watch_events", {})
    defensive = payload["defensive"]
    latest_defensive_events = payload["latest_defensive_events"]

    lines = [
        "Relative Rotation Paper Live v1",
        f"Closed candle: {latest}",
        f"Held asset: {held}",
    ]

    primary = selected.get("primary_confirmed")
    if primary:
        lines.extend(
            [
                "ROTATION CONFIRMED",
                _event_line(primary),
                "Historical model action only — manual approval required.",
            ]
        )
        extra = [event for event in selected.get("confirmed", []) if event is not primary]
        if extra:
            lines.append("Other confirmed outbound candidates:")
            lines.extend(f"- {_event_line(event)}" for event in extra)
    elif selected.get("armed"):
        lines.append("ARMED / PREWATCH")
        lines.extend(f"- {_event_line(event)}" for event in selected["armed"])
        lines.append("15% threshold reached; no rotation until 3% reversal confirms.")
    elif payload.get("force_notify"):
        lines.append("Initialization snapshot — no confirmed rotation from held asset on this candle.")

    for asset, watched in watch_events.items():
        primary_watch = watched.get("primary_confirmed")
        if primary_watch:
            lines.extend(
                [
                    f"{asset} WATCH — EXIT CONFIRMED",
                    _event_line(primary_watch),
                    "Manual review only — this watch does not place an order.",
                ]
            )
            extra_watch = [event for event in watched.get("confirmed", []) if event is not primary_watch]
            if extra_watch:
                lines.append(f"Other {asset} confirmed outbound candidates:")
                lines.extend(f"- {_event_line(event)}" for event in extra_watch)
        elif watched.get("armed"):
            lines.append(f"{asset} WATCH — ARMED / PREWATCH")
            lines.extend(f"- {_event_line(event)}" for event in watched["armed"])
            lines.append("15% threshold reached; wait for 3% reversal confirmation.")

    if latest_defensive_events:
        for event in latest_defensive_events:
            if event["event"] == "DEFENSIVE_ENTER":
                lines.append(
                    f"DEFENSIVE ENTER candidate: breadth {event['breadth']}/{len(ASSETS)}; "
                    f"low-vol token {event['defensive_asset']}."
                )
            elif event["event"] == "DEFENSIVE_EXIT":
                lines.append(
                    f"DEFENSIVE EXIT candidate: breadth {event['breadth']}/{len(ASSETS)}; "
                    "return-to-shadow routing remains manual."
                )

    lines.append(
        "Defensive status: "
        + (f"ON ({defensive['defensive_asset']})" if defensive["active"] else "OFF")
        + f"; breadth={defensive.get('breadth')}/{len(ASSETS)}"
    )
    lines.append("PAPER/MANUAL ONLY — no exchange orders, no API trading keys.")
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
        f"Persistent watch assets: **{', '.join(payload.get('watch_assets', [])) or '-'}**",
        f"Monitor start: **{payload['monitor_start']}**",
        f"Status: **{payload['status']}**",
        "",
        "## Frozen relative-rotation engine",
        "",
        f"- Assets: {', '.join(ASSETS)}",
        f"- Pair graph: {len(ASSETS)} assets / {len(ASSETS) * (len(ASSETS) - 1) // 2} undirected pairs",
        f"- Rolling median: {LOOKBACK} closed daily candles",
        f"- ARM threshold: {ARM_THRESHOLD:.0%}",
        f"- Reversal confirmation: {REVERSAL:.0%}",
        "- Router conflict rule: strongest confirmed max dislocation",
        "- Execution: MANUAL ONLY; monitor never places an order",
        "",
        "## Latest held-asset events",
        "",
    ]

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
            f"- Current breadth above SMA{DEFENSIVE_SMA_LOOKBACK}: **{defensive.get('breadth')}/{len(ASSETS)}**",
            f"- Enter: breadth <= {DEFENSIVE_ENTER_BREADTH} for {DEFENSIVE_CONFIRM_DAYS} closed days",
            f"- Exit: breadth >= {DEFENSIVE_EXIT_BREADTH} for {DEFENSIVE_CONFIRM_DAYS} closed days",
            f"- Defensive token: lowest {DEFENSIVE_VOL_LOOKBACK}-day realized close-to-close volatility at entry",
            "- This overlay is reported as research evidence only; it does not override manual approval.",
            "",
            "## Notification policy",
            "",
            "Telegram is requested for a new latest-candle ARMED/CONFIRMED event from the configured held asset, "
            "a new ARMED/CONFIRMED outbound event from any persistent watch asset (ATOM is configured), "
            "a latest-candle defensive ENTER/EXIT event, or an explicit force-notify run.",
            "",
            "## Safety boundary",
            "",
            "No broker/exchange API keys are used. No order placement code exists in this monitor. "
            "A real swap must be manually approved and executed by Vahram, then the configured held asset must be updated.",
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
    events, pair_states = build_pair_monitor(panel)
    held_events = choose_held_events(events, held_asset=config["held_asset"], latest_date=latest)
    independent_watch_assets = [asset for asset in config["watch_assets"] if asset != config["held_asset"]]
    watch_events = {
        asset: choose_held_events(events, held_asset=asset, latest_date=latest)
        for asset in independent_watch_assets
    }
    defensive_events, defensive, defensive_diagnostics = evaluate_defensive_mode(panel)

    latest_iso = latest.isoformat()
    latest_defensive_events = [event for event in defensive_events if event["date"] == latest_iso]
    held_pair_states = [
        row
        for row in pair_states
        if row.get("from_asset") == config["held_asset"]
        or config["held_asset"] in row["pair"].split("/")
    ]

    after_monitor_start = latest >= config["monitor_start"]
    held_signal = bool(held_events["armed"] or held_events["confirmed"])
    watch_signal = any(
        watched["armed"] or watched["confirmed"]
        for watched in watch_events.values()
    )
    new_signal = bool(held_signal or watch_signal or latest_defensive_events)
    should_notify = bool(args.force_notify or (after_monitor_start and new_signal))

    generated_at = pd.Timestamp.now(tz="UTC")
    run_id = generated_at.strftime("%Y%m%dT%H%M%SZ")
    run_dir = args.output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "schema_version": 1,
        "strategy": config.get("strategy", "RELATIVE_ROTATION_GRAPH_10_V1"),
        "status": "PAPER_LIVE_MONITOR / MANUAL_EXECUTION_ONLY",
        "generated_at": generated_at.isoformat(),
        "source_commit_sha": _source_commit(),
        "held_asset": config["held_asset"],
        "watch_assets": config["watch_assets"],
        "monitor_start": config["monitor_start"].isoformat(),
        "latest_closed_candle": latest_iso,
        "force_notify": bool(args.force_notify),
        "should_notify": should_notify,
        "notification_reason": {
            "after_monitor_start": after_monitor_start,
            "held_armed": len(held_events["armed"]),
            "held_confirmed": len(held_events["confirmed"]),
            "watch_events": {
                asset: {
                    "armed": len(watched["armed"]),
                    "confirmed": len(watched["confirmed"]),
                }
                for asset, watched in watch_events.items()
            },
            "defensive_events": len(latest_defensive_events),
            "force_notify": bool(args.force_notify),
        },
        "frozen_parameters": {
            "lookback": LOOKBACK,
            "arm_threshold": ARM_THRESHOLD,
            "reversal": REVERSAL,
            "assets": list(ASSETS),
            "pair_count": len(ASSETS) * (len(ASSETS) - 1) // 2,
        },
        "held_events": held_events,
        "watch_events": watch_events,
        "held_pair_states": held_pair_states,
        "latest_pair_states": pair_states,
        "defensive": defensive,
        "latest_defensive_events": latest_defensive_events,
        "data": data_metadata,
    }

    _write_json(run_dir / "report.json", payload)
    (run_dir / "report.md").write_text(build_report_markdown(payload), encoding="utf-8")
    (run_dir / "notification.txt").write_text(build_notification(payload), encoding="utf-8")
    defensive_diagnostics.to_csv(run_dir / "defensive_diagnostics.csv", index=False)
    pd.DataFrame(events).to_csv(run_dir / "pair_events.csv", index=False)
    pd.DataFrame(pair_states).to_csv(run_dir / "pair_states.csv", index=False)

    print(f"run_dir={run_dir}")
    print(f"latest_closed_candle={latest_iso}")
    print(f"held_asset={config['held_asset']}")
    print(f"held_armed={len(held_events['armed'])}")
    print(f"held_confirmed={len(held_events['confirmed'])}")
    for asset, watched in watch_events.items():
        print(f"watch_{asset}_armed={len(watched['armed'])}")
        print(f"watch_{asset}_confirmed={len(watched['confirmed'])}")
    print(f"defensive_active={defensive['active']}")
    print(f"defensive_breadth={defensive.get('breadth')}")
    print(f"should_notify={should_notify}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
