from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
from pathlib import Path
from typing import Iterable

import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.link_level_grid.oss_forward_candidate import (
    OssMidCandidateConfig,
    run_oss_mid_candidate,
)
from strategies.crypto.link_level_grid.strategy import (
    GridBacktestConfig,
    RollingRangePolicy,
    run_grid_backtest,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SYMBOLS = ("LINKUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "BTCUSDT")
DEFAULT_PAPER_START = "2026-09-26T00:00:00Z"

PROFILES = {
    "CONTROL_BASE": {
        "micro_exit_sublevels": 1,
        "mid_recovery_sublevels": 10,
        "layer": "BOTH",
        "engine": "CANONICAL",
    },
    "CANDIDATE_WIDE": {
        "micro_exit_sublevels": 6,
        "mid_recovery_sublevels": 18,
        "layer": "BOTH",
        "engine": "CANONICAL",
    },
    "MICRO_ONLY_WIDE": {
        "micro_exit_sublevels": 6,
        "mid_recovery_sublevels": 18,
        "layer": "MICRO",
        "engine": "CANONICAL",
    },
    "MID_ONLY_WIDE": {
        "micro_exit_sublevels": 6,
        "mid_recovery_sublevels": 18,
        "layer": "MID",
        "engine": "CANONICAL",
    },
    "MID_OSS_ATR50_TRAIL7": {
        "micro_exit_sublevels": 6,
        "mid_recovery_sublevels": 18,
        "layer": "MID",
        "engine": "OSS_FORWARD_CANDIDATE",
        "atr_regrid_threshold": 0.50,
        "regrid_cooldown_candles": 60,
        "exit_retracement": 0.07,
    },
}


def _utc(value) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _cutoff(now=None) -> pd.Timestamp:
    current = pd.Timestamp.now(tz="UTC") if now is None else _utc(now)
    return current.floor("D")


def _source_commit() -> str:
    env_sha = os.environ.get("SOURCE_COMMIT_SHA")
    if env_sha:
        return env_sha
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO_ROOT,
        text=True,
    ).strip()


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, default=str, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def _pct(value: float) -> str:
    return f"{value * 100:+.2f}%"


def _money(value: float) -> str:
    return f"{value:,.2f}"


def _safe_concat(frames: Iterable[pd.DataFrame]) -> pd.DataFrame:
    usable = [frame for frame in frames if frame is not None and not frame.empty]
    return pd.concat(usable, ignore_index=True) if usable else pd.DataFrame()


def _profile_layer(profile: str) -> str:
    return str(PROFILES[profile]["layer"])


def _profile_engine(profile: str) -> str:
    return str(PROFILES[profile].get("engine", "CANONICAL"))


def _scale_single_layer_frame(profile: str, frame: pd.DataFrame) -> pd.DataFrame:
    """Project one independent layer to the same 2,000-unit normalized capital."""
    if frame.empty:
        return frame.copy()

    layer = _profile_layer(profile)
    if layer == "BOTH":
        return frame.copy()

    scoped = frame[frame["layer"] == layer].copy()
    for column in (
        "units",
        "cash_value",
        "invested_cash",
        "proceeds",
        "runner_units",
        "realized_profit",
        "reinvested_profit",
        "reserved_profit",
        "next_slot_cash",
    ):
        if column in scoped.columns:
            scoped[column] = scoped[column].astype(float) * 2.0
    return scoped


def _single_layer_max_drawdown(
    equity_curve: pd.DataFrame,
    *,
    column: str,
    initial_capital: float,
) -> float:
    peak = float(initial_capital)
    max_drawdown = 0.0
    for value in equity_curve[column].astype(float):
        peak = max(peak, float(value))
        drawdown = 1.0 - (float(value) / peak)
        max_drawdown = max(max_drawdown, drawdown)
    return max_drawdown


def _candidate_metrics(result) -> dict:
    summary = result.summary
    return {
        "equity": float(summary["final_equity"]),
        "return": float(summary["total_return"]),
        "max_drawdown": float(summary["max_drawdown"]),
        "open_micro_lots": 0,
        "open_mid_lots": int(summary["open_mid_lots_end"]),
        "closed_trade_count": int(summary["closed_trade_count"]),
    }


def _profile_metrics(profile: str, result) -> dict:
    layer = _profile_layer(profile)
    summary = result.summary
    if layer == "BOTH":
        return {
            "equity": float(summary["total_final_equity"]),
            "return": float(summary["total_return"]),
            "max_drawdown": float(summary["max_drawdown"]),
            "open_micro_lots": int(summary["open_micro_lots_end"]),
            "open_mid_lots": int(summary["open_mid_lots_end"]),
            "closed_trade_count": int(summary["closed_trade_count"]),
        }

    prefix = layer.lower()
    layer_initial = float(summary[f"{prefix}_initial_capital"])
    layer_equity = float(summary[f"{prefix}_final_equity"])
    layer_trades = result.trades
    if not layer_trades.empty:
        layer_trades = layer_trades[layer_trades["layer"] == layer]

    return {
        "equity": layer_equity * 2.0,
        "return": float(summary[f"{prefix}_total_return"]),
        "max_drawdown": _single_layer_max_drawdown(
            result.equity_curve,
            column=f"{prefix}_equity",
            initial_capital=layer_initial,
        ),
        "open_micro_lots": int(summary["open_micro_lots_end"]) if layer == "MICRO" else 0,
        "open_mid_lots": int(summary["open_mid_lots_end"]) if layer == "MID" else 0,
        "closed_trade_count": len(layer_trades),
    }


def _config(profile: str) -> GridBacktestConfig:
    params = PROFILES[profile]
    return GridBacktestConfig(
        micro_capital=1000.0,
        mid_capital=1000.0,
        allocation_preset="linear_depth_reserved",
        micro_allocation_power=1.0,
        mid_allocation_power=1.0,
        micro_exit_sublevels=params["micro_exit_sublevels"],
        mid_recovery_sublevels=params["mid_recovery_sublevels"],
        mid_target_scale=1.0,
        profit_reinvest_fraction=1.0,
        runner_fraction=0.0,
        fee_bps=10.0,
        slippage_bps=5.0,
        rolling_range=RollingRangePolicy(
            lookback_candles=1095,
            min_history_candles=1095,
            refresh_candles=30,
        ),
        ten_sublevel_from_main=7,
        liquidate_at_end=False,
    )


def _initial_range(candles: pd.DataFrame, paper_start: pd.Timestamp) -> tuple[float, float]:
    frame = candles.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    history = frame[frame["timestamp"] < paper_start].tail(1095)
    if len(history) < 1095:
        raise ValueError(f"Need 1095 prehistory candles, got {len(history)}")
    return float(history["high"].max()), float(history["low"].min())


def _build_report_markdown(payload: dict) -> str:
    lines = [
        "# Grid Paper Live v1",
        "",
        f"Generated: {payload['generated_at']}",
        f"Paper start: {payload['paper_start']}",
        f"Latest closed candle: {payload.get('latest_closed_candle') or 'not available yet'}",
        f"Completed paper candles: **{payload['completed_paper_candles']}**",
        f"Status: **{payload['status']}**",
        "",
    ]

    if payload["completed_paper_candles"] == 0:
        lines.extend(
            [
                "No full paper-trading candle has closed since launch.",
                "",
                "The system is initialized from the preceding 1095 daily candles and will begin evaluating the first complete paper day after it closes.",
                "",
            ]
        )
    else:
        lines.extend(
            [
                "## Portfolio comparison",
                "",
                "| Profile | Equity | Return | Today BUY | Today SELL |",
                "|---|---:|---:|---:|---:|",
            ]
        )
        for row in payload["portfolio"]:
            lines.append(
                f"| {row['profile']} | {_money(row['equity'])} | "
                f"{_pct(row['return'])} | {row['today_buys']} | {row['today_sells']} |"
            )
        lines.extend(["", "## Per asset", ""])
        lines.append(
            "| Profile | Symbol | Equity | Return | Max DD | Open Micro | Open Mid | Today events |"
        )
        lines.append("|---|---|---:|---:|---:|---:|---:|---:|")
        for row in payload["rows"]:
            lines.append(
                f"| {row['profile']} | {row['symbol']} | {_money(row['equity'])} | "
                f"{_pct(row['return'])} | {_pct(row['max_drawdown'])} | "
                f"{row['open_micro_lots']} | {row['open_mid_lots']} | {row['today_events']} |"
            )

    lines.extend(
        [
            "",
            "## Current H/L snapshot",
            "",
            "| Symbol | H | L |",
            "|---|---:|---:|",
        ]
    )
    for symbol, anchors in payload["initial_ranges"].items():
        lines.append(f"| {symbol} | {anchors['high']:.8f} | {anchors['low']:.8f} |")

    if payload.get("milestone"):
        lines.extend(["", f"## Milestone: {payload['milestone']}", ""])
        lines.append("This is a forward paper observation milestone, not a live-money approval.")

    lines.extend(
        [
            "",
            "## Assumptions",
            "",
            "- Daily Binance Spot candles only.",
            "- First H/L uses the preceding 1095 closed daily candles.",
            "- Canonical profiles retain the 30-candle H/L refresh research assumption.",
            "- MID_OSS_ATR50_TRAIL7 keeps a 1095-candle causal H/L but refreshes only after a 60-candle cooldown when ATR14 shifts >50% (or price escapes the active range).",
            "- MID_OSS_ATR50_TRAIL7 arms a trailing exit after the normal MID target is reached and sells after a later 7% retracement from the post-target peak.",
            "- Fees: 10 bps; slippage: 5 bps.",
            "- 100% positive-profit reinvestment; no permanent runner.",
            "- No broker keys, no exchange orders, no real money.",
            "",
        ]
    )
    return "\n".join(lines) + "\n"


def _notification_text(payload: dict) -> str:
    latest = payload.get("latest_closed_candle") or "waiting"
    lines = [
        "Grid Paper Live v1",
        f"Closed candle: {latest}",
        f"Paper days: {payload['completed_paper_candles']}",
    ]

    active_rows = [
        row for row in payload.get("rows", [])
        if int(row.get("today_events", 0)) > 0
    ]
    if active_rows:
        lines.append("Signals:")
        for row in active_rows:
            lines.append(
                f"{row['profile']} {row['symbol']}: "
                f"BUY {row['today_buys']}, SELL {row['today_sells']}"
            )

    active_profiles = {row["profile"] for row in active_rows}
    if active_profiles or payload.get("milestone"):
        lines.append("Profile snapshot:")
        for row in payload.get("portfolio", []):
            if payload.get("milestone") or row["profile"] in active_profiles:
                lines.append(
                    f"{row['profile']}: {_pct(row['return'])}, "
                    f"BUY {row['today_buys']}, SELL {row['today_sells']}"
                )

    if payload.get("milestone"):
        lines.append(f"Milestone: {payload['milestone']}")
    lines.append("PAPER ONLY — no real orders.")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Forward paper replay for the level-grid strategy.")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--paper-start", default=DEFAULT_PAPER_START)
    parser.add_argument("--profiles", default=",".join(PROFILES))
    parser.add_argument("--cutoff")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=REPO_ROOT / "paper_artifacts" / "grid_paper_live_v1",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    symbols = tuple(item.strip().upper() for item in args.symbols.split(",") if item.strip())
    profiles = tuple(item.strip().upper() for item in args.profiles.split(",") if item.strip())
    unknown = set(profiles).difference(PROFILES)
    if unknown:
        raise ValueError(f"Unknown profiles: {sorted(unknown)}")

    paper_start = _utc(args.paper_start)
    cutoff = _utc(args.cutoff) if args.cutoff else _cutoff()
    if cutoff <= paper_start - pd.DateOffset(years=3):
        raise ValueError("cutoff is too early for required prehistory")

    source_commit_sha = _source_commit()
    client = BinanceSpotRestClient()
    start = paper_start - pd.DateOffset(years=3, days=7)

    rows: list[dict] = []
    event_frames: list[pd.DataFrame] = []
    trade_frames: list[pd.DataFrame] = []
    initial_ranges: dict[str, dict] = {}
    completed_counts: list[int] = []
    latest_closed: pd.Timestamp | None = None

    for symbol in symbols:
        download = download_historical_dataset(
            client,
            symbol=symbol,
            start=start,
            end=cutoff,
            timeframe="1D",
            as_of=cutoff,
        )
        if download.dataset is None:
            raise RuntimeError(f"{symbol}: download failed: {download.metadata.status}")
        if download.dataset.quality.has_critical_issues:
            raise RuntimeError(f"{symbol}: critical data quality: {download.dataset.quality}")

        candles = download.dataset.candles.copy()
        candles["timestamp"] = pd.to_datetime(candles["timestamp"], utc=True)
        high, low = _initial_range(candles, paper_start)
        initial_ranges[symbol] = {"high": high, "low": low}

        eligible = candles[candles["timestamp"] >= paper_start]
        completed_counts.append(len(eligible))
        if not candles.empty:
            symbol_latest = pd.Timestamp(candles.iloc[-1]["timestamp"])
            latest_closed = symbol_latest if latest_closed is None else min(latest_closed, symbol_latest)

        if eligible.empty:
            for profile in profiles:
                rows.append(
                    {
                        "profile": profile,
                        "symbol": symbol,
                        "equity": 2000.0,
                        "return": 0.0,
                        "max_drawdown": 0.0,
                        "open_micro_lots": 0,
                        "open_mid_lots": 0,
                        "today_events": 0,
                        "today_buys": 0,
                        "today_sells": 0,
                        "closed_trade_count": 0,
                    }
                )
            continue

        result_cache: dict[tuple[int, int], object] = {}
        for profile in profiles:
            params = PROFILES[profile]
            engine = _profile_engine(profile)

            if engine == "OSS_FORWARD_CANDIDATE":
                result = run_oss_mid_candidate(
                    candles,
                    dataset_id=download.dataset.dataset_id,
                    source_commit_sha=source_commit_sha,
                    evaluation_start=paper_start,
                    config=OssMidCandidateConfig(
                        initial_capital=2000.0,
                        lookback_candles=1095,
                        atr_period=14,
                        atr_regrid_threshold=float(params["atr_regrid_threshold"]),
                        regrid_cooldown_candles=int(params["regrid_cooldown_candles"]),
                        exit_retracement=float(params["exit_retracement"]),
                        mid_recovery_sublevels=int(params["mid_recovery_sublevels"]),
                        ten_sublevel_from_main=7,
                        fee_bps=10.0,
                        slippage_bps=5.0,
                    ),
                )
                metrics = _candidate_metrics(result)
                events = result.events.copy()
                trades = result.trades.copy()
            else:
                cache_key = (
                    int(params["micro_exit_sublevels"]),
                    int(params["mid_recovery_sublevels"]),
                )
                if cache_key not in result_cache:
                    result_cache[cache_key] = run_grid_backtest(
                        candles,
                        dataset_id=download.dataset.dataset_id,
                        source_commit_sha=source_commit_sha,
                        config=_config(profile),
                        evaluation_start=paper_start,
                    )
                result = result_cache[cache_key]
                metrics = _profile_metrics(profile, result)
                events = _scale_single_layer_frame(profile, result.events)
                trades = _scale_single_layer_frame(profile, result.trades)

            latest_eval = pd.Timestamp(result.equity_curve.iloc[-1]["timestamp"])

            if not events.empty:
                events["timestamp"] = pd.to_datetime(events["timestamp"], utc=True)
                events.insert(0, "profile", profile)
                events.insert(1, "symbol", symbol)
                event_frames.append(events)
                today = events[events["timestamp"] == latest_eval]
                buys = int((today["event_type"] == "BUY").sum())
                sells = int((today["event_type"] == "SELL").sum())
                today_count = buys + sells
            else:
                buys = sells = today_count = 0

            if not trades.empty:
                trades.insert(0, "profile", profile)
                trades.insert(1, "symbol", symbol)
                trade_frames.append(trades)

            rows.append(
                {
                    "profile": profile,
                    "symbol": symbol,
                    **metrics,
                    "today_events": today_count,
                    "today_buys": buys,
                    "today_sells": sells,
                }
            )

    completed_paper_candles = min(completed_counts) if completed_counts else 0
    rows_df = pd.DataFrame(rows)
    events_df = _safe_concat(event_frames)
    trades_df = _safe_concat(trade_frames)

    portfolio: list[dict] = []
    for profile in profiles:
        scoped = rows_df[rows_df["profile"] == profile]
        initial = 2000.0 * len(scoped)
        equity = float(scoped["equity"].sum())
        portfolio.append(
            {
                "profile": profile,
                "equity": equity,
                "return": (equity / initial) - 1.0,
                "today_buys": int(scoped["today_buys"].sum()),
                "today_sells": int(scoped["today_sells"].sum()),
            }
        )

    milestone = None
    if completed_paper_candles == 7:
        milestone = "WEEK_1"
    elif completed_paper_candles == 30:
        milestone = "MONTH_1"

    total_today_events = int(rows_df["today_events"].sum()) if not rows_df.empty else 0
    should_notify = total_today_events > 0 or milestone is not None

    payload = {
        "status": "PAPER_LIVE_WAITING" if completed_paper_candles == 0 else "PAPER_LIVE_OBSERVATION",
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit_sha": source_commit_sha,
        "paper_start": paper_start.isoformat(),
        "cutoff": cutoff.isoformat(),
        "latest_closed_candle": latest_closed.isoformat() if latest_closed is not None else None,
        "completed_paper_candles": completed_paper_candles,
        "symbols": list(symbols),
        "profiles": list(profiles),
        "rows": rows,
        "portfolio": portfolio,
        "initial_ranges": initial_ranges,
        "today_event_count": total_today_events,
        "milestone": milestone,
        "should_notify": should_notify,
        "notification_text": "",
        "research_assumptions": {
            "range_lookback_candles": 1095,
            "range_refresh_candles": 30,
            "fees_bps": 10.0,
            "slippage_bps": 5.0,
            "profit_reinvest_fraction": 1.0,
            "runner_fraction": 0.0,
            "single_layer_profiles_normalized_total_capital": 2000.0,
            "single_layer_projection_uses_independent_engine": True,
            "oss_forward_candidate": {
                "profile": "MID_OSS_ATR50_TRAIL7",
                "atr_period": 14,
                "atr_regrid_threshold": 0.50,
                "regrid_cooldown_candles": 60,
                "exit_retracement": 0.07,
                "selection_status": "FROZEN_FORWARD_CANDIDATE",
            },
            "real_orders": False,
        },
    }
    payload["notification_text"] = _notification_text(payload)

    run_key = cutoff.strftime("%Y%m%d")
    run_dir = args.output_root / run_key
    run_dir.mkdir(parents=True, exist_ok=True)
    rows_df.to_csv(run_dir / "current_summary.csv", index=False)
    events_df.to_csv(run_dir / "events.csv", index=False)
    trades_df.to_csv(run_dir / "closed_trades.csv", index=False)
    _write_json(run_dir / "report.json", payload)
    report_md = _build_report_markdown(payload)
    (run_dir / "report.md").write_text(report_md, encoding="utf-8")
    (run_dir / "notification.txt").write_text(payload["notification_text"] + "\n", encoding="utf-8")

    print(f"paper_status={payload['status']}")
    print(f"completed_paper_candles={completed_paper_candles}")
    print(f"latest_closed_candle={payload['latest_closed_candle']}")
    print(f"today_event_count={total_today_events}")
    print(f"milestone={milestone or 'NONE'}")
    print(f"should_notify={'true' if should_notify else 'false'}")
    for item in portfolio:
        print(
            f"profile={item['profile']} "
            f"equity={item['equity']:.6f} "
            f"return={item['return']:.6f} "
            f"today_buys={item['today_buys']} "
            f"today_sells={item['today_sells']}"
        )
    print(f"output={run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
