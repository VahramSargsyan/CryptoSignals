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


def _is_calendar_month_end(timestamp) -> bool:
    ts = _utc(timestamp).normalize()
    return (ts + pd.Timedelta(days=1)).month != ts.month


def _profile_equity_frame(profile: str, result) -> pd.DataFrame:
    frame = result.equity_curve.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)

    if _profile_engine(profile) == "OSS_FORWARD_CANDIDATE":
        column = "mid_equity"
        scale = 1.0
    else:
        layer = _profile_layer(profile)
        if layer == "BOTH":
            column = "total_equity"
            scale = 1.0
        elif layer == "MICRO":
            column = "micro_equity"
            scale = 2.0
        elif layer == "MID":
            column = "mid_equity"
            scale = 2.0
        else:
            raise ValueError(f"Unknown profile layer: {layer}")

    out = frame.loc[:, ["timestamp", column]].copy()
    out["equity"] = out[column].astype(float) * scale
    return out.loc[:, ["timestamp", "equity"]]


def _curve_period_metrics(
    curve: pd.DataFrame,
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    initial_equity: float,
) -> dict:
    frame = curve.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame = frame.sort_values("timestamp", kind="stable").reset_index(drop=True)
    start = _utc(start)
    end = _utc(end)

    previous = frame[frame["timestamp"] < start]
    baseline = (
        float(previous.iloc[-1]["equity"])
        if not previous.empty
        else float(initial_equity)
    )
    window = frame[(frame["timestamp"] >= start) & (frame["timestamp"] <= end)]
    if window.empty:
        return {
            "start_equity": baseline,
            "end_equity": baseline,
            "return": 0.0,
            "max_drawdown": 0.0,
        }

    end_equity = float(window.iloc[-1]["equity"])
    peak = baseline
    max_drawdown = 0.0
    for value in window["equity"].astype(float):
        peak = max(peak, float(value))
        max_drawdown = max(max_drawdown, 1.0 - (float(value) / peak))

    return {
        "start_equity": baseline,
        "end_equity": end_equity,
        "return": (end_equity / baseline) - 1.0 if baseline else 0.0,
        "max_drawdown": max_drawdown,
    }


def _build_calendar_month_report(
    *,
    latest_closed: pd.Timestamp | None,
    paper_start: pd.Timestamp,
    profiles: tuple[str, ...],
    symbols: tuple[str, ...],
    rows_df: pd.DataFrame,
    events_df: pd.DataFrame,
    trades_df: pd.DataFrame,
    equity_frames: dict[tuple[str, str], pd.DataFrame],
) -> dict | None:
    if latest_closed is None or not _is_calendar_month_end(latest_closed):
        return None

    latest = _utc(latest_closed).normalize()
    month_start = latest.replace(day=1)
    effective_start = max(month_start, _utc(paper_start).normalize())

    events = events_df.copy()
    if not events.empty and "timestamp" in events.columns:
        events["timestamp"] = pd.to_datetime(events["timestamp"], utc=True)
    trades = trades_df.copy()
    if not trades.empty and "exit_timestamp" in trades.columns:
        trades["exit_timestamp"] = pd.to_datetime(trades["exit_timestamp"], utc=True)

    profile_summary: list[dict] = []
    asset_summary: list[dict] = []

    for profile in profiles:
        symbol_curves = []
        for symbol in symbols:
            curve = equity_frames.get((profile, symbol))
            if curve is None or curve.empty:
                continue
            series = curve.set_index("timestamp")["equity"].rename(symbol)
            symbol_curves.append(series)

            metrics = _curve_period_metrics(
                curve,
                start=effective_start,
                end=latest,
                initial_equity=2000.0,
            )
            scoped_row = rows_df[
                (rows_df["profile"] == profile) & (rows_df["symbol"] == symbol)
            ]
            row = scoped_row.iloc[0] if not scoped_row.empty else None

            month_events = events
            if not events.empty:
                month_events = events[
                    (events["profile"] == profile)
                    & (events["symbol"] == symbol)
                    & (events["timestamp"] >= effective_start)
                    & (events["timestamp"] <= latest)
                ]
            month_trades = trades
            if not trades.empty:
                month_trades = trades[
                    (trades["profile"] == profile)
                    & (trades["symbol"] == symbol)
                    & (trades["exit_timestamp"] >= effective_start)
                    & (trades["exit_timestamp"] <= latest)
                ]

            asset_summary.append(
                {
                    "profile": profile,
                    "symbol": symbol,
                    "month_return": metrics["return"],
                    "month_max_drawdown": metrics["max_drawdown"],
                    "inception_return": float(row["return"]) if row is not None else 0.0,
                    "inception_max_drawdown": float(row["max_drawdown"]) if row is not None else 0.0,
                    "month_buys": int((month_events["event_type"] == "BUY").sum()) if not month_events.empty else 0,
                    "month_sells": int((month_events["event_type"] == "SELL").sum()) if not month_events.empty else 0,
                    "month_closed_trades": int(len(month_trades)),
                    "open_micro_lots": int(row["open_micro_lots"]) if row is not None else 0,
                    "open_mid_lots": int(row["open_mid_lots"]) if row is not None else 0,
                }
            )

        if not symbol_curves:
            continue

        portfolio_curve = pd.concat(symbol_curves, axis=1).sort_index().ffill()
        portfolio_curve = portfolio_curve.dropna(how="any")
        aggregate = pd.DataFrame(
            {
                "timestamp": portfolio_curve.index,
                "equity": portfolio_curve.sum(axis=1).astype(float),
            }
        ).reset_index(drop=True)

        month_metrics = _curve_period_metrics(
            aggregate,
            start=effective_start,
            end=latest,
            initial_equity=2000.0 * len(symbol_curves),
        )
        inception_metrics = _curve_period_metrics(
            aggregate,
            start=_utc(paper_start),
            end=latest,
            initial_equity=2000.0 * len(symbol_curves),
        )

        scoped_events = events
        if not events.empty:
            scoped_events = events[
                (events["profile"] == profile)
                & (events["timestamp"] >= effective_start)
                & (events["timestamp"] <= latest)
            ]
        scoped_trades = trades
        if not trades.empty:
            scoped_trades = trades[
                (trades["profile"] == profile)
                & (trades["exit_timestamp"] >= effective_start)
                & (trades["exit_timestamp"] <= latest)
            ]
        current_rows = rows_df[rows_df["profile"] == profile]

        profile_summary.append(
            {
                "profile": profile,
                "month_start_equity": month_metrics["start_equity"],
                "month_end_equity": month_metrics["end_equity"],
                "month_return": month_metrics["return"],
                "month_max_drawdown": month_metrics["max_drawdown"],
                "inception_return": inception_metrics["return"],
                "inception_max_drawdown": inception_metrics["max_drawdown"],
                "month_buys": int((scoped_events["event_type"] == "BUY").sum()) if not scoped_events.empty else 0,
                "month_sells": int((scoped_events["event_type"] == "SELL").sum()) if not scoped_events.empty else 0,
                "month_closed_trades": int(len(scoped_trades)),
                "open_micro_lots": int(current_rows["open_micro_lots"].sum()) if not current_rows.empty else 0,
                "open_mid_lots": int(current_rows["open_mid_lots"].sum()) if not current_rows.empty else 0,
            }
        )

    best_profile = None
    worst_profile = None
    if profile_summary:
        best_profile = max(profile_summary, key=lambda row: row["month_return"])["profile"]
        worst_profile = min(profile_summary, key=lambda row: row["month_return"])["profile"]

    return {
        "period": latest.strftime("%Y-%m"),
        "month_start": effective_start.isoformat(),
        "month_end": latest.isoformat(),
        "profile_summary": profile_summary,
        "asset_summary": asset_summary,
        "highest_month_return_profile": best_profile,
        "lowest_month_return_profile": worst_profile,
        "paper_only": True,
    }


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

    monthly = payload.get("monthly_report")
    if monthly:
        lines.extend(
            [
                "",
                f"## Calendar monthly report — {monthly['period']}",
                "",
                "| Profile | Month return | Month max DD | Since start | Inception max DD | BUY | SELL | Closed trades | Open Micro | Open Mid |",
                "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for row in monthly["profile_summary"]:
            lines.append(
                f"| {row['profile']} | {_pct(row['month_return'])} | "
                f"{_pct(row['month_max_drawdown'])} | {_pct(row['inception_return'])} | "
                f"{_pct(row['inception_max_drawdown'])} | {row['month_buys']} | "
                f"{row['month_sells']} | {row['month_closed_trades']} | "
                f"{row['open_micro_lots']} | {row['open_mid_lots']} |"
            )
        lines.extend(
            [
                "",
                f"Highest monthly return profile: **{monthly['highest_month_return_profile']}**",
                f"Lowest monthly return profile: **{monthly['lowest_month_return_profile']}**",
                "",
                "### Monthly per-asset detail",
                "",
                "| Profile | Symbol | Month | Since start | Month DD | BUY | SELL | Closed | Open Micro | Open Mid |",
                "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for row in monthly["asset_summary"]:
            lines.append(
                f"| {row['profile']} | {row['symbol']} | {_pct(row['month_return'])} | "
                f"{_pct(row['inception_return'])} | {_pct(row['month_max_drawdown'])} | "
                f"{row['month_buys']} | {row['month_sells']} | {row['month_closed_trades']} | "
                f"{row['open_micro_lots']} | {row['open_mid_lots']} |"
            )

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
    monthly = payload.get("monthly_report")
    if monthly:
        lines = [
            f"Grid Paper Live — monthly report {monthly['period']}",
            f"Closed candle: {latest}",
        ]
        for row in monthly["profile_summary"]:
            lines.append(
                f"{row['profile']}: month {_pct(row['month_return'])}, "
                f"since start {_pct(row['inception_return'])}, "
                f"DD {_pct(row['month_max_drawdown'])}, "
                f"BUY {row['month_buys']}, SELL {row['month_sells']}, "
                f"closed {row['month_closed_trades']}"
            )
        lines.append(
            f"Highest month return: {monthly['highest_month_return_profile']}; "
            f"lowest: {monthly['lowest_month_return_profile']}"
        )
        lines.append("PAPER ONLY — no real orders.")
        return "\n".join(lines)

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


def _notification_text_ru(payload: dict) -> str:
    latest = payload.get("latest_closed_candle") or "ожидание"
    monthly = payload.get("monthly_report")
    if monthly:
        lines = [
            f"📊 Сетка — месячный бумажный отчёт {monthly['period']}",
            f"Закрытая свеча: {latest}",
        ]
        for row in monthly["profile_summary"]:
            lines.append(
                f"{row['profile']}: месяц {_pct(row['month_return'])}, "
                f"с начала {_pct(row['inception_return'])}, "
                f"DD месяца {_pct(row['month_max_drawdown'])}, "
                f"BUY {row['month_buys']}, SELL {row['month_sells']}, "
                f"закрыто {row['month_closed_trades']}, "
                f"открыто Mµ {row['open_micro_lots']} / Mid {row['open_mid_lots']}"
            )
        lines.append(
            f"Макс. доходность месяца: {monthly['highest_month_return_profile']}; "
            f"мин.: {monthly['lowest_month_return_profile']}"
        )
        lines.append("БУМАЖНЫЙ РЕЖИМ — реальные ордера не отправляются.")
        return "\n".join(lines)

    active_rows = [
        row for row in payload.get("rows", [])
        if int(row.get("today_events", 0)) > 0
    ]

    if not active_rows and not payload.get("milestone"):
        return (
            "Сетка: сигналов нет.\n"
            f"Закрытая свеча: {latest}\n"
            f"Дней бумажного наблюдения: {payload['completed_paper_candles']}"
        )

    lines = [
        "Сетка — бумажный монитор v1",
        f"Закрытая свеча: {latest}",
        f"Дней бумажного наблюдения: {payload['completed_paper_candles']}",
    ]

    if active_rows:
        lines.append("🚨 СИГНАЛЫ:")
        for row in active_rows:
            lines.append(
                f"{row['profile']} {row['symbol']}: "
                f"ПОКУПКА {row['today_buys']}, ПРОДАЖА {row['today_sells']}"
            )

    active_profiles = {row["profile"] for row in active_rows}
    if active_profiles or payload.get("milestone"):
        lines.append("Снимок профилей:")
        for row in payload.get("portfolio", []):
            if payload.get("milestone") or row["profile"] in active_profiles:
                lines.append(
                    f"{row['profile']}: {_pct(row['return'])}, "
                    f"ПОКУПКА {row['today_buys']}, ПРОДАЖА {row['today_sells']}"
                )

    if payload.get("milestone"):
        lines.append(f"Контрольная точка: {payload['milestone']}")
    lines.append("БУМАЖНЫЙ РЕЖИМ — реальные ордера не отправляются.")
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
    equity_frames: dict[tuple[str, str], pd.DataFrame] = {}
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

            equity_frames[(profile, symbol)] = _profile_equity_frame(profile, result)
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

    monthly_report = _build_calendar_month_report(
        latest_closed=latest_closed,
        paper_start=paper_start,
        profiles=profiles,
        symbols=symbols,
        rows_df=rows_df,
        events_df=events_df,
        trades_df=trades_df,
        equity_frames=equity_frames,
    )

    total_today_events = int(rows_df["today_events"].sum()) if not rows_df.empty else 0
    should_notify = (
        total_today_events > 0
        or milestone is not None
        or monthly_report is not None
    )

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
        "monthly_report": monthly_report,
        "should_notify": should_notify,
        "notification_text": "",
        "telegram_text_ru": "",
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
    payload["telegram_text_ru"] = _notification_text_ru(payload)

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
    print(
        "monthly_report="
        + (monthly_report["period"] if monthly_report is not None else "NONE")
    )
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
