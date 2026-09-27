from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd

from scripts.research_defensive_low_vol_untouched import (
    ASSETS,
    CONFIRM_DAYS,
    DefenseResult,
    ENTER_BREADTH_MAX,
    EXIT_BREADTH_MIN,
    HIST_START,
    REPRO_END,
    REPRO_START,
    SMA_DAYS,
    TRANSITION_COST,
    UNTOUCHED_END,
    UNTOUCHED_START,
    VOL_DAYS,
    add_defensive_features,
    reproduction_pass,
    run_defensive_backtest,
    summarize as summarize_low_vol,
)
from scripts.research_relative_rotation_graph_intelligence import (
    PairSignal,
    build_pair_context,
    choose_candidate,
    download_panel,
    run_backtest,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
CASH_LABEL = "CASH_PROXY"
CASH_YIELD = 0.0
ROBUSTNESS_DAYS = (180, 120)


def _source_commit() -> str:
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, text=True
    ).strip()


def _json_safe(value):
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, np.generic):
        return _json_safe(value.item())
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_json_safe(payload), indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def run_cash_backtest(
    panel: pd.DataFrame,
    signals_by_date: dict[pd.Timestamp, list[PairSignal]],
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    start_asset: str,
    episode_log: list[dict] | None = None,
    daily_log: list[dict] | None = None,
) -> DefenseResult:
    window = panel[(panel["timestamp"] >= start) & (panel["timestamp"] <= end)].copy()
    if window.empty:
        raise ValueError("Empty evaluation window")

    first = window.iloc[0]
    shadow_asset = start_asset
    actual_asset: str | None = start_asset
    qty = 1.0 / float(first[f"{start_asset}_open"])
    cash_value: float | None = None

    pending_shadow: PairSignal | None = None
    pending_defense_action: str | None = None

    defensive = False
    low_streak = 0
    high_streak = 0

    actual_transitions = 0
    defensive_transitions = 0
    defensive_days = 0
    defensive_entries = 0
    equity: list[float] = []
    dates: list[pd.Timestamp] = []

    for pos, (_, row) in enumerate(window.iterrows()):
        ts = row["timestamp"]

        previous_shadow = shadow_asset
        if pending_shadow is not None:
            shadow_asset = pending_shadow.to_asset
            pending_shadow = None

        if pending_defense_action is not None:
            action = pending_defense_action
            pending_defense_action = None

            if action == "ENTER":
                if actual_asset is None:
                    raise RuntimeError("Cannot ENTER cash from cash")
                value = qty * float(row[f"{actual_asset}_open"])
                cash_value = value * (1.0 - TRANSITION_COST)
                qty = 0.0
                actual_asset = None
                defensive = True
                actual_transitions += 1
                defensive_transitions += 1
                defensive_entries += 1
                if episode_log is not None:
                    episode_log.append(
                        {
                            "start_asset": start_asset,
                            "action": "ENTER",
                            "date": ts.isoformat(),
                            "defensive_asset": CASH_LABEL,
                            "shadow_asset": shadow_asset,
                            "breadth_trigger": int(row["breadth_sma200"]),
                        }
                    )

            elif action == "EXIT":
                if cash_value is None:
                    raise RuntimeError("EXIT requires cash value")
                target = shadow_asset
                qty = cash_value * (1.0 - TRANSITION_COST) / float(
                    row[f"{target}_open"]
                )
                cash_value = None
                actual_asset = target
                defensive = False
                actual_transitions += 1
                defensive_transitions += 1
                if episode_log is not None:
                    episode_log.append(
                        {
                            "start_asset": start_asset,
                            "action": "EXIT",
                            "date": ts.isoformat(),
                            "defensive_asset": CASH_LABEL,
                            "shadow_asset": shadow_asset,
                            "breadth_trigger": int(row["breadth_sma200"]),
                        }
                    )
            else:
                raise RuntimeError(f"Unknown defense action: {action}")

        elif not defensive and shadow_asset != previous_shadow:
            if actual_asset is None:
                raise RuntimeError("Non-defensive state cannot hold cash")
            if actual_asset != shadow_asset:
                value = qty * float(row[f"{actual_asset}_open"])
                qty = value * (1.0 - TRANSITION_COST) / float(
                    row[f"{shadow_asset}_open"]
                )
                actual_asset = shadow_asset
                actual_transitions += 1

        if defensive:
            if cash_value is None:
                raise RuntimeError("Defensive cash state missing cash value")
            value_close = cash_value * (1.0 + CASH_YIELD)
            defensive_days += 1
            actual_label = CASH_LABEL
        else:
            if actual_asset is None:
                raise RuntimeError("Active crypto state missing actual asset")
            value_close = qty * float(row[f"{actual_asset}_close"])
            actual_label = actual_asset

        equity.append(value_close)
        dates.append(ts)

        if daily_log is not None:
            daily_log.append(
                {
                    "start_asset": start_asset,
                    "date": ts.isoformat(),
                    "equity": value_close,
                    "shadow_asset": shadow_asset,
                    "actual_asset": actual_label,
                    "defensive": defensive,
                    "breadth": int(row["breadth_sma200"]),
                    "lowest_vol_asset": row["lowest_vol_asset"],
                }
            )

        if pos == len(window) - 1:
            continue

        candidates = [
            signal
            for signal in signals_by_date.get(ts, [])
            if signal.from_asset == shadow_asset
        ]
        if candidates:
            pending_shadow = choose_candidate(candidates, "BASELINE", None)

        breadth = int(row["breadth_sma200"])
        if breadth <= ENTER_BREADTH_MAX:
            low_streak += 1
        else:
            low_streak = 0

        if breadth >= EXIT_BREADTH_MIN:
            high_streak += 1
        else:
            high_streak = 0

        if not defensive and pending_defense_action is None and low_streak >= CONFIRM_DAYS:
            pending_defense_action = "ENTER"
            low_streak = 0
            high_streak = 0
        elif defensive and pending_defense_action is None and high_streak >= CONFIRM_DAYS:
            pending_defense_action = "EXIT"
            low_streak = 0
            high_streak = 0

    series = pd.Series(equity, index=dates, dtype=float)
    total_return = float(series.iloc[-1] / series.iloc[0] - 1.0)
    max_drawdown = float((series / series.cummax() - 1.0).min())

    return DefenseResult(
        start_asset=start_asset,
        total_return=total_return,
        max_drawdown=max_drawdown,
        actual_transitions=actual_transitions,
        defensive_transitions=defensive_transitions,
        defensive_days=defensive_days,
        defensive_entries=defensive_entries,
        period_days=len(window),
    )


def evaluate_period(
    panel: pd.DataFrame,
    signals_by_date: dict[pd.Timestamp, list[PairSignal]],
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    with_daily: bool = False,
) -> dict[str, pd.DataFrame]:
    base_rows: list[dict] = []
    low_rows: list[dict] = []
    cash_rows: list[dict] = []
    low_episode_rows: list[dict] = []
    cash_episode_rows: list[dict] = []
    low_daily_rows: list[dict] = []
    cash_daily_rows: list[dict] = []

    for asset in ASSETS:
        base = run_backtest(
            panel,
            signals_by_date,
            {},
            start=start,
            end=end,
            start_asset=asset,
            variant="BASELINE",
        )
        base_rows.append(base.__dict__)

        low = run_defensive_backtest(
            panel,
            signals_by_date,
            start=start,
            end=end,
            start_asset=asset,
            episode_log=low_episode_rows,
            daily_log=low_daily_rows if with_daily else None,
        )
        low_rows.append(low.__dict__)

        cash = run_cash_backtest(
            panel,
            signals_by_date,
            start=start,
            end=end,
            start_asset=asset,
            episode_log=cash_episode_rows,
            daily_log=cash_daily_rows if with_daily else None,
        )
        cash_rows.append(cash.__dict__)

    return {
        "baseline": pd.DataFrame(base_rows),
        "low_vol": pd.DataFrame(low_rows),
        "cash": pd.DataFrame(cash_rows),
        "low_episode_log": pd.DataFrame(low_episode_rows),
        "cash_episode_log": pd.DataFrame(cash_episode_rows),
        "low_daily": pd.DataFrame(low_daily_rows),
        "cash_daily": pd.DataFrame(cash_daily_rows),
    }


def _defense_summary(df: pd.DataFrame) -> dict:
    return {
        "median_return": float(df["total_return"].median()),
        "worst_return": float(df["total_return"].min()),
        "median_max_drawdown": float(df["max_drawdown"].median()),
        "worst_max_drawdown": float(df["max_drawdown"].min()),
        "positive_starts": int((df["total_return"] > 0).sum()),
        "median_actual_transitions": float(df["actual_transitions"].median()),
        "median_defensive_transitions": float(df["defensive_transitions"].median()),
        "median_defensive_entries": float(df["defensive_entries"].median()),
        "median_defensive_days": float(df["defensive_days"].median()),
        "median_defensive_exposure": float(
            (df["defensive_days"] / df["period_days"]).median()
        ),
    }


def summarize_ablation(result: dict[str, pd.DataFrame]) -> dict:
    low = _defense_summary(result["low_vol"])
    cash = _defense_summary(result["cash"])
    baseline = {
        "median_return": float(result["baseline"]["total_return"].median()),
        "worst_return": float(result["baseline"]["total_return"].min()),
        "median_max_drawdown": float(result["baseline"]["max_drawdown"].median()),
        "positive_starts": int((result["baseline"]["total_return"] > 0).sum()),
        "median_transitions": float(result["baseline"]["transitions"].median()),
    }
    return {
        "BASELINE": baseline,
        "LOW_VOL_CRYPTO": low,
        "CASH_PROXY": cash,
        "CASH_MINUS_LOW_VOL": {
            "median_return_difference": cash["median_return"] - low["median_return"],
            "median_max_drawdown_difference": (
                cash["median_max_drawdown"] - low["median_max_drawdown"]
            ),
            "worst_return_difference": cash["worst_return"] - low["worst_return"],
            "defensive_exposure_difference": (
                cash["median_defensive_exposure"] - low["median_defensive_exposure"]
            ),
        },
    }


def first_fully_eligible_date(panel: pd.DataFrame) -> pd.Timestamp:
    required = [f"{asset}_sma{SMA_DAYS}" for asset in ASSETS]
    mask = panel[required].notna().all(axis=1) & panel["lowest_vol_asset"].notna()
    eligible = panel.loc[mask, "timestamp"]
    if eligible.empty:
        raise RuntimeError("No fully eligible SMA200/VOL30 date")
    return pd.Timestamp(eligible.iloc[0])


def complete_windows(
    anchor: pd.Timestamp,
    final_end: pd.Timestamp,
    days: int,
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    windows: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    start = anchor
    while True:
        end = start + pd.Timedelta(days=days - 1)
        if end > final_end:
            break
        windows.append((start, end))
        start = end + pd.Timedelta(days=1)
    return windows


def build_robustness_table(
    panel: pd.DataFrame,
    signals_by_date: dict[pd.Timestamp, list[PairSignal]],
    *,
    anchor: pd.Timestamp,
    final_end: pd.Timestamp,
) -> pd.DataFrame:
    rows: list[dict] = []
    for days in ROBUSTNESS_DAYS:
        for index, (start, end) in enumerate(
            complete_windows(anchor, final_end, days), start=1
        ):
            result = evaluate_period(
                panel,
                signals_by_date,
                start=start,
                end=end,
                with_daily=False,
            )
            summary = summarize_ablation(result)
            rows.append(
                {
                    "window_days": days,
                    "window_index": index,
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "baseline_median_return": summary["BASELINE"]["median_return"],
                    "baseline_median_max_drawdown": summary["BASELINE"][
                        "median_max_drawdown"
                    ],
                    "low_vol_median_return": summary["LOW_VOL_CRYPTO"][
                        "median_return"
                    ],
                    "low_vol_median_max_drawdown": summary["LOW_VOL_CRYPTO"][
                        "median_max_drawdown"
                    ],
                    "cash_median_return": summary["CASH_PROXY"]["median_return"],
                    "cash_median_max_drawdown": summary["CASH_PROXY"][
                        "median_max_drawdown"
                    ],
                    "cash_minus_low_return": summary["CASH_MINUS_LOW_VOL"][
                        "median_return_difference"
                    ],
                    "cash_minus_low_drawdown": summary["CASH_MINUS_LOW_VOL"][
                        "median_max_drawdown_difference"
                    ],
                    "low_vol_defensive_exposure": summary["LOW_VOL_CRYPTO"][
                        "median_defensive_exposure"
                    ],
                    "cash_defensive_exposure": summary["CASH_PROXY"][
                        "median_defensive_exposure"
                    ],
                }
            )
    return pd.DataFrame(rows)


def build_episode_comparison(
    low_daily: pd.DataFrame,
    cash_daily: pd.DataFrame,
    low_episode_log: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if low_episode_log.empty:
        return pd.DataFrame(), pd.DataFrame()

    events = low_episode_log[
        low_episode_log["start_asset"] == ASSETS[0]
    ].copy()
    events["date"] = pd.to_datetime(events["date"], utc=True)
    events = events.sort_values("date").reset_index(drop=True)

    pairs: list[tuple[pd.Series, pd.Series | None]] = []
    pending_entry: pd.Series | None = None
    for _, row in events.iterrows():
        if row["action"] == "ENTER":
            pending_entry = row
        elif row["action"] == "EXIT" and pending_entry is not None:
            pairs.append((pending_entry, row))
            pending_entry = None
    if pending_entry is not None:
        pairs.append((pending_entry, None))

    low = low_daily.copy()
    cash = cash_daily.copy()
    low["date"] = pd.to_datetime(low["date"], utc=True)
    cash["date"] = pd.to_datetime(cash["date"], utc=True)

    rows: list[dict] = []
    for episode_id, (entry, exit_row) in enumerate(pairs, start=1):
        entry_date = pd.Timestamp(entry["date"])
        exit_date = (
            pd.Timestamp(exit_row["date"]) if exit_row is not None else pd.NaT
        )

        for start_asset in ASSETS:
            low_asset = low[low["start_asset"] == start_asset].sort_values("date")
            cash_asset = cash[cash["start_asset"] == start_asset].sort_values("date")

            low_pre = low_asset[low_asset["date"] < entry_date]
            cash_pre = cash_asset[cash_asset["date"] < entry_date]
            if low_pre.empty or cash_pre.empty:
                continue

            if pd.isna(exit_date):
                low_end = low_asset[low_asset["date"] >= entry_date]
                cash_end = cash_asset[cash_asset["date"] >= entry_date]
            else:
                low_end = low_asset[
                    (low_asset["date"] >= entry_date)
                    & (low_asset["date"] < exit_date)
                ]
                cash_end = cash_asset[
                    (cash_asset["date"] >= entry_date)
                    & (cash_asset["date"] < exit_date)
                ]

            if low_end.empty or cash_end.empty:
                continue

            low_return = float(
                low_end.iloc[-1]["equity"] / low_pre.iloc[-1]["equity"] - 1.0
            )
            cash_return = float(
                cash_end.iloc[-1]["equity"] / cash_pre.iloc[-1]["equity"] - 1.0
            )
            rows.append(
                {
                    "episode_id": episode_id,
                    "start_asset": start_asset,
                    "entry_execution_date": entry_date.isoformat(),
                    "exit_execution_date": (
                        exit_date.isoformat() if pd.notna(exit_date) else None
                    ),
                    "low_vol_defensive_asset": entry["defensive_asset"],
                    "low_vol_episode_return_pre_entry_close_to_last_defensive_close": low_return,
                    "cash_episode_return_pre_entry_close_to_last_defensive_close": cash_return,
                    "cash_minus_low_vol_episode_return": cash_return - low_return,
                }
            )

    raw = pd.DataFrame(rows)
    if raw.empty:
        return raw, pd.DataFrame()

    agg = (
        raw.groupby(
            [
                "episode_id",
                "entry_execution_date",
                "exit_execution_date",
                "low_vol_defensive_asset",
            ],
            as_index=False,
            dropna=False,
        )
        .agg(
            low_vol_median_episode_return=(
                "low_vol_episode_return_pre_entry_close_to_last_defensive_close",
                "median",
            ),
            cash_median_episode_return=(
                "cash_episode_return_pre_entry_close_to_last_defensive_close",
                "median",
            ),
            cash_minus_low_vol_median_episode_return=(
                "cash_minus_low_vol_episode_return",
                "median",
            ),
        )
    )
    return raw, agg


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Frozen destination ablation: LOW_VOL_CRYPTO vs CASH_PROXY."
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=REPO_ROOT
        / "research_artifacts"
        / "cash_defense_destination_ablation_v1",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    panel, dataset_meta = download_panel(HIST_START, UNTOUCHED_END)
    panel = add_defensive_features(panel)
    signals_by_date, _ = build_pair_context(panel)

    reproduction = evaluate_period(
        panel,
        signals_by_date,
        start=REPRO_START,
        end=REPRO_END,
        with_daily=False,
    )
    reproduction_summary = summarize_ablation(reproduction)
    low_repro_summary = summarize_low_vol(
        reproduction["baseline"], reproduction["low_vol"]
    )
    gate_pass, gate = reproduction_pass(low_repro_summary)

    run_id = f"CASH_DEFENSE_DESTINATION_ABLATION_V1_{_source_commit()[:12]}"
    run_dir = args.output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    reproduction["baseline"].to_csv(
        run_dir / "reproduction_baseline_by_start.csv", index=False
    )
    reproduction["low_vol"].to_csv(
        run_dir / "reproduction_low_vol_by_start.csv", index=False
    )
    reproduction["cash"].to_csv(
        run_dir / "reproduction_cash_by_start.csv", index=False
    )

    if not gate_pass:
        _write_json(
            run_dir / "summary.json",
            {
                "run_id": run_id,
                "source_commit_sha": _source_commit(),
                "status": "REPRODUCTION_FAILED_ABLATION_NOT_INTERPRETED",
                "reproduction_gate": gate,
                "reproduction_summary": reproduction_summary,
                "dataset": dataset_meta,
            },
        )
        print(json.dumps(_json_safe(gate), indent=2, sort_keys=True))
        print("status=REPRODUCTION_FAILED_ABLATION_NOT_INTERPRETED")
        return 4

    untouched = evaluate_period(
        panel,
        signals_by_date,
        start=UNTOUCHED_START,
        end=UNTOUCHED_END,
        with_daily=False,
    )
    untouched_summary = summarize_ablation(untouched)

    full_start = first_fully_eligible_date(panel)
    full = evaluate_period(
        panel,
        signals_by_date,
        start=full_start,
        end=UNTOUCHED_END,
        with_daily=True,
    )
    full_summary = summarize_ablation(full)

    robustness = build_robustness_table(
        panel,
        signals_by_date,
        anchor=full_start,
        final_end=UNTOUCHED_END,
    )

    episode_raw, episode_agg = build_episode_comparison(
        full["low_daily"],
        full["cash_daily"],
        full["low_episode_log"],
    )

    untouched["baseline"].to_csv(
        run_dir / "untouched_baseline_by_start.csv", index=False
    )
    untouched["low_vol"].to_csv(
        run_dir / "untouched_low_vol_by_start.csv", index=False
    )
    untouched["cash"].to_csv(
        run_dir / "untouched_cash_by_start.csv", index=False
    )
    full["baseline"].to_csv(run_dir / "full_history_baseline_by_start.csv", index=False)
    full["low_vol"].to_csv(run_dir / "full_history_low_vol_by_start.csv", index=False)
    full["cash"].to_csv(run_dir / "full_history_cash_by_start.csv", index=False)
    full["low_episode_log"].to_csv(
        run_dir / "full_history_low_vol_episode_log.csv", index=False
    )
    full["cash_episode_log"].to_csv(
        run_dir / "full_history_cash_episode_log.csv", index=False
    )
    robustness.to_csv(run_dir / "robustness_windows.csv", index=False)
    episode_raw.to_csv(run_dir / "episode_comparison_by_start.csv", index=False)
    episode_agg.to_csv(run_dir / "episode_comparison_median.csv", index=False)

    report = {
        "run_id": run_id,
        "source_commit_sha": _source_commit(),
        "status": "CASH_DEFENSE_DESTINATION_ABLATION_EXECUTED",
        "cash_model": {
            "label": CASH_LABEL,
            "yield": CASH_YIELD,
            "crypto_price_exposure_while_defensive": False,
            "transition_cost": TRANSITION_COST,
            "counterparty_or_depeg_risk_modeled": False,
        },
        "frozen_timing": {
            "sma_days": SMA_DAYS,
            "enter_breadth_max": ENTER_BREADTH_MAX,
            "exit_breadth_min": EXIT_BREADTH_MIN,
            "confirmation_days": CONFIRM_DAYS,
            "volatility_days_low_vol_variant": VOL_DAYS,
            "relative_router_continues_in_shadow": True,
            "next_open_execution": True,
            "timing_changed": False,
        },
        "reproduction_gate": gate,
        "reproduction_period": {
            "start": REPRO_START.date().isoformat(),
            "end": REPRO_END.date().isoformat(),
            "summary": reproduction_summary,
        },
        "untouched_already_open_period": {
            "start": UNTOUCHED_START.date().isoformat(),
            "end": UNTOUCHED_END.date().isoformat(),
            "summary": untouched_summary,
            "note": "This period was previously opened by the frozen LOW_VOL validation and is not untouched for this cash hypothesis.",
        },
        "full_history": {
            "start": full_start.date().isoformat(),
            "end": UNTOUCHED_END.date().isoformat(),
            "summary": full_summary,
        },
        "robustness_contract": {
            "anchor": full_start.date().isoformat(),
            "window_days": list(ROBUSTNESS_DAYS),
            "non_overlapping": True,
            "complete_windows_only": True,
            "state_reset_each_window": True,
            "window_count_180": int((robustness["window_days"] == 180).sum()),
            "window_count_120": int((robustness["window_days"] == 120).sum()),
        },
        "episode_count": int(episode_agg["episode_id"].nunique())
        if not episode_agg.empty
        else 0,
        "dataset": dataset_meta,
        "interpretation_boundary": [
            "This test changes destination only; defensive timing is frozen.",
            "CASH_PROXY is modeled at 0% yield and is not a claim about USDT, USDC, fiat custody, depeg, counterparty, tax, or execution risk.",
            "Macro BROAD_RISK_OFF is not used as an entry trigger in this test.",
            "No automatic trading is authorized.",
        ],
    }
    _write_json(run_dir / "summary.json", report)

    print(json.dumps(_json_safe(report), indent=2, sort_keys=True))
    print(f"output={run_dir}")
    print("status=CASH_DEFENSE_DESTINATION_ABLATION_EXECUTED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
