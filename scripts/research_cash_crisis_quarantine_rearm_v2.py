from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from scripts.research_cash_crisis_quarantine_v1 import (
    QUARANTINE_DAYS,
    ROBUSTNESS_DAYS,
)
from scripts.research_cash_defense_destination_ablation_v1 import (
    CASH_LABEL,
    CASH_YIELD,
    complete_windows,
    evaluate_period as evaluate_frozen_destination,
    first_fully_eligible_date,
    summarize_ablation,
)
from scripts.research_defensive_low_vol_untouched import (
    ASSETS,
    CONFIRM_DAYS,
    ENTER_BREADTH_MAX,
    EXIT_BREADTH_MIN,
    HIST_START,
    REPRO_END,
    REPRO_START,
    TRANSITION_COST,
    UNTOUCHED_END,
    UNTOUCHED_START,
    add_defensive_features,
    reproduction_pass,
    summarize as summarize_low_vol,
)
from scripts.research_relative_rotation_graph_intelligence import (
    PairSignal,
    build_pair_context,
    choose_candidate,
    download_panel,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class RearmResult:
    start_asset: str
    total_return: float
    max_drawdown: float
    actual_transitions: int
    cash_entries: int
    cash_days: int
    rearm_count: int
    disarmed_days: int
    period_days: int


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


def run_cash_quarantine_rearm_backtest(
    panel: pd.DataFrame,
    signals_by_date: dict[pd.Timestamp, list[PairSignal]],
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    start_asset: str,
    quarantine_days: int,
    episode_log: list[dict] | None = None,
    daily_log: list[dict] | None = None,
) -> RearmResult:
    if quarantine_days <= 0:
        raise ValueError("quarantine_days must be positive")

    window = panel[(panel["timestamp"] >= start) & (panel["timestamp"] <= end)].copy()
    if window.empty:
        raise ValueError("Empty evaluation window")

    first = window.iloc[0]
    shadow_asset = start_asset
    actual_asset: str | None = start_asset
    qty = 1.0 / float(first[f"{start_asset}_open"])
    cash_value: float | None = None

    pending_shadow: PairSignal | None = None
    pending_action: str | None = None

    state = "ARMED"
    low_streak = 0
    recovery_streak = 0
    cash_day_number = 0

    actual_transitions = 0
    cash_entries = 0
    cash_days = 0
    rearm_count = 0
    disarmed_days = 0

    equity: list[float] = []
    dates: list[pd.Timestamp] = []

    for pos, (_, row) in enumerate(window.iterrows()):
        ts = row["timestamp"]

        previous_shadow = shadow_asset
        if pending_shadow is not None:
            shadow_asset = pending_shadow.to_asset
            pending_shadow = None

        if pending_action is not None:
            action = pending_action
            pending_action = None

            if action == "ENTER":
                if state != "ARMED" or actual_asset is None:
                    raise RuntimeError("ENTER requires ARMED crypto state")
                value = qty * float(row[f"{actual_asset}_open"])
                cash_value = value * (1.0 - TRANSITION_COST)
                qty = 0.0
                actual_asset = None
                state = "CASH"
                cash_day_number = 0
                low_streak = 0
                recovery_streak = 0
                actual_transitions += 1
                cash_entries += 1
                if episode_log is not None:
                    episode_log.append(
                        {
                            "start_asset": start_asset,
                            "action": "ENTER",
                            "date": ts.isoformat(),
                            "state_after": state,
                            "shadow_asset": shadow_asset,
                            "breadth": int(row["breadth_sma200"]),
                            "quarantine_days": quarantine_days,
                        }
                    )

            elif action == "EXIT":
                if state != "CASH" or cash_value is None:
                    raise RuntimeError("EXIT requires CASH state")
                target = shadow_asset
                qty = cash_value * (1.0 - TRANSITION_COST) / float(
                    row[f"{target}_open"]
                )
                cash_value = None
                actual_asset = target
                state = "POST_CASH_DISARMED"
                cash_day_number = 0
                low_streak = 0
                recovery_streak = 0
                actual_transitions += 1
                if episode_log is not None:
                    episode_log.append(
                        {
                            "start_asset": start_asset,
                            "action": "EXIT",
                            "date": ts.isoformat(),
                            "state_after": state,
                            "shadow_asset": shadow_asset,
                            "breadth": int(row["breadth_sma200"]),
                            "quarantine_days": quarantine_days,
                        }
                    )
            else:
                raise RuntimeError(f"Unknown pending action: {action}")

        elif state != "CASH" and shadow_asset != previous_shadow:
            if actual_asset is None:
                raise RuntimeError("Non-cash state missing actual asset")
            if actual_asset != shadow_asset:
                value = qty * float(row[f"{actual_asset}_open"])
                qty = value * (1.0 - TRANSITION_COST) / float(
                    row[f"{shadow_asset}_open"]
                )
                actual_asset = shadow_asset
                actual_transitions += 1

        if state == "CASH":
            if cash_value is None:
                raise RuntimeError("CASH state missing cash value")
            value_close = cash_value * (1.0 + CASH_YIELD)
            actual_label = CASH_LABEL
            cash_day_number += 1
            cash_days += 1
        else:
            if actual_asset is None:
                raise RuntimeError("Crypto state missing actual asset")
            value_close = qty * float(row[f"{actual_asset}_close"])
            actual_label = actual_asset
            if state == "POST_CASH_DISARMED":
                disarmed_days += 1

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
                    "state": state,
                    "low_streak": low_streak,
                    "recovery_streak": recovery_streak,
                    "cash_day_number": cash_day_number if state == "CASH" else 0,
                    "quarantine_days": quarantine_days,
                    "breadth": int(row["breadth_sma200"]),
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

        if state == "CASH":
            low_streak = 0
            recovery_streak = 0
            if cash_day_number >= quarantine_days:
                pending_action = "EXIT"
            continue

        if state == "POST_CASH_DISARMED":
            low_streak = 0
            if breadth >= EXIT_BREADTH_MIN:
                recovery_streak += 1
            else:
                recovery_streak = 0

            if recovery_streak >= CONFIRM_DAYS:
                state = "ARMED"
                recovery_streak = 0
                low_streak = 0
                rearm_count += 1
                if episode_log is not None:
                    episode_log.append(
                        {
                            "start_asset": start_asset,
                            "action": "REARM",
                            "date": ts.isoformat(),
                            "state_after": state,
                            "shadow_asset": shadow_asset,
                            "breadth": breadth,
                            "quarantine_days": quarantine_days,
                        }
                    )
            continue

        if state != "ARMED":
            raise RuntimeError(f"Unexpected state: {state}")

        recovery_streak = 0
        if breadth <= ENTER_BREADTH_MAX:
            low_streak += 1
        else:
            low_streak = 0

        if low_streak >= CONFIRM_DAYS:
            pending_action = "ENTER"
            low_streak = 0

    series = pd.Series(equity, index=dates, dtype=float)
    total_return = float(series.iloc[-1] / series.iloc[0] - 1.0)
    max_drawdown = float((series / series.cummax() - 1.0).min())

    return RearmResult(
        start_asset=start_asset,
        total_return=total_return,
        max_drawdown=max_drawdown,
        actual_transitions=actual_transitions,
        cash_entries=cash_entries,
        cash_days=cash_days,
        rearm_count=rearm_count,
        disarmed_days=disarmed_days,
        period_days=len(window),
    )


def _summary(df: pd.DataFrame) -> dict:
    return {
        "median_return": float(df["total_return"].median()),
        "worst_return": float(df["total_return"].min()),
        "median_max_drawdown": float(df["max_drawdown"].median()),
        "worst_max_drawdown": float(df["max_drawdown"].min()),
        "positive_starts": int((df["total_return"] > 0).sum()),
        "median_actual_transitions": float(df["actual_transitions"].median()),
        "median_cash_entries": float(df["cash_entries"].median()),
        "median_cash_days": float(df["cash_days"].median()),
        "median_cash_exposure": float((df["cash_days"] / df["period_days"]).median()),
        "median_rearm_count": float(df["rearm_count"].median()),
        "median_disarmed_days": float(df["disarmed_days"].median()),
        "median_disarmed_exposure": float(
            (df["disarmed_days"] / df["period_days"]).median()
        ),
    }


def evaluate_period(
    panel: pd.DataFrame,
    signals_by_date: dict[pd.Timestamp, list[PairSignal]],
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    with_logs: bool = False,
) -> tuple[dict[int, pd.DataFrame], pd.DataFrame, pd.DataFrame]:
    results: dict[int, pd.DataFrame] = {}
    episodes: list[dict] = []
    daily: list[dict] = []

    for days in QUARANTINE_DAYS:
        rows: list[dict] = []
        for asset in ASSETS:
            r = run_cash_quarantine_rearm_backtest(
                panel,
                signals_by_date,
                start=start,
                end=end,
                start_asset=asset,
                quarantine_days=days,
                episode_log=episodes if with_logs else None,
                daily_log=daily if with_logs else None,
            )
            rows.append(r.__dict__)
        results[days] = pd.DataFrame(rows)

    return results, pd.DataFrame(episodes), pd.DataFrame(daily)


def summarize_variants(results: dict[int, pd.DataFrame]) -> dict:
    return {f"CASH_REARM_{days}D": _summary(df) for days, df in results.items()}


def build_robustness(
    panel: pd.DataFrame,
    signals_by_date: dict[pd.Timestamp, list[PairSignal]],
    *,
    anchor: pd.Timestamp,
    final_end: pd.Timestamp,
) -> pd.DataFrame:
    rows: list[dict] = []

    for window_days in ROBUSTNESS_DAYS:
        for window_index, (start, end) in enumerate(
            complete_windows(anchor, final_end, window_days), start=1
        ):
            frozen = evaluate_frozen_destination(
                panel,
                signals_by_date,
                start=start,
                end=end,
                with_daily=False,
            )
            frozen_summary = summarize_ablation(frozen)
            variants, _, _ = evaluate_period(
                panel,
                signals_by_date,
                start=start,
                end=end,
                with_logs=False,
            )

            low = frozen_summary["LOW_VOL_CRYPTO"]
            old_cash = frozen_summary["CASH_PROXY"]

            for days, df in variants.items():
                s = _summary(df)
                rows.append(
                    {
                        "window_days": window_days,
                        "window_index": window_index,
                        "start": start.isoformat(),
                        "end": end.isoformat(),
                        "quarantine_days": days,
                        "median_return": s["median_return"],
                        "median_max_drawdown": s["median_max_drawdown"],
                        "worst_return": s["worst_return"],
                        "positive_starts": s["positive_starts"],
                        "cash_entries": s["median_cash_entries"],
                        "cash_exposure": s["median_cash_exposure"],
                        "rearm_count": s["median_rearm_count"],
                        "disarmed_exposure": s["median_disarmed_exposure"],
                        "low_vol_return": low["median_return"],
                        "low_vol_drawdown": low["median_max_drawdown"],
                        "frozen_cash_return": old_cash["median_return"],
                        "frozen_cash_drawdown": old_cash["median_max_drawdown"],
                        "beats_low_vol_return": s["median_return"] > low["median_return"],
                        "beats_low_vol_drawdown": (
                            s["median_max_drawdown"] > low["median_max_drawdown"]
                        ),
                        "beats_low_vol_both": (
                            s["median_return"] > low["median_return"]
                            and s["median_max_drawdown"] > low["median_max_drawdown"]
                        ),
                        "beats_frozen_cash_return": (
                            s["median_return"] > old_cash["median_return"]
                        ),
                        "beats_frozen_cash_drawdown": (
                            s["median_max_drawdown"]
                            > old_cash["median_max_drawdown"]
                        ),
                    }
                )

    return pd.DataFrame(rows)


def robustness_summary(df: pd.DataFrame) -> list[dict]:
    rows: list[dict] = []
    for days, group in df.groupby("quarantine_days"):
        row = {
            "quarantine_days": int(days),
            "windows": int(len(group)),
            "beats_low_vol_return": int(group["beats_low_vol_return"].sum()),
            "beats_low_vol_drawdown": int(group["beats_low_vol_drawdown"].sum()),
            "beats_low_vol_both": int(group["beats_low_vol_both"].sum()),
            "beats_frozen_cash_return": int(group["beats_frozen_cash_return"].sum()),
            "beats_frozen_cash_drawdown": int(
                group["beats_frozen_cash_drawdown"].sum()
            ),
            "median_return": float(group["median_return"].median()),
            "median_max_drawdown": float(group["median_max_drawdown"].median()),
            "median_cash_exposure": float(group["cash_exposure"].median()),
            "median_disarmed_exposure": float(group["disarmed_exposure"].median()),
        }
        rows.append(row)
    return rows


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="One cash quarantine per crisis with recovery re-arm."
    )
    p.add_argument(
        "--output-root",
        type=Path,
        default=REPO_ROOT
        / "research_artifacts"
        / "cash_crisis_quarantine_rearm_v2",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    panel, dataset_meta = download_panel(HIST_START, UNTOUCHED_END)
    panel = add_defensive_features(panel)
    signals_by_date, _ = build_pair_context(panel)

    frozen_repro = evaluate_frozen_destination(
        panel,
        signals_by_date,
        start=REPRO_START,
        end=REPRO_END,
        with_daily=False,
    )
    low_repro = summarize_low_vol(
        frozen_repro["baseline"], frozen_repro["low_vol"]
    )
    gate_pass, gate = reproduction_pass(low_repro)

    run_id = f"CASH_CRISIS_QUARANTINE_REARM_V2_{_source_commit()[:12]}"
    run_dir = args.output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    if not gate_pass:
        _write_json(
            run_dir / "summary.json",
            {
                "run_id": run_id,
                "status": "REPRODUCTION_FAILED_V2_NOT_INTERPRETED",
                "reproduction_gate": gate,
                "dataset": dataset_meta,
            },
        )
        print(json.dumps(_json_safe(gate), indent=2, sort_keys=True))
        return 4

    repro, _, _ = evaluate_period(
        panel,
        signals_by_date,
        start=REPRO_START,
        end=REPRO_END,
        with_logs=False,
    )
    opened, _, _ = evaluate_period(
        panel,
        signals_by_date,
        start=UNTOUCHED_START,
        end=UNTOUCHED_END,
        with_logs=False,
    )

    full_start = first_fully_eligible_date(panel)
    full_frozen = evaluate_frozen_destination(
        panel,
        signals_by_date,
        start=full_start,
        end=UNTOUCHED_END,
        with_daily=False,
    )
    full_frozen_summary = summarize_ablation(full_frozen)
    full, episodes, daily = evaluate_period(
        panel,
        signals_by_date,
        start=full_start,
        end=UNTOUCHED_END,
        with_logs=True,
    )

    robustness = build_robustness(
        panel,
        signals_by_date,
        anchor=full_start,
        final_end=UNTOUCHED_END,
    )

    for days, df in repro.items():
        df.to_csv(run_dir / f"reproduction_rearm_{days}d_by_start.csv", index=False)
    for days, df in opened.items():
        df.to_csv(run_dir / f"opened_2026_rearm_{days}d_by_start.csv", index=False)
    for days, df in full.items():
        df.to_csv(run_dir / f"full_history_rearm_{days}d_by_start.csv", index=False)

    episodes.to_csv(run_dir / "full_history_rearm_episode_log.csv", index=False)
    daily.to_csv(run_dir / "full_history_rearm_daily.csv", index=False)
    robustness.to_csv(run_dir / "robustness_windows.csv", index=False)

    report = {
        "run_id": run_id,
        "source_commit_sha": _source_commit(),
        "status": "CASH_CRISIS_QUARANTINE_REARM_V2_EXECUTED",
        "quarantine_days": list(QUARANTINE_DAYS),
        "state_machine": {
            "initial_state": "ARMED",
            "cash_exit_after_fixed_days": True,
            "post_cash_state": "POST_CASH_DISARMED",
            "rearm_condition": f"breadth>={EXIT_BREADTH_MIN} for {CONFIRM_DAYS} closes",
            "recovery_condition_controls_cash_exit": False,
            "new_crisis_requires_fresh_low_streak_after_rearm": True,
            "crisis_detection_while_cash": False,
            "shadow_router_continues": True,
            "cash_yield": CASH_YIELD,
            "transition_cost": TRANSITION_COST,
        },
        "reproduction_gate": gate,
        "reproduction_period": {
            "start": REPRO_START.date().isoformat(),
            "end": REPRO_END.date().isoformat(),
            "variants": summarize_variants(repro),
        },
        "opened_2026_period": {
            "start": UNTOUCHED_START.date().isoformat(),
            "end": UNTOUCHED_END.date().isoformat(),
            "variants": summarize_variants(opened),
            "note": "Already-open history; descriptive only.",
        },
        "full_history": {
            "start": full_start.date().isoformat(),
            "end": UNTOUCHED_END.date().isoformat(),
            "frozen_comparators": full_frozen_summary,
            "variants": summarize_variants(full),
        },
        "robustness": {
            "anchor": full_start.date().isoformat(),
            "window_days": list(ROBUSTNESS_DAYS),
            "summary_by_duration": robustness_summary(robustness),
        },
        "dataset": dataset_meta,
        "interpretation_boundary": [
            "All durations were preregistered and are reported together.",
            "The recovery rule only re-arms future crisis detection; it does not extend cash.",
            "No macro or Fed-liquidity signal is used.",
            "No duration is production-approved from this sample.",
        ],
    }
    _write_json(run_dir / "summary.json", report)

    print(json.dumps(_json_safe(report), indent=2, sort_keys=True))
    print(f"output={run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
