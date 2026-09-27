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

from scripts.research_cash_defense_destination_ablation_v1 import (
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
DEFENSIVE_ASSET = "TRX"
ROBUSTNESS_DAYS = (180, 120)

OLD_V3_REFERENCE = {
    "period": "2025-03-29 -> 2026-03-28",
    "median_return": -0.0278,
    "median_max_drawdown": -0.5657,
    "median_defensive_transitions": 11.5,
    "verdict": "DEVELOPMENT_FAIL_DO_NOT_PROMOTE",
}


@dataclass(frozen=True)
class V4Result:
    start_asset: str
    total_return: float
    max_drawdown: float
    actual_transitions: int
    defensive_entries: int
    defensive_days: int
    router_reactivation_exits: int
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


def run_one_trx_per_crisis_backtest(
    panel: pd.DataFrame,
    signals_by_date: dict[pd.Timestamp, list[PairSignal]],
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    start_asset: str,
    episode_log: list[dict] | None = None,
    daily_log: list[dict] | None = None,
) -> V4Result:
    window = panel[(panel["timestamp"] >= start) & (panel["timestamp"] <= end)].copy()
    if window.empty:
        raise ValueError("Empty evaluation window")

    first = window.iloc[0]
    shadow_asset = start_asset
    actual_asset = start_asset
    qty = 1.0 / float(first[f"{actual_asset}_open"])

    pending_shadow: PairSignal | None = None
    pending_entry = False
    pending_router_exit = False

    state = "ARMED"
    low_streak = 0
    recovery_streak = 0

    actual_transitions = 0
    defensive_entries = 0
    defensive_days = 0
    router_reactivation_exits = 0
    rearm_count = 0
    disarmed_days = 0

    equity: list[float] = []
    dates: list[pd.Timestamp] = []

    for pos, (_, row) in enumerate(window.iterrows()):
        ts = row["timestamp"]

        previous_shadow = shadow_asset
        shadow_transitioned = False
        if pending_shadow is not None:
            shadow_asset = pending_shadow.to_asset
            pending_shadow = None
            shadow_transitioned = shadow_asset != previous_shadow

        if pending_entry:
            pending_entry = False
            target = DEFENSIVE_ASSET
            if actual_asset != target:
                value = qty * float(row[f"{actual_asset}_open"])
                qty = value * (1.0 - TRANSITION_COST) / float(row[f"{target}_open"])
                actual_asset = target
                actual_transitions += 1

            state = "TRX_DEFENSE"
            defensive_entries += 1
            pending_router_exit = False
            low_streak = 0
            recovery_streak = 0

            if episode_log is not None:
                episode_log.append(
                    {
                        "start_asset": start_asset,
                        "action": "ENTER_TRX_DEFENSE",
                        "date": ts.isoformat(),
                        "actual_asset": actual_asset,
                        "shadow_asset": shadow_asset,
                        "breadth": int(row["breadth_sma200"]),
                    }
                )

        elif state == "TRX_DEFENSE" and pending_router_exit and shadow_transitioned:
            pending_router_exit = False
            target = shadow_asset

            if actual_asset != target:
                value = qty * float(row[f"{actual_asset}_open"])
                qty = value * (1.0 - TRANSITION_COST) / float(row[f"{target}_open"])
                actual_asset = target
                actual_transitions += 1

            state = "POST_TRX_DISARMED"
            router_reactivation_exits += 1
            low_streak = 0
            recovery_streak = 0

            if episode_log is not None:
                episode_log.append(
                    {
                        "start_asset": start_asset,
                        "action": "EXIT_TRX_ON_ROUTER_SIGNAL",
                        "date": ts.isoformat(),
                        "actual_asset": actual_asset,
                        "shadow_asset": shadow_asset,
                        "breadth": int(row["breadth_sma200"]),
                    }
                )

        elif state != "TRX_DEFENSE" and shadow_transitioned:
            if actual_asset != shadow_asset:
                value = qty * float(row[f"{actual_asset}_open"])
                qty = value * (1.0 - TRANSITION_COST) / float(
                    row[f"{shadow_asset}_open"]
                )
                actual_asset = shadow_asset
                actual_transitions += 1

        value_close = qty * float(row[f"{actual_asset}_close"])
        equity.append(value_close)
        dates.append(ts)

        if state == "TRX_DEFENSE":
            defensive_days += 1
        elif state == "POST_TRX_DISARMED":
            disarmed_days += 1

        if daily_log is not None:
            daily_log.append(
                {
                    "start_asset": start_asset,
                    "date": ts.isoformat(),
                    "equity": value_close,
                    "actual_asset": actual_asset,
                    "shadow_asset": shadow_asset,
                    "state": state,
                    "breadth": int(row["breadth_sma200"]),
                    "low_streak": low_streak,
                    "recovery_streak": recovery_streak,
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
            if state == "TRX_DEFENSE":
                pending_router_exit = True

        breadth = int(row["breadth_sma200"])

        if state == "TRX_DEFENSE":
            # No breadth-based exit and no crisis/recovery streaks while TRX is active.
            low_streak = 0
            recovery_streak = 0
            continue

        if state == "POST_TRX_DISARMED":
            # Normal router is active, but another TRX defense is forbidden
            # until the old crisis has formally recovered.
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
                            "action": "REARM_AFTER_RECOVERY",
                            "date": ts.isoformat(),
                            "actual_asset": actual_asset,
                            "shadow_asset": shadow_asset,
                            "breadth": breadth,
                        }
                    )
            continue

        if state != "ARMED":
            raise RuntimeError(f"Unknown state: {state}")

        recovery_streak = 0
        if breadth <= ENTER_BREADTH_MAX:
            low_streak += 1
        else:
            low_streak = 0

        if low_streak >= CONFIRM_DAYS:
            pending_entry = True
            low_streak = 0

    series = pd.Series(equity, index=dates, dtype=float)
    return V4Result(
        start_asset=start_asset,
        total_return=float(series.iloc[-1] / series.iloc[0] - 1.0),
        max_drawdown=float((series / series.cummax() - 1.0).min()),
        actual_transitions=actual_transitions,
        defensive_entries=defensive_entries,
        defensive_days=defensive_days,
        router_reactivation_exits=router_reactivation_exits,
        rearm_count=rearm_count,
        disarmed_days=disarmed_days,
        period_days=len(window),
    )


def evaluate_v4(
    panel: pd.DataFrame,
    signals_by_date: dict[pd.Timestamp, list[PairSignal]],
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    with_logs: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rows: list[dict] = []
    episodes: list[dict] = []
    daily: list[dict] = []

    for asset in ASSETS:
        result = run_one_trx_per_crisis_backtest(
            panel,
            signals_by_date,
            start=start,
            end=end,
            start_asset=asset,
            episode_log=episodes if with_logs else None,
            daily_log=daily if with_logs else None,
        )
        rows.append(result.__dict__)

    return pd.DataFrame(rows), pd.DataFrame(episodes), pd.DataFrame(daily)


def summarize_v4(df: pd.DataFrame) -> dict:
    return {
        "median_return": float(df["total_return"].median()),
        "worst_return": float(df["total_return"].min()),
        "median_max_drawdown": float(df["max_drawdown"].median()),
        "worst_max_drawdown": float(df["max_drawdown"].min()),
        "positive_starts": int((df["total_return"] > 0).sum()),
        "median_actual_transitions": float(df["actual_transitions"].median()),
        "median_defensive_entries": float(df["defensive_entries"].median()),
        "median_defensive_days": float(df["defensive_days"].median()),
        "median_defensive_exposure": float(
            (df["defensive_days"] / df["period_days"]).median()
        ),
        "median_router_reactivation_exits": float(
            df["router_reactivation_exits"].median()
        ),
        "median_rearm_count": float(df["rearm_count"].median()),
        "median_disarmed_days": float(df["disarmed_days"].median()),
        "median_disarmed_exposure": float(
            (df["disarmed_days"] / df["period_days"]).median()
        ),
    }


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
            v4, _, _ = evaluate_v4(
                panel,
                signals_by_date,
                start=start,
                end=end,
                with_logs=False,
            )
            v4s = summarize_v4(v4)
            low = frozen_summary["LOW_VOL_CRYPTO"]
            base = frozen_summary["BASELINE"]

            rows.append(
                {
                    "window_days": window_days,
                    "window_index": window_index,
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "baseline_median_return": base["median_return"],
                    "baseline_median_max_drawdown": base["median_max_drawdown"],
                    "low_vol_median_return": low["median_return"],
                    "low_vol_median_max_drawdown": low["median_max_drawdown"],
                    "v4_median_return": v4s["median_return"],
                    "v4_median_max_drawdown": v4s["median_max_drawdown"],
                    "v4_actual_transitions": v4s["median_actual_transitions"],
                    "v4_defensive_entries": v4s["median_defensive_entries"],
                    "v4_defensive_exposure": v4s["median_defensive_exposure"],
                    "v4_disarmed_exposure": v4s["median_disarmed_exposure"],
                    "beats_low_vol_return": v4s["median_return"] > low["median_return"],
                    "beats_low_vol_drawdown": (
                        v4s["median_max_drawdown"] > low["median_max_drawdown"]
                    ),
                    "beats_low_vol_both": (
                        v4s["median_return"] > low["median_return"]
                        and v4s["median_max_drawdown"] > low["median_max_drawdown"]
                    ),
                }
            )

    return pd.DataFrame(rows)


def robustness_summary(df: pd.DataFrame) -> dict:
    result: dict[str, dict] = {}
    for days, group in df.groupby("window_days"):
        key = f"{int(days)}d"
        result[key] = {
            "windows": int(len(group)),
            "beats_low_vol_return": int(group["beats_low_vol_return"].sum()),
            "beats_low_vol_drawdown": int(group["beats_low_vol_drawdown"].sum()),
            "beats_low_vol_both": int(group["beats_low_vol_both"].sum()),
            "median_v4_return_across_windows": float(
                group["v4_median_return"].median()
            ),
            "median_v4_drawdown_across_windows": float(
                group["v4_median_max_drawdown"].median()
            ),
            "median_v4_actual_transitions": float(
                group["v4_actual_transitions"].median()
            ),
        }
    return result


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="One TRX defensive reaction per crisis with router-reactivation exit."
    )
    p.add_argument(
        "--output-root",
        type=Path,
        default=REPO_ROOT / "research_artifacts" / "one_trx_defense_per_crisis_v4",
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

    run_id = f"ONE_TRX_DEFENSE_PER_CRISIS_V4_{_source_commit()[:12]}"
    run_dir = args.output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    if not gate_pass:
        _write_json(
            run_dir / "summary.json",
            {
                "run_id": run_id,
                "status": "REPRODUCTION_FAILED_V4_NOT_INTERPRETED",
                "reproduction_gate": gate,
                "dataset": dataset_meta,
            },
        )
        print(json.dumps(_json_safe(gate), indent=2, sort_keys=True))
        return 4

    repro_v4, _, _ = evaluate_v4(
        panel,
        signals_by_date,
        start=REPRO_START,
        end=REPRO_END,
        with_logs=False,
    )
    opened_v4, _, _ = evaluate_v4(
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
    full_v4, episodes, daily = evaluate_v4(
        panel,
        signals_by_date,
        start=full_start,
        end=UNTOUCHED_END,
        with_logs=True,
    )
    full_v4_summary = summarize_v4(full_v4)

    robustness = build_robustness(
        panel,
        signals_by_date,
        anchor=full_start,
        final_end=UNTOUCHED_END,
    )

    repro_v4.to_csv(run_dir / "reproduction_v4_by_start.csv", index=False)
    opened_v4.to_csv(run_dir / "opened_2026_v4_by_start.csv", index=False)
    full_v4.to_csv(run_dir / "full_history_v4_by_start.csv", index=False)
    episodes.to_csv(run_dir / "full_history_v4_episode_log.csv", index=False)
    daily.to_csv(run_dir / "full_history_v4_daily.csv", index=False)
    robustness.to_csv(run_dir / "robustness_windows.csv", index=False)

    report = {
        "run_id": run_id,
        "source_commit_sha": _source_commit(),
        "status": "ONE_TRX_DEFENSE_PER_CRISIS_V4_EXECUTED",
        "state_machine": {
            "defensive_asset": DEFENSIVE_ASSET,
            "entry": f"breadth<={ENTER_BREADTH_MAX} for {CONFIRM_DAYS} closes",
            "defensive_exit": "first NEW confirmed shadow-router transition after entry",
            "direct_actual_execution": "TRX -> new shadow target",
            "post_exit_state": "POST_TRX_DISARMED",
            "rearm": f"breadth>={EXIT_BREADTH_MIN} for {CONFIRM_DAYS} closes",
            "fresh_low_streak_required_after_rearm": True,
            "second_trx_entry_same_crisis_allowed": False,
            "transition_cost": TRANSITION_COST,
        },
        "reproduction_gate": gate,
        "old_v3_reference": OLD_V3_REFERENCE,
        "reproduction_period": {
            "start": REPRO_START.date().isoformat(),
            "end": REPRO_END.date().isoformat(),
            "frozen_comparators": summarize_ablation(frozen_repro),
            "v4": summarize_v4(repro_v4),
        },
        "opened_2026_period": {
            "start": UNTOUCHED_START.date().isoformat(),
            "end": UNTOUCHED_END.date().isoformat(),
            "v4": summarize_v4(opened_v4),
            "note": "Already-open history; descriptive only.",
        },
        "full_history": {
            "start": full_start.date().isoformat(),
            "end": UNTOUCHED_END.date().isoformat(),
            "frozen_comparators": full_frozen_summary,
            "v4": full_v4_summary,
        },
        "robustness": {
            "anchor": full_start.date().isoformat(),
            "window_days": list(ROBUSTNESS_DAYS),
            "summary": robustness_summary(robustness),
        },
        "dataset": dataset_meta,
        "interpretation_boundary": [
            "This is retrospective research, not untouched validation.",
            "No numeric threshold was added or tuned.",
            "The only semantic change versus old V3 is one TRX defense per crisis.",
            "No production or paper-live behavior is changed.",
        ],
    }

    _write_json(run_dir / "summary.json", report)
    print(json.dumps(_json_safe(report), indent=2, sort_keys=True))
    print(f"output={run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
