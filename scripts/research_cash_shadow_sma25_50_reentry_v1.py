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
FAST_SMA = 25
SLOW_SMA = 50
ROBUSTNESS_DAYS = (180, 120)


@dataclass(frozen=True)
class SmaReentryResult:
    start_asset: str
    total_return: float
    max_drawdown: float
    actual_transitions: int
    cash_entries: int
    cash_days: int
    sma_reentry_exits: int
    rearm_count: int
    disarmed_days: int
    unresolved_cash_end: bool
    median_cash_wait_days: float
    max_cash_wait_days: float
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


def add_sma25_50_features(panel: pd.DataFrame) -> pd.DataFrame:
    out = panel.copy()
    for asset in ASSETS:
        close = out[f"{asset}_close"].astype(float)
        fast = close.rolling(FAST_SMA, min_periods=FAST_SMA).mean()
        slow = close.rolling(SLOW_SMA, min_periods=SLOW_SMA).mean()
        out[f"{asset}_sma{FAST_SMA}"] = fast
        out[f"{asset}_sma{SLOW_SMA}"] = slow
        out[f"{asset}_sma{FAST_SMA}_{SLOW_SMA}_cross_up"] = (
            (fast > slow) & (fast.shift(1) <= slow.shift(1))
        )
    return out


def _cross_up(row: pd.Series, asset: str) -> bool:
    value = row.get(f"{asset}_sma{FAST_SMA}_{SLOW_SMA}_cross_up", False)
    return bool(value) if pd.notna(value) else False


def run_cash_sma_reentry_backtest(
    panel: pd.DataFrame,
    signals_by_date: dict[pd.Timestamp, list[PairSignal]],
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    start_asset: str,
    episode_log: list[dict] | None = None,
    daily_log: list[dict] | None = None,
) -> SmaReentryResult:
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
    pending_exit_target: str | None = None

    state = "ARMED"
    low_streak = 0
    recovery_streak = 0

    actual_transitions = 0
    cash_entries = 0
    cash_days = 0
    sma_reentry_exits = 0
    rearm_count = 0
    disarmed_days = 0

    current_cash_entry_date: pd.Timestamp | None = None
    cash_wait_days: list[int] = []

    equity: list[float] = []
    dates: list[pd.Timestamp] = []

    for pos, (_, row) in enumerate(window.iterrows()):
        ts = pd.Timestamp(row["timestamp"])

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
                low_streak = 0
                recovery_streak = 0
                actual_transitions += 1
                cash_entries += 1
                current_cash_entry_date = ts

                if episode_log is not None:
                    episode_log.append(
                        {
                            "start_asset": start_asset,
                            "action": "ENTER_CASH",
                            "date": ts.isoformat(),
                            "actual_asset": CASH_LABEL,
                            "shadow_asset": shadow_asset,
                            "breadth": int(row["breadth_sma200"]),
                        }
                    )

            elif action == "EXIT":
                if state != "CASH" or cash_value is None:
                    raise RuntimeError("EXIT requires CASH state")
                if pending_exit_target is None:
                    raise RuntimeError("EXIT missing target")
                if shadow_asset != pending_exit_target:
                    raise RuntimeError(
                        f"Shadow target mismatch at exit: expected {pending_exit_target}, got {shadow_asset}"
                    )

                target = shadow_asset
                qty = cash_value * (1.0 - TRANSITION_COST) / float(
                    row[f"{target}_open"]
                )
                cash_value = None
                actual_asset = target
                state = "POST_CASH_DISARMED"
                low_streak = 0
                recovery_streak = 0
                actual_transitions += 1
                sma_reentry_exits += 1

                wait_days = (
                    int((ts - current_cash_entry_date).days)
                    if current_cash_entry_date is not None
                    else 0
                )
                cash_wait_days.append(wait_days)
                current_cash_entry_date = None
                pending_exit_target = None

                if episode_log is not None:
                    episode_log.append(
                        {
                            "start_asset": start_asset,
                            "action": "EXIT_CASH_ON_SMA25_50_CROSS",
                            "date": ts.isoformat(),
                            "actual_asset": actual_asset,
                            "shadow_asset": shadow_asset,
                            "breadth": int(row["breadth_sma200"]),
                            "cash_wait_days": wait_days,
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
                    "actual_asset": actual_label,
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
        pending_shadow = (
            choose_candidate(candidates, "BASELINE", None) if candidates else None
        )
        prospective_shadow = (
            pending_shadow.to_asset if pending_shadow is not None else shadow_asset
        )

        breadth = int(row["breadth_sma200"])

        if state == "CASH":
            low_streak = 0
            recovery_streak = 0
            if _cross_up(row, prospective_shadow):
                pending_action = "EXIT"
                pending_exit_target = prospective_shadow
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
            pending_action = "ENTER"
            low_streak = 0

    series = pd.Series(equity, index=dates, dtype=float)
    median_wait = (
        float(np.median(cash_wait_days)) if cash_wait_days else float("nan")
    )
    max_wait = float(max(cash_wait_days)) if cash_wait_days else float("nan")

    return SmaReentryResult(
        start_asset=start_asset,
        total_return=float(series.iloc[-1] / series.iloc[0] - 1.0),
        max_drawdown=float((series / series.cummax() - 1.0).min()),
        actual_transitions=actual_transitions,
        cash_entries=cash_entries,
        cash_days=cash_days,
        sma_reentry_exits=sma_reentry_exits,
        rearm_count=rearm_count,
        disarmed_days=disarmed_days,
        unresolved_cash_end=(state == "CASH"),
        median_cash_wait_days=median_wait,
        max_cash_wait_days=max_wait,
        period_days=len(window),
    )


def evaluate_sma_reentry(
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
        r = run_cash_sma_reentry_backtest(
            panel,
            signals_by_date,
            start=start,
            end=end,
            start_asset=asset,
            episode_log=episodes if with_logs else None,
            daily_log=daily if with_logs else None,
        )
        rows.append(r.__dict__)

    return pd.DataFrame(rows), pd.DataFrame(episodes), pd.DataFrame(daily)


def summarize_sma(df: pd.DataFrame) -> dict:
    waits = pd.to_numeric(df["median_cash_wait_days"], errors="coerce").dropna()
    max_waits = pd.to_numeric(df["max_cash_wait_days"], errors="coerce").dropna()
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
        "median_sma_reentry_exits": float(df["sma_reentry_exits"].median()),
        "median_rearm_count": float(df["rearm_count"].median()),
        "median_disarmed_days": float(df["disarmed_days"].median()),
        "median_disarmed_exposure": float(
            (df["disarmed_days"] / df["period_days"]).median()
        ),
        "unresolved_cash_starts": int(df["unresolved_cash_end"].sum()),
        "median_cash_wait_days_across_starts": (
            float(waits.median()) if len(waits) else None
        ),
        "max_cash_wait_days_across_starts": (
            float(max_waits.max()) if len(max_waits) else None
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
            sma, _, _ = evaluate_sma_reentry(
                panel,
                signals_by_date,
                start=start,
                end=end,
                with_logs=False,
            )
            s = summarize_sma(sma)
            low = frozen_summary["LOW_VOL_CRYPTO"]
            old_cash = frozen_summary["CASH_PROXY"]

            rows.append(
                {
                    "window_days": window_days,
                    "window_index": window_index,
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "sma_median_return": s["median_return"],
                    "sma_median_max_drawdown": s["median_max_drawdown"],
                    "sma_worst_return": s["worst_return"],
                    "sma_positive_starts": s["positive_starts"],
                    "sma_cash_exposure": s["median_cash_exposure"],
                    "sma_cash_entries": s["median_cash_entries"],
                    "sma_reentry_exits": s["median_sma_reentry_exits"],
                    "sma_unresolved_cash_starts": s["unresolved_cash_starts"],
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
                        s["median_max_drawdown"] > old_cash["median_max_drawdown"]
                    ),
                }
            )

    return pd.DataFrame(rows)


def robustness_summary(df: pd.DataFrame) -> dict:
    out: dict[str, dict] = {}
    for days, group in df.groupby("window_days"):
        out[f"{int(days)}d"] = {
            "windows": int(len(group)),
            "beats_low_vol_return": int(group["beats_low_vol_return"].sum()),
            "beats_low_vol_drawdown": int(group["beats_low_vol_drawdown"].sum()),
            "beats_low_vol_both": int(group["beats_low_vol_both"].sum()),
            "beats_frozen_cash_return": int(group["beats_frozen_cash_return"].sum()),
            "beats_frozen_cash_drawdown": int(
                group["beats_frozen_cash_drawdown"].sum()
            ),
            "median_sma_return_across_windows": float(
                group["sma_median_return"].median()
            ),
            "median_sma_drawdown_across_windows": float(
                group["sma_median_max_drawdown"].median()
            ),
            "windows_with_any_unresolved_cash": int(
                (group["sma_unresolved_cash_starts"] > 0).sum()
            ),
        }
    return out


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Cash re-entry on shadow-target SMA25 bullish crossover above SMA50."
    )
    p.add_argument(
        "--output-root",
        type=Path,
        default=REPO_ROOT / "research_artifacts" / "cash_shadow_sma25_50_reentry_v1",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    panel, dataset_meta = download_panel(HIST_START, UNTOUCHED_END)
    panel = add_defensive_features(panel)
    panel = add_sma25_50_features(panel)
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

    run_id = f"CASH_SHADOW_SMA25_50_REENTRY_V1_{_source_commit()[:12]}"
    run_dir = args.output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    if not gate_pass:
        _write_json(
            run_dir / "summary.json",
            {
                "run_id": run_id,
                "status": "REPRODUCTION_FAILED_SMA_REENTRY_NOT_INTERPRETED",
                "reproduction_gate": gate,
                "dataset": dataset_meta,
            },
        )
        print(json.dumps(_json_safe(gate), indent=2, sort_keys=True))
        return 4

    repro, _, _ = evaluate_sma_reentry(
        panel,
        signals_by_date,
        start=REPRO_START,
        end=REPRO_END,
        with_logs=False,
    )
    opened, _, _ = evaluate_sma_reentry(
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
    full, episodes, daily = evaluate_sma_reentry(
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

    repro.to_csv(run_dir / "reproduction_sma_reentry_by_start.csv", index=False)
    opened.to_csv(run_dir / "opened_2026_sma_reentry_by_start.csv", index=False)
    full.to_csv(run_dir / "full_history_sma_reentry_by_start.csv", index=False)
    episodes.to_csv(run_dir / "full_history_sma_reentry_episode_log.csv", index=False)
    daily.to_csv(run_dir / "full_history_sma_reentry_daily.csv", index=False)
    robustness.to_csv(run_dir / "robustness_windows.csv", index=False)

    report = {
        "run_id": run_id,
        "source_commit_sha": _source_commit(),
        "status": "CASH_SHADOW_SMA25_50_REENTRY_V1_EXECUTED",
        "state_machine": {
            "cash_entry": f"breadth<={ENTER_BREADTH_MAX} for {CONFIRM_DAYS} closes",
            "cash_exit_signal": "prospective next-open shadow target SMA25 crosses above SMA50",
            "fast_sma": FAST_SMA,
            "slow_sma": SLOW_SMA,
            "cross_confirmation_days": 0,
            "cash_exit_execution": "next open directly into current shadow target",
            "post_exit_state": "POST_CASH_DISARMED",
            "rearm": f"breadth>={EXIT_BREADTH_MIN} for {CONFIRM_DAYS} closes",
            "second_cash_entry_same_crisis_allowed": False,
            "cash_yield": CASH_YIELD,
            "transition_cost": TRANSITION_COST,
        },
        "reproduction_gate": gate,
        "reproduction_period": {
            "start": REPRO_START.date().isoformat(),
            "end": REPRO_END.date().isoformat(),
            "frozen_comparators": summarize_ablation(frozen_repro),
            "sma_reentry": summarize_sma(repro),
        },
        "opened_2026_period": {
            "start": UNTOUCHED_START.date().isoformat(),
            "end": UNTOUCHED_END.date().isoformat(),
            "sma_reentry": summarize_sma(opened),
            "note": "Already-open history; descriptive only.",
        },
        "full_history": {
            "start": full_start.date().isoformat(),
            "end": UNTOUCHED_END.date().isoformat(),
            "frozen_comparators": full_frozen_summary,
            "sma_reentry": summarize_sma(full),
        },
        "robustness": {
            "anchor": full_start.date().isoformat(),
            "window_days": list(ROBUSTNESS_DAYS),
            "summary": robustness_summary(robustness),
        },
        "dataset": dataset_meta,
        "interpretation_boundary": [
            "SMA25/50 lengths were user-specified and preregistered before execution.",
            "Only a fresh post-entry bullish crossover counts.",
            "Macro and Fed-liquidity are not used in this test.",
            "This is retrospective research, not untouched validation.",
            "No automatic execution is authorized.",
        ],
    }

    _write_json(run_dir / "summary.json", report)
    print(json.dumps(_json_safe(report), indent=2, sort_keys=True))
    print(f"output={run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
