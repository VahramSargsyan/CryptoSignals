from __future__ import annotations

import argparse
import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from scripts.research_relative_rotation_graph_intelligence import (
    ASSETS,
    TRANSITION_COST,
    PairSignal,
    build_pair_context,
    choose_candidate,
    download_panel,
    run_backtest,
)

REPO_ROOT = Path(__file__).resolve().parents[1]

HIST_START = pd.Timestamp("2023-05-05", tz="UTC")
REPRO_START = pd.Timestamp("2025-03-29", tz="UTC")
REPRO_END = pd.Timestamp("2026-03-28", tz="UTC")
UNTOUCHED_START = pd.Timestamp("2026-03-29", tz="UTC")
UNTOUCHED_END = pd.Timestamp("2026-09-26", tz="UTC")

SMA_DAYS = 200
ENTER_BREADTH_MAX = 3
EXIT_BREADTH_MIN = 5
CONFIRM_DAYS = 3
VOL_DAYS = 30

EXPECTED_BASE_RETURN = 0.416
EXPECTED_BASE_DD = -0.622
EXPECTED_DEF_RETURN = 0.494
EXPECTED_DEF_DD = -0.471
REPRO_TOLERANCE = 0.05


@dataclass(frozen=True)
class DefenseResult:
    start_asset: str
    total_return: float
    max_drawdown: float
    actual_transitions: int
    defensive_transitions: int
    defensive_days: int
    defensive_entries: int
    period_days: int


def _source_commit() -> str:
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO_ROOT,
        text=True,
    ).strip()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def add_defensive_features(panel: pd.DataFrame) -> pd.DataFrame:
    out = panel.copy()
    breadth_parts = []
    vol_columns = []

    for asset in ASSETS:
        close_col = f"{asset}_close"
        sma_col = f"{asset}_sma{SMA_DAYS}"
        vol_col = f"{asset}_vol{VOL_DAYS}"
        out[sma_col] = out[close_col].rolling(SMA_DAYS, min_periods=SMA_DAYS).mean()
        out[vol_col] = out[close_col].pct_change().rolling(VOL_DAYS, min_periods=VOL_DAYS).std()
        breadth_parts.append((out[close_col] > out[sma_col]).astype(int))
        vol_columns.append(vol_col)

    out["breadth_sma200"] = sum(breadth_parts)
    out["lowest_vol_asset"] = out[vol_columns].idxmin(axis=1).str.replace(
        f"_vol{VOL_DAYS}", "", regex=False
    )
    out.loc[out[vol_columns].isna().any(axis=1), "lowest_vol_asset"] = pd.NA
    return out


def run_defensive_backtest(
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
    actual_asset = start_asset
    qty = 1.0 / float(first[f"{actual_asset}_open"])

    pending_shadow: PairSignal | None = None
    pending_defense_action: tuple[str, str | None] | None = None

    defensive = False
    defensive_asset: str | None = None
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
            action, target = pending_defense_action
            pending_defense_action = None

            if action == "ENTER":
                if target is None:
                    raise RuntimeError("ENTER requires defensive target")
                if actual_asset != target:
                    value = qty * float(row[f"{actual_asset}_open"])
                    qty = value * (1.0 - TRANSITION_COST) / float(row[f"{target}_open"])
                    actual_asset = target
                    actual_transitions += 1
                    defensive_transitions += 1
                defensive = True
                defensive_asset = target
                defensive_entries += 1
                if episode_log is not None:
                    episode_log.append(
                        {
                            "start_asset": start_asset,
                            "action": "ENTER",
                            "date": ts.isoformat(),
                            "defensive_asset": target,
                            "shadow_asset": shadow_asset,
                            "breadth_trigger": int(row["breadth_sma200"]),
                        }
                    )

            elif action == "EXIT":
                target = shadow_asset
                if actual_asset != target:
                    value = qty * float(row[f"{actual_asset}_open"])
                    qty = value * (1.0 - TRANSITION_COST) / float(row[f"{target}_open"])
                    actual_asset = target
                    actual_transitions += 1
                    defensive_transitions += 1
                if episode_log is not None:
                    episode_log.append(
                        {
                            "start_asset": start_asset,
                            "action": "EXIT",
                            "date": ts.isoformat(),
                            "defensive_asset": defensive_asset,
                            "shadow_asset": shadow_asset,
                            "breadth_trigger": int(row["breadth_sma200"]),
                        }
                    )
                defensive = False
                defensive_asset = None
            else:
                raise RuntimeError(f"Unknown defense action: {action}")

        elif not defensive and shadow_asset != previous_shadow:
            if actual_asset != shadow_asset:
                value = qty * float(row[f"{actual_asset}_open"])
                qty = value * (1.0 - TRANSITION_COST) / float(row[f"{shadow_asset}_open"])
                actual_asset = shadow_asset
                actual_transitions += 1

        value_close = qty * float(row[f"{actual_asset}_close"])
        equity.append(value_close)
        dates.append(ts)
        if defensive:
            defensive_days += 1

        if daily_log is not None:
            daily_log.append(
                {
                    "start_asset": start_asset,
                    "date": ts.isoformat(),
                    "equity": value_close,
                    "shadow_asset": shadow_asset,
                    "actual_asset": actual_asset,
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
            target = row["lowest_vol_asset"]
            if pd.isna(target):
                raise RuntimeError(f"Missing VOL{VOL_DAYS} defensive asset at {ts}")
            pending_defense_action = ("ENTER", str(target))
            low_streak = 0
            high_streak = 0
        elif defensive and pending_defense_action is None and high_streak >= CONFIRM_DAYS:
            pending_defense_action = ("EXIT", None)
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
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    base_rows = []
    defense_rows = []
    episode_rows: list[dict] = []
    daily_rows: list[dict] = []

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
        defense = run_defensive_backtest(
            panel,
            signals_by_date,
            start=start,
            end=end,
            start_asset=asset,
            episode_log=episode_rows,
            daily_log=daily_rows,
        )
        defense_rows.append(defense.__dict__)

    return (
        pd.DataFrame(base_rows),
        pd.DataFrame(defense_rows),
        pd.DataFrame(episode_rows),
        pd.DataFrame(daily_rows),
    )


def summarize(base: pd.DataFrame, defense: pd.DataFrame) -> dict:
    return {
        "BASELINE": {
            "median_return": float(base["total_return"].median()),
            "worst_return": float(base["total_return"].min()),
            "median_max_drawdown": float(base["max_drawdown"].median()),
            "positive_starts": int((base["total_return"] > 0).sum()),
            "median_transitions": float(base["transitions"].median()),
        },
        "DEFENSIVE_LOW_VOL_CRYPTO": {
            "median_return": float(defense["total_return"].median()),
            "worst_return": float(defense["total_return"].min()),
            "median_max_drawdown": float(defense["max_drawdown"].median()),
            "positive_starts": int((defense["total_return"] > 0).sum()),
            "median_actual_transitions": float(defense["actual_transitions"].median()),
            "median_defensive_transitions": float(defense["defensive_transitions"].median()),
            "median_defensive_entries": float(defense["defensive_entries"].median()),
            "median_defensive_days": float(defense["defensive_days"].median()),
            "median_defensive_exposure": float(
                (defense["defensive_days"] / defense["period_days"]).median()
            ),
        },
    }


def reproduction_pass(summary: dict) -> tuple[bool, dict]:
    observed = {
        "base_return": summary["BASELINE"]["median_return"],
        "base_dd": summary["BASELINE"]["median_max_drawdown"],
        "def_return": summary["DEFENSIVE_LOW_VOL_CRYPTO"]["median_return"],
        "def_dd": summary["DEFENSIVE_LOW_VOL_CRYPTO"]["median_max_drawdown"],
    }
    expected = {
        "base_return": EXPECTED_BASE_RETURN,
        "base_dd": EXPECTED_BASE_DD,
        "def_return": EXPECTED_DEF_RETURN,
        "def_dd": EXPECTED_DEF_DD,
    }
    differences = {k: observed[k] - expected[k] for k in observed}
    passed = all(abs(value) <= REPRO_TOLERANCE for value in differences.values())
    return passed, {
        "expected": expected,
        "observed": observed,
        "differences": differences,
        "tolerance": REPRO_TOLERANCE,
        "pass": passed,
    }


def monthly_median_returns(daily: pd.DataFrame) -> pd.DataFrame:
    if daily.empty:
        return pd.DataFrame()
    x = daily.copy()
    x["date"] = pd.to_datetime(x["date"], utc=True)
    x["month"] = x["date"].dt.to_period("M").astype(str)
    x["month_first"] = x.groupby(["start_asset", "month"])["equity"].transform("first")
    x["month_last"] = x.groupby(["start_asset", "month"])["equity"].transform("last")
    one = x.drop_duplicates(["start_asset", "month"])[["start_asset", "month", "month_first", "month_last"]]
    one["monthly_return"] = one["month_last"] / one["month_first"] - 1.0
    return (
        one.groupby("month", as_index=False)["monthly_return"]
        .median()
        .rename(columns={"monthly_return": "defense_median_monthly_return"})
    )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Frozen untouched validation for DEFENSIVE_LOW_VOL_CRYPTO."
    )
    p.add_argument(
        "--output-root",
        type=Path,
        default=REPO_ROOT / "research_artifacts" / "defensive_low_vol_untouched",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    # Phase 1: reproduce only the already-seen historical candidate.
    repro_panel, repro_meta = download_panel(HIST_START, REPRO_END)
    repro_panel = add_defensive_features(repro_panel)
    repro_signals, _ = build_pair_context(repro_panel)
    repro_base, repro_def, repro_episodes, repro_daily = evaluate_period(
        repro_panel,
        repro_signals,
        start=REPRO_START,
        end=REPRO_END,
    )
    repro_summary = summarize(repro_base, repro_def)
    gate_pass, gate = reproduction_pass(repro_summary)

    run_id = f"DEF_LOW_VOL_UNTOUCHED_V1_{_source_commit()[:12]}"
    run_dir = args.output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    repro_base.to_csv(run_dir / "reproduction_baseline_by_start.csv", index=False)
    repro_def.to_csv(run_dir / "reproduction_defense_by_start.csv", index=False)
    repro_episodes.to_csv(run_dir / "reproduction_defense_episodes.csv", index=False)

    print("reproduction:")
    print(json.dumps(repro_summary, indent=2))
    print("gate:")
    print(json.dumps(gate, indent=2))

    if not gate_pass:
        _write_json(
            run_dir / "summary.json",
            {
                "run_id": run_id,
                "source_commit_sha": _source_commit(),
                "status": "REPRODUCTION_FAILED_UNTOUCHED_NOT_OPENED",
                "reproduction": repro_summary,
                "reproduction_gate": gate,
                "reproduction_dataset": repro_meta,
            },
        )
        print("status=REPRODUCTION_FAILED_UNTOUCHED_NOT_OPENED")
        return 4

    # Phase 2: gate passed. Only now retrieve/open the untouched period.
    unseen_panel, unseen_meta = download_panel(HIST_START, UNTOUCHED_END)
    unseen_panel = add_defensive_features(unseen_panel)
    unseen_signals, _ = build_pair_context(unseen_panel)
    unseen_base, unseen_def, unseen_episodes, unseen_daily = evaluate_period(
        unseen_panel,
        unseen_signals,
        start=UNTOUCHED_START,
        end=UNTOUCHED_END,
    )
    unseen_summary = summarize(unseen_base, unseen_def)

    unseen_base.to_csv(run_dir / "untouched_baseline_by_start.csv", index=False)
    unseen_def.to_csv(run_dir / "untouched_defense_by_start.csv", index=False)
    unseen_episodes.to_csv(run_dir / "untouched_defense_episodes.csv", index=False)
    unseen_daily.to_csv(run_dir / "untouched_defense_daily.csv", index=False)
    monthly_median_returns(unseen_daily).to_csv(
        run_dir / "untouched_defense_monthly_median.csv",
        index=False,
    )

    report = {
        "run_id": run_id,
        "source_commit_sha": _source_commit(),
        "status": "UNTOUCHED_VALIDATION_EXECUTED",
        "strategy": "DEFENSIVE_LOW_VOL_CRYPTO_SMA200_BREADTH_3_5_CONFIRM3_VOL30",
        "frozen_parameters": {
            "universe": list(ASSETS),
            "sma_days": SMA_DAYS,
            "enter_breadth_max": ENTER_BREADTH_MAX,
            "exit_breadth_min": EXIT_BREADTH_MIN,
            "confirmation_days": CONFIRM_DAYS,
            "volatility_days": VOL_DAYS,
            "transition_cost": TRANSITION_COST,
            "defensive_asset_reselected_while_active": False,
            "relative_router_continues_in_shadow": True,
            "window_state_reset": True,
        },
        "reproduction": repro_summary,
        "reproduction_gate": gate,
        "untouched_period": {
            "start": UNTOUCHED_START.date().isoformat(),
            "end": UNTOUCHED_END.date().isoformat(),
            "summary": unseen_summary,
        },
        "reproduction_dataset": repro_meta,
        "untouched_dataset": unseen_meta,
        "validation_note": (
            "No data after 2026-03-28 was retrieved by this runner until the historical reproduction gate passed."
        ),
    }
    _write_json(run_dir / "summary.json", report)

    print("untouched:")
    print(json.dumps(unseen_summary, indent=2))
    print("episodes:")
    if unseen_episodes.empty:
        print("NONE")
    else:
        print(unseen_episodes.to_string(index=False))
    print(f"output={run_dir}")
    print("status=UNTOUCHED_VALIDATION_EXECUTED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
