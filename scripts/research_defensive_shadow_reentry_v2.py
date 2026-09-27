from __future__ import annotations

import json
import os
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd

from scripts.research_defensive_low_vol_untouched import (
    CONFIRM_DAYS,
    ENTER_BREADTH_MAX,
    HIST_START,
    add_defensive_features,
    run_defensive_backtest,
)
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
END = pd.Timestamp("2026-03-28", tz="UTC")

YEAR_WINDOW = ("2025-03-29", "2026-03-28")
WINDOWS_180D = (
    ("2023-10-31", "2024-04-27"),
    ("2024-04-28", "2024-10-24"),
    ("2024-10-25", "2025-04-22"),
    ("2025-04-23", "2025-10-19"),
    ("2025-10-20", "2026-03-28"),
)
WINDOWS_120D = (
    ("2023-10-31", "2024-02-27"),
    ("2024-02-28", "2024-06-26"),
    ("2024-06-27", "2024-10-24"),
    ("2024-10-25", "2025-02-21"),
    ("2025-02-22", "2025-06-21"),
    ("2025-06-22", "2025-10-19"),
    ("2025-10-20", "2026-02-16"),
)

WEAK_180D = {
    ("2024-04-28", "2024-10-24"),
    ("2025-10-20", "2026-03-28"),
}
NON_WEAK_180D = {
    ("2023-10-31", "2024-04-27"),
    ("2024-10-25", "2025-04-22"),
    ("2025-04-23", "2025-10-19"),
}


@dataclass(frozen=True)
class CandidateResult:
    start_asset: str
    total_return: float
    max_drawdown: float
    actual_transitions: int
    defensive_transitions: int
    defensive_days: int
    defensive_entries: int
    period_days: int


def _utc(value: str) -> pd.Timestamp:
    return pd.Timestamp(value, tz="UTC")


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


def update_shadow_recovery_streak(
    *,
    previous_target: str | None,
    current_target: str,
    target_above_sma200: bool,
    streak: int,
) -> tuple[str, int]:
    if previous_target != current_target:
        streak = 0
    if target_above_sma200:
        streak += 1
    else:
        streak = 0
    return current_target, streak


def run_shadow_reentry_backtest(
    panel: pd.DataFrame,
    signals_by_date: dict[pd.Timestamp, list[PairSignal]],
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    start_asset: str,
    episode_log: list[dict] | None = None,
) -> CandidateResult:
    window = panel[(panel["timestamp"] >= start) & (panel["timestamp"] <= end)].copy()
    if window.empty:
        raise ValueError("Empty evaluation window")

    first = window.iloc[0]
    shadow_asset = start_asset
    actual_asset = start_asset
    qty = 1.0 / float(first[f"{actual_asset}_open"])

    pending_shadow: PairSignal | None = None
    pending_action: tuple[str, str | None] | None = None

    defensive = False
    defensive_asset: str | None = None
    low_streak = 0
    recovery_target: str | None = None
    recovery_streak = 0

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

        if pending_action is not None:
            action, target = pending_action
            pending_action = None

            if action == "ENTER":
                if target is None:
                    raise RuntimeError("ENTER requires target")
                if actual_asset != target:
                    value = qty * float(row[f"{actual_asset}_open"])
                    qty = value * (1.0 - TRANSITION_COST) / float(row[f"{target}_open"])
                    actual_asset = target
                    actual_transitions += 1
                    defensive_transitions += 1
                defensive = True
                defensive_asset = target
                defensive_entries += 1
                recovery_target = None
                recovery_streak = 0
                if episode_log is not None:
                    episode_log.append(
                        {
                            "start_asset": start_asset,
                            "action": "ENTER",
                            "date": ts.isoformat(),
                            "defensive_asset": target,
                            "shadow_asset": shadow_asset,
                            "breadth": int(row["breadth_sma200"]),
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
                            "breadth": int(row["breadth_sma200"]),
                        }
                    )
                defensive = False
                defensive_asset = None
                recovery_target = None
                recovery_streak = 0
                low_streak = 0
            else:
                raise RuntimeError(f"Unknown action {action}")

        elif not defensive and shadow_asset != previous_shadow and actual_asset != shadow_asset:
            value = qty * float(row[f"{actual_asset}_open"])
            qty = value * (1.0 - TRANSITION_COST) / float(row[f"{shadow_asset}_open"])
            actual_asset = shadow_asset
            actual_transitions += 1

        value_close = qty * float(row[f"{actual_asset}_close"])
        equity.append(value_close)
        dates.append(ts)

        if defensive:
            defensive_days += 1

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

        if not defensive and pending_action is None and low_streak >= CONFIRM_DAYS:
            target = row["lowest_vol_asset"]
            if pd.isna(target):
                raise RuntimeError(f"Missing defensive asset at {ts}")
            pending_action = ("ENTER", str(target))
            low_streak = 0
            continue

        if defensive and pending_action is None:
            target_close = float(row[f"{shadow_asset}_close"])
            target_sma = row[f"{shadow_asset}_sma200"]
            above = pd.notna(target_sma) and target_close > float(target_sma)
            recovery_target, recovery_streak = update_shadow_recovery_streak(
                previous_target=recovery_target,
                current_target=shadow_asset,
                target_above_sma200=above,
                streak=recovery_streak,
            )
            if recovery_streak >= CONFIRM_DAYS:
                pending_action = ("EXIT", None)
                recovery_streak = 0

    series = pd.Series(equity, index=dates, dtype=float)
    return CandidateResult(
        start_asset=start_asset,
        total_return=float(series.iloc[-1] / series.iloc[0] - 1.0),
        max_drawdown=float((series / series.cummax() - 1.0).min()),
        actual_transitions=actual_transitions,
        defensive_transitions=defensive_transitions,
        defensive_days=defensive_days,
        defensive_entries=defensive_entries,
        period_days=len(window),
    )


def summarize_variant(df: pd.DataFrame, *, base: bool = False) -> dict:
    result = {
        "median_return": float(df["total_return"].median()),
        "worst_return": float(df["total_return"].min()),
        "median_max_drawdown": float(df["max_drawdown"].median()),
        "positive_starts": int((df["total_return"] > 0).sum()),
    }
    if base:
        result["median_transitions"] = float(df["transitions"].median())
        result["median_defensive_exposure"] = 0.0
        result["median_defensive_transitions"] = 0.0
    else:
        result["median_actual_transitions"] = float(df["actual_transitions"].median())
        result["median_defensive_transitions"] = float(df["defensive_transitions"].median())
        result["median_defensive_exposure"] = float(
            (df["defensive_days"] / df["period_days"]).median()
        )
    return result


def evaluate_window(
    panel: pd.DataFrame,
    signals_by_date: dict[pd.Timestamp, list[PairSignal]],
    *,
    start_text: str,
    end_text: str,
    window_type: str,
    episode_rows: list[dict],
) -> list[dict]:
    start = _utc(start_text)
    end = _utc(end_text)

    base_rows = []
    original_rows = []
    v2_rows = []

    for asset in ASSETS:
        base_rows.append(
            asdict(
                run_backtest(
                    panel,
                    signals_by_date,
                    {},
                    start=start,
                    end=end,
                    start_asset=asset,
                    variant="BASELINE",
                )
            )
        )
        original_rows.append(
            asdict(
                run_defensive_backtest(
                    panel,
                    signals_by_date,
                    start=start,
                    end=end,
                    start_asset=asset,
                )
            )
        )
        local_episodes: list[dict] = []
        v2_rows.append(
            asdict(
                run_shadow_reentry_backtest(
                    panel,
                    signals_by_date,
                    start=start,
                    end=end,
                    start_asset=asset,
                    episode_log=local_episodes,
                )
            )
        )
        for row in local_episodes:
            row["window_type"] = window_type
            row["period_start"] = start_text
            row["period_end"] = end_text
            episode_rows.append(row)

    return [
        {
            "window_type": window_type,
            "period_start": start_text,
            "period_end": end_text,
            "variant": "BASELINE",
            **summarize_variant(pd.DataFrame(base_rows), base=True),
        },
        {
            "window_type": window_type,
            "period_start": start_text,
            "period_end": end_text,
            "variant": "ORIGINAL_BREADTH_3_5",
            **summarize_variant(pd.DataFrame(original_rows)),
        },
        {
            "window_type": window_type,
            "period_start": start_text,
            "period_end": end_text,
            "variant": "V2_SHADOW_SMA200_CONFIRM3",
            **summarize_variant(pd.DataFrame(v2_rows)),
        },
    ]


def evaluate_gates(summary: pd.DataFrame) -> dict:
    d180 = summary[summary["window_type"] == "180D"].copy()
    original = d180[d180["variant"] == "ORIGINAL_BREADTH_3_5"].set_index(
        ["period_start", "period_end"]
    )
    v2 = d180[d180["variant"] == "V2_SHADOW_SMA200_CONFIRM3"].set_index(
        ["period_start", "period_end"]
    )

    protection = {}
    for key in sorted(WEAK_180D):
        original_dd = float(original.loc[key, "median_max_drawdown"])
        v2_dd = float(v2.loc[key, "median_max_drawdown"])
        deterioration = abs(v2_dd) - abs(original_dd)
        protection["|".join(key)] = {
            "original_dd": original_dd,
            "v2_dd": v2_dd,
            "drawdown_deterioration": deterioration,
            "pass": deterioration <= 0.10,
        }

    opportunity_wins = 0
    exposure_wins = 0
    for key in NON_WEAK_180D:
        if float(v2.loc[key, "median_return"]) > float(original.loc[key, "median_return"]):
            opportunity_wins += 1
        if float(v2.loc[key, "median_defensive_exposure"]) < float(
            original.loc[key, "median_defensive_exposure"]
        ):
            exposure_wins += 1

    churn_pass = bool((v2["median_defensive_transitions"] <= 6).all())

    catastrophic = {}
    catastrophic_pass = True
    for key in original.index:
        orig_ret = float(original.loc[key, "median_return"])
        v2_ret = float(v2.loc[key, "median_return"])
        if orig_ret > 0 and v2_ret < -0.10:
            catastrophic_pass = False
        catastrophic["|".join(key)] = {
            "original_return": orig_ret,
            "v2_return": v2_ret,
            "pass": not (orig_ret > 0 and v2_ret < -0.10),
        }

    gates = {
        "protection_retention": {
            "details": protection,
            "pass": all(x["pass"] for x in protection.values()),
        },
        "opportunity_cost_improvement": {
            "wins_required": 2,
            "wins_observed": opportunity_wins,
            "pass": opportunity_wins >= 2,
        },
        "exposure_improvement": {
            "wins_required": 2,
            "wins_observed": exposure_wins,
            "pass": exposure_wins >= 2,
        },
        "churn_control": {
            "max_median_defensive_transitions_per_180d": 6,
            "pass": churn_pass,
        },
        "no_catastrophic_regression": {
            "details": catastrophic,
            "pass": catastrophic_pass,
        },
    }
    gates["all_pass"] = all(
        value["pass"] for key, value in gates.items() if key != "all_pass"
    )
    return gates


def main() -> int:
    panel, metadata = download_panel(HIST_START, END)
    panel = add_defensive_features(panel)
    signals_by_date, _ = build_pair_context(panel)

    summary_rows: list[dict] = []
    episode_rows: list[dict] = []

    summary_rows.extend(
        evaluate_window(
            panel,
            signals_by_date,
            start_text=YEAR_WINDOW[0],
            end_text=YEAR_WINDOW[1],
            window_type="1Y",
            episode_rows=episode_rows,
        )
    )
    for start_text, end_text in WINDOWS_180D:
        summary_rows.extend(
            evaluate_window(
                panel,
                signals_by_date,
                start_text=start_text,
                end_text=end_text,
                window_type="180D",
                episode_rows=episode_rows,
            )
        )
    for start_text, end_text in WINDOWS_120D:
        summary_rows.extend(
            evaluate_window(
                panel,
                signals_by_date,
                start_text=start_text,
                end_text=end_text,
                window_type="120D",
                episode_rows=episode_rows,
            )
        )

    summary = pd.DataFrame(summary_rows)
    gates = evaluate_gates(summary)

    run_id = f"DEF_SHADOW_REENTRY_V2_{_source_commit()[:12]}"
    run_dir = (
        REPO_ROOT
        / "research_artifacts"
        / "defensive_shadow_reentry_v2"
        / run_id
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    summary.to_csv(run_dir / "window_summary.csv", index=False)
    pd.DataFrame(episode_rows).to_csv(run_dir / "v2_episodes.csv", index=False)

    report = {
        "run_id": run_id,
        "source_commit_sha": _source_commit(),
        "status": (
            "DEVELOPMENT_PASS_FORWARD_CANDIDATE"
            if gates["all_pass"]
            else "DEVELOPMENT_FAIL_DO_NOT_PROMOTE"
        ),
        "candidate": "DEFENSIVE_LOW_VOL_CRYPTO_SHADOW_SMA200_CONFIRM3_V2",
        "data_end": END.date().isoformat(),
        "post_2026_03_28_data_used": False,
        "dataset": metadata,
        "predeclared_gates": gates,
        "summary": summary.astype(object).where(pd.notna(summary), None).to_dict(orient="records"),
    }
    _write_json(run_dir / "summary.json", report)

    print(summary.to_string(index=False))
    print("gates:")
    print(json.dumps(gates, indent=2))
    print(f"status={report['status']}")
    print(f"output={run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
