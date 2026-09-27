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
from scripts.research_defensive_shadow_reentry_v2 import (
    NON_WEAK_180D,
    WEAK_180D,
    WINDOWS_120D,
    WINDOWS_180D,
    YEAR_WINDOW,
    summarize_variant,
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


@dataclass(frozen=True)
class V3Result:
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


def run_router_reactivation_backtest(
    panel: pd.DataFrame,
    signals_by_date: dict[pd.Timestamp, list[PairSignal]],
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    start_asset: str,
    episode_log: list[dict] | None = None,
) -> V3Result:
    window = panel[(panel["timestamp"] >= start) & (panel["timestamp"] <= end)].copy()
    if window.empty:
        raise ValueError("Empty evaluation window")

    first = window.iloc[0]
    shadow_asset = start_asset
    actual_asset = start_asset
    qty = 1.0 / float(first[f"{actual_asset}_open"])

    pending_shadow: PairSignal | None = None
    pending_entry: str | None = None
    pending_router_exit = False

    defensive = False
    defensive_asset: str | None = None
    low_streak = 0

    actual_transitions = 0
    defensive_transitions = 0
    defensive_days = 0
    defensive_entries = 0
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

        if pending_entry is not None:
            target = pending_entry
            pending_entry = None
            if actual_asset != target:
                value = qty * float(row[f"{actual_asset}_open"])
                qty = value * (1.0 - TRANSITION_COST) / float(row[f"{target}_open"])
                actual_asset = target
                actual_transitions += 1
                defensive_transitions += 1
            defensive = True
            defensive_asset = target
            defensive_entries += 1
            pending_router_exit = False
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

        elif defensive and pending_router_exit and shadow_transitioned:
            target = shadow_asset
            pending_router_exit = False
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
                        "action": "EXIT_ROUTER_REACTIVATION",
                        "date": ts.isoformat(),
                        "defensive_asset": defensive_asset,
                        "shadow_asset": shadow_asset,
                        "breadth": int(row["breadth_sma200"]),
                    }
                )
            defensive = False
            defensive_asset = None
            low_streak = 0

        elif not defensive and shadow_transitioned:
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

        if pos == len(window) - 1:
            continue

        candidates = [
            signal
            for signal in signals_by_date.get(ts, [])
            if signal.from_asset == shadow_asset
        ]
        if candidates:
            pending_shadow = choose_candidate(candidates, "BASELINE", None)
            if defensive:
                pending_router_exit = True

        breadth = int(row["breadth_sma200"])
        if breadth <= ENTER_BREADTH_MAX:
            low_streak += 1
        else:
            low_streak = 0

        if not defensive and pending_entry is None and low_streak >= CONFIRM_DAYS:
            target = row["lowest_vol_asset"]
            if pd.isna(target):
                raise RuntimeError(f"Missing defensive asset at {ts}")
            pending_entry = str(target)
            low_streak = 0

    series = pd.Series(equity, index=dates, dtype=float)
    return V3Result(
        start_asset=start_asset,
        total_return=float(series.iloc[-1] / series.iloc[0] - 1.0),
        max_drawdown=float((series / series.cummax() - 1.0).min()),
        actual_transitions=actual_transitions,
        defensive_transitions=defensive_transitions,
        defensive_days=defensive_days,
        defensive_entries=defensive_entries,
        period_days=len(window),
    )


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
    v3_rows = []

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
        v3_rows.append(
            asdict(
                run_router_reactivation_backtest(
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
            "variant": "V3_ROUTER_REACTIVATION",
            **summarize_variant(pd.DataFrame(v3_rows)),
        },
    ]


def evaluate_gates(summary: pd.DataFrame) -> dict:
    d180 = summary[summary["window_type"] == "180D"].copy()
    original = d180[d180["variant"] == "ORIGINAL_BREADTH_3_5"].set_index(
        ["period_start", "period_end"]
    )
    v3 = d180[d180["variant"] == "V3_ROUTER_REACTIVATION"].set_index(
        ["period_start", "period_end"]
    )

    protection = {}
    for key in sorted(WEAK_180D):
        original_dd = float(original.loc[key, "median_max_drawdown"])
        v3_dd = float(v3.loc[key, "median_max_drawdown"])
        deterioration = abs(v3_dd) - abs(original_dd)
        protection["|".join(key)] = {
            "original_dd": original_dd,
            "v3_dd": v3_dd,
            "drawdown_deterioration": deterioration,
            "pass": deterioration <= 0.10,
        }

    opportunity_wins = sum(
        float(v3.loc[key, "median_return"]) > float(original.loc[key, "median_return"])
        for key in NON_WEAK_180D
    )
    exposure_wins = sum(
        float(v3.loc[key, "median_defensive_exposure"])
        < float(original.loc[key, "median_defensive_exposure"])
        for key in NON_WEAK_180D
    )

    churn_pass = bool((v3["median_defensive_transitions"] <= 6).all())

    catastrophic = {}
    catastrophic_pass = True
    for key in original.index:
        orig_ret = float(original.loc[key, "median_return"])
        v3_ret = float(v3.loc[key, "median_return"])
        fail = orig_ret > 0 and v3_ret < -0.10
        catastrophic_pass = catastrophic_pass and not fail
        catastrophic["|".join(key)] = {
            "original_return": orig_ret,
            "v3_return": v3_ret,
            "pass": not fail,
        }

    gates = {
        "protection_retention": {
            "details": protection,
            "pass": all(v["pass"] for v in protection.values()),
        },
        "opportunity_cost_improvement": {
            "wins_required": 2,
            "wins_observed": int(opportunity_wins),
            "pass": opportunity_wins >= 2,
        },
        "exposure_improvement": {
            "wins_required": 2,
            "wins_observed": int(exposure_wins),
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
        v["pass"] for k, v in gates.items() if k != "all_pass"
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

    run_id = f"DEF_ROUTER_REACT_V3_{_source_commit()[:12]}"
    run_dir = (
        REPO_ROOT
        / "research_artifacts"
        / "defensive_router_reactivation_v3"
        / run_id
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    summary.to_csv(run_dir / "window_summary.csv", index=False)
    pd.DataFrame(episode_rows).to_csv(run_dir / "v3_episodes.csv", index=False)

    safe_summary = summary.astype(object).where(pd.notna(summary), None)
    report = {
        "run_id": run_id,
        "source_commit_sha": _source_commit(),
        "status": (
            "DEVELOPMENT_PASS_FORWARD_CANDIDATE"
            if gates["all_pass"]
            else "DEVELOPMENT_FAIL_DO_NOT_PROMOTE"
        ),
        "candidate": "DEFENSIVE_LOW_VOL_CRYPTO_ROUTER_REACTIVATION_V3",
        "post_oos_redesign_attempt_number": 2,
        "data_end": END.date().isoformat(),
        "post_2026_03_28_data_used": False,
        "dataset": metadata,
        "predeclared_gates": gates,
        "summary": safe_summary.to_dict(orient="records"),
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
