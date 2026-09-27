from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pandas as pd

import scripts.research_defensive_probation_memory_v4 as v4
from scripts.research_defensive_low_vol_untouched import HIST_START, add_defensive_features
from scripts.research_defensive_shadow_reentry_v2 import (
    NON_WEAK_180D,
    WEAK_180D,
    WINDOWS_120D,
    WINDOWS_180D,
    YEAR_WINDOW,
)
from scripts.research_relative_rotation_graph_intelligence import (
    build_pair_context,
    download_panel,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEV_END = pd.Timestamp("2026-03-28", tz="UTC")
REPLAY_END = pd.Timestamp("2026-09-26", tz="UTC")
DURATIONS = (3, 5, 7, 10, 14, 21, 30, 45, 60)


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


def _evaluate_all_windows(
    panel: pd.DataFrame,
    signals_by_date,
    duration: int,
) -> pd.DataFrame:
    v4.PROBATION_DAYS = duration
    rows: list[dict] = []
    episodes: list[dict] = []

    rows.extend(
        v4.evaluate_window(
            panel,
            signals_by_date,
            start_text=YEAR_WINDOW[0],
            end_text=YEAR_WINDOW[1],
            window_type="1Y",
            v4_episode_rows=episodes,
        )
    )
    for start_text, end_text in WINDOWS_180D:
        rows.extend(
            v4.evaluate_window(
                panel,
                signals_by_date,
                start_text=start_text,
                end_text=end_text,
                window_type="180D",
                v4_episode_rows=episodes,
            )
        )
    for start_text, end_text in WINDOWS_120D:
        rows.extend(
            v4.evaluate_window(
                panel,
                signals_by_date,
                start_text=start_text,
                end_text=end_text,
                window_type="120D",
                v4_episode_rows=episodes,
            )
        )

    frame = pd.DataFrame(rows)
    frame["probation_days_parameter"] = duration
    return frame


def _evaluate_duration_gates(frame: pd.DataFrame) -> dict:
    d180 = frame[frame["window_type"] == "180D"].copy()
    original = d180[d180["variant"] == "ORIGINAL_BREADTH_3_5"].set_index(
        ["period_start", "period_end"]
    )
    candidate = d180[d180["variant"] == "V4_PROBATION14_MEMORY"].set_index(
        ["period_start", "period_end"]
    )

    protection = {}
    for key in sorted(WEAK_180D):
        original_dd = float(original.loc[key, "median_max_drawdown"])
        candidate_dd = float(candidate.loc[key, "median_max_drawdown"])
        deterioration = abs(candidate_dd) - abs(original_dd)
        protection["|".join(key)] = {
            "original_dd": original_dd,
            "candidate_dd": candidate_dd,
            "drawdown_deterioration": deterioration,
            "pass": deterioration <= 0.10,
        }

    opportunity_wins = sum(
        float(candidate.loc[key, "median_return"])
        > float(original.loc[key, "median_return"])
        for key in NON_WEAK_180D
    )

    occupancy_wins = sum(
        float(candidate.loc[key, "median_actual_defensive_token_exposure"])
        < float(original.loc[key, "median_actual_defensive_token_exposure"])
        for key in NON_WEAK_180D
    )

    churn_pass = bool((candidate["median_defensive_transitions"] <= 6).all())

    catastrophic = {}
    catastrophic_pass = True
    for key in original.index:
        orig_ret = float(original.loc[key, "median_return"])
        cand_ret = float(candidate.loc[key, "median_return"])
        fail = orig_ret > 0 and cand_ret < -0.10
        catastrophic_pass = catastrophic_pass and not fail
        catastrophic["|".join(key)] = {
            "original_return": orig_ret,
            "candidate_return": cand_ret,
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
        "defensive_occupancy_improvement": {
            "wins_required": 2,
            "wins_observed": int(occupancy_wins),
            "pass": occupancy_wins >= 2,
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


def _development_rollup(frame: pd.DataFrame, duration: int, gates: dict) -> dict:
    year = frame[
        (frame["window_type"] == "1Y")
        & (frame["variant"] == "V4_PROBATION14_MEMORY")
    ].iloc[0]
    d180 = frame[
        (frame["window_type"] == "180D")
        & (frame["variant"] == "V4_PROBATION14_MEMORY")
    ]
    original180 = frame[
        (frame["window_type"] == "180D")
        & (frame["variant"] == "ORIGINAL_BREADTH_3_5")
    ]

    return {
        "probation_days": duration,
        "all_gates_pass": bool(gates["all_pass"]),
        "year_median_return": float(year["median_return"]),
        "year_median_max_drawdown": float(year["median_max_drawdown"]),
        "year_defensive_token_exposure": float(
            year["median_actual_defensive_token_exposure"]
        ),
        "year_probation_exposure": float(year["median_probation_exposure"]),
        "year_defensive_transitions": float(year["median_defensive_transitions"]),
        "median_180d_return": float(d180["median_return"].median()),
        "worst_180d_return": float(d180["median_return"].min()),
        "median_180d_max_drawdown": float(d180["median_max_drawdown"].median()),
        "median_180d_defensive_token_exposure": float(
            d180["median_actual_defensive_token_exposure"].median()
        ),
        "median_180d_probation_exposure": float(
            d180["median_probation_exposure"].median()
        ),
        "median_180d_defensive_transitions": float(
            d180["median_defensive_transitions"].median()
        ),
        "original_median_180d_return": float(original180["median_return"].median()),
        "original_median_180d_max_drawdown": float(
            original180["median_max_drawdown"].median()
        ),
        "opportunity_wins": int(
            gates["opportunity_cost_improvement"]["wins_observed"]
        ),
        "occupancy_wins": int(
            gates["defensive_occupancy_improvement"]["wins_observed"]
        ),
        "protection_pass": bool(gates["protection_retention"]["pass"]),
        "churn_pass": bool(gates["churn_control"]["pass"]),
        "catastrophic_pass": bool(gates["no_catastrophic_regression"]["pass"]),
    }


def _find_plateaus(rollup: pd.DataFrame) -> list[list[int]]:
    passed = set(
        int(x)
        for x in rollup.loc[rollup["all_gates_pass"], "probation_days"].tolist()
    )
    plateaus: list[list[int]] = []
    current: list[int] = []
    for duration in DURATIONS:
        if duration in passed:
            if current:
                prev_index = DURATIONS.index(current[-1])
                cur_index = DURATIONS.index(duration)
                if cur_index == prev_index + 1:
                    current.append(duration)
                else:
                    if len(current) >= 2:
                        plateaus.append(current)
                    current = [duration]
            else:
                current = [duration]
        else:
            if len(current) >= 2:
                plateaus.append(current)
            current = []
    if len(current) >= 2:
        plateaus.append(current)
    return plateaus


def _replay_duration(
    panel: pd.DataFrame,
    signals_by_date,
    duration: int,
) -> dict:
    v4.PROBATION_DAYS = duration
    rows = v4.evaluate_window(
        panel,
        signals_by_date,
        start_text=v4.REPLAY_START.date().isoformat(),
        end_text=v4.REPLAY_END.date().isoformat(),
        window_type="POST_HOC_2026",
        v4_episode_rows=[],
    )
    frame = pd.DataFrame(rows)
    candidate = frame[frame["variant"] == "V4_PROBATION14_MEMORY"].iloc[0]
    return {
        "probation_days": duration,
        "median_return": float(candidate["median_return"]),
        "median_max_drawdown": float(candidate["median_max_drawdown"]),
        "defensive_token_exposure": float(
            candidate["median_actual_defensive_token_exposure"]
        ),
        "probation_exposure": float(candidate["median_probation_exposure"]),
        "defensive_transitions": float(candidate["median_defensive_transitions"]),
        "actual_transitions": float(candidate["median_actual_transitions"]),
        "probes_started": float(candidate["median_probes_started"]),
        "probes_succeeded": float(candidate["median_probes_succeeded"]),
        "probes_failed": float(candidate["median_probes_failed"]),
    }


def main() -> int:
    run_id = f"DEF_PROB_DURATION_V4A_{_source_commit()[:12]}"
    run_dir = (
        REPO_ROOT
        / "research_artifacts"
        / "defensive_probation_duration_v4a"
        / run_id
    )
    run_dir.mkdir(parents=True, exist_ok=True)

    # Development sweep only through 2026-03-28.
    dev_panel, dev_meta = download_panel(HIST_START, DEV_END)
    dev_panel = add_defensive_features(dev_panel)
    dev_signals, _ = build_pair_context(dev_panel)

    all_dev_frames: list[pd.DataFrame] = []
    gate_records: dict[str, dict] = {}
    rollups: list[dict] = []

    for duration in DURATIONS:
        frame = _evaluate_all_windows(dev_panel, dev_signals, duration)
        gates = _evaluate_duration_gates(frame)
        gate_records[str(duration)] = gates
        rollups.append(_development_rollup(frame, duration, gates))
        all_dev_frames.append(frame)

    dev_detail = pd.concat(all_dev_frames, ignore_index=True)
    dev_rollup = pd.DataFrame(rollups)
    plateaus = _find_plateaus(dev_rollup)

    if plateaus:
        conclusion = "ROBUST_PLATEAU_FOUND"
    elif bool(dev_rollup["all_gates_pass"].any()):
        conclusion = "NO_ROBUST_DURATION_FOUND"
    else:
        conclusion = "DURATION_ALONE_DOES_NOT_FIX_V4"

    dev_detail.to_csv(run_dir / "development_detail.csv", index=False)
    dev_rollup.to_csv(run_dir / "development_duration_rollup.csv", index=False)

    # Post-hoc replay is diagnostic only and never changes the development conclusion.
    replay_panel, replay_meta = download_panel(HIST_START, REPLAY_END)
    replay_panel = add_defensive_features(replay_panel)
    replay_signals, _ = build_pair_context(replay_panel)
    replay_rows = [
        _replay_duration(replay_panel, replay_signals, duration)
        for duration in DURATIONS
    ]
    replay = pd.DataFrame(replay_rows)
    replay.to_csv(run_dir / "posthoc_2026_duration_sensitivity.csv", index=False)

    report = {
        "run_id": run_id,
        "source_commit_sha": _source_commit(),
        "status": conclusion,
        "candidate_family": "DEFENSIVE_PROBATION_MEMORY_DURATION_SENSITIVITY_V4A",
        "durations": list(DURATIONS),
        "selection_rule": "robust plateau requires >=2 adjacent predeclared durations passing all gates",
        "development": {
            "data_end": DEV_END.date().isoformat(),
            "dataset": dev_meta,
            "gates_by_duration": gate_records,
            "plateaus": plateaus,
            "rollup": dev_rollup.astype(object)
            .where(pd.notna(dev_rollup), None)
            .to_dict(orient="records"),
        },
        "posthoc_2026_replay": {
            "label": "POST_HOC_DIAGNOSTIC_REPLAY_NOT_SELECTION_DATA",
            "dataset": replay_meta,
            "rows": replay.astype(object)
            .where(pd.notna(replay), None)
            .to_dict(orient="records"),
        },
    }
    _write_json(run_dir / "summary.json", report)

    print("development_rollup:")
    print(dev_rollup.to_string(index=False))
    print("plateaus:")
    print(json.dumps(plateaus))
    print(f"development_conclusion={conclusion}")
    print("posthoc_2026:")
    print(replay.to_string(index=False))
    print(f"output={run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
