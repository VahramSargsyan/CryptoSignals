from __future__ import annotations

import json
import math
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from scripts.research_defensive_low_vol_untouched import (
    HIST_START,
    add_defensive_features,
)
from scripts.research_relative_rotation_graph_intelligence import ASSETS, download_panel

REPO_ROOT = Path(__file__).resolve().parents[1]
DEV_END = pd.Timestamp("2026-03-28", tz="UTC")
REPLAY_END = pd.Timestamp("2026-09-26", tz="UTC")

ENTER_MAX = 3
EXIT_MIN = 5
CONFIRM_DAYS = 3
HORIZONS = (7, 14, 30, 45, 60)

FEATURES = (
    "breadth_sma200",
    "episode_age_days",
    "breadth_delta_7d",
    "median_sma200_gap",
    "dispersion_sma200_gap",
    "median_vol30",
)


@dataclass(frozen=True)
class Episode:
    episode_id: str
    start: pd.Timestamp
    end: pd.Timestamp | None
    completed: bool
    duration_days: int | None
    states: pd.DataFrame


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


def prepare_duration_features(panel: pd.DataFrame) -> pd.DataFrame:
    out = add_defensive_features(panel).copy()

    gap_cols = []
    vol_cols = []
    for asset in ASSETS:
        gap_col = f"{asset}_sma200_gap"
        sma_col = f"{asset}_sma200"
        close_col = f"{asset}_close"
        vol_col = f"{asset}_vol30"

        out[gap_col] = out[close_col] / out[sma_col] - 1.0
        gap_cols.append(gap_col)
        vol_cols.append(vol_col)

    out["breadth_delta_7d"] = (
        out["breadth_sma200"] - out["breadth_sma200"].shift(7)
    )
    out["median_sma200_gap"] = out[gap_cols].median(axis=1)
    out["dispersion_sma200_gap"] = out[gap_cols].std(axis=1, ddof=0)
    out["median_vol30"] = out[vol_cols].median(axis=1)

    required = (
        [f"{asset}_sma200" for asset in ASSETS]
        + [f"{asset}_vol30" for asset in ASSETS]
        + [
            "breadth_delta_7d",
            "median_sma200_gap",
            "dispersion_sma200_gap",
            "median_vol30",
        ]
    )
    out["duration_features_valid"] = out[required].notna().all(axis=1)
    return out


def extract_stress_episodes(panel: pd.DataFrame) -> list[Episode]:
    valid = panel[panel["duration_features_valid"]].copy()
    valid = valid.sort_values("timestamp").reset_index(drop=True)
    episodes: list[Episode] = []

    active_start_idx: int | None = None
    low_streak = 0
    high_streak = 0
    serial = 0

    for idx, row in valid.iterrows():
        breadth = int(row["breadth_sma200"])

        if active_start_idx is None:
            if breadth <= ENTER_MAX:
                low_streak += 1
            else:
                low_streak = 0

            if low_streak >= CONFIRM_DAYS:
                active_start_idx = idx
                high_streak = 0
                serial += 1
            continue

        if breadth >= EXIT_MIN:
            high_streak += 1
        else:
            high_streak = 0

        if high_streak >= CONFIRM_DAYS:
            states = valid.iloc[active_start_idx : idx + 1].copy()
            start = pd.Timestamp(states.iloc[0]["timestamp"])
            end = pd.Timestamp(states.iloc[-1]["timestamp"])
            duration_days = int((end - start).days)
            states["episode_id"] = f"STRESS_{serial:03d}"
            states["episode_start"] = start
            states["episode_end"] = end
            states["episode_age_days"] = (
                states["timestamp"] - start
            ).dt.days.astype(int)
            states["remaining_stress_days"] = (
                end - states["timestamp"]
            ).dt.days.astype(int)
            states["episode_duration_days"] = duration_days

            episodes.append(
                Episode(
                    episode_id=f"STRESS_{serial:03d}",
                    start=start,
                    end=end,
                    completed=True,
                    duration_days=duration_days,
                    states=states,
                )
            )
            active_start_idx = None
            low_streak = 0
            high_streak = 0

    if active_start_idx is not None:
        states = valid.iloc[active_start_idx:].copy()
        start = pd.Timestamp(states.iloc[0]["timestamp"])
        states["episode_id"] = f"STRESS_{serial:03d}"
        states["episode_start"] = start
        states["episode_end"] = pd.NaT
        states["episode_age_days"] = (
            states["timestamp"] - start
        ).dt.days.astype(int)
        states["remaining_stress_days"] = pd.NA
        states["episode_duration_days"] = pd.NA
        episodes.append(
            Episode(
                episode_id=f"STRESS_{serial:03d}",
                start=start,
                end=None,
                completed=False,
                duration_days=None,
                states=states,
            )
        )

    return episodes


def _robust_scale_params(train_states: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    center = train_states[list(FEATURES)].median()
    q25 = train_states[list(FEATURES)].quantile(0.25)
    q75 = train_states[list(FEATURES)].quantile(0.75)
    scale = q75 - q25
    scale = scale.where(scale.abs() > 1e-12, 1.0)
    return center.astype(float), scale.astype(float)


def _episode_balanced_analogs(
    test_row: pd.Series,
    train_episodes: list[Episode],
) -> pd.DataFrame:
    train_states = pd.concat(
        [ep.states for ep in train_episodes],
        ignore_index=True,
    )
    center, scale = _robust_scale_params(train_states)

    target = (test_row[list(FEATURES)].astype(float) - center) / scale

    chosen = []
    for episode in train_episodes:
        states = episode.states.copy()
        z = (states[list(FEATURES)].astype(float) - center) / scale
        distances = ((z - target) ** 2).sum(axis=1).pow(0.5)
        best_pos = int(distances.to_numpy().argmin())
        best = states.iloc[best_pos]
        chosen.append(
            {
                "episode_id": episode.episode_id,
                "analog_date": best["timestamp"],
                "distance": float(distances.iloc[best_pos]),
                "remaining_days": int(best["remaining_stress_days"]),
                "episode_duration_days": int(best["episode_duration_days"]),
            }
        )

    return pd.DataFrame(chosen).sort_values(
        ["distance", "episode_id"]
    ).reset_index(drop=True)


def _distribution_summary(values: list[int]) -> dict:
    series = pd.Series(values, dtype=float)
    result = {
        "predicted_median": float(series.median()),
        "p25": float(series.quantile(0.25)),
        "p75": float(series.quantile(0.75)),
    }
    for horizon in HORIZONS:
        result[f"p_recovery_le_{horizon}d"] = float((series <= horizon).mean())
    return result


def predict_state(
    test_row: pd.Series,
    train_episodes: list[Episode],
) -> dict:
    analogs = _episode_balanced_analogs(test_row, train_episodes)
    analog_values = analogs["remaining_days"].astype(int).tolist()

    age = int(test_row["episode_age_days"])
    baseline_values = [
        max(int(ep.duration_days) - age, 0)
        for ep in train_episodes
        if ep.duration_days is not None
    ]

    analog = _distribution_summary(analog_values)
    baseline = _distribution_summary(baseline_values)

    return {
        "analog": analog,
        "baseline": baseline,
        "analog_count": len(analog_values),
        "analog_details": analogs,
    }


def _score_probability(probability: float, actual: bool) -> float:
    y = 1.0 if actual else 0.0
    return (probability - y) ** 2


def walk_forward_episode_validation(
    episodes: list[Episode],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    completed = [ep for ep in episodes if ep.completed]
    completed.sort(key=lambda ep: ep.start)

    rows: list[dict] = []
    episode_rows: list[dict] = []

    for test_ep in completed:
        train = [
            ep
            for ep in completed
            if ep.end is not None and ep.end < test_ep.start
        ]
        if len(train) < 2:
            continue

        local_rows = []
        for _, state in test_ep.states.iterrows():
            pred = predict_state(state, train)
            actual = int(state["remaining_stress_days"])

            row = {
                "test_episode_id": test_ep.episode_id,
                "test_episode_start": test_ep.start,
                "test_episode_end": test_ep.end,
                "date": state["timestamp"],
                "episode_age_days": int(state["episode_age_days"]),
                "actual_remaining_days": actual,
                "training_episode_count": len(train),
                "analog_predicted_median": pred["analog"]["predicted_median"],
                "analog_p25": pred["analog"]["p25"],
                "analog_p75": pred["analog"]["p75"],
                "baseline_predicted_median": pred["baseline"]["predicted_median"],
                "baseline_p25": pred["baseline"]["p25"],
                "baseline_p75": pred["baseline"]["p75"],
            }

            for horizon in HORIZONS:
                actual_event = actual <= horizon
                ap = pred["analog"][f"p_recovery_le_{horizon}d"]
                bp = pred["baseline"][f"p_recovery_le_{horizon}d"]
                row[f"analog_p_recovery_le_{horizon}d"] = ap
                row[f"baseline_p_recovery_le_{horizon}d"] = bp
                row[f"analog_brier_{horizon}d"] = _score_probability(
                    ap, actual_event
                )
                row[f"baseline_brier_{horizon}d"] = _score_probability(
                    bp, actual_event
                )

            rows.append(row)
            local_rows.append(row)

        local = pd.DataFrame(local_rows)
        if local.empty:
            continue

        analog_abs = (
            local["analog_predicted_median"] - local["actual_remaining_days"]
        ).abs()
        baseline_abs = (
            local["baseline_predicted_median"] - local["actual_remaining_days"]
        ).abs()

        episode_rows.append(
            {
                "test_episode_id": test_ep.episode_id,
                "start": test_ep.start,
                "end": test_ep.end,
                "duration_days": test_ep.duration_days,
                "training_episode_count": int(local["training_episode_count"].max()),
                "scored_states": len(local),
                "analog_mae": float(analog_abs.mean()),
                "baseline_mae": float(baseline_abs.mean()),
                "analog_median_abs_error": float(analog_abs.median()),
                "baseline_median_abs_error": float(baseline_abs.median()),
                "analog_mae_better": bool(analog_abs.mean() < baseline_abs.mean()),
            }
        )

    return pd.DataFrame(rows), pd.DataFrame(episode_rows)


def aggregate_metrics(scored: pd.DataFrame, episode_metrics: pd.DataFrame) -> dict:
    if scored.empty:
        return {
            "scored_states": 0,
            "scored_test_episodes": 0,
        }

    analog_error = (
        scored["analog_predicted_median"] - scored["actual_remaining_days"]
    )
    baseline_error = (
        scored["baseline_predicted_median"] - scored["actual_remaining_days"]
    )

    analog_brier_cols = [f"analog_brier_{h}d" for h in HORIZONS]
    baseline_brier_cols = [f"baseline_brier_{h}d" for h in HORIZONS]

    analog_coverage = (
        (scored["actual_remaining_days"] >= scored["analog_p25"])
        & (scored["actual_remaining_days"] <= scored["analog_p75"])
    )
    baseline_coverage = (
        (scored["actual_remaining_days"] >= scored["baseline_p25"])
        & (scored["actual_remaining_days"] <= scored["baseline_p75"])
    )

    result = {
        "scored_states": len(scored),
        "scored_test_episodes": int(scored["test_episode_id"].nunique()),
        "analog_mae": float(analog_error.abs().mean()),
        "baseline_mae": float(baseline_error.abs().mean()),
        "analog_median_abs_error": float(analog_error.abs().median()),
        "baseline_median_abs_error": float(baseline_error.abs().median()),
        "analog_signed_mean_error": float(analog_error.mean()),
        "baseline_signed_mean_error": float(baseline_error.mean()),
        "analog_p25_p75_coverage": float(analog_coverage.mean()),
        "baseline_p25_p75_coverage": float(baseline_coverage.mean()),
        "analog_mean_interval_width": float(
            (scored["analog_p75"] - scored["analog_p25"]).mean()
        ),
        "baseline_mean_interval_width": float(
            (scored["baseline_p75"] - scored["baseline_p25"]).mean()
        ),
        "analog_mean_brier": float(scored[analog_brier_cols].to_numpy().mean()),
        "baseline_mean_brier": float(
            scored[baseline_brier_cols].to_numpy().mean()
        ),
        "episodes_analog_mae_better": int(
            episode_metrics["analog_mae_better"].sum()
        )
        if not episode_metrics.empty
        else 0,
    }
    return result


def diagnostic_verdict(
    aggregate: dict,
    episode_metrics: pd.DataFrame,
) -> tuple[str, dict]:
    scored_episodes = int(aggregate.get("scored_test_episodes", 0))
    scored_states = int(aggregate.get("scored_states", 0))

    evidence_pass = scored_episodes >= 3 and scored_states >= 30
    if not evidence_pass:
        return (
            "INSUFFICIENT_INDEPENDENT_EPISODES",
            {
                "minimum_scored_test_episodes": 3,
                "minimum_scored_states": 30,
                "observed_scored_test_episodes": scored_episodes,
                "observed_scored_states": scored_states,
                "pass": False,
            },
        )

    half_episodes = math.ceil(scored_episodes / 2)
    checks = {
        "analog_mae_better": aggregate["analog_mae"] < aggregate["baseline_mae"],
        "analog_median_abs_error_not_worse": (
            aggregate["analog_median_abs_error"]
            <= aggregate["baseline_median_abs_error"]
        ),
        "analog_brier_better": (
            aggregate["analog_mean_brier"] < aggregate["baseline_mean_brier"]
        ),
        "episode_mae_better_at_least_half": (
            aggregate["episodes_analog_mae_better"] >= half_episodes
        ),
    }
    verdict = (
        "PREDICTIVE_SIGNAL_PRESENT_V1"
        if all(checks.values())
        else "WEAK_OR_NO_PREDICTIVE_SIGNAL_V1"
    )
    return verdict, {
        "minimum_evidence_pass": True,
        "required_episode_mae_wins": half_episodes,
        "checks": checks,
        "all_checks_pass": all(checks.values()),
    }


def episode_catalog(episodes: list[Episode]) -> pd.DataFrame:
    rows = []
    for ep in episodes:
        rows.append(
            {
                "episode_id": ep.episode_id,
                "start": ep.start,
                "end": ep.end,
                "completed": ep.completed,
                "duration_days": ep.duration_days,
                "state_rows": len(ep.states),
            }
        )
    return pd.DataFrame(rows)


def replay_2026(
    episodes: list[Episode],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    completed = [ep for ep in episodes if ep.completed]
    targets = [
        ep
        for ep in completed
        if ep.start >= pd.Timestamp("2026-03-29", tz="UTC")
        and ep.start <= REPLAY_END
    ]

    rows = []
    summaries = []
    for test_ep in targets:
        train = [
            ep
            for ep in completed
            if ep.end is not None and ep.end < test_ep.start
        ]
        if len(train) < 2:
            continue

        local = []
        for _, state in test_ep.states.iterrows():
            pred = predict_state(state, train)
            actual = int(state["remaining_stress_days"])
            row = {
                "episode_id": test_ep.episode_id,
                "date": state["timestamp"],
                "episode_age_days": int(state["episode_age_days"]),
                "actual_remaining_days": actual,
                "analog_predicted_median": pred["analog"]["predicted_median"],
                "analog_p25": pred["analog"]["p25"],
                "analog_p75": pred["analog"]["p75"],
                "baseline_predicted_median": pred["baseline"]["predicted_median"],
                "breadth_sma200": int(state["breadth_sma200"]),
                "breadth_delta_7d": float(state["breadth_delta_7d"]),
                "median_sma200_gap": float(state["median_sma200_gap"]),
                "dispersion_sma200_gap": float(
                    state["dispersion_sma200_gap"]
                ),
                "median_vol30": float(state["median_vol30"]),
                "training_episode_count": len(train),
            }
            for horizon in HORIZONS:
                row[f"p_recovery_le_{horizon}d"] = pred["analog"][
                    f"p_recovery_le_{horizon}d"
                ]
            rows.append(row)
            local.append(row)

        local_df = pd.DataFrame(local)
        if not local_df.empty:
            abs_error = (
                local_df["analog_predicted_median"]
                - local_df["actual_remaining_days"]
            ).abs()
            summaries.append(
                {
                    "episode_id": test_ep.episode_id,
                    "start": test_ep.start,
                    "end": test_ep.end,
                    "duration_days": test_ep.duration_days,
                    "training_episode_count": len(train),
                    "states": len(local_df),
                    "analog_mae": float(abs_error.mean()),
                    "analog_median_abs_error": float(abs_error.median()),
                }
            )

    return pd.DataFrame(rows), pd.DataFrame(summaries)


def main() -> int:
    run_id = f"ADAPT_STRESS_DURATION_V1_{_source_commit()[:12]}"
    run_dir = (
        REPO_ROOT
        / "research_artifacts"
        / "adaptive_stress_duration_v1"
        / run_id
    )
    run_dir.mkdir(parents=True, exist_ok=True)

    # Development diagnostic. Hard cutoff at 2026-03-28.
    dev_panel, dev_meta = download_panel(HIST_START, DEV_END)
    dev_panel = prepare_duration_features(dev_panel)
    dev_episodes = extract_stress_episodes(dev_panel)

    catalog = episode_catalog(dev_episodes)
    scored, episode_metrics = walk_forward_episode_validation(dev_episodes)
    aggregate = aggregate_metrics(scored, episode_metrics)
    verdict, gate = diagnostic_verdict(aggregate, episode_metrics)

    catalog.to_csv(run_dir / "development_episode_catalog.csv", index=False)
    scored.to_csv(run_dir / "development_walk_forward_predictions.csv", index=False)
    episode_metrics.to_csv(
        run_dir / "development_episode_metrics.csv", index=False
    )

    # Post-hoc replay. It cannot change the development verdict.
    replay_panel, replay_meta = download_panel(HIST_START, REPLAY_END)
    replay_panel = prepare_duration_features(replay_panel)
    replay_episodes = extract_stress_episodes(replay_panel)
    replay_rows, replay_summary = replay_2026(replay_episodes)
    replay_rows.to_csv(run_dir / "posthoc_2026_forecast_path.csv", index=False)
    replay_summary.to_csv(
        run_dir / "posthoc_2026_episode_summary.csv", index=False
    )

    report = {
        "run_id": run_id,
        "source_commit_sha": _source_commit(),
        "status": verdict,
        "model": "ADAPTIVE_STRESS_DURATION_V1_EPISODE_BALANCED_ANALOGS",
        "features": list(FEATURES),
        "horizons_days": list(HORIZONS),
        "development": {
            "data_end": DEV_END.date().isoformat(),
            "dataset": dev_meta,
            "episode_catalog": catalog.astype(object)
            .where(pd.notna(catalog), None)
            .to_dict(orient="records"),
            "aggregate_metrics": aggregate,
            "diagnostic_gate": gate,
            "episode_metrics": episode_metrics.astype(object)
            .where(pd.notna(episode_metrics), None)
            .to_dict(orient="records"),
        },
        "posthoc_2026_replay": {
            "label": "POST_HOC_2026_FORECAST_REPLAY_NOT_VALIDATION",
            "dataset": replay_meta,
            "episode_summary": replay_summary.astype(object)
            .where(pd.notna(replay_summary), None)
            .to_dict(orient="records"),
        },
        "production_impact": "NONE",
        "migration_required": False,
    }
    _write_json(run_dir / "summary.json", report)

    print("development_episode_catalog:")
    print(catalog.to_string(index=False))
    print("development_aggregate:")
    print(json.dumps(aggregate, indent=2))
    print("diagnostic_gate:")
    print(json.dumps(gate, indent=2))
    print(f"diagnostic_verdict={verdict}")
    print("posthoc_2026_episode_summary:")
    if replay_summary.empty:
        print("NONE")
    else:
        print(replay_summary.to_string(index=False))
    print("posthoc_2026_forecast_checkpoints:")
    if replay_rows.empty:
        print("NONE")
    else:
        for episode_id, group in replay_rows.groupby("episode_id"):
            group = group.reset_index(drop=True)
            indices = sorted(
                set(
                    [
                        0,
                        len(group) // 4,
                        len(group) // 2,
                        (3 * len(group)) // 4,
                        len(group) - 1,
                    ]
                )
            )
            print(group.iloc[indices].to_string(index=False))
    print(f"output={run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
