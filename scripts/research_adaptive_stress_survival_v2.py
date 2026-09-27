from __future__ import annotations

import json
import math
import os
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd
from lifelines import KaplanMeierFitter, WeibullFitter

from scripts.research_adaptive_stress_duration_v1 import (
    DEV_END,
    FEATURES,
    HIST_START,
    HORIZONS,
    REPLAY_END,
    Episode,
    extract_stress_episodes,
    predict_state,
    prepare_duration_features,
)
from scripts.research_relative_rotation_graph_intelligence import download_panel

REPO_ROOT = Path(__file__).resolve().parents[1]
MIN_PRIOR_COMPLETED = 3


@dataclass(frozen=True)
class SurvivalForecast:
    support_status: str
    prior_completed_count: int
    max_prior_completed_duration: int
    km_median_remaining: float | None
    weibull_median_remaining: float | None
    km_probabilities: dict[int, float]
    weibull_probabilities: dict[int, float]
    weibull_lambda: float
    weibull_rho: float


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


def duration_support_status(
    age_days: int,
    prior_completed_durations: list[int],
) -> tuple[str, int]:
    if not prior_completed_durations:
        return "NO_COMPLETED_HISTORY", 0
    max_duration = max(prior_completed_durations)
    status = (
        "OUT_OF_DURATION_SUPPORT"
        if age_days > max_duration
        else "IN_DURATION_SUPPORT"
    )
    return status, max_duration


def _conditional_probability(
    survival_now: float,
    survival_future: float,
) -> float:
    if not math.isfinite(survival_now) or survival_now <= 0:
        return 0.0
    value = 1.0 - survival_future / survival_now
    return float(min(1.0, max(0.0, value)))


def _km_conditional_median_remaining(
    kmf: KaplanMeierFitter,
    age_days: int,
) -> float | None:
    s_age = float(kmf.predict(age_days))
    if not math.isfinite(s_age) or s_age <= 0:
        return None

    threshold = 0.5 * s_age
    sf = kmf.survival_function_.iloc[:, 0]
    candidates = sf[(sf.index >= age_days) & (sf <= threshold)]
    if candidates.empty:
        return None
    return float(candidates.index[0] - age_days)


def _weibull_conditional_median_remaining(
    wf: WeibullFitter,
    age_days: int,
) -> float | None:
    lam = float(wf.lambda_)
    rho = float(wf.rho_)
    if not (math.isfinite(lam) and math.isfinite(rho) and lam > 0 and rho > 0):
        return None

    base = (age_days / lam) ** rho
    target_time = lam * (base + math.log(2.0)) ** (1.0 / rho)
    remaining = target_time - age_days
    if not math.isfinite(remaining):
        return None
    return float(max(0.0, remaining))


def fit_survival_forecast(
    prior_completed_durations: list[int],
    *,
    current_age_days: int,
) -> SurvivalForecast:
    if len(prior_completed_durations) < MIN_PRIOR_COMPLETED:
        raise ValueError(
            f"Need at least {MIN_PRIOR_COMPLETED} prior completed episodes"
        )

    durations = [float(x) for x in prior_completed_durations]
    events = [1] * len(prior_completed_durations)
    if current_age_days > 0:
        durations.append(float(current_age_days))
        events.append(0)

    support_status, max_duration = duration_support_status(
        current_age_days,
        prior_completed_durations,
    )

    kmf = KaplanMeierFitter()
    kmf.fit(durations=durations, event_observed=events)

    wf = WeibullFitter()
    wf.fit(durations=durations, event_observed=events)

    km_probs: dict[int, float] = {}
    wb_probs: dict[int, float] = {}

    km_s_now = float(kmf.predict(current_age_days))
    wb_s_now = float(wf.survival_function_at_times(current_age_days).iloc[0])

    for horizon in HORIZONS:
        future_age = current_age_days + horizon
        km_s_future = float(kmf.predict(future_age))
        wb_s_future = float(
            wf.survival_function_at_times(future_age).iloc[0]
        )
        km_probs[horizon] = _conditional_probability(
            km_s_now,
            km_s_future,
        )
        wb_probs[horizon] = _conditional_probability(
            wb_s_now,
            wb_s_future,
        )

    return SurvivalForecast(
        support_status=support_status,
        prior_completed_count=len(prior_completed_durations),
        max_prior_completed_duration=max_duration,
        km_median_remaining=_km_conditional_median_remaining(
            kmf,
            current_age_days,
        ),
        weibull_median_remaining=_weibull_conditional_median_remaining(
            wf,
            current_age_days,
        ),
        km_probabilities=km_probs,
        weibull_probabilities=wb_probs,
        weibull_lambda=float(wf.lambda_),
        weibull_rho=float(wf.rho_),
    )


def _brier(probability: float, actual_event: bool) -> float:
    y = 1.0 if actual_event else 0.0
    return float((probability - y) ** 2)


def _prior_completed(
    completed: list[Episode],
    test_ep: Episode,
) -> list[Episode]:
    return [
        ep
        for ep in completed
        if ep.end is not None and ep.end < test_ep.start
    ]


def walk_forward_survival(
    episodes: list[Episode],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    completed = sorted(
        [ep for ep in episodes if ep.completed],
        key=lambda ep: ep.start,
    )

    rows: list[dict] = []
    episode_rows: list[dict] = []

    for test_ep in completed:
        prior = _prior_completed(completed, test_ep)
        if len(prior) < MIN_PRIOR_COMPLETED:
            continue

        prior_durations = [
            int(ep.duration_days)
            for ep in prior
            if ep.duration_days is not None
        ]

        local_rows: list[dict] = []
        for _, state in test_ep.states.iterrows():
            age = int(state["episode_age_days"])
            actual_remaining = int(state["remaining_stress_days"])

            survival = fit_survival_forecast(
                prior_durations,
                current_age_days=age,
            )
            v1 = predict_state(state, prior)

            row = {
                "test_episode_id": test_ep.episode_id,
                "date": state["timestamp"],
                "episode_age_days": age,
                "actual_remaining_days": actual_remaining,
                "prior_completed_count": len(prior),
                "max_prior_completed_duration": (
                    survival.max_prior_completed_duration
                ),
                "support_status": survival.support_status,
                "km_median_remaining": survival.km_median_remaining,
                "weibull_median_remaining": survival.weibull_median_remaining,
                "v1_analog_median_remaining": v1["analog"][
                    "predicted_median"
                ],
                "age_only_median_remaining": v1["baseline"][
                    "predicted_median"
                ],
                "weibull_lambda": survival.weibull_lambda,
                "weibull_rho": survival.weibull_rho,
            }

            for horizon in HORIZONS:
                actual_event = actual_remaining <= horizon
                km_p = survival.km_probabilities[horizon]
                wb_p = survival.weibull_probabilities[horizon]
                v1_p = v1["analog"][f"p_recovery_le_{horizon}d"]
                age_p = v1["baseline"][f"p_recovery_le_{horizon}d"]

                row[f"km_p_recovery_le_{horizon}d"] = km_p
                row[f"weibull_p_recovery_le_{horizon}d"] = wb_p
                row[f"v1_p_recovery_le_{horizon}d"] = v1_p
                row[f"age_p_recovery_le_{horizon}d"] = age_p

                row[f"km_brier_{horizon}d"] = _brier(km_p, actual_event)
                row[f"weibull_brier_{horizon}d"] = _brier(
                    wb_p,
                    actual_event,
                )
                row[f"v1_brier_{horizon}d"] = _brier(v1_p, actual_event)
                row[f"age_brier_{horizon}d"] = _brier(age_p, actual_event)

            rows.append(row)
            local_rows.append(row)

        local = pd.DataFrame(local_rows)
        if local.empty:
            continue

        episode_rows.append(_summarize_episode(local, test_ep))

    return pd.DataFrame(rows), pd.DataFrame(episode_rows)


def _safe_abs_error(
    predicted: pd.Series,
    actual: pd.Series,
) -> pd.Series:
    mask = predicted.notna()
    return (predicted[mask].astype(float) - actual[mask].astype(float)).abs()


def _mean_brier(frame: pd.DataFrame, prefix: str) -> float:
    cols = [f"{prefix}_brier_{h}d" for h in HORIZONS]
    return float(frame[cols].to_numpy(dtype=float).mean())


def _summarize_episode(frame: pd.DataFrame, episode: Episode) -> dict:
    km_err = _safe_abs_error(
        frame["km_median_remaining"],
        frame["actual_remaining_days"],
    )
    wb_err = _safe_abs_error(
        frame["weibull_median_remaining"],
        frame["actual_remaining_days"],
    )
    v1_err = _safe_abs_error(
        frame["v1_analog_median_remaining"],
        frame["actual_remaining_days"],
    )
    age_err = _safe_abs_error(
        frame["age_only_median_remaining"],
        frame["actual_remaining_days"],
    )

    return {
        "test_episode_id": episode.episode_id,
        "start": episode.start,
        "end": episode.end,
        "duration_days": episode.duration_days,
        "scored_states": len(frame),
        "out_of_support_states": int(
            (frame["support_status"] == "OUT_OF_DURATION_SUPPORT").sum()
        ),
        "km_mean_brier": _mean_brier(frame, "km"),
        "weibull_mean_brier": _mean_brier(frame, "weibull"),
        "v1_mean_brier": _mean_brier(frame, "v1"),
        "age_mean_brier": _mean_brier(frame, "age"),
        "km_median_abs_error": (
            float(km_err.median()) if not km_err.empty else None
        ),
        "weibull_median_abs_error": (
            float(wb_err.median()) if not wb_err.empty else None
        ),
        "v1_median_abs_error": (
            float(v1_err.median()) if not v1_err.empty else None
        ),
        "age_median_abs_error": (
            float(age_err.median()) if not age_err.empty else None
        ),
    }


def aggregate_metrics(
    scored: pd.DataFrame,
    episode_metrics: pd.DataFrame,
) -> dict:
    if scored.empty:
        return {
            "scored_states": 0,
            "scored_test_episodes": 0,
        }

    km_err = _safe_abs_error(
        scored["km_median_remaining"],
        scored["actual_remaining_days"],
    )
    wb_err = _safe_abs_error(
        scored["weibull_median_remaining"],
        scored["actual_remaining_days"],
    )
    v1_err = _safe_abs_error(
        scored["v1_analog_median_remaining"],
        scored["actual_remaining_days"],
    )
    age_err = _safe_abs_error(
        scored["age_only_median_remaining"],
        scored["actual_remaining_days"],
    )

    expected_oos = (
        scored["episode_age_days"]
        > scored["max_prior_completed_duration"]
    )
    flagged_oos = scored["support_status"] == "OUT_OF_DURATION_SUPPORT"

    return {
        "scored_states": len(scored),
        "scored_test_episodes": int(scored["test_episode_id"].nunique()),
        "km_mean_brier": _mean_brier(scored, "km"),
        "weibull_mean_brier": _mean_brier(scored, "weibull"),
        "v1_mean_brier": _mean_brier(scored, "v1"),
        "age_mean_brier": _mean_brier(scored, "age"),
        "km_finite_median_states": int(scored["km_median_remaining"].notna().sum()),
        "km_median_abs_error": (
            float(km_err.median()) if not km_err.empty else None
        ),
        "weibull_median_abs_error": (
            float(wb_err.median()) if not wb_err.empty else None
        ),
        "v1_median_abs_error": (
            float(v1_err.median()) if not v1_err.empty else None
        ),
        "age_median_abs_error": (
            float(age_err.median()) if not age_err.empty else None
        ),
        "out_of_support_states": int(flagged_oos.sum()),
        "out_of_support_fraction": float(flagged_oos.mean()),
        "support_flag_accuracy": float((expected_oos == flagged_oos).mean()),
        "episode_metrics_count": len(episode_metrics),
    }


def diagnostic_verdict(aggregate: dict) -> tuple[str, dict]:
    episodes = int(aggregate.get("scored_test_episodes", 0))
    states = int(aggregate.get("scored_states", 0))

    if episodes < 3 or states < 30:
        return (
            "INSUFFICIENT_INDEPENDENT_EPISODES",
            {
                "minimum_episodes": 3,
                "minimum_states": 30,
                "observed_episodes": episodes,
                "observed_states": states,
                "pass": False,
            },
        )

    checks = {
        "weibull_brier_better_than_v1": (
            aggregate["weibull_mean_brier"]
            < aggregate["v1_mean_brier"]
        ),
        "km_brier_not_worse_than_age": (
            aggregate["km_mean_brier"]
            <= aggregate["age_mean_brier"]
        ),
        "weibull_median_error_not_worse_than_v1": (
            aggregate["weibull_median_abs_error"]
            <= aggregate["v1_median_abs_error"]
        ),
        "support_flags_exact": (
            aggregate["support_flag_accuracy"] == 1.0
        ),
    }

    verdict = (
        "CENSOR_AWARE_SURVIVAL_SIGNAL_V2"
        if all(checks.values())
        else "SURVIVAL_V2_NOT_YET_BETTER"
    )
    return verdict, {
        "checks": checks,
        "all_checks_pass": all(checks.values()),
    }


def replay_2026(
    episodes: list[Episode],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    completed = [ep for ep in episodes if ep.completed]
    replay_start = pd.Timestamp("2026-03-29", tz="UTC")
    targets = [
        ep
        for ep in completed
        if ep.end is not None
        and ep.end >= replay_start
        and ep.start <= REPLAY_END
    ]

    rows: list[dict] = []
    summaries: list[dict] = []

    for test_ep in targets:
        prior = _prior_completed(completed, test_ep)
        if len(prior) < MIN_PRIOR_COMPLETED:
            continue

        prior_durations = [
            int(ep.duration_days)
            for ep in prior
            if ep.duration_days is not None
        ]

        states = test_ep.states[
            (test_ep.states["timestamp"] >= replay_start)
            & (test_ep.states["timestamp"] <= REPLAY_END)
        ]

        local: list[dict] = []
        for _, state in states.iterrows():
            age = int(state["episode_age_days"])
            actual_remaining = int(state["remaining_stress_days"])
            survival = fit_survival_forecast(
                prior_durations,
                current_age_days=age,
            )
            v1 = predict_state(state, prior)

            row = {
                "episode_id": test_ep.episode_id,
                "date": state["timestamp"],
                "episode_age_days": age,
                "actual_remaining_days": actual_remaining,
                "support_status": survival.support_status,
                "max_prior_completed_duration": (
                    survival.max_prior_completed_duration
                ),
                "km_median_remaining": survival.km_median_remaining,
                "weibull_median_remaining": survival.weibull_median_remaining,
                "v1_analog_median_remaining": v1["analog"][
                    "predicted_median"
                ],
                "weibull_lambda": survival.weibull_lambda,
                "weibull_rho": survival.weibull_rho,
            }
            for horizon in HORIZONS:
                row[f"km_p_recovery_le_{horizon}d"] = (
                    survival.km_probabilities[horizon]
                )
                row[f"weibull_p_recovery_le_{horizon}d"] = (
                    survival.weibull_probabilities[horizon]
                )
                row[f"v1_p_recovery_le_{horizon}d"] = v1["analog"][
                    f"p_recovery_le_{horizon}d"
                ]
            rows.append(row)
            local.append(row)

        local_df = pd.DataFrame(local)
        if not local_df.empty:
            summaries.append(
                {
                    "episode_id": test_ep.episode_id,
                    "start": test_ep.start,
                    "end": test_ep.end,
                    "duration_days": test_ep.duration_days,
                    "states": len(local_df),
                    "out_of_support_states": int(
                        (
                            local_df["support_status"]
                            == "OUT_OF_DURATION_SUPPORT"
                        ).sum()
                    ),
                    "out_of_support_fraction": float(
                        (
                            local_df["support_status"]
                            == "OUT_OF_DURATION_SUPPORT"
                        ).mean()
                    ),
                    "weibull_median_abs_error": float(
                        (
                            local_df["weibull_median_remaining"]
                            - local_df["actual_remaining_days"]
                        )
                        .abs()
                        .median()
                    ),
                    "v1_median_abs_error": float(
                        (
                            local_df["v1_analog_median_remaining"]
                            - local_df["actual_remaining_days"]
                        )
                        .abs()
                        .median()
                    ),
                }
            )

    return pd.DataFrame(rows), pd.DataFrame(summaries)


def main() -> int:
    run_id = f"ADAPT_STRESS_SURVIVAL_V2_{_source_commit()[:12]}"
    run_dir = (
        REPO_ROOT
        / "research_artifacts"
        / "adaptive_stress_survival_v2"
        / run_id
    )
    run_dir.mkdir(parents=True, exist_ok=True)

    dev_panel, dev_meta = download_panel(HIST_START, DEV_END)
    dev_panel = prepare_duration_features(dev_panel)
    dev_episodes = extract_stress_episodes(dev_panel)

    scored, episode_metrics = walk_forward_survival(dev_episodes)
    aggregate = aggregate_metrics(scored, episode_metrics)
    verdict, gate = diagnostic_verdict(aggregate)

    scored.to_csv(run_dir / "development_survival_predictions.csv", index=False)
    episode_metrics.to_csv(
        run_dir / "development_episode_metrics.csv",
        index=False,
    )

    replay_panel, replay_meta = download_panel(HIST_START, REPLAY_END)
    replay_panel = prepare_duration_features(replay_panel)
    replay_episodes = extract_stress_episodes(replay_panel)
    replay_rows, replay_summary = replay_2026(replay_episodes)

    replay_rows.to_csv(run_dir / "posthoc_2026_survival_path.csv", index=False)
    replay_summary.to_csv(
        run_dir / "posthoc_2026_survival_summary.csv",
        index=False,
    )

    report = {
        "run_id": run_id,
        "source_commit_sha": _source_commit(),
        "status": verdict,
        "model_family": "ADAPTIVE_STRESS_SURVIVAL_V2",
        "lifelines_version": "0.30.3",
        "lifelines_upstream_tag_commit": (
            "a21e4328fa30bc107ae2a3e0276ec57e892e6504"
        ),
        "development": {
            "data_end": DEV_END.date().isoformat(),
            "dataset": dev_meta,
            "aggregate_metrics": aggregate,
            "diagnostic_gate": gate,
            "episode_metrics": episode_metrics.astype(object)
            .where(pd.notna(episode_metrics), None)
            .to_dict(orient="records"),
        },
        "posthoc_2026_replay": {
            "label": "POST_HOC_2026_SURVIVAL_REPLAY_NOT_VALIDATION",
            "dataset": replay_meta,
            "episode_summary": replay_summary.astype(object)
            .where(pd.notna(replay_summary), None)
            .to_dict(orient="records"),
        },
        "production_impact": "NONE",
        "paper_live_impact": "NONE",
        "migration_required": False,
    }
    _write_json(run_dir / "summary.json", report)

    print("development_aggregate:")
    print(json.dumps(aggregate, indent=2))
    print("diagnostic_gate:")
    print(json.dumps(gate, indent=2))
    print(f"diagnostic_verdict={verdict}")

    print("development_episode_metrics:")
    if episode_metrics.empty:
        print("NONE")
    else:
        print(episode_metrics.to_string(index=False))

    print("posthoc_2026_summary:")
    if replay_summary.empty:
        print("NONE")
    else:
        print(replay_summary.to_string(index=False))

    print("posthoc_2026_checkpoints:")
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
            cols = [
                "episode_id",
                "date",
                "episode_age_days",
                "actual_remaining_days",
                "support_status",
                "max_prior_completed_duration",
                "km_median_remaining",
                "weibull_median_remaining",
                "v1_analog_median_remaining",
                "km_p_recovery_le_30d",
                "weibull_p_recovery_le_30d",
                "v1_p_recovery_le_30d",
            ]
            print(group.iloc[indices][cols].to_string(index=False))

    print(f"output={run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
