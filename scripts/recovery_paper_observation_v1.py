from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
from pathlib import Path
from typing import Any

import lifelines
import pandas as pd
import sklearn
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

from scripts.research_adaptive_stress_duration_v1 import (
    HORIZONS,
    Episode,
    extract_stress_episodes,
    prepare_duration_features,
)
from scripts.research_adaptive_stress_survival_v2 import (
    MIN_PRIOR_COMPLETED,
    fit_survival_forecast,
)
from scripts.research_defensive_low_vol_untouched import HIST_START
from scripts.research_relative_rotation_graph_intelligence import (
    ASSETS,
    download_panel,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_ID = "RECOVERY_PAPER_OBSERVATION_V1"
STATE_HORIZONS = (7, 14)

REPRO_CUTOFF = pd.Timestamp("2026-03-28", tz="UTC")
EXPECTED_REPRO_BRIER = {
    7: 0.2104,
    14: 0.2066,
}
REPRO_TOLERANCE = 0.0010

DEFAULT_OUTPUT_ROOT = REPO_ROOT / "paper_artifacts" / "recovery_paper_observation_v1"
DEFAULT_JOURNAL_PATH = (
    REPO_ROOT / "paper_observations" / "recovery_v1" / "observations.csv"
)

JOURNAL_COLUMNS = [
    "observation_candle",
    "generated_at",
    "model_id",
    "source_commit_sha",
    "market_status",
    "stress_episode_id",
    "stress_start",
    "episode_age_days",
    "stress_entry_streak",
    "recovery_confirmation_streak",
    "completed_training_episodes",
    "breadth_sma200",
    "median_sma200_gap",
    "breadth_delta_7d",
    "state_p_recovery_le_7d",
    "state_p_recovery_le_14d",
    "support_status",
    "max_prior_completed_duration",
    "km_median_remaining",
    "weibull_median_remaining",
    "km_p_recovery_le_7d",
    "km_p_recovery_le_14d",
    "km_p_recovery_le_30d",
    "km_p_recovery_le_45d",
    "km_p_recovery_le_60d",
    "weibull_p_recovery_le_7d",
    "weibull_p_recovery_le_14d",
    "weibull_p_recovery_le_30d",
    "weibull_p_recovery_le_45d",
    "weibull_p_recovery_le_60d",
    "latest_completed_stress_end",
    "latest_completed_stress_duration_days",
    "dataset_ids_json",
    "panel_start",
    "panel_end",
    "panel_rows",
    "missing_common_dates_count",
    "paper_only",
]


def _utc(value: Any) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _source_commit() -> str:
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO_ROOT,
        text=True,
    ).strip()


def latest_fully_closed_d1(now: Any | None = None) -> pd.Timestamp:
    current = pd.Timestamp.now(tz="UTC") if now is None else _utc(now)
    return current.floor("D") - pd.Timedelta(days=1)


def _json_safe(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if pd.isna(value):
        return None
    return value


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def _episode_training_frame(episode: Episode, horizon: int) -> pd.DataFrame:
    if not episode.completed or episode.end is None:
        raise ValueError("training episode must be completed")

    # The confirming recovery close is excluded. The model must predict recovery
    # before the frozen breadth>=5 x3 rule has already declared it.
    states = episode.states.iloc[:-1].copy()
    if states.empty:
        raise ValueError(f"{episode.episode_id}: no pre-recovery states")

    frame = states[
        ["breadth_sma200", "median_sma200_gap", "remaining_stress_days"]
    ].copy()
    frame["target"] = (
        frame["remaining_stress_days"].astype(int) <= int(horizon)
    ).astype(int)
    frame["sample_weight"] = 1.0 / float(len(frame))
    return frame


def fit_state_recovery_model(
    prior_completed: list[Episode],
    *,
    horizon: int,
) -> tuple[StandardScaler, LogisticRegression | None, float | None]:
    if horizon not in STATE_HORIZONS:
        raise ValueError(f"unsupported state horizon: {horizon}")
    if len(prior_completed) < MIN_PRIOR_COMPLETED:
        raise ValueError(
            f"Need at least {MIN_PRIOR_COMPLETED} prior completed episodes"
        )

    frames = [
        _episode_training_frame(episode, horizon)
        for episode in prior_completed
    ]
    train = pd.concat(frames, ignore_index=True)

    features = train[["breadth_sma200", "median_sma200_gap"]].astype(float)
    target = train["target"].astype(int)
    weights = train["sample_weight"].astype(float)

    scaler = StandardScaler().fit(features)
    if target.nunique() < 2:
        return scaler, None, float(target.mean())

    model = LogisticRegression(
        C=1.0,
        solver="lbfgs",
        max_iter=1000,
    )
    model.fit(
        scaler.transform(features),
        target,
        sample_weight=weights,
    )
    return scaler, model, None


def predict_state_recovery_probability(
    fitted: tuple[StandardScaler, LogisticRegression | None, float | None],
    row: pd.Series,
) -> float:
    scaler, model, constant = fitted
    if model is None:
        if constant is None:
            raise RuntimeError("constant probability is missing")
        return float(constant)

    frame = pd.DataFrame(
        [
            {
                "breadth_sma200": float(row["breadth_sma200"]),
                "median_sma200_gap": float(row["median_sma200_gap"]),
            }
        ]
    )
    return float(model.predict_proba(scaler.transform(frame))[0, 1])


def _prior_completed(
    episodes: list[Episode],
    *,
    before: pd.Timestamp,
) -> list[Episode]:
    return sorted(
        [
            ep
            for ep in episodes
            if ep.completed
            and ep.end is not None
            and ep.end < before
        ],
        key=lambda ep: ep.start,
    )


def _trailing_streak(values: pd.Series, predicate) -> int:
    streak = 0
    for value in reversed(values.tolist()):
        if predicate(int(value)):
            streak += 1
        else:
            break
    return streak


def _current_episode(
    episodes: list[Episode],
    latest_candle: pd.Timestamp,
) -> Episode | None:
    active = [
        ep
        for ep in episodes
        if not ep.completed
        and not ep.states.empty
        and pd.Timestamp(ep.states.iloc[-1]["timestamp"]) == latest_candle
    ]
    if len(active) > 1:
        raise RuntimeError("multiple active stress episodes")
    return active[0] if active else None


def _market_status(
    features: pd.DataFrame,
    active_episode: Episode | None,
) -> tuple[str, int, int]:
    valid = features[features["duration_features_valid"]].copy()
    if valid.empty:
        raise RuntimeError("no valid duration-feature rows")

    breadth = valid["breadth_sma200"].astype(int)
    entry_streak = _trailing_streak(breadth, lambda x: x <= 3)

    if active_episode is None:
        pending = min(entry_streak, 2)
        if pending > 0:
            return f"STRESS_ENTRY_PENDING_{pending}_OF_3", entry_streak, 0
        return "NORMAL", entry_streak, 0

    recovery_streak = _trailing_streak(
        active_episode.states["breadth_sma200"].astype(int),
        lambda x: x >= 5,
    )
    if recovery_streak > 0:
        return (
            f"RECOVERY_CONFIRMATION_{min(recovery_streak, 2)}_OF_3",
            entry_streak,
            recovery_streak,
        )
    return "STRESS_ACTIVE", entry_streak, recovery_streak


def _latest_completed_episode(episodes: list[Episode]) -> Episode | None:
    completed = [
        ep
        for ep in episodes
        if ep.completed and ep.end is not None
    ]
    return max(completed, key=lambda ep: ep.end) if completed else None


def _dataset_ids(metadata: dict) -> dict[str, str]:
    return {
        asset: str(metadata[asset]["dataset_id"])
        for asset in ASSETS
    }


def build_observation(
    features: pd.DataFrame,
    metadata: dict,
    *,
    source_commit_sha: str,
    generated_at: pd.Timestamp,
) -> tuple[dict, list[Episode]]:
    valid = features[features["duration_features_valid"]].copy()
    if valid.empty:
        raise RuntimeError("no valid feature rows")

    latest_row = valid.iloc[-1]
    latest_candle = pd.Timestamp(latest_row["timestamp"])
    episodes = extract_stress_episodes(features)
    active = _current_episode(episodes, latest_candle)
    market_status, entry_streak, recovery_streak = _market_status(
        features,
        active,
    )

    completed = sorted(
        [ep for ep in episodes if ep.completed and ep.end is not None],
        key=lambda ep: ep.start,
    )
    latest_completed = _latest_completed_episode(episodes)

    row: dict[str, Any] = {
        "observation_candle": latest_candle.isoformat(),
        "generated_at": generated_at.isoformat(),
        "model_id": MODEL_ID,
        "source_commit_sha": source_commit_sha,
        "market_status": market_status,
        "stress_episode_id": None,
        "stress_start": None,
        "episode_age_days": None,
        "stress_entry_streak": int(entry_streak),
        "recovery_confirmation_streak": int(recovery_streak),
        "completed_training_episodes": len(completed),
        "breadth_sma200": int(latest_row["breadth_sma200"]),
        "median_sma200_gap": float(latest_row["median_sma200_gap"]),
        "breadth_delta_7d": float(latest_row["breadth_delta_7d"]),
        "state_p_recovery_le_7d": None,
        "state_p_recovery_le_14d": None,
        "support_status": None,
        "max_prior_completed_duration": None,
        "km_median_remaining": None,
        "weibull_median_remaining": None,
        "latest_completed_stress_end": (
            latest_completed.end.isoformat()
            if latest_completed is not None and latest_completed.end is not None
            else None
        ),
        "latest_completed_stress_duration_days": (
            int(latest_completed.duration_days)
            if latest_completed is not None
            and latest_completed.duration_days is not None
            else None
        ),
        "dataset_ids_json": json.dumps(
            _dataset_ids(metadata),
            sort_keys=True,
            separators=(",", ":"),
        ),
        "panel_start": str(metadata["panel"]["start"]),
        "panel_end": str(metadata["panel"]["end"]),
        "panel_rows": int(metadata["panel"]["rows"]),
        "missing_common_dates_count": len(
            metadata["panel"].get("missing_common_dates", [])
        ),
        "paper_only": True,
    }

    for horizon in HORIZONS:
        row[f"km_p_recovery_le_{horizon}d"] = None
        row[f"weibull_p_recovery_le_{horizon}d"] = None

    if active is None:
        return row, episodes

    prior = _prior_completed(episodes, before=active.start)
    active_latest = active.states.iloc[-1]
    row["stress_episode_id"] = active.episode_id
    row["stress_start"] = active.start.isoformat()
    row["episode_age_days"] = int(active_latest["episode_age_days"])
    row["completed_training_episodes"] = len(prior)

    if len(prior) < MIN_PRIOR_COMPLETED:
        row["support_status"] = "INSUFFICIENT_COMPLETED_HISTORY"
        return row, episodes

    for horizon in STATE_HORIZONS:
        fitted = fit_state_recovery_model(prior, horizon=horizon)
        row[f"state_p_recovery_le_{horizon}d"] = (
            predict_state_recovery_probability(fitted, active_latest)
        )

    durations = [
        int(ep.duration_days)
        for ep in prior
        if ep.duration_days is not None
    ]
    survival = fit_survival_forecast(
        durations,
        current_age_days=int(active_latest["episode_age_days"]),
    )
    row["support_status"] = survival.support_status
    row["max_prior_completed_duration"] = (
        survival.max_prior_completed_duration
    )
    row["km_median_remaining"] = survival.km_median_remaining
    row["weibull_median_remaining"] = survival.weibull_median_remaining

    for horizon in HORIZONS:
        row[f"km_p_recovery_le_{horizon}d"] = (
            survival.km_probabilities[horizon]
        )
        row[f"weibull_p_recovery_le_{horizon}d"] = (
            survival.weibull_probabilities[horizon]
        )

    return row, episodes


def historical_reproduction_gate(
    features: pd.DataFrame,
) -> dict:
    scoped = features[
        features["timestamp"] <= REPRO_CUTOFF
    ].copy()
    episodes = sorted(
        [
            ep
            for ep in extract_stress_episodes(scoped)
            if ep.completed and ep.end is not None
        ],
        key=lambda ep: ep.start,
    )

    by_horizon: dict[int, list[float]] = {
        horizon: [] for horizon in STATE_HORIZONS
    }
    scored_episode_ids: list[str] = []

    for index, test_episode in enumerate(episodes):
        prior = episodes[:index]
        if len(prior) < MIN_PRIOR_COMPLETED:
            continue

        test_states = test_episode.states.iloc[:-1].copy()
        if test_states.empty:
            continue

        scored_episode_ids.append(test_episode.episode_id)
        for horizon in STATE_HORIZONS:
            fitted = fit_state_recovery_model(prior, horizon=horizon)
            errors: list[float] = []
            for _, state in test_states.iterrows():
                probability = predict_state_recovery_probability(
                    fitted,
                    state,
                )
                actual = (
                    int(state["remaining_stress_days"]) <= horizon
                )
                y = 1.0 if actual else 0.0
                errors.append((probability - y) ** 2)
            by_horizon[horizon].append(float(pd.Series(errors).mean()))

    observed = {
        horizon: float(pd.Series(scores).mean())
        for horizon, scores in by_horizon.items()
    }
    checks = {
        horizon: abs(observed[horizon] - EXPECTED_REPRO_BRIER[horizon])
        <= REPRO_TOLERANCE
        for horizon in STATE_HORIZONS
    }
    return {
        "status": "PASS" if all(checks.values()) else "FAIL",
        "cutoff": REPRO_CUTOFF.isoformat(),
        "scored_episode_ids": scored_episode_ids,
        "observed_episode_balanced_brier": observed,
        "expected_episode_balanced_brier": EXPECTED_REPRO_BRIER,
        "tolerance": REPRO_TOLERANCE,
        "checks": checks,
    }


def _empty_journal() -> pd.DataFrame:
    return pd.DataFrame(columns=JOURNAL_COLUMNS)


def load_journal(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return _empty_journal()

    try:
        frame = pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return _empty_journal()
    missing = [column for column in JOURNAL_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"journal missing columns: {missing}")
    if frame["observation_candle"].duplicated().any():
        raise ValueError("journal contains duplicate observation_candle values")
    return frame[JOURNAL_COLUMNS].copy()


def append_observation(
    path: Path,
    observation: dict,
) -> tuple[pd.DataFrame, bool]:
    frame = load_journal(path)
    key = str(observation["observation_candle"])
    if not frame.empty and key in set(frame["observation_candle"].astype(str)):
        return frame, False

    row = {
        column: _json_safe(observation.get(column))
        for column in JOURNAL_COLUMNS
    }
    updated = pd.concat(
        [frame, pd.DataFrame([row], columns=JOURNAL_COLUMNS)],
        ignore_index=True,
    )
    updated["_sort"] = pd.to_datetime(
        updated["observation_candle"],
        utc=True,
    )
    updated = (
        updated.sort_values("_sort")
        .drop(columns=["_sort"])
        .reset_index(drop=True)
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    updated.to_csv(path, index=False)
    return updated, True


def resolve_history(
    journal: pd.DataFrame,
    episodes: list[Episode],
) -> pd.DataFrame:
    completed_by_start = {
        ep.start.isoformat(): ep
        for ep in episodes
        if ep.completed and ep.end is not None
    }

    rows: list[dict] = []
    for _, source in journal.iterrows():
        row = source.to_dict()
        stress_start = source.get("stress_start")
        episode = (
            completed_by_start.get(str(stress_start))
            if pd.notna(stress_start)
            else None
        )

        actual_recovery = None
        actual_remaining = None
        if episode is not None and episode.end is not None:
            observation_candle = _utc(source["observation_candle"])
            actual_recovery = episode.end.isoformat()
            actual_remaining = int((episode.end - observation_candle).days)

        row["resolved"] = episode is not None
        row["actual_recovery_candle"] = actual_recovery
        row["actual_remaining_days"] = actual_remaining

        for horizon in STATE_HORIZONS:
            probability = source.get(f"state_p_recovery_le_{horizon}d")
            if (
                episode is not None
                and actual_remaining is not None
                and pd.notna(probability)
            ):
                y = 1.0 if actual_remaining <= horizon else 0.0
                row[f"state_brier_{horizon}d"] = (
                    float(probability) - y
                ) ** 2
            else:
                row[f"state_brier_{horizon}d"] = None

        rows.append(row)

    return pd.DataFrame(rows)


def episode_catalog(episodes: list[Episode]) -> pd.DataFrame:
    rows = []
    for episode in episodes:
        rows.append(
            {
                "episode_id": episode.episode_id,
                "start": episode.start,
                "end": episode.end,
                "completed": episode.completed,
                "duration_days": episode.duration_days,
                "state_rows": len(episode.states),
            }
        )
    return pd.DataFrame(rows)


def _report_markdown(
    observation: dict,
    reproduction: dict,
    journal_appended: bool,
) -> str:
    lines = [
        "# Recovery Paper Observation V1",
        "",
        f"Observation candle: **{observation['observation_candle']}**",
        f"Market status: **{observation['market_status']}**",
        f"Reproduction gate: **{reproduction['status']}**",
        f"Journal: **{'APPENDED' if journal_appended else 'ALREADY_OBSERVED'}**",
        "",
        "## Current market state",
        "",
        f"- Breadth above SMA200: {observation['breadth_sma200']}/8",
        f"- Median SMA200 gap: {float(observation['median_sma200_gap']) * 100:+.2f}%",
        f"- Breadth delta 7d: {float(observation['breadth_delta_7d']):+.0f}",
    ]

    if observation.get("stress_start"):
        lines.extend(
            [
                f"- Stress start: {observation['stress_start']}",
                f"- Stress age: {observation['episode_age_days']}d",
                f"- Recovery confirmation streak: {observation['recovery_confirmation_streak']}/3",
                f"- Duration support: {observation['support_status']}",
                "",
                "## Recovery evidence",
                "",
                f"- State P(recovery <=7d): {float(observation['state_p_recovery_le_7d'] or 0.0):.4f}",
                f"- State P(recovery <=14d): {float(observation['state_p_recovery_le_14d'] or 0.0):.4f}",
                f"- KM median remaining: {observation['km_median_remaining']}",
                f"- Weibull median remaining: {observation['weibull_median_remaining']}",
            ]
        )

    lines.extend(
        [
            "",
            "## Safety",
            "",
            "- PAPER OBSERVATION ONLY.",
            "- No BUY/SELL/order output exists in this module.",
            "- Predictions never modify the real strategy.",
            "- Historical prediction rows are append-only.",
            "",
        ]
    )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Forward paper observation for crypto stress recovery models."
    )
    parser.add_argument(
        "--cutoff-date",
        help="Inclusive latest fully closed UTC D1 candle date. Default: yesterday UTC.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
    )
    parser.add_argument(
        "--journal-path",
        type=Path,
        default=DEFAULT_JOURNAL_PATH,
    )
    parser.add_argument(
        "--append-journal",
        action="store_true",
        help="Append exactly one row for a new closed candle.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    source_commit_sha = _source_commit()
    generated_at = pd.Timestamp.now(tz="UTC")
    cutoff = (
        _utc(args.cutoff_date).floor("D")
        if args.cutoff_date
        else latest_fully_closed_d1(generated_at)
    )

    panel, metadata = download_panel(HIST_START, cutoff)
    if panel.empty:
        raise RuntimeError("downloaded common panel is empty")
    latest_downloaded = pd.Timestamp(panel.iloc[-1]["timestamp"])
    if latest_downloaded != cutoff:
        raise RuntimeError(
            f"latest common candle {latest_downloaded} != requested cutoff {cutoff}"
        )

    features = prepare_duration_features(panel)
    reproduction = historical_reproduction_gate(features)
    if reproduction["status"] != "PASS":
        raise RuntimeError(
            "historical reproduction gate failed: "
            + json.dumps(reproduction, default=str, sort_keys=True)
        )

    observation, episodes = build_observation(
        features,
        metadata,
        source_commit_sha=source_commit_sha,
        generated_at=generated_at,
    )

    run_id = (
        f"{MODEL_ID}_{cutoff.date().isoformat()}_{source_commit_sha[:12]}"
    )
    run_dir = args.output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    journal = load_journal(args.journal_path)
    journal_appended = False
    if args.append_journal:
        journal, journal_appended = append_observation(
            args.journal_path,
            observation,
        )

    resolved = resolve_history(journal, episodes)

    _write_json(run_dir / "observation.json", observation)
    pd.DataFrame([observation]).to_csv(
        run_dir / "observation.csv",
        index=False,
    )
    episode_catalog(episodes).to_csv(
        run_dir / "episode_catalog.csv",
        index=False,
    )
    resolved.to_csv(
        run_dir / "resolved_history.csv",
        index=False,
    )

    manifest = {
        "model_id": MODEL_ID,
        "run_id": run_id,
        "source_commit_sha": source_commit_sha,
        "generated_at": generated_at.isoformat(),
        "cutoff": cutoff.isoformat(),
        "paper_only": True,
        "journal_path": str(args.journal_path),
        "journal_appended": journal_appended,
        "reproduction_gate": reproduction,
        "dependencies": {
            "pandas": pd.__version__,
            "scikit_learn": sklearn.__version__,
            "lifelines": lifelines.__version__,
        },
        "dataset": metadata,
        "production_impact": "NONE",
        "paper_live_strategy_impact": "NONE",
        "migration_required": False,
    }
    _write_json(run_dir / "run_manifest.json", manifest)

    report = _report_markdown(
        observation,
        reproduction,
        journal_appended,
    )
    (run_dir / "report.md").write_text(report, encoding="utf-8")

    print(report)
    print(f"run_dir={run_dir}")
    print(f"journal_path={args.journal_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
