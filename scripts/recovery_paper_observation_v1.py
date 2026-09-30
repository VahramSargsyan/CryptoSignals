from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import lifelines
import pandas as pd
import sklearn
from lifelines import KaplanMeierFitter, WeibullFitter
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient

REPO_ROOT = Path(__file__).resolve().parents[1]

ASSETS = ("ATOM", "TWT", "PEPE", "BNB", "SOL", "TRX", "AAVE", "LINK")
SYMBOLS = {asset: f"{asset}USDT" for asset in ASSETS}
HIST_START = pd.Timestamp("2023-05-05", tz="UTC")

SMA_DAYS = 200
VOL_DAYS = 30
ENTER_MAX = 3
EXIT_MIN = 5
CONFIRM_DAYS = 3
HORIZONS = (7, 14, 30, 45, 60)
MIN_PRIOR_COMPLETED = 3


@dataclass(frozen=True)
class Episode:
    episode_id: str
    start: pd.Timestamp
    end: pd.Timestamp | None
    completed: bool
    duration_days: int | None
    states: pd.DataFrame


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


def download_panel(
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> tuple[pd.DataFrame, dict]:
    client = BinanceSpotRestClient()
    cutoff = end + pd.Timedelta(days=1)
    pieces: list[pd.DataFrame] = []
    metadata: dict[str, dict] = {}

    for asset in ASSETS:
        result = download_historical_dataset(
            client,
            symbol=SYMBOLS[asset],
            start=start,
            end=cutoff,
            timeframe="1D",
            as_of=cutoff,
        )
        if result.dataset is None:
            raise RuntimeError(
                f"No dataset for {asset}: {result.metadata.status}"
            )
        if result.dataset.quality.has_critical_issues:
            raise RuntimeError(
                f"Critical data quality issue for {asset}: "
                f"{result.dataset.quality}"
            )

        frame = result.dataset.candles[["timestamp", "close"]].copy()
        frame = frame.rename(columns={"close": f"{asset}_close"})
        pieces.append(frame)
        metadata[asset] = {
            "dataset_id": result.dataset.dataset_id,
            "rows": len(frame),
            "actual_start": result.metadata.actual_start,
            "actual_end": result.metadata.actual_end,
            "listing_truncated": result.metadata.listing_truncated,
            "quality": result.dataset.quality.__dict__,
        }

    panel = pieces[0]
    for frame in pieces[1:]:
        panel = panel.merge(
            frame,
            on="timestamp",
            how="inner",
            validate="one_to_one",
        )
    panel = panel.sort_values("timestamp").reset_index(drop=True)

    expected = pd.date_range(
        panel["timestamp"].min(),
        panel["timestamp"].max(),
        freq="D",
        tz="UTC",
    )
    actual = pd.DatetimeIndex(panel["timestamp"])
    missing = expected.difference(actual)
    metadata["panel"] = {
        "rows": len(panel),
        "start": panel["timestamp"].min().isoformat(),
        "end": panel["timestamp"].max().isoformat(),
        "missing_common_dates": [x.isoformat() for x in missing],
    }
    return panel, metadata


def prepare_duration_features(panel: pd.DataFrame) -> pd.DataFrame:
    out = panel.copy()
    breadth_parts = []
    gap_cols = []
    vol_cols = []

    for asset in ASSETS:
        close_col = f"{asset}_close"
        sma_col = f"{asset}_sma{SMA_DAYS}"
        gap_col = f"{asset}_sma200_gap"
        vol_col = f"{asset}_vol{VOL_DAYS}"

        out[sma_col] = out[close_col].rolling(
            SMA_DAYS,
            min_periods=SMA_DAYS,
        ).mean()
        out[vol_col] = (
            out[close_col]
            .pct_change()
            .rolling(VOL_DAYS, min_periods=VOL_DAYS)
            .std()
        )
        out[gap_col] = out[close_col] / out[sma_col] - 1.0

        breadth_parts.append(
            (out[close_col] > out[sma_col]).astype(int)
        )
        gap_cols.append(gap_col)
        vol_cols.append(vol_col)

    out["breadth_sma200"] = sum(breadth_parts)
    out["breadth_delta_7d"] = (
        out["breadth_sma200"] - out["breadth_sma200"].shift(7)
    )
    out["median_sma200_gap"] = out[gap_cols].median(axis=1)
    out["dispersion_sma200_gap"] = out[gap_cols].std(axis=1, ddof=0)
    out["median_vol30"] = out[vol_cols].median(axis=1)

    required = (
        [f"{asset}_sma{SMA_DAYS}" for asset in ASSETS]
        + [f"{asset}_vol{VOL_DAYS}" for asset in ASSETS]
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
    if not (
        math.isfinite(lam)
        and math.isfinite(rho)
        and lam > 0
        and rho > 0
    ):
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
    wb_s_now = float(
        wf.survival_function_at_times(current_age_days).iloc[0]
    )

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
