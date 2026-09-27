from __future__ import annotations

import argparse
import io
import json
import math
import os
import subprocess
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from scripts.research_defensive_low_vol_untouched import (
    ASSETS,
    ENTER_BREADTH_MAX,
    EXIT_BREADTH_MIN,
    CONFIRM_DAYS,
    SMA_DAYS,
    add_defensive_features,
)
from scripts.research_relative_rotation_graph_intelligence import download_panel

REPO_ROOT = Path(__file__).resolve().parents[1]

MACRO_HISTORY_START = pd.Timestamp("2021-01-01")
CRYPTO_DOWNLOAD_START = pd.Timestamp("2023-05-05", tz="UTC")
ANALYSIS_END = pd.Timestamp("2026-09-26", tz="UTC")
MIN_COMPONENTS = 4

FRED_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"


@dataclass(frozen=True)
class MacroSpec:
    series_id: str
    label: str
    transform: str
    direction: float
    z_window: int
    z_min_periods: int
    availability_lag_days: int = 0


MACRO_SPECS = (
    MacroSpec("VIXCLS", "VIX level", "level", +1.0, 252, 126, 0),
    MacroSpec("DTWEXBGS", "Broad USD 20-observation return", "pct20", +1.0, 252, 126, 0),
    MacroSpec("DFII10", "10Y real yield 20-observation change", "diff20", +1.0, 252, 126, 0),
    MacroSpec("BAA10Y", "Baa-Treasury spread 20-observation change", "diff20", +1.0, 252, 126, 0),
    MacroSpec("NASDAQCOM", "Nasdaq 20-observation return, inverted", "pct20", -1.0, 252, 126, 0),
    # NFCI is observed for a week ending Friday and normally published the following Wednesday.
    # A conservative +5 calendar-day availability lag avoids using the Friday label as if known on Friday.
    MacroSpec("NFCI", "Chicago Fed NFCI level", "level", +1.0, 52, 26, 5),
)


def _source_commit() -> str:
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO_ROOT,
        text=True,
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


def rolling_zscore(series: pd.Series, window: int, min_periods: int) -> pd.Series:
    mean = series.rolling(window, min_periods=min_periods).mean()
    std = series.rolling(window, min_periods=min_periods).std(ddof=0)
    return (series - mean) / std.replace(0.0, np.nan)


def availability_date(observation_date: pd.Series, lag_days: int) -> pd.Series:
    return pd.to_datetime(observation_date) + pd.to_timedelta(lag_days, unit="D")


def _transform(values: pd.Series, transform: str) -> pd.Series:
    if transform == "level":
        return values.astype(float)
    if transform == "pct20":
        return values.astype(float).pct_change(20, fill_method=None)
    if transform == "diff20":
        return values.astype(float).diff(20)
    raise ValueError(f"Unknown transform: {transform}")


def fetch_fred_component(
    spec: MacroSpec,
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> tuple[pd.DataFrame, dict]:
    query = urllib.parse.urlencode(
        {
            "id": spec.series_id,
            "cosd": start.date().isoformat(),
            "coed": end.date().isoformat(),
        }
    )
    request = urllib.request.Request(
        f"{FRED_URL}?{query}",
        headers={"User-Agent": "CryptoSignals research/global-macro-risk-regime-v1"},
    )
    payload = None
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=90) as response:
                payload = response.read().decode("utf-8")
            break
        except (TimeoutError, OSError) as exc:
            last_error = exc
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)
    if payload is None:
        raise RuntimeError(f"FRED download failed for {spec.series_id}") from last_error

    raw = pd.read_csv(io.StringIO(payload))
    if raw.shape[1] < 2:
        raise RuntimeError(f"Unexpected FRED payload for {spec.series_id}")

    date_col = raw.columns[0]
    value_col = raw.columns[1]
    x = pd.DataFrame(
        {
            "observation_date": pd.to_datetime(raw[date_col], errors="coerce"),
            "value": pd.to_numeric(raw[value_col], errors="coerce"),
        }
    ).dropna(subset=["observation_date", "value"])
    x = x.sort_values("observation_date").reset_index(drop=True)

    feature = _transform(x["value"], spec.transform)
    component = spec.direction * rolling_zscore(feature, spec.z_window, spec.z_min_periods)

    out = pd.DataFrame(
        {
            "observation_date": x["observation_date"],
            "available_date": availability_date(x["observation_date"], spec.availability_lag_days),
            f"{spec.series_id}_component": component,
        }
    ).dropna(subset=[f"{spec.series_id}_component"])

    metadata = {
        "series_id": spec.series_id,
        "label": spec.label,
        "transform": spec.transform,
        "direction": spec.direction,
        "z_window_observations": spec.z_window,
        "z_min_periods": spec.z_min_periods,
        "availability_lag_days": spec.availability_lag_days,
        "first_observation": x["observation_date"].min().date().isoformat() if not x.empty else None,
        "last_observation": x["observation_date"].max().date().isoformat() if not x.empty else None,
        "observation_count": int(len(x)),
    }
    return out, metadata


def build_macro_daily(
    dates: pd.Series,
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> tuple[pd.DataFrame, list[dict]]:
    base = pd.DataFrame({"date": pd.to_datetime(dates).dt.tz_localize(None)}).sort_values("date")
    source_meta: list[dict] = []
    component_cols: list[str] = []

    for spec in MACRO_SPECS:
        comp, meta = fetch_fred_component(spec, start=start, end=end)
        source_meta.append(meta)
        name = f"{spec.series_id}_component"
        component_cols.append(name)
        right = comp[["available_date", name]].sort_values("available_date")
        base = pd.merge_asof(
            base.sort_values("date"),
            right,
            left_on="date",
            right_on="available_date",
            direction="backward",
        )
        base = base.drop(columns=["available_date"])

    base["available_component_count"] = base[component_cols].notna().sum(axis=1)
    base["macro_risk_score"] = base[component_cols].mean(axis=1, skipna=True)
    base.loc[base["available_component_count"] < MIN_COMPONENTS, "macro_risk_score"] = np.nan
    base["macro_risk_change_30d"] = base["macro_risk_score"] - base["macro_risk_score"].shift(30)
    return base, source_meta


def build_crypto_breadth(panel: pd.DataFrame) -> pd.DataFrame:
    x = add_defensive_features(panel)
    sma_cols = [f"{asset}_sma{SMA_DAYS}" for asset in ASSETS]
    ready = x[sma_cols].notna().all(axis=1)
    out = x.loc[ready, ["timestamp", "breadth_sma200"]].copy()
    out["date"] = pd.to_datetime(out["timestamp"], utc=True).dt.tz_localize(None)
    out["breadth"] = out["breadth_sma200"].astype(int)
    return out[["date", "breadth"]].sort_values("date").reset_index(drop=True)


def build_crypto_stress_episodes(daily: pd.DataFrame) -> pd.DataFrame:
    x = daily.sort_values("date").reset_index(drop=True)
    mode = "NORMAL"
    low_streak = 0
    high_streak = 0
    current: dict | None = None
    episodes: list[dict] = []

    for i, row in x.iterrows():
        date = pd.Timestamp(row["date"])
        breadth = int(row["breadth"])
        next_date = pd.Timestamp(x.iloc[i + 1]["date"]) if i + 1 < len(x) else pd.NaT

        if mode == "NORMAL":
            low_streak = low_streak + 1 if breadth <= ENTER_BREADTH_MAX else 0
            high_streak = 0
            if low_streak >= CONFIRM_DAYS and not pd.isna(next_date):
                current = {
                    "entry_signal_date": date,
                    "entry_execution_date": next_date,
                    "entry_breadth": breadth,
                    "exit_signal_date": pd.NaT,
                    "exit_execution_date": pd.NaT,
                    "exit_breadth": np.nan,
                }
                mode = "DEFENSIVE"
                low_streak = 0
                high_streak = 0
        else:
            high_streak = high_streak + 1 if breadth >= EXIT_BREADTH_MIN else 0
            low_streak = 0
            if high_streak >= CONFIRM_DAYS and not pd.isna(next_date):
                assert current is not None
                current["exit_signal_date"] = date
                current["exit_execution_date"] = next_date
                current["exit_breadth"] = breadth
                episodes.append(current)
                current = None
                mode = "NORMAL"
                low_streak = 0
                high_streak = 0

    if current is not None:
        episodes.append(current)

    return pd.DataFrame(episodes)


def build_event_study(
    daily: pd.DataFrame,
    episodes: pd.DataFrame,
    offsets: tuple[int, ...] = (-30, -14, -7, 0, 7, 14, 30),
) -> pd.DataFrame:
    by_date = daily.set_index("date")
    rows: list[dict] = []

    for _, ep in episodes.iterrows():
        for event_type, field in (
            ("ENTRY_SIGNAL", "entry_signal_date"),
            ("EXIT_SIGNAL", "exit_signal_date"),
        ):
            signal = ep.get(field)
            if pd.isna(signal):
                continue
            signal = pd.Timestamp(signal)
            for offset in offsets:
                date = signal + pd.Timedelta(days=offset)
                if date not in by_date.index:
                    continue
                row = by_date.loc[date]
                if isinstance(row, pd.DataFrame):
                    row = row.iloc[-1]
                rows.append(
                    {
                        "event_type": event_type,
                        "signal_date": signal,
                        "offset_days": offset,
                        "date": date,
                        "breadth": int(row["breadth"]),
                        "macro_risk_score": float(row["macro_risk_score"])
                        if pd.notna(row["macro_risk_score"])
                        else np.nan,
                    }
                )

    return pd.DataFrame(rows)


def lead_lag_correlations(
    daily: pd.DataFrame,
    leads: tuple[int, ...] = (0, 7, 14, 21, 30),
) -> pd.DataFrame:
    rows: list[dict] = []
    base = daily[["macro_risk_score", "breadth"]].copy()
    base["crypto_stress_intensity"] = len(ASSETS) - base["breadth"]

    for lead in leads:
        x = base["macro_risk_score"]
        y = base["crypto_stress_intensity"].shift(-lead)
        pair = pd.concat([x, y], axis=1).dropna()
        pearson = pair.iloc[:, 0].corr(pair.iloc[:, 1]) if len(pair) >= 10 else np.nan
        spearman = (
            pair.iloc[:, 0].rank().corr(pair.iloc[:, 1].rank())
            if len(pair) >= 10
            else np.nan
        )
        rows.append(
            {
                "macro_leads_crypto_days": lead,
                "observations": int(len(pair)),
                "pearson_macro_vs_future_stress": float(pearson) if pd.notna(pearson) else np.nan,
                "spearman_macro_vs_future_stress": float(spearman) if pd.notna(spearman) else np.nan,
            }
        )
    return pd.DataFrame(rows)


def summarize_event_study(event_study: pd.DataFrame) -> list[dict]:
    if event_study.empty:
        return []
    grouped = (
        event_study.groupby(["event_type", "offset_days"], as_index=False)["macro_risk_score"]
        .median()
        .sort_values(["event_type", "offset_days"])
    )
    return grouped.to_dict(orient="records")


def known_2026_episode_check(episodes: pd.DataFrame) -> dict:
    expected_entry = pd.Timestamp("2026-04-01")
    expected_exit = pd.Timestamp("2026-08-23")
    if episodes.empty:
        return {"pass": False, "reason": "no episodes"}

    entry = pd.to_datetime(episodes["entry_execution_date"], errors="coerce")
    exit_ = pd.to_datetime(episodes["exit_execution_date"], errors="coerce")
    mask = (entry == expected_entry) & (exit_ == expected_exit)
    return {
        "pass": bool(mask.any()),
        "expected_entry_execution_date": expected_entry.date().isoformat(),
        "expected_exit_execution_date": expected_exit.date().isoformat(),
        "matching_rows": int(mask.sum()),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Research-only macro regime diagnostic against frozen crypto breadth stress."
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=REPO_ROOT / "research_artifacts" / "global_macro_risk_regime_v1",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    panel, crypto_meta = download_panel(CRYPTO_DOWNLOAD_START, ANALYSIS_END)
    crypto = build_crypto_breadth(panel)

    macro, source_meta = build_macro_daily(
        crypto["date"],
        start=MACRO_HISTORY_START,
        end=ANALYSIS_END.tz_localize(None),
    )
    daily = crypto.merge(macro, on="date", how="left")
    daily = daily[daily["macro_risk_score"].notna()].reset_index(drop=True)

    episodes = build_crypto_stress_episodes(daily)
    check = known_2026_episode_check(episodes)
    if not check["pass"]:
        raise RuntimeError(
            "Frozen breadth episode integration check failed: expected "
            "2026-04-01 -> 2026-08-23 execution episode was not reproduced."
        )

    event_study = build_event_study(daily, episodes)
    lead_lag = lead_lag_correlations(daily)

    run_id = f"GLOBAL_MACRO_RISK_V1_{_source_commit()[:12]}"
    run_dir = args.output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    component_cols = [f"{spec.series_id}_component" for spec in MACRO_SPECS]
    daily_export_cols = [
        "date",
        "breadth",
        "available_component_count",
        "macro_risk_score",
        "macro_risk_change_30d",
        *component_cols,
    ]
    daily[daily_export_cols].to_csv(run_dir / "macro_crypto_daily_derived.csv", index=False)
    episodes.to_csv(run_dir / "crypto_stress_episodes.csv", index=False)
    event_study.to_csv(run_dir / "event_study.csv", index=False)
    lead_lag.to_csv(run_dir / "lead_lag_correlations.csv", index=False)
    _write_json(run_dir / "macro_source_metadata.json", source_meta)

    latest = daily.iloc[-1]
    summary = {
        "run_id": run_id,
        "source_commit_sha": _source_commit(),
        "status": "RESEARCH_ONLY_MACRO_DIAGNOSTIC_EXECUTED",
        "analysis_end": ANALYSIS_END.date().isoformat(),
        "crypto_universe": list(ASSETS),
        "frozen_crypto_stress_rule": {
            "sma_days": SMA_DAYS,
            "enter_breadth_max": ENTER_BREADTH_MAX,
            "exit_breadth_min": EXIT_BREADTH_MIN,
            "confirmation_days": CONFIRM_DAYS,
            "changed": False,
        },
        "macro_design": {
            "composite": "equal-weight mean of directional rolling z-scores",
            "minimum_available_components": MIN_COMPONENTS,
            "series": [spec.__dict__ for spec in MACRO_SPECS],
            "optimization_or_threshold_tuning": False,
            "trading_actions": False,
        },
        "crypto_dataset": crypto_meta,
        "macro_sources": source_meta,
        "known_2026_episode_check": check,
        "episode_count": int(len(episodes)),
        "closed_episode_count": int(episodes["exit_execution_date"].notna().sum())
        if not episodes.empty
        else 0,
        "event_study_median_macro_score": summarize_event_study(event_study),
        "lead_lag_correlations": lead_lag.to_dict(orient="records"),
        "latest_fixed_end_state": {
            "date": pd.Timestamp(latest["date"]).date().isoformat(),
            "breadth": int(latest["breadth"]),
            "macro_risk_score": float(latest["macro_risk_score"]),
            "macro_risk_change_30d": float(latest["macro_risk_change_30d"])
            if pd.notna(latest["macro_risk_change_30d"])
            else None,
            "components": {
                spec.series_id: (
                    float(latest[f"{spec.series_id}_component"])
                    if pd.notna(latest[f"{spec.series_id}_component"])
                    else None
                )
                for spec in MACRO_SPECS
            },
        },
        "limitations": [
            "This is descriptive research, not a trading rule.",
            "FRED historical series can be revised; vintage/realtime data are not modeled.",
            "NFCI availability uses a conservative +5 calendar-day approximation.",
            "Daily market series are treated as available after that day's close.",
            "Equal weighting and transforms are preregistered research choices, not optimized parameters.",
        ],
    }
    _write_json(run_dir / "summary.json", summary)

    print(json.dumps(_json_safe(summary), indent=2, sort_keys=True, allow_nan=False))
    print(f"output={run_dir}")
    print("status=RESEARCH_ONLY_MACRO_DIAGNOSTIC_EXECUTED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
