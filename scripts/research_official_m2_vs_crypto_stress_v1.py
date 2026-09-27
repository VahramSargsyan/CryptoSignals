from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd

from scripts.research_global_macro_risk_regime_v1 import (
    ANALYSIS_END,
    CRYPTO_DOWNLOAD_START,
    build_crypto_breadth,
    build_crypto_stress_episodes,
)
from scripts.research_official_liquidity_data_feasibility_v1 import _request_bytes
from scripts.research_official_m2_source_feasibility_v1 import (
    H6_URL,
    inspect_h6_m2,
)
from scripts.research_oss_market_regime_comparator_v1 import add_crypto_mode
from scripts.research_relative_rotation_graph_intelligence import download_panel

REPO_ROOT = Path(__file__).resolve().parents[1]
CHANGE_MONTHS = (3, 6, 12)
EXPECTED_EPISODES = 8

# Actual Federal Reserve H.6 release dates, frozen before execution.
# Each release is mapped to the immediately preceding observation month.
RELEASE_DATES = tuple(pd.Timestamp(x) for x in (
    "2022-01-25","2022-02-22","2022-03-22","2022-04-26","2022-05-24","2022-06-28",
    "2022-07-26","2022-08-23","2022-09-27","2022-10-25","2022-11-22","2022-12-27",
    "2023-01-24","2023-02-28","2023-03-28","2023-04-25","2023-05-23","2023-06-27",
    "2023-07-25","2023-08-22","2023-09-26","2023-10-24","2023-11-28","2023-12-26",
    "2024-01-23","2024-02-27","2024-03-26","2024-04-23","2024-05-28","2024-06-25",
    "2024-07-23","2024-08-27","2024-09-24","2024-10-22","2024-11-26","2024-12-26",
    "2025-01-28","2025-02-25","2025-03-25","2025-04-22","2025-05-27","2025-06-24",
    "2025-07-22","2025-08-26","2025-09-23","2025-10-28","2025-11-25","2025-12-23",
    "2026-01-27","2026-02-24","2026-03-24","2026-04-28","2026-05-26","2026-06-23",
    "2026-07-28","2026-08-25","2026-09-22"
))


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


def release_map() -> pd.DataFrame:
    rows = []
    for release in RELEASE_DATES:
        observation_month_end = (release - pd.offsets.MonthEnd(1)).normalize()
        # H.6 is published around 13:00 ET, after the crypto 00:00 UTC close.
        # Conservatively expose it starting next UTC calendar day.
        available = release.normalize() + pd.Timedelta(days=1)
        rows.append({
            "observation_month_end": observation_month_end,
            "release_date": release.normalize(),
            "available_date": available,
        })
    out = pd.DataFrame(rows).sort_values("observation_month_end").reset_index(drop=True)
    if out["observation_month_end"].duplicated().any():
        raise RuntimeError("Duplicate observation month in H6 release map")
    return out


def build_causal_m2(raw: pd.DataFrame) -> pd.DataFrame:
    x = raw.copy()
    x["observation_month_end"] = pd.to_datetime(x["period_date"]).dt.normalize()
    x = x.sort_values("observation_month_end").reset_index(drop=True)
    for months in CHANGE_MONTHS:
        x[f"m2_change_{months}m"] = x["m2_value"] / x["m2_value"].shift(months) - 1.0

    mapped = x.merge(
        release_map(),
        on="observation_month_end",
        how="left",
        validate="one_to_one",
    )

    needed = mapped[
        (mapped["observation_month_end"] >= pd.Timestamp("2022-12-31"))
        & (mapped["observation_month_end"] <= pd.Timestamp("2026-08-31"))
    ]
    missing = needed[needed["available_date"].isna()]
    if not missing.empty:
        raise RuntimeError(
            "Missing causal H6 release dates for required M2 months: "
            + ",".join(missing["observation_month_end"].dt.date.astype(str))
        )

    return mapped


def align_m2_to_crypto(crypto_daily: pd.DataFrame, m2: pd.DataFrame) -> pd.DataFrame:
    left = crypto_daily.sort_values("date").copy()
    right_cols = [
        "observation_month_end",
        "release_date",
        "available_date",
        "m2_value",
        *[f"m2_change_{m}m" for m in CHANGE_MONTHS],
    ]
    right = m2.dropna(subset=["available_date"]).sort_values("available_date")[right_cols].copy()
    out = pd.merge_asof(
        left,
        right,
        left_on="date",
        right_on="available_date",
        direction="backward",
    )
    return out


def event_snapshots(daily: pd.DataFrame, episodes: pd.DataFrame) -> pd.DataFrame:
    by_date = daily.set_index("date")
    rows = []
    for episode_id, (_, ep) in enumerate(episodes.iterrows(), start=1):
        for event_type, field in (
            ("ENTRY_SIGNAL", "entry_signal_date"),
            ("EXIT_SIGNAL", "exit_signal_date"),
        ):
            value = ep.get(field)
            if pd.isna(value):
                continue
            date = pd.Timestamp(value)
            if date not in by_date.index:
                raise RuntimeError(f"Event date missing from daily alignment: {date}")
            row = by_date.loc[date]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[-1]
            payload = {
                "episode_id": episode_id,
                "event_type": event_type,
                "signal_date": date,
                "breadth": int(row["breadth"]),
                "crypto_mode": row["crypto_mode"],
                "m2_observation_month_end": row.get("observation_month_end"),
                "m2_release_date": row.get("release_date"),
                "m2_available_date": row.get("available_date"),
                "m2_value": row.get("m2_value"),
            }
            for months in CHANGE_MONTHS:
                payload[f"m2_change_{months}m"] = row.get(f"m2_change_{months}m")
            rows.append(payload)
    return pd.DataFrame(rows)


def sign_stats(
    daily: pd.DataFrame,
    events: pd.DataFrame,
    *,
    event_type: str,
    baseline_mode: str,
    positive: bool,
) -> dict:
    result = {}
    event_rows = events[events["event_type"] == event_type]
    baseline = daily[daily["crypto_mode"] == baseline_mode]

    for months in CHANGE_MONTHS:
        col = f"m2_change_{months}m"
        ev = pd.to_numeric(event_rows[col], errors="coerce").dropna()
        base = pd.to_numeric(baseline[col], errors="coerce").dropna()

        if positive:
            event_hits = int((ev > 0).sum())
            baseline_hits = int((base > 0).sum())
        else:
            event_hits = int((ev < 0).sum())
            baseline_hits = int((base < 0).sum())

        event_frac = event_hits / len(ev) if len(ev) else np.nan
        baseline_frac = baseline_hits / len(base) if len(base) else np.nan
        lift = (
            event_frac / baseline_frac
            if pd.notna(event_frac) and pd.notna(baseline_frac) and baseline_frac > 0
            else np.nan
        )

        result[f"{months}m"] = {
            "event_known": int(len(ev)),
            "event_hits": event_hits,
            "event_fraction": float(event_frac) if pd.notna(event_frac) else np.nan,
            "baseline_known_days": int(len(base)),
            "baseline_hits": baseline_hits,
            "baseline_fraction": float(baseline_frac) if pd.notna(baseline_frac) else np.nan,
            "descriptive_lift": float(lift) if pd.notna(lift) else np.nan,
            "event_median_change": float(ev.median()) if len(ev) else np.nan,
            "baseline_median_change": float(base.median()) if len(base) else np.nan,
        }
    return result


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Official M2 money-supply context vs frozen crypto stress.")
    p.add_argument(
        "--output-root",
        type=Path,
        default=REPO_ROOT / "research_artifacts" / "official_m2_vs_crypto_stress_v1",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    payload = _request_bytes(H6_URL, timeout=180)
    raw_m2, source_meta = inspect_h6_m2(payload)
    m2 = build_causal_m2(raw_m2)

    panel, crypto_meta = download_panel(CRYPTO_DOWNLOAD_START, ANALYSIS_END)
    crypto = build_crypto_breadth(panel)
    episodes = build_crypto_stress_episodes(crypto)
    closed = episodes.dropna(subset=["exit_signal_date"]).copy()
    if len(closed) != EXPECTED_EPISODES:
        raise RuntimeError(
            f"Expected {EXPECTED_EPISODES} closed crypto stress episodes, found {len(closed)}"
        )

    crypto = add_crypto_mode(crypto, episodes)
    daily = align_m2_to_crypto(crypto, m2)
    events = event_snapshots(daily, closed)

    entry = sign_stats(
        daily, events,
        event_type="ENTRY_SIGNAL",
        baseline_mode="NORMAL",
        positive=False,
    )
    exit_stats = sign_stats(
        daily, events,
        event_type="EXIT_SIGNAL",
        baseline_mode="DEFENSIVE",
        positive=True,
    )

    latest = daily.dropna(subset=["m2_value"]).iloc[-1]

    report = {
        "run_id": f"OFFICIAL_M2_VS_CRYPTO_STRESS_V1_{_source_commit()[:12]}",
        "source_commit_sha": _source_commit(),
        "status": "OFFICIAL_M2_VS_CRYPTO_STRESS_V1_EXECUTED",
        "source": {
            "name": "Federal Reserve Board H.6 Money Stock Measures",
            "series": "M2.M",
            "adjustment": source_meta["series_attributes"].get("ADJUSTED"),
            "unit_mult": source_meta["series_attributes"].get("UNIT_MULT"),
            "rows": source_meta["rows"],
            "first_period": source_meta["first_period"],
            "last_period": source_meta["last_period"],
        },
        "causal_contract": {
            "release_dates": "Frozen actual Federal Reserve H.6 schedule 2022-2026",
            "release_time": "generally 13:00 ET",
            "crypto_availability": "next UTC calendar day after official H6 release date",
            "silent_release_date_approximation": False,
        },
        "transformations": [f"{m}m_pct_change" for m in CHANGE_MONTHS],
        "closed_crypto_episodes": int(len(closed)),
        "entry_negative_growth_vs_normal": entry,
        "exit_positive_growth_vs_defensive": exit_stats,
        "latest_fixed_end": {
            "crypto_date": latest["date"],
            "m2_observation_month_end": latest["observation_month_end"],
            "m2_release_date": latest["release_date"],
            "m2_available_date": latest["available_date"],
            "m2_value_usd_bn": latest["m2_value"],
            **{f"m2_change_{m}m": latest[f"m2_change_{m}m"] for m in CHANGE_MONTHS},
        },
        "revision_limitation": (
            "Current official seasonally adjusted H6 history is used; historical vintages/revisions are not reconstructed."
        ),
        "crypto_dataset": crypto_meta,
        "interpretation_boundary": [
            "Descriptive event study only.",
            "No M2 horizon is selected post hoc.",
            "No BTC or net-liquidity signal is combined in this V1.",
            "No trading rule or production change is authorized.",
        ],
    }

    run_dir = args.output_root / report["run_id"]
    run_dir.mkdir(parents=True, exist_ok=True)

    raw_m2.to_csv(run_dir / "official_h6_m2_raw.csv", index=False)
    m2.to_csv(run_dir / "official_h6_m2_causal.csv", index=False)
    daily.to_csv(run_dir / "crypto_m2_daily_alignment.csv", index=False)
    closed.to_csv(run_dir / "crypto_stress_episodes.csv", index=False)
    events.to_csv(run_dir / "crypto_stress_m2_event_snapshots.csv", index=False)
    _write_json(run_dir / "summary.json", report)

    print(json.dumps(_json_safe(report), indent=2, sort_keys=True))
    print(f"output={run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
