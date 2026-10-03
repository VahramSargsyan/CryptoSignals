from __future__ import annotations

import json
import os
import subprocess
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

import scripts.research_rr_lookback_baseline_family_v1 as prev
from strategies.crypto.relative_rotation.paper_live import TARGET_ASSETS

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "rr_median_cascade_25_50_90_180_v1"

AS_OF = prev.AS_OF
RECENT_START = pd.Timestamp("2026-09-03T00:00:00Z")
TRACE_START = pd.Timestamp("2026-08-01T00:00:00Z")
AUDIT_DATES = tuple(pd.Timestamp(x) for x in (
    "2026-09-28T00:00:00Z",
    "2026-09-29T00:00:00Z",
    "2026-09-30T00:00:00Z",
    "2026-10-01T00:00:00Z",
))
HORIZONS = (3, 7, 14, 30)
PERIODS = (25, 50, 90, 180)
STAGES = ("S1_FAST_CROSS", "S2_MID_STACK", "S3_FULL_STACK")


def source_sha() -> str:
    return os.environ.get("SOURCE_COMMIT_SHA") or subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def ordering_state(m25: float, m50: float, m90: float, m180: float) -> str:
    vals = (m25, m50, m90, m180)
    if any(pd.isna(x) for x in vals):
        return "NONE"
    if m25 > m50 > m90 > m180:
        return "S3_FULL_STACK"
    if m25 > m50 > m90:
        return "S2_STACK"
    if m25 > m50:
        return "S1_ONLY"
    return "NONE"


def down_ordering_state(m25: float, m50: float, m90: float, m180: float) -> str:
    vals = (m25, m50, m90, m180)
    if any(pd.isna(x) for x in vals):
        return "NONE"
    if m25 < m50 < m90 < m180:
        return "D3_FULL_STACK"
    if m25 < m50 < m90:
        return "D2_STACK"
    if m25 < m50:
        return "D1_ONLY"
    return "NONE"


def forward_relative_excess(
    panel: pd.DataFrame,
    candidate: str,
    baseline: str,
    event_i: int,
    horizon: int,
) -> float:
    start_i = event_i + 1
    end_i = start_i + horizon
    if start_i >= len(panel) or end_i >= len(panel):
        return np.nan
    c_ret = (
        float(panel.iloc[end_i][f"{candidate}_open"])
        / float(panel.iloc[start_i][f"{candidate}_open"])
        - 1.0
    )
    b_ret = (
        float(panel.iloc[end_i][f"{baseline}_open"])
        / float(panel.iloc[start_i][f"{baseline}_open"])
        - 1.0
    )
    return (1.0 + c_ret) / (1.0 + b_ret) - 1.0


def build_pair_cascade(
    panel: pd.DataFrame,
    candidate: str,
    baseline: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ratio = panel[f"{candidate}_close"] / panel[f"{baseline}_close"]
    medians = {
        p: ratio.rolling(p, min_periods=p).median()
        for p in PERIODS
    }
    mom10 = ratio / ratio.shift(10) - 1.0

    daily = pd.DataFrame(
        {
            "date": pd.to_datetime(panel["timestamp"], utc=True),
            "candidate": candidate,
            "baseline": baseline,
            "ratio": ratio.astype(float),
            "m25": medians[25].astype(float),
            "m50": medians[50].astype(float),
            "m90": medians[90].astype(float),
            "m180": medians[180].astype(float),
            "mom10": mom10.astype(float),
        }
    )
    daily["up_state"] = [
        ordering_state(a, b, c, d)
        for a, b, c, d in zip(
            daily["m25"], daily["m50"], daily["m90"], daily["m180"]
        )
    ]
    daily["down_state"] = [
        down_ordering_state(a, b, c, d)
        for a, b, c, d in zip(
            daily["m25"], daily["m50"], daily["m90"], daily["m180"]
        )
    ]

    up_events = []
    down_events = []

    up_seq = 0
    up_seq_id = 0
    up_s1_i = None
    up_s2_i = None

    down_seq = 0
    down_seq_id = 0
    down_d1_i = None
    down_d2_i = None

    for i in range(1, len(daily)):
        cur = daily.iloc[i]
        prv = daily.iloc[i - 1]

        values = [cur["m25"], cur["m50"], cur["m90"], cur["m180"]]
        pvalues = [prv["m25"], prv["m50"], prv["m90"], prv["m180"]]
        if any(pd.isna(x) for x in values + pvalues):
            continue

        up_cross = cur["m25"] > cur["m50"] and prv["m25"] <= prv["m50"]
        down_cross = cur["m25"] < cur["m50"] and prv["m25"] >= prv["m50"]

        if down_cross:
            up_seq = 0
            up_s1_i = None
            up_s2_i = None

        if up_cross:
            up_seq_id += 1
            up_seq = 1
            up_s1_i = i
            up_s2_i = None
            up_events.append(
                {
                    "candidate": candidate,
                    "baseline": baseline,
                    "direction": "UP",
                    "sequence_id": up_seq_id,
                    "stage": "S1_FAST_CROSS",
                    "event_i": i,
                    "date": cur["date"],
                    "ratio": cur["ratio"],
                    "m25": cur["m25"],
                    "m50": cur["m50"],
                    "m90": cur["m90"],
                    "m180": cur["m180"],
                    "mom10": cur["mom10"],
                }
            )
        elif up_seq == 1 and up_s1_i is not None and i > up_s1_i:
            if cur["m25"] > cur["m50"] > cur["m90"]:
                up_seq = 2
                up_s2_i = i
                up_events.append(
                    {
                        "candidate": candidate,
                        "baseline": baseline,
                        "direction": "UP",
                        "sequence_id": up_seq_id,
                        "stage": "S2_MID_STACK",
                        "event_i": i,
                        "date": cur["date"],
                        "ratio": cur["ratio"],
                        "m25": cur["m25"],
                        "m50": cur["m50"],
                        "m90": cur["m90"],
                        "m180": cur["m180"],
                        "mom10": cur["mom10"],
                    }
                )
        elif up_seq == 2 and up_s2_i is not None and i > up_s2_i:
            if cur["m25"] > cur["m50"] > cur["m90"] > cur["m180"]:
                up_seq = 3
                up_events.append(
                    {
                        "candidate": candidate,
                        "baseline": baseline,
                        "direction": "UP",
                        "sequence_id": up_seq_id,
                        "stage": "S3_FULL_STACK",
                        "event_i": i,
                        "date": cur["date"],
                        "ratio": cur["ratio"],
                        "m25": cur["m25"],
                        "m50": cur["m50"],
                        "m90": cur["m90"],
                        "m180": cur["m180"],
                        "mom10": cur["mom10"],
                    }
                )

        if up_cross:
            down_seq = 0
            down_d1_i = None
            down_d2_i = None

        if down_cross:
            down_seq_id += 1
            down_seq = 1
            down_d1_i = i
            down_d2_i = None
            down_events.append(
                {
                    "candidate": candidate,
                    "baseline": baseline,
                    "direction": "DOWN",
                    "sequence_id": down_seq_id,
                    "stage": "D1_FAST_CROSS",
                    "event_i": i,
                    "date": cur["date"],
                    "ratio": cur["ratio"],
                    "m25": cur["m25"],
                    "m50": cur["m50"],
                    "m90": cur["m90"],
                    "m180": cur["m180"],
                    "mom10": cur["mom10"],
                }
            )
        elif down_seq == 1 and down_d1_i is not None and i > down_d1_i:
            if cur["m25"] < cur["m50"] < cur["m90"]:
                down_seq = 2
                down_d2_i = i
                down_events.append(
                    {
                        "candidate": candidate,
                        "baseline": baseline,
                        "direction": "DOWN",
                        "sequence_id": down_seq_id,
                        "stage": "D2_MID_STACK",
                        "event_i": i,
                        "date": cur["date"],
                        "ratio": cur["ratio"],
                        "m25": cur["m25"],
                        "m50": cur["m50"],
                        "m90": cur["m90"],
                        "m180": cur["m180"],
                        "mom10": cur["mom10"],
                    }
                )
        elif down_seq == 2 and down_d2_i is not None and i > down_d2_i:
            if cur["m25"] < cur["m50"] < cur["m90"] < cur["m180"]:
                down_seq = 3
                down_events.append(
                    {
                        "candidate": candidate,
                        "baseline": baseline,
                        "direction": "DOWN",
                        "sequence_id": down_seq_id,
                        "stage": "D3_FULL_STACK",
                        "event_i": i,
                        "date": cur["date"],
                        "ratio": cur["ratio"],
                        "m25": cur["m25"],
                        "m50": cur["m50"],
                        "m90": cur["m90"],
                        "m180": cur["m180"],
                        "mom10": cur["mom10"],
                    }
                )

    up_df = pd.DataFrame(up_events)
    down_df = pd.DataFrame(down_events)

    if not up_df.empty:
        for h in HORIZONS:
            up_df[f"relative_excess_{h}d"] = [
                forward_relative_excess(
                    panel, candidate, baseline, int(i), h
                )
                for i in up_df["event_i"]
            ]

    if not down_df.empty:
        for h in HORIZONS:
            down_df[f"relative_excess_{h}d"] = [
                forward_relative_excess(
                    panel, candidate, baseline, int(i), h
                )
                for i in down_df["event_i"]
            ]

    return daily, up_df, down_df


def build_all_cascades(
    panel: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    daily_parts = []
    up_parts = []
    down_parts = []

    for baseline in TARGET_ASSETS:
        for candidate in TARGET_ASSETS:
            if candidate == baseline:
                continue
            daily, up, down = build_pair_cascade(panel, candidate, baseline)
            daily_parts.append(daily)
            if not up.empty:
                up_parts.append(up)
            if not down.empty:
                down_parts.append(down)

    daily = pd.concat(daily_parts, ignore_index=True)
    up = pd.concat(up_parts, ignore_index=True) if up_parts else pd.DataFrame()
    down = pd.concat(down_parts, ignore_index=True) if down_parts else pd.DataFrame()
    return daily, up, down


def summarize_stage_events(events: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for stage in STAGES:
        sub = events[events["stage"] == stage].copy()
        for h in HORIZONS:
            col = f"relative_excess_{h}d"
            vals = sub[col].dropna().astype(float)
            rows.append(
                {
                    "stage": stage,
                    "horizon_days": h,
                    "event_count": int(len(sub)),
                    "forward_n": int(len(vals)),
                    "candidate_beats_baseline_rate": (
                        float((vals > 0).mean()) if len(vals) else np.nan
                    ),
                    "median_relative_excess": (
                        float(vals.median()) if len(vals) else np.nan
                    ),
                    "mean_relative_excess": (
                        float(vals.mean()) if len(vals) else np.nan
                    ),
                    "p25_relative_excess": (
                        float(vals.quantile(0.25)) if len(vals) else np.nan
                    ),
                    "p75_relative_excess": (
                        float(vals.quantile(0.75)) if len(vals) else np.nan
                    ),
                    "ge10_rate": (
                        float((vals >= 0.10).mean()) if len(vals) else np.nan
                    ),
                    "ge20_rate": (
                        float((vals >= 0.20).mean()) if len(vals) else np.nan
                    ),
                    "nonoutperformance_rate": (
                        float((vals <= 0).mean()) if len(vals) else np.nan
                    ),
                }
            )
    return pd.DataFrame(rows)


def build_timing(events: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    if events.empty:
        return pd.DataFrame(), {}

    pivot = (
        events.pivot_table(
            index=["candidate", "baseline", "sequence_id"],
            columns="stage",
            values="date",
            aggfunc="first",
        )
        .reset_index()
    )
    for col in STAGES:
        if col not in pivot.columns:
            pivot[col] = pd.NaT

    pivot["s1_to_s2_days"] = (
        pd.to_datetime(pivot["S2_MID_STACK"], utc=True)
        - pd.to_datetime(pivot["S1_FAST_CROSS"], utc=True)
    ).dt.days
    pivot["s2_to_s3_days"] = (
        pd.to_datetime(pivot["S3_FULL_STACK"], utc=True)
        - pd.to_datetime(pivot["S2_MID_STACK"], utc=True)
    ).dt.days
    pivot["s1_to_s3_days"] = (
        pd.to_datetime(pivot["S3_FULL_STACK"], utc=True)
        - pd.to_datetime(pivot["S1_FAST_CROSS"], utc=True)
    ).dt.days

    def stats(series: pd.Series) -> dict:
        vals = series.dropna().astype(float)
        if not len(vals):
            return {"n": 0}
        return {
            "n": int(len(vals)),
            "median": float(vals.median()),
            "mean": float(vals.mean()),
            "p25": float(vals.quantile(0.25)),
            "p75": float(vals.quantile(0.75)),
        }

    s1_count = int(pivot["S1_FAST_CROSS"].notna().sum())
    summary = {
        "s1_to_s2_days": stats(pivot["s1_to_s2_days"]),
        "s2_to_s3_days": stats(pivot["s2_to_s3_days"]),
        "s1_to_s3_days": stats(pivot["s1_to_s3_days"]),
        "s1_reaches_s2_within_30d_rate": (
            float(
                (
                    pivot["s1_to_s2_days"].notna()
                    & (pivot["s1_to_s2_days"] <= 30)
                ).sum()
                / s1_count
            )
            if s1_count else np.nan
        ),
        "s1_reaches_s3_within_60d_rate": (
            float(
                (
                    pivot["s1_to_s3_days"].notna()
                    & (pivot["s1_to_s3_days"] <= 60)
                ).sum()
                / s1_count
            )
            if s1_count else np.nan
        ),
    }
    return pivot, summary


def build_year_summary(events: pd.DataFrame) -> pd.DataFrame:
    s2 = events[events["stage"] == "S2_MID_STACK"].copy()
    if s2.empty:
        return pd.DataFrame()
    s2["year"] = pd.to_datetime(s2["date"], utc=True).dt.year
    rows = []
    for year, sub in s2.groupby("year"):
        vals = sub["relative_excess_14d"].dropna().astype(float)
        rows.append(
            {
                "year": int(year),
                "event_count": int(len(sub)),
                "forward_n_14d": int(len(vals)),
                "beats_rate_14d": (
                    float((vals > 0).mean()) if len(vals) else np.nan
                ),
                "median_excess_14d": (
                    float(vals.median()) if len(vals) else np.nan
                ),
                "mean_excess_14d": (
                    float(vals.mean()) if len(vals) else np.nan
                ),
            }
        )
    return pd.DataFrame(rows)


def index_daily(
    cascade_daily: pd.DataFrame,
) -> dict[tuple[pd.Timestamp, str, str], dict]:
    return {
        (
            pd.Timestamp(row["date"]),
            str(row["candidate"]),
            str(row["baseline"]),
        ): row.to_dict()
        for _, row in cascade_daily.iterrows()
    }


def index_events(
    cascade_events: pd.DataFrame,
) -> dict[tuple[pd.Timestamp, str, str], list[dict]]:
    out: dict[tuple[pd.Timestamp, str, str], list[dict]] = defaultdict(list)
    for _, row in cascade_events.iterrows():
        out[
            (
                pd.Timestamp(row["date"]),
                str(row["baseline"]),
                str(row["stage"]),
            )
        ].append(row.to_dict())
    return out


def build_pre_entry_audit(
    routes: pd.DataFrame,
    daily_idx: dict,
) -> pd.DataFrame:
    rank = {
        "NONE": 0,
        "S1_ONLY": 1,
        "S2_STACK": 2,
        "S3_FULL_STACK": 3,
    }
    rows = []
    for route_id, route in routes.iterrows():
        signal_date = pd.Timestamp(route["signal_date"])
        baseline = str(route["effective_to"])
        candidates = []
        for candidate in TARGET_ASSETS:
            if candidate == baseline:
                continue
            row = daily_idx.get((signal_date, candidate, baseline))
            if row is None:
                continue
            state = str(row["up_state"])
            candidates.append(
                {
                    "candidate": candidate,
                    "state": state,
                    "stage_rank": rank.get(state, 0),
                    "mom10": row.get("mom10", np.nan),
                    "ratio": row.get("ratio", np.nan),
                }
            )
        candidates.sort(
            key=lambda r: (
                -r["stage_rank"],
                -(r["mom10"] if pd.notna(r["mom10"]) else -999.0),
                r["candidate"],
            )
        )
        best = candidates[0] if candidates else {
            "candidate": "",
            "state": "NONE",
            "stage_rank": 0,
            "mom10": np.nan,
            "ratio": np.nan,
        }
        rows.append(
            {
                "route_id": int(route_id),
                "signal_date": signal_date,
                "source": str(route["source"]),
                "effective_to": baseline,
                "best_candidate": best["candidate"],
                "best_state": best["state"],
                "best_stage_rank": best["stage_rank"],
                "best_mom10": best["mom10"],
                "best_ratio": best["ratio"],
            }
        )
    return pd.DataFrame(rows)


def build_overlay_diagnostic(
    panel: pd.DataFrame,
    routes: pd.DataFrame,
    events_idx: dict,
) -> pd.DataFrame:
    date_to_i = {
        pd.Timestamp(ts): i
        for i, ts in enumerate(pd.to_datetime(panel["timestamp"], utc=True))
    }
    rows = []

    for route_id, route in routes.iterrows():
        signal_date = pd.Timestamp(route["signal_date"])
        signal_i = date_to_i[signal_date]
        entry_i = signal_i + 1
        if entry_i >= len(panel):
            continue
        baseline = str(route["effective_to"])

        for stage in STAGES:
            chosen = None
            chosen_event_i = None

            for obs_i in range(entry_i, min(entry_i + 30, len(panel))):
                dt = pd.Timestamp(panel.iloc[obs_i]["timestamp"])
                candidates = [
                    x
                    for x in events_idx.get((dt, baseline, stage), [])
                    if str(x["candidate"]) != baseline
                ]
                if not candidates:
                    continue
                candidates.sort(
                    key=lambda x: (
                        -(float(x.get("mom10")) if pd.notna(x.get("mom10")) else -999.0),
                        str(x["candidate"]),
                    )
                )
                chosen = candidates[0]
                chosen_event_i = obs_i
                break

            row = {
                "route_id": int(route_id),
                "signal_date": signal_date,
                "source": str(route["source"]),
                "baseline": baseline,
                "stage": stage,
                "found_within_30d": chosen is not None,
            }

            if chosen is not None and chosen_event_i is not None:
                candidate = str(chosen["candidate"])
                row.update(
                    {
                        "candidate": candidate,
                        "event_date": pd.Timestamp(
                            panel.iloc[chosen_event_i]["timestamp"]
                        ),
                        "bars_after_entry": int(chosen_event_i - entry_i + 1),
                        "mom10": chosen.get("mom10", np.nan),
                    }
                )
                for h in HORIZONS:
                    row[f"relative_excess_{h}d"] = forward_relative_excess(
                        panel, candidate, baseline, chosen_event_i, h
                    )
            rows.append(row)

    return pd.DataFrame(rows)


def summarize_overlay(overlay: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for stage in STAGES:
        sub = overlay[
            (overlay["stage"] == stage)
            & overlay["found_within_30d"]
        ].copy()
        total = int((overlay["stage"] == stage).sum())
        for h in HORIZONS:
            col = f"relative_excess_{h}d"
            vals = (
                sub[col].dropna().astype(float)
                if col in sub.columns
                else pd.Series(dtype=float)
            )
            rows.append(
                {
                    "stage": stage,
                    "horizon_days": h,
                    "route_count": total,
                    "found_count": int(len(sub)),
                    "found_rate": (
                        float(len(sub) / total) if total else np.nan
                    ),
                    "forward_n": int(len(vals)),
                    "candidate_beats_baseline_rate": (
                        float((vals > 0).mean()) if len(vals) else np.nan
                    ),
                    "median_relative_excess": (
                        float(vals.median()) if len(vals) else np.nan
                    ),
                    "mean_relative_excess": (
                        float(vals.mean()) if len(vals) else np.nan
                    ),
                    "ge20_rate": (
                        float((vals >= 0.20).mean()) if len(vals) else np.nan
                    ),
                }
            )
    return pd.DataFrame(rows)


def current_pair_trace(
    cascade_daily: pd.DataFrame,
    events: pd.DataFrame,
) -> tuple[pd.DataFrame, dict]:
    trace = cascade_daily[
        (cascade_daily["candidate"] == "AAVE")
        & (cascade_daily["baseline"] == "TRX")
        & (cascade_daily["date"] >= TRACE_START)
    ].copy()

    pair_events = events[
        (events["candidate"] == "AAVE")
        & (events["baseline"] == "TRX")
    ].copy().sort_values("date")

    latest_date = pd.Timestamp(trace["date"].max())
    latest_events = pair_events[pair_events["date"] <= latest_date]

    last_s1 = latest_events[
        latest_events["stage"] == "S1_FAST_CROSS"
    ].tail(1)
    last_s1_date = None if last_s1.empty else pd.Timestamp(last_s1.iloc[0]["date"])

    subsequent = (
        latest_events[latest_events["date"] >= last_s1_date]
        if last_s1_date is not None
        else latest_events.iloc[0:0]
    )
    s2 = subsequent[subsequent["stage"] == "S2_MID_STACK"].head(1)
    s3 = subsequent[subsequent["stage"] == "S3_FULL_STACK"].head(1)

    latest = trace.sort_values("date").iloc[-1]
    info = {
        "latest_date": latest_date.isoformat(),
        "latest_ratio": float(latest["ratio"]),
        "latest_m25": float(latest["m25"]),
        "latest_m50": float(latest["m50"]),
        "latest_m90": float(latest["m90"]),
        "latest_m180": float(latest["m180"]),
        "latest_state": str(latest["up_state"]),
        "latest_s1_date": (
            None if last_s1_date is None else last_s1_date.isoformat()
        ),
        "subsequent_s2_date": (
            None if s2.empty else pd.Timestamp(s2.iloc[0]["date"]).isoformat()
        ),
        "subsequent_s3_date": (
            None if s3.empty else pd.Timestamp(s3.iloc[0]["date"]).isoformat()
        ),
    }
    return trace, info


def build_audit_states(cascade_daily: pd.DataFrame) -> pd.DataFrame:
    dates = list(AUDIT_DATES)
    latest = pd.Timestamp(cascade_daily["date"].max())
    if latest not in dates:
        dates.append(latest)
    return cascade_daily[
        cascade_daily["date"].isin(dates)
    ].copy()


def classify(
    stage_summary: pd.DataFrame,
    year_summary: pd.DataFrame,
) -> tuple[str, dict]:
    s1 = stage_summary[
        (stage_summary["stage"] == "S1_FAST_CROSS")
        & (stage_summary["horizon_days"] == 14)
    ].iloc[0]
    s2 = stage_summary[
        (stage_summary["stage"] == "S2_MID_STACK")
        & (stage_summary["horizon_days"] == 14)
    ].iloc[0]

    promising = (
        s2["candidate_beats_baseline_rate"] > 0.50
        and s2["median_relative_excess"] > 0
        and s2["forward_n"] >= 100
    )
    not_supported = (
        s2["candidate_beats_baseline_rate"] <= 0.50
        and s2["median_relative_excess"] <= 0
    )

    qualifying_years = year_summary[
        (year_summary["forward_n_14d"] >= 20)
        & (year_summary["beats_rate_14d"] > 0.50)
        & (year_summary["median_excess_14d"] > 0)
    ]

    strong = (
        promising
        and s2["ge20_rate"] > s1["ge20_rate"]
        and len(qualifying_years) >= 2
    )

    if strong:
        label = "CASCADE_STRONG_RESEARCH_SIGNAL"
    elif promising:
        label = "CASCADE_PROMISING_DIAGNOSTIC"
    elif not_supported:
        label = "CASCADE_NOT_SUPPORTED"
    else:
        label = "CASCADE_MIXED"

    gates = {
        "s2_14d_forward_n": int(s2["forward_n"]),
        "s2_14d_win_rate": float(s2["candidate_beats_baseline_rate"]),
        "s2_14d_median_excess": float(s2["median_relative_excess"]),
        "s1_14d_ge20_rate": float(s1["ge20_rate"]),
        "s2_14d_ge20_rate": float(s2["ge20_rate"]),
        "qualifying_calendar_years": int(len(qualifying_years)),
    }
    return label, gates


def fmt_pct(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{100*float(v):+.2f}%"


def fmt_rate(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{100*float(v):.1f}%"


def main() -> None:
    panel, data_meta = prev.download_panel()

    canonical_events, canonical_states = prev.build_monitor_history(
        panel, "MEDIAN", 180
    )
    routes = prev.build_routes(
        panel, canonical_events, canonical_states
    )

    cascade_daily, up_events, down_events = build_all_cascades(panel)
    stage_summary = summarize_stage_events(up_events)
    timing_detail, timing_summary = build_timing(up_events)
    year_summary = build_year_summary(up_events)

    daily_idx = index_daily(cascade_daily)
    events_idx = index_events(up_events)
    pre_entry = build_pre_entry_audit(routes, daily_idx)
    overlay = build_overlay_diagnostic(panel, routes, events_idx)
    overlay_summary = summarize_overlay(overlay)

    aave_trace, aave_info = current_pair_trace(cascade_daily, up_events)
    audit_states = build_audit_states(cascade_daily)
    recent_events = up_events[up_events["date"] >= RECENT_START].copy()

    classification, gates = classify(stage_summary, year_summary)

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    cascade_daily.to_csv(run_dir / "all_pair_daily_cascade_states.csv", index=False)
    up_events.to_csv(run_dir / "upward_cascade_events.csv", index=False)
    down_events.to_csv(run_dir / "downward_cascade_events.csv", index=False)
    stage_summary.to_csv(run_dir / "stage_forward_summary.csv", index=False)
    timing_detail.to_csv(run_dir / "cascade_timing_detail.csv", index=False)
    year_summary.to_csv(run_dir / "s2_calendar_year_summary.csv", index=False)
    pre_entry.to_csv(run_dir / "rr_pre_entry_cascade_audit.csv", index=False)
    overlay.to_csv(run_dir / "rr_overlay_cascade_events.csv", index=False)
    overlay_summary.to_csv(run_dir / "rr_overlay_summary.csv", index=False)
    aave_trace.to_csv(run_dir / "aave_trx_daily_trace.csv", index=False)
    audit_states.to_csv(run_dir / "audit_dates_all_pair_states.csv", index=False)
    recent_events.to_csv(run_dir / "recent_month_upward_events.csv", index=False)
    routes.to_csv(run_dir / "canonical_rr_ddg_routes.csv", index=False)

    summary = {
        "experiment": "RR_MEDIAN_CASCADE_25_50_90_180_V1",
        "workflow_mode": "STRESS_TEST_ONLY",
        "source_commit_sha": source_sha(),
        "as_of": AS_OF.isoformat(),
        "latest_closed_candle": pd.Timestamp(panel.iloc[-1]["timestamp"]).isoformat(),
        "classification": classification,
        "classification_gates": gates,
        "timing_summary": timing_summary,
        "aave_trx": aave_info,
        "upward_event_count": int(len(up_events)),
        "downward_event_count": int(len(down_events)),
        "recent_upward_event_count": int(len(recent_events)),
        "production_changes": "NONE",
        "test_level": "GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_STRESS_TEST",
        "data_metadata": data_meta,
    }
    (run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )

    lines = [
        "# RR MEDIAN CASCADE 25/50/90/180 V1 — RESULTS",
        "",
        "Mode: STRESS_TEST_ONLY",
        "",
        f"Classification: {classification}",
        f"As-of: {AS_OF.isoformat()}",
        f"Latest closed D1: {pd.Timestamp(panel.iloc[-1]['timestamp']).isoformat()}",
        f"Upward cascade events: {len(up_events)}",
        "",
        "## Pair-event forward results",
        "",
        "|Stage|Horizon|N|Candidate beats baseline|Median excess|Mean excess|>=10%|>=20%|",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, r in stage_summary.iterrows():
        lines.append(
            f"|{r['stage']}|{int(r['horizon_days'])}d|{int(r['forward_n'])}|"
            f"{fmt_rate(r['candidate_beats_baseline_rate'])}|"
            f"{fmt_pct(r['median_relative_excess'])}|"
            f"{fmt_pct(r['mean_relative_excess'])}|"
            f"{fmt_rate(r['ge10_rate'])}|{fmt_rate(r['ge20_rate'])}|"
        )

    lines += [
        "",
        "## Cascade timing",
        "",
        f"- S1 -> S2 median days: {timing_summary.get('s1_to_s2_days', {}).get('median', '—')}",
        f"- S2 -> S3 median days: {timing_summary.get('s2_to_s3_days', {}).get('median', '—')}",
        f"- S1 -> S3 median days: {timing_summary.get('s1_to_s3_days', {}).get('median', '—')}",
        f"- S1 reaching S2 within 30d: {fmt_rate(timing_summary.get('s1_reaches_s2_within_30d_rate'))}",
        f"- S1 reaching S3 within 60d: {fmt_rate(timing_summary.get('s1_reaches_s3_within_60d_rate'))}",
        "",
        "## RR overlay diagnostic",
        "",
        "|Stage|Horizon|Found N|Found rate|Candidate beats RR destination|Median excess|Mean excess|>=20%|",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, r in overlay_summary.iterrows():
        lines.append(
            f"|{r['stage']}|{int(r['horizon_days'])}d|{int(r['forward_n'])}|"
            f"{fmt_rate(r['found_rate'])}|"
            f"{fmt_rate(r['candidate_beats_baseline_rate'])}|"
            f"{fmt_pct(r['median_relative_excess'])}|"
            f"{fmt_pct(r['mean_relative_excess'])}|"
            f"{fmt_rate(r['ge20_rate'])}|"
        )

    lines += [
        "",
        "## AAVE / TRX current episode",
        "",
        f"- Latest state: {aave_info['latest_state']}",
        f"- Latest S1 date: {aave_info['latest_s1_date']}",
        f"- Subsequent S2 date: {aave_info['subsequent_s2_date']}",
        f"- Subsequent S3 date: {aave_info['subsequent_s3_date']}",
        f"- Latest M25: {aave_info['latest_m25']:.8f}",
        f"- Latest M50: {aave_info['latest_m50']:.8f}",
        f"- Latest M90: {aave_info['latest_m90']:.8f}",
        f"- Latest M180: {aave_info['latest_m180']:.8f}",
        "",
        "## Calendar-year S2 14d check",
        "",
        "|Year|N|Win rate|Median excess|Mean excess|",
        "|---:|---:|---:|---:|---:|",
    ]
    for _, r in year_summary.iterrows():
        lines.append(
            f"|{int(r['year'])}|{int(r['forward_n_14d'])}|"
            f"{fmt_rate(r['beats_rate_14d'])}|"
            f"{fmt_pct(r['median_excess_14d'])}|"
            f"{fmt_pct(r['mean_excess_14d'])}|"
        )

    lines += [
        "",
        "## Classification gates",
        "",
        f"- S2 14d N: {gates['s2_14d_forward_n']}",
        f"- S2 14d win rate: {fmt_rate(gates['s2_14d_win_rate'])}",
        f"- S2 14d median excess: {fmt_pct(gates['s2_14d_median_excess'])}",
        f"- S1 >=20% continuation: {fmt_rate(gates['s1_14d_ge20_rate'])}",
        f"- S2 >=20% continuation: {fmt_rate(gates['s2_14d_ge20_rate'])}",
        f"- qualifying calendar years: {gates['qualifying_calendar_years']}",
        "",
        "## Boundary",
        "",
        "- Canonical MEDIAN_180 RR core was not modified.",
        "- Cascade is diagnostic only in V1.",
        "- No production/live/Telegram/exchange behavior changed.",
        "- Pair-event samples are correlated and are not independent trades.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_STRESS_TEST",
        "",
    ]

    report = "\n".join(lines)
    (run_dir / "report.md").write_text(report, encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()
