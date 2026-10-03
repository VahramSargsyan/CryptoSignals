from __future__ import annotations

import itertools
import json
import os
import subprocess
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import (
    ARM_THRESHOLD,
    ASSETS,
    DESTINATION_DOMINANCE_MIN_STRENGTH_RATIO,
    LOOKBACK,
    REVERSAL,
    TARGET_ASSETS,
    PairState,
    choose_destination_dominance_override,
    choose_held_events,
    find_route_conflicts,
)

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "rr_ddg_acceleration_fork_v1"

AS_OF = pd.Timestamp("2026-10-03T16:30:00Z")
DOWNLOAD_START = pd.Timestamp("2023-05-05T00:00:00Z")
RECENT_START = pd.Timestamp("2026-09-03T00:00:00Z")
CASE_DATE = pd.Timestamp("2026-09-28T00:00:00Z")
ACCEL_THRESHOLD = 0.10
OBS_DAYS = (1, 2, 3)
HORIZONS = (3, 7, 14, 30)


def source_sha() -> str:
    return os.environ.get("SOURCE_COMMIT_SHA") or subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def download_panel() -> tuple[pd.DataFrame, dict]:
    client = BinanceSpotRestClient()
    pieces = []
    meta = {}

    for asset in ASSETS:
        result = download_historical_dataset(
            client,
            symbol=f"{asset}USDT",
            start=DOWNLOAD_START,
            end=AS_OF,
            timeframe="1D",
            as_of=AS_OF,
        )
        if result.dataset is None:
            raise RuntimeError(f"{asset}: no D1 dataset ({result.metadata.status})")
        if result.dataset.quality.has_critical_issues:
            raise RuntimeError(f"{asset}: critical D1 quality issue")

        frame = result.dataset.candles[["timestamp", "open", "close"]].copy()
        frame = frame.rename(
            columns={"open": f"{asset}_open", "close": f"{asset}_close"}
        )
        pieces.append(frame)
        meta[asset] = {
            "dataset_id": result.dataset.dataset_id,
            "actual_start": result.metadata.actual_start,
            "actual_end": result.metadata.actual_end,
            "rows": int(len(frame)),
        }

    panel = pieces[0]
    for frame in pieces[1:]:
        panel = panel.merge(frame, on="timestamp", how="inner", validate="one_to_one")
    panel = panel.sort_values("timestamp", kind="stable").reset_index(drop=True)

    meta["panel"] = {
        "rows": int(len(panel)),
        "start": pd.Timestamp(panel.iloc[0]["timestamp"]).isoformat(),
        "end": pd.Timestamp(panel.iloc[-1]["timestamp"]).isoformat(),
    }
    return panel, meta


def _event_row(
    ts: pd.Timestamp,
    event: str,
    pair: str,
    from_asset: str,
    to_asset: str,
    ratio: float,
    median: float,
    deviation: float,
    max_dislocation: float,
    reversal_from_extreme: float,
) -> dict:
    return {
        "date": ts.isoformat(),
        "event": event,
        "pair": pair,
        "from_asset": from_asset,
        "to_asset": to_asset,
        "ratio": ratio,
        "median": median,
        "deviation": deviation,
        "max_dislocation": max_dislocation,
        "reversal_from_extreme": reversal_from_extreme,
    }


def build_monitor_history(
    panel: pd.DataFrame,
) -> tuple[dict[pd.Timestamp, list[dict]], dict[pd.Timestamp, list[dict]]]:
    events_by_date: dict[pd.Timestamp, list[dict]] = defaultdict(list)
    states_by_date: dict[pd.Timestamp, list[dict]] = defaultdict(list)

    for left, right in itertools.combinations(ASSETS, 2):
        ratio_series = panel[f"{right}_close"] / panel[f"{left}_close"]
        median_series = ratio_series.rolling(LOOKBACK, min_periods=LOOKBACK).median()
        deviation_series = ratio_series / median_series - 1.0
        state = PairState()

        for idx, ts_raw in enumerate(panel["timestamp"]):
            ts = pd.Timestamp(ts_raw)
            median_raw = median_series.iloc[idx]
            dev_raw = deviation_series.iloc[idx]
            if pd.isna(median_raw) or pd.isna(dev_raw):
                continue

            ratio = float(ratio_series.iloc[idx])
            median = float(median_raw)
            deviation = float(dev_raw)
            pair = f"{left}/{right}"

            if state.mode == "NONE":
                if deviation >= ARM_THRESHOLD:
                    state = PairState(
                        mode="HIGH",
                        extreme=ratio,
                        max_dislocation=abs(deviation),
                        armed_at=ts,
                    )
                    events_by_date[ts].append(
                        _event_row(
                            ts, "ARMED", pair, right, left, ratio, median,
                            deviation, abs(deviation), 0.0
                        )
                    )
                elif deviation <= -ARM_THRESHOLD:
                    state = PairState(
                        mode="LOW",
                        extreme=ratio,
                        max_dislocation=abs(deviation),
                        armed_at=ts,
                    )
                    events_by_date[ts].append(
                        _event_row(
                            ts, "ARMED", pair, left, right, ratio, median,
                            deviation, abs(deviation), 0.0
                        )
                    )
            elif state.mode == "HIGH":
                assert state.extreme is not None
                if ratio > state.extreme:
                    state.extreme = ratio
                    state.max_dislocation = max(
                        state.max_dislocation, abs(deviation)
                    )
                retracement = 1.0 - ratio / state.extreme
                if retracement >= REVERSAL:
                    events_by_date[ts].append(
                        _event_row(
                            ts, "CONFIRMED", pair, right, left, ratio, median,
                            deviation, state.max_dislocation, retracement
                        )
                    )
                    state = PairState()
            elif state.mode == "LOW":
                assert state.extreme is not None
                if ratio < state.extreme:
                    state.extreme = ratio
                    state.max_dislocation = max(
                        state.max_dislocation, abs(deviation)
                    )
                retracement = ratio / state.extreme - 1.0
                if retracement >= REVERSAL:
                    events_by_date[ts].append(
                        _event_row(
                            ts, "CONFIRMED", pair, left, right, ratio, median,
                            deviation, state.max_dislocation, retracement
                        )
                    )
                    state = PairState()

            prospective_from = None
            prospective_to = None
            retracement_now = None
            if state.mode == "HIGH" and state.extreme is not None:
                prospective_from, prospective_to = right, left
                retracement_now = max(0.0, 1.0 - ratio / state.extreme)
            elif state.mode == "LOW" and state.extreme is not None:
                prospective_from, prospective_to = left, right
                retracement_now = max(0.0, ratio / state.extreme - 1.0)

            states_by_date[ts].append(
                {
                    "pair": pair,
                    "mode": state.mode,
                    "from_asset": prospective_from,
                    "to_asset": prospective_to,
                    "armed_at": (
                        None if state.armed_at is None
                        else pd.Timestamp(state.armed_at).isoformat()
                    ),
                    "ratio": ratio,
                    "median": median,
                    "deviation": deviation,
                    "extreme": state.extreme,
                    "max_dislocation": state.max_dislocation,
                    "reversal_from_extreme": retracement_now,
                }
            )

    return events_by_date, states_by_date


def build_effective_routes(
    panel: pd.DataFrame,
    events_by_date: dict[pd.Timestamp, list[dict]],
    states_by_date: dict[pd.Timestamp, list[dict]],
) -> pd.DataFrame:
    rows = []
    for ts_raw in panel["timestamp"]:
        ts = pd.Timestamp(ts_raw)
        events = events_by_date.get(ts, [])
        states = states_by_date.get(ts, [])
        if not events:
            continue

        for source in ASSETS:
            picked = choose_held_events(
                events,
                held_asset=source,
                latest_date=ts,
                allowed_to_assets=TARGET_ASSETS,
            )
            primary = picked.get("primary_confirmed")
            if primary is None:
                continue

            conflicts = find_route_conflicts(
                events,
                states,
                primary_confirmed=primary,
                latest_date=ts,
                allowed_to_assets=TARGET_ASSETS,
            )
            effective, chosen_conflict = choose_destination_dominance_override(
                primary, conflicts
            )
            if effective is None:
                continue

            competing = (
                {}
                if chosen_conflict is None
                else dict(chosen_conflict.get("competing_candidate") or {})
            )
            relation = (
                {}
                if chosen_conflict is None
                else dict(chosen_conflict.get("destination_relation") or {})
            )

            rows.append(
                {
                    "signal_date": ts,
                    "source": source,
                    "baseline_to": str(primary["to_asset"]).upper(),
                    "effective_to": str(effective["to_asset"]).upper(),
                    "baseline_pair": primary.get("pair"),
                    "baseline_max_dislocation": float(
                        primary.get("max_dislocation") or 0.0
                    ),
                    "baseline_reversal": float(
                        primary.get("reversal_from_extreme") or 0.0
                    ),
                    "override": chosen_conflict is not None,
                    "competing_to": str(competing.get("to_asset") or "").upper(),
                    "competing_state": str(competing.get("event") or ""),
                    "competing_max_dislocation": (
                        np.nan
                        if not competing
                        else float(competing.get("max_dislocation") or 0.0)
                    ),
                    "strength_ratio": (
                        np.nan
                        if chosen_conflict is None
                        else float(chosen_conflict.get("strength_ratio") or np.nan)
                    ),
                    "destination_relation_state": str(
                        relation.get("event") or ""
                    ),
                    "effective_pair": effective.get("pair"),
                }
            )

    return (
        pd.DataFrame(rows)
        .sort_values(["signal_date", "source"])
        .reset_index(drop=True)
    )


def rel_impulse(
    panel: pd.DataFrame,
    candidate: str,
    held: str,
    entry_i: int,
    obs_i: int,
) -> float:
    c = (
        float(panel.iloc[obs_i][f"{candidate}_close"])
        / float(panel.iloc[entry_i][f"{candidate}_open"])
    )
    h = (
        float(panel.iloc[obs_i][f"{held}_close"])
        / float(panel.iloc[entry_i][f"{held}_open"])
    )
    return c / h - 1.0


def strongest_accel(
    panel: pd.DataFrame,
    held: str,
    entry_i: int,
    obs_i: int,
    exclude: set[str] | None = None,
) -> tuple[str, float, list[tuple[str, float]]]:
    exclude = set() if exclude is None else {x.upper() for x in exclude}
    candidates = [
        a for a in TARGET_ASSETS
        if a != held and a not in exclude
    ]
    scores = [(a, rel_impulse(panel, a, held, entry_i, obs_i)) for a in candidates]
    scores.sort(key=lambda x: (-x[1], x[0]))
    return scores[0][0], float(scores[0][1]), scores


def fwd_open_ret(
    panel: pd.DataFrame, asset: str, start_i: int, horizon: int
) -> float:
    end_i = start_i + horizon
    if start_i >= len(panel) or end_i >= len(panel):
        return np.nan
    return (
        float(panel.iloc[end_i][f"{asset}_open"])
        / float(panel.iloc[start_i][f"{asset}_open"])
        - 1.0
    )


def latest_ret(panel: pd.DataFrame, asset: str, start_i: int) -> float:
    if start_i >= len(panel):
        return np.nan
    return (
        float(panel.iloc[-1][f"{asset}_close"])
        / float(panel.iloc[start_i][f"{asset}_open"])
        - 1.0
    )


def rel_excess(c_ret: float, b_ret: float) -> float:
    if pd.isna(c_ret) or pd.isna(b_ret):
        return np.nan
    return (1.0 + float(c_ret)) / (1.0 + float(b_ret)) - 1.0


def build_post_route_acceleration(
    panel: pd.DataFrame, routes: pd.DataFrame
) -> pd.DataFrame:
    date_to_i = {
        pd.Timestamp(ts): i
        for i, ts in enumerate(pd.to_datetime(panel["timestamp"], utc=True))
    }
    rows = []

    for rid, route in routes.iterrows():
        signal_date = pd.Timestamp(route["signal_date"])
        signal_i = date_to_i[signal_date]
        entry_i = signal_i + 1
        if entry_i + 2 >= len(panel):
            continue

        held = str(route["effective_to"])
        trigger = None
        for day in OBS_DAYS:
            obs_i = entry_i + day - 1
            candidate, impulse, scores = strongest_accel(
                panel, held, entry_i, obs_i
            )
            if impulse >= ACCEL_THRESHOLD:
                trigger = (day, obs_i, candidate, impulse, scores)
                break

        base = {
            "route_id": int(rid),
            "signal_date": signal_date,
            "source": str(route["source"]),
            "baseline_to": str(route["baseline_to"]),
            "effective_to": held,
            "ddg_override": bool(route["override"]),
            "triggered": trigger is not None,
        }
        if trigger is None:
            rows.append(base)
            continue

        day, obs_i, candidate, impulse, scores = trigger
        switch_i = obs_i + 1
        row = {
            **base,
            "candidate": candidate,
            "detection_day": day,
            "detection_date": pd.Timestamp(panel.iloc[obs_i]["timestamp"]),
            "switch_date": (
                pd.Timestamp(panel.iloc[switch_i]["timestamp"])
                if switch_i < len(panel) else pd.NaT
            ),
            "trigger_impulse": impulse,
        }
        for h in HORIZONS:
            cr = fwd_open_ret(panel, candidate, switch_i, h)
            br = fwd_open_ret(panel, held, switch_i, h)
            row[f"candidate_fwd_{h}d"] = cr
            row[f"held_fwd_{h}d"] = br
            row[f"relative_excess_{h}d"] = rel_excess(cr, br)
        rows.append(row)

    return pd.DataFrame(rows)


def build_next_signal_forks(
    panel: pd.DataFrame, routes: pd.DataFrame
) -> pd.DataFrame:
    date_to_i = {
        pd.Timestamp(ts): i
        for i, ts in enumerate(pd.to_datetime(panel["timestamp"], utc=True))
    }
    route_map = {
        (pd.Timestamp(r["signal_date"]), str(r["source"])): r
        for _, r in routes.iterrows()
    }
    rows = []

    for rid, first in routes.iterrows():
        signal_date = pd.Timestamp(first["signal_date"])
        signal_i = date_to_i[signal_date]
        entry_i = signal_i + 1
        if entry_i >= len(panel):
            continue

        held = str(first["effective_to"])
        found = None

        for day in OBS_DAYS:
            obs_i = entry_i + day - 1
            if obs_i >= len(panel):
                break
            obs_date = pd.Timestamp(panel.iloc[obs_i]["timestamp"])
            nxt = route_map.get((obs_date, held))
            if nxt is not None:
                found = (day, obs_i, nxt)
                break

        if found is None:
            continue

        day, obs_i, nxt = found
        rr_dest = str(nxt["effective_to"])
        candidate, impulse, scores = strongest_accel(
            panel,
            held,
            entry_i,
            obs_i,
            exclude={rr_dest},
        )
        conflict = candidate != rr_dest and impulse >= ACCEL_THRESHOLD
        switch_i = obs_i + 1

        row = {
            "initial_route_id": int(rid),
            "initial_signal_date": signal_date,
            "initial_source": str(first["source"]),
            "initial_effective_to": held,
            "initial_ddg_override": bool(first["override"]),
            "next_signal_day": day,
            "next_signal_date": pd.Timestamp(panel.iloc[obs_i]["timestamp"]),
            "rr_next_to": rr_dest,
            "rr_next_ddg_override": bool(nxt["override"]),
            "accel_candidate": candidate,
            "accel_impulse_vs_held": impulse,
            "conflict": conflict,
            "switch_date": (
                pd.Timestamp(panel.iloc[switch_i]["timestamp"])
                if switch_i < len(panel) else pd.NaT
            ),
        }

        if conflict:
            for h in HORIZONS:
                cr = fwd_open_ret(panel, candidate, switch_i, h)
                rr = fwd_open_ret(panel, rr_dest, switch_i, h)
                row[f"candidate_fwd_{h}d"] = cr
                row[f"rr_fwd_{h}d"] = rr
                row[f"candidate_vs_rr_excess_{h}d"] = rel_excess(cr, rr)

        rows.append(row)

    return pd.DataFrame(rows)


def summarize_accel(accel: pd.DataFrame) -> pd.DataFrame:
    trig = accel[accel["triggered"]].copy()
    rows = []
    for h in HORIZONS:
        vals = trig[f"relative_excess_{h}d"].dropna().astype(float)
        rows.append(
            {
                "horizon_days": h,
                "eligible_routes": int(len(accel)),
                "trigger_count": int(len(trig)),
                "trigger_rate": float(len(trig) / len(accel)) if len(accel) else np.nan,
                "forward_n": int(len(vals)),
                "candidate_beats_held_rate": (
                    float((vals > 0).mean()) if len(vals) else np.nan
                ),
                "median_relative_excess": (
                    float(vals.median()) if len(vals) else np.nan
                ),
                "mean_relative_excess": (
                    float(vals.mean()) if len(vals) else np.nan
                ),
                "strong_continuation_ge20_rate": (
                    float((vals >= 0.20).mean()) if len(vals) else np.nan
                ),
            }
        )
    return pd.DataFrame(rows)


def summarize_forks(forks: pd.DataFrame) -> pd.DataFrame:
    conflicts = forks[forks["conflict"]].copy()
    rows = []
    for h in HORIZONS:
        col = f"candidate_vs_rr_excess_{h}d"
        vals = conflicts[col].dropna().astype(float) if col in conflicts else pd.Series(dtype=float)
        rows.append(
            {
                "horizon_days": h,
                "short_hop_cases": int(len(forks)),
                "conflict_count": int(len(conflicts)),
                "conflict_rate": (
                    float(len(conflicts) / len(forks)) if len(forks) else np.nan
                ),
                "forward_n": int(len(vals)),
                "candidate_beats_rr_rate": (
                    float((vals > 0).mean()) if len(vals) else np.nan
                ),
                "median_candidate_vs_rr_excess": (
                    float(vals.median()) if len(vals) else np.nan
                ),
                "mean_candidate_vs_rr_excess": (
                    float(vals.mean()) if len(vals) else np.nan
                ),
                "strong_continuation_ge20_rate": (
                    float((vals >= 0.20).mean()) if len(vals) else np.nan
                ),
            }
        )
    return pd.DataFrame(rows)


def build_case(
    panel: pd.DataFrame,
    routes: pd.DataFrame,
) -> tuple[pd.DataFrame, dict]:
    route = routes[
        (routes["signal_date"] == CASE_DATE)
        & (routes["source"] == "LINK")
    ].copy()
    if route.empty:
        raise RuntimeError("Missing LINK route on 2026-09-28")
    route = route.iloc[0]

    dates = pd.to_datetime(panel["timestamp"], utc=True)
    signal_i = int(np.flatnonzero(dates.eq(CASE_DATE).to_numpy())[0])
    entry_i = signal_i + 1
    corrected = str(route["effective_to"])

    rows = []
    aave_first = None
    for day in OBS_DAYS:
        obs_i = entry_i + day - 1
        top, top_imp, scores = strongest_accel(
            panel, corrected, entry_i, obs_i
        )
        score_map = dict(scores)
        aave_imp = float(score_map["AAVE"])
        rows.append(
            {
                "day": day,
                "date": pd.Timestamp(panel.iloc[obs_i]["timestamp"]),
                "corrected_held": corrected,
                "top_candidate": top,
                "top_impulse": top_imp,
                "aave_impulse_vs_corrected": aave_imp,
                "aave_rank": 1 + [a for a, _ in scores].index("AAVE"),
            }
        )
        if aave_first is None and aave_imp >= ACCEL_THRESHOLD:
            switch_i = obs_i + 1
            aave_first = {
                "detection_day": day,
                "detection_date": pd.Timestamp(panel.iloc[obs_i]["timestamp"]).isoformat(),
                "switch_date": (
                    pd.Timestamp(panel.iloc[switch_i]["timestamp"]).isoformat()
                    if switch_i < len(panel) else None
                ),
                "aave_impulse": aave_imp,
                "top_candidate": top,
                "aave_was_top": top == "AAVE",
                "aave_latest_return": latest_ret(panel, "AAVE", switch_i),
                "corrected_latest_return": latest_ret(panel, corrected, switch_i),
            }
            aave_first["aave_vs_corrected_latest_excess"] = rel_excess(
                aave_first["aave_latest_return"],
                aave_first["corrected_latest_return"],
            )

    info = {
        "signal_date": CASE_DATE.isoformat(),
        "baseline_to": str(route["baseline_to"]),
        "effective_to": corrected,
        "ddg_override": bool(route["override"]),
        "competing_to": str(route["competing_to"]),
        "strength_ratio": (
            None if pd.isna(route["strength_ratio"])
            else float(route["strength_ratio"])
        ),
        "entry_date": pd.Timestamp(panel.iloc[entry_i]["timestamp"]).isoformat(),
        "aave_first_10pct": aave_first,
    }
    return pd.DataFrame(rows), info


def fmt_pct(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{100*float(v):+.2f}%"


def fmt_rate(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{100*float(v):.1f}%"


def fmt_ratio(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{float(v):.3f}x"


def main() -> None:
    panel, data_meta = download_panel()
    events_by_date, states_by_date = build_monitor_history(panel)
    routes = build_effective_routes(panel, events_by_date, states_by_date)

    accel = build_post_route_acceleration(panel, routes)
    forks = build_next_signal_forks(panel, routes)
    accel_summary = summarize_accel(accel)
    fork_summary = summarize_forks(forks)

    recent_routes = routes[routes["signal_date"] >= RECENT_START].copy()
    recent_accel = accel[accel["signal_date"] >= RECENT_START].copy()
    recent_forks = forks[forks["initial_signal_date"] >= RECENT_START].copy()

    case_table, case_info = build_case(panel, routes)

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    routes.to_csv(run_dir / "all_effective_routes.csv", index=False)
    recent_routes.to_csv(run_dir / "recent_month_effective_routes.csv", index=False)
    accel.to_csv(run_dir / "post_route_acceleration.csv", index=False)
    recent_accel.to_csv(run_dir / "recent_month_acceleration.csv", index=False)
    forks.to_csv(run_dir / "next_signal_acceleration_forks.csv", index=False)
    recent_forks.to_csv(run_dir / "recent_month_forks.csv", index=False)
    accel_summary.to_csv(run_dir / "acceleration_summary.csv", index=False)
    fork_summary.to_csv(run_dir / "fork_summary.csv", index=False)
    case_table.to_csv(run_dir / "link_case_corrected_vs_aave.csv", index=False)
    (run_dir / "link_case.json").write_text(
        json.dumps(case_info, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )

    a14 = accel_summary[accel_summary["horizon_days"] == 14].iloc[0]
    f14 = fork_summary[fork_summary["horizon_days"] == 14].iloc[0]
    summary = {
        "experiment": "RR_DDG_ACCELERATION_FORK_V1",
        "source_commit_sha": source_sha(),
        "as_of": AS_OF.isoformat(),
        "latest_closed_candle": pd.Timestamp(panel.iloc[-1]["timestamp"]).isoformat(),
        "recent_start": RECENT_START.isoformat(),
        "all_effective_route_count": int(len(routes)),
        "recent_effective_route_count": int(len(recent_routes)),
        "post_route_acceleration_14d": {
            k: (None if pd.isna(v) else float(v) if isinstance(v, (float, np.floating)) else int(v))
            for k, v in a14.to_dict().items()
        },
        "next_signal_fork_14d": {
            k: (None if pd.isna(v) else float(v) if isinstance(v, (float, np.floating)) else int(v))
            for k, v in f14.to_dict().items()
        },
        "recent_acceleration_triggers": int(recent_accel["triggered"].sum()) if len(recent_accel) else 0,
        "recent_fork_conflicts": int(recent_forks["conflict"].sum()) if len(recent_forks) else 0,
        "link_case": case_info,
        "production_changes": "NONE",
        "test_level": "GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_STRESS_TEST",
        "data_metadata": data_meta,
    }
    (run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )

    lines = [
        "# RR DDG + ACCELERATION FORK V1 — RESULTS",
        "",
        "Mode: STRESS_TEST_ONLY",
        "",
        f"As-of: {AS_OF.isoformat()}",
        f"Latest closed D1: {pd.Timestamp(panel.iloc[-1]['timestamp']).isoformat()}",
        f"All corrected RR+DDG route opportunities: {len(routes)}",
        f"Recent-month corrected routes from {RECENT_START.date()}: {len(recent_routes)}",
        "",
        "## Test B — post-route acceleration after corrected RR+DDG destination",
        "",
        "|Horizon|Triggers|Trigger rate|Forward N|Candidate beats corrected held|Median excess|Mean excess|>=20% continuation|",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, r in accel_summary.iterrows():
        lines.append(
            f"|{int(r['horizon_days'])}d|{int(r['trigger_count'])}|"
            f"{fmt_rate(r['trigger_rate'])}|{int(r['forward_n'])}|"
            f"{fmt_rate(r['candidate_beats_held_rate'])}|"
            f"{fmt_pct(r['median_relative_excess'])}|"
            f"{fmt_pct(r['mean_relative_excess'])}|"
            f"{fmt_rate(r['strong_continuation_ge20_rate'])}|"
        )

    lines += [
        "",
        "## Test C — next RR signal vs acceleration fork",
        "",
        "|Horizon|Short-hop cases|Conflicts|Conflict rate|Forward N|Acceleration beats RR|Median excess|Mean excess|>=20% continuation|",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, r in fork_summary.iterrows():
        lines.append(
            f"|{int(r['horizon_days'])}d|{int(r['short_hop_cases'])}|"
            f"{int(r['conflict_count'])}|{fmt_rate(r['conflict_rate'])}|"
            f"{int(r['forward_n'])}|{fmt_rate(r['candidate_beats_rr_rate'])}|"
            f"{fmt_pct(r['median_candidate_vs_rr_excess'])}|"
            f"{fmt_pct(r['mean_candidate_vs_rr_excess'])}|"
            f"{fmt_rate(r['strong_continuation_ge20_rate'])}|"
        )

    lines += [
        "",
        "## LINK 2026-09-28 under current corrected router",
        "",
        f"- old baseline: LINK -> {case_info['baseline_to']}",
        f"- current effective route: LINK -> {case_info['effective_to']}",
        f"- DDG override: {case_info['ddg_override']}",
        f"- competing destination: {case_info['competing_to']}",
        f"- strength ratio: {case_info['strength_ratio']}",
        "",
        "|Day|Date|Corrected held|Top acceleration|Top impulse|AAVE impulse|AAVE rank|",
        "|---:|---|---|---|---:|---:|---:|",
    ]
    for _, r in case_table.iterrows():
        lines.append(
            f"|{int(r['day'])}|{pd.Timestamp(r['date']).date()}|"
            f"{r['corrected_held']}|{r['top_candidate']}|"
            f"{fmt_pct(r['top_impulse'])}|"
            f"{fmt_pct(r['aave_impulse_vs_corrected'])}|"
            f"{int(r['aave_rank'])}|"
        )

    cross = case_info["aave_first_10pct"]
    if cross is None:
        lines += ["", "- AAVE did not cross +10% versus corrected destination in first 3 bars."]
    else:
        lines += [
            "",
            f"- First AAVE +10% detection: day {cross['detection_day']} at {cross['detection_date']}.",
            f"- Hypothetical switch: {cross['switch_date']}.",
            f"- AAVE was top candidate: {cross['aave_was_top']}.",
            f"- AAVE relative excess vs corrected destination to latest close: {fmt_pct(cross['aave_vs_corrected_latest_excess'])}.",
        ]

    lines += [
        "",
        "## Recent-month corrected route ledger",
        "",
        "|Signal date|Source|Baseline|Effective|DDG override|Strength ratio|",
        "|---|---|---|---|---|---:|",
    ]
    for _, r in recent_routes.iterrows():
        lines.append(
            f"|{pd.Timestamp(r['signal_date']).date()}|{r['source']}|"
            f"{r['baseline_to']}|{r['effective_to']}|"
            f"{'YES' if r['override'] else 'NO'}|"
            f"{fmt_ratio(r['strength_ratio'])}|"
        )

    lines += [
        "",
        "## Boundary",
        "",
        "- Current production behavior was not changed.",
        "- Recent-month tables are descriptive; full-history summaries are the main generalization check.",
        "- Any promotion requires a separate full-path/path-dependent simulation with execution costs.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_STRESS_TEST",
        "",
    ]

    report = "\n".join(lines)
    (run_dir / "report.md").write_text(report, encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()
