from __future__ import annotations

import json
import os
import subprocess
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

import scripts.research_rr_ddg_acceleration_fork_v1 as base
from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import ASSETS, TARGET_ASSETS

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "rr_persist2_reexpand_fullpath_v1"

AS_OF = base.AS_OF
START = pd.Timestamp("2023-10-31T00:00:00Z")
PRIMARY_COST = 0.001
COST_GRID = (0.001, 0.005, 0.01, 0.02, 0.03)
ROLLING_DAYS = 365
ROLLING_STEP = 30
TARGET_STARTS = tuple(TARGET_ASSETS)
SECONDARY_STARTS = ("ATOM", "SOL", "LINK")
ACCEL_THRESHOLD = 0.10
TOL = 1e-12


def source_sha() -> str:
    return os.environ.get("SOURCE_COMMIT_SHA") or subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def download_btc_regime() -> pd.DataFrame:
    client = BinanceSpotRestClient()
    result = download_historical_dataset(
        client,
        symbol="BTCUSDT",
        start=pd.Timestamp("2022-01-01T00:00:00Z"),
        end=AS_OF,
        timeframe="1D",
        as_of=AS_OF,
    )
    if result.dataset is None:
        raise RuntimeError(f"BTCUSDT: no D1 dataset ({result.metadata.status})")
    if result.dataset.quality.has_critical_issues:
        raise RuntimeError("BTCUSDT: critical D1 data quality")

    frame = result.dataset.candles[["timestamp", "close"]].copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame["btc_sma200"] = frame["close"].rolling(200, min_periods=200).mean()
    frame["btc_regime"] = np.where(
        frame["btc_sma200"].isna(),
        "UNKNOWN",
        np.where(
            frame["close"] > frame["btc_sma200"],
            "BTC_BULL_START",
            "BTC_BEAR_START",
        ),
    )
    return frame.rename(columns={"close": "btc_close"})


def route_lookup(routes: pd.DataFrame) -> dict[tuple[pd.Timestamp, str], dict]:
    out = {}
    for _, row in routes.iterrows():
        key = (pd.Timestamp(row["signal_date"]), str(row["source"]).upper())
        out[key] = row.to_dict()
    return out


def top_candidate_and_impulse(
    panel: pd.DataFrame,
    held: str,
    entry_i: int,
    obs_i: int,
) -> tuple[str, float, list[tuple[str, float]]]:
    return base.strongest_accel(panel, held, entry_i, obs_i)


def candidate_impulse(
    scores: list[tuple[str, float]],
    candidate: str,
) -> float:
    mapping = dict(scores)
    return float(mapping[candidate])


def simulate_path(
    panel: pd.DataFrame,
    routes_map: dict[tuple[pd.Timestamp, str], dict],
    start_i: int,
    end_i: int,
    start_asset: str,
    cost: float,
    overlay: bool,
    collect_daily: bool = False,
) -> dict:
    if start_i >= end_i:
        raise ValueError("start_i must be < end_i")

    held = start_asset.upper()
    qty = 100.0 / float(panel.iloc[start_i][f"{held}_open"])
    entry_i = start_i

    overlay_armed = False
    stage = "INACTIVE"
    trigger_candidate: str | None = None
    trigger_impulse: float | None = None
    trigger_i: int | None = None

    scheduled: dict | None = None
    transitions = []
    daily = []
    held_days = Counter()

    initial_capital = 100.0
    peak = initial_capital
    max_drawdown = 0.0

    rr_count = 0
    accel_count = 0
    agree_count = 0

    def reset_overlay(armed: bool) -> None:
        nonlocal overlay_armed, stage, trigger_candidate, trigger_impulse, trigger_i
        overlay_armed = bool(armed)
        stage = "SEARCH" if armed else "INACTIVE"
        trigger_candidate = None
        trigger_impulse = None
        trigger_i = None

    for i in range(start_i, end_i + 1):
        ts = pd.Timestamp(panel.iloc[i]["timestamp"])

        if scheduled is not None:
            from_asset = held
            to_asset = str(scheduled["to_asset"]).upper()
            reason = str(scheduled["reason"])
            capital_before = qty * float(panel.iloc[i][f"{from_asset}_open"])
            capital_after = capital_before * (1.0 - cost)
            qty = capital_after / float(panel.iloc[i][f"{to_asset}_open"])
            held = to_asset
            entry_i = i

            transitions.append(
                {
                    "execute_date": ts,
                    "signal_date": scheduled["signal_date"],
                    "from_asset": from_asset,
                    "to_asset": to_asset,
                    "reason": reason,
                    "capital_before": capital_before,
                    "cost_rate": cost,
                    "cost_amount": capital_before * cost,
                    "capital_after": capital_after,
                }
            )

            if reason in {"RR", "CORE_ACCEL_AGREE"}:
                rr_count += 1
            if reason in {"ACCEL_OVERRIDE", "CORE_ACCEL_AGREE"}:
                accel_count += 1
            if reason == "CORE_ACCEL_AGREE":
                agree_count += 1

            # The frozen overlay is armed only after a core RR+DDG transition.
            reset_overlay(
                overlay and reason in {"RR", "CORE_ACCEL_AGREE"}
            )
            scheduled = None

        close_equity = qty * float(panel.iloc[i][f"{held}_close"])
        peak = max(peak, close_equity)
        dd = close_equity / peak - 1.0
        max_drawdown = min(max_drawdown, dd)
        held_days[held] += 1

        if collect_daily:
            daily.append(
                {
                    "date": ts,
                    "held_asset": held,
                    "equity": close_equity,
                    "drawdown": dd,
                    "overlay_stage": stage,
                    "overlay_armed": overlay_armed,
                }
            )

        if i >= end_i:
            continue

        rr_row = routes_map.get((ts, held))
        rr_dest = None if rr_row is None else str(rr_row["effective_to"]).upper()

        accel_pass_dest = None

        if overlay and overlay_armed:
            holding_day = i - entry_i + 1

            if stage == "SEARCH":
                if 1 <= holding_day <= 3:
                    top, impulse, scores = top_candidate_and_impulse(
                        panel, held, entry_i, i
                    )
                    if impulse >= ACCEL_THRESHOLD:
                        trigger_candidate = top
                        trigger_impulse = float(impulse)
                        trigger_i = i
                        stage = "WAIT1"
                    elif holding_day >= 3:
                        stage = "FAILED"
                elif holding_day > 3:
                    stage = "FAILED"

            elif stage == "WAIT1":
                assert trigger_candidate is not None
                assert trigger_i is not None
                if i == trigger_i + 1:
                    top, _, scores = top_candidate_and_impulse(
                        panel, held, entry_i, i
                    )
                    if top == trigger_candidate:
                        stage = "WAIT2"
                    else:
                        stage = "FAILED"

            elif stage == "WAIT2":
                assert trigger_candidate is not None
                assert trigger_impulse is not None
                assert trigger_i is not None
                if i == trigger_i + 2:
                    top, _, scores = top_candidate_and_impulse(
                        panel, held, entry_i, i
                    )
                    imp = candidate_impulse(scores, trigger_candidate)
                    if top == trigger_candidate and imp > trigger_impulse:
                        accel_pass_dest = trigger_candidate
                        stage = "PASSED"
                    else:
                        stage = "FAILED"

        if accel_pass_dest is not None:
            if rr_dest is not None and rr_dest == accel_pass_dest:
                reason = "CORE_ACCEL_AGREE"
            else:
                reason = "ACCEL_OVERRIDE"
            scheduled = {
                "signal_date": ts,
                "to_asset": accel_pass_dest,
                "reason": reason,
            }
        elif rr_dest is not None:
            # Core route interrupts any not-yet-confirmed acceleration setup.
            scheduled = {
                "signal_date": ts,
                "to_asset": rr_dest,
                "reason": "RR",
            }
            reset_overlay(False)

    final_equity = qty * float(panel.iloc[end_i][f"{held}_close"])
    total_return = final_equity / initial_capital - 1.0

    return {
        "start_asset": start_asset,
        "start_date": pd.Timestamp(panel.iloc[start_i]["timestamp"]),
        "end_date": pd.Timestamp(panel.iloc[end_i]["timestamp"]),
        "cost": float(cost),
        "overlay": bool(overlay),
        "final_asset": held,
        "final_capital": float(final_equity),
        "total_return": float(total_return),
        "max_drawdown": float(max_drawdown),
        "transition_count": int(len(transitions)),
        "rr_transition_count": int(rr_count),
        "accel_transition_count": int(accel_count),
        "core_accel_agree_count": int(agree_count),
        "held_days": dict(held_days),
        "transitions": transitions,
        "daily": daily,
    }


def pair_result(
    baseline: dict,
    overlay: dict,
) -> dict:
    return {
        "start_asset": baseline["start_asset"],
        "start_date": baseline["start_date"],
        "end_date": baseline["end_date"],
        "cost": baseline["cost"],
        "baseline_final_capital": baseline["final_capital"],
        "overlay_final_capital": overlay["final_capital"],
        "baseline_total_return": baseline["total_return"],
        "overlay_total_return": overlay["total_return"],
        "return_delta": overlay["total_return"] - baseline["total_return"],
        "capital_ratio_overlay_vs_baseline": (
            overlay["final_capital"] / baseline["final_capital"]
        ),
        "baseline_max_drawdown": baseline["max_drawdown"],
        "overlay_max_drawdown": overlay["max_drawdown"],
        "max_drawdown_delta": (
            overlay["max_drawdown"] - baseline["max_drawdown"]
        ),
        "baseline_transitions": baseline["transition_count"],
        "overlay_transitions": overlay["transition_count"],
        "extra_transitions": (
            overlay["transition_count"] - baseline["transition_count"]
        ),
        "overlay_accel_transitions": overlay["accel_transition_count"],
        "overlay_rr_transitions": overlay["rr_transition_count"],
        "baseline_final_asset": baseline["final_asset"],
        "overlay_final_asset": overlay["final_asset"],
        "result": (
            "BETTER"
            if overlay["final_capital"] > baseline["final_capital"] + TOL
            else "WORSE"
            if overlay["final_capital"] < baseline["final_capital"] - TOL
            else "EQUAL"
        ),
    }


def full_period_grid(
    panel: pd.DataFrame,
    routes_map: dict,
    start_i: int,
    end_i: int,
) -> tuple[pd.DataFrame, dict]:
    rows = []
    trace_store = {}

    for cost in COST_GRID:
        for asset in TARGET_STARTS:
            b = simulate_path(
                panel, routes_map, start_i, end_i, asset, cost, False
            )
            o = simulate_path(
                panel, routes_map, start_i, end_i, asset, cost, True
            )
            rows.append(pair_result(b, o))
            if cost == PRIMARY_COST:
                trace_store[(asset, "baseline")] = b
                trace_store[(asset, "overlay")] = o

    for asset in SECONDARY_STARTS:
        b = simulate_path(
            panel, routes_map, start_i, end_i, asset, PRIMARY_COST, False
        )
        o = simulate_path(
            panel, routes_map, start_i, end_i, asset, PRIMARY_COST, True
        )
        row = pair_result(b, o)
        row["secondary_start"] = True
        rows.append(row)
        trace_store[(asset, "baseline")] = b
        trace_store[(asset, "overlay")] = o

    frame = pd.DataFrame(rows)
    if "secondary_start" not in frame:
        frame["secondary_start"] = False
    frame["secondary_start"] = frame["secondary_start"].fillna(False)
    return frame, trace_store


def summarize_cost_grid(full: pd.DataFrame) -> pd.DataFrame:
    rows = []
    primary = full[~full["secondary_start"]].copy()
    for cost, sub in primary.groupby("cost", sort=True):
        rows.append(
            {
                "cost": float(cost),
                "starts": int(len(sub)),
                "better": int((sub["result"] == "BETTER").sum()),
                "equal": int((sub["result"] == "EQUAL").sum()),
                "worse": int((sub["result"] == "WORSE").sum()),
                "baseline_median_final_capital": float(
                    sub["baseline_final_capital"].median()
                ),
                "overlay_median_final_capital": float(
                    sub["overlay_final_capital"].median()
                ),
                "baseline_median_return": float(
                    sub["baseline_total_return"].median()
                ),
                "overlay_median_return": float(
                    sub["overlay_total_return"].median()
                ),
                "median_return_delta": float(sub["return_delta"].median()),
                "baseline_median_max_drawdown": float(
                    sub["baseline_max_drawdown"].median()
                ),
                "overlay_median_max_drawdown": float(
                    sub["overlay_max_drawdown"].median()
                ),
                "median_max_drawdown_delta": float(
                    sub["max_drawdown_delta"].median()
                ),
                "median_extra_transitions": float(
                    sub["extra_transitions"].median()
                ),
                "median_accel_transitions": float(
                    sub["overlay_accel_transitions"].median()
                ),
            }
        )
    return pd.DataFrame(rows)


def rolling_windows(
    panel: pd.DataFrame,
    routes_map: dict,
    btc_regime: pd.DataFrame,
    start_i: int,
    end_i: int,
) -> pd.DataFrame:
    btc_map = {
        pd.Timestamp(row["timestamp"]): str(row["btc_regime"])
        for _, row in btc_regime.iterrows()
    }
    rows = []

    last_start = end_i - ROLLING_DAYS
    for s in range(start_i, last_start + 1, ROLLING_STEP):
        e = s + ROLLING_DAYS
        start_date = pd.Timestamp(panel.iloc[s]["timestamp"])
        regime = btc_map.get(start_date, "UNKNOWN")

        for asset in TARGET_STARTS:
            b = simulate_path(
                panel, routes_map, s, e, asset, PRIMARY_COST, False
            )
            o = simulate_path(
                panel, routes_map, s, e, asset, PRIMARY_COST, True
            )
            row = pair_result(b, o)
            row["btc_regime"] = regime
            rows.append(row)

    return pd.DataFrame(rows)


def summarize_rolling(rolling: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for group_name, sub in [("ALL", rolling)] + [
        (regime, rolling[rolling["btc_regime"] == regime])
        for regime in ("BTC_BULL_START", "BTC_BEAR_START")
    ]:
        if sub.empty:
            continue
        rows.append(
            {
                "group": group_name,
                "paired_observations": int(len(sub)),
                "better": int((sub["result"] == "BETTER").sum()),
                "equal": int((sub["result"] == "EQUAL").sum()),
                "worse": int((sub["result"] == "WORSE").sum()),
                "better_rate": float((sub["result"] == "BETTER").mean()),
                "median_return_delta": float(sub["return_delta"].median()),
                "mean_return_delta": float(sub["return_delta"].mean()),
                "median_max_drawdown_delta": float(
                    sub["max_drawdown_delta"].median()
                ),
                "median_extra_transitions": float(
                    sub["extra_transitions"].median()
                ),
            }
        )
    return pd.DataFrame(rows)


def current_case_trace(
    panel: pd.DataFrame,
    routes_map: dict,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = pd.to_datetime(panel["timestamp"], utc=True)
    start_hits = np.flatnonzero(
        dates.eq(pd.Timestamp("2026-09-28T00:00:00Z")).to_numpy()
    )
    if not len(start_hits):
        raise RuntimeError("Current-case start date missing")
    s = int(start_hits[0])
    e = len(panel) - 1

    b = simulate_path(
        panel, routes_map, s, e, "LINK", PRIMARY_COST, False, collect_daily=True
    )
    o = simulate_path(
        panel, routes_map, s, e, "LINK", PRIMARY_COST, True, collect_daily=True
    )

    transitions = []
    for label, trace in (("BASELINE", b), ("OVERLAY", o)):
        for row in trace["transitions"]:
            transitions.append({"strategy": label, **row})

    daily = []
    for label, trace in (("BASELINE", b), ("OVERLAY", o)):
        for row in trace["daily"]:
            daily.append({"strategy": label, **row})

    return pd.DataFrame(transitions), pd.DataFrame(daily)


def classify(
    primary_row: pd.Series,
    rolling_summary: pd.DataFrame,
) -> str:
    rolling_all = rolling_summary[rolling_summary["group"] == "ALL"]
    if rolling_all.empty:
        return "FULL_PATH_MIXED"
    r = rolling_all.iloc[0]

    gate = (
        primary_row["overlay_median_final_capital"]
        > primary_row["baseline_median_final_capital"]
        and primary_row["better"] > primary_row["starts"] / 2.0
        and primary_row["overlay_median_max_drawdown"]
        >= primary_row["baseline_median_max_drawdown"] - 0.05
        and r["median_return_delta"] > 0
    )
    if gate:
        return "FULL_PATH_IMPROVEMENT_RESEARCH_ONLY"

    if (
        primary_row["overlay_median_final_capital"]
        < primary_row["baseline_median_final_capital"]
        and r["median_return_delta"] < 0
        and primary_row["better"] < primary_row["worse"]
    ):
        return "FULL_PATH_WORSE"
    return "FULL_PATH_MIXED"


def fmt_pct(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{100.0 * float(v):+.2f}%"


def fmt_num(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{float(v):.2f}"


def main() -> None:
    panel, data_meta = base.download_panel()
    events_by_date, states_by_date = base.build_monitor_history(panel)
    routes = base.build_effective_routes(panel, events_by_date, states_by_date)
    routes_map = route_lookup(routes)
    btc_regime = download_btc_regime()

    dates = pd.to_datetime(panel["timestamp"], utc=True)
    start_hits = np.flatnonzero(dates.eq(START).to_numpy())
    if not len(start_hits):
        raise RuntimeError(f"Primary start date missing: {START}")
    start_i = int(start_hits[0])
    end_i = len(panel) - 1

    full, traces = full_period_grid(
        panel, routes_map, start_i, end_i
    )
    cost_summary = summarize_cost_grid(full)

    rolling = rolling_windows(
        panel, routes_map, btc_regime, start_i, end_i
    )
    rolling_summary = summarize_rolling(rolling)

    case_transitions, case_daily = current_case_trace(panel, routes_map)

    primary_row = cost_summary[
        np.isclose(cost_summary["cost"], PRIMARY_COST)
    ].iloc[0]
    classification = classify(primary_row, rolling_summary)

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    full.to_csv(run_dir / "full_period_start_comparison.csv", index=False)
    cost_summary.to_csv(run_dir / "cost_sensitivity_summary.csv", index=False)
    rolling.to_csv(run_dir / "rolling_365d_pairs.csv", index=False)
    rolling_summary.to_csv(run_dir / "rolling_365d_summary.csv", index=False)
    case_transitions.to_csv(run_dir / "current_link_case_transitions.csv", index=False)
    case_daily.to_csv(run_dir / "current_link_case_daily.csv", index=False)

    transition_rows = []
    for (asset, strategy), trace in traces.items():
        for row in trace["transitions"]:
            transition_rows.append(
                {
                    "start_asset": asset,
                    "strategy": strategy.upper(),
                    **row,
                }
            )
    pd.DataFrame(transition_rows).to_csv(
        run_dir / "full_period_transition_ledger_primary_cost.csv",
        index=False,
    )

    summary = {
        "experiment": "RR_PERSIST2_REEXPAND_FULLPATH_V1",
        "workflow_mode": "STRESS_TEST_ONLY",
        "source_commit_sha": source_sha(),
        "start": START.isoformat(),
        "end": pd.Timestamp(panel.iloc[end_i]["timestamp"]).isoformat(),
        "as_of": AS_OF.isoformat(),
        "classification": classification,
        "primary_cost": PRIMARY_COST,
        "primary": primary_row.to_dict(),
        "rolling": json.loads(rolling_summary.to_json(orient="records")),
        "cost_sensitivity": json.loads(cost_summary.to_json(orient="records")),
        "production_changes": "NONE",
        "test_level": "GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_PATH_DEPENDENT_STRESS_TEST",
        "data_metadata": data_meta,
    }
    (run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )

    lines = [
        "# RR PERSIST2_REEXPAND FULL-PATH V1 — RESULTS",
        "",
        "Mode: STRESS_TEST_ONLY",
        "",
        f"Classification: {classification}",
        f"Start: {START.isoformat()}",
        f"End: {pd.Timestamp(panel.iloc[end_i]['timestamp']).isoformat()}",
        "",
        "## Full-period primary TARGET starts",
        "",
        "|Cost|Better / 10|Worse / 10|Baseline median capital|Overlay median capital|Median return delta|Baseline median DD|Overlay median DD|Median extra transitions|Median accel transitions|",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in cost_summary.iterrows():
        lines.append(
            f"|{100*row['cost']:.2f}%|{int(row['better'])}|{int(row['worse'])}|"
            f"{row['baseline_median_final_capital']:.2f}|"
            f"{row['overlay_median_final_capital']:.2f}|"
            f"{fmt_pct(row['median_return_delta'])}|"
            f"{fmt_pct(row['baseline_median_max_drawdown'])}|"
            f"{fmt_pct(row['overlay_median_max_drawdown'])}|"
            f"{row['median_extra_transitions']:.1f}|"
            f"{row['median_accel_transitions']:.1f}|"
        )

    primary_starts = full[
        (~full["secondary_start"])
        & np.isclose(full["cost"], PRIMARY_COST)
    ].copy()
    lines += [
        "",
        "## Primary 0.10% start paths",
        "",
        "|Start|Baseline capital|Overlay capital|Return delta|Baseline DD|Overlay DD|Transitions B/O|Accel switches|Final B/O|Result|",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for _, row in primary_starts.sort_values("start_asset").iterrows():
        lines.append(
            f"|{row['start_asset']}|{row['baseline_final_capital']:.2f}|"
            f"{row['overlay_final_capital']:.2f}|{fmt_pct(row['return_delta'])}|"
            f"{fmt_pct(row['baseline_max_drawdown'])}|"
            f"{fmt_pct(row['overlay_max_drawdown'])}|"
            f"{int(row['baseline_transitions'])}/{int(row['overlay_transitions'])}|"
            f"{int(row['overlay_accel_transitions'])}|"
            f"{row['baseline_final_asset']}/{row['overlay_final_asset']}|"
            f"{row['result']}|"
        )

    lines += [
        "",
        "## Rolling 365-day paired robustness",
        "",
        "|Group|Pairs|Better|Equal|Worse|Better rate|Median return delta|Mean return delta|Median DD delta|Median extra transitions|",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in rolling_summary.iterrows():
        lines.append(
            f"|{row['group']}|{int(row['paired_observations'])}|"
            f"{int(row['better'])}|{int(row['equal'])}|{int(row['worse'])}|"
            f"{100*row['better_rate']:.1f}%|{fmt_pct(row['median_return_delta'])}|"
            f"{fmt_pct(row['mean_return_delta'])}|"
            f"{fmt_pct(row['median_max_drawdown_delta'])}|"
            f"{row['median_extra_transitions']:.1f}|"
        )

    lines += [
        "",
        "## Current LINK -> TRX -> AAVE semantic trace",
        "",
        "|Strategy|Signal date|Execute date|From|To|Reason|",
        "|---|---|---|---|---|---|",
    ]
    for _, row in case_transitions.iterrows():
        lines.append(
            f"|{row['strategy']}|{pd.Timestamp(row['signal_date']).date()}|"
            f"{pd.Timestamp(row['execute_date']).date()}|"
            f"{row['from_asset']}|{row['to_asset']}|{row['reason']}|"
        )

    lines += [
        "",
        "## Decision boundary",
        "",
        "- The PERSIST2_REEXPAND rule was not retuned.",
        "- The overlay is path-dependent; acceleration switches alter the asset from which later RR signals are consumed.",
        "- Pending acceleration setups are cancelled by earlier core RR routes.",
        "- Cost sensitivity is predeclared and not optimized.",
        "- No production/live/Telegram/exchange behavior changed.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_PATH_DEPENDENT_STRESS_TEST",
        "",
    ]
    report = "\n".join(lines)
    (run_dir / "report.md").write_text(report, encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()
