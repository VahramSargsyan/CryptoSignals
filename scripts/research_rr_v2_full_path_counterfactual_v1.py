from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass, asdict
from pathlib import Path

import numpy as np
import pandas as pd

import scripts.research_rr_ddg_acceleration_fork_v1 as base
from strategies.crypto.relative_rotation.paper_live import ASSETS, TARGET_ASSETS

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "rr_v2_full_path_counterfactual_v1"

AS_OF = base.AS_OF
PRIMARY_COST = 0.001
COSTS = (0.001, 0.005, 0.01, 0.03)
ACCEL_THRESHOLD = 0.10
OBS_DAYS = (1, 2, 3)

START_ASSETS = tuple(ASSETS)
TARGET_START_ASSETS = tuple(TARGET_ASSETS)

WINDOW_STARTS = {
    "MATURE": pd.Timestamp("2023-10-31T00:00:00Z"),
    "LAST_2Y": pd.Timestamp("2024-10-03T00:00:00Z"),
    "LAST_1Y": pd.Timestamp("2025-10-03T00:00:00Z"),
}

VARIANTS = ("CORE", "CORE_PLUS_V2")
PRIORITIES = ("V2_PRIORITY", "CORE_PRIORITY")


@dataclass
class Watch:
    entry_i: int
    stage: str = "SEARCH"
    candidate: str | None = None
    trigger_i: int | None = None
    trigger_impulse: float | None = None
    confirm1_i: int | None = None


def source_sha() -> str:
    return os.environ.get("SOURCE_COMMIT_SHA") or subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def first_index_on_or_after(timestamps: pd.Series, ts: pd.Timestamp) -> int:
    arr = pd.to_datetime(timestamps, utc=True)
    hits = np.flatnonzero(arr.ge(ts).to_numpy())
    if not len(hits):
        raise RuntimeError(f"No timestamp on/after {ts}")
    return int(hits[0])


def build_route_map(routes: pd.DataFrame) -> dict[tuple[pd.Timestamp, str], dict]:
    out = {}
    for _, row in routes.iterrows():
        out[(pd.Timestamp(row["signal_date"]), str(row["source"]))] = row.to_dict()
    return out


def strongest_candidate(
    panel: pd.DataFrame,
    held: str,
    entry_i: int,
    obs_i: int,
) -> tuple[str, float]:
    candidate, impulse, _ = base.strongest_accel(
        panel,
        held,
        entry_i,
        obs_i,
    )
    return str(candidate), float(impulse)


def candidate_impulse(
    panel: pd.DataFrame,
    candidate: str,
    held: str,
    entry_i: int,
    obs_i: int,
) -> float:
    return float(base.rel_impulse(panel, candidate, held, entry_i, obs_i))


def transition(
    panel: pd.DataFrame,
    idx: int,
    asset: str,
    qty: float,
    target: str,
    cost_rate: float,
) -> tuple[str, float, float, float, float]:
    if target == asset:
        value = qty * float(panel.iloc[idx][f"{asset}_open"])
        return asset, qty, 0.0, value, value

    before = qty * float(panel.iloc[idx][f"{asset}_open"])
    fee = before * cost_rate
    after = before - fee
    new_qty = after / float(panel.iloc[idx][f"{target}_open"])
    return target, new_qty, fee, before, after


def evaluate_watch_close(
    panel: pd.DataFrame,
    idx: int,
    held: str,
    watch: Watch | None,
) -> tuple[Watch | None, dict | None, dict]:
    diag = {
        "base_trigger": False,
        "confirm1": False,
        "confirm2": False,
        "watch_failed": False,
        "watch_expired": False,
    }
    if watch is None:
        return None, None, diag

    if watch.stage == "SEARCH":
        day = idx - watch.entry_i + 1
        if day < 1:
            return watch, None, diag
        if day > max(OBS_DAYS):
            diag["watch_expired"] = True
            return None, None, diag

        top, impulse = strongest_candidate(panel, held, watch.entry_i, idx)
        if impulse >= ACCEL_THRESHOLD:
            watch.stage = "TRIGGERED"
            watch.candidate = top
            watch.trigger_i = idx
            watch.trigger_impulse = impulse
            diag["base_trigger"] = True
        return watch, None, diag

    if watch.stage == "TRIGGERED":
        assert watch.candidate is not None
        assert watch.trigger_i is not None
        assert watch.trigger_impulse is not None

        if idx <= watch.trigger_i:
            return watch, None, diag

        top, _ = strongest_candidate(panel, held, watch.entry_i, idx)
        cand_imp = candidate_impulse(
            panel, watch.candidate, held, watch.entry_i, idx
        )

        if top != watch.candidate:
            diag["watch_failed"] = True
            return None, None, diag

        watch.stage = "CONFIRM1"
        watch.confirm1_i = idx
        diag["confirm1"] = True
        return watch, None, diag

    if watch.stage == "CONFIRM1":
        assert watch.candidate is not None
        assert watch.confirm1_i is not None
        assert watch.trigger_impulse is not None

        if idx <= watch.confirm1_i:
            return watch, None, diag

        top, _ = strongest_candidate(panel, held, watch.entry_i, idx)
        cand_imp = candidate_impulse(
            panel, watch.candidate, held, watch.entry_i, idx
        )

        if top != watch.candidate or cand_imp <= watch.trigger_impulse:
            diag["watch_failed"] = True
            return None, None, diag

        diag["confirm2"] = True
        action = {
            "target": watch.candidate,
            "reason": "V2",
            "trigger_impulse": float(watch.trigger_impulse),
            "confirm2_impulse": float(cand_imp),
            "trigger_i": int(watch.trigger_i),
            "confirm2_i": int(idx),
        }
        return None, action, diag

    raise RuntimeError(f"Unknown watch stage {watch.stage}")


def simulate(
    panel: pd.DataFrame,
    route_map: dict[tuple[pd.Timestamp, str], dict],
    start_i: int,
    end_i: int,
    start_asset: str,
    variant: str,
    cost_rate: float,
    priority: str,
    collect_trace: bool = False,
) -> tuple[dict, pd.DataFrame | None]:
    if variant not in VARIANTS:
        raise ValueError(variant)
    if priority not in PRIORITIES:
        raise ValueError(priority)

    capital0 = 100.0
    asset = start_asset
    qty = capital0 / float(panel.iloc[start_i][f"{asset}_open"])

    watch: Watch | None = None
    pending: dict | None = None

    transitions = 0
    core_moves = 0
    v2_moves = 0
    v2_base_triggers = 0
    v2_confirmations = 0
    v2_cancelled_by_core = 0
    v2_failed_or_expired = 0
    same_close_conflicts = 0
    confirmed_v2_blocked_by_core_priority = 0
    cost_paid = 0.0

    equity = [capital0]
    trace = []

    for idx in range(start_i, end_i + 1):
        ts = pd.Timestamp(panel.iloc[idx]["timestamp"])

        if pending is not None:
            old_asset = asset
            asset, qty, fee, before, after = transition(
                panel, idx, asset, qty, pending["target"], cost_rate
            )
            if asset != old_asset:
                transitions += 1
                cost_paid += fee
                if pending["reason"] == "CORE":
                    core_moves += 1
                    if variant == "CORE_PLUS_V2":
                        watch = Watch(entry_i=idx)
                elif pending["reason"] == "V2":
                    v2_moves += 1
                    watch = None

                if collect_trace:
                    trace.append(
                        {
                            "date": ts,
                            "event": f"EXECUTE_{pending['reason']}",
                            "from_asset": old_asset,
                            "to_asset": asset,
                            "value_before": before,
                            "fee": fee,
                            "value_after": after,
                            "trigger_impulse": pending.get("trigger_impulse"),
                            "confirm2_impulse": pending.get("confirm2_impulse"),
                        }
                    )
            pending = None

        close_value = qty * float(panel.iloc[idx][f"{asset}_close"])
        equity.append(float(close_value))

        if idx == end_i:
            continue

        v2_action = None
        if variant == "CORE_PLUS_V2":
            prior_watch = watch
            watch, v2_action, diag = evaluate_watch_close(
                panel, idx, asset, watch
            )
            if diag["base_trigger"]:
                v2_base_triggers += 1
                if collect_trace and watch is not None:
                    trace.append(
                        {
                            "date": ts,
                            "event": "V2_BASE_TRIGGER",
                            "from_asset": asset,
                            "to_asset": watch.candidate,
                            "value_before": close_value,
                            "fee": 0.0,
                            "value_after": close_value,
                            "trigger_impulse": watch.trigger_impulse,
                            "confirm2_impulse": np.nan,
                        }
                    )
            if diag["confirm1"] and collect_trace and watch is not None:
                trace.append(
                    {
                        "date": ts,
                        "event": "V2_CONFIRM1",
                        "from_asset": asset,
                        "to_asset": watch.candidate,
                        "value_before": close_value,
                        "fee": 0.0,
                        "value_after": close_value,
                        "trigger_impulse": watch.trigger_impulse,
                        "confirm2_impulse": candidate_impulse(
                            panel, str(watch.candidate), asset, watch.entry_i, idx
                        ),
                    }
                )
            if diag["confirm2"]:
                v2_confirmations += 1
                if collect_trace and v2_action is not None:
                    trace.append(
                        {
                            "date": ts,
                            "event": "V2_CONFIRM2_PASS",
                            "from_asset": asset,
                            "to_asset": v2_action["target"],
                            "value_before": close_value,
                            "fee": 0.0,
                            "value_after": close_value,
                            "trigger_impulse": v2_action["trigger_impulse"],
                            "confirm2_impulse": v2_action["confirm2_impulse"],
                        }
                    )
            if diag["watch_failed"] or diag["watch_expired"]:
                v2_failed_or_expired += 1
                if collect_trace and prior_watch is not None:
                    trace.append(
                        {
                            "date": ts,
                            "event": "V2_WATCH_END_NO_PASS",
                            "from_asset": asset,
                            "to_asset": (
                                prior_watch.candidate
                                if prior_watch.candidate is not None
                                else ""
                            ),
                            "value_before": close_value,
                            "fee": 0.0,
                            "value_after": close_value,
                            "trigger_impulse": prior_watch.trigger_impulse,
                            "confirm2_impulse": np.nan,
                        }
                    )

        route = route_map.get((ts, asset))
        core_action = None
        if route is not None:
            target = str(route["effective_to"])
            if target != asset:
                core_action = {
                    "target": target,
                    "reason": "CORE",
                    "ddg_override": bool(route.get("override", False)),
                }

        if v2_action is not None and core_action is not None:
            same_close_conflicts += 1
            if priority == "V2_PRIORITY":
                pending = v2_action
            else:
                pending = core_action
                confirmed_v2_blocked_by_core_priority += 1
        elif v2_action is not None:
            pending = v2_action
        elif core_action is not None:
            if variant == "CORE_PLUS_V2" and watch is not None:
                v2_cancelled_by_core += 1
                if collect_trace:
                    trace.append(
                        {
                            "date": ts,
                            "event": "V2_CANCELLED_BY_CORE",
                            "from_asset": asset,
                            "to_asset": core_action["target"],
                            "value_before": close_value,
                            "fee": 0.0,
                            "value_after": close_value,
                            "trigger_impulse": watch.trigger_impulse,
                            "confirm2_impulse": np.nan,
                        }
                    )
                watch = None
            pending = core_action

        if collect_trace and core_action is not None:
            trace.append(
                {
                    "date": ts,
                    "event": "CORE_SIGNAL",
                    "from_asset": asset,
                    "to_asset": core_action["target"],
                    "value_before": close_value,
                    "fee": 0.0,
                    "value_after": close_value,
                    "trigger_impulse": np.nan,
                    "confirm2_impulse": np.nan,
                }
            )

    arr = np.asarray(equity, dtype=float)
    peaks = np.maximum.accumulate(arr)
    max_dd = float(np.min(arr / peaks - 1.0))
    final_capital = float(arr[-1])

    result = {
        "variant": variant,
        "priority": priority,
        "cost_rate": float(cost_rate),
        "start_asset": start_asset,
        "start_date": pd.Timestamp(panel.iloc[start_i]["timestamp"]),
        "end_date": pd.Timestamp(panel.iloc[end_i]["timestamp"]),
        "final_asset": asset,
        "final_capital": final_capital,
        "return": final_capital / capital0 - 1.0,
        "max_dd": max_dd,
        "transitions": int(transitions),
        "core_moves": int(core_moves),
        "v2_moves": int(v2_moves),
        "v2_base_triggers": int(v2_base_triggers),
        "v2_confirmations": int(v2_confirmations),
        "v2_cancelled_by_core": int(v2_cancelled_by_core),
        "v2_failed_or_expired": int(v2_failed_or_expired),
        "same_close_conflicts": int(same_close_conflicts),
        "confirmed_v2_blocked_by_core_priority": int(
            confirmed_v2_blocked_by_core_priority
        ),
        "modeled_cost_paid": float(cost_paid),
    }

    trace_df = pd.DataFrame(trace) if collect_trace else None
    return result, trace_df


def aggregate(frame: pd.DataFrame) -> dict:
    return {
        "start_count": int(len(frame)),
        "median_final_capital": float(frame["final_capital"].median()),
        "median_return": float(frame["return"].median()),
        "worst_return": float(frame["return"].min()),
        "best_return": float(frame["return"].max()),
        "positive_start_rate": float((frame["return"] > 0).mean()),
        "median_max_dd": float(frame["max_dd"].median()),
        "worst_max_dd": float(frame["max_dd"].min()),
        "median_transitions": float(frame["transitions"].median()),
        "median_core_moves": float(frame["core_moves"].median()),
        "median_v2_moves": float(frame["v2_moves"].median()),
        "median_modeled_cost_paid": float(frame["modeled_cost_paid"].median()),
        "median_v2_base_triggers": float(frame["v2_base_triggers"].median()),
        "median_v2_confirmations": float(frame["v2_confirmations"].median()),
        "median_v2_cancelled_by_core": float(
            frame["v2_cancelled_by_core"].median()
        ),
    }


def compare_pair(core: pd.DataFrame, overlay: pd.DataFrame) -> dict:
    merged = core.merge(
        overlay,
        on=["start_asset", "start_date", "end_date", "cost_rate"],
        suffixes=("_core", "_v2"),
        validate="one_to_one",
    )
    ratio = merged["final_capital_v2"] / merged["final_capital_core"]
    return {
        "n": int(len(merged)),
        "improved_count": int((ratio > 1.0).sum()),
        "improved_share": float((ratio > 1.0).mean()),
        "median_final_capital_ratio": float(ratio.median()),
        "median_return_delta": float(
            (merged["return_v2"] - merged["return_core"]).median()
        ),
        "median_max_dd_delta": float(
            (merged["max_dd_v2"] - merged["max_dd_core"]).median()
        ),
        "median_transition_delta": float(
            (merged["transitions_v2"] - merged["transitions_core"]).median()
        ),
        "median_cost_delta": float(
            (
                merged["modeled_cost_paid_v2"]
                - merged["modeled_cost_paid_core"]
            ).median()
        ),
    }


def primary_window_runs(
    panel: pd.DataFrame,
    route_map: dict,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    end_i = len(panel) - 1
    rows = []

    for window, start_ts in WINDOW_STARTS.items():
        start_i = first_index_on_or_after(panel["timestamp"], start_ts)
        for cost in COSTS:
            for asset in START_ASSETS:
                core, _ = simulate(
                    panel, route_map, start_i, end_i, asset,
                    "CORE", cost, "V2_PRIORITY"
                )
                core["window"] = window
                rows.append(core)

                for priority in PRIORITIES:
                    overlay, _ = simulate(
                        panel, route_map, start_i, end_i, asset,
                        "CORE_PLUS_V2", cost, priority
                    )
                    overlay["window"] = window
                    rows.append(overlay)

    detail = pd.DataFrame(rows)

    summary_rows = []
    comparison_rows = []
    for window in WINDOW_STARTS:
        for cost in COSTS:
            core = detail[
                (detail["window"] == window)
                & (detail["cost_rate"] == cost)
                & (detail["variant"] == "CORE")
            ].copy()
            summary_rows.append(
                {
                    "window": window,
                    "cost_rate": cost,
                    "variant": "CORE",
                    "priority": "NA",
                    **aggregate(core),
                }
            )

            for priority in PRIORITIES:
                overlay = detail[
                    (detail["window"] == window)
                    & (detail["cost_rate"] == cost)
                    & (detail["variant"] == "CORE_PLUS_V2")
                    & (detail["priority"] == priority)
                ].copy()
                summary_rows.append(
                    {
                        "window": window,
                        "cost_rate": cost,
                        "variant": "CORE_PLUS_V2",
                        "priority": priority,
                        **aggregate(overlay),
                    }
                )
                comparison_rows.append(
                    {
                        "window": window,
                        "cost_rate": cost,
                        "priority": priority,
                        **compare_pair(core, overlay),
                    }
                )

    return detail, pd.DataFrame(summary_rows), pd.DataFrame(comparison_rows)


def rolling_365(
    panel: pd.DataFrame,
    route_map: dict,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = pd.to_datetime(panel["timestamp"], utc=True)
    first = max(
        pd.Timestamp("2023-11-01T00:00:00Z"),
        pd.Timestamp(dates.iloc[0]),
    )
    last = pd.Timestamp(dates.iloc[-1])

    starts = pd.date_range(first, last, freq="MS", tz="UTC")
    rows = []

    for start in starts:
        end = start + pd.Timedelta(days=364)
        if end > last:
            continue
        start_i = first_index_on_or_after(panel["timestamp"], start)
        end_i = first_index_on_or_after(panel["timestamp"], end)
        if pd.Timestamp(panel.iloc[end_i]["timestamp"]) > end:
            end_i -= 1
        if end_i <= start_i:
            continue

        for asset in TARGET_START_ASSETS:
            core, _ = simulate(
                panel, route_map, start_i, end_i, asset,
                "CORE", PRIMARY_COST, "V2_PRIORITY"
            )
            overlay, _ = simulate(
                panel, route_map, start_i, end_i, asset,
                "CORE_PLUS_V2", PRIMARY_COST, "V2_PRIORITY"
            )
            rows.append(
                {
                    "window_start": pd.Timestamp(panel.iloc[start_i]["timestamp"]),
                    "window_end": pd.Timestamp(panel.iloc[end_i]["timestamp"]),
                    "start_asset": asset,
                    "core_return": core["return"],
                    "v2_return": overlay["return"],
                    "return_delta": overlay["return"] - core["return"],
                    "core_max_dd": core["max_dd"],
                    "v2_max_dd": overlay["max_dd"],
                    "max_dd_delta": overlay["max_dd"] - core["max_dd"],
                    "core_transitions": core["transitions"],
                    "v2_transitions": overlay["transitions"],
                }
            )

    detail = pd.DataFrame(rows)
    summary_rows = []
    for start, sub in detail.groupby("window_start", sort=True):
        summary_rows.append(
            {
                "window_start": start,
                "window_end": sub["window_end"].iloc[0],
                "start_count": int(len(sub)),
                "core_median_return": float(sub["core_return"].median()),
                "v2_median_return": float(sub["v2_return"].median()),
                "median_return_delta": float(sub["return_delta"].median()),
                "v2_better_share": float((sub["return_delta"] > 0).mean()),
                "core_median_max_dd": float(sub["core_max_dd"].median()),
                "v2_median_max_dd": float(sub["v2_max_dd"].median()),
                "median_max_dd_delta": float(sub["max_dd_delta"].median()),
                "median_transition_delta": float(
                    (sub["v2_transitions"] - sub["core_transitions"]).median()
                ),
            }
        )
    return detail, pd.DataFrame(summary_rows)


def classify(
    comparisons: pd.DataFrame,
    rolling_summary: pd.DataFrame,
) -> tuple[str, dict]:
    mature_primary = comparisons[
        (comparisons["window"] == "MATURE")
        & np.isclose(comparisons["cost_rate"], 0.001)
        & (comparisons["priority"] == "V2_PRIORITY")
    ].iloc[0]
    mature_05 = comparisons[
        (comparisons["window"] == "MATURE")
        & np.isclose(comparisons["cost_rate"], 0.005)
        & (comparisons["priority"] == "V2_PRIORITY")
    ].iloc[0]

    rolling_better_rate = float(
        (rolling_summary["median_return_delta"] > 0).mean()
    ) if len(rolling_summary) else np.nan

    gates = {
        "mature_median_return_delta_positive": bool(
            mature_primary["median_return_delta"] > 0
        ),
        "mature_improved_share_ge_50pct": bool(
            mature_primary["improved_share"] >= 0.50
        ),
        "mature_median_dd_delta_ge_minus_10pp": bool(
            mature_primary["median_max_dd_delta"] >= -0.10
        ),
        "rolling_better_rate_ge_40pct": bool(
            pd.notna(rolling_better_rate) and rolling_better_rate >= 0.40
        ),
        "mature_0p5pct_median_return_delta_positive": bool(
            mature_05["median_return_delta"] > 0
        ),
        "rolling_better_rate": rolling_better_rate,
    }

    if all(
        gates[k]
        for k in (
            "mature_median_return_delta_positive",
            "mature_improved_share_ge_50pct",
            "mature_median_dd_delta_ge_minus_10pp",
            "rolling_better_rate_ge_40pct",
            "mature_0p5pct_median_return_delta_positive",
        )
    ):
        label = "FULL_PATH_PROMISING_NOT_PRODUCTION_READY"
    elif (
        not gates["mature_median_return_delta_positive"]
        and not gates["mature_improved_share_ge_50pct"]
    ):
        label = "FULL_PATH_REJECTED"
    else:
        label = "FULL_PATH_MIXED"

    return label, gates


def fmt_pct(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{100*float(v):+.2f}%"


def fmt_rate(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{100*float(v):.1f}%"


def build_case_trace(
    panel: pd.DataFrame,
    route_map: dict,
) -> tuple[pd.DataFrame, dict]:
    start_i = first_index_on_or_after(
        panel["timestamp"], pd.Timestamp("2026-09-28T00:00:00Z")
    )
    end_i = len(panel) - 1
    result, trace = simulate(
        panel,
        route_map,
        start_i,
        end_i,
        "LINK",
        "CORE_PLUS_V2",
        PRIMARY_COST,
        "V2_PRIORITY",
        collect_trace=True,
    )
    assert trace is not None
    return trace, result


def main() -> None:
    panel, data_meta = base.download_panel()
    events_by_date, states_by_date = base.build_monitor_history(panel)
    routes = base.build_effective_routes(panel, events_by_date, states_by_date)
    route_map = build_route_map(routes)

    primary_detail, primary_summary, comparisons = primary_window_runs(
        panel, route_map
    )
    rolling_detail, rolling_summary = rolling_365(panel, route_map)
    classification, gates = classify(comparisons, rolling_summary)
    case_trace, case_result = build_case_trace(panel, route_map)

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    primary_detail.to_csv(run_dir / "primary_path_results.csv", index=False)
    primary_summary.to_csv(run_dir / "primary_summary.csv", index=False)
    comparisons.to_csv(run_dir / "core_vs_v2_comparison.csv", index=False)
    rolling_detail.to_csv(run_dir / "rolling_365_detail.csv", index=False)
    rolling_summary.to_csv(run_dir / "rolling_365_summary.csv", index=False)
    case_trace.to_csv(run_dir / "link_aave_case_trace.csv", index=False)
    routes.to_csv(run_dir / "effective_route_ledger.csv", index=False)

    summary_json = {
        "experiment": "RR_V2_FULL_PATH_COUNTERFACTUAL_V1",
        "workflow_mode": "STRESS_TEST_ONLY",
        "source_commit_sha": source_sha(),
        "as_of": AS_OF.isoformat(),
        "latest_closed_candle": pd.Timestamp(panel.iloc[-1]["timestamp"]).isoformat(),
        "classification": classification,
        "classification_gates": gates,
        "costs": list(COSTS),
        "start_assets": list(START_ASSETS),
        "window_starts": {k: v.isoformat() for k, v in WINDOW_STARTS.items()},
        "primary_comparisons": json.loads(
            comparisons.to_json(orient="records", date_format="iso")
        ),
        "rolling_window_count": int(len(rolling_summary)),
        "rolling_better_window_rate": (
            None if not len(rolling_summary)
            else float((rolling_summary["median_return_delta"] > 0).mean())
        ),
        "case_result": case_result,
        "production_changes": "NONE",
        "test_level": "GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_PATH_DEPENDENT_STRESS_TEST",
        "data_metadata": data_meta,
    }
    (run_dir / "summary.json").write_text(
        json.dumps(summary_json, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )

    primary_cmp = comparisons[
        (comparisons["priority"] == "V2_PRIORITY")
    ].copy()

    lines = [
        "# RR V2 FULL-PATH COUNTERFACTUAL V1 — RESULTS",
        "",
        "Mode: STRESS_TEST_ONLY",
        "",
        f"Classification: {classification}",
        f"As-of: {AS_OF.isoformat()}",
        f"Latest closed D1: {pd.Timestamp(panel.iloc[-1]['timestamp']).isoformat()}",
        "",
        "## CORE vs CORE_PLUS_V2 — V2 priority",
        "",
        "|Window|Cost|Starts improved|Median final-capital ratio|Median return delta|Median DD delta|Median extra transitions|Median extra modeled cost|",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in primary_cmp.iterrows():
        lines.append(
            f"|{row['window']}|{100*row['cost_rate']:.2f}%|"
            f"{int(row['improved_count'])}/{int(row['n'])} ({fmt_rate(row['improved_share'])})|"
            f"{row['median_final_capital_ratio']:.4f}x|"
            f"{fmt_pct(row['median_return_delta'])}|"
            f"{fmt_pct(row['median_max_dd_delta'])}|"
            f"{row['median_transition_delta']:+.1f}|"
            f"{row['median_cost_delta']:+.4f}|"
        )

    lines += [
        "",
        "## Primary window absolute summaries at 0.10% cost",
        "",
        "|Window|Variant|Priority|Median return|Worst return|Median DD|Worst DD|Median transitions|Median V2 moves|",
        "|---|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    primary_abs = primary_summary[
        np.isclose(primary_summary["cost_rate"], PRIMARY_COST)
    ].copy()
    for _, row in primary_abs.iterrows():
        lines.append(
            f"|{row['window']}|{row['variant']}|{row['priority']}|"
            f"{fmt_pct(row['median_return'])}|{fmt_pct(row['worst_return'])}|"
            f"{fmt_pct(row['median_max_dd'])}|{fmt_pct(row['worst_max_dd'])}|"
            f"{row['median_transitions']:.1f}|{row['median_v2_moves']:.1f}|"
        )

    lines += [
        "",
        "## Rolling 365d robustness",
        "",
        f"- windows: {len(rolling_summary)}",
        f"- windows with higher V2 median return: {fmt_rate(gates['rolling_better_rate'])}",
        f"- median rolling return delta: {fmt_pct(rolling_summary['median_return_delta'].median() if len(rolling_summary) else np.nan)}",
        f"- worst rolling return delta: {fmt_pct(rolling_summary['median_return_delta'].min() if len(rolling_summary) else np.nan)}",
        f"- median rolling DD delta: {fmt_pct(rolling_summary['median_max_dd_delta'].median() if len(rolling_summary) else np.nan)}",
        "",
        "## Frozen classification gates",
        "",
    ]
    for key, value in gates.items():
        if key == "rolling_better_rate":
            continue
        lines.append(f"- {key}: {value}")

    lines += [
        "",
        "## LINK -> TRX -> AAVE case trace",
        "",
        f"- final asset: {case_result['final_asset']}",
        f"- final capital: {case_result['final_capital']:.4f}",
        f"- return: {fmt_pct(case_result['return'])}",
        f"- transitions: {case_result['transitions']}",
        f"- CORE moves: {case_result['core_moves']}",
        f"- V2 moves: {case_result['v2_moves']}",
        "",
        "|Date|Event|From|To|Fee|Trigger impulse|Confirm2 impulse|",
        "|---|---|---|---|---:|---:|---:|",
    ]
    for _, row in case_trace.iterrows():
        lines.append(
            f"|{pd.Timestamp(row['date']).date()}|{row['event']}|"
            f"{row['from_asset']}|{row['to_asset']}|"
            f"{float(row['fee']):.5f}|"
            f"{fmt_pct(row['trigger_impulse'])}|"
            f"{fmt_pct(row['confirm2_impulse'])}|"
        )

    lines += [
        "",
        "## Boundary",
        "",
        "- V2 rule was not retuned.",
        "- No production/live/Telegram/exchange behavior changed.",
        "- This is a full path-dependent historical counterfactual, not forward proof.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_PATH_DEPENDENT_STRESS_TEST",
        "",
    ]

    report = "\n".join(lines)
    (run_dir / "report.md").write_text(report, encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()
