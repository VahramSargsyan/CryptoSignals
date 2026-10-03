from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd

import scripts.research_rr_ddg_acceleration_fork_v1 as rrbase
from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import TARGET_ASSETS

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "rr_v2_full_path_counterfactual_v1"

AS_OF = pd.Timestamp("2026-10-03T16:30:00Z")
MATURE_START = pd.Timestamp("2023-10-31T00:00:00Z")
LAST_2Y_START = pd.Timestamp("2024-10-03T00:00:00Z")
LAST_1Y_START = pd.Timestamp("2025-10-03T00:00:00Z")
CASE_START = pd.Timestamp("2026-09-28T00:00:00Z")

TARGETS = tuple(TARGET_ASSETS)
START_ASSETS = tuple(rrbase.ASSETS)
COSTS = (0.001, 0.005, 0.01, 0.03)
PRIMARY_COST = 0.001
ROLL_DAYS = 365
ACCEL_THRESHOLD = 0.10
OBS_DAYS = (1, 2, 3)
TOL = 1e-12

PRIMARY_PRIORITY = "V2_PRIORITY"
ROBUSTNESS_PRIORITY = "CORE_PRIORITY"


def source_sha() -> str:
    return os.environ.get("SOURCE_COMMIT_SHA") or subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def make_route_map(routes: pd.DataFrame) -> dict[tuple[pd.Timestamp, str], dict]:
    out: dict[tuple[pd.Timestamp, str], dict] = {}
    for _, row in routes.iterrows():
        key = (pd.Timestamp(row["signal_date"]), str(row["source"]).upper())
        out[key] = row.to_dict()
    return out


def build_panel_maps(panel: pd.DataFrame):
    timestamps = pd.to_datetime(panel["timestamp"], utc=True)
    date_to_i = {pd.Timestamp(ts): i for i, ts in enumerate(timestamps)}
    opens = {
        asset: panel[f"{asset}_open"].to_numpy(float)
        for asset in START_ASSETS
    }
    closes = {
        asset: panel[f"{asset}_close"].to_numpy(float)
        for asset in START_ASSETS
    }
    return timestamps, date_to_i, opens, closes


def strongest_accel(
    closes: dict[str, np.ndarray],
    opens: dict[str, np.ndarray],
    held: str,
    entry_i: int,
    obs_i: int,
) -> tuple[str, float, list[tuple[str, float]]]:
    held_factor = closes[held][obs_i] / opens[held][entry_i]
    scores = []
    for asset in TARGETS:
        if asset == held:
            continue
        asset_factor = closes[asset][obs_i] / opens[asset][entry_i]
        impulse = asset_factor / held_factor - 1.0
        scores.append((asset, float(impulse)))
    scores.sort(key=lambda x: (-x[1], x[0]))
    return scores[0][0], float(scores[0][1]), scores


def max_drawdown(equity: list[float]) -> float:
    arr = np.asarray(equity, dtype=float)
    if len(arr) == 0:
        return np.nan
    peaks = np.maximum.accumulate(arr)
    dd = arr / peaks - 1.0
    return float(np.min(dd))


def resolve_bounds(
    timestamps: pd.Series,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> tuple[int, int]:
    vals = pd.DatetimeIndex(timestamps)
    start_pos = int(vals.searchsorted(start, side="left"))
    end_pos = int(vals.searchsorted(end, side="right") - 1)
    if start_pos < 0 or end_pos >= len(vals) or start_pos > end_pos:
        raise RuntimeError(f"Invalid bounds: {start} -> {end}")
    return start_pos, end_pos


def simulate_path(
    panel: pd.DataFrame,
    route_map: dict[tuple[pd.Timestamp, str], dict],
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    start_asset: str,
    cost: float,
    overlay_enabled: bool,
    priority: str = PRIMARY_PRIORITY,
    collect_daily: bool = False,
) -> dict:
    timestamps, _, opens, closes = build_panel_maps(panel)
    start_i, end_i = resolve_bounds(timestamps, start_date, end_date)

    start_asset = start_asset.upper()
    current = start_asset
    qty = 100.0 / float(opens[current][start_i])
    pending: dict | None = None
    watch: dict | None = None

    equity_rows = []
    transitions = []
    overlay_actions = []
    core_moves = 0
    overlay_moves = 0
    v2_base_triggers = 0
    v2_confirmations = 0
    v2_core_cancellations = 0
    total_cost_paid = 0.0

    for i in range(start_i, end_i + 1):
        date = pd.Timestamp(timestamps.iloc[i])

        if pending is not None:
            if pending["from_asset"] != current:
                raise RuntimeError(
                    f"Pending source mismatch on {date}: "
                    f"{pending['from_asset']} != {current}"
                )
            destination = str(pending["to_asset"]).upper()
            value_before = qty * float(opens[current][i])
            cost_paid = value_before * cost
            value_after = value_before - cost_paid
            qty = value_after / float(opens[destination][i])

            transition = {
                "execute_date": date,
                "signal_date": pending["signal_date"],
                "type": pending["type"],
                "from_asset": current,
                "to_asset": destination,
                "capital_before": float(value_before),
                "cost_rate": float(cost),
                "cost_paid": float(cost_paid),
                "capital_after": float(value_after),
            }
            for key in (
                "trigger_date",
                "trigger_impulse",
                "confirm1_date",
                "confirm2_date",
                "confirm2_impulse",
            ):
                if key in pending:
                    transition[key] = pending[key]
            transitions.append(transition)
            total_cost_paid += cost_paid

            current = destination
            if pending["type"] == "CORE":
                core_moves += 1
                watch = (
                    {
                        "held": current,
                        "entry_i": i,
                        "stage": "SEARCH",
                        "trigger_candidate": None,
                        "trigger_i": None,
                        "trigger_date": None,
                        "trigger_impulse": None,
                        "confirm1_date": None,
                    }
                    if overlay_enabled
                    else None
                )
            else:
                overlay_moves += 1
                overlay_actions.append(dict(transition))
                watch = None

            pending = None

        equity_close = qty * float(closes[current][i])
        if collect_daily:
            equity_rows.append(
                {
                    "date": date,
                    "asset": current,
                    "equity_close": float(equity_close),
                }
            )
        else:
            equity_rows.append(float(equity_close))

        if i >= end_i:
            continue

        core_route = route_map.get((date, current))
        core_pending = None
        if core_route is not None:
            destination = str(core_route["effective_to"]).upper()
            if destination != current:
                core_pending = {
                    "type": "CORE",
                    "signal_date": date,
                    "from_asset": current,
                    "to_asset": destination,
                    "baseline_to": str(core_route["baseline_to"]).upper(),
                    "ddg_override": bool(core_route["override"]),
                }

        overlay_pending = None

        if overlay_enabled and watch is not None and watch["held"] == current:
            stage = watch["stage"]
            if stage == "SEARCH":
                day = i - int(watch["entry_i"]) + 1
                if day in OBS_DAYS:
                    candidate, impulse, _ = strongest_accel(
                        closes, opens, current, int(watch["entry_i"]), i
                    )
                    if impulse >= ACCEL_THRESHOLD:
                        v2_base_triggers += 1
                        watch["stage"] = "CONFIRM1"
                        watch["trigger_candidate"] = candidate
                        watch["trigger_i"] = i
                        watch["trigger_date"] = date
                        watch["trigger_impulse"] = float(impulse)
                elif day > max(OBS_DAYS):
                    watch = None

            elif stage == "CONFIRM1":
                expected_i = int(watch["trigger_i"]) + 1
                if i == expected_i:
                    top, _, scores = strongest_accel(
                        closes, opens, current, int(watch["entry_i"]), i
                    )
                    candidate = str(watch["trigger_candidate"])
                    impulse_map = dict(scores)
                    candidate_impulse = float(impulse_map[candidate])
                    if top == candidate:
                        watch["stage"] = "CONFIRM2"
                        watch["confirm1_date"] = date
                        watch["confirm1_impulse"] = candidate_impulse
                    else:
                        watch = None

            elif stage == "CONFIRM2":
                expected_i = int(watch["trigger_i"]) + 2
                if i == expected_i:
                    top, _, scores = strongest_accel(
                        closes, opens, current, int(watch["entry_i"]), i
                    )
                    candidate = str(watch["trigger_candidate"])
                    impulse_map = dict(scores)
                    candidate_impulse = float(impulse_map[candidate])
                    passed = (
                        top == candidate
                        and candidate_impulse > float(watch["trigger_impulse"])
                    )
                    if passed:
                        v2_confirmations += 1
                        overlay_pending = {
                            "type": "V2",
                            "signal_date": date,
                            "from_asset": current,
                            "to_asset": candidate,
                            "trigger_date": watch["trigger_date"],
                            "trigger_impulse": float(watch["trigger_impulse"]),
                            "confirm1_date": watch.get("confirm1_date"),
                            "confirm2_date": date,
                            "confirm2_impulse": candidate_impulse,
                        }
                    watch = None

        if overlay_pending is not None and core_pending is not None:
            pending = (
                overlay_pending
                if priority == PRIMARY_PRIORITY
                else core_pending
            )
        elif overlay_pending is not None:
            pending = overlay_pending
        elif core_pending is not None:
            if overlay_enabled and watch is not None:
                v2_core_cancellations += 1
                watch = None
            pending = core_pending

    final_capital = qty * float(closes[current][end_i])
    equity_values = (
        [float(row["equity_close"]) for row in equity_rows]
        if collect_daily
        else [float(x) for x in equity_rows]
    )

    for action in overlay_actions:
        later_core = next(
            (
                t for t in transitions
                if t["type"] == "CORE"
                and pd.Timestamp(t["execute_date"])
                > pd.Timestamp(action["execute_date"])
            ),
            None,
        )
        action["next_core_execute_date"] = (
            None if later_core is None else later_core["execute_date"]
        )
        action["next_core_from"] = (
            None if later_core is None else later_core["from_asset"]
        )
        action["next_core_to"] = (
            None if later_core is None else later_core["to_asset"]
        )

    return {
        "start_date": pd.Timestamp(timestamps.iloc[start_i]),
        "end_date": pd.Timestamp(timestamps.iloc[end_i]),
        "start_asset": start_asset,
        "variant": "CORE_PLUS_V2" if overlay_enabled else "CORE",
        "priority": priority if overlay_enabled else "CORE_ONLY",
        "cost": float(cost),
        "initial_capital": 100.0,
        "final_capital": float(final_capital),
        "total_return": float(final_capital / 100.0 - 1.0),
        "max_drawdown": max_drawdown(equity_values),
        "transition_count": int(len(transitions)),
        "core_moves": int(core_moves),
        "overlay_moves": int(overlay_moves),
        "v2_base_triggers": int(v2_base_triggers),
        "v2_confirmations": int(v2_confirmations),
        "v2_core_cancellations": int(v2_core_cancellations),
        "total_cost_paid": float(total_cost_paid),
        "final_asset": current,
        "transitions": transitions,
        "overlay_actions": overlay_actions,
        "daily": equity_rows if collect_daily else None,
    }


def path_row(result: dict, window: str) -> dict:
    return {
        "window": window,
        "start_date": result["start_date"],
        "end_date": result["end_date"],
        "start_asset": result["start_asset"],
        "variant": result["variant"],
        "priority": result["priority"],
        "cost": result["cost"],
        "final_capital": result["final_capital"],
        "total_return": result["total_return"],
        "max_drawdown": result["max_drawdown"],
        "transition_count": result["transition_count"],
        "core_moves": result["core_moves"],
        "overlay_moves": result["overlay_moves"],
        "v2_base_triggers": result["v2_base_triggers"],
        "v2_confirmations": result["v2_confirmations"],
        "v2_core_cancellations": result["v2_core_cancellations"],
        "total_cost_paid": result["total_cost_paid"],
        "final_asset": result["final_asset"],
    }


def aggregate_window(paths: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for keys, sub in paths.groupby(
        ["window", "variant", "priority", "cost"], sort=True
    ):
        window, variant, priority, cost = keys
        rows.append(
            {
                "window": window,
                "variant": variant,
                "priority": priority,
                "cost": float(cost),
                "n_starts": int(len(sub)),
                "median_final_capital": float(sub["final_capital"].median()),
                "median_total_return": float(sub["total_return"].median()),
                "worst_total_return": float(sub["total_return"].min()),
                "best_total_return": float(sub["total_return"].max()),
                "positive_start_rate": float((sub["total_return"] > 0).mean()),
                "median_max_drawdown": float(sub["max_drawdown"].median()),
                "worst_max_drawdown": float(sub["max_drawdown"].min()),
                "median_transition_count": float(sub["transition_count"].median()),
                "median_core_moves": float(sub["core_moves"].median()),
                "median_overlay_moves": float(sub["overlay_moves"].median()),
                "median_total_cost_paid": float(sub["total_cost_paid"].median()),
            }
        )
    return pd.DataFrame(rows)


def paired_start_comparison(paths: pd.DataFrame) -> pd.DataFrame:
    core = paths[
        (paths["variant"] == "CORE")
        & (paths["priority"] == "CORE_ONLY")
    ].copy()
    overlay = paths[
        (paths["variant"] == "CORE_PLUS_V2")
        & (paths["priority"] == PRIMARY_PRIORITY)
    ].copy()
    key = ["window", "start_asset", "cost"]
    merged = core.merge(
        overlay,
        on=key,
        suffixes=("_core", "_overlay"),
        validate="one_to_one",
    )
    merged["final_capital_ratio"] = (
        merged["final_capital_overlay"] / merged["final_capital_core"]
    )
    merged["return_delta"] = (
        merged["total_return_overlay"] - merged["total_return_core"]
    )
    merged["max_drawdown_delta"] = (
        merged["max_drawdown_overlay"] - merged["max_drawdown_core"]
    )
    merged["transition_delta"] = (
        merged["transition_count_overlay"] - merged["transition_count_core"]
    )
    merged["cost_paid_delta"] = (
        merged["total_cost_paid_overlay"] - merged["total_cost_paid_core"]
    )
    return merged


def download_btc_regime() -> pd.DataFrame:
    client = BinanceSpotRestClient()
    result = download_historical_dataset(
        client,
        symbol="BTCUSDT",
        start=pd.Timestamp("2022-12-01T00:00:00Z"),
        end=AS_OF,
        timeframe="1D",
        as_of=AS_OF,
    )
    if result.dataset is None:
        raise RuntimeError("BTCUSDT D1 unavailable for regime diagnostic")
    frame = result.dataset.candles[["timestamp", "close"]].copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame["sma200"] = frame["close"].rolling(200, min_periods=200).mean()
    frame["regime"] = np.where(
        frame["sma200"].isna(),
        "UNKNOWN",
        np.where(
            frame["close"] >= frame["sma200"],
            "BTC_BULL_START",
            "BTC_BEAR_START",
        ),
    )
    return frame


def rolling_365(
    panel: pd.DataFrame,
    route_map: dict[tuple[pd.Timestamp, str], dict],
    btc: pd.DataFrame,
) -> pd.DataFrame:
    timestamps = pd.DatetimeIndex(pd.to_datetime(panel["timestamp"], utc=True))
    last = pd.Timestamp(timestamps[-1])
    btc_map = {
        pd.Timestamp(row["timestamp"]): str(row["regime"])
        for _, row in btc.iterrows()
    }
    rows = []

    for start_date in timestamps:
        end_target = pd.Timestamp(start_date) + pd.Timedelta(days=ROLL_DAYS)
        if end_target > last:
            break
        end_pos = int(timestamps.searchsorted(end_target, side="right") - 1)
        end_date = pd.Timestamp(timestamps[end_pos])
        if end_date <= start_date:
            continue

        core_results = []
        overlay_results = []
        for start_asset in TARGETS:
            core_results.append(
                simulate_path(
                    panel, route_map, pd.Timestamp(start_date), end_date,
                    start_asset, PRIMARY_COST, False
                )
            )
            overlay_results.append(
                simulate_path(
                    panel, route_map, pd.Timestamp(start_date), end_date,
                    start_asset, PRIMARY_COST, True, PRIMARY_PRIORITY
                )
            )

        core_ret = np.median([x["total_return"] for x in core_results])
        overlay_ret = np.median([x["total_return"] for x in overlay_results])
        core_dd = np.median([x["max_drawdown"] for x in core_results])
        overlay_dd = np.median([x["max_drawdown"] for x in overlay_results])
        core_tr = np.median([x["transition_count"] for x in core_results])
        overlay_tr = np.median([x["transition_count"] for x in overlay_results])
        delta = float(overlay_ret - core_ret)
        regime = btc_map.get(pd.Timestamp(start_date), "UNKNOWN")

        rows.append(
            {
                "start_date": pd.Timestamp(start_date),
                "end_date": end_date,
                "btc_start_regime": regime,
                "core_median_return": float(core_ret),
                "overlay_median_return": float(overlay_ret),
                "return_delta": delta,
                "core_median_max_drawdown": float(core_dd),
                "overlay_median_max_drawdown": float(overlay_dd),
                "max_drawdown_delta": float(overlay_dd - core_dd),
                "core_median_transitions": float(core_tr),
                "overlay_median_transitions": float(overlay_tr),
                "transition_delta": float(overlay_tr - core_tr),
                "comparison": (
                    "BETTER" if delta > TOL
                    else "WORSE" if delta < -TOL
                    else "EQUAL"
                ),
            }
        )
    return pd.DataFrame(rows)


def rolling_summary(rolling: pd.DataFrame) -> dict:
    if rolling.empty:
        return {}
    deltas = rolling["return_delta"].astype(float)
    return {
        "windows": int(len(rolling)),
        "better": int((rolling["comparison"] == "BETTER").sum()),
        "equal": int((rolling["comparison"] == "EQUAL").sum()),
        "worse": int((rolling["comparison"] == "WORSE").sum()),
        "median_return_delta": float(deltas.median()),
        "p25_return_delta": float(deltas.quantile(0.25)),
        "p75_return_delta": float(deltas.quantile(0.75)),
        "worst_return_delta": float(deltas.min()),
        "best_return_delta": float(deltas.max()),
        "median_max_drawdown_delta": float(
            rolling["max_drawdown_delta"].median()
        ),
        "median_transition_delta": float(
            rolling["transition_delta"].median()
        ),
    }


def regime_summary(rolling: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for regime, sub in rolling.groupby("btc_start_regime", sort=True):
        if regime == "UNKNOWN":
            continue
        rows.append(
            {
                "btc_start_regime": regime,
                "windows": int(len(sub)),
                "better": int((sub["comparison"] == "BETTER").sum()),
                "equal": int((sub["comparison"] == "EQUAL").sum()),
                "worse": int((sub["comparison"] == "WORSE").sum()),
                "median_return_delta": float(sub["return_delta"].median()),
                "median_max_drawdown_delta": float(
                    sub["max_drawdown_delta"].median()
                ),
            }
        )
    return pd.DataFrame(rows)


def classify(
    aggregate: pd.DataFrame,
    rolling_stats: dict,
) -> tuple[str, dict]:
    def row(window: str, variant: str, priority: str, cost: float) -> pd.Series:
        hit = aggregate[
            (aggregate["window"] == window)
            & (aggregate["variant"] == variant)
            & (aggregate["priority"] == priority)
            & (np.isclose(aggregate["cost"], cost))
        ]
        if hit.empty:
            raise RuntimeError(
                f"Missing aggregate row {window} {variant} {priority} {cost}"
            )
        return hit.iloc[0]

    core_primary = row("MATURE", "CORE", "CORE_ONLY", PRIMARY_COST)
    overlay_primary = row(
        "MATURE", "CORE_PLUS_V2", PRIMARY_PRIORITY, PRIMARY_COST
    )
    core_05 = row("MATURE", "CORE", "CORE_ONLY", 0.005)
    overlay_05 = row(
        "MATURE", "CORE_PLUS_V2", PRIMARY_PRIORITY, 0.005
    )

    mature_return_delta = (
        float(overlay_primary["median_total_return"])
        - float(core_primary["median_total_return"])
    )
    mature_dd_delta = (
        float(overlay_primary["median_max_drawdown"])
        - float(core_primary["median_max_drawdown"])
    )
    cost05_return_delta = (
        float(overlay_05["median_total_return"])
        - float(core_05["median_total_return"])
    )

    gates = {
        "mature_return_improves": mature_return_delta > 0,
        "mature_dd_not_worse_than_5pp": mature_dd_delta >= -0.05,
        "rolling_better_gt_worse": (
            rolling_stats.get("better", 0) > rolling_stats.get("worse", 0)
        ),
        "rolling_median_delta_positive": (
            rolling_stats.get("median_return_delta", np.nan) > 0
        ),
        "cost_0p5pct_delta_nonnegative": cost05_return_delta >= 0,
    }

    if all(gates.values()):
        label = "FULL_PATH_PROMISING_NOT_PRODUCTION_READY"
    elif gates["mature_return_improves"]:
        label = "FULL_PATH_MIXED"
    else:
        label = "FULL_PATH_REJECTED"

    details = {
        "mature_return_delta": mature_return_delta,
        "mature_max_drawdown_delta": mature_dd_delta,
        "cost_0p5pct_mature_return_delta": cost05_return_delta,
        "gates": gates,
    }
    return label, details


def fmt_pct(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{100.0 * float(v):+.2f}%"


def fmt_num(v, digits=2) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{float(v):.{digits}f}"


def main() -> None:
    panel, data_meta = rrbase.download_panel()
    events_by_date, states_by_date = rrbase.build_monitor_history(panel)
    routes = rrbase.build_effective_routes(panel, events_by_date, states_by_date)
    route_map = make_route_map(routes)
    btc = download_btc_regime()

    latest = pd.Timestamp(
        pd.to_datetime(panel["timestamp"], utc=True).iloc[-1]
    )

    windows = {
        "MATURE": MATURE_START,
        "LAST_2Y": LAST_2Y_START,
        "LAST_1Y": LAST_1Y_START,
    }

    path_rows = []
    all_overlay_actions = []
    mature_transition_rows = []

    for window_name, start_date in windows.items():
        for cost in COSTS:
            for start_asset in START_ASSETS:
                core = simulate_path(
                    panel, route_map, start_date, latest, start_asset,
                    cost, False
                )
                overlay = simulate_path(
                    panel, route_map, start_date, latest, start_asset,
                    cost, True, PRIMARY_PRIORITY
                )
                path_rows.append(path_row(core, window_name))
                path_rows.append(path_row(overlay, window_name))

                for action in overlay["overlay_actions"]:
                    all_overlay_actions.append(
                        {
                            "window": window_name,
                            "start_asset": start_asset,
                            "cost": cost,
                            **action,
                        }
                    )

                if window_name == "MATURE" and np.isclose(cost, PRIMARY_COST):
                    for t in core["transitions"]:
                        mature_transition_rows.append(
                            {
                                "variant": "CORE",
                                "start_asset": start_asset,
                                **t,
                            }
                        )
                    for t in overlay["transitions"]:
                        mature_transition_rows.append(
                            {
                                "variant": "CORE_PLUS_V2",
                                "start_asset": start_asset,
                                **t,
                            }
                        )

    core_priority_rows = []
    for window_name, start_date in windows.items():
        for start_asset in START_ASSETS:
            result = simulate_path(
                panel, route_map, start_date, latest, start_asset,
                PRIMARY_COST, True, ROBUSTNESS_PRIORITY
            )
            row = path_row(result, window_name)
            core_priority_rows.append(row)

    paths = pd.DataFrame(path_rows)
    core_priority = pd.DataFrame(core_priority_rows)
    aggregate = aggregate_window(paths)
    core_priority_aggregate = aggregate_window(core_priority)
    paired = paired_start_comparison(paths)

    rolling = rolling_365(panel, route_map, btc)
    rolling_stats = rolling_summary(rolling)
    regimes = regime_summary(rolling)

    classification, decision_details = classify(
        aggregate, rolling_stats
    )

    case_core = simulate_path(
        panel, route_map, CASE_START, latest, "LINK",
        PRIMARY_COST, False, collect_daily=True
    )
    case_overlay = simulate_path(
        panel, route_map, CASE_START, latest, "LINK",
        PRIMARY_COST, True, PRIMARY_PRIORITY, collect_daily=True
    )

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    paths.to_csv(run_dir / "full_path_start_results.csv", index=False)
    aggregate.to_csv(run_dir / "aggregate_window_cost_summary.csv", index=False)
    paired.to_csv(run_dir / "paired_start_comparison.csv", index=False)
    core_priority.to_csv(run_dir / "core_priority_start_results.csv", index=False)
    core_priority_aggregate.to_csv(
        run_dir / "core_priority_aggregate.csv", index=False
    )
    pd.DataFrame(all_overlay_actions).to_csv(
        run_dir / "overlay_action_ledger.csv", index=False
    )
    pd.DataFrame(mature_transition_rows).to_csv(
        run_dir / "mature_transition_ledger.csv", index=False
    )
    rolling.to_csv(run_dir / "rolling_365d.csv", index=False)
    regimes.to_csv(run_dir / "rolling_regime_summary.csv", index=False)
    pd.DataFrame(case_core["transitions"]).to_csv(
        run_dir / "case_link_core_transitions.csv", index=False
    )
    pd.DataFrame(case_overlay["transitions"]).to_csv(
        run_dir / "case_link_overlay_transitions.csv", index=False
    )
    pd.DataFrame(case_core["daily"]).to_csv(
        run_dir / "case_link_core_daily.csv", index=False
    )
    pd.DataFrame(case_overlay["daily"]).to_csv(
        run_dir / "case_link_overlay_daily.csv", index=False
    )

    def aggrow(window, variant, priority, cost):
        hit = aggregate[
            (aggregate["window"] == window)
            & (aggregate["variant"] == variant)
            & (aggregate["priority"] == priority)
            & (np.isclose(aggregate["cost"], cost))
        ]
        return hit.iloc[0].to_dict()

    primary_core = aggrow("MATURE", "CORE", "CORE_ONLY", PRIMARY_COST)
    primary_overlay = aggrow(
        "MATURE", "CORE_PLUS_V2", PRIMARY_PRIORITY, PRIMARY_COST
    )

    case_summary = {
        "core_final_capital": case_core["final_capital"],
        "overlay_final_capital": case_overlay["final_capital"],
        "overlay_vs_core_final_ratio": (
            case_overlay["final_capital"] / case_core["final_capital"]
        ),
        "core_final_asset": case_core["final_asset"],
        "overlay_final_asset": case_overlay["final_asset"],
        "core_transitions": case_core["transitions"],
        "overlay_transitions": case_overlay["transitions"],
    }

    summary = {
        "experiment": "RR_V2_FULL_PATH_COUNTERFACTUAL_V1",
        "workflow_mode": "STRESS_TEST_ONLY",
        "source_commit_sha": source_sha(),
        "as_of": AS_OF.isoformat(),
        "latest_closed_candle": latest.isoformat(),
        "classification": classification,
        "decision_details": decision_details,
        "primary_cost": PRIMARY_COST,
        "cost_scenarios": list(COSTS),
        "primary_mature_core": primary_core,
        "primary_mature_overlay": primary_overlay,
        "rolling_365d": rolling_stats,
        "regimes": json.loads(regimes.to_json(orient="records")),
        "case_link": case_summary,
        "production_changes": "NONE",
        "test_level": (
            "GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_PATH_DEPENDENT_STRESS_TEST"
        ),
        "data_metadata": data_meta,
    }
    (run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )

    lines = [
        "# RR V2 FULL-PATH COUNTERFACTUAL V1 — RESULTS",
        "",
        "Mode: STRESS_TEST_ONLY",
        "",
        f"Classification: {classification}",
        f"As-of: {AS_OF.isoformat()}",
        f"Latest closed D1: {latest.isoformat()}",
        "",
        "## Mature primary cost — 0.10%",
        "",
        "|Metric|CORE|CORE + V2|Delta|",
        "|---|---:|---:|---:|",
        f"|Median final capital|{fmt_num(primary_core['median_final_capital'])}|{fmt_num(primary_overlay['median_final_capital'])}|{fmt_num(primary_overlay['median_final_capital']-primary_core['median_final_capital'])}|",
        f"|Median total return|{fmt_pct(primary_core['median_total_return'])}|{fmt_pct(primary_overlay['median_total_return'])}|{fmt_pct(primary_overlay['median_total_return']-primary_core['median_total_return'])}|",
        f"|Median max drawdown|{fmt_pct(primary_core['median_max_drawdown'])}|{fmt_pct(primary_overlay['median_max_drawdown'])}|{fmt_pct(primary_overlay['median_max_drawdown']-primary_core['median_max_drawdown'])}|",
        f"|Median transitions|{fmt_num(primary_core['median_transition_count'],1)}|{fmt_num(primary_overlay['median_transition_count'],1)}|{fmt_num(primary_overlay['median_transition_count']-primary_core['median_transition_count'],1)}|",
        f"|Median modeled cost paid|{fmt_num(primary_core['median_total_cost_paid'])}|{fmt_num(primary_overlay['median_total_cost_paid'])}|{fmt_num(primary_overlay['median_total_cost_paid']-primary_core['median_total_cost_paid'])}|",
        "",
        "## Cost sensitivity — MATURE median return",
        "",
        "|Cost|CORE|CORE + V2|Delta|",
        "|---:|---:|---:|---:|",
    ]

    for cost in COSTS:
        c = aggrow("MATURE", "CORE", "CORE_ONLY", cost)
        o = aggrow("MATURE", "CORE_PLUS_V2", PRIMARY_PRIORITY, cost)
        lines.append(
            f"|{100*cost:.2f}%|{fmt_pct(c['median_total_return'])}|"
            f"{fmt_pct(o['median_total_return'])}|"
            f"{fmt_pct(o['median_total_return']-c['median_total_return'])}|"
        )

    lines += [
        "",
        "## Primary paired MATURE starts",
        "",
    ]
    p = paired[
        (paired["window"] == "MATURE")
        & (np.isclose(paired["cost"], PRIMARY_COST))
    ]
    lines += [
        f"- starts: {len(p)}",
        f"- overlay higher final capital: {int((p['final_capital_ratio'] > 1).sum())}",
        f"- overlay lower final capital: {int((p['final_capital_ratio'] < 1).sum())}",
        f"- median final-capital ratio: {p['final_capital_ratio'].median():.4f}x",
        f"- median return delta: {fmt_pct(p['return_delta'].median())}",
        f"- median max-DD delta: {fmt_pct(p['max_drawdown_delta'].median())}",
        f"- median transition delta: {p['transition_delta'].median():+.1f}",
        "",
        "## Rolling 365-day robustness",
        "",
        f"- windows: {rolling_stats.get('windows', 0)}",
        f"- better / equal / worse: {rolling_stats.get('better', 0)} / {rolling_stats.get('equal', 0)} / {rolling_stats.get('worse', 0)}",
        f"- median return delta: {fmt_pct(rolling_stats.get('median_return_delta'))}",
        f"- p25 / p75 delta: {fmt_pct(rolling_stats.get('p25_return_delta'))} / {fmt_pct(rolling_stats.get('p75_return_delta'))}",
        f"- worst / best delta: {fmt_pct(rolling_stats.get('worst_return_delta'))} / {fmt_pct(rolling_stats.get('best_return_delta'))}",
        f"- median max-DD delta: {fmt_pct(rolling_stats.get('median_max_drawdown_delta'))}",
        "",
        "## Rolling start-regime diagnostic",
        "",
        "|Regime|N|Better|Equal|Worse|Median return delta|Median max-DD delta|",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for _, r in regimes.iterrows():
        lines.append(
            f"|{r['btc_start_regime']}|{int(r['windows'])}|"
            f"{int(r['better'])}|{int(r['equal'])}|{int(r['worse'])}|"
            f"{fmt_pct(r['median_return_delta'])}|"
            f"{fmt_pct(r['median_max_drawdown_delta'])}|"
        )

    lines += [
        "",
        "## Current LINK -> TRX -> AAVE path check",
        "",
        f"- CORE final asset: {case_core['final_asset']}",
        f"- CORE+V2 final asset: {case_overlay['final_asset']}",
        f"- CORE final capital: {case_core['final_capital']:.4f}",
        f"- CORE+V2 final capital: {case_overlay['final_capital']:.4f}",
        f"- overlay/core final ratio: {case_overlay['final_capital']/case_core['final_capital']:.4f}x",
        "",
        "CORE+V2 transitions:",
    ]
    for t in case_overlay["transitions"]:
        lines.append(
            f"- {pd.Timestamp(t['execute_date']).date()}: "
            f"{t['type']} {t['from_asset']} -> {t['to_asset']} "
            f"(cost {100*t['cost_rate']:.2f}%)"
        )

    lines += [
        "",
        "## Decision gates",
        "",
    ]
    for k, v in decision_details["gates"].items():
        lines.append(f"- {k}: {v}")

    lines += [
        "",
        "## Boundary",
        "",
        "- Frozen V2 classifier was not retuned.",
        "- No production/live/Telegram/exchange behavior changed.",
        "- Even a promising research classification requires unseen forward evidence and explicit promotion approval.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_PATH_DEPENDENT_STRESS_TEST",
        "",
    ]

    report = "\n".join(lines)
    (run_dir / "report.md").write_text(report, encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()
