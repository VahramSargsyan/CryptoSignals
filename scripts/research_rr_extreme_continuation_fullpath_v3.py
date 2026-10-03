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
from strategies.crypto.relative_rotation.paper_live import TARGET_ASSETS

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "rr_extreme_continuation_fullpath_v3"

AS_OF = base.AS_OF
MATURE_START = pd.Timestamp("2023-10-31T00:00:00Z")
COSTS = (0.001, 0.005, 0.01, 0.03)
PRIMARY_COST = 0.001
ACCEL_THRESHOLD = 0.10
OBS_DAYS = (1, 2, 3)
ROLLING_LENGTHS = (365, 730)
TOL = 1e-12
U10 = tuple(TARGET_ASSETS)


def source_sha() -> str:
    return os.environ.get("SOURCE_COMMIT_SHA") or subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def prepare_arrays(panel: pd.DataFrame):
    timestamps = pd.DatetimeIndex(pd.to_datetime(panel["timestamp"], utc=True))
    assets = sorted({
        col[:-5] for col in panel.columns if col.endswith("_open")
    })
    opens = {
        asset: panel[f"{asset}_open"].astype(float).to_numpy()
        for asset in assets
    }
    closes = {
        asset: panel[f"{asset}_close"].astype(float).to_numpy()
        for asset in assets
    }
    return timestamps, opens, closes


def build_route_map(
    panel: pd.DataFrame,
    routes: pd.DataFrame,
) -> dict[tuple[int, str], dict]:
    date_to_i = {
        pd.Timestamp(ts): i
        for i, ts in enumerate(pd.to_datetime(panel["timestamp"], utc=True))
    }
    out = {}
    for _, row in routes.iterrows():
        date = pd.Timestamp(row["signal_date"])
        idx = date_to_i.get(date)
        if idx is None:
            continue
        out[(idx, str(row["source"]).upper())] = row.to_dict()
    return out


def strongest_accel_fast(
    opens: dict[str, np.ndarray],
    closes: dict[str, np.ndarray],
    held: str,
    entry_i: int,
    obs_i: int,
) -> tuple[str, float, list[tuple[str, float]]]:
    held_factor = (
        float(closes[held][obs_i])
        / float(opens[held][entry_i])
    )
    scores = []
    for candidate in U10:
        if candidate == held:
            continue
        factor = (
            float(closes[candidate][obs_i])
            / float(opens[candidate][entry_i])
        )
        impulse = factor / held_factor - 1.0
        scores.append((candidate, float(impulse)))
    scores.sort(key=lambda x: (-x[1], x[0]))
    return scores[0][0], scores[0][1], scores


def candidate_impulse(
    scores: list[tuple[str, float]],
    candidate: str,
) -> float:
    for asset, value in scores:
        if asset == candidate:
            return float(value)
    raise RuntimeError(f"candidate not in scores: {candidate}")


def local_relative_excess_open(
    opens: dict[str, np.ndarray],
    candidate: str,
    baseline: str,
    start_i: int,
    horizon: int,
) -> float:
    end_i = start_i + horizon
    if end_i >= len(opens[candidate]):
        return np.nan
    c = float(opens[candidate][end_i]) / float(opens[candidate][start_i]) - 1.0
    b = float(opens[baseline][end_i]) / float(opens[baseline][start_i]) - 1.0
    return (1.0 + c) / (1.0 + b) - 1.0


def download_btc_regime() -> tuple[pd.DataFrame, dict]:
    client = BinanceSpotRestClient()
    result = download_historical_dataset(
        client,
        symbol="BTCUSDT",
        start=base.DOWNLOAD_START,
        end=AS_OF,
        timeframe="1D",
        as_of=AS_OF,
    )
    if result.dataset is None:
        raise RuntimeError(f"BTC: no D1 dataset ({result.metadata.status})")
    if result.dataset.quality.has_critical_issues:
        raise RuntimeError("BTC: critical D1 data quality")

    frame = result.dataset.candles[["timestamp", "close"]].copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame["close"] = frame["close"].astype(float)
    frame["sma200"] = frame["close"].rolling(200, min_periods=200).mean()
    frame["regime"] = np.where(
        frame["close"] >= frame["sma200"],
        "BTC_BULL",
        "BTC_BEAR",
    )
    frame.loc[frame["sma200"].isna(), "regime"] = "UNKNOWN"
    meta = {
        "dataset_id": result.dataset.dataset_id,
        "rows": int(len(frame)),
        "start": pd.Timestamp(frame.iloc[0]["timestamp"]).isoformat(),
        "end": pd.Timestamp(frame.iloc[-1]["timestamp"]).isoformat(),
    }
    return frame, meta


def btc_regime_map(frame: pd.DataFrame) -> dict[pd.Timestamp, str]:
    return {
        pd.Timestamp(row["timestamp"]): str(row["regime"])
        for _, row in frame.iterrows()
    }


def execute_transition(
    *,
    i: int,
    action: dict,
    current: str,
    qty: float,
    cost: float,
    opens: dict[str, np.ndarray],
    scale: float,
    timestamps: pd.DatetimeIndex,
) -> tuple[str, float, dict]:
    target = str(action["to_asset"]).upper()
    value_before = qty * float(opens[current][i])
    cost_value = value_before * cost
    value_after = value_before - cost_value
    qty_after = value_after / float(opens[target][i])

    trace = {
        "action_type": action["action_type"],
        "signal_date": pd.Timestamp(action["signal_date"]),
        "execute_date": pd.Timestamp(timestamps[i]),
        "from_asset": current,
        "to_asset": target,
        "capital_before_cost_usdt": float(value_before * scale),
        "transition_cost_usdt": float(cost_value * scale),
        "capital_after_cost_usdt": float(value_after * scale),
        "cost_rate": float(cost),
        "ddg_override": bool(action.get("ddg_override", False)),
        "baseline_to": str(action.get("baseline_to", "")),
    }
    for key in (
        "trigger_date",
        "trigger_impulse",
        "confirm1_date",
        "confirm1_impulse",
        "confirm2_date",
        "confirm2_impulse",
    ):
        trace[key] = action.get(key, np.nan)

    return target, qty_after, trace


def simulate_path(
    *,
    timestamps: pd.DatetimeIndex,
    opens: dict[str, np.ndarray],
    closes: dict[str, np.ndarray],
    route_map: dict[tuple[int, str], dict],
    start_i: int,
    end_i: int,
    start_asset: str,
    cost: float,
    overlay: bool,
    regime_by_date: dict[pd.Timestamp, str] | None = None,
    collect_trace: bool = False,
) -> dict:
    current = start_asset
    qty = 1.0 / float(opens[current][start_i])
    initial_equity = qty * float(closes[current][start_i])
    scale = 100.0 / initial_equity

    equity = np.empty(end_i - start_i + 1, dtype=float)
    pending_action = None
    monitor = None
    transitions = []
    overlay_local = []
    rr_count = 0
    overlay_count = 0
    canceled_monitors = 0

    for i in range(start_i, end_i + 1):
        if pending_action is not None and pending_action["execute_i"] == i:
            old_current = current
            current, qty, trace = execute_transition(
                i=i,
                action=pending_action,
                current=current,
                qty=qty,
                cost=cost,
                opens=opens,
                scale=scale,
                timestamps=timestamps,
            )
            transitions.append(trace)

            if pending_action["action_type"] == "RR_DDG":
                rr_count += 1
                monitor = {
                    "held": current,
                    "entry_i": i,
                    "phase": "SEARCH",
                    "trigger_candidate": None,
                    "trigger_i": None,
                    "trigger_impulse": None,
                    "confirm1_i": None,
                    "confirm1_impulse": None,
                } if overlay else None
            else:
                overlay_count += 1
                monitor = None
                confirmation_date = pd.Timestamp(pending_action["confirm2_date"])
                regime = (
                    regime_by_date.get(confirmation_date, "UNKNOWN")
                    if regime_by_date is not None else "UNKNOWN"
                )
                rel14 = local_relative_excess_open(
                    opens,
                    current,
                    old_current,
                    i,
                    14,
                )
                overlay_local.append(
                    {
                        "confirmation_date": confirmation_date,
                        "execute_date": pd.Timestamp(timestamps[i]),
                        "from_asset": old_current,
                        "to_asset": current,
                        "btc_regime": regime,
                        "local_14d_relative_excess": rel14,
                        "trigger_impulse": float(pending_action["trigger_impulse"]),
                        "confirm1_impulse": float(pending_action["confirm1_impulse"]),
                        "confirm2_impulse": float(pending_action["confirm2_impulse"]),
                    }
                )
            pending_action = None

        equity[i - start_i] = qty * float(closes[current][i])

        if i >= end_i:
            continue

        route = route_map.get((i, current))
        if route is not None:
            if monitor is not None:
                canceled_monitors += 1
            monitor = None
            pending_action = {
                "action_type": "RR_DDG",
                "signal_date": pd.Timestamp(timestamps[i]),
                "execute_i": i + 1,
                "to_asset": str(route["effective_to"]).upper(),
                "ddg_override": bool(route["override"]),
                "baseline_to": str(route["baseline_to"]).upper(),
            }
            continue

        if not overlay or monitor is None:
            continue
        if monitor["held"] != current:
            monitor = None
            continue

        entry_i = int(monitor["entry_i"])
        phase = str(monitor["phase"])

        if phase == "SEARCH":
            day = i - entry_i + 1
            if day < 1:
                continue
            if day > max(OBS_DAYS):
                monitor = None
                continue
            candidate, impulse, _ = strongest_accel_fast(
                opens, closes, current, entry_i, i
            )
            if impulse >= ACCEL_THRESHOLD:
                monitor.update(
                    {
                        "phase": "CONFIRM1",
                        "trigger_candidate": candidate,
                        "trigger_i": i,
                        "trigger_impulse": float(impulse),
                    }
                )
            elif day == max(OBS_DAYS):
                monitor = None

        elif phase == "CONFIRM1":
            expected_i = int(monitor["trigger_i"]) + 1
            if i < expected_i:
                continue
            if i > expected_i:
                monitor = None
                continue

            top, _, scores = strongest_accel_fast(
                opens, closes, current, entry_i, i
            )
            candidate = str(monitor["trigger_candidate"])
            impulse = candidate_impulse(scores, candidate)
            if top != candidate:
                monitor = None
            else:
                monitor.update(
                    {
                        "phase": "CONFIRM2",
                        "confirm1_i": i,
                        "confirm1_impulse": float(impulse),
                    }
                )

        elif phase == "CONFIRM2":
            expected_i = int(monitor["trigger_i"]) + 2
            if i < expected_i:
                continue
            if i > expected_i:
                monitor = None
                continue

            top, _, scores = strongest_accel_fast(
                opens, closes, current, entry_i, i
            )
            candidate = str(monitor["trigger_candidate"])
            impulse = candidate_impulse(scores, candidate)
            passed = (
                top == candidate
                and float(impulse) > float(monitor["trigger_impulse"])
            )
            if passed:
                pending_action = {
                    "action_type": "OVERLAY",
                    "signal_date": pd.Timestamp(timestamps[i]),
                    "execute_i": i + 1,
                    "to_asset": candidate,
                    "ddg_override": False,
                    "baseline_to": "",
                    "trigger_date": pd.Timestamp(
                        timestamps[int(monitor["trigger_i"])]
                    ),
                    "trigger_impulse": float(monitor["trigger_impulse"]),
                    "confirm1_date": pd.Timestamp(
                        timestamps[int(monitor["confirm1_i"])]
                    ),
                    "confirm1_impulse": float(monitor["confirm1_impulse"]),
                    "confirm2_date": pd.Timestamp(timestamps[i]),
                    "confirm2_impulse": float(impulse),
                }
            monitor = None

    running_peak = np.maximum.accumulate(equity)
    dd = equity / running_peak - 1.0

    result = {
        "start_asset": start_asset,
        "return": float(equity[-1] / equity[0] - 1.0),
        "max_dd": float(np.min(dd)),
        "transition_count": int(len(transitions)),
        "rr_transition_count": int(rr_count),
        "overlay_transition_count": int(overlay_count),
        "canceled_overlay_monitors": int(canceled_monitors),
        "final_asset": current,
        "equity": equity,
        "overlay_local": overlay_local,
    }
    if collect_trace:
        result["transitions"] = transitions
    return result


def aggregate_runs(runs: list[dict]) -> dict:
    returns = np.asarray([r["return"] for r in runs], dtype=float)
    dds = np.asarray([r["max_dd"] for r in runs], dtype=float)
    transitions = np.asarray([r["transition_count"] for r in runs], dtype=float)
    overlays = np.asarray([r["overlay_transition_count"] for r in runs], dtype=float)
    finals = Counter(str(r["final_asset"]) for r in runs)
    return {
        "median_return": float(np.median(returns)),
        "worst_return": float(np.min(returns)),
        "best_return": float(np.max(returns)),
        "positive_start_rate": float(np.mean(returns > 0)),
        "median_max_dd": float(np.median(dds)),
        "worst_max_dd": float(np.min(dds)),
        "median_transitions": float(np.median(transitions)),
        "median_overlay_transitions": float(np.median(overlays)),
        "total_overlay_transitions": int(np.sum(overlays)),
        "final_asset_distribution": json.dumps(dict(sorted(finals.items()))),
    }


def evaluate_window(
    *,
    timestamps,
    opens,
    closes,
    route_map,
    start_i,
    end_i,
    cost,
    overlay,
    regime_by_date,
    collect_trace=False,
) -> tuple[pd.DataFrame, dict, list[dict]]:
    runs = []
    rows = []
    for start_asset in U10:
        result = simulate_path(
            timestamps=timestamps,
            opens=opens,
            closes=closes,
            route_map=route_map,
            start_i=start_i,
            end_i=end_i,
            start_asset=start_asset,
            cost=cost,
            overlay=overlay,
            regime_by_date=regime_by_date,
            collect_trace=collect_trace,
        )
        runs.append(result)
        rows.append(
            {
                "start_asset": start_asset,
                "return": result["return"],
                "max_dd": result["max_dd"],
                "transition_count": result["transition_count"],
                "rr_transition_count": result["rr_transition_count"],
                "overlay_transition_count": result["overlay_transition_count"],
                "canceled_overlay_monitors": result["canceled_overlay_monitors"],
                "final_asset": result["final_asset"],
            }
        )
    return pd.DataFrame(rows), aggregate_runs(runs), runs


def build_windows(timestamps: pd.DatetimeIndex) -> dict[str, tuple[int, int]]:
    end_i = len(timestamps) - 1
    end_date = pd.Timestamp(timestamps[end_i])
    definitions = {
        "LAST_1Y": end_date - pd.Timedelta(days=364),
        "LAST_2Y": end_date - pd.Timedelta(days=729),
        "MATURE": MATURE_START,
    }
    windows = {}
    for name, start_date in definitions.items():
        start_i = int(timestamps.searchsorted(start_date, side="left"))
        windows[name] = (start_i, end_i)
    return windows


def evaluate_primary_grid(
    *,
    timestamps,
    opens,
    closes,
    route_map,
    windows,
    regime_by_date,
) -> tuple[pd.DataFrame, pd.DataFrame, list[dict]]:
    summary_rows = []
    start_rows = []
    mature_overlay_runs_primary = []

    for cost in COSTS:
        for window_name, (start_i, end_i) in windows.items():
            for variant, overlay in (
                ("BASELINE", False),
                ("OVERLAY_V2", True),
            ):
                starts, agg, runs = evaluate_window(
                    timestamps=timestamps,
                    opens=opens,
                    closes=closes,
                    route_map=route_map,
                    start_i=start_i,
                    end_i=end_i,
                    cost=cost,
                    overlay=overlay,
                    regime_by_date=regime_by_date,
                    collect_trace=(
                        cost == PRIMARY_COST
                        and window_name == "MATURE"
                        and overlay
                    ),
                )
                for _, row in starts.iterrows():
                    start_rows.append(
                        {
                            "cost": cost,
                            "window": window_name,
                            "variant": variant,
                            **row.to_dict(),
                        }
                    )
                summary_rows.append(
                    {
                        "cost": cost,
                        "window": window_name,
                        "variant": variant,
                        **agg,
                    }
                )
                if (
                    cost == PRIMARY_COST
                    and window_name == "MATURE"
                    and overlay
                ):
                    mature_overlay_runs_primary = runs

    return (
        pd.DataFrame(summary_rows),
        pd.DataFrame(start_rows),
        mature_overlay_runs_primary,
    )


def compare_summary(summary: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (cost, window), group in summary.groupby(
        ["cost", "window"], sort=True
    ):
        base_row = group[group["variant"] == "BASELINE"].iloc[0]
        over_row = group[group["variant"] == "OVERLAY_V2"].iloc[0]
        rows.append(
            {
                "cost": float(cost),
                "window": window,
                "baseline_median_return": float(base_row["median_return"]),
                "overlay_median_return": float(over_row["median_return"]),
                "median_return_delta": float(
                    over_row["median_return"] - base_row["median_return"]
                ),
                "baseline_worst_return": float(base_row["worst_return"]),
                "overlay_worst_return": float(over_row["worst_return"]),
                "baseline_median_max_dd": float(base_row["median_max_dd"]),
                "overlay_median_max_dd": float(over_row["median_max_dd"]),
                "median_max_dd_delta": float(
                    over_row["median_max_dd"] - base_row["median_max_dd"]
                ),
                "baseline_median_transitions": float(
                    base_row["median_transitions"]
                ),
                "overlay_median_transitions": float(
                    over_row["median_transitions"]
                ),
                "overlay_total_overlay_transitions": int(
                    over_row["total_overlay_transitions"]
                ),
            }
        )
    return pd.DataFrame(rows)


def rolling_comparison(
    *,
    timestamps,
    opens,
    closes,
    route_map,
    regime_by_date,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    mature_start_i = int(
        timestamps.searchsorted(MATURE_START, side="left")
    )
    detail_rows = []

    for length in ROLLING_LENGTHS:
        last_start = len(timestamps) - length
        if last_start < mature_start_i:
            continue
        for start_i in range(mature_start_i, last_start + 1):
            end_i = start_i + length - 1
            base_returns = []
            overlay_returns = []
            for start_asset in U10:
                b = simulate_path(
                    timestamps=timestamps,
                    opens=opens,
                    closes=closes,
                    route_map=route_map,
                    start_i=start_i,
                    end_i=end_i,
                    start_asset=start_asset,
                    cost=PRIMARY_COST,
                    overlay=False,
                    regime_by_date=regime_by_date,
                    collect_trace=False,
                )
                o = simulate_path(
                    timestamps=timestamps,
                    opens=opens,
                    closes=closes,
                    route_map=route_map,
                    start_i=start_i,
                    end_i=end_i,
                    start_asset=start_asset,
                    cost=PRIMARY_COST,
                    overlay=True,
                    regime_by_date=regime_by_date,
                    collect_trace=False,
                )
                base_returns.append(b["return"])
                overlay_returns.append(o["return"])

            b_med = float(np.median(base_returns))
            o_med = float(np.median(overlay_returns))
            detail_rows.append(
                {
                    "window_days": length,
                    "start_date": pd.Timestamp(timestamps[start_i]),
                    "end_date": pd.Timestamp(timestamps[end_i]),
                    "baseline_median_return": b_med,
                    "overlay_median_return": o_med,
                    "delta": o_med - b_med,
                }
            )

    detail = pd.DataFrame(detail_rows)
    summary_rows = []
    for length, sub in detail.groupby("window_days", sort=True):
        delta = sub["delta"].astype(float)
        summary_rows.append(
            {
                "window_days": int(length),
                "window_count": int(len(sub)),
                "better_count": int((delta > TOL).sum()),
                "equal_count": int((delta.abs() <= TOL).sum()),
                "worse_count": int((delta < -TOL).sum()),
                "median_delta": float(delta.median()),
                "worst_delta": float(delta.min()),
                "best_delta": float(delta.max()),
            }
        )
    return detail, pd.DataFrame(summary_rows)


def overlay_regime_summary(
    mature_runs: list[dict],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    seen = set()
    for run in mature_runs:
        for event in run["overlay_local"]:
            key = (
                pd.Timestamp(event["confirmation_date"]),
                event["from_asset"],
                event["to_asset"],
            )
            if key in seen:
                continue
            seen.add(key)
            rows.append(dict(event))

    events = pd.DataFrame(rows)
    if events.empty:
        return events, pd.DataFrame()

    summary_rows = []
    for regime, sub in events.groupby("btc_regime", sort=True):
        vals = sub["local_14d_relative_excess"].dropna().astype(float)
        summary_rows.append(
            {
                "btc_regime": regime,
                "unique_overlay_switches": int(len(sub)),
                "complete_14d_n": int(len(vals)),
                "win_rate": (
                    float((vals > 0).mean()) if len(vals) else np.nan
                ),
                "median_relative_excess": (
                    float(vals.median()) if len(vals) else np.nan
                ),
                "mean_relative_excess": (
                    float(vals.mean()) if len(vals) else np.nan
                ),
                "strong_ge20_rate": (
                    float((vals >= 0.20).mean()) if len(vals) else np.nan
                ),
            }
        )
    return events, pd.DataFrame(summary_rows)


def collect_mature_trace(
    mature_runs: list[dict],
) -> pd.DataFrame:
    rows = []
    for run in mature_runs:
        for idx, transition in enumerate(run.get("transitions", []), start=1):
            rows.append(
                {
                    "start_asset": run["start_asset"],
                    "transition_index": idx,
                    **transition,
                }
            )
    return pd.DataFrame(rows)


def link_aave_diagnostic(
    *,
    timestamps,
    opens,
    closes,
    route_map,
    regime_by_date,
) -> tuple[dict, pd.DataFrame]:
    start_i = int(
        np.flatnonzero(
            timestamps == pd.Timestamp("2026-09-28T00:00:00Z")
        )[0]
    )
    end_i = len(timestamps) - 1
    result = simulate_path(
        timestamps=timestamps,
        opens=opens,
        closes=closes,
        route_map=route_map,
        start_i=start_i,
        end_i=end_i,
        start_asset="LINK",
        cost=PRIMARY_COST,
        overlay=True,
        regime_by_date=regime_by_date,
        collect_trace=True,
    )
    transitions = pd.DataFrame(result.get("transitions", []))
    payload = {
        "start_date": pd.Timestamp(timestamps[start_i]).isoformat(),
        "end_date": pd.Timestamp(timestamps[end_i]).isoformat(),
        "return": result["return"],
        "max_dd": result["max_dd"],
        "transition_count": result["transition_count"],
        "rr_transition_count": result["rr_transition_count"],
        "overlay_transition_count": result["overlay_transition_count"],
        "final_asset": result["final_asset"],
    }
    return payload, transitions


def classify(
    comparison: pd.DataFrame,
    rolling_summary: pd.DataFrame,
) -> str:
    p = comparison[
        (comparison["cost"] == PRIMARY_COST)
        & (comparison["window"] == "MATURE")
    ].iloc[0]
    c05 = comparison[
        (comparison["cost"] == 0.005)
        & (comparison["window"] == "MATURE")
    ].iloc[0]
    r365 = rolling_summary[
        rolling_summary["window_days"] == 365
    ].iloc[0]

    return_improved = p["median_return_delta"] > 0
    dd_ok = p["overlay_median_max_dd"] >= (
        p["baseline_median_max_dd"] - 0.02
    )
    rolling_ok = r365["better_count"] > r365["worse_count"]
    cost_ok = c05["median_return_delta"] > 0

    if return_improved and dd_ok and rolling_ok and cost_ok:
        return "FULLPATH_PROMISING_RESEARCH_SIGNAL"
    if (
        (p["median_return_delta"] > 0 and p["median_max_dd_delta"] < -0.02)
        or (p["median_return_delta"] < 0 and p["median_max_dd_delta"] > 0.02)
    ):
        return "FULLPATH_TRADEOFF_ONLY"
    return "FULLPATH_REJECTED"


def fmt_pct(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{100*float(v):+.2f}%"


def fmt_rate(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{100*float(v):.1f}%"


def main() -> None:
    panel, data_meta = base.download_panel()
    events_by_date, states_by_date = base.build_monitor_history(panel)
    routes = base.build_effective_routes(
        panel, events_by_date, states_by_date
    )
    timestamps, opens, closes = prepare_arrays(panel)
    route_map = build_route_map(panel, routes)

    btc, btc_meta = download_btc_regime()
    regime_by_date = btc_regime_map(btc)

    windows = build_windows(timestamps)

    summary, starts, mature_overlay_runs = evaluate_primary_grid(
        timestamps=timestamps,
        opens=opens,
        closes=closes,
        route_map=route_map,
        windows=windows,
        regime_by_date=regime_by_date,
    )
    comparison = compare_summary(summary)

    rolling_detail, rolling_summary = rolling_comparison(
        timestamps=timestamps,
        opens=opens,
        closes=closes,
        route_map=route_map,
        regime_by_date=regime_by_date,
    )

    regime_events, regime_summary = overlay_regime_summary(
        mature_overlay_runs
    )
    mature_trace = collect_mature_trace(mature_overlay_runs)
    link_case, link_trace = link_aave_diagnostic(
        timestamps=timestamps,
        opens=opens,
        closes=closes,
        route_map=route_map,
        regime_by_date=regime_by_date,
    )

    classification = classify(comparison, rolling_summary)

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    summary.to_csv(run_dir / "window_variant_summary.csv", index=False)
    starts.to_csv(run_dir / "window_start_asset_results.csv", index=False)
    comparison.to_csv(run_dir / "baseline_vs_overlay_comparison.csv", index=False)
    rolling_detail.to_csv(run_dir / "rolling_detail.csv", index=False)
    rolling_summary.to_csv(run_dir / "rolling_summary.csv", index=False)
    regime_events.to_csv(run_dir / "overlay_regime_events.csv", index=False)
    regime_summary.to_csv(run_dir / "overlay_regime_summary.csv", index=False)
    mature_trace.to_csv(run_dir / "mature_overlay_transition_trace.csv", index=False)
    link_trace.to_csv(run_dir / "link_aave_diagnostic_trace.csv", index=False)
    (run_dir / "link_aave_diagnostic.json").write_text(
        json.dumps(link_case, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )

    primary = comparison[
        (comparison["cost"] == PRIMARY_COST)
        & (comparison["window"] == "MATURE")
    ].iloc[0]
    summary_json = {
        "experiment": "RR_EXTREME_CONTINUATION_FULLPATH_V3",
        "workflow_mode": "STRESS_TEST_ONLY",
        "classification": classification,
        "source_commit_sha": source_sha(),
        "as_of": AS_OF.isoformat(),
        "latest_closed_candle": pd.Timestamp(timestamps[-1]).isoformat(),
        "u10": list(U10),
        "cost_grid": list(COSTS),
        "primary_mature": {
            key: (
                int(value) if isinstance(value, (np.integer, int))
                else float(value) if isinstance(value, (np.floating, float))
                else value
            )
            for key, value in primary.to_dict().items()
        },
        "rolling_summary": json.loads(
            rolling_summary.to_json(orient="records", date_format="iso")
        ),
        "regime_summary": json.loads(
            regime_summary.to_json(orient="records", date_format="iso")
        ),
        "link_case": link_case,
        "data_metadata": data_meta,
        "btc_metadata": btc_meta,
        "production_changes": "NONE",
        "test_level": "GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_FULL_PATH_STRESS_TEST",
    }
    (run_dir / "summary.json").write_text(
        json.dumps(summary_json, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )

    lines = [
        "# RR EXTREME CONTINUATION FULL-PATH V3 — RESULTS",
        "",
        "Mode: STRESS_TEST_ONLY",
        "",
        f"Classification: {classification}",
        f"As-of: {AS_OF.isoformat()}",
        f"Latest closed D1: {pd.Timestamp(timestamps[-1]).isoformat()}",
        "",
        "## Full-path comparison",
        "",
        "|Cost|Window|Baseline median|Overlay median|Delta|Baseline median DD|Overlay median DD|DD delta|Baseline transitions|Overlay transitions|Overlay switches total|",
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in comparison.sort_values(["cost", "window"]).iterrows():
        lines.append(
            f"|{100*row['cost']:.2f}%|{row['window']}|"
            f"{fmt_pct(row['baseline_median_return'])}|"
            f"{fmt_pct(row['overlay_median_return'])}|"
            f"{fmt_pct(row['median_return_delta'])}|"
            f"{fmt_pct(row['baseline_median_max_dd'])}|"
            f"{fmt_pct(row['overlay_median_max_dd'])}|"
            f"{fmt_pct(row['median_max_dd_delta'])}|"
            f"{row['baseline_median_transitions']:.1f}|"
            f"{row['overlay_median_transitions']:.1f}|"
            f"{int(row['overlay_total_overlay_transitions'])}|"
        )

    lines += [
        "",
        "## Rolling stability at 0.10% cost",
        "",
        "|Window|N|Better|Equal|Worse|Median delta|Worst delta|Best delta|",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in rolling_summary.iterrows():
        lines.append(
            f"|{int(row['window_days'])}d|{int(row['window_count'])}|"
            f"{int(row['better_count'])}|{int(row['equal_count'])}|"
            f"{int(row['worse_count'])}|{fmt_pct(row['median_delta'])}|"
            f"{fmt_pct(row['worst_delta'])}|{fmt_pct(row['best_delta'])}|"
        )

    lines += [
        "",
        "## Overlay switch BTC-SMA200 regime diagnostic",
        "",
        "|Regime|Unique switches|14d N|Win rate|Median excess|Mean excess|>=20%|",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    if regime_summary.empty:
        lines.append("|—|0|0|—|—|—|—|")
    else:
        for _, row in regime_summary.iterrows():
            lines.append(
                f"|{row['btc_regime']}|{int(row['unique_overlay_switches'])}|"
                f"{int(row['complete_14d_n'])}|{fmt_rate(row['win_rate'])}|"
                f"{fmt_pct(row['median_relative_excess'])}|"
                f"{fmt_pct(row['mean_relative_excess'])}|"
                f"{fmt_rate(row['strong_ge20_rate'])}|"
            )

    lines += [
        "",
        "## LINK -> TRX -> AAVE diagnostic",
        "",
        f"- start: {link_case['start_date']}",
        f"- end: {link_case['end_date']}",
        f"- final asset: {link_case['final_asset']}",
        f"- transitions: {link_case['transition_count']}",
        f"- RR transitions: {link_case['rr_transition_count']}",
        f"- overlay transitions: {link_case['overlay_transition_count']}",
        f"- path return over diagnostic slice: {fmt_pct(link_case['return'])}",
        "",
    ]
    if not link_trace.empty:
        lines += [
            "|Type|Signal|Execute|From|To|Cost|Trigger|Confirm2|",
            "|---|---|---|---|---|---:|---|---|",
        ]
        for _, row in link_trace.iterrows():
            lines.append(
                f"|{row['action_type']}|{pd.Timestamp(row['signal_date']).date()}|"
                f"{pd.Timestamp(row['execute_date']).date()}|{row['from_asset']}|"
                f"{row['to_asset']}|{100*float(row['cost_rate']):.2f}%|"
                f"{'—' if pd.isna(row['trigger_date']) else pd.Timestamp(row['trigger_date']).date()}|"
                f"{'—' if pd.isna(row['confirm2_date']) else pd.Timestamp(row['confirm2_date']).date()}|"
            )

    lines += [
        "",
        "## Boundary",
        "",
        "- V2 PERSIST2_REEXPAND semantics were not changed.",
        "- Canonical RR+DDG wins any same-close conflict.",
        "- No acceleration-on-acceleration cascade is allowed.",
        "- No production/live/Telegram/exchange behavior changed.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_FULL_PATH_STRESS_TEST",
        "",
    ]
    report = "\n".join(lines)
    (run_dir / "report.md").write_text(report, encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()
