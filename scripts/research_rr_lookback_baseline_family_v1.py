from __future__ import annotations

import itertools
import json
import os
import subprocess
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import (
    ARM_THRESHOLD,
    ASSETS,
    DESTINATION_DOMINANCE_MIN_STRENGTH_RATIO,
    REVERSAL,
    TARGET_ASSETS,
    PairState,
    choose_destination_dominance_override,
    choose_held_events,
    find_route_conflicts,
)

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "rr_lookback_baseline_family_v1"

AS_OF = pd.Timestamp("2026-10-03T16:30:00Z")
DOWNLOAD_START = pd.Timestamp("2023-01-01T00:00:00Z")
COST = 0.001

LOOKBACKS = (25, 45, 50, 90, 180)
BASELINES = ("MEDIAN", "SMA")
VARIANTS = tuple(
    f"{baseline}_{lookback}"
    for baseline in BASELINES
    for lookback in LOOKBACKS
)
CANONICAL = "MEDIAN_180"

WINDOW_STARTS = {
    "MATURE": pd.Timestamp("2023-10-31T00:00:00Z"),
    "LAST_2Y": pd.Timestamp("2024-10-03T00:00:00Z"),
    "LAST_1Y": pd.Timestamp("2025-10-03T00:00:00Z"),
}

AUDIT_DATES = tuple(
    pd.Timestamp(x)
    for x in (
        "2026-09-28T00:00:00Z",
        "2026-09-29T00:00:00Z",
        "2026-09-30T00:00:00Z",
        "2026-10-01T00:00:00Z",
    )
)
AUDIT_SOURCES = ("LINK", "TRX", "AAVE")


def source_sha() -> str:
    return os.environ.get("SOURCE_COMMIT_SHA") or subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def download_panel() -> tuple[pd.DataFrame, dict]:
    client = BinanceSpotRestClient()
    pieces = []
    meta: dict[str, dict] = {}

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


def parse_variant(variant: str) -> tuple[str, int]:
    baseline, n = variant.split("_", 1)
    return baseline, int(n)


def reference_series(ratio: pd.Series, baseline: str, lookback: int) -> pd.Series:
    rolling = ratio.rolling(lookback, min_periods=lookback)
    if baseline == "MEDIAN":
        return rolling.median()
    if baseline == "SMA":
        return rolling.mean()
    raise ValueError(baseline)


def event_row(
    ts: pd.Timestamp,
    event: str,
    pair: str,
    from_asset: str,
    to_asset: str,
    ratio: float,
    reference: float,
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
        "median": reference,
        "deviation": deviation,
        "max_dislocation": max_dislocation,
        "reversal_from_extreme": reversal_from_extreme,
    }


def build_monitor_history(
    panel: pd.DataFrame,
    baseline: str,
    lookback: int,
) -> tuple[dict[pd.Timestamp, list[dict]], dict[pd.Timestamp, list[dict]]]:
    events_by_date: dict[pd.Timestamp, list[dict]] = defaultdict(list)
    states_by_date: dict[pd.Timestamp, list[dict]] = defaultdict(list)

    for left, right in itertools.combinations(ASSETS, 2):
        ratio_series = panel[f"{right}_close"] / panel[f"{left}_close"]
        ref_series = reference_series(ratio_series, baseline, lookback)
        deviation_series = ratio_series / ref_series - 1.0
        state = PairState()

        for idx, ts_raw in enumerate(panel["timestamp"]):
            ts = pd.Timestamp(ts_raw)
            ref_raw = ref_series.iloc[idx]
            dev_raw = deviation_series.iloc[idx]
            if pd.isna(ref_raw) or pd.isna(dev_raw):
                continue

            ratio = float(ratio_series.iloc[idx])
            ref = float(ref_raw)
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
                        event_row(
                            ts, "ARMED", pair, right, left,
                            ratio, ref, deviation, abs(deviation), 0.0
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
                        event_row(
                            ts, "ARMED", pair, left, right,
                            ratio, ref, deviation, abs(deviation), 0.0
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
                        event_row(
                            ts, "CONFIRMED", pair, right, left,
                            ratio, ref, deviation,
                            state.max_dislocation, retracement
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
                        event_row(
                            ts, "CONFIRMED", pair, left, right,
                            ratio, ref, deviation,
                            state.max_dislocation, retracement
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
                    "median": ref,
                    "deviation": deviation,
                    "extreme": state.extreme,
                    "max_dislocation": state.max_dislocation,
                    "reversal_from_extreme": retracement_now,
                }
            )

    return events_by_date, states_by_date


def build_routes(
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

            rows.append(
                {
                    "signal_date": ts,
                    "source": source,
                    "baseline_to": str(primary["to_asset"]).upper(),
                    "effective_to": str(effective["to_asset"]).upper(),
                    "override": chosen_conflict is not None,
                    "baseline_max_dislocation": float(
                        primary.get("max_dislocation") or 0.0
                    ),
                    "effective_max_dislocation": float(
                        effective.get("max_dislocation") or 0.0
                    ),
                    "strength_ratio": (
                        np.nan
                        if chosen_conflict is None
                        else float(chosen_conflict.get("strength_ratio") or np.nan)
                    ),
                }
            )
    if not rows:
        return pd.DataFrame(
            columns=[
                "signal_date", "source", "baseline_to", "effective_to",
                "override", "baseline_max_dislocation",
                "effective_max_dislocation", "strength_ratio",
            ]
        )
    return pd.DataFrame(rows).sort_values(
        ["signal_date", "source"], kind="stable"
    ).reset_index(drop=True)


def route_map(routes: pd.DataFrame) -> dict[tuple[pd.Timestamp, str], dict]:
    return {
        (pd.Timestamp(row["signal_date"]), str(row["source"])): row.to_dict()
        for _, row in routes.iterrows()
    }


def first_index_on_or_after(panel: pd.DataFrame, ts: pd.Timestamp) -> int:
    dates = pd.to_datetime(panel["timestamp"], utc=True)
    hits = np.flatnonzero(dates.ge(ts).to_numpy())
    if not len(hits):
        raise RuntimeError(f"No date on/after {ts}")
    return int(hits[0])


def simulate(
    panel: pd.DataFrame,
    routes: dict[tuple[pd.Timestamp, str], dict],
    start_i: int,
    end_i: int,
    start_asset: str,
    collect_trace: bool = False,
) -> tuple[dict, pd.DataFrame | None]:
    asset = start_asset
    qty = 100.0 / float(panel.iloc[start_i][f"{asset}_open"])
    pending: dict | None = None
    transitions = 0
    cost_paid = 0.0
    equity = [100.0]
    last_exec_i = start_i
    holding_days = []
    trace = []

    for idx in range(start_i, end_i + 1):
        ts = pd.Timestamp(panel.iloc[idx]["timestamp"])

        if pending is not None:
            old = asset
            before = qty * float(panel.iloc[idx][f"{old}_open"])
            fee = before * COST
            after = before - fee
            asset = str(pending["target"])
            qty = after / float(panel.iloc[idx][f"{asset}_open"])
            transitions += 1
            cost_paid += fee
            holding_days.append(max(1, idx - last_exec_i))
            last_exec_i = idx

            if collect_trace:
                trace.append(
                    {
                        "date": ts,
                        "event": "EXECUTE",
                        "from_asset": old,
                        "to_asset": asset,
                        "value_before": before,
                        "fee": fee,
                        "value_after": after,
                        "override": bool(pending.get("override", False)),
                    }
                )
            pending = None

        close_value = qty * float(panel.iloc[idx][f"{asset}_close"])
        equity.append(float(close_value))

        if idx == end_i:
            continue

        route = routes.get((ts, asset))
        if route is not None:
            target = str(route["effective_to"])
            if target != asset:
                pending = {
                    "target": target,
                    "override": bool(route.get("override", False)),
                }
                if collect_trace:
                    trace.append(
                        {
                            "date": ts,
                            "event": "SIGNAL",
                            "from_asset": asset,
                            "to_asset": target,
                            "value_before": close_value,
                            "fee": 0.0,
                            "value_after": close_value,
                            "override": bool(route.get("override", False)),
                        }
                    )

    holding_days.append(max(1, end_i - last_exec_i + 1))

    arr = np.asarray(equity, dtype=float)
    peaks = np.maximum.accumulate(arr)
    max_dd = float(np.min(arr / peaks - 1.0))
    final_capital = float(arr[-1])

    result = {
        "start_asset": start_asset,
        "final_asset": asset,
        "final_capital": final_capital,
        "return": final_capital / 100.0 - 1.0,
        "max_dd": max_dd,
        "transitions": int(transitions),
        "median_holding_days": float(np.median(holding_days)),
        "modeled_cost_paid": float(cost_paid),
    }
    return result, (pd.DataFrame(trace) if collect_trace else None)


def aggregate(frame: pd.DataFrame) -> dict:
    ends = Counter(frame["final_asset"].astype(str))
    modal_asset, modal_count = ends.most_common(1)[0]
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
        "median_holding_days": float(frame["median_holding_days"].median()),
        "median_modeled_cost_paid": float(frame["modeled_cost_paid"].median()),
        "modal_end_asset": modal_asset,
        "modal_end_asset_share": float(modal_count / len(frame)),
    }


def evaluate_variant(
    panel: pd.DataFrame,
    variant: str,
    routes_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    rmap = route_map(routes_df)
    end_i = len(panel) - 1
    detail_rows = []
    summary_rows = []

    for window, start_ts in WINDOW_STARTS.items():
        start_i = first_index_on_or_after(panel, start_ts)
        local = []
        for start_asset in ASSETS:
            result, _ = simulate(
                panel, rmap, start_i, end_i, start_asset, collect_trace=False
            )
            result["variant"] = variant
            result["window"] = window
            result["start_date"] = pd.Timestamp(panel.iloc[start_i]["timestamp"])
            result["end_date"] = pd.Timestamp(panel.iloc[end_i]["timestamp"])
            detail_rows.append(result)
            local.append(result)

        summary_rows.append(
            {
                "variant": variant,
                "window": window,
                **aggregate(pd.DataFrame(local)),
            }
        )

    return pd.DataFrame(detail_rows), pd.DataFrame(summary_rows)


def monthly_rolling_starts(panel: pd.DataFrame) -> list[tuple[int, int]]:
    dates = pd.to_datetime(panel["timestamp"], utc=True)
    first = max(
        pd.Timestamp("2023-11-01T00:00:00Z"),
        pd.Timestamp(dates.iloc[0]),
    )
    last = pd.Timestamp(dates.iloc[-1])
    pairs = []
    for start in pd.date_range(first, last, freq="MS", tz="UTC"):
        end = start + pd.Timedelta(days=364)
        if end > last:
            continue
        si = first_index_on_or_after(panel, start)
        ei = first_index_on_or_after(panel, end)
        if pd.Timestamp(panel.iloc[ei]["timestamp"]) > end:
            ei -= 1
        if ei > si:
            pairs.append((si, ei))
    return pairs


def evaluate_rolling(
    panel: pd.DataFrame,
    variant_routes: dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    windows = monthly_rolling_starts(panel)
    rows = []

    for variant in VARIANTS:
        rmap = route_map(variant_routes[variant])
        for si, ei in windows:
            returns = []
            dds = []
            transitions = []
            holds = []
            for asset in TARGET_ASSETS:
                result, _ = simulate(panel, rmap, si, ei, asset)
                returns.append(result["return"])
                dds.append(result["max_dd"])
                transitions.append(result["transitions"])
                holds.append(result["median_holding_days"])
            rows.append(
                {
                    "variant": variant,
                    "window_start": pd.Timestamp(panel.iloc[si]["timestamp"]),
                    "window_end": pd.Timestamp(panel.iloc[ei]["timestamp"]),
                    "median_return": float(np.median(returns)),
                    "median_max_dd": float(np.median(dds)),
                    "median_transitions": float(np.median(transitions)),
                    "median_holding_days": float(np.median(holds)),
                }
            )

    detail = pd.DataFrame(rows)
    canonical = detail[detail["variant"] == CANONICAL][
        ["window_start", "median_return"]
    ].rename(columns={"median_return": "canonical_return"})
    detail = detail.merge(
        canonical, on="window_start", how="left", validate="many_to_one"
    )
    detail["return_delta_vs_canonical"] = (
        detail["median_return"] - detail["canonical_return"]
    )

    summaries = []
    for variant, sub in detail.groupby("variant", sort=False):
        summaries.append(
            {
                "variant": variant,
                "window_count": int(len(sub)),
                "median_rolling_return": float(sub["median_return"].median()),
                "worst_rolling_return": float(sub["median_return"].min()),
                "beat_canonical_rate": float(
                    (sub["return_delta_vs_canonical"] > 0).mean()
                ),
                "median_return_delta_vs_canonical": float(
                    sub["return_delta_vs_canonical"].median()
                ),
                "median_rolling_max_dd": float(sub["median_max_dd"].median()),
                "median_rolling_transitions": float(
                    sub["median_transitions"].median()
                ),
                "median_rolling_holding_days": float(
                    sub["median_holding_days"].median()
                ),
            }
        )
    return detail, pd.DataFrame(summaries)


def classify_variants(
    primary_summary: pd.DataFrame,
    rolling_summary: pd.DataFrame,
) -> pd.DataFrame:
    canon = primary_summary[
        primary_summary["variant"] == CANONICAL
    ].set_index("window")
    rolling_map = rolling_summary.set_index("variant")
    rows = []

    for variant in VARIANTS:
        if variant == CANONICAL:
            label = "CANONICAL"
        else:
            sub = primary_summary[
                primary_summary["variant"] == variant
            ].set_index("window")
            mature_better = (
                sub.loc["MATURE", "median_return"]
                > canon.loc["MATURE", "median_return"]
            )
            two_y_ok = (
                sub.loc["LAST_2Y", "median_return"]
                >= canon.loc["LAST_2Y", "median_return"]
            )
            dd_ok = (
                sub.loc["MATURE", "median_max_dd"]
                >= canon.loc["MATURE", "median_max_dd"] - 0.10
            )
            rolling_ok = (
                rolling_map.loc[variant, "beat_canonical_rate"] >= 0.50
            )
            turnover_ok = (
                sub.loc["MATURE", "median_transitions"]
                <= 2.0 * canon.loc["MATURE", "median_transitions"]
            )

            if mature_better and two_y_ok and dd_ok and rolling_ok and turnover_ok:
                label = "ROBUST_IMPROVEMENT_CANDIDATE"
            elif (
                not mature_better
                and rolling_map.loc[variant, "beat_canonical_rate"] < 0.50
            ):
                label = "NOT_SUPPORTED"
            else:
                label = "MIXED"

        rows.append(
            {
                "variant": variant,
                "classification": label,
            }
        )
    return pd.DataFrame(rows)


def audit_routes(
    variant_routes: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    rows = []
    for variant, routes in variant_routes.items():
        for dt in AUDIT_DATES:
            for source in AUDIT_SOURCES:
                hit = routes[
                    (routes["signal_date"] == dt)
                    & (routes["source"] == source)
                ]
                if hit.empty:
                    rows.append(
                        {
                            "variant": variant,
                            "signal_date": dt,
                            "source": source,
                            "has_route": False,
                            "baseline_to": "",
                            "effective_to": "",
                            "override": False,
                            "strength_ratio": np.nan,
                        }
                    )
                else:
                    r = hit.iloc[0]
                    rows.append(
                        {
                            "variant": variant,
                            "signal_date": dt,
                            "source": source,
                            "has_route": True,
                            "baseline_to": str(r["baseline_to"]),
                            "effective_to": str(r["effective_to"]),
                            "override": bool(r["override"]),
                            "strength_ratio": r["strength_ratio"],
                        }
                    )
    return pd.DataFrame(rows)


def recent_link_traces(
    panel: pd.DataFrame,
    variant_routes: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    si = first_index_on_or_after(
        panel, pd.Timestamp("2026-09-28T00:00:00Z")
    )
    ei = len(panel) - 1
    pieces = []
    for variant, routes in variant_routes.items():
        result, trace = simulate(
            panel, route_map(routes), si, ei, "LINK", collect_trace=True
        )
        if trace is None or trace.empty:
            trace = pd.DataFrame(
                columns=[
                    "date", "event", "from_asset", "to_asset",
                    "value_before", "fee", "value_after", "override",
                ]
            )
        trace["variant"] = variant
        trace["final_asset"] = result["final_asset"]
        trace["final_return"] = result["return"]
        pieces.append(trace)
    return pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame()


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
    variant_routes: dict[str, pd.DataFrame] = {}
    primary_detail_parts = []
    primary_summary_parts = []
    route_stats = []

    for variant in VARIANTS:
        baseline, lookback = parse_variant(variant)
        events_by_date, states_by_date = build_monitor_history(
            panel, baseline, lookback
        )
        routes = build_routes(panel, events_by_date, states_by_date)
        routes["variant"] = variant
        variant_routes[variant] = routes

        detail, summary = evaluate_variant(panel, variant, routes)
        primary_detail_parts.append(detail)
        primary_summary_parts.append(summary)

        route_stats.append(
            {
                "variant": variant,
                "route_opportunities": int(len(routes)),
                "ddg_overrides": int(routes["override"].sum()) if len(routes) else 0,
            }
        )

    primary_detail = pd.concat(primary_detail_parts, ignore_index=True)
    primary_summary = pd.concat(primary_summary_parts, ignore_index=True)
    route_stats_df = pd.DataFrame(route_stats)

    rolling_detail, rolling_summary = evaluate_rolling(panel, variant_routes)
    classifications = classify_variants(primary_summary, rolling_summary)
    audit = audit_routes(variant_routes)
    link_trace = recent_link_traces(panel, variant_routes)

    joined = (
        primary_summary[
            primary_summary["window"] == "MATURE"
        ][
            [
                "variant", "median_return", "median_max_dd",
                "median_transitions", "median_holding_days",
                "modal_end_asset", "modal_end_asset_share",
            ]
        ]
        .merge(rolling_summary, on="variant", how="left")
        .merge(classifications, on="variant", how="left")
        .merge(route_stats_df, on="variant", how="left")
    )

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    primary_detail.to_csv(run_dir / "primary_path_detail.csv", index=False)
    primary_summary.to_csv(run_dir / "primary_summary.csv", index=False)
    rolling_detail.to_csv(run_dir / "rolling_365_detail.csv", index=False)
    rolling_summary.to_csv(run_dir / "rolling_365_summary.csv", index=False)
    classifications.to_csv(run_dir / "classifications.csv", index=False)
    audit.to_csv(run_dir / "sep28_oct01_signal_audit.csv", index=False)
    link_trace.to_csv(run_dir / "sep28_link_path_traces.csv", index=False)
    joined.to_csv(run_dir / "variant_scorecard.csv", index=False)
    route_stats_df.to_csv(run_dir / "route_stats.csv", index=False)

    for variant, routes in variant_routes.items():
        routes.to_csv(run_dir / f"routes_{variant.lower()}.csv", index=False)

    summary_json = {
        "experiment": "RR_LOOKBACK_BASELINE_FAMILY_V1",
        "workflow_mode": "STRESS_TEST_ONLY",
        "source_commit_sha": source_sha(),
        "as_of": AS_OF.isoformat(),
        "latest_closed_candle": pd.Timestamp(panel.iloc[-1]["timestamp"]).isoformat(),
        "canonical": CANONICAL,
        "variants": list(VARIANTS),
        "arm_threshold": ARM_THRESHOLD,
        "reversal": REVERSAL,
        "ddg_strength_ratio": DESTINATION_DOMINANCE_MIN_STRENGTH_RATIO,
        "transition_cost": COST,
        "scorecard": json.loads(
            joined.to_json(orient="records", date_format="iso")
        ),
        "production_changes": "NONE",
        "test_level": "GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_PATH_DEPENDENT_STRESS_TEST",
        "data_metadata": data_meta,
    }
    (run_dir / "summary.json").write_text(
        json.dumps(summary_json, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )

    lines = [
        "# RR LOOKBACK + BASELINE FAMILY V1 — RESULTS",
        "",
        "Mode: STRESS_TEST_ONLY",
        "",
        f"Canonical: {CANONICAL}",
        f"As-of: {AS_OF.isoformat()}",
        f"Latest closed D1: {pd.Timestamp(panel.iloc[-1]['timestamp']).isoformat()}",
        "",
        "## MATURE + rolling scorecard",
        "",
        "|Variant|Class|MATURE return|MATURE DD|Transitions|Median hold|Rolling beat canonical|Rolling median delta|Route opps|DDG overrides|",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]

    for _, row in joined.iterrows():
        lines.append(
            f"|{row['variant']}|{row['classification']}|"
            f"{fmt_pct(row['median_return'])}|"
            f"{fmt_pct(row['median_max_dd'])}|"
            f"{row['median_transitions']:.1f}|"
            f"{row['median_holding_days']:.1f}d|"
            f"{fmt_rate(row['beat_canonical_rate'])}|"
            f"{fmt_pct(row['median_return_delta_vs_canonical'])}|"
            f"{int(row['route_opportunities'])}|"
            f"{int(row['ddg_overrides'])}|"
        )

    lines += [
        "",
        "## Primary windows",
        "",
        "|Variant|Window|Median return|Worst return|Median DD|Transitions|Median hold|Modal end asset|",
        "|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for _, row in primary_summary.iterrows():
        lines.append(
            f"|{row['variant']}|{row['window']}|"
            f"{fmt_pct(row['median_return'])}|{fmt_pct(row['worst_return'])}|"
            f"{fmt_pct(row['median_max_dd'])}|"
            f"{row['median_transitions']:.1f}|"
            f"{row['median_holding_days']:.1f}d|"
            f"{row['modal_end_asset']} ({fmt_rate(row['modal_end_asset_share'])})|"
        )

    lines += [
        "",
        "## 2026-09-28 through 2026-10-01 route audit",
        "",
        "|Variant|Date|Source|Route?|Baseline to|Effective to|DDG?|Strength ratio|",
        "|---|---|---|---|---|---|---|---:|",
    ]
    for _, row in audit.iterrows():
        lines.append(
            f"|{row['variant']}|{pd.Timestamp(row['signal_date']).date()}|"
            f"{row['source']}|{'YES' if row['has_route'] else 'NO'}|"
            f"{row['baseline_to']}|{row['effective_to']}|"
            f"{'YES' if row['override'] else 'NO'}|"
            f"{fmt_ratio(row['strength_ratio'])}|"
        )

    lines += [
        "",
        "## Boundary",
        "",
        "- MEDIAN and SMA families were both fixed before runtime.",
        "- No additional lookback was added after results.",
        "- Production/live/Telegram/exchange behavior is unchanged.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_PATH_DEPENDENT_STRESS_TEST",
        "",
    ]

    report = "\n".join(lines)
    (run_dir / "report.md").write_text(report, encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()
