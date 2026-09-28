from __future__ import annotations

import argparse
import bisect
import hashlib
import json
import os
import subprocess
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

import scripts.research_robust_exhaustive_u9_u10_atom_replacement_v1 as core


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "rr_u10_rotation_price_snapshot_audit_v1"
TOTAL_CACHE = ROOT / "data" / "market" / "cryptocap_total_d1.csv"
TOTAL_CACHE_SHA256 = "d0c9b3bac915e484069c6302ddce1a0b050abbb8de155e4106e39cf6e519d3ee"

U10 = (
    "TWT", "PEPE", "BNB", "TRX", "AAVE",
    "AVAX", "FIL", "ALGO", "XRP", "HBAR",
)
START = pd.Timestamp("2023-10-31", tz="UTC")
CUTOFF = pd.Timestamp("2026-09-26", tz="UTC")
TOL = 1e-9


def source_sha() -> str:
    return os.environ.get("SOURCE_COMMIT_SHA") or subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def regime_group(state: str) -> str:
    if state in ("BULL_BUILDING", "FULL_BULL_ALIGNMENT"):
        return "BULL"
    if state in ("BEAR_BUILDING", "FULL_BEAR_ALIGNMENT"):
        return "BEAR"
    if state == "MIXED":
        return "MIXED"
    return "UNKNOWN"


def load_total_regime() -> pd.DataFrame:
    digest = hashlib.sha256(TOTAL_CACHE.read_bytes()).hexdigest()
    if digest != TOTAL_CACHE_SHA256:
        raise RuntimeError(f"TOTAL cache SHA mismatch: {digest}")

    frame = pd.read_csv(TOTAL_CACHE)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame["date"] = frame["timestamp"].dt.normalize()
    if "ma_state" not in frame.columns:
        raise RuntimeError("TOTAL cache missing ma_state")
    frame["regime_group"] = frame["ma_state"].map(regime_group)
    return (
        frame[["date", "ma_state", "regime_group"]]
        .drop_duplicates("date", keep="last")
        .sort_values("date")
        .reset_index(drop=True)
    )


def regime_at(regimes: pd.DataFrame, when: pd.Timestamp) -> tuple[str, str]:
    hit = regimes[regimes["date"] <= when.normalize()]
    if hit.empty:
        return "UNKNOWN", "UNKNOWN"
    row = hit.iloc[-1]
    return str(row["ma_state"]), str(row["regime_group"])


def majority_regime(
    regimes: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> str:
    sub = regimes[
        (regimes["date"] >= start.normalize())
        & (regimes["date"] <= end.normalize())
    ]
    if sub.empty:
        return "UNKNOWN"
    counts = Counter(sub["regime_group"].astype(str))
    tie_order = {"MIXED": 0, "BULL": 1, "BEAR": 2, "UNKNOWN": 3}
    return sorted(
        counts.items(),
        key=lambda kv: (-kv[1], tie_order.get(kv[0], 99), kv[0]),
    )[0][0]


def reference_final_asset(
    signal_positions,
    signal_candidates,
    assets,
    start_i,
    end_i,
    start_asset,
):
    aset = set(assets)
    current = start_asset
    search_from = start_i
    transitions = 0
    while True:
        pos_list = signal_positions[current]
        cand_list = signal_candidates[current]
        p = bisect.bisect_left(pos_list, search_from)
        while p < len(pos_list):
            signal_i = pos_list[p]
            if signal_i >= end_i:
                return current, transitions
            eligible = [
                event for event in cand_list[p]
                if event["to_asset"] in aset
            ]
            if not eligible:
                p += 1
                continue
            current = eligible[0]["to_asset"]
            search_from = signal_i + 1
            transitions += 1
            break
        else:
            return current, transitions


def trace_one(
    timestamps,
    opens,
    closes,
    signal_positions,
    signal_candidates,
    assets,
    start_i,
    end_i,
    start_asset,
    regimes,
):
    aset = set(assets)
    current = start_asset
    qty = 1.0 / float(opens[current][start_i])
    initial_equity = qty * float(closes[current][start_i])
    scale = 100.0 / initial_equity

    segment_start = start_i
    search_from = start_i
    transitions = []
    legs = []
    snapshots = []
    active_leg = None
    transition_index = 0

    def open_snapshot(execute_i: int) -> dict:
        return {asset: float(opens[asset][execute_i]) for asset in assets}

    while segment_start <= end_i:
        pos_list = signal_positions[current]
        cand_list = signal_candidates[current]
        p = bisect.bisect_left(pos_list, search_from)
        found = False

        while p < len(pos_list):
            signal_i = pos_list[p]
            if signal_i >= end_i:
                break
            eligible = [
                event for event in cand_list[p]
                if event["to_asset"] in aset
            ]
            if not eligible:
                p += 1
                continue

            chosen = eligible[0]
            execute_i = signal_i + 1
            signal_date = pd.Timestamp(timestamps[signal_i])
            execute_date = pd.Timestamp(timestamps[execute_i])

            value_before_raw = qty * float(opens[current][execute_i])
            capital_before = value_before_raw * scale
            next_asset = chosen["to_asset"]
            capital_after = capital_before * (1.0 - core.COST)
            snapshot = open_snapshot(execute_i)

            if active_leg is not None:
                entry_snapshot = active_leg["entry_snapshot"]
                token_returns = {
                    asset: snapshot[asset] / entry_snapshot[asset] - 1.0
                    for asset in assets
                }
                cross_values = np.asarray(list(token_returns.values()), dtype=float)
                held_ret = token_returns[current]
                ranks = sorted(
                    token_returns.items(),
                    key=lambda kv: (-kv[1], kv[0]),
                )
                rank_map = {
                    asset: rank + 1
                    for rank, (asset, _) in enumerate(ranks)
                }
                entry_state, entry_group = regime_at(
                    regimes, active_leg["entry_execute_date"]
                )
                exit_state, exit_group = regime_at(regimes, execute_date)

                leg = {
                    "start_asset": start_asset,
                    "leg_index": active_leg["leg_index"],
                    "completed": True,
                    "entry_signal_date": active_leg["entry_signal_date"],
                    "entry_execute_date": active_leg["entry_execute_date"],
                    "previous_asset": active_leg["previous_asset"],
                    "held_asset": current,
                    "entry_pair": active_leg["entry_pair"],
                    "entry_max_dislocation": active_leg["entry_max_dislocation"],
                    "entry_price_open": active_leg["entry_price_open"],
                    "entry_capital_after_cost_usdt": active_leg[
                        "entry_capital_after_cost_usdt"
                    ],
                    "exit_signal_date": signal_date,
                    "exit_execute_date": execute_date,
                    "next_asset": next_asset,
                    "exit_price_open": float(opens[current][execute_i]),
                    "exit_capital_before_cost_usdt": capital_before,
                    "next_entry_capital_after_cost_usdt": capital_after,
                    "holding_days": int(
                        (execute_date - active_leg["entry_execute_date"]).days
                    ),
                    "gross_holding_return": float(held_ret),
                    "net_cycle_return": float(
                        capital_after
                        / active_leg["entry_capital_after_cost_usdt"]
                        - 1.0
                    ),
                    "u10_median_return_same_interval": float(
                        np.median(cross_values)
                    ),
                    "u10_mean_return_same_interval": float(np.mean(cross_values)),
                    "held_excess_vs_u10_median": float(
                        held_ret - np.median(cross_values)
                    ),
                    "held_return_rank_1_best": int(rank_map[current]),
                    "entry_total_state": entry_state,
                    "entry_regime_group": entry_group,
                    "exit_total_state": exit_state,
                    "exit_regime_group": exit_group,
                    "majority_regime_group": majority_regime(
                        regimes,
                        active_leg["entry_execute_date"],
                        execute_date,
                    ),
                }
                for asset in assets:
                    leg[f"entry_{asset}_open"] = entry_snapshot[asset]
                    leg[f"exit_{asset}_open"] = snapshot[asset]
                    leg[f"return_{asset}"] = token_returns[asset]
                legs.append(leg)

            transition_index += 1
            transitions.append(
                {
                    "start_asset": start_asset,
                    "transition_index": transition_index,
                    "signal_date": signal_date,
                    "execute_date": execute_date,
                    "from_asset": current,
                    "to_asset": next_asset,
                    "pair": chosen["pair"],
                    "max_dislocation": float(chosen["max_dislocation"]),
                    "from_open": float(opens[current][execute_i]),
                    "to_open": float(opens[next_asset][execute_i]),
                    "capital_before_cost_usdt": capital_before,
                    "transition_cost_usdt": capital_before * core.COST,
                    "capital_after_cost_usdt": capital_after,
                }
            )

            snaprow = {
                "start_asset": start_asset,
                "transition_index": transition_index,
                "signal_date": signal_date,
                "execute_date": execute_date,
                "from_asset": current,
                "to_asset": next_asset,
            }
            for asset in assets:
                snaprow[f"{asset}_open"] = snapshot[asset]
            snapshots.append(snaprow)

            qty = (
                value_before_raw
                * (1.0 - core.COST)
                / float(opens[next_asset][execute_i])
            )
            active_leg = {
                "leg_index": transition_index,
                "entry_signal_date": signal_date,
                "entry_execute_date": execute_date,
                "previous_asset": current,
                "entry_pair": chosen["pair"],
                "entry_max_dislocation": float(chosen["max_dislocation"]),
                "entry_price_open": float(opens[next_asset][execute_i]),
                "entry_capital_after_cost_usdt": capital_after,
                "entry_snapshot": snapshot,
            }
            current = next_asset
            segment_start = execute_i
            search_from = execute_i
            found = True
            break

        if found:
            continue
        break

    final_raw = qty * float(closes[current][end_i])
    final_capital = final_raw * scale
    final_date = pd.Timestamp(timestamps[end_i])

    if active_leg is not None:
        entry_snapshot = active_leg["entry_snapshot"]
        token_returns = {
            asset: float(closes[asset][end_i]) / entry_snapshot[asset] - 1.0
            for asset in assets
        }
        arr = np.asarray(list(token_returns.values()), dtype=float)
        ranks = sorted(token_returns.items(), key=lambda kv: (-kv[1], kv[0]))
        rank_map = {
            asset: rank + 1
            for rank, (asset, _) in enumerate(ranks)
        }
        entry_state, entry_group = regime_at(
            regimes, active_leg["entry_execute_date"]
        )
        exit_state, exit_group = regime_at(regimes, final_date)

        leg = {
            "start_asset": start_asset,
            "leg_index": active_leg["leg_index"],
            "completed": False,
            "entry_signal_date": active_leg["entry_signal_date"],
            "entry_execute_date": active_leg["entry_execute_date"],
            "previous_asset": active_leg["previous_asset"],
            "held_asset": current,
            "entry_pair": active_leg["entry_pair"],
            "entry_max_dislocation": active_leg["entry_max_dislocation"],
            "entry_price_open": active_leg["entry_price_open"],
            "entry_capital_after_cost_usdt": active_leg[
                "entry_capital_after_cost_usdt"
            ],
            "exit_signal_date": pd.NaT,
            "exit_execute_date": final_date,
            "next_asset": "",
            "exit_price_open": np.nan,
            "exit_capital_before_cost_usdt": final_capital,
            "next_entry_capital_after_cost_usdt": np.nan,
            "holding_days": int(
                (final_date - active_leg["entry_execute_date"]).days
            ),
            "gross_holding_return": float(token_returns[current]),
            "net_cycle_return": np.nan,
            "u10_median_return_same_interval": float(np.median(arr)),
            "u10_mean_return_same_interval": float(np.mean(arr)),
            "held_excess_vs_u10_median": float(
                token_returns[current] - np.median(arr)
            ),
            "held_return_rank_1_best": int(rank_map[current]),
            "entry_total_state": entry_state,
            "entry_regime_group": entry_group,
            "exit_total_state": exit_state,
            "exit_regime_group": exit_group,
            "majority_regime_group": majority_regime(
                regimes, active_leg["entry_execute_date"], final_date
            ),
        }
        for asset in assets:
            leg[f"entry_{asset}_open"] = entry_snapshot[asset]
            leg[f"exit_{asset}_open"] = float(closes[asset][end_i])
            leg[f"return_{asset}"] = token_returns[asset]
        legs.append(leg)

    return {
        "start_asset": start_asset,
        "initial_capital_usdt": 100.0,
        "final_capital_usdt": float(final_capital),
        "final_asset": current,
        "transition_count": len(transitions),
        "transitions": transitions,
        "legs": legs,
        "snapshots": snapshots,
    }


def dedupe_legs(legs: pd.DataFrame) -> pd.DataFrame:
    complete = legs[legs["completed"]].copy()
    if complete.empty:
        return complete
    keycols = [
        "entry_execute_date", "held_asset",
        "exit_execute_date", "next_asset",
    ]
    rows = []
    for _, group in complete.groupby(keycols, sort=True, dropna=False):
        first = group.iloc[0].copy()
        first["shared_start_path_count"] = int(group["start_asset"].nunique())
        first["shared_start_assets"] = "|".join(
            sorted(group["start_asset"].astype(str).unique())
        )
        for col in (
            "entry_capital_after_cost_usdt",
            "exit_capital_before_cost_usdt",
            "next_entry_capital_after_cost_usdt",
        ):
            first[col] = float(group[col].median())
        rows.append(first)
    return pd.DataFrame(rows).sort_values(
        ["entry_execute_date", "held_asset", "exit_execute_date"]
    ).reset_index(drop=True)


def conditioned_product(values: pd.Series) -> float:
    arr = values.dropna().to_numpy(float)
    if len(arr) == 0:
        return np.nan
    return float(np.prod(1.0 + arr) - 1.0)


def build_start_summary(legs: pd.DataFrame, traces: list[dict]) -> pd.DataFrame:
    rows = []
    trace_map = {t["start_asset"]: t for t in traces}
    for start_asset in U10:
        sub = legs[
            (legs["start_asset"] == start_asset)
            & (legs["completed"])
        ].copy()
        t = trace_map[start_asset]
        net = sub["net_cycle_return"].dropna()
        row = {
            "start_asset": start_asset,
            "completed_rotated_legs": int(len(sub)),
            "positive_legs": int((net > 0).sum()),
            "negative_legs": int((net < 0).sum()),
            "zero_legs": int((net == 0).sum()),
            "positive_leg_rate": float((net > 0).mean()) if len(net) else np.nan,
            "all_transition_capital_monotonic": (
                bool((net >= -1e-15).all()) if len(net) else True
            ),
            "median_net_cycle_return": float(net.median()) if len(net) else np.nan,
            "final_capital_usdt": float(t["final_capital_usdt"]),
            "total_return": float(t["final_capital_usdt"] / 100.0 - 1.0),
            "final_asset": t["final_asset"],
            "transition_count": int(t["transition_count"]),
        }
        for group in ("BULL", "MIXED", "BEAR"):
            g = sub[sub["entry_regime_group"] == group]
            row[f"{group.lower()}_entry_leg_count"] = int(len(g))
            row[f"{group.lower()}_conditioned_compound"] = conditioned_product(
                g["net_cycle_return"]
            )
            row[f"{group.lower()}_positive_leg_rate"] = (
                float((g["net_cycle_return"] > 0).mean())
                if len(g) else np.nan
            )
        rows.append(row)
    return pd.DataFrame(rows)


def build_regime_summary(
    legs: pd.DataFrame,
    unique: pd.DataFrame,
    start_summary: pd.DataFrame,
) -> pd.DataFrame:
    rows = []
    complete = legs[legs["completed"]].copy()
    for group in ("BULL", "MIXED", "BEAR"):
        pooled = complete[complete["entry_regime_group"] == group]
        uniq = unique[unique["entry_regime_group"] == group]
        conditioned = start_summary[
            f"{group.lower()}_conditioned_compound"
        ].dropna()
        rows.append(
            {
                "regime_group": group,
                "pooled_start_path_leg_count": int(len(pooled)),
                "unique_leg_count": int(len(uniq)),
                "unique_positive_leg_rate": (
                    float((uniq["net_cycle_return"] > 0).mean())
                    if len(uniq) else np.nan
                ),
                "unique_median_net_cycle_return": (
                    float(uniq["net_cycle_return"].median())
                    if len(uniq) else np.nan
                ),
                "unique_median_gross_holding_return": (
                    float(uniq["gross_holding_return"].median())
                    if len(uniq) else np.nan
                ),
                "unique_median_held_rank": (
                    float(uniq["held_return_rank_1_best"].median())
                    if len(uniq) else np.nan
                ),
                "unique_median_excess_vs_u10_median": (
                    float(uniq["held_excess_vs_u10_median"].median())
                    if len(uniq) else np.nan
                ),
                "median_start_conditioned_compound": (
                    float(conditioned.median())
                    if len(conditioned) else np.nan
                ),
                "worst_start_conditioned_compound": (
                    float(conditioned.min())
                    if len(conditioned) else np.nan
                ),
                "best_start_conditioned_compound": (
                    float(conditioned.max())
                    if len(conditioned) else np.nan
                ),
                "positive_conditioned_start_rate": (
                    float((conditioned > 0).mean())
                    if len(conditioned) else np.nan
                ),
            }
        )
    return pd.DataFrame(rows)


def classify_hypothesis(
    unique: pd.DataFrame,
    start_summary: pd.DataFrame,
    regime_summary: pd.DataFrame,
) -> str:
    all_unique_positive = (
        bool((unique["net_cycle_return"] >= -1e-15).all())
        if len(unique) else False
    )
    all_starts_monotonic = bool(
        start_summary["all_transition_capital_monotonic"].all()
    )
    if all_unique_positive and all_starts_monotonic:
        return "STRICT_ALWAYS_INCREASES"

    regime_vals = regime_summary[
        "median_start_conditioned_compound"
    ].dropna()
    if len(regime_vals) == 3 and bool((regime_vals > 0).all()):
        return "NOT_STRICTLY_MONOTONIC_BUT_POSITIVE_ACROSS_REGIMES"
    if len(regime_vals) and bool((regime_vals > 0).any()):
        return "REGIME_DEPENDENT_ROTATION_EDGE"
    return "NO_ROTATION_EDGE"


def pct(x) -> str:
    return "—" if pd.isna(x) else f"{100.0 * float(x):+.1f}%"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff", default="2026-09-26T00:00:00Z")
    args = parser.parse_args()
    cutoff = core.utc(args.cutoff)
    if cutoff.normalize() != CUTOFF:
        raise RuntimeError(
            f"Expected cutoff {CUTOFF.date()}, got {cutoff.date()}"
        )

    regimes = load_total_regime()

    core.POOL = U10
    panel, data_meta = core.download_panel(CUTOFF + pd.Timedelta(days=1))
    timestamps, opens, closes = core.prepare_arrays(panel)
    signal_positions, signal_candidates = core.build_signal_index(panel)
    start_i, end_i = core.bounds(timestamps, START, CUTOFF)

    traces = []
    equivalence_rows = []
    all_legs = []
    all_transitions = []
    all_snapshots = []

    for start_asset in U10:
        trace = trace_one(
            timestamps, opens, closes,
            signal_positions, signal_candidates,
            U10, start_i, end_i, start_asset, regimes,
        )
        traces.append(trace)
        all_legs.extend(trace["legs"])
        all_transitions.extend(trace["transitions"])
        all_snapshots.extend(trace["snapshots"])

        canonical = core.simulate_one(
            opens, closes,
            signal_positions, signal_candidates,
            U10, start_i, end_i, start_asset,
            collect_equity=True,
        )
        ref_final_asset, ref_transitions = reference_final_asset(
            signal_positions, signal_candidates,
            U10, start_i, end_i, start_asset,
        )
        canonical_final = 100.0 * (1.0 + float(canonical["return"]))
        capital_diff = trace["final_capital_usdt"] - canonical_final
        equivalence_rows.append(
            {
                "start_asset": start_asset,
                "trace_transitions": trace["transition_count"],
                "canonical_transitions": int(canonical["transitions"]),
                "reference_transitions": int(ref_transitions),
                "trace_final_asset": trace["final_asset"],
                "reference_final_asset": ref_final_asset,
                "trace_final_capital_usdt": trace["final_capital_usdt"],
                "canonical_final_capital_usdt": canonical_final,
                "capital_difference_usdt": capital_diff,
                "transition_count_match": (
                    trace["transition_count"]
                    == int(canonical["transitions"])
                    == int(ref_transitions)
                ),
                "final_asset_match": trace["final_asset"] == ref_final_asset,
                "capital_match": abs(capital_diff) <= TOL,
            }
        )

    equivalence = pd.DataFrame(equivalence_rows)
    if not (
        equivalence["transition_count_match"].all()
        and equivalence["final_asset_match"].all()
        and equivalence["capital_match"].all()
    ):
        raise RuntimeError(
            "Canonical equivalence failed:\n"
            + equivalence.to_string(index=False)
        )

    legs = pd.DataFrame(all_legs)
    snapshots = pd.DataFrame(all_snapshots)
    unique = dedupe_legs(legs)
    start_summary = build_start_summary(legs, traces)
    regime_summary = build_regime_summary(
        legs, unique, start_summary
    )
    classification = classify_hypothesis(
        unique, start_summary, regime_summary
    )

    complete_unique = unique[unique["completed"]].copy()
    worst = (
        complete_unique.sort_values("net_cycle_return").iloc[0]
        if len(complete_unique) else None
    )
    best = (
        complete_unique.sort_values("net_cycle_return").iloc[-1]
        if len(complete_unique) else None
    )
    positive_unique = int((complete_unique["net_cycle_return"] > 0).sum())
    negative_unique = int((complete_unique["net_cycle_return"] < 0).sum())
    zero_unique = int((complete_unique["net_cycle_return"] == 0).sum())

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    snapshots.to_csv(run_dir / "rotation_price_snapshots.csv", index=False)
    legs.to_csv(run_dir / "rotation_legs_all_starts.csv", index=False)
    unique.to_csv(run_dir / "rotation_legs_unique.csv", index=False)
    start_summary.to_csv(run_dir / "start_path_summary.csv", index=False)
    regime_summary.to_csv(run_dir / "regime_summary.csv", index=False)
    equivalence.to_csv(run_dir / "canonical_equivalence.csv", index=False)

    summary = {
        "experiment": "RR_U10_ROTATION_PRICE_SNAPSHOT_AUDIT_V1",
        "workflow_mode": "STRESS_TEST_ONLY",
        "source_commit_sha": source_sha(),
        "cutoff": CUTOFF.isoformat(),
        "frozen_universe_id": "RR_TARGET_U10_CANDIDATE_HBAR_V1",
        "assets": list(U10),
        "cost": core.COST,
        "canonical_equivalence_pass": True,
        "classification": classification,
        "completed_unique_legs": int(len(complete_unique)),
        "positive_unique_legs": positive_unique,
        "negative_unique_legs": negative_unique,
        "zero_unique_legs": zero_unique,
        "all_start_paths_monotonic": bool(
            start_summary["all_transition_capital_monotonic"].all()
        ),
        "median_final_capital_usdt": float(
            start_summary["final_capital_usdt"].median()
        ),
        "median_total_return": float(
            start_summary["total_return"].median()
        ),
        "worst_unique_leg": None if worst is None else {
            "entry_date": pd.Timestamp(
                worst["entry_execute_date"]
            ).isoformat(),
            "held_asset": str(worst["held_asset"]),
            "next_asset": str(worst["next_asset"]),
            "net_cycle_return": float(worst["net_cycle_return"]),
            "entry_regime_group": str(worst["entry_regime_group"]),
        },
        "best_unique_leg": None if best is None else {
            "entry_date": pd.Timestamp(
                best["entry_execute_date"]
            ).isoformat(),
            "held_asset": str(best["held_asset"]),
            "next_asset": str(best["next_asset"]),
            "net_cycle_return": float(best["net_cycle_return"]),
            "entry_regime_group": str(best["entry_regime_group"]),
        },
        "regime_summary": json.loads(
            regime_summary.to_json(orient="records")
        ),
        "data_metadata": data_meta,
        "production_changes": "NONE",
        "test_level": (
            "GITHUB_ACTIONS_REPOSITORY_CACHE_PLUS_LIVE_PUBLIC_DATA_STRESS_TEST"
        ),
    }
    (run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    lines = [
        "# RR U10 ROTATION PRICE SNAPSHOT AUDIT V1",
        "",
        "Mode: STRESS_TEST_ONLY",
        "",
        f"Classification: {classification}",
        "",
        "## Canonical equivalence",
        "",
        f"- starts verified: {len(equivalence)}",
        f"- transition count match: "
        f"{bool(equivalence['transition_count_match'].all())}",
        f"- final asset match: "
        f"{bool(equivalence['final_asset_match'].all())}",
        f"- final capital match: "
        f"{bool(equivalence['capital_match'].all())}",
        "",
        "## Strict always-increases test",
        "",
        f"- unique completed rotation legs: {len(complete_unique)}",
        f"- positive unique legs: {positive_unique}",
        f"- negative unique legs: {negative_unique}",
        f"- zero unique legs: {zero_unique}",
        f"- all 10 start paths monotonic at every transition: "
        f"{bool(start_summary['all_transition_capital_monotonic'].all())}",
        f"- median final normalized capital: "
        f"{start_summary['final_capital_usdt'].median():.2f} USDT",
        f"- median total return: "
        f"{pct(start_summary['total_return'].median())}",
    ]

    if worst is not None:
        lines += [
            f"- worst unique leg: {worst['held_asset']} "
            f"{pd.Timestamp(worst['entry_execute_date']).date()} -> "
            f"{pd.Timestamp(worst['exit_execute_date']).date()} "
            f"{pct(worst['net_cycle_return'])} "
            f"(entry regime {worst['entry_regime_group']})",
            f"- best unique leg: {best['held_asset']} "
            f"{pd.Timestamp(best['entry_execute_date']).date()} -> "
            f"{pd.Timestamp(best['exit_execute_date']).date()} "
            f"{pct(best['net_cycle_return'])} "
            f"(entry regime {best['entry_regime_group']})",
        ]

    lines += [
        "",
        "## Regime summary — completed rotated legs",
        "",
        "|Entry regime|Unique legs|Positive legs|Median net leg|Median held rank|Median excess vs U10 median|Median conditioned compound across starts|Positive conditioned starts|",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in regime_summary.iterrows():
        lines.append(
            f"|{row['regime_group']}|"
            f"{int(row['unique_leg_count'])}|"
            f"{100*row['unique_positive_leg_rate']:.1f}%|"
            f"{pct(row['unique_median_net_cycle_return'])}|"
            f"{row['unique_median_held_rank']:.1f}|"
            f"{pct(row['unique_median_excess_vs_u10_median'])}|"
            f"{pct(row['median_start_conditioned_compound'])}|"
            f"{100*row['positive_conditioned_start_rate']:.1f}%|"
        )

    lines += [
        "",
        "## Start-path summary",
        "",
        "|Start|Transitions|Final capital|Total return|Positive legs|Negative legs|Monotonic|Bull conditioned|Mixed conditioned|Bear conditioned|",
        "|---|---:|---:|---:|---:|---:|---|---:|---:|---:|",
    ]
    for _, row in start_summary.iterrows():
        lines.append(
            f"|{row['start_asset']}|{int(row['transition_count'])}|"
            f"{row['final_capital_usdt']:.2f}|"
            f"{pct(row['total_return'])}|"
            f"{int(row['positive_legs'])}|"
            f"{int(row['negative_legs'])}|"
            f"{'yes' if row['all_transition_capital_monotonic'] else 'no'}|"
            f"{pct(row['bull_conditioned_compound'])}|"
            f"{pct(row['mixed_conditioned_compound'])}|"
            f"{pct(row['bear_conditioned_compound'])}|"
        )

    lines += [
        "",
        "## Interpretation boundary",
        "",
        "- A rotation exchange itself only changes normalized capital by the modeled transition cost; leg profit is measured from token entry until the next rotation.",
        "- Regime grouping is assigned from canonical TOTAL state at leg entry; majority regime is also persisted per leg.",
        "- Post-convergence duplicate paths are de-duplicated for unique-leg evidence.",
        "- This test explicitly falsifies or confirms the word always; it does not assume monotonicity.",
        "- No live/paper/Telegram/exchange behavior changed.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_REPOSITORY_CACHE_PLUS_LIVE_PUBLIC_DATA_STRESS_TEST",
        "",
    ]
    report = "\n".join(lines)
    (run_dir / "report.md").write_text(report, encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()
