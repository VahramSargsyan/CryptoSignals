from __future__ import annotations

import argparse
import bisect
import json
import os
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd

import scripts.research_robust_exhaustive_u9_u10_atom_replacement_v1 as core


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "rr_u10_day0_price_reset_counterfactual_v1"

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


def trace_pair(
    timestamps,
    opens,
    closes,
    signal_positions,
    signal_candidates,
    assets,
    start_i,
    end_i,
    start_asset,
    day0_prices,
):
    aset = set(assets)
    current = start_asset

    baseline_qty = 100.0 / float(day0_prices[current])
    reset_qty = 100.0 / float(day0_prices[current])

    search_from = start_i
    rows = []
    route = []
    transition_index = 0
    reset_marked_points = [100.0]

    while True:
        pos_list = signal_positions[current]
        cand_list = signal_candidates[current]
        p = bisect.bisect_left(pos_list, search_from)

        found = False
        while p < len(pos_list):
            signal_i = pos_list[p]
            if signal_i >= end_i:
                break

            eligible = [
                e for e in cand_list[p]
                if e["to_asset"] in aset
            ]
            if not eligible:
                p += 1
                continue

            chosen = eligible[0]
            execute_i = signal_i + 1
            next_asset = chosen["to_asset"]

            signal_date = pd.Timestamp(timestamps[signal_i])
            execute_date = pd.Timestamp(timestamps[execute_i])

            from_real_open = float(opens[current][execute_i])
            to_real_open = float(opens[next_asset][execute_i])
            to_day0_open = float(day0_prices[next_asset])

            baseline_before = baseline_qty * from_real_open
            reset_before = reset_qty * from_real_open

            baseline_after_cost = baseline_before * (1.0 - core.COST)
            reset_after_cost = reset_before * (1.0 - core.COST)

            baseline_next_qty = baseline_after_cost / to_real_open
            reset_next_qty = reset_after_cost / to_day0_open

            baseline_marked_after_entry = baseline_next_qty * to_real_open
            reset_marked_after_entry = reset_next_qty * to_real_open

            transition_index += 1
            row = {
                "start_asset": start_asset,
                "transition_index": transition_index,
                "signal_date": signal_date,
                "execute_date": execute_date,
                "from_asset": current,
                "to_asset": next_asset,
                "pair": chosen["pair"],
                "max_dislocation": float(chosen["max_dislocation"]),
                "from_real_open": from_real_open,
                "to_real_open": to_real_open,
                "to_day0_frozen_open": to_day0_open,
                "destination_price_substitution_ratio": (
                    to_real_open / to_day0_open
                ),
                "baseline_capital_before_cost": baseline_before,
                "baseline_capital_after_cost": baseline_after_cost,
                "reset_capital_before_cost": reset_before,
                "reset_capital_after_cost": reset_after_cost,
                "baseline_destination_qty": baseline_next_qty,
                "reset_destination_qty": reset_next_qty,
                "baseline_marked_real_open_after_entry": (
                    baseline_marked_after_entry
                ),
                "reset_marked_real_open_after_entry": (
                    reset_marked_after_entry
                ),
                "reset_vs_baseline_marked_multiple": (
                    reset_marked_after_entry
                    / baseline_marked_after_entry
                ),
            }
            rows.append(row)
            route.append(
                (
                    signal_date,
                    execute_date,
                    current,
                    next_asset,
                    chosen["pair"],
                )
            )
            reset_marked_points.append(reset_marked_after_entry)

            baseline_qty = baseline_next_qty
            reset_qty = reset_next_qty
            current = next_asset
            search_from = execute_i
            found = True
            break

        if not found:
            break

    final_close = float(closes[current][end_i])
    baseline_final = baseline_qty * final_close
    reset_final = reset_qty * final_close

    reset_points_with_final = reset_marked_points + [reset_final]
    monotonic = all(
        b >= a - 1e-12
        for a, b in zip(
            reset_points_with_final[:-1],
            reset_points_with_final[1:],
        )
    )

    return {
        "start_asset": start_asset,
        "transition_count": transition_index,
        "route": route,
        "rows": rows,
        "final_asset": current,
        "baseline_final_capital": baseline_final,
        "reset_final_capital": reset_final,
        "reset_vs_baseline_final_multiple": (
            reset_final / baseline_final
        ),
        "reset_all_points_monotonic": monotonic,
        "reset_min_capital_point": float(
            np.min(reset_points_with_final)
        ),
        "reset_max_capital_point": float(
            np.max(reset_points_with_final)
        ),
    }


def canonical_route_and_final(
    timestamps,
    opens,
    closes,
    signal_positions,
    signal_candidates,
    assets,
    start_i,
    end_i,
    start_asset,
):
    aset = set(assets)
    current = start_asset
    qty = 100.0 / float(opens[current][start_i])
    search_from = start_i
    route = []
    transitions = 0

    while True:
        pos_list = signal_positions[current]
        cand_list = signal_candidates[current]
        p = bisect.bisect_left(pos_list, search_from)
        found = False

        while p < len(pos_list):
            signal_i = pos_list[p]
            if signal_i >= end_i:
                break
            eligible = [
                e for e in cand_list[p]
                if e["to_asset"] in aset
            ]
            if not eligible:
                p += 1
                continue

            chosen = eligible[0]
            execute_i = signal_i + 1
            next_asset = chosen["to_asset"]
            value_before = qty * float(opens[current][execute_i])
            qty = (
                value_before
                * (1.0 - core.COST)
                / float(opens[next_asset][execute_i])
            )
            route.append(
                (
                    pd.Timestamp(timestamps[signal_i]),
                    pd.Timestamp(timestamps[execute_i]),
                    current,
                    next_asset,
                    chosen["pair"],
                )
            )
            current = next_asset
            search_from = execute_i
            transitions += 1
            found = True
            break

        if not found:
            break

    return {
        "route": route,
        "transitions": transitions,
        "final_asset": current,
        "final_capital": qty * float(closes[current][end_i]),
    }


def pct(x: float) -> str:
    return f"{100.0 * float(x):+.2f}%"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cutoff",
        default="2026-09-26T00:00:00Z",
    )
    args = parser.parse_args()
    cutoff = core.utc(args.cutoff)
    if cutoff.normalize() != CUTOFF:
        raise RuntimeError(
            f"Expected cutoff {CUTOFF.date()}, got {cutoff.date()}"
        )

    core.POOL = U10
    panel, data_meta = core.download_panel(
        CUTOFF + pd.Timedelta(days=1)
    )
    timestamps, opens, closes = core.prepare_arrays(panel)
    signal_positions, signal_candidates = core.build_signal_index(panel)
    start_i, end_i = core.bounds(timestamps, START, CUTOFF)

    actual_start_date = pd.Timestamp(timestamps[start_i])
    day0_prices = {
        asset: float(opens[asset][start_i])
        for asset in U10
    }

    snapshot = pd.DataFrame(
        [
            {
                "snapshot_date": actual_start_date,
                "asset": asset,
                "day0_open_price_usdt": day0_prices[asset],
            }
            for asset in U10
        ]
    )

    traces = []
    ledger_rows = []
    equivalence_rows = []

    for start_asset in U10:
        trace = trace_pair(
            timestamps,
            opens,
            closes,
            signal_positions,
            signal_candidates,
            U10,
            start_i,
            end_i,
            start_asset,
            day0_prices,
        )
        canonical = canonical_route_and_final(
            timestamps,
            opens,
            closes,
            signal_positions,
            signal_candidates,
            U10,
            start_i,
            end_i,
            start_asset,
        )
        route_match = trace["route"] == canonical["route"]
        baseline_match = (
            abs(
                trace["baseline_final_capital"]
                - canonical["final_capital"]
            )
            <= TOL
        )
        final_asset_match = (
            trace["final_asset"] == canonical["final_asset"]
        )
        count_match = (
            trace["transition_count"]
            == canonical["transitions"]
        )

        equivalence_rows.append(
            {
                "start_asset": start_asset,
                "route_match": route_match,
                "transition_count_match": count_match,
                "final_asset_match": final_asset_match,
                "baseline_capital_match": baseline_match,
                "transition_count": trace["transition_count"],
                "baseline_final_capital": trace[
                    "baseline_final_capital"
                ],
                "canonical_final_capital": canonical[
                    "final_capital"
                ],
                "final_asset": trace["final_asset"],
            }
        )

        traces.append(trace)
        ledger_rows.extend(trace["rows"])

    equivalence = pd.DataFrame(equivalence_rows)
    if not (
        equivalence["route_match"].all()
        and equivalence["transition_count_match"].all()
        and equivalence["final_asset_match"].all()
        and equivalence["baseline_capital_match"].all()
    ):
        raise RuntimeError(
            "Route/baseline equivalence failed:\n"
            + equivalence.to_string(index=False)
        )

    ledger = pd.DataFrame(ledger_rows)
    start_summary = pd.DataFrame(
        [
            {
                "start_asset": t["start_asset"],
                "transition_count": t["transition_count"],
                "final_asset": t["final_asset"],
                "baseline_final_capital_usdt": t[
                    "baseline_final_capital"
                ],
                "day0_reset_final_capital_usdt": t[
                    "reset_final_capital"
                ],
                "day0_reset_vs_baseline_multiple": t[
                    "reset_vs_baseline_final_multiple"
                ],
                "day0_reset_all_points_monotonic": t[
                    "reset_all_points_monotonic"
                ],
                "day0_reset_min_capital_point": t[
                    "reset_min_capital_point"
                ],
                "day0_reset_max_capital_point": t[
                    "reset_max_capital_point"
                ],
            }
            for t in traces
        ]
    )

    median_baseline = float(
        start_summary["baseline_final_capital_usdt"].median()
    )
    median_reset = float(
        start_summary["day0_reset_final_capital_usdt"].median()
    )
    median_multiple = float(
        start_summary[
            "day0_reset_vs_baseline_multiple"
        ].median()
    )

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime(
        "%Y%m%dT%H%M%SZ"
    )
    run_dir.mkdir(parents=True, exist_ok=True)

    snapshot.to_csv(
        run_dir / "day0_price_snapshot.csv",
        index=False,
    )
    ledger.to_csv(
        run_dir / "transition_counterfactual_ledger.csv",
        index=False,
    )
    start_summary.to_csv(
        run_dir / "start_path_summary.csv",
        index=False,
    )
    equivalence.to_csv(
        run_dir / "route_equivalence.csv",
        index=False,
    )

    summary = {
        "experiment": "RR_U10_DAY0_PRICE_RESET_COUNTERFACTUAL_V1",
        "workflow_mode": "STRESS_TEST_ONLY",
        "source_commit_sha": source_sha(),
        "snapshot_date": actual_start_date.isoformat(),
        "cutoff": CUTOFF.isoformat(),
        "day0_prices": day0_prices,
        "route_equivalence_pass": True,
        "median_baseline_final_capital_usdt": median_baseline,
        "median_day0_reset_final_capital_usdt": median_reset,
        "median_day0_reset_vs_baseline_multiple": median_multiple,
        "all_start_paths_reset_monotonic": bool(
            start_summary[
                "day0_reset_all_points_monotonic"
            ].all()
        ),
        "data_metadata": data_meta,
        "production_changes": "NONE",
        "test_level": (
            "GITHUB_ACTIONS_LIVE_PUBLIC_DATA_COUNTERFACTUAL_STRESS_TEST"
        ),
    }
    (run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    lines = [
        "# RR U10 DAY-0 PRICE RESET COUNTERFACTUAL V1",
        "",
        "Mode: STRESS_TEST_ONLY",
        "",
        "This is an intentionally synthetic counterfactual, not executable P&L.",
        "",
        f"Snapshot date: {actual_start_date.date()}",
        "",
        "## Frozen day-0 prices",
        "",
        "|Asset|Day-0 open USDT|",
        "|---|---:|",
    ]
    for _, row in snapshot.iterrows():
        lines.append(
            f"|{row['asset']}|"
            f"{row['day0_open_price_usdt']:.12g}|"
        )

    lines += [
        "",
        "## Route equivalence",
        "",
        f"- starts verified: {len(equivalence)}",
        f"- identical routes: {bool(equivalence['route_match'].all())}",
        f"- identical transition counts: "
        f"{bool(equivalence['transition_count_match'].all())}",
        f"- baseline final capitals match canonical: "
        f"{bool(equivalence['baseline_capital_match'].all())}",
        "",
        "## Final capital from 100 USDT",
        "",
        "|Start|Transitions|Baseline|Day-0 reset|Reset / baseline|Reset path monotonic?|",
        "|---|---:|---:|---:|---:|---|",
    ]
    for _, row in start_summary.iterrows():
        lines.append(
            f"|{row['start_asset']}|"
            f"{int(row['transition_count'])}|"
            f"{row['baseline_final_capital_usdt']:.2f}|"
            f"{row['day0_reset_final_capital_usdt']:.6g}|"
            f"{row['day0_reset_vs_baseline_multiple']:.4g}x|"
            f"{'yes' if row['day0_reset_all_points_monotonic'] else 'no'}|"
        )

    lines += [
        "",
        "## Aggregate",
        "",
        f"- median baseline final capital: {median_baseline:.2f} USDT",
        f"- median DAY0_RESET final capital: {median_reset:.6g} USDT",
        f"- median DAY0_RESET / baseline multiple: {median_multiple:.4g}x",
        f"- all reset paths monotonic at every recorded point: "
        f"{bool(start_summary['day0_reset_all_points_monotonic'].all())}",
        "",
        "## Interpretation boundary",
        "",
        "- Every destination purchase after the start date uses that token's frozen first-day open price.",
        "- The held token still moves through its real historical prices until the next canonical rotation.",
        "- This creates deliberately impossible stale-price purchases and therefore cannot be interpreted as achievable return or arbitrage.",
        "- The route itself is unchanged; only destination purchase price is counterfactually reset.",
        "- No live/paper/Telegram/exchange behavior changed.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_COUNTERFACTUAL_STRESS_TEST",
        "",
    ]
    report = "\n".join(lines)
    (run_dir / "report.md").write_text(
        report,
        encoding="utf-8",
    )
    print(report)


if __name__ == "__main__":
    main()
