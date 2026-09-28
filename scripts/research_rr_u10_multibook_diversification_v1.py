from __future__ import annotations

import argparse
import bisect
import itertools
import json
import os
import subprocess
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

import scripts.research_robust_exhaustive_u9_u10_atom_replacement_v1 as core


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "rr_u10_multibook_diversification_v1"

U10 = (
    "TWT", "PEPE", "BNB", "TRX", "AAVE",
    "AVAX", "FIL", "ALGO", "XRP", "HBAR",
)
VARIANTS = ("V2_FREE", "V3_COLLISION_GUARD")
COST = 0.001

WINDOWS = {
    "last_1y": (
        pd.Timestamp("2025-09-27", tz="UTC"),
        pd.Timestamp("2026-09-26", tz="UTC"),
    ),
    "last_2y": (
        pd.Timestamp("2024-09-27", tz="UTC"),
        pd.Timestamp("2026-09-26", tz="UTC"),
    ),
    "mature": (
        pd.Timestamp("2023-10-31", tz="UTC"),
        pd.Timestamp("2026-09-26", tz="UTC"),
    ),
}
EVAL_END = pd.Timestamp("2026-09-26", tz="UTC")


def source_sha():
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def exact_candidates(signal_positions, signal_candidates, asset, idx):
    positions = signal_positions[asset]
    p = bisect.bisect_left(positions, idx)
    if p >= len(positions) or positions[p] != idx:
        return []
    return signal_candidates[asset][p]


def option_list(candidates, current_asset):
    options = []
    seen = set()
    for rank, event in enumerate(candidates):
        target = event["to_asset"]
        if target in seen:
            continue
        seen.add(target)
        options.append(
            {
                "target": target,
                "move": True,
                "score": float(event["max_dislocation"]),
                "rank": rank,
            }
        )
    options.append(
        {
            "target": current_asset,
            "move": False,
            "score": 0.0,
            "rank": 10_000,
        }
    )
    return options


def select_v2(currents, candidate_lists):
    selected = []
    for current, candidates in zip(currents, candidate_lists):
        if candidates:
            e = candidates[0]
            selected.append(
                {
                    "target": e["to_asset"],
                    "move": True,
                    "score": float(e["max_dislocation"]),
                    "rank": 0,
                }
            )
        else:
            selected.append(
                {
                    "target": current,
                    "move": False,
                    "score": 0.0,
                    "rank": 10_000,
                }
            )
    return selected, {
        "guard_intervention": 0,
        "conflicted_books": 0,
        "alternative_moves": 0,
        "forced_stays": 0,
    }


def select_v3(currents, candidate_lists):
    option_sets = [
        option_list(candidates, current)
        for current, candidates in zip(currents, candidate_lists)
    ]
    independent = [
        options[0] if options and options[0]["move"] else options[-1]
        for options in option_sets
    ]

    best = None
    best_key = None
    for combo in itertools.product(*option_sets):
        targets = tuple(x["target"] for x in combo)
        if len(set(targets)) != 3:
            continue
        total_score = float(sum(x["score"] for x in combo))
        moves = int(sum(bool(x["move"]) for x in combo))
        key = (total_score, moves)
        if best is None or key > best_key:
            best = combo
            best_key = key

    if best is None:
        raise RuntimeError("V3 found no feasible distinct final holdings")

    selected = [dict(x) for x in best]
    conflicted = 0
    alternatives = 0
    forced = 0
    for chosen, top in zip(selected, independent):
        if chosen["target"] != top["target"]:
            conflicted += 1
            if chosen["move"]:
                alternatives += 1
            elif top["move"]:
                forced += 1

    return selected, {
        "guard_intervention": int(conflicted > 0),
        "conflicted_books": conflicted,
        "alternative_moves": alternatives,
        "forced_stays": forced,
    }


def concentration_snapshot(currents, values):
    total = float(sum(values))
    by_asset = defaultdict(float)
    for asset, value in zip(currents, values):
        by_asset[asset] += float(value)
    shares = {asset: value / total for asset, value in by_asset.items()}
    largest = max(shares.values())
    unique = len(by_asset)
    return largest, unique


def simulate(
    timestamps,
    opens,
    closes,
    signal_positions,
    signal_candidates,
    start_i,
    end_i,
    start_assets,
    variant,
):
    if len(start_assets) != 3 or len(set(start_assets)) != 3:
        raise ValueError("start_assets must contain three distinct assets")

    books = [
        {
            "asset": asset,
            "qty": (1.0 / 3.0) / float(opens[asset][start_i]),
        }
        for asset in start_assets
    ]

    pending = None
    equity = []
    largest_shares = []
    unique_counts = []
    transitions = 0
    cost_paid = 0.0
    guard_interventions = 0
    conflicted_books = 0
    alternative_moves = 0
    forced_stays = 0

    for idx in range(start_i, end_i + 1):
        if pending is not None:
            for book, choice in zip(books, pending):
                if not choice["move"]:
                    continue
                current = book["asset"]
                target = choice["target"]
                value_before = (
                    float(book["qty"]) * float(opens[current][idx])
                )
                fee = value_before * COST
                value_after = value_before - fee
                book["asset"] = target
                book["qty"] = value_after / float(opens[target][idx])
                transitions += 1
                cost_paid += fee

        currents = [book["asset"] for book in books]
        values = [
            float(book["qty"]) * float(closes[book["asset"]][idx])
            for book in books
        ]
        total = float(sum(values))
        largest, unique = concentration_snapshot(currents, values)
        equity.append(total)
        largest_shares.append(largest)
        unique_counts.append(unique)

        if idx == end_i:
            continue

        candidate_lists = [
            exact_candidates(
                signal_positions,
                signal_candidates,
                book["asset"],
                idx,
            )
            for book in books
        ]

        if variant == "V2_FREE":
            pending, diag = select_v2(currents, candidate_lists)
        elif variant == "V3_COLLISION_GUARD":
            pending, diag = select_v3(currents, candidate_lists)
        else:
            raise ValueError(variant)

        guard_interventions += diag["guard_intervention"]
        conflicted_books += diag["conflicted_books"]
        alternative_moves += diag["alternative_moves"]
        forced_stays += diag["forced_stays"]

    arr = np.asarray(equity, dtype=float)
    peaks = np.maximum.accumulate(arr)
    dd = float(np.min(arr / peaks - 1.0))
    conc = np.asarray(largest_shares, dtype=float)
    uniq = np.asarray(unique_counts, dtype=int)

    return {
        "variant": variant,
        "start_assets": "|".join(start_assets),
        "return": float(arr[-1] / arr[0] - 1.0),
        "max_dd": dd,
        "transitions": int(transitions),
        "transition_cost_paid": float(cost_paid),
        "max_largest_asset_share": float(np.max(conc)),
        "median_largest_asset_share": float(np.median(conc)),
        "days_gt_50_share": float(np.mean(conc > 0.50)),
        "days_ge_2_3_share": float(np.mean(conc >= (2.0 / 3.0 - 1e-12))),
        "days_ge_95_share": float(np.mean(conc >= 0.95)),
        "collision_day_share": float(np.mean(uniq < 3)),
        "full_convergence_day_share": float(np.mean(uniq == 1)),
        "guard_interventions": int(guard_interventions),
        "conflicted_books": int(conflicted_books),
        "alternative_moves": int(alternative_moves),
        "forced_stays": int(forced_stays),
        "end_assets": "|".join(book["asset"] for book in books),
    }


def all_triplets():
    return list(itertools.combinations(U10, 3))


def aggregate(frame):
    return {
        "triplet_count": int(len(frame)),
        "median_return": float(frame["return"].median()),
        "worst_return": float(frame["return"].min()),
        "best_return": float(frame["return"].max()),
        "positive_triplet_rate": float((frame["return"] > 0).mean()),
        "median_max_dd": float(frame["max_dd"].median()),
        "worst_max_dd": float(frame["max_dd"].min()),
        "median_max_largest_asset_share": float(
            frame["max_largest_asset_share"].median()
        ),
        "worst_max_largest_asset_share": float(
            frame["max_largest_asset_share"].max()
        ),
        "median_daily_largest_asset_share": float(
            frame["median_largest_asset_share"].median()
        ),
        "median_days_gt_50_share": float(frame["days_gt_50_share"].median()),
        "median_days_ge_2_3_share": float(frame["days_ge_2_3_share"].median()),
        "median_days_ge_95_share": float(frame["days_ge_95_share"].median()),
        "median_collision_day_share": float(frame["collision_day_share"].median()),
        "worst_collision_day_share": float(frame["collision_day_share"].max()),
        "median_full_convergence_day_share": float(
            frame["full_convergence_day_share"].median()
        ),
        "worst_full_convergence_day_share": float(
            frame["full_convergence_day_share"].max()
        ),
        "median_transitions": float(frame["transitions"].median()),
        "median_transition_cost_paid": float(
            frame["transition_cost_paid"].median()
        ),
        "median_guard_interventions": float(
            frame["guard_interventions"].median()
        ),
        "median_conflicted_books": float(frame["conflicted_books"].median()),
        "median_alternative_moves": float(frame["alternative_moves"].median()),
        "median_forced_stays": float(frame["forced_stays"].median()),
    }


def evaluate_window(
    timestamps,
    opens,
    closes,
    signal_positions,
    signal_candidates,
    start,
    end,
    label,
):
    start_i, end_i = core.bounds(timestamps, start, end)
    rows = []
    for triplet in all_triplets():
        for variant in VARIANTS:
            row = simulate(
                timestamps,
                opens,
                closes,
                signal_positions,
                signal_candidates,
                start_i,
                end_i,
                triplet,
                variant,
            )
            row["window"] = label
            row["window_start"] = pd.Timestamp(start).isoformat()
            row["window_end"] = pd.Timestamp(end).isoformat()
            rows.append(row)
    return pd.DataFrame(rows)


def monthly_windows(months):
    first = pd.Timestamp("2023-11-01", tz="UTC")
    rows = []
    for start in pd.date_range(first, EVAL_END, freq="MS", tz="UTC"):
        end = core.utc(
            start + pd.DateOffset(months=months) - pd.Timedelta(days=1)
        )
        if end <= EVAL_END:
            rows.append((core.utc(start), end))
    return rows


def rolling_evidence(
    timestamps,
    opens,
    closes,
    signal_positions,
    signal_candidates,
):
    details = []
    summaries = []
    for months in (12, 24):
        for start, end in monthly_windows(months):
            frame = evaluate_window(
                timestamps,
                opens,
                closes,
                signal_positions,
                signal_candidates,
                start,
                end,
                f"rolling_{months}m",
            )
            for variant in VARIANTS:
                sub = frame[frame["variant"] == variant].copy()
                agg = aggregate(sub)
                details.append(
                    {
                        "months": months,
                        "start": start.isoformat(),
                        "end": end.isoformat(),
                        "variant": variant,
                        **agg,
                    }
                )

    detail_df = pd.DataFrame(details)
    for months in (12, 24):
        for variant in VARIANTS:
            sub = detail_df[
                (detail_df["months"] == months)
                & (detail_df["variant"] == variant)
            ]
            summaries.append(
                {
                    "months": months,
                    "variant": variant,
                    "window_count": int(len(sub)),
                    "median_window_median_return": float(
                        sub["median_return"].median()
                    ),
                    "worst_window_median_return": float(
                        sub["median_return"].min()
                    ),
                    "positive_window_rate": float(
                        (sub["median_return"] > 0).mean()
                    ),
                    "median_window_worst_triplet_return": float(
                        sub["worst_return"].median()
                    ),
                    "worst_window_worst_triplet_return": float(
                        sub["worst_return"].min()
                    ),
                    "median_window_median_max_dd": float(
                        sub["median_max_dd"].median()
                    ),
                    "worst_window_worst_max_dd": float(
                        sub["worst_max_dd"].min()
                    ),
                    "median_window_concentration_peak": float(
                        sub["median_max_largest_asset_share"].median()
                    ),
                    "worst_window_concentration_peak": float(
                        sub["worst_max_largest_asset_share"].max()
                    ),
                    "median_window_collision_share": float(
                        sub["median_collision_day_share"].median()
                    ),
                }
            )
    return detail_df, pd.DataFrame(summaries)


def pct(value):
    return f"{100.0 * float(value):+.1f}%"


def report_primary(primary_summary):
    lines = [
        "|window|variant|median return|worst return|median DD|worst DD|median peak concentration|worst peak concentration|median collision days|median full convergence days|",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in primary_summary.iterrows():
        lines.append(
            f"|{row['window']}|{row['variant']}|"
            f"{pct(row['median_return'])}|{pct(row['worst_return'])}|"
            f"{pct(row['median_max_dd'])}|{pct(row['worst_max_dd'])}|"
            f"{100*row['median_max_largest_asset_share']:.1f}%|"
            f"{100*row['worst_max_largest_asset_share']:.1f}%|"
            f"{100*row['median_collision_day_share']:.1f}%|"
            f"{100*row['median_full_convergence_day_share']:.1f}%|"
        )
    return "\n".join(lines)


def report_rolling(rolling_summary):
    lines = [
        "|months|variant|windows|median window return|worst median-return window|positive windows|median concentration peak|worst concentration peak|median collision days|",
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in rolling_summary.iterrows():
        lines.append(
            f"|{int(row['months'])}|{row['variant']}|{int(row['window_count'])}|"
            f"{pct(row['median_window_median_return'])}|"
            f"{pct(row['worst_window_median_return'])}|"
            f"{100*row['positive_window_rate']:.1f}%|"
            f"{100*row['median_window_concentration_peak']:.1f}%|"
            f"{100*row['worst_window_concentration_peak']:.1f}%|"
            f"{100*row['median_window_collision_share']:.1f}%|"
        )
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff", default="2026-09-27T00:00:00Z")
    args = parser.parse_args()

    core.POOL = U10
    panel, data_meta = core.download_panel(core.utc(args.cutoff))
    timestamps, opens, closes = core.prepare_arrays(panel)
    signal_positions, signal_candidates = core.build_signal_index(panel)

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    primary_frames = []
    for label, (start, end) in WINDOWS.items():
        primary_frames.append(
            evaluate_window(
                timestamps,
                opens,
                closes,
                signal_positions,
                signal_candidates,
                start,
                end,
                label,
            )
        )
    primary = pd.concat(primary_frames, ignore_index=True)
    primary.to_csv(run_dir / "primary_triplet_results.csv", index=False)

    summary_rows = []
    for label in WINDOWS:
        for variant in VARIANTS:
            sub = primary[
                (primary["window"] == label)
                & (primary["variant"] == variant)
            ]
            summary_rows.append(
                {"window": label, "variant": variant, **aggregate(sub)}
            )
    primary_summary = pd.DataFrame(summary_rows)
    primary_summary.to_csv(run_dir / "primary_summary.csv", index=False)

    rolling_detail, rolling_summary = rolling_evidence(
        timestamps,
        opens,
        closes,
        signal_positions,
        signal_candidates,
    )
    rolling_detail.to_csv(run_dir / "rolling_detail.csv", index=False)
    rolling_summary.to_csv(run_dir / "rolling_summary.csv", index=False)

    comparison = []
    for label in WINDOWS:
        free = primary_summary[
            (primary_summary["window"] == label)
            & (primary_summary["variant"] == "V2_FREE")
        ].iloc[0]
        guard = primary_summary[
            (primary_summary["window"] == label)
            & (primary_summary["variant"] == "V3_COLLISION_GUARD")
        ].iloc[0]
        comparison.append(
            {
                "window": label,
                "return_delta_v3_minus_v2": float(
                    guard["median_return"] - free["median_return"]
                ),
                "median_dd_delta_v3_minus_v2": float(
                    guard["median_max_dd"] - free["median_max_dd"]
                ),
                "peak_concentration_delta_v3_minus_v2": float(
                    guard["median_max_largest_asset_share"]
                    - free["median_max_largest_asset_share"]
                ),
                "collision_share_delta_v3_minus_v2": float(
                    guard["median_collision_day_share"]
                    - free["median_collision_day_share"]
                ),
                "transition_delta_v3_minus_v2": float(
                    guard["median_transitions"] - free["median_transitions"]
                ),
            }
        )
    comparison_df = pd.DataFrame(comparison)
    comparison_df.to_csv(run_dir / "v3_minus_v2_comparison.csv", index=False)

    summary = {
        "experiment": "RR_U10_MULTIBOOK_DIVERSIFICATION_V1",
        "workflow_mode": "STRESS_TEST_ONLY",
        "source_commit_sha": source_sha(),
        "cutoff": core.utc(args.cutoff).isoformat(),
        "frozen_universe_id": "RR_TARGET_U10_CANDIDATE_HBAR_V1",
        "assets": list(U10),
        "triplets": len(all_triplets()),
        "parameters": {
            "books": 3,
            "equal_initial_allocation": True,
            "lookback": core.LOOKBACK,
            "arm_threshold": core.ARM,
            "reversal": core.REVERSAL,
            "transition_cost": COST,
            "execution": "confirmed T -> next-day open",
            "v2": "independent books; asset collisions allowed",
            "v3": "independent books; final holdings must be distinct; maximize total max_dislocation",
        },
        "data_metadata": data_meta,
        "common_panel": {
            "rows": int(len(panel)),
            "start": timestamps[0].isoformat(),
            "end": timestamps[-1].isoformat(),
        },
        "primary_summary": json.loads(
            primary_summary.to_json(orient="records")
        ),
        "rolling_summary": json.loads(
            rolling_summary.to_json(orient="records")
        ),
        "comparison": json.loads(
            comparison_df.to_json(orient="records")
        ),
        "production_changes": "NONE",
        "test_level": "GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    }
    (run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    report = [
        "# RR U10 MULTIBOOK DIVERSIFICATION V1",
        "",
        "Mode: STRESS_TEST_ONLY",
        "",
        f"Frozen universe: RR_TARGET_U10_CANDIDATE_HBAR_V1 ({', '.join(U10)})",
        "",
        "Start-state coverage: all 120 distinct 3-of-10 triplets.",
        "",
        "## Primary windows",
        "",
        report_primary(primary_summary),
        "",
        "## Rolling checks",
        "",
        report_rolling(rolling_summary),
        "",
        "## Safety",
        "",
        "- Production/live strategy: unchanged.",
        "- Telegram behavior: unchanged.",
        "- No cross-book daily rebalancing was introduced.",
        "- V3 controls deliberate book convergence, not passive value drift of one outperforming book.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
        "",
    ]
    text = "\n".join(report)
    (run_dir / "report.md").write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
