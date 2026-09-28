from __future__ import annotations

import argparse
import bisect
import itertools
import json
import math
import os
import subprocess
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

import scripts.research_robust_exhaustive_u9_u10_atom_replacement_v1 as core
import scripts.research_rr_u10_multibook_diversification_v1 as prior


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "rr_u10_multibook_max2_shadow_fallback_v1"

U10 = (
    "TWT", "PEPE", "BNB", "TRX", "AAVE",
    "AVAX", "FIL", "ALGO", "XRP", "HBAR",
)
ASSET_RANK = {asset: idx for idx, asset in enumerate(U10)}
COST = 0.001
VOL_LOOKBACK = 30
MAX_BOOKS_PER_ASSET = 2

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


def source_sha() -> str:
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


def build_volatility(panel: pd.DataFrame) -> dict[str, np.ndarray]:
    frame = panel.set_index("timestamp")
    out: dict[str, np.ndarray] = {}
    for asset in U10:
        close = frame[f"{asset}_close"].astype(float)
        ret = close.pct_change(fill_method=None)
        vol = ret.rolling(VOL_LOOKBACK, min_periods=VOL_LOOKBACK).std()
        out[asset] = vol.to_numpy(dtype=float)
    return out


def vol_at_open(volatility, asset: str, open_idx: int) -> float:
    source_idx = open_idx - 1
    if source_idx < 0:
        return math.inf
    value = float(volatility[asset][source_idx])
    return value if math.isfinite(value) else math.inf


def physical_assignment_max2(books, volatility, open_idx):
    """
    Allow at most two physical books per token.

    For a shadow target wanted by all three books, allocate the two slots to:
    1) current physical residents,
    2) strongest freshly confirmed shadow transitions,
    3) deterministic book index.

    The blocked third book keeps an already-frozen defensive token if feasible;
    otherwise it takes the lowest causal 30d-volatility token with occupancy < 2.
    """
    desired_groups: dict[str, list[int]] = {}
    for idx, book in enumerate(books):
        desired_groups.setdefault(book["shadow"], []).append(idx)

    assignments: list[dict | None] = [None, None, None]
    occupancy = Counter()
    losers: list[int] = []

    for target, members in desired_groups.items():
        def priority(idx):
            book = books[idx]
            resident = int(book["actual"] == target)
            fresh_strength = (
                float(book.get("shadow_score", 0.0))
                if book.get("shadow_transitioned", False)
                else 0.0
            )
            return (resident, fresh_strength, -idx)

        ordered = sorted(members, key=priority, reverse=True)
        winners = ordered[:MAX_BOOKS_PER_ASSET]
        losers.extend(ordered[MAX_BOOKS_PER_ASSET:])

        for idx in winners:
            assignments[idx] = {
                "target": target,
                "kind": "sync",
                "vol": 0.0,
            }
            occupancy[target] += 1

    # Preserve an already-frozen defensive token if it still has a free slot.
    unresolved = []
    for idx in losers:
        book = books[idx]
        actual = book["actual"]
        desired = book["shadow"]
        if (
            book.get("prev_parked", False)
            and actual != desired
            and occupancy[actual] < MAX_BOOKS_PER_ASSET
        ):
            assignments[idx] = {
                "target": actual,
                "kind": "park_stay",
                "vol": vol_at_open(volatility, actual, open_idx),
            }
            occupancy[actual] += 1
        else:
            unresolved.append(idx)

    # New parking uses the lowest-vol feasible token.
    for idx in unresolved:
        ranked = sorted(
            U10,
            key=lambda asset: (
                vol_at_open(volatility, asset, open_idx),
                ASSET_RANK[asset],
            ),
        )
        chosen = next(
            (
                asset for asset in ranked
                if occupancy[asset] < MAX_BOOKS_PER_ASSET
            ),
            None,
        )
        if chosen is None:
            raise RuntimeError("No feasible U10 parking token under max-2 cap")
        assignments[idx] = {
            "target": chosen,
            "kind": "park_new",
            "vol": vol_at_open(volatility, chosen, open_idx),
        }
        occupancy[chosen] += 1

    if any(row is None for row in assignments):
        raise RuntimeError(f"Incomplete physical assignment: {assignments}")

    targets = [row["target"] for row in assignments]
    counts = Counter(targets)
    if max(counts.values()) > MAX_BOOKS_PER_ASSET:
        raise RuntimeError(f"MAX2 violation: {targets}")
    return assignments


def concentration_snapshot(books, closes, idx):
    by_asset = Counter()
    for book in books:
        value = float(book["qty"]) * float(closes[book["actual"]][idx])
        by_asset[book["actual"]] += value
    total = float(sum(by_asset.values()))
    largest = max(by_asset.values()) / total
    return total, float(largest)


def simulate_max2_shadow(
    timestamps,
    opens,
    closes,
    signal_positions,
    signal_candidates,
    volatility,
    start_i,
    end_i,
    start_assets,
):
    if len(start_assets) != 3 or len(set(start_assets)) != 3:
        raise ValueError("start_assets must be three distinct U10 assets")

    books = [
        {
            "shadow": asset,
            "actual": asset,
            "qty": (1.0 / 3.0) / float(opens[asset][start_i]),
            "shadow_score": 0.0,
            "shadow_transitioned": False,
            "prev_parked": False,
        }
        for asset in start_assets
    ]

    pending_shadow = [None, None, None]

    equity = []
    largest_shares = []
    actual_two_plus_one = []
    actual_all_distinct = []
    actual_full_convergence = []
    shadow_collision = []
    shadow_full_convergence = []
    any_mismatch = []

    mismatch_book_days = 0
    physical_transitions = 0
    shadow_transitions = 0
    actual_cost_paid = 0.0
    parking_entries = 0
    resyncs = 0
    defensive_displacements = 0
    parking_book_days = 0
    trx_parking_book_days = 0
    parking_token_days = Counter()
    parking_entry_tokens = Counter()

    for idx in range(start_i, end_i + 1):
        prev_parked_flags = [
            book["actual"] != book["shadow"] for book in books
        ]
        old_actual = [book["actual"] for book in books]

        # Shadow core advances independently of physical parking.
        for j, (book, event) in enumerate(zip(books, pending_shadow)):
            book["prev_parked"] = prev_parked_flags[j]
            book["shadow_transitioned"] = False
            book["shadow_score"] = 0.0
            if event is not None:
                book["shadow"] = str(event["to_asset"])
                book["shadow_score"] = float(event["max_dislocation"])
                book["shadow_transitioned"] = True
                shadow_transitions += 1

        if idx != start_i:
            choices = physical_assignment_max2(books, volatility, idx)

            for j, (book, choice) in enumerate(zip(books, choices)):
                before_actual = old_actual[j]
                was_parked = prev_parked_flags[j]
                target = choice["target"]
                new_shadow = book["shadow"]

                if target != before_actual:
                    value_before = (
                        float(book["qty"]) * float(opens[before_actual][idx])
                    )
                    fee = value_before * COST
                    value_after = value_before - fee
                    book["actual"] = target
                    book["qty"] = value_after / float(opens[target][idx])
                    physical_transitions += 1
                    actual_cost_paid += fee

                now_parked = book["actual"] != new_shadow
                if not was_parked and now_parked:
                    parking_entries += 1
                    parking_entry_tokens[book["actual"]] += 1
                elif was_parked and not now_parked:
                    resyncs += 1
                elif (
                    was_parked
                    and now_parked
                    and book["actual"] != before_actual
                ):
                    defensive_displacements += 1
                    parking_entry_tokens[book["actual"]] += 1

        total, largest = concentration_snapshot(books, closes, idx)
        equity.append(total)
        largest_shares.append(largest)

        actuals = [book["actual"] for book in books]
        shadows = [book["shadow"] for book in books]
        actual_counts = Counter(actuals)
        shadow_counts = Counter(shadows)

        max_actual = max(actual_counts.values())
        max_shadow = max(shadow_counts.values())
        actual_full_convergence.append(max_actual == 3)
        actual_two_plus_one.append(max_actual == 2)
        actual_all_distinct.append(max_actual == 1)
        shadow_collision.append(max_shadow >= 2)
        shadow_full_convergence.append(max_shadow == 3)

        mismatch_flags = [
            book["actual"] != book["shadow"] for book in books
        ]
        any_mismatch.append(any(mismatch_flags))
        mismatch_book_days += sum(mismatch_flags)

        for book, mismatch in zip(books, mismatch_flags):
            if mismatch:
                parking_book_days += 1
                parking_token_days[book["actual"]] += 1
                if book["actual"] == "TRX":
                    trx_parking_book_days += 1

        if idx == end_i:
            continue

        next_pending = []
        for book in books:
            candidates = exact_candidates(
                signal_positions,
                signal_candidates,
                book["shadow"],
                idx,
            )
            next_pending.append(candidates[0] if candidates else None)
        pending_shadow = next_pending

    arr = np.asarray(equity, dtype=float)
    peaks = np.maximum.accumulate(arr)
    dd = float(np.min(arr / peaks - 1.0))
    shares = np.asarray(largest_shares, dtype=float)
    days = len(arr)
    parking_total = sum(parking_token_days.values())

    return {
        "variant": "V4_MAX2_SHADOW_FALLBACK",
        "start_assets": "|".join(start_assets),
        "return": float(arr[-1] / arr[0] - 1.0),
        "max_dd": dd,
        "physical_transitions": int(physical_transitions),
        "shadow_transitions": int(shadow_transitions),
        "transition_cost_paid": float(actual_cost_paid),
        "max_largest_asset_share": float(np.max(shares)),
        "median_largest_asset_share": float(np.median(shares)),
        "days_gt_50_share": float(np.mean(shares > 0.50)),
        "days_ge_2_3_share": float(np.mean(shares >= (2.0 / 3.0 - 1e-12))),
        "actual_full_convergence_day_share": float(
            np.mean(actual_full_convergence)
        ),
        "actual_two_plus_one_day_share": float(np.mean(actual_two_plus_one)),
        "actual_all_distinct_day_share": float(np.mean(actual_all_distinct)),
        "shadow_collision_day_share": float(np.mean(shadow_collision)),
        "shadow_full_convergence_day_share": float(
            np.mean(shadow_full_convergence)
        ),
        "any_mismatch_day_share": float(np.mean(any_mismatch)),
        "mismatch_book_day_share": float(
            mismatch_book_days / (days * 3)
        ),
        "parking_entries": int(parking_entries),
        "resyncs": int(resyncs),
        "defensive_displacements": int(defensive_displacements),
        "parking_book_days": int(parking_book_days),
        "trx_parking_book_days": int(trx_parking_book_days),
        "trx_parking_share": (
            float(trx_parking_book_days / parking_total)
            if parking_total else 0.0
        ),
        "parking_entry_tokens_json": json.dumps(
            dict(sorted(parking_entry_tokens.items())), sort_keys=True
        ),
        "parking_token_days_json": json.dumps(
            dict(sorted(parking_token_days.items())), sort_keys=True
        ),
        "end_actual_assets": "|".join(book["actual"] for book in books),
        "end_shadow_assets": "|".join(book["shadow"] for book in books),
    }


def simulate_v2(
    timestamps,
    opens,
    closes,
    signal_positions,
    signal_candidates,
    start_i,
    end_i,
    start_assets,
):
    raw = prior.simulate(
        timestamps,
        opens,
        closes,
        signal_positions,
        signal_candidates,
        start_i,
        end_i,
        start_assets,
        "V2_FREE",
    )
    return {
        "variant": "V2_FREE",
        "start_assets": raw["start_assets"],
        "return": raw["return"],
        "max_dd": raw["max_dd"],
        "physical_transitions": raw["transitions"],
        "shadow_transitions": raw["transitions"],
        "transition_cost_paid": raw["transition_cost_paid"],
        "max_largest_asset_share": raw["max_largest_asset_share"],
        "median_largest_asset_share": raw["median_largest_asset_share"],
        "days_gt_50_share": raw["days_gt_50_share"],
        "days_ge_2_3_share": raw["days_ge_2_3_share"],
        "actual_full_convergence_day_share": raw["full_convergence_day_share"],
        "actual_two_plus_one_day_share": float(
            raw["collision_day_share"] - raw["full_convergence_day_share"]
        ),
        "actual_all_distinct_day_share": float(
            1.0 - raw["collision_day_share"]
        ),
        "shadow_collision_day_share": raw["collision_day_share"],
        "shadow_full_convergence_day_share": raw["full_convergence_day_share"],
        "any_mismatch_day_share": 0.0,
        "mismatch_book_day_share": 0.0,
        "parking_entries": 0,
        "resyncs": 0,
        "defensive_displacements": 0,
        "parking_book_days": 0,
        "trx_parking_book_days": 0,
        "trx_parking_share": 0.0,
        "parking_entry_tokens_json": "{}",
        "parking_token_days_json": "{}",
        "end_actual_assets": raw["end_assets"],
        "end_shadow_assets": raw["end_assets"],
    }


def triplets():
    return list(itertools.combinations(U10, 3))


def aggregate(frame: pd.DataFrame) -> dict:
    return {
        "triplet_count": int(len(frame)),
        "median_return": float(frame["return"].median()),
        "worst_return": float(frame["return"].min()),
        "best_return": float(frame["return"].max()),
        "positive_triplet_rate": float((frame["return"] > 0).mean()),
        "median_max_dd": float(frame["max_dd"].median()),
        "worst_max_dd": float(frame["max_dd"].min()),
        "median_peak_concentration": float(
            frame["max_largest_asset_share"].median()
        ),
        "worst_peak_concentration": float(
            frame["max_largest_asset_share"].max()
        ),
        "median_daily_largest_share": float(
            frame["median_largest_asset_share"].median()
        ),
        "median_days_gt_50_share": float(
            frame["days_gt_50_share"].median()
        ),
        "median_days_ge_2_3_share": float(
            frame["days_ge_2_3_share"].median()
        ),
        "median_full_convergence_share": float(
            frame["actual_full_convergence_day_share"].median()
        ),
        "median_two_plus_one_share": float(
            frame["actual_two_plus_one_day_share"].median()
        ),
        "median_all_distinct_share": float(
            frame["actual_all_distinct_day_share"].median()
        ),
        "median_shadow_collision_share": float(
            frame["shadow_collision_day_share"].median()
        ),
        "median_shadow_full_convergence_share": float(
            frame["shadow_full_convergence_day_share"].median()
        ),
        "median_any_mismatch_share": float(
            frame["any_mismatch_day_share"].median()
        ),
        "median_mismatch_book_day_share": float(
            frame["mismatch_book_day_share"].median()
        ),
        "median_physical_transitions": float(
            frame["physical_transitions"].median()
        ),
        "median_shadow_transitions": float(
            frame["shadow_transitions"].median()
        ),
        "median_cost_paid": float(frame["transition_cost_paid"].median()),
        "median_parking_entries": float(frame["parking_entries"].median()),
        "median_resyncs": float(frame["resyncs"].median()),
        "median_defensive_displacements": float(
            frame["defensive_displacements"].median()
        ),
        "median_parking_book_days": float(
            frame["parking_book_days"].median()
        ),
        "median_trx_parking_share": float(
            frame["trx_parking_share"].median()
        ),
    }


def evaluate_window(
    timestamps,
    opens,
    closes,
    signal_positions,
    signal_candidates,
    volatility,
    start,
    end,
    label,
):
    start_i, end_i = core.bounds(timestamps, start, end)
    rows = []
    for start_assets in triplets():
        control = simulate_v2(
            timestamps,
            opens,
            closes,
            signal_positions,
            signal_candidates,
            start_i,
            end_i,
            start_assets,
        )
        max2 = simulate_max2_shadow(
            timestamps,
            opens,
            closes,
            signal_positions,
            signal_candidates,
            volatility,
            start_i,
            end_i,
            start_assets,
        )
        for row in (control, max2):
            row["window"] = label
            row["window_start"] = pd.Timestamp(start).isoformat()
            row["window_end"] = pd.Timestamp(end).isoformat()
            rows.append(row)
    return pd.DataFrame(rows)


def monthly_windows(months: int):
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
    volatility,
):
    rows = []
    for months in (12, 24):
        for start, end in monthly_windows(months):
            frame = evaluate_window(
                timestamps,
                opens,
                closes,
                signal_positions,
                signal_candidates,
                volatility,
                start,
                end,
                f"rolling_{months}m",
            )
            for variant in ("V2_FREE", "V4_MAX2_SHADOW_FALLBACK"):
                sub = frame[frame["variant"] == variant].copy()
                rows.append(
                    {
                        "months": months,
                        "start": start.isoformat(),
                        "end": end.isoformat(),
                        "variant": variant,
                        **aggregate(sub),
                    }
                )

    detail = pd.DataFrame(rows)
    summary_rows = []
    for months in (12, 24):
        for variant in ("V2_FREE", "V4_MAX2_SHADOW_FALLBACK"):
            sub = detail[
                (detail["months"] == months)
                & (detail["variant"] == variant)
            ]
            summary_rows.append(
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
                    "median_window_median_dd": float(
                        sub["median_max_dd"].median()
                    ),
                    "worst_window_worst_dd": float(
                        sub["worst_max_dd"].min()
                    ),
                    "median_window_peak_concentration": float(
                        sub["median_peak_concentration"].median()
                    ),
                    "worst_window_peak_concentration": float(
                        sub["worst_peak_concentration"].max()
                    ),
                    "median_window_days_gt_50_share": float(
                        sub["median_days_gt_50_share"].median()
                    ),
                    "median_window_mismatch_share": float(
                        sub["median_any_mismatch_share"].median()
                    ),
                    "median_window_trx_parking_share": float(
                        sub["median_trx_parking_share"].median()
                    ),
                }
            )
    return detail, pd.DataFrame(summary_rows)


def parking_totals(frame: pd.DataFrame, field: str) -> Counter:
    total = Counter()
    for raw in frame[field]:
        total.update(json.loads(raw))
    return total


def pct(value) -> str:
    return f"{100.0 * float(value):+.1f}%"


def primary_table(frame: pd.DataFrame) -> str:
    lines = [
        "|window|variant|median return|worst return|median DD|worst DD|median peak conc|median daily largest|days >50%|days >=2/3|full 3-in-1|2+1 days|1+1+1 days|mismatch days|TRX parking|",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in frame.iterrows():
        lines.append(
            f"|{row['window']}|{row['variant']}|"
            f"{pct(row['median_return'])}|{pct(row['worst_return'])}|"
            f"{pct(row['median_max_dd'])}|{pct(row['worst_max_dd'])}|"
            f"{100*row['median_peak_concentration']:.1f}%|"
            f"{100*row['median_daily_largest_share']:.1f}%|"
            f"{100*row['median_days_gt_50_share']:.1f}%|"
            f"{100*row['median_days_ge_2_3_share']:.1f}%|"
            f"{100*row['median_full_convergence_share']:.1f}%|"
            f"{100*row['median_two_plus_one_share']:.1f}%|"
            f"{100*row['median_all_distinct_share']:.1f}%|"
            f"{100*row['median_any_mismatch_share']:.1f}%|"
            f"{100*row['median_trx_parking_share']:.1f}%|"
        )
    return "\n".join(lines)


def rolling_table(frame: pd.DataFrame) -> str:
    lines = [
        "|months|variant|windows|median window return|worst median window|positive windows|median DD|peak conc|days >50%|mismatch days|TRX parking|",
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in frame.iterrows():
        lines.append(
            f"|{int(row['months'])}|{row['variant']}|"
            f"{int(row['window_count'])}|"
            f"{pct(row['median_window_median_return'])}|"
            f"{pct(row['worst_window_median_return'])}|"
            f"{100*row['positive_window_rate']:.1f}%|"
            f"{pct(row['median_window_median_dd'])}|"
            f"{100*row['median_window_peak_concentration']:.1f}%|"
            f"{100*row['median_window_days_gt_50_share']:.1f}%|"
            f"{100*row['median_window_mismatch_share']:.1f}%|"
            f"{100*row['median_window_trx_parking_share']:.1f}%|"
        )
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff", default="2026-09-27T00:00:00Z")
    args = parser.parse_args()

    core.POOL = U10
    prior.U10 = U10

    panel, data_meta = core.download_panel(core.utc(args.cutoff))
    timestamps, opens, closes = core.prepare_arrays(panel)
    signal_positions, signal_candidates = core.build_signal_index(panel)
    volatility = build_volatility(panel)

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    primary_parts = []
    for label, (start, end) in WINDOWS.items():
        primary_parts.append(
            evaluate_window(
                timestamps,
                opens,
                closes,
                signal_positions,
                signal_candidates,
                volatility,
                start,
                end,
                label,
            )
        )
    primary = pd.concat(primary_parts, ignore_index=True)
    primary.to_csv(run_dir / "primary_triplet_results.csv", index=False)

    summary_rows = []
    for label in WINDOWS:
        for variant in ("V2_FREE", "V4_MAX2_SHADOW_FALLBACK"):
            sub = primary[
                (primary["window"] == label)
                & (primary["variant"] == variant)
            ]
            summary_rows.append(
                {"window": label, "variant": variant, **aggregate(sub)}
            )
    primary_summary = pd.DataFrame(summary_rows)
    primary_summary.to_csv(run_dir / "primary_summary.csv", index=False)

    parking_rows = []
    for label in WINDOWS:
        sub = primary[
            (primary["window"] == label)
            & (primary["variant"] == "V4_MAX2_SHADOW_FALLBACK")
        ]
        entries = parking_totals(sub, "parking_entry_tokens_json")
        days = parking_totals(sub, "parking_token_days_json")
        total_entries = sum(entries.values())
        total_days = sum(days.values())
        for asset in U10:
            parking_rows.append(
                {
                    "window": label,
                    "asset": asset,
                    "parking_entries": int(entries.get(asset, 0)),
                    "parking_entry_share": (
                        float(entries.get(asset, 0) / total_entries)
                        if total_entries else 0.0
                    ),
                    "parking_book_days": int(days.get(asset, 0)),
                    "parking_day_share": (
                        float(days.get(asset, 0) / total_days)
                        if total_days else 0.0
                    ),
                }
            )
    parking = pd.DataFrame(parking_rows)
    parking.to_csv(run_dir / "parking_token_distribution.csv", index=False)

    rolling_detail, rolling_summary = rolling_evidence(
        timestamps,
        opens,
        closes,
        signal_positions,
        signal_candidates,
        volatility,
    )
    rolling_detail.to_csv(run_dir / "rolling_detail.csv", index=False)
    rolling_summary.to_csv(run_dir / "rolling_summary.csv", index=False)

    comparison_rows = []
    for label in WINDOWS:
        v2 = primary_summary[
            (primary_summary["window"] == label)
            & (primary_summary["variant"] == "V2_FREE")
        ].iloc[0]
        v4 = primary_summary[
            (primary_summary["window"] == label)
            & (primary_summary["variant"] == "V4_MAX2_SHADOW_FALLBACK")
        ].iloc[0]
        comparison_rows.append(
            {
                "window": label,
                "return_delta_v4_minus_v2": float(
                    v4["median_return"] - v2["median_return"]
                ),
                "dd_delta_v4_minus_v2": float(
                    v4["median_max_dd"] - v2["median_max_dd"]
                ),
                "peak_concentration_delta_v4_minus_v2": float(
                    v4["median_peak_concentration"]
                    - v2["median_peak_concentration"]
                ),
            }
        )
    comparison = pd.DataFrame(comparison_rows)
    comparison.to_csv(run_dir / "v4_max2_minus_v2.csv", index=False)

    summary = {
        "experiment": "RR_U10_MULTIBOOK_MAX2_SHADOW_FALLBACK_V1",
        "workflow_mode": "STRESS_TEST_ONLY",
        "source_commit_sha": source_sha(),
        "cutoff": core.utc(args.cutoff).isoformat(),
        "frozen_universe_id": "RR_TARGET_U10_CANDIDATE_HBAR_V1",
        "assets": list(U10),
        "triplets": len(triplets()),
        "parameters": {
            "books": 3,
            "max_books_per_actual_asset": MAX_BOOKS_PER_ASSET,
            "equal_initial_allocation": True,
            "lookback": core.LOOKBACK,
            "arm_threshold": core.ARM,
            "reversal": core.REVERSAL,
            "transition_cost": COST,
            "defensive_vol_lookback": VOL_LOOKBACK,
            "shadow_core_continues_while_parked": True,
            "parking_token": "lowest causal 30d realized-vol feasible U10 token",
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
            comparison.to_json(orient="records")
        ),
        "production_changes": "NONE",
        "test_level": "GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    }
    (run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    report = [
        "# RR U10 MULTIBOOK MAX2 SHADOW FALLBACK V1",
        "",
        "Mode: STRESS_TEST_ONLY",
        "",
        "Frozen universe: RR_TARGET_U10_CANDIDATE_HBAR_V1",
        "",
        "Rule: at most 2 of 3 physical books per asset.",
        "",
        "All 120 distinct 3-of-10 starting triplets.",
        "",
        "## Primary",
        "",
        primary_table(primary_summary),
        "",
        "## Rolling",
        "",
        rolling_table(rolling_summary),
        "",
        "## Parking-token distribution — MATURE",
        "",
    ]

    mature_parking = parking[parking["window"] == "mature"].sort_values(
        ["parking_day_share", "parking_entry_share"],
        ascending=False,
    )
    report += [
        "|asset|parking entries|entry share|parking book-days|day share|",
        "|---|---:|---:|---:|---:|",
    ]
    for _, row in mature_parking.iterrows():
        report.append(
            f"|{row['asset']}|{int(row['parking_entries'])}|"
            f"{100*row['parking_entry_share']:.1f}%|"
            f"{int(row['parking_book_days'])}|"
            f"{100*row['parking_day_share']:.1f}%|"
        )

    report += [
        "",
        "## Safety",
        "",
        "- 3-in-1 physical convergence is forbidden.",
        "- 2+1 physical consensus is allowed.",
        "- Shadow core continues normally for the blocked third book.",
        "- Defensive parking is causal and low-volatility based; TRX is not hardcoded.",
        "- No production, paper-live, Telegram, or exchange behavior is changed.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
        "",
    ]

    text_report = "\n".join(report)
    (run_dir / "report.md").write_text(text_report, encoding="utf-8")
    print(text_report)


if __name__ == "__main__":
    main()
