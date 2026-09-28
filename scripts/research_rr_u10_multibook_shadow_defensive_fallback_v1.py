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
OUT = ROOT / "research_artifacts" / "rr_u10_multibook_shadow_defensive_fallback_v1"

U10 = (
    "TWT", "PEPE", "BNB", "TRX", "AAVE",
    "AVAX", "FIL", "ALGO", "XRP", "HBAR",
)
ASSET_RANK = {asset: idx for idx, asset in enumerate(U10)}
COST = 0.001
VOL_LOOKBACK = 30

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
    # Causal: execution at open T uses only close information through T-1.
    source_idx = open_idx - 1
    if source_idx < 0:
        return math.inf
    value = float(volatility[asset][source_idx])
    return value if math.isfinite(value) else math.inf


def physical_assignment(books, volatility, open_idx):
    """
    Choose three distinct physical assets after shadow state has updated.

    Exact three-book priority:
    1) for each distinct shadow target, at most one physical book may occupy it;
    2) a current physical resident of that shadow target wins first;
    3) otherwise the strongest just-confirmed shadow transition wins;
    4) deterministic book index breaks remaining ties;
    5) losing books remain in an already-frozen defensive token when feasible;
    6) otherwise they park in the lowest causal 30d-volatility free U10 token.

    With three books this directly implements the preregistered objective without
    enumerating every 10^3 physical assignment.
    """
    desired_groups = {}
    for idx, book in enumerate(books):
        desired_groups.setdefault(book["shadow"], []).append(idx)

    synced_winners = set()
    used_targets = set()

    for target, members in desired_groups.items():
        def winner_key(idx):
            book = books[idx]
            resident = int(book["actual"] == target)
            fresh_strength = (
                float(book.get("shadow_score", 0.0))
                if book.get("shadow_transitioned", False)
                else 0.0
            )
            # max resident, max strength, then lowest deterministic book index
            return (resident, fresh_strength, -idx)

        winner = max(members, key=winner_key)
        synced_winners.add(winner)
        used_targets.add(target)

    assignments = [None, None, None]

    # Winners synchronize to shadow.
    for idx in synced_winners:
        assignments[idx] = {
            "target": books[idx]["shadow"],
            "kind": "sync",
            "vol": 0.0,
        }

    parking_books = [idx for idx in range(3) if idx not in synced_winners]

    # Preserve already-frozen defensive tokens whenever a winner does not claim them.
    for idx in parking_books:
        book = books[idx]
        actual = book["actual"]
        desired = book["shadow"]
        if (
            book.get("prev_parked", False)
            and actual != desired
            and actual not in used_targets
        ):
            assignments[idx] = {
                "target": actual,
                "kind": "park_stay",
                "vol": vol_at_open(volatility, actual, open_idx),
            }
            used_targets.add(actual)

    # Remaining losers use the lowest-volatility currently free token.
    for idx in parking_books:
        if assignments[idx] is not None:
            continue
        book = books[idx]
        ranked = sorted(
            U10,
            key=lambda asset: (
                vol_at_open(volatility, asset, open_idx),
                ASSET_RANK[asset],
            ),
        )
        chosen = None
        for asset in ranked:
            if asset in used_targets:
                continue
            chosen = asset
            break
        if chosen is None:
            raise RuntimeError("No free U10 asset for defensive parking")
        assignments[idx] = {
            "target": chosen,
            "kind": "park_new",
            "vol": vol_at_open(volatility, chosen, open_idx),
        }
        used_targets.add(chosen)

    targets = [row["target"] for row in assignments]
    if len(set(targets)) != 3:
        raise RuntimeError(f"Physical assignment collision: {targets}")
    return assignments


def concentration_snapshot(books, closes, idx):
    values = [
        float(book["qty"]) * float(closes[book["actual"]][idx])
        for book in books
    ]
    total = float(sum(values))
    shares = [value / total for value in values]
    return total, max(shares)


def simulate_shadow_defensive(
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
    max_share_series = []
    actual_collision = []
    shadow_collision = []
    any_mismatch = []
    mismatch_book_days = 0

    physical_transitions = 0
    actual_cost_paid = 0.0
    parking_entries = 0
    resyncs = 0
    defensive_displacements = 0
    parking_book_days = 0
    trx_parking_book_days = 0
    parking_token_days = Counter()
    parking_entry_tokens = Counter()
    shadow_transitions = 0

    for idx in range(start_i, end_i + 1):
        # State entering this open.
        old_shadow = [book["shadow"] for book in books]
        old_actual = [book["actual"] for book in books]
        prev_parked_flags = [
            book["actual"] != book["shadow"] for book in books
        ]

        # Shadow core follows ordinary U10 regardless of physical parking.
        for j, (book, event) in enumerate(zip(books, pending_shadow)):
            book["prev_parked"] = prev_parked_flags[j]
            book["shadow_transitioned"] = False
            book["shadow_score"] = 0.0
            if event is not None:
                book["shadow"] = str(event["to_asset"])
                book["shadow_score"] = float(event["max_dislocation"])
                book["shadow_transitioned"] = True
                shadow_transitions += 1

        # Initial bar starts already aligned; no synthetic initial transaction.
        if idx != start_i:
            choices = physical_assignment(books, volatility, idx)

            for j, (book, choice) in enumerate(zip(books, choices)):
                before_actual = book["actual"]
                before_shadow = old_shadow[j]
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

        # Close valuation / diagnostics.
        total, largest_share = concentration_snapshot(books, closes, idx)
        equity.append(total)
        max_share_series.append(largest_share)

        actuals = [book["actual"] for book in books]
        shadows = [book["shadow"] for book in books]
        actual_collision.append(len(set(actuals)) < 3)
        shadow_collision.append(len(set(shadows)) < 3)

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

        # Schedule next-open shadow transitions using only this closed candle.
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
    shares = np.asarray(max_share_series, dtype=float)
    days = len(arr)
    book_days = days * 3

    parking_total = sum(parking_token_days.values())
    trx_share = (
        float(trx_parking_book_days / parking_total)
        if parking_total
        else 0.0
    )

    return {
        "variant": "V3_SHADOW_DEFENSIVE_FALLBACK",
        "start_assets": "|".join(start_assets),
        "return": float(arr[-1] / arr[0] - 1.0),
        "max_dd": dd,
        "physical_transitions": int(physical_transitions),
        "shadow_transitions": int(shadow_transitions),
        "transition_cost_paid": float(actual_cost_paid),
        "max_largest_asset_share": float(np.max(shares)),
        "median_largest_asset_share": float(np.median(shares)),
        "days_gt_50_share": float(np.mean(shares > 0.50)),
        "actual_collision_day_share": float(np.mean(actual_collision)),
        "shadow_collision_day_share": float(np.mean(shadow_collision)),
        "any_mismatch_day_share": float(np.mean(any_mismatch)),
        "mismatch_book_day_share": float(mismatch_book_days / book_days),
        "parking_entries": int(parking_entries),
        "resyncs": int(resyncs),
        "defensive_displacements": int(defensive_displacements),
        "parking_book_days": int(parking_book_days),
        "trx_parking_book_days": int(trx_parking_book_days),
        "trx_parking_share": trx_share,
        "parking_entry_tokens_json": json.dumps(
            dict(sorted(parking_entry_tokens.items())),
            sort_keys=True,
        ),
        "parking_token_days_json": json.dumps(
            dict(sorted(parking_token_days.items())),
            sort_keys=True,
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
        "actual_collision_day_share": raw["collision_day_share"],
        "shadow_collision_day_share": raw["collision_day_share"],
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
        "median_actual_collision_share": float(
            frame["actual_collision_day_share"].median()
        ),
        "median_shadow_collision_share": float(
            frame["shadow_collision_day_share"].median()
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
        v2 = simulate_v2(
            timestamps,
            opens,
            closes,
            signal_positions,
            signal_candidates,
            start_i,
            end_i,
            start_assets,
        )
        v3 = simulate_shadow_defensive(
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
        for row in (v2, v3):
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
            for variant in ("V2_FREE", "V3_SHADOW_DEFENSIVE_FALLBACK"):
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
        for variant in ("V2_FREE", "V3_SHADOW_DEFENSIVE_FALLBACK"):
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
        "|window|variant|median return|worst return|median DD|worst DD|median peak concentration|actual collision days|shadow collision days|any mismatch days|TRX share of parking|",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in frame.iterrows():
        lines.append(
            f"|{row['window']}|{row['variant']}|"
            f"{pct(row['median_return'])}|{pct(row['worst_return'])}|"
            f"{pct(row['median_max_dd'])}|{pct(row['worst_max_dd'])}|"
            f"{100*row['median_peak_concentration']:.1f}%|"
            f"{100*row['median_actual_collision_share']:.1f}%|"
            f"{100*row['median_shadow_collision_share']:.1f}%|"
            f"{100*row['median_any_mismatch_share']:.1f}%|"
            f"{100*row['median_trx_parking_share']:.1f}%|"
        )
    return "\n".join(lines)


def rolling_table(frame: pd.DataFrame) -> str:
    lines = [
        "|months|variant|windows|median window return|worst median-return window|positive windows|median DD|peak concentration|mismatch days|TRX parking share|",
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|",
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
        for variant in ("V2_FREE", "V3_SHADOW_DEFENSIVE_FALLBACK"):
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
            & (primary["variant"] == "V3_SHADOW_DEFENSIVE_FALLBACK")
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
        v3 = primary_summary[
            (primary_summary["window"] == label)
            & (primary_summary["variant"] == "V3_SHADOW_DEFENSIVE_FALLBACK")
        ].iloc[0]
        comparison_rows.append(
            {
                "window": label,
                "return_delta_v3_minus_v2": float(
                    v3["median_return"] - v2["median_return"]
                ),
                "dd_delta_v3_minus_v2": float(
                    v3["median_max_dd"] - v2["median_max_dd"]
                ),
                "peak_concentration_delta_v3_minus_v2": float(
                    v3["median_peak_concentration"]
                    - v2["median_peak_concentration"]
                ),
            }
        )
    comparison = pd.DataFrame(comparison_rows)
    comparison.to_csv(run_dir / "v3_shadow_minus_v2.csv", index=False)

    summary = {
        "experiment": "RR_U10_MULTIBOOK_SHADOW_DEFENSIVE_FALLBACK_V1",
        "workflow_mode": "STRESS_TEST_ONLY",
        "source_commit_sha": source_sha(),
        "cutoff": core.utc(args.cutoff).isoformat(),
        "frozen_universe_id": "RR_TARGET_U10_CANDIDATE_HBAR_V1",
        "assets": list(U10),
        "triplets": len(triplets()),
        "parameters": {
            "books": 3,
            "equal_initial_allocation": True,
            "lookback": core.LOOKBACK,
            "arm_threshold": core.ARM,
            "reversal": core.REVERSAL,
            "transition_cost": COST,
            "defensive_vol_lookback": VOL_LOOKBACK,
            "physical_holdings_distinct": True,
            "shadow_core_continues_while_parked": True,
            "defensive_token_frozen_until_resync_when_feasible": True,
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
        "# RR U10 MULTIBOOK SHADOW DEFENSIVE FALLBACK V1",
        "",
        "Mode: STRESS_TEST_ONLY",
        "",
        "Frozen universe: RR_TARGET_U10_CANDIDATE_HBAR_V1",
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
        "- Actual and shadow assets are tracked separately.",
        "- Shadow core continues unconstrained while physical capital is defensively parked.",
        "- No production, paper-live or Telegram behavior is changed.",
        "- No automatic exchange execution.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
        "",
    ]
    text = "\n".join(report)
    (run_dir / "report.md").write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
