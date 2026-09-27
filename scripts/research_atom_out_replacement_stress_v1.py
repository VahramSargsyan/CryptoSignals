from __future__ import annotations

import argparse
import json
import os
import subprocess
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import build_pair_monitor

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "atom_out_replacement_stress_v1"

POOL = (
    "ATOM", "TWT", "PEPE", "BNB", "SOL", "TRX", "AAVE", "LINK",
    "AVAX", "FIL", "ETH", "ALGO", "ADA", "XRP", "HBAR",
)
BASE_U9 = ("TWT", "PEPE", "BNB", "SOL", "TRX", "AAVE", "LINK", "FIL", "HBAR")
CANDIDATES = ("AVAX", "ETH", "ALGO", "ADA", "XRP")
REFERENCE = "ATOM"

LOOKBACK = 180
ARM = 0.15
REVERSAL = 0.03
COST = 0.001

DATA_START = pd.Timestamp("2023-05-05", tz="UTC")
MATURE_START = pd.Timestamp("2023-10-31", tz="UTC")
EVAL_END = pd.Timestamp("2026-09-26", tz="UTC")
YEAR_START = pd.Timestamp("2025-09-27", tz="UTC")
TWO_YEAR_START = pd.Timestamp("2024-09-27", tz="UTC")
ENDPOINT_OFFSETS = (0, 30, 60, 90, 120, 180)


def utc(value):
    t = pd.Timestamp(value)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def source_sha():
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def download_panel(cutoff):
    client = BinanceSpotRestClient()
    panel = None
    meta = {}
    for asset in POOL:
        result = download_historical_dataset(
            client,
            symbol=asset + "USDT",
            start=DATA_START,
            end=cutoff,
            timeframe="1D",
            as_of=cutoff,
        )
        if result.dataset is None:
            raise RuntimeError(f"{asset}: {result.metadata.status}")
        if result.dataset.quality.has_critical_issues:
            raise RuntimeError(f"{asset}: critical data quality issues")
        frame = result.dataset.candles[["timestamp", "open", "close"]].copy()
        frame = frame.rename(
            columns={"open": asset + "_open", "close": asset + "_close"}
        )
        meta[asset] = {
            "rows": len(frame),
            "start": pd.Timestamp(frame.iloc[0]["timestamp"]).isoformat(),
            "end": pd.Timestamp(frame.iloc[-1]["timestamp"]).isoformat(),
        }
        panel = (
            frame
            if panel is None
            else panel.merge(frame, on="timestamp", how="inner", validate="one_to_one")
        )
    return panel.sort_values("timestamp").reset_index(drop=True), meta


def build_events(panel):
    cols = ["timestamp"] + [a + "_close" for a in POOL]
    events, _ = build_pair_monitor(
        panel[cols].copy(),
        assets=POOL,
        lookback=LOOKBACK,
        arm_threshold=ARM,
        reversal=REVERSAL,
    )
    by_date = defaultdict(list)
    for event in events:
        if event["event"] == "CONFIRMED":
            by_date[utc(event["date"])].append(event)
    return by_date


def run_one(panel, events, assets, start, end, start_asset, candidate=None, collect=False):
    aset = set(assets)
    window = panel[
        (panel["timestamp"] >= start) & (panel["timestamp"] <= end)
    ].copy()
    if window.empty:
        raise ValueError(f"empty window {start} -> {end}")
    if start_asset not in aset:
        raise ValueError(f"start asset {start_asset} absent")

    current = start_asset
    qty = 1.0 / float(window.iloc[0][current + "_open"])
    pending = None
    equity = []
    timestamps = []
    holdings = Counter()
    transitions = []
    route = [current]
    conflicts = 0

    candidate_entry_value = None
    candidate_spells = []

    for pos, (_, row) in enumerate(window.iterrows()):
        ts = utc(row["timestamp"])

        if pending is not None:
            from_asset = current
            to_asset = pending["to_asset"]
            value_before = qty * float(row[from_asset + "_open"])
            value_after_cost = value_before * (1.0 - COST)
            current = to_asset
            qty = value_after_cost / float(row[current + "_open"])
            transitions.append(
                {
                    "execution_date": ts.isoformat(),
                    "signal_date": pending["signal_date"],
                    "from_asset": from_asset,
                    "to_asset": to_asset,
                    "value_before": value_before,
                    "value_after_cost": value_after_cost,
                    "max_dislocation": pending["max_dislocation"],
                }
            )
            route.append(current)

            if candidate is not None:
                if to_asset == candidate and from_asset != candidate:
                    candidate_entry_value = value_after_cost
                elif from_asset == candidate and to_asset != candidate:
                    if candidate_entry_value is not None:
                        exit_value_after_cost = value_before * (1.0 - COST)
                        candidate_spells.append(
                            exit_value_after_cost / candidate_entry_value - 1.0
                        )
                    candidate_entry_value = None
            pending = None

        holdings[current] += 1
        timestamps.append(ts)
        equity.append(qty * float(row[current + "_close"]))

        if pos == len(window) - 1:
            continue

        candidates = [
            event
            for event in events.get(ts, [])
            if event["from_asset"] == current and event["to_asset"] in aset
        ]
        if not candidates:
            continue
        if len(candidates) > 1:
            conflicts += 1
        chosen = sorted(
            candidates,
            key=lambda event: (
                -float(event["max_dislocation"]),
                event["to_asset"],
                event["pair"],
            ),
        )[0]
        pending = {
            "to_asset": chosen["to_asset"],
            "signal_date": ts.isoformat(),
            "max_dislocation": float(chosen["max_dislocation"]),
        }

    series = pd.Series(equity, index=pd.DatetimeIndex(timestamps), dtype=float)
    out = {
        "start_asset": start_asset,
        "return": float(series.iloc[-1] / series.iloc[0] - 1.0),
        "max_dd": float((series / series.cummax() - 1.0).min()),
        "transitions": len(transitions),
        "conflicts": conflicts,
    }
    if collect:
        out["holdings"] = dict(holdings)
        out["route"] = route
        out["transition_log"] = transitions
        out["candidate_spells"] = candidate_spells
        out["equity_index"] = [t.isoformat() for t in series.index]
        out["equity_values"] = [float(x) for x in series.values]
    return out


def summarize(panel, events, assets, start, end, starts, candidate=None, collect=False):
    runs = []
    details = []
    for start_asset in starts:
        result = run_one(
            panel,
            events,
            assets,
            start,
            end,
            start_asset,
            candidate=candidate,
            collect=collect,
        )
        runs.append(
            {
                key: value
                for key, value in result.items()
                if key
                in {
                    "start_asset",
                    "return",
                    "max_dd",
                    "transitions",
                    "conflicts",
                }
            }
        )
        if collect:
            details.append(result)

    df = pd.DataFrame(runs)
    out = {
        "median_return": float(df["return"].median()),
        "mean_return": float(df["return"].mean()),
        "worst_return": float(df["return"].min()),
        "best_return": float(df["return"].max()),
        "positive_starts": int((df["return"] > 0).sum()),
        "start_count": int(len(df)),
        "median_max_dd": float(df["max_dd"].median()),
        "worst_max_dd": float(df["max_dd"].min()),
        "median_transitions": float(df["transitions"].median()),
        "median_conflicts": float(df["conflicts"].median()),
    }

    if collect:
        total_holding_days = 0
        candidate_holding_days = 0
        candidate_entries = 0
        candidate_exits = 0
        neighbors = Counter()
        spell_returns = []
        for detail in details:
            total_holding_days += sum(detail["holdings"].values())
            if candidate is not None:
                candidate_holding_days += detail["holdings"].get(candidate, 0)
                spell_returns.extend(detail["candidate_spells"])
                for tr in detail["transition_log"]:
                    if tr["to_asset"] == candidate and tr["from_asset"] != candidate:
                        candidate_entries += 1
                        neighbors[tr["from_asset"]] += 1
                    if tr["from_asset"] == candidate and tr["to_asset"] != candidate:
                        candidate_exits += 1
                        neighbors[tr["to_asset"]] += 1

        neighbor_total = sum(neighbors.values())
        top_neighbor, top_neighbor_count = (None, 0)
        if neighbors:
            top_neighbor, top_neighbor_count = neighbors.most_common(1)[0]

        out["candidate_stats"] = {
            "candidate": candidate,
            "occupancy_share": (
                candidate_holding_days / total_holding_days
                if candidate is not None and total_holding_days
                else 0.0
            ),
            "entries": candidate_entries,
            "exits": candidate_exits,
            "completed_spells": len(spell_returns),
            "spell_win_rate": (
                sum(x > 0 for x in spell_returns) / len(spell_returns)
                if spell_returns
                else None
            ),
            "median_spell_return": (
                float(pd.Series(spell_returns, dtype=float).median())
                if spell_returns
                else None
            ),
            "top_neighbor": top_neighbor,
            "top_neighbor_share": (
                top_neighbor_count / neighbor_total if neighbor_total else None
            ),
            "neighbor_counts": dict(neighbors),
        }
        out["_details"] = details

    return out


def monthly_windows(months):
    starts = []
    first = pd.Timestamp("2023-11-01", tz="UTC")
    for start in pd.date_range(first, EVAL_END, freq="MS", tz="UTC"):
        end = start + pd.DateOffset(months=months) - pd.Timedelta(days=1)
        end = utc(end)
        if end <= EVAL_END:
            starts.append((utc(start), end))
    return starts


def rolling_compare(panel, events, candidate, months):
    cand_assets = BASE_U9 + (candidate,)
    rows = []
    for start, end in monthly_windows(months):
        base = summarize(
            panel, events, BASE_U9, start, end, BASE_U9
        )
        cand = summarize(
            panel, events, cand_assets, start, end, BASE_U9
        )
        rows.append(
            {
                "start": start.isoformat(),
                "end": end.isoformat(),
                "base_return": base["median_return"],
                "candidate_return": cand["median_return"],
                "return_delta": cand["median_return"] - base["median_return"],
                "base_dd": base["median_max_dd"],
                "candidate_dd": cand["median_max_dd"],
                "dd_delta": cand["median_max_dd"] - base["median_max_dd"],
            }
        )
    df = pd.DataFrame(rows)
    return {
        "window_count": int(len(df)),
        "return_improvement_rate": float((df["return_delta"] > 0).mean()),
        "dd_improvement_rate": float((df["dd_delta"] > 0).mean()),
        "median_return_delta": float(df["return_delta"].median()),
        "worst_return_delta": float(df["return_delta"].min()),
        "best_return_delta": float(df["return_delta"].max()),
        "median_dd_delta": float(df["dd_delta"].median()),
        "rows": rows,
    }


def endpoint_compare(panel, events, candidate):
    cand_assets = BASE_U9 + (candidate,)
    rows = []
    for offset in ENDPOINT_OFFSETS:
        end = EVAL_END - pd.Timedelta(days=offset)
        start = end - pd.DateOffset(years=1) + pd.Timedelta(days=1)
        start = utc(start)
        end = utc(end)
        base = summarize(panel, events, BASE_U9, start, end, BASE_U9)
        cand = summarize(panel, events, cand_assets, start, end, BASE_U9)
        rows.append(
            {
                "offset_days": offset,
                "start": start.isoformat(),
                "end": end.isoformat(),
                "return_delta": cand["median_return"] - base["median_return"],
                "dd_delta": cand["median_max_dd"] - base["median_max_dd"],
            }
        )
    df = pd.DataFrame(rows)
    return {
        "endpoint_count": len(rows),
        "return_improvement_rate": float((df["return_delta"] > 0).mean()),
        "dd_improvement_rate": float((df["dd_delta"] > 0).mean()),
        "median_return_delta": float(df["return_delta"].median()),
        "worst_return_delta": float(df["return_delta"].min()),
        "rows": rows,
    }


def bull_day_map(panel):
    returns = pd.DataFrame(
        {
            asset: panel[asset + "_close"].astype(float)
            / panel[asset + "_close"].astype(float).shift(30)
            - 1.0
            for asset in POOL
        }
    )
    broad = returns.median(axis=1, skipna=True)
    return {
        utc(ts): bool(flag)
        for ts, flag in zip(panel["timestamp"], broad >= 0.25)
    }


def neutralized_return(detail, bull_days):
    idx = pd.DatetimeIndex([utc(x) for x in detail["equity_index"]])
    eq = pd.Series(detail["equity_values"], index=idx, dtype=float)
    daily = eq.pct_change().fillna(0.0)
    adjusted = []
    for ts, value in daily.items():
        if bull_days.get(utc(ts), False) and value > 0:
            adjusted.append(0.0)
        else:
            adjusted.append(float(value))
    return float((pd.Series(adjusted, dtype=float) + 1.0).prod() - 1.0)


def bull_neutral_summary(summary_with_details, bull_days):
    values = [
        neutralized_return(detail, bull_days)
        for detail in summary_with_details["_details"]
    ]
    s = pd.Series(values, dtype=float)
    return {
        "median_return": float(s.median()),
        "mean_return": float(s.mean()),
        "worst_return": float(s.min()),
        "best_return": float(s.max()),
    }


def neighbor_dependency(panel, events, candidate):
    rows = []
    for removed in BASE_U9:
        reduced_base = tuple(a for a in BASE_U9 if a != removed)
        reduced_candidate = reduced_base + (candidate,)
        starts = reduced_base

        base_1y = summarize(panel, events, reduced_base, YEAR_START, EVAL_END, starts)
        cand_1y = summarize(
            panel, events, reduced_candidate, YEAR_START, EVAL_END, starts
        )
        base_2y = summarize(
            panel, events, reduced_base, TWO_YEAR_START, EVAL_END, starts
        )
        cand_2y = summarize(
            panel, events, reduced_candidate, TWO_YEAR_START, EVAL_END, starts
        )

        rows.append(
            {
                "removed_neighbor": removed,
                "return_delta_1y": cand_1y["median_return"] - base_1y["median_return"],
                "dd_delta_1y": cand_1y["median_max_dd"] - base_1y["median_max_dd"],
                "return_delta_2y": cand_2y["median_return"] - base_2y["median_return"],
                "dd_delta_2y": cand_2y["median_max_dd"] - base_2y["median_max_dd"],
            }
        )

    df = pd.DataFrame(rows)
    return {
        "neighbor_count": len(rows),
        "positive_return_rate_1y": float((df["return_delta_1y"] > 0).mean()),
        "positive_return_rate_2y": float((df["return_delta_2y"] > 0).mean()),
        "median_return_delta_1y": float(df["return_delta_1y"].median()),
        "median_return_delta_2y": float(df["return_delta_2y"].median()),
        "worst_return_delta_1y": float(df["return_delta_1y"].min()),
        "worst_return_delta_2y": float(df["return_delta_2y"].min()),
        "most_sensitive_neighbor_1y": str(
            df.loc[df["return_delta_1y"].idxmin(), "removed_neighbor"]
        ),
        "most_sensitive_neighbor_2y": str(
            df.loc[df["return_delta_2y"].idxmin(), "removed_neighbor"]
        ),
        "rows": rows,
    }


def slim_summary(value):
    return {
        key: val
        for key, val in value.items()
        if key != "_details"
    }


def candidate_result(panel, events, bull_days, candidate, base_refs):
    assets = BASE_U9 + (candidate,)

    one_year = summarize(
        panel, events, assets, YEAR_START, EVAL_END, BASE_U9, candidate, collect=True
    )
    two_year = summarize(
        panel,
        events,
        assets,
        TWO_YEAR_START,
        EVAL_END,
        BASE_U9,
        candidate,
        collect=True,
    )
    long = summarize(
        panel,
        events,
        assets,
        MATURE_START,
        EVAL_END,
        BASE_U9,
        candidate,
        collect=True,
    )

    neutral = bull_neutral_summary(long, bull_days)
    rolling_12 = rolling_compare(panel, events, candidate, 12)
    rolling_24 = rolling_compare(panel, events, candidate, 24)
    endpoints = endpoint_compare(panel, events, candidate)
    neighbors = neighbor_dependency(panel, events, candidate)

    checks = {
        "one_year_return_better": (
            one_year["median_return"] > base_refs["one_year"]["median_return"]
        ),
        "two_year_return_better": (
            two_year["median_return"] > base_refs["two_year"]["median_return"]
        ),
        "one_year_dd_not_materially_worse": (
            one_year["median_max_dd"]
            >= base_refs["one_year"]["median_max_dd"] - 0.03
        ),
        "rolling_12_majority_better": (
            rolling_12["return_improvement_rate"] > 0.50
        ),
        "rolling_24_majority_better": (
            rolling_24["return_improvement_rate"] > 0.50
        ),
        "endpoint_majority_better": (
            endpoints["return_improvement_rate"] >= 0.50
        ),
        "bull_neutral_better": (
            neutral["median_return"]
            > base_refs["bull_neutral_long"]["median_return"]
        ),
        "neighbor_majority_better": (
            min(
                neighbors["positive_return_rate_1y"],
                neighbors["positive_return_rate_2y"],
            )
            >= 0.50
        ),
    }

    return {
        "candidate": candidate,
        "one_year": slim_summary(one_year),
        "two_year": slim_summary(two_year),
        "long": slim_summary(long),
        "bull_neutral_long": neutral,
        "rolling_12": rolling_12,
        "rolling_24": rolling_24,
        "endpoint_sensitivity": endpoints,
        "neighbor_dependency": neighbors,
        "checks": checks,
        "checks_passed": int(sum(bool(x) for x in checks.values())),
    }


def rank_key(result):
    return (
        result["checks_passed"],
        result["rolling_12"]["return_improvement_rate"],
        result["rolling_24"]["return_improvement_rate"],
        result["endpoint_sensitivity"]["return_improvement_rate"],
        min(
            result["neighbor_dependency"]["positive_return_rate_1y"],
            result["neighbor_dependency"]["positive_return_rate_2y"],
        ),
        result["bull_neutral_long"]["median_return"],
        result["two_year"]["median_return"],
    )


def pct(value):
    return f"{100.0 * value:+.2f}%"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff", default="2026-09-27T00:00:00Z")
    args = parser.parse_args()

    panel, meta = download_panel(utc(args.cutoff))
    panel = panel[panel["timestamp"] <= EVAL_END].copy().reset_index(drop=True)
    events = build_events(panel)
    bull_days = bull_day_map(panel)

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    base_1y = summarize(
        panel, events, BASE_U9, YEAR_START, EVAL_END, BASE_U9, collect=True
    )
    base_2y = summarize(
        panel, events, BASE_U9, TWO_YEAR_START, EVAL_END, BASE_U9, collect=True
    )
    base_long = summarize(
        panel, events, BASE_U9, MATURE_START, EVAL_END, BASE_U9, collect=True
    )
    base_neutral = bull_neutral_summary(base_long, bull_days)

    base_refs = {
        "one_year": slim_summary(base_1y),
        "two_year": slim_summary(base_2y),
        "long": slim_summary(base_long),
        "bull_neutral_long": base_neutral,
    }

    results = [
        candidate_result(panel, events, bull_days, candidate, base_refs)
        for candidate in CANDIDATES + (REFERENCE,)
    ]

    eligible = [r for r in results if r["candidate"] in CANDIDATES]
    eligible_sorted = sorted(eligible, key=rank_key, reverse=True)
    leader = eligible_sorted[0]

    nothing_is_preferred = leader["checks_passed"] < 5
    proposed = "NOTHING" if nothing_is_preferred else leader["candidate"]

    output = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit": source_sha(),
        "mode": "STRESS_TEST_ONLY",
        "data": meta,
        "frozen_mechanics": {
            "lookback": LOOKBACK,
            "arm": ARM,
            "reversal": REVERSAL,
            "cost": COST,
            "data_start": DATA_START.isoformat(),
            "mature_start": MATURE_START.isoformat(),
            "eval_end": EVAL_END.isoformat(),
            "common_starts": list(BASE_U9),
            "base_u9": list(BASE_U9),
            "eligible_candidates": list(CANDIDATES),
            "reference_only": REFERENCE,
            "bull_day_rule": "cross-sectional median 30d return of 15-token pool >= +25%",
        },
        "base_u9": base_refs,
        "candidates": results,
        "robustness_order": [r["candidate"] for r in eligible_sorted],
        "leader": leader["candidate"],
        "proposed_slot": proposed,
        "nothing_rule": "NOTHING if best eligible candidate passes fewer than 5 of 8 preregistered robustness checks",
    }

    (run_dir / "results.json").write_text(
        json.dumps(output, indent=2, sort_keys=True, allow_nan=False),
        encoding="utf-8",
    )

    rows = []
    for r in eligible_sorted:
        stats = r["long"]["candidate_stats"]
        rows.append(
            [
                r["candidate"],
                str(r["checks_passed"]) + "/8",
                pct(r["one_year"]["median_return"] - base_refs["one_year"]["median_return"]),
                pct(r["two_year"]["median_return"] - base_refs["two_year"]["median_return"]),
                f"{100*r['rolling_12']['return_improvement_rate']:.0f}%",
                f"{100*r['rolling_24']['return_improvement_rate']:.0f}%",
                f"{100*r['endpoint_sensitivity']['return_improvement_rate']:.0f}%",
                pct(r["bull_neutral_long"]["median_return"] - base_neutral["median_return"]),
                f"{100*stats['occupancy_share']:.1f}%",
                f"{stats['entries']}/{stats['exits']}",
                str(stats["top_neighbor"] or "-"),
                (
                    f"{100*stats['top_neighbor_share']:.0f}%"
                    if stats["top_neighbor_share"] is not None
                    else "-"
                ),
            ]
        )

    lines = [
        "# ATOM OUT Replacement Stress v1",
        "",
        "Mode: STRESS_TEST_ONLY",
        "Production changes: NONE",
        "",
        "## Base U9",
        "",
        "TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR",
        "",
        f"1Y median return: {pct(base_refs['one_year']['median_return'])}",
        f"1Y median max DD: {pct(base_refs['one_year']['median_max_dd'])}",
        f"2Y median return: {pct(base_refs['two_year']['median_return'])}",
        f"2Y median max DD: {pct(base_refs['two_year']['median_max_dd'])}",
        f"Long bull-neutralized median return: {pct(base_neutral['median_return'])}",
        "",
        "## Eligible replacement robustness table",
        "",
        "| Candidate | Checks | 1Y delta | 2Y delta | Rolling12 win | Rolling24 win | Endpoint win | Bull-neutral delta | Long occupancy | Entries/Exits | Top neighbor | Neighbor share |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")

    lines += [
        "",
        "## Decision output",
        "",
        f"Robustness leader: **{leader['candidate']}**",
        f"Preregistered proposed slot: **{proposed}**",
        f"Leader checks passed: {leader['checks_passed']}/8",
        "",
        "ATOM was tested as a reference arm only and was not eligible to win the replacement slot.",
        "",
        "## Notes",
        "",
        "- Common-start rule prevents the added tenth token from improving its score merely by adding itself as a start asset.",
        "- Broad-bull neutralization preserves route and losses while stripping positive route-equity returns on broad bull days.",
        "- Neighbor dependency compares candidate incremental value after removing each base-U9 token from both sides.",
        "- Historical results do not establish future performance.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    ]

    (run_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    print("run_dir=" + str(run_dir))
    print("base_u9_1y=" + pct(base_refs["one_year"]["median_return"]))
    print("base_u9_2y=" + pct(base_refs["two_year"]["median_return"]))
    for r in eligible_sorted:
        print(
            "candidate=%s checks=%d/8 d1y=%s d2y=%s roll12=%.3f roll24=%.3f endpoint=%.3f bull_delta=%s"
            % (
                r["candidate"],
                r["checks_passed"],
                pct(r["one_year"]["median_return"] - base_refs["one_year"]["median_return"]),
                pct(r["two_year"]["median_return"] - base_refs["two_year"]["median_return"]),
                r["rolling_12"]["return_improvement_rate"],
                r["rolling_24"]["return_improvement_rate"],
                r["endpoint_sensitivity"]["return_improvement_rate"],
                pct(r["bull_neutral_long"]["median_return"] - base_neutral["median_return"]),
            )
        )
    print("robustness_leader=" + leader["candidate"])
    print("proposed_slot=" + proposed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
