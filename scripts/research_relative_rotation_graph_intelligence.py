from __future__ import annotations

import argparse
import itertools
import json
import math
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import choix
import evalica
from evalica import Winner

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient

REPO_ROOT = Path(__file__).resolve().parents[1]

ASSETS = ("ATOM", "TWT", "PEPE", "BNB", "SOL", "TRX", "AAVE", "LINK")
SYMBOLS = {asset: f"{asset}USDT" for asset in ASSETS}

LOOKBACK = 180
ARM_THRESHOLD = 0.15
REVERSAL = 0.03
TRANSITION_COST = 0.001

REFERENCE_OOS_START = pd.Timestamp("2025-03-29", tz="UTC")
REFERENCE_END = pd.Timestamp("2026-03-28", tz="UTC")
REFERENCE_BASELINE_MEDIAN = 0.416
BASELINE_REPRO_TOLERANCE = 0.05

SEQUENTIAL_180D = (
    ("2023-10-31", "2024-04-27"),
    ("2024-04-28", "2024-10-24"),
    ("2024-10-25", "2025-04-22"),
    ("2025-04-23", "2025-10-19"),
    ("2025-10-20", "2026-03-28"),
)

RANKERS = ("BASELINE", "BRADLEY_TERRY", "PAGERANK", "RANK_CENTRALITY", "CONSENSUS_3")


@dataclass(frozen=True)
class PairSignal:
    date: pd.Timestamp
    from_asset: str
    to_asset: str
    strength: float
    pair: str


@dataclass
class PairState:
    mode: str = "NONE"
    extreme: float | None = None
    max_dislocation: float = 0.0


@dataclass(frozen=True)
class BacktestResult:
    variant: str
    start_asset: str
    period_start: str
    period_end: str
    total_return: float
    max_drawdown: float
    transitions: int
    conflicts: int
    changed_conflict_choices: int


def _source_commit() -> str:
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO_ROOT,
        text=True,
    ).strip()


def _utc(value: str | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def download_panel(start: pd.Timestamp, end: pd.Timestamp) -> tuple[pd.DataFrame, dict]:
    client = BinanceSpotRestClient()
    cutoff = end + pd.Timedelta(days=1)
    pieces: list[pd.DataFrame] = []
    metadata: dict[str, dict] = {}

    for asset in ASSETS:
        result = download_historical_dataset(
            client,
            symbol=SYMBOLS[asset],
            start=start,
            end=cutoff,
            timeframe="1D",
            as_of=cutoff,
        )
        if result.dataset is None:
            raise RuntimeError(f"No dataset for {asset}: {result.metadata.status}")
        if result.dataset.quality.has_critical_issues:
            raise RuntimeError(f"Critical data quality issue for {asset}: {result.dataset.quality}")

        frame = result.dataset.candles[["timestamp", "open", "close"]].copy()
        frame = frame.rename(columns={"open": f"{asset}_open", "close": f"{asset}_close"})
        pieces.append(frame)
        metadata[asset] = {
            "dataset_id": result.dataset.dataset_id,
            "rows": len(frame),
            "actual_start": result.metadata.actual_start,
            "actual_end": result.metadata.actual_end,
            "listing_truncated": result.metadata.listing_truncated,
            "quality": result.dataset.quality.__dict__,
        }

    panel = pieces[0]
    for frame in pieces[1:]:
        panel = panel.merge(frame, on="timestamp", how="inner", validate="one_to_one")
    panel = panel.sort_values("timestamp").reset_index(drop=True)

    expected = pd.date_range(panel["timestamp"].min(), panel["timestamp"].max(), freq="D", tz="UTC")
    actual = pd.DatetimeIndex(panel["timestamp"])
    missing = expected.difference(actual)
    metadata["panel"] = {
        "rows": len(panel),
        "start": panel["timestamp"].min().isoformat(),
        "end": panel["timestamp"].max().isoformat(),
        "missing_common_dates": [x.isoformat() for x in missing],
    }
    return panel, metadata


def build_pair_context(panel: pd.DataFrame) -> tuple[dict[pd.Timestamp, list[PairSignal]], dict[pd.Timestamp, dict]]:
    signals_by_date: dict[pd.Timestamp, list[PairSignal]] = {
        ts: [] for ts in panel["timestamp"]
    }
    graph_by_date: dict[pd.Timestamp, dict] = {}

    ratios: dict[tuple[str, str], pd.Series] = {}
    medians: dict[tuple[str, str], pd.Series] = {}
    deviations: dict[tuple[str, str], pd.Series] = {}

    for left, right in itertools.combinations(ASSETS, 2):
        ratio = panel[f"{right}_close"] / panel[f"{left}_close"]
        median = ratio.rolling(LOOKBACK, min_periods=LOOKBACK).median()
        ratios[(left, right)] = ratio
        medians[(left, right)] = median
        deviations[(left, right)] = ratio / median - 1.0

    states = {pair: PairState() for pair in ratios}

    for idx, ts in enumerate(panel["timestamp"]):
        # First build causal pairwise preference snapshot from the same closed candle.
        comparisons: list[tuple[str, str, str, float]] = []
        for left, right in itertools.combinations(ASSETS, 2):
            dev = deviations[(left, right)].iloc[idx]
            if pd.isna(dev):
                continue
            if dev > 0:
                winner, loser = left, right
            elif dev < 0:
                winner, loser = right, left
            else:
                continue
            comparisons.append((left, right, winner, abs(float(dev))))

        if len(comparisons) == math.comb(len(ASSETS), 2):
            graph_by_date[ts] = graph_snapshot(comparisons)

        # Then update the stateful ARM -> extreme -> reversal engine.
        for pair, ratio_series in ratios.items():
            left, right = pair
            ratio = float(ratio_series.iloc[idx])
            median = medians[pair].iloc[idx]
            dev = deviations[pair].iloc[idx]
            if pd.isna(median) or pd.isna(dev):
                continue
            dev = float(dev)
            state = states[pair]

            if state.mode == "NONE":
                if dev >= ARM_THRESHOLD:
                    state.mode = "HIGH"
                    state.extreme = ratio
                    state.max_dislocation = abs(dev)
                elif dev <= -ARM_THRESHOLD:
                    state.mode = "LOW"
                    state.extreme = ratio
                    state.max_dislocation = abs(dev)
                continue

            if state.mode == "HIGH":
                assert state.extreme is not None
                if ratio > state.extreme:
                    state.extreme = ratio
                    state.max_dislocation = max(state.max_dislocation, abs(dev))
                if ratio <= state.extreme * (1.0 - REVERSAL):
                    signals_by_date[ts].append(
                        PairSignal(
                            date=ts,
                            from_asset=right,
                            to_asset=left,
                            strength=state.max_dislocation,
                            pair=f"{left}/{right}",
                        )
                    )
                    states[pair] = PairState()
                continue

            if state.mode == "LOW":
                assert state.extreme is not None
                if ratio < state.extreme:
                    state.extreme = ratio
                    state.max_dislocation = max(state.max_dislocation, abs(dev))
                if ratio >= state.extreme * (1.0 + REVERSAL):
                    signals_by_date[ts].append(
                        PairSignal(
                            date=ts,
                            from_asset=left,
                            to_asset=right,
                            strength=state.max_dislocation,
                            pair=f"{left}/{right}",
                        )
                    )
                    states[pair] = PairState()

    return signals_by_date, graph_by_date


def graph_snapshot(comparisons: Iterable[tuple[str, str, str, float]]) -> dict:
    rows = list(comparisons)
    xs = [left for left, _, _, _ in rows]
    ys = [right for _, right, _, _ in rows]
    winners = [
        Winner.X if winner == left else Winner.Y
        for left, right, winner, _ in rows
    ]
    index = pd.Index(ASSETS)

    bt = evalica.bradley_terry(xs, ys, winners, index=index, solver="naive").scores
    pr = evalica.pagerank(xs, ys, winners, index=index, solver="naive").scores

    asset_to_idx = {asset: i for i, asset in enumerate(ASSETS)}
    rc_data = [
        (asset_to_idx[winner], asset_to_idx[right if winner == left else left])
        for left, right, winner, _ in rows
    ]
    rc_values = choix.rank_centrality(len(ASSETS), rc_data, alpha=1e-6)
    rc = pd.Series(rc_values, index=index, name="rank_centrality").sort_values(
        ascending=False,
        kind="stable",
    )

    preference = {
        frozenset((left, right)): winner
        for left, right, winner, _ in rows
    }
    cyclic = 0
    triples = 0
    for a, b, c in itertools.combinations(ASSETS, 3):
        triples += 1
        ab = preference[frozenset((a, b))]
        bc = preference[frozenset((b, c))]
        ac = preference[frozenset((a, c))]
        if (ab == a and bc == b and ac == c) or (ab == b and bc == c and ac == a):
            cyclic += 1

    ranks: dict[str, dict[str, int]] = {}
    for name, series in (
        ("BRADLEY_TERRY", bt),
        ("PAGERANK", pr),
        ("RANK_CENTRALITY", rc),
    ):
        ranks[name] = {asset: rank + 1 for rank, asset in enumerate(series.index)}

    consensus = {
        asset: ranks["BRADLEY_TERRY"][asset]
        + ranks["PAGERANK"][asset]
        + ranks["RANK_CENTRALITY"][asset]
        for asset in ASSETS
    }

    return {
        "scores": {
            "BRADLEY_TERRY": bt.to_dict(),
            "PAGERANK": pr.to_dict(),
            "RANK_CENTRALITY": rc.to_dict(),
        },
        "ranks": ranks,
        "consensus_rank_sum": consensus,
        "cycle_ratio": cyclic / triples if triples else 0.0,
        "cyclic_triples": cyclic,
        "triple_count": triples,
    }


def choose_candidate(
    candidates: list[PairSignal],
    variant: str,
    graph: dict | None,
) -> PairSignal:
    baseline = sorted(
        candidates,
        key=lambda x: (-x.strength, x.to_asset, x.pair),
    )[0]
    if variant == "BASELINE" or len(candidates) == 1 or graph is None:
        return baseline

    if variant in ("BRADLEY_TERRY", "PAGERANK", "RANK_CENTRALITY"):
        ranks = graph["ranks"][variant]
        return sorted(
            candidates,
            key=lambda x: (ranks[x.to_asset], -x.strength, x.to_asset),
        )[0]

    if variant == "CONSENSUS_3":
        rank_sum = graph["consensus_rank_sum"]
        return sorted(
            candidates,
            key=lambda x: (rank_sum[x.to_asset], -x.strength, x.to_asset),
        )[0]

    raise ValueError(f"Unknown variant: {variant}")


def run_backtest(
    panel: pd.DataFrame,
    signals_by_date: dict[pd.Timestamp, list[PairSignal]],
    graph_by_date: dict[pd.Timestamp, dict],
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    start_asset: str,
    variant: str,
    conflict_log: list[dict] | None = None,
) -> BacktestResult:
    window = panel[(panel["timestamp"] >= start) & (panel["timestamp"] <= end)].copy()
    if window.empty:
        raise ValueError("Empty evaluation window")

    dates = list(window["timestamp"])
    first = window.iloc[0]
    current = start_asset
    qty = 1.0 / float(first[f"{current}_open"])
    pending: PairSignal | None = None
    equity: list[float] = []
    transitions = 0
    conflicts = 0
    changed = 0

    for pos, (_, row) in enumerate(window.iterrows()):
        ts = row["timestamp"]

        if pending is not None:
            current_value = qty * float(row[f"{current}_open"])
            qty = current_value * (1.0 - TRANSITION_COST) / float(row[f"{pending.to_asset}_open"])
            current = pending.to_asset
            transitions += 1
            pending = None

        equity.append(qty * float(row[f"{current}_close"]))

        if pos == len(window) - 1:
            continue

        candidates = [
            sig for sig in signals_by_date.get(ts, [])
            if sig.from_asset == current
        ]
        if not candidates:
            continue

        graph = graph_by_date.get(ts)
        baseline = choose_candidate(candidates, "BASELINE", graph)
        chosen = choose_candidate(candidates, variant, graph)

        if len(candidates) > 1:
            conflicts += 1
            if chosen.to_asset != baseline.to_asset:
                changed += 1
            if conflict_log is not None:
                conflict_log.append(
                    {
                        "date": ts.isoformat(),
                        "variant": variant,
                        "start_asset": start_asset,
                        "held_asset": current,
                        "candidate_count": len(candidates),
                        "candidates": "|".join(
                            f"{x.to_asset}:{x.strength:.6f}" for x in sorted(candidates, key=lambda z: z.to_asset)
                        ),
                        "baseline_choice": baseline.to_asset,
                        "chosen": chosen.to_asset,
                        "cycle_ratio": graph["cycle_ratio"] if graph else None,
                        "bt_rank": graph["ranks"]["BRADLEY_TERRY"].get(chosen.to_asset) if graph else None,
                        "pagerank_rank": graph["ranks"]["PAGERANK"].get(chosen.to_asset) if graph else None,
                        "rc_rank": graph["ranks"]["RANK_CENTRALITY"].get(chosen.to_asset) if graph else None,
                    }
                )

        pending = chosen

    series = pd.Series(equity, index=dates, dtype=float)
    total_return = float(series.iloc[-1] / series.iloc[0] - 1.0)
    dd = series / series.cummax() - 1.0
    max_drawdown = float(dd.min())

    return BacktestResult(
        variant=variant,
        start_asset=start_asset,
        period_start=start.date().isoformat(),
        period_end=end.date().isoformat(),
        total_return=total_return,
        max_drawdown=max_drawdown,
        transitions=transitions,
        conflicts=conflicts,
        changed_conflict_choices=changed,
    )


def evaluate_window(
    panel: pd.DataFrame,
    signals_by_date: dict[pd.Timestamp, list[PairSignal]],
    graph_by_date: dict[pd.Timestamp, dict],
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    conflict_log: list[dict],
) -> list[BacktestResult]:
    results: list[BacktestResult] = []
    for variant in RANKERS:
        for asset in ASSETS:
            results.append(
                run_backtest(
                    panel,
                    signals_by_date,
                    graph_by_date,
                    start=start,
                    end=end,
                    start_asset=asset,
                    variant=variant,
                    conflict_log=conflict_log if start == REFERENCE_OOS_START else None,
                )
            )
    return results


def summarize(results: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        results.groupby(["period_start", "period_end", "variant"], as_index=False)
        .agg(
            median_return=("total_return", "median"),
            worst_return=("total_return", "min"),
            median_max_drawdown=("max_drawdown", "median"),
            median_transitions=("transitions", "median"),
            median_conflicts=("conflicts", "median"),
            median_changed_conflicts=("changed_conflict_choices", "median"),
            positive_starts=("total_return", lambda x: int((x > 0).sum())),
        )
    )
    return grouped


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Research-only graph-intelligence layer for the 8-node relative router.")
    p.add_argument("--data-start", default="2023-05-05")
    p.add_argument("--end", default="2026-03-28")
    p.add_argument(
        "--output-root",
        type=Path,
        default=REPO_ROOT / "research_artifacts" / "relative_graph_intelligence",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    data_start = _utc(args.data_start)
    end = _utc(args.end)
    if end != REFERENCE_END:
        raise ValueError(
            "This first graph-intelligence experiment is frozen to 2026-03-28 so it cannot contaminate the untouched defensive validation period."
        )

    panel, data_meta = download_panel(data_start, end)
    signals_by_date, graph_by_date = build_pair_context(panel)

    conflict_log: list[dict] = []
    all_results: list[BacktestResult] = []

    all_results.extend(
        evaluate_window(
            panel,
            signals_by_date,
            graph_by_date,
            start=REFERENCE_OOS_START,
            end=REFERENCE_END,
            conflict_log=conflict_log,
        )
    )

    for start_text, end_text in SEQUENTIAL_180D:
        all_results.extend(
            evaluate_window(
                panel,
                signals_by_date,
                graph_by_date,
                start=_utc(start_text),
                end=_utc(end_text),
                conflict_log=conflict_log,
            )
        )

    result_df = pd.DataFrame([r.__dict__ for r in all_results])
    summary_df = summarize(result_df)

    ref = summary_df[
        (summary_df["period_start"] == REFERENCE_OOS_START.date().isoformat())
        & (summary_df["period_end"] == REFERENCE_END.date().isoformat())
    ].copy()
    baseline_row = ref[ref["variant"] == "BASELINE"].iloc[0]
    baseline_median = float(baseline_row["median_return"])
    baseline_error = baseline_median - REFERENCE_BASELINE_MEDIAN
    baseline_reproduced = abs(baseline_error) <= BASELINE_REPRO_TOLERANCE

    cycle_rows = []
    for ts, graph in graph_by_date.items():
        if REFERENCE_OOS_START <= ts <= REFERENCE_END:
            cycle_rows.append(
                {
                    "date": ts.isoformat(),
                    "cycle_ratio": graph["cycle_ratio"],
                    "cyclic_triples": graph["cyclic_triples"],
                    "bt_top": min(graph["ranks"]["BRADLEY_TERRY"], key=graph["ranks"]["BRADLEY_TERRY"].get),
                    "pagerank_top": min(graph["ranks"]["PAGERANK"], key=graph["ranks"]["PAGERANK"].get),
                    "rc_top": min(graph["ranks"]["RANK_CENTRALITY"], key=graph["ranks"]["RANK_CENTRALITY"].get),
                }
            )

    run_id = f"GRAPH_INTEL_V1_{_source_commit()[:12]}"
    run_dir = args.output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    result_df.to_csv(run_dir / "results_by_start.csv", index=False)
    summary_df.to_csv(run_dir / "window_summary.csv", index=False)
    pd.DataFrame(conflict_log).to_csv(run_dir / "reference_conflicts.csv", index=False)
    pd.DataFrame(cycle_rows).to_csv(run_dir / "graph_diagnostics.csv", index=False)

    report = {
        "run_id": run_id,
        "strategy": "8_NODE_RELATIVE_ROTATION_GRAPH_INTELLIGENCE_V1",
        "status": "RESEARCH_ONLY",
        "source_commit_sha": _source_commit(),
        "dataset": data_meta,
        "universe": list(ASSETS),
        "graph_size": {"nodes": 8, "pairs": 28},
        "base_router": {
            "lookback_days": LOOKBACK,
            "arm_threshold": ARM_THRESHOLD,
            "reversal_confirmation": REVERSAL,
            "execution": "NEXT_DAILY_OPEN",
            "transition_cost": TRANSITION_COST,
        },
        "oss": {
            "evalica": {
                "version": evalica.__version__,
                "license": "Apache-2.0",
                "methods": ["Bradley-Terry", "PageRank"],
                "upstream_commit_reviewed": "c3c89eb81fda382ebd96458e376e515d912e1815",
            },
            "choix": {
                "version": getattr(choix, "__version__", "0.4.1"),
                "license": "MIT",
                "methods": ["Rank Centrality"],
                "upstream_commit_reviewed": "491d8ed9d59035f31049b047ff0d7b820f1c5b59",
            },
        },
        "baseline_reproduction": {
            "documented_reference_median_return": REFERENCE_BASELINE_MEDIAN,
            "reproduced_median_return": baseline_median,
            "difference": baseline_error,
            "tolerance": BASELINE_REPRO_TOLERANCE,
            "pass": baseline_reproduced,
        },
        "interpretation_gate": (
            "COMPARABLE_TO_PRIOR_RESEARCH"
            if baseline_reproduced
            else "BASELINE_REPRODUCTION_MISMATCH_DO_NOT_PROMOTE_GRAPH_RESULTS"
        ),
        "reference_window": ref.to_dict(orient="records"),
    }
    _json(run_dir / "summary.json", report)

    print(f"run_id={run_id}")
    print(f"panel_rows={len(panel)}")
    print(f"panel_start={panel['timestamp'].min().date()}")
    print(f"panel_end={panel['timestamp'].max().date()}")
    print(f"baseline_median={baseline_median:.6f}")
    print(f"baseline_reference={REFERENCE_BASELINE_MEDIAN:.6f}")
    print(f"baseline_reproduced={baseline_reproduced}")
    print("reference_window:")
    print(ref[[
        "variant",
        "median_return",
        "worst_return",
        "median_max_drawdown",
        "median_transitions",
        "median_conflicts",
        "median_changed_conflicts",
        "positive_starts",
    ]].to_string(index=False))
    print("sequential_180d:")
    seq = summary_df[
        ~(
            (summary_df["period_start"] == REFERENCE_OOS_START.date().isoformat())
            & (summary_df["period_end"] == REFERENCE_END.date().isoformat())
        )
    ]
    print(seq[["period_start", "period_end", "variant", "median_return"]].to_string(index=False))
    print(f"output={run_dir}")
    print("status=RESEARCH_ONLY_NO_PRODUCTION_CHANGE")
    return 0 if baseline_reproduced else 4


if __name__ == "__main__":
    raise SystemExit(main())
