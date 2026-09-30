from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import build_pair_monitor

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "u20_deep_dive_v1"
DATA_START = pd.Timestamp("2023-04-01", tz="UTC")
LOOKBACK = 180
ARM = 0.15
REVERSAL = 0.03
COST = 0.001

BASE8 = ("ATOM","TWT","PEPE","BNB","SOL","TRX","AAVE","LINK")
ADDITIONS = ("BTC","ETH","XRP","DOGE","ADA","AVAX","DOT","LTC","BCH","NEAR","UNI","FIL")
U20 = BASE8 + ADDITIONS

PARAM_VARIANTS = {
    "CANONICAL_180_15_3": (180, 0.15, 0.03),
    "L120_A15_R3": (120, 0.15, 0.03),
    "L240_A15_R3": (240, 0.15, 0.03),
    "L180_A12_5_R3": (180, 0.125, 0.03),
    "L180_A20_R3": (180, 0.20, 0.03),
    "L180_A15_R2": (180, 0.15, 0.02),
    "L180_A15_R5": (180, 0.15, 0.05),
}
ROUTERS = ("strongest","weakest","first","skip_conflict")
COSTS = (0.001, 0.0025, 0.005, 0.01)


def utc(x):
    t = pd.Timestamp(x)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def source_sha():
    v = os.environ.get("SOURCE_COMMIT_SHA")
    if v:
        return v
    return subprocess.check_output(["git","rev-parse","HEAD"], cwd=ROOT, text=True).strip()


def max_drawdown(values):
    s = pd.Series(values, dtype=float)
    return float((s / s.cummax() - 1.0).min())


def download_panel(cutoff):
    client = BinanceSpotRestClient()
    panel = None
    metadata = {}
    for asset in U20:
        result = download_historical_dataset(
            client,
            symbol=asset+"USDT",
            start=DATA_START,
            end=cutoff,
            timeframe="1D",
            as_of=cutoff,
        )
        if result.dataset is None:
            raise RuntimeError(f"{asset}: {result.metadata.status}")
        if result.dataset.quality.has_critical_issues:
            raise RuntimeError(f"{asset}: critical data quality issues")
        f = result.dataset.candles[["timestamp","open","close"]].copy()
        f = f.rename(columns={"open":asset+"_open","close":asset+"_close"})
        metadata[asset] = {
            "rows": len(f),
            "start": pd.Timestamp(f.iloc[0]["timestamp"]).isoformat(),
            "end": pd.Timestamp(f.iloc[-1]["timestamp"]).isoformat(),
        }
        panel = f if panel is None else panel.merge(f, on="timestamp", how="inner", validate="one_to_one")
    panel = panel.sort_values("timestamp").reset_index(drop=True)
    return panel, metadata


def event_map(panel, assets, lookback=LOOKBACK, arm=ARM, reversal=REVERSAL):
    cols = ["timestamp"] + [a+"_close" for a in assets]
    events, _ = build_pair_monitor(
        panel[cols].copy(),
        assets=assets,
        lookback=lookback,
        arm_threshold=arm,
        reversal=reversal,
    )
    by_date = defaultdict(list)
    for e in events:
        if e["event"] == "CONFIRMED":
            by_date[utc(e["date"])].append(e)
    return by_date


def choose(candidates, router):
    if not candidates:
        return None
    if router == "skip_conflict":
        return None if len(candidates) > 1 else candidates[0]
    if router == "strongest":
        return sorted(candidates, key=lambda e:(-float(e["max_dislocation"]), e["to_asset"], e["pair"]))[0]
    if router == "weakest":
        return sorted(candidates, key=lambda e:(float(e["max_dislocation"]), e["to_asset"], e["pair"]))[0]
    if router == "first":
        return sorted(candidates, key=lambda e:(e["to_asset"], e["pair"], -float(e["max_dislocation"])))[0]
    raise ValueError(router)


def run_one(panel, emap, start, end, start_asset, *, router="strongest", cost=COST, collect=False):
    w = panel[(panel["timestamp"] >= start) & (panel["timestamp"] <= end)].copy()
    if w.empty:
        raise ValueError("empty window")
    current = start_asset
    qty = 1.0 / float(w.iloc[0][current+"_open"])
    pending = None
    equity = []
    holdings = []
    trades = []
    transitions = 0
    conflicts = 0

    first_row = w.iloc[0]
    last_row = w.iloc[-1]
    hodl_return = float(last_row[start_asset+"_close"] / first_row[start_asset+"_close"] - 1.0)
    eq20_return = float(pd.Series([
        last_row[a+"_close"] / first_row[a+"_close"] - 1.0 for a in U20
    ]).mean())

    for pos, (_, row) in enumerate(w.iterrows()):
        ts = utc(row["timestamp"])
        if pending is not None:
            value = qty * float(row[current+"_open"])
            from_asset = current
            current = pending["to_asset"]
            qty = value * (1.0-cost) / float(row[current+"_open"])
            transitions += 1
            if collect:
                trades.append({
                    "signal_date": pending["date"],
                    "execution_date": ts.isoformat(),
                    "from_asset": from_asset,
                    "to_asset": current,
                    "max_dislocation": float(pending["max_dislocation"]),
                    "candidate_count": int(pending["_candidate_count"]),
                })
            pending = None

        equity.append(qty * float(row[current+"_close"]))
        holdings.append(current)

        if pos == len(w)-1:
            continue
        candidates = [e for e in emap.get(ts, []) if e["from_asset"] == current]
        if not candidates:
            continue
        if len(candidates) > 1:
            conflicts += 1
        selected = choose(candidates, router)
        if selected is not None:
            selected = dict(selected)
            selected["_candidate_count"] = len(candidates)
            pending = selected

    ret = float(equity[-1] / equity[0] - 1.0)
    out = {
        "start_asset": start_asset,
        "return": ret,
        "max_dd": max_drawdown(equity),
        "transitions": transitions,
        "conflicts": conflicts,
        "hodl_return": hodl_return,
        "excess_vs_hodl": ret - hodl_return,
        "equal_weight_u20_return": eq20_return,
        "excess_vs_equal_weight_u20": ret - eq20_return,
    }
    if collect:
        out["holdings"] = holdings
        out["trades"] = trades
    return out


def aggregate_runs(runs):
    df = pd.DataFrame(runs)
    atom = df[df["start_asset"]=="ATOM"].iloc[0]
    return {
        "starts": len(df),
        "median_return": float(df["return"].median()),
        "worst_return": float(df["return"].min()),
        "best_return": float(df["return"].max()),
        "positive_starts": int((df["return"] > 0).sum()),
        "median_max_dd": float(df["max_dd"].median()),
        "worst_max_dd": float(df["max_dd"].min()),
        "median_excess_vs_hodl": float(df["excess_vs_hodl"].median()),
        "beat_hodl_starts": int((df["excess_vs_hodl"] > 0).sum()),
        "median_excess_vs_equal_weight_u20": float(df["excess_vs_equal_weight_u20"].median()),
        "beat_equal_weight_starts": int((df["excess_vs_equal_weight_u20"] > 0).sum()),
        "median_transitions": float(df["transitions"].median()),
        "median_conflicts": float(df["conflicts"].median()),
        "atom_return": float(atom["return"]),
        "atom_max_dd": float(atom["max_dd"]),
        "atom_excess_vs_hodl": float(atom["excess_vs_hodl"]),
        "atom_excess_vs_equal_weight_u20": float(atom["excess_vs_equal_weight_u20"]),
        "atom_transitions": int(atom["transitions"]),
    }


def monthly_windows(panel, mature_start, months):
    last = utc(panel.iloc[-1]["timestamp"])
    cursor = pd.Timestamp(mature_start.year, mature_start.month, 1, tz="UTC")
    if cursor < mature_start:
        cursor = cursor + pd.offsets.MonthBegin(1)
    windows = []
    while True:
        start_candidates = panel[panel["timestamp"] >= cursor]
        if start_candidates.empty:
            break
        start = utc(start_candidates.iloc[0]["timestamp"])
        end_target = start + pd.DateOffset(months=months) - pd.Timedelta("1D")
        if end_target > last:
            break
        end_candidates = panel[panel["timestamp"] <= end_target]
        if end_candidates.empty:
            break
        end = utc(end_candidates.iloc[-1]["timestamp"])
        windows.append((start,end))
        cursor = cursor + pd.offsets.MonthBegin(1)
    return windows


def rolling_eval(panel, emap, mature_start, months, start_assets=BASE8, *, router="strongest", cost=COST):
    rows = []
    for start,end in monthly_windows(panel, mature_start, months):
        runs = [run_one(panel, emap, start, end, a, router=router, cost=cost) for a in start_assets]
        agg = aggregate_runs(runs)
        rows.append({"start":start.isoformat(),"end":end.isoformat(),**agg})
    df = pd.DataFrame(rows)
    if df.empty:
        return [], {"window_count":0}
    summary = {
        "window_count": len(df),
        "median_window_median_return": float(df["median_return"].median()),
        "worst_window_median_return": float(df["median_return"].min()),
        "positive_window_rate": float((df["median_return"] > 0).mean()),
        "median_window_atom_return": float(df["atom_return"].median()),
        "worst_window_atom_return": float(df["atom_return"].min()),
        "atom_positive_window_rate": float((df["atom_return"] > 0).mean()),
        "median_window_max_dd": float(df["median_max_dd"].median()),
        "worst_window_max_dd": float(df["worst_max_dd"].min()),
        "median_excess_vs_hodl": float(df["median_excess_vs_hodl"].median()),
        "positive_median_excess_vs_hodl_rate": float((df["median_excess_vs_hodl"] > 0).mean()),
        "median_excess_vs_equal_weight_u20": float(df["median_excess_vs_equal_weight_u20"].median()),
        "positive_median_excess_vs_equal_weight_rate": float((df["median_excess_vs_equal_weight_u20"] > 0).mean()),
        "median_transitions": float(df["median_transitions"].median()),
        "median_conflicts": float(df["median_conflicts"].median()),
    }
    return rows, summary


def atom_rolling(panel, emap, mature_start, months, *, router="strongest", cost=COST):
    rows = []
    for start,end in monthly_windows(panel, mature_start, months):
        r = run_one(panel, emap, start, end, "ATOM", router=router, cost=cost)
        rows.append({"start":start.isoformat(),"end":end.isoformat(),**r})
    df = pd.DataFrame(rows)
    if df.empty:
        return {"window_count":0}
    return {
        "window_count": len(df),
        "median_return": float(df["return"].median()),
        "worst_return": float(df["return"].min()),
        "positive_rate": float((df["return"] > 0).mean()),
        "median_max_dd": float(df["max_dd"].median()),
        "worst_max_dd": float(df["max_dd"].min()),
        "median_excess_vs_hodl": float(df["excess_vs_hodl"].median()),
        "beat_hodl_rate": float((df["excess_vs_hodl"] > 0).mean()),
        "median_excess_vs_equal_weight_u20": float(df["excess_vs_equal_weight_u20"].median()),
        "beat_equal_weight_rate": float((df["excess_vs_equal_weight_u20"] > 0).mean()),
        "median_transitions": float(df["transitions"].median()),
        "median_conflicts": float(df["conflicts"].median()),
    }


def full_eval(panel, emap, start, end, *, router="strongest", cost=COST):
    runs = [run_one(panel, emap, start, end, a, router=router, cost=cost) for a in BASE8]
    return aggregate_runs(runs)


def compact_case(panel, emap, mature_start, end, *, router="strongest", cost=COST):
    return {
        "full_mature": full_eval(panel, emap, mature_start, end, router=router, cost=cost),
        "atom_12m": atom_rolling(panel, emap, mature_start, 12, router=router, cost=cost),
        "atom_24m": atom_rolling(panel, emap, mature_start, 24, router=router, cost=cost),
    }


def centrality(panel, emap, mature_start, end):
    in_count = Counter()
    out_count = Counter()
    occupancy = Counter()
    edges = Counter()
    conflict_trades = Counter()
    total_days = 0
    for start_asset in BASE8:
        r = run_one(panel, emap, mature_start, end, start_asset, collect=True)
        total_days += len(r["holdings"])
        occupancy.update(r["holdings"])
        for t in r["trades"]:
            out_count[t["from_asset"]] += 1
            in_count[t["to_asset"]] += 1
            edges[(t["from_asset"],t["to_asset"])] += 1
            if t["candidate_count"] > 1:
                conflict_trades[t["to_asset"]] += 1
    nodes = []
    for a in U20:
        nodes.append({
            "asset":a,
            "entries":in_count[a],
            "exits":out_count[a],
            "occupancy_days":occupancy[a],
            "occupancy_share":occupancy[a]/total_days if total_days else 0.0,
            "chosen_from_conflicts":conflict_trades[a],
            "bridge_activity":min(in_count[a],out_count[a]),
        })
    edge_rows = [
        {"from_asset":a,"to_asset":b,"count":n}
        for (a,b),n in edges.most_common()
    ]
    return nodes, edge_rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cutoff", default="2026-09-27T00:00:00Z")
    args = ap.parse_args()
    cutoff = utc(args.cutoff)
    panel, metadata = download_panel(cutoff)
    common_start = utc(panel.iloc[0]["timestamp"])
    end = utc(panel.iloc[-1]["timestamp"])
    mature_start = utc(panel.iloc[LOOKBACK-1]["timestamp"])

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    canonical_u20 = event_map(panel, U20)
    canonical_u8 = event_map(panel, BASE8)

    payload = {
        "generated_at":pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit":source_sha(),
        "cutoff":cutoff.isoformat(),
        "common_start":common_start.isoformat(),
        "mature_start":mature_start.isoformat(),
        "end":end.isoformat(),
        "mature_days":int((end-mature_start).days+1),
        "mature_months_approx":float((end-mature_start).days/30.4375),
        "has_mature_36m":bool(mature_start + pd.DateOffset(months=36) <= end),
        "data":metadata,
        "canonical":{},
        "ablation":{},
        "marginal_add":{},
        "router_sensitivity":{},
        "parameter_sensitivity":{},
        "cost_sensitivity":{},
    }

    for name,assets,emap in (("U8",BASE8,canonical_u8),("U20",U20,canonical_u20)):
        full_available = full_eval(panel, emap, common_start, end)
        full_mature = full_eval(panel, emap, mature_start, end)
        roll12_rows, roll12 = rolling_eval(panel, emap, mature_start, 12)
        roll24_rows, roll24 = rolling_eval(panel, emap, mature_start, 24)
        payload["canonical"][name] = {
            "full_available":full_available,
            "full_mature":full_mature,
            "rolling_12m":roll12,
            "rolling_24m":roll24,
        }
        pd.DataFrame(roll12_rows).to_csv(run_dir/f"{name.lower()}_rolling_12m.csv", index=False)
        pd.DataFrame(roll24_rows).to_csv(run_dir/f"{name.lower()}_rolling_24m.csv", index=False)

    baseline_case = compact_case(panel, canonical_u20, mature_start, end)

    for asset in ADDITIONS:
        reduced = tuple(a for a in U20 if a != asset)
        emap = event_map(panel, reduced)
        case = compact_case(panel, emap, mature_start, end)
        case["delta_full_median_vs_u20"] = case["full_mature"]["median_return"] - baseline_case["full_mature"]["median_return"]
        case["delta_full_atom_vs_u20"] = case["full_mature"]["atom_return"] - baseline_case["full_mature"]["atom_return"]
        payload["ablation"][asset] = case

    for asset in ADDITIONS:
        assets = BASE8 + (asset,)
        emap = event_map(panel, assets)
        case = compact_case(panel, emap, mature_start, end)
        payload["marginal_add"][asset] = case

    nodes, edges = centrality(panel, canonical_u20, mature_start, end)
    payload["node_centrality"] = nodes
    payload["edge_usage"] = edges
    pd.DataFrame(nodes).sort_values(["bridge_activity","occupancy_share"], ascending=[False,False]).to_csv(run_dir/"node_centrality.csv", index=False)
    pd.DataFrame(edges).to_csv(run_dir/"edge_usage.csv", index=False)

    for router in ROUTERS:
        payload["router_sensitivity"][router] = compact_case(
            panel, canonical_u20, mature_start, end, router=router
        )

    for name,(lookback,arm,reversal) in PARAM_VARIANTS.items():
        emap = canonical_u20 if name=="CANONICAL_180_15_3" else event_map(
            panel,U20,lookback=lookback,arm=arm,reversal=reversal
        )
        pstart = utc(panel.iloc[lookback-1]["timestamp"])
        payload["parameter_sensitivity"][name] = {
            "lookback":lookback,"arm":arm,"reversal":reversal,
            **compact_case(panel, emap, pstart, end)
        }

    for cost in COSTS:
        payload["cost_sensitivity"][f"{100*cost:.2f}%"] = compact_case(
            panel, canonical_u20, mature_start, end, cost=cost
        )

    ablation_rows = []
    for asset,case in payload["ablation"].items():
        ablation_rows.append({
            "asset":asset,
            "full_median":case["full_mature"]["median_return"],
            "full_atom":case["full_mature"]["atom_return"],
            "delta_full_median_vs_u20":case["delta_full_median_vs_u20"],
            "delta_full_atom_vs_u20":case["delta_full_atom_vs_u20"],
            "atom_12m_median":case["atom_12m"].get("median_return"),
            "atom_12m_worst":case["atom_12m"].get("worst_return"),
            "atom_24m_median":case["atom_24m"].get("median_return"),
            "atom_24m_worst":case["atom_24m"].get("worst_return"),
        })
    pd.DataFrame(ablation_rows).sort_values("delta_full_atom_vs_u20").to_csv(run_dir/"ablation.csv", index=False)

    marginal_rows = []
    for asset,case in payload["marginal_add"].items():
        marginal_rows.append({
            "asset":asset,
            "full_median":case["full_mature"]["median_return"],
            "full_atom":case["full_mature"]["atom_return"],
            "atom_12m_median":case["atom_12m"].get("median_return"),
            "atom_24m_median":case["atom_24m"].get("median_return"),
        })
    pd.DataFrame(marginal_rows).sort_values("full_atom", ascending=False).to_csv(run_dir/"marginal_add.csv", index=False)

    (run_dir/"results.json").write_text(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False), encoding="utf-8")

    u8 = payload["canonical"]["U8"]
    u20 = payload["canonical"]["U20"]
    report = [
        "# U20 Relative Rotation Deep Dive v1","",
        "Mode: STRESS_TEST_ONLY",
        f"Common data: {common_start.date()} -> {end.date()}",
        f"Mature start after 180 rows: {mature_start.date()}",
        f"Mature span: {payload['mature_months_approx']:.1f} months",
        f"Full mature 36m available: {payload['has_mature_36m']}",
        "",
        "## U8 vs U20 long-cycle",
        "",
        "| Metric | U8 | U20 |",
        "|---|---:|---:|",
        f"| Full mature median | {100*u8['full_mature']['median_return']:+.2f}% | {100*u20['full_mature']['median_return']:+.2f}% |",
        f"| Full mature ATOM | {100*u8['full_mature']['atom_return']:+.2f}% | {100*u20['full_mature']['atom_return']:+.2f}% |",
        f"| Full mature median DD | {100*u8['full_mature']['median_max_dd']:+.2f}% | {100*u20['full_mature']['median_max_dd']:+.2f}% |",
        f"| 12m rolling median of window medians | {100*u8['rolling_12m']['median_window_median_return']:+.2f}% | {100*u20['rolling_12m']['median_window_median_return']:+.2f}% |",
        f"| 12m worst window median | {100*u8['rolling_12m']['worst_window_median_return']:+.2f}% | {100*u20['rolling_12m']['worst_window_median_return']:+.2f}% |",
        f"| 24m rolling median of window medians | {100*u8['rolling_24m']['median_window_median_return']:+.2f}% | {100*u20['rolling_24m']['median_window_median_return']:+.2f}% |",
        f"| 24m worst window median | {100*u8['rolling_24m']['worst_window_median_return']:+.2f}% | {100*u20['rolling_24m']['worst_window_median_return']:+.2f}% |",
        "",
        "## Guardrails",
        "",
        "- No live monitor changes.",
        "- No automatic trading.",
        "- 36m mature rolling test is not invented if history is insufficient.",
        "- This is one historical sample; ablation and sensitivity identify fragility but do not create independent future validation.",
    ]
    (run_dir/"report.md").write_text("\n".join(report)+"\n", encoding="utf-8")

    print("run_dir="+str(run_dir))
    print("mature_start="+mature_start.isoformat())
    print("mature_months=%.3f" % payload["mature_months_approx"])
    print("mature_36m_available="+str(payload["has_mature_36m"]))
    print("U8_full_median=%.6f" % u8["full_mature"]["median_return"])
    print("U20_full_median=%.6f" % u20["full_mature"]["median_return"])
    print("U8_full_atom=%.6f" % u8["full_mature"]["atom_return"])
    print("U20_full_atom=%.6f" % u20["full_mature"]["atom_return"])
    print("U8_12m_worst=%.6f" % u8["rolling_12m"]["worst_window_median_return"])
    print("U20_12m_worst=%.6f" % u20["rolling_12m"]["worst_window_median_return"])
    print("U8_24m_worst=%.6f" % u8["rolling_24m"]["worst_window_median_return"])
    print("U20_24m_worst=%.6f" % u20["rolling_24m"]["worst_window_median_return"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
