from __future__ import annotations

import argparse
import itertools
import json
import os
import subprocess
from collections import defaultdict
from pathlib import Path

import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import build_pair_monitor

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "u8_origin_selection_audit_v1"

SEEDS = ("ATOM", "TWT")
POOL = (
    "ATOM","TWT","PEPE","BNB","SOL","TRX","AAVE","LINK",
    "AVAX","FIL","ETH","ALGO","ADA","XRP","HBAR",
)
ORIGINAL_U8 = ("ATOM","TWT","PEPE","BNB","SOL","TRX","AAVE","LINK")
ORIGINAL_PEPE_TOP5 = {"BNB","SOL","TRX","AAVE","LINK"}

LOOKBACK = 180
ARM = 0.15
REVERSAL = 0.03
COST = 0.001
DATA_START = pd.Timestamp("2023-05-05", tz="UTC")

CASES = {
    "PRIMARY_2024_10_31": {
        "selection_cutoff": "2024-10-31",
        "train_start": "2023-10-31",
        "train_end": "2024-10-31",
        "oos_start": "2024-11-01",
        "oos_end": "2026-09-26",
    },
    "SECONDARY_2025_03_28": {
        "selection_cutoff": "2025-03-28",
        "train_start": "2024-03-29",
        "train_end": "2025-03-28",
        "oos_start": "2025-03-29",
        "oos_end": "2026-09-26",
    },
}


def utc(value):
    ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def source_sha():
    v = os.environ.get("SOURCE_COMMIT_SHA")
    if v:
        return v
    return subprocess.check_output(["git","rev-parse","HEAD"], cwd=ROOT, text=True).strip()


def download_panel(cutoff):
    client = BinanceSpotRestClient()
    panel = None
    metadata = {}
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


def make_event_map(panel):
    close_cols = ["timestamp"] + [a+"_close" for a in POOL]
    events, _ = build_pair_monitor(
        panel[close_cols].copy(),
        assets=POOL,
        lookback=LOOKBACK,
        arm_threshold=ARM,
        reversal=REVERSAL,
    )
    by_date = defaultdict(list)
    for e in events:
        if e["event"] == "CONFIRMED":
            by_date[utc(e["date"])].append(e)
    return by_date


def window_rows(panel, start, end):
    w = panel[(panel["timestamp"] >= start) & (panel["timestamp"] <= end)].copy()
    if w.empty:
        raise ValueError(f"empty window {start} -> {end}")
    return w


def _prepared_window(panel, assets, start, end):
    w = window_rows(panel, start, end)
    timestamps = [utc(x) for x in w["timestamp"].tolist()]
    opens = {a:w[a+"_open"].to_numpy() for a in assets}
    closes = {a:w[a+"_close"].to_numpy() for a in assets}
    return timestamps, opens, closes


def _run_prepared(event_map, assets, timestamps, opens, closes, start_asset):
    aset = set(assets)
    if start_asset not in aset:
        raise ValueError(start_asset)
    current = start_asset
    qty = 1.0 / float(opens[current][0])
    pending = None
    equity = []
    transitions = 0
    conflicts = 0
    route = [current]

    for pos, ts in enumerate(timestamps):
        if pending is not None:
            value = qty * float(opens[current][pos])
            current = pending["to_asset"]
            qty = value * (1.0 - COST) / float(opens[current][pos])
            transitions += 1
            route.append(current)
            pending = None

        equity.append(qty * float(closes[current][pos]))

        if pos == len(timestamps)-1:
            continue
        candidates = [
            e for e in event_map.get(ts, [])
            if e["from_asset"] == current and e["to_asset"] in aset
        ]
        if not candidates:
            continue
        if len(candidates) > 1:
            conflicts += 1
        pending = sorted(
            candidates,
            key=lambda e:(-float(e["max_dislocation"]), e["to_asset"], e["pair"]),
        )[0]

    series = pd.Series(equity, dtype=float)
    ret = float(series.iloc[-1] / series.iloc[0] - 1.0)
    dd = float((series / series.cummax() - 1.0).min())
    return {
        "return":ret,
        "max_dd":dd,
        "transitions":transitions,
        "conflicts":conflicts,
        "route":route,
    }


def run_graph(panel, event_map, assets, start, end, start_asset):
    timestamps, opens, closes = _prepared_window(panel, assets, start, end)
    return _run_prepared(event_map, assets, timestamps, opens, closes, start_asset)


def graph_summary(panel, event_map, assets, start, end):
    timestamps, opens, closes = _prepared_window(panel, assets, start, end)
    runs = []
    for a in assets:
        r = _run_prepared(event_map, assets, timestamps, opens, closes, a)
        runs.append({"start_asset":a, **r})
    df = pd.DataFrame(runs)
    atom = df[df["start_asset"]=="ATOM"].iloc[0]
    return {
        "median_return":float(df["return"].median()),
        "worst_return":float(df["return"].min()),
        "best_return":float(df["return"].max()),
        "positive_starts":int((df["return"]>0).sum()),
        "median_max_dd":float(df["max_dd"].median()),
        "worst_max_dd":float(df["max_dd"].min()),
        "median_transitions":float(df["transitions"].median()),
        "median_conflicts":float(df["conflicts"].median()),
        "atom_return":float(atom["return"]),
        "atom_max_dd":float(atom["max_dd"]),
        "atom_route":atom["route"],
    }


def pair_window_starts(panel, cutoff):
    mature_start = utc(panel.iloc[LOOKBACK-1]["timestamp"])
    starts = []
    cursor = mature_start
    while True:
        end_target = cursor + pd.DateOffset(years=1) - pd.Timedelta("1D")
        if end_target > cutoff:
            break
        starts.append((cursor, end_target))
        cursor = cursor + pd.Timedelta(days=60)
    return starts


def pair_run(panel, event_map, left, right, start, end, start_asset):
    assets = (left, right)
    return run_graph(panel, event_map, assets, start, end, start_asset)["return"]


def pair_scores(panel, event_map, cutoff):
    rows = []
    for left, right in itertools.combinations(POOL, 2):
        observations = []
        for start, end in pair_window_starts(panel, cutoff):
            w = window_rows(panel, start, end)
            first = w.iloc[0]
            last = w.iloc[-1]
            hodl_left = float(last[left+"_close"] / first[left+"_close"] - 1.0)
            hodl_right = float(last[right+"_close"] / first[right+"_close"] - 1.0)
            fifty = 0.5 * (hodl_left + hodl_right)
            best_hodl = max(hodl_left, hodl_right)
            for starter in (left, right):
                strat = pair_run(panel, event_map, left, right, start, end, starter)
                observations.append({
                    "strategy":strat,
                    "excess_50":strat-fifty,
                    "excess_best":strat-best_hodl,
                })
        if not observations:
            continue
        df = pd.DataFrame(observations)
        rows.append({
            "left":left,
            "right":right,
            "pair":left+"/"+right,
            "observations":len(df),
            "median_excess_50":float(df["excess_50"].median()),
            "beat_50_rate":float((df["excess_50"]>0).mean()),
            "median_excess_best":float(df["excess_best"].median()),
            "median_strategy_return":float(df["strategy"].median()),
        })
    return pd.DataFrame(rows)


def neighbor_ranking(score_df, hub):
    rows = []
    for _, row in score_df.iterrows():
        if row["left"] == hub:
            neighbor = row["right"]
        elif row["right"] == hub:
            neighbor = row["left"]
        else:
            continue
        if neighbor in SEEDS or neighbor == hub:
            continue
        rows.append({**row.to_dict(), "neighbor":neighbor})
    ranked = pd.DataFrame(rows)
    if ranked.empty:
        return ranked
    return ranked.sort_values(
        ["median_excess_50","beat_50_rate","median_excess_best","neighbor"],
        ascending=[False,False,False,True],
        kind="stable",
    ).reset_index(drop=True)


def hub_universe(score_df, hub):
    ranked = neighbor_ranking(score_df, hub)
    top5 = ranked.head(5)["neighbor"].tolist()
    if len(top5) != 5:
        raise RuntimeError(f"not enough neighbours for {hub}")
    assets = list(SEEDS)
    if hub not in assets:
        assets.append(hub)
    for a in top5:
        if a not in assets:
            assets.append(a)
    if len(assets) < 8:
        for a in ranked["neighbor"].tolist()[5:]:
            if a not in assets:
                assets.append(a)
            if len(assets) == 8:
                break
    if len(assets) != 8:
        raise RuntimeError(f"hub set {hub} produced {len(assets)} assets")
    return tuple(assets), top5


def set_key(assets):
    return "|".join(sorted(assets))


def exhaustive_sets():
    remaining = [a for a in POOL if a not in SEEDS]
    for extra in itertools.combinations(remaining, 6):
        yield tuple(SEEDS + extra)


def evaluate_case(panel, event_map, name, cfg, run_dir):
    cutoff = utc(cfg["selection_cutoff"])
    train_start = utc(cfg["train_start"])
    train_end = utc(cfg["train_end"])
    oos_start = utc(cfg["oos_start"])
    oos_end = utc(cfg["oos_end"])

    scores = pair_scores(panel, event_map, cutoff)
    scores.to_csv(run_dir/f"{name.lower()}_pair_scores.csv", index=False)

    star_assets, star_top5 = hub_universe(scores, "PEPE")

    hub_rows = []
    for hub in [a for a in POOL if a not in SEEDS]:
        assets, top5 = hub_universe(scores, hub)
        tr = graph_summary(panel,event_map,assets,train_start,train_end)
        oo = graph_summary(panel,event_map,assets,oos_start,oos_end)
        hub_rows.append({
            "hub":hub,
            "assets":"|".join(assets),
            "top5":"|".join(top5),
            "train_median":tr["median_return"],
            "train_atom":tr["atom_return"],
            "oos_median":oo["median_return"],
            "oos_atom":oo["atom_return"],
            "oos_dd":oo["median_max_dd"],
        })
    hub_df = pd.DataFrame(hub_rows).sort_values(
        ["train_median","hub"],ascending=[False,True],kind="stable"
    ).reset_index(drop=True)
    hub_df["train_rank"] = range(1,len(hub_df)+1)
    hub_df["oos_rank"] = hub_df["oos_median"].rank(method="min",ascending=False).astype(int)
    hub_df.to_csv(run_dir/f"{name.lower()}_hub_sets.csv", index=False)

    exhaustive_rows = []
    for assets in exhaustive_sets():
        tr = graph_summary(panel,event_map,assets,train_start,train_end)
        oo = graph_summary(panel,event_map,assets,oos_start,oos_end)
        exhaustive_rows.append({
            "key":set_key(assets),
            "assets":"|".join(assets),
            "train_median":tr["median_return"],
            "train_atom":tr["atom_return"],
            "train_dd":tr["median_max_dd"],
            "oos_median":oo["median_return"],
            "oos_atom":oo["atom_return"],
            "oos_dd":oo["median_max_dd"],
            "oos_positive_starts":oo["positive_starts"],
        })

    ex = pd.DataFrame(exhaustive_rows)
    ex["train_rank"] = ex["train_median"].rank(method="min",ascending=False).astype(int)
    ex["oos_rank"] = ex["oos_median"].rank(method="min",ascending=False).astype(int)
    ex = ex.sort_values(["train_rank","key"],kind="stable").reset_index(drop=True)
    ex.to_csv(run_dir/f"{name.lower()}_all_1716_sets.csv", index=False)

    train_rank_series = ex["train_median"].rank(method="average",ascending=False)
    oos_rank_series = ex["oos_median"].rank(method="average",ascending=False)
    rank_corr = float(train_rank_series.corr(oos_rank_series))

    original_key = set_key(ORIGINAL_U8)
    star_key = set_key(star_assets)
    original = ex[ex["key"]==original_key].iloc[0].to_dict()
    star = ex[ex["key"]==star_key].iloc[0].to_dict()
    top_train = ex.sort_values(["train_rank","key"]).iloc[0].to_dict()
    best_oos = ex.sort_values(["oos_rank","key"]).iloc[0].to_dict()

    original_oos = float(original["oos_median"])
    beat_original_count = int((ex["oos_median"] > original_oos).sum())

    hub_original = hub_df[hub_df["hub"]=="PEPE"].iloc[0]
    hub_beat_original = int((hub_df["oos_median"] > original_oos).sum())

    return {
        "selection_cutoff":cutoff.isoformat(),
        "train_start":train_start.isoformat(),
        "train_end":train_end.isoformat(),
        "oos_start":oos_start.isoformat(),
        "oos_end":oos_end.isoformat(),
        "pair_windows":len(pair_window_starts(panel,cutoff)),
        "causal_pepe_top5":star_top5,
        "causal_pepe_u8":list(star_assets),
        "causal_pepe_matches_original":set(star_assets)==set(ORIGINAL_U8),
        "rank_correlation_train_vs_oos":rank_corr,
        "total_u8_sets":len(ex),
        "original_u8":original,
        "causal_pepe_u8_result":star,
        "top_training_u8":top_train,
        "best_future_u8_hindsight_only":best_oos,
        "sets_beating_original_u8_oos":beat_original_count,
        "hub_sets_beating_original_u8_oos":hub_beat_original,
        "pepe_hub_train_rank":int(hub_original["train_rank"]),
        "pepe_hub_oos_rank":int(hub_original["oos_rank"]),
        "top5_training_sets":ex.nsmallest(5,"train_rank").to_dict(orient="records"),
        "top5_oos_sets_hindsight_only":ex.nsmallest(5,"oos_rank").to_dict(orient="records"),
        "hub_table":hub_df.to_dict(orient="records"),
    }


def historical_reconstruction(panel,event_map):
    cutoff=utc("2026-03-28")
    scores=pair_scores(panel,event_map,cutoff)
    assets,top5=hub_universe(scores,"PEPE")
    return {
        "cutoff":cutoff.isoformat(),
        "top5":top5,
        "u8":list(assets),
        "top5_overlap_with_documented":len(set(top5)&ORIGINAL_PEPE_TOP5),
        "matches_documented_top5":set(top5)==ORIGINAL_PEPE_TOP5,
    }


def write_report(payload, run_dir):
    lines=[
        "# U8 Origin & Selection Audit v1","",
        "Mode: STRESS_TEST_ONLY",
        "Live graph remains unchanged.","",
        "Historical origin: ATOM/TWT seed + PEPE hub + five historically strongest PEPE neighbours.",
        "",
        "## Historical reconstruction check","",
        f"Reconstructed PEPE top5 at 2026-03-28: {', '.join(payload['historical_reconstruction']['top5'])}",
        f"Overlap with documented top5: {payload['historical_reconstruction']['top5_overlap_with_documented']}/5",
        "",
    ]
    for name,res in payload["cases"].items():
        o=res["original_u8"]
        s=res["causal_pepe_u8_result"]
        t=res["top_training_u8"]
        lines += [
            f"## {name}","",
            f"Selection cutoff: {res['selection_cutoff']}",
            f"Future: {res['oos_start']} -> {res['oos_end']}",
            f"Pair-screen windows available: {res['pair_windows']}",
            f"Causal PEPE top5: {', '.join(res['causal_pepe_top5'])}",
            f"Causal PEPE set matches historical U8: {res['causal_pepe_matches_original']}",
            "",
            "| Set | Train median | Future median | Future ATOM | Future rank / 1716 |",
            "|---|---:|---:|---:|---:|",
            f"| Historical U8 | {100*o['train_median']:+.2f}% | {100*o['oos_median']:+.2f}% | {100*o['oos_atom']:+.2f}% | {int(o['oos_rank'])} |",
            f"| Causal PEPE U8 | {100*s['train_median']:+.2f}% | {100*s['oos_median']:+.2f}% | {100*s['oos_atom']:+.2f}% | {int(s['oos_rank'])} |",
            f"| Top TRAIN U8 | {100*t['train_median']:+.2f}% | {100*t['oos_median']:+.2f}% | {100*t['oos_atom']:+.2f}% | {int(t['oos_rank'])} |",
            "",
            f"Train-vs-future rank correlation across all 1,716 sets: {res['rank_correlation_train_vs_oos']:+.3f}",
            f"Sets beating historical U8 in future: {res['sets_beating_original_u8_oos']} / 1716",
            f"Hub-style sets beating historical U8 in future: {res['hub_sets_beating_original_u8_oos']} / 13",
            f"PEPE hub rank by training: {res['pepe_hub_train_rank']} / 13",
            f"PEPE hub rank by future: {res['pepe_hub_oos_rank']} / 13",
            "",
        ]
    lines += [
        "## Guardrails","",
        "- Best future set is hindsight-only and is not a recommendation.",
        "- Live U8 is unchanged.",
        "- No parameter tuning was performed.",
        "- Overlapping pair-screen windows are not statistically independent.",
    ]
    (run_dir/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cutoff",default="2026-09-27T00:00:00Z")
    args=ap.parse_args()
    cutoff=utc(args.cutoff)
    panel,metadata=download_panel(cutoff)
    event_map=make_event_map(panel)

    run_dir=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True,exist_ok=True)

    payload={
        "generated_at":pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit":source_sha(),
        "data":metadata,
        "pool":list(POOL),
        "original_u8":list(ORIGINAL_U8),
        "historical_reconstruction":historical_reconstruction(panel,event_map),
        "cases":{},
    }

    for name,cfg in CASES.items():
        payload["cases"][name]=evaluate_case(panel,event_map,name,cfg,run_dir)

    (run_dir/"results.json").write_text(
        json.dumps(payload,indent=2,sort_keys=True,allow_nan=False),
        encoding="utf-8",
    )
    write_report(payload,run_dir)

    print("run_dir="+str(run_dir))
    hr=payload["historical_reconstruction"]
    print("reconstruction_top5="+",".join(hr["top5"]))
    print("reconstruction_overlap="+str(hr["top5_overlap_with_documented"]))
    for name,res in payload["cases"].items():
        print(name+"_causal_pepe="+",".join(res["causal_pepe_u8"]))
        print(name+"_causal_match="+str(res["causal_pepe_matches_original"]))
        print(name+"_original_oos=%.6f" % res["original_u8"]["oos_median"])
        print(name+"_original_rank="+str(int(res["original_u8"]["oos_rank"])))
        print(name+"_star_oos=%.6f" % res["causal_pepe_u8_result"]["oos_median"])
        print(name+"_star_rank="+str(int(res["causal_pepe_u8_result"]["oos_rank"])))
        print(name+"_train_top_oos=%.6f" % res["top_training_u8"]["oos_median"])
        print(name+"_train_top_rank="+str(int(res["top_training_u8"]["oos_rank"])))
        print(name+"_rank_corr=%.6f" % res["rank_correlation_train_vs_oos"])
    return 0


if __name__=="__main__":
    raise SystemExit(main())
