from __future__ import annotations

import argparse
import itertools
import json
import math
import os
import subprocess
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import build_pair_monitor

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "u8_diversity_selection_engine_v1"

SEEDS = ("ATOM","TWT")
POOL = (
    "ATOM","TWT","PEPE","BNB","SOL","TRX","AAVE","LINK",
    "AVAX","FIL","ETH","ALGO","ADA","XRP","HBAR",
)
ORIGINAL_U8 = ("ATOM","TWT","PEPE","BNB","SOL","TRX","AAVE","LINK")
NICHES = {
    "ATOM":"INTEROPERABILITY",
    "TWT":"WALLET",
    "PEPE":"MEME",
    "BNB":"EXCHANGE_PLATFORM",
    "SOL":"SMART_CONTRACT_L1",
    "TRX":"PAYMENTS",
    "AAVE":"LENDING",
    "LINK":"ORACLE",
    "AVAX":"SMART_CONTRACT_L1",
    "FIL":"DECENTRALIZED_STORAGE",
    "ETH":"SMART_CONTRACT_L1",
    "ALGO":"SMART_CONTRACT_L1",
    "ADA":"SMART_CONTRACT_L1",
    "XRP":"PAYMENTS",
    "HBAR":"ENTERPRISE_DLT",
}
LOOKBACK = 180
ARM = 0.15
REVERSAL = 0.03
COST = 0.001
DATA_START = pd.Timestamp("2023-05-05", tz="UTC")

CASES = {
    "PRIMARY_2024_10_31": {
        "train_start":"2023-10-31",
        "train_end":"2024-10-31",
        "future_start":"2024-11-01",
        "future_end":"2026-09-26",
    },
    "SECONDARY_2025_03_28": {
        "train_start":"2024-03-29",
        "train_end":"2025-03-28",
        "future_start":"2025-03-29",
        "future_end":"2026-09-26",
    },
}


def utc(x):
    t = pd.Timestamp(x)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def source_sha():
    v = os.environ.get("SOURCE_COMMIT_SHA")
    if v:
        return v
    return subprocess.check_output(["git","rev-parse","HEAD"], cwd=ROOT, text=True).strip()


def download_panel(cutoff):
    client = BinanceSpotRestClient()
    panel = None
    meta = {}
    for asset in POOL:
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
            raise RuntimeError(f"{asset}: critical data")
        f = result.dataset.candles[["timestamp","open","close"]].copy()
        f = f.rename(columns={"open":asset+"_open","close":asset+"_close"})
        meta[asset] = {
            "rows":len(f),
            "start":pd.Timestamp(f.iloc[0]["timestamp"]).isoformat(),
            "end":pd.Timestamp(f.iloc[-1]["timestamp"]).isoformat(),
        }
        panel = f if panel is None else panel.merge(f,on="timestamp",how="inner",validate="one_to_one")
    return panel.sort_values("timestamp").reset_index(drop=True), meta


def build_events(panel):
    cols = ["timestamp"] + [a+"_close" for a in POOL]
    events, _ = build_pair_monitor(
        panel[cols].copy(),
        assets=POOL,
        lookback=LOOKBACK,
        arm_threshold=ARM,
        reversal=REVERSAL,
    )
    by_date = defaultdict(list)
    confirmed = []
    for e in events:
        if e["event"] == "CONFIRMED":
            x = dict(e)
            x["_date"] = utc(e["date"])
            by_date[x["_date"]].append(x)
            confirmed.append(x)
    return by_date, confirmed


def combinations_u8():
    rest = [a for a in POOL if a not in SEEDS]
    for extra in itertools.combinations(rest,6):
        yield tuple(SEEDS + extra)


def key(assets):
    return "|".join(sorted(assets))


def niche_count(assets):
    return len({NICHES[a] for a in assets})


def entropy(counter, possible=None):
    total = sum(counter.values())
    if total <= 0:
        return 0.0
    vals = [v/total for v in counter.values() if v > 0]
    h = -sum(p*math.log(p) for p in vals)
    denom_n = possible if possible is not None else len(vals)
    if denom_n <= 1:
        return 0.0
    return float(h / math.log(denom_n))


def route_run(panel, events_by_date, assets, start, end, start_asset, collect=False):
    aset = set(assets)
    w = panel[(panel["timestamp"]>=start)&(panel["timestamp"]<=end)].copy()
    current = start_asset
    qty = 1.0 / float(w.iloc[0][current+"_open"])
    pending = None
    equity = []
    holdings = Counter()
    edges = Counter()
    transition_count = 0
    conflict_selected = 0

    for pos, (_, row) in enumerate(w.iterrows()):
        ts = utc(row["timestamp"])
        if pending is not None:
            value = qty * float(row[current+"_open"])
            old = current
            current = pending["to_asset"]
            qty = value * (1.0-COST) / float(row[current+"_open"])
            edges[(old,current)] += 1
            transition_count += 1
            if pending["_candidate_count"] > 1:
                conflict_selected += 1
            pending = None

        holdings[current] += 1
        equity.append(qty * float(row[current+"_close"]))

        if pos == len(w)-1:
            continue
        cands = [
            e for e in events_by_date.get(ts,[])
            if e["from_asset"]==current and e["to_asset"] in aset
        ]
        if not cands:
            continue
        chosen = sorted(
            cands,
            key=lambda e:(-float(e["max_dislocation"]),e["to_asset"],e["pair"])
        )[0]
        pending = dict(chosen)
        pending["_candidate_count"] = len(cands)

    s = pd.Series(equity,dtype=float)
    out = {
        "return":float(s.iloc[-1]/s.iloc[0]-1.0),
        "max_dd":float((s/s.cummax()-1.0).min()),
        "transitions":transition_count,
        "conflict_selected":conflict_selected,
    }
    if collect:
        out["holdings"] = holdings
        out["edges"] = edges
    return out


def graph_summary(panel, events_by_date, assets, start, end):
    rows=[]
    for a in assets:
        r=route_run(panel,events_by_date,assets,start,end,a)
        rows.append({"start_asset":a,**r})
    df=pd.DataFrame(rows)
    atom=df[df["start_asset"]=="ATOM"].iloc[0]
    return {
        "median_return":float(df["return"].median()),
        "worst_return":float(df["return"].min()),
        "positive_starts":int((df["return"]>0).sum()),
        "median_max_dd":float(df["max_dd"].median()),
        "atom_return":float(atom["return"]),
    }


def structural_features(panel, events_by_date, confirmed, assets, start, end):
    aset=set(assets)
    w=panel[(panel["timestamp"]>=start)&(panel["timestamp"]<=end)].copy()

    rets=pd.DataFrame({
        a:np.log(w[a+"_close"].astype(float)).diff()
        for a in assets
    }).dropna()

    corr=rets.corr().to_numpy()
    tri=np.triu_indices(len(assets),k=1)
    mean_abs_corr=float(np.mean(np.abs(corr[tri])))

    cov=np.cov(rets.to_numpy(),rowvar=False)
    eig=np.linalg.eigvalsh(cov)
    pca1_share=float(eig[-1]/eig.sum()) if eig.sum()>0 else 1.0

    relative_vols=[]
    for a,b in itertools.combinations(assets,2):
        relative_vols.append(float((rets[b]-rets[a]).std(ddof=1)))
    mean_relative_vol=float(np.mean(relative_vols))

    signal_edges=Counter()
    for e in confirmed:
        if e["_date"]<start or e["_date"]>end:
            continue
        if e["from_asset"] in aset and e["to_asset"] in aset:
            signal_edges[(e["from_asset"],e["to_asset"])] += 1
    signal_edge_entropy=entropy(signal_edges,possible=len(assets)*(len(assets)-1))

    holdings=Counter()
    route_edges=Counter()
    total_transitions=0
    conflict_selected=0
    for starter in assets:
        r=route_run(panel,events_by_date,assets,start,end,starter,collect=True)
        holdings.update(r["holdings"])
        route_edges.update(r["edges"])
        total_transitions += r["transitions"]
        conflict_selected += r["conflict_selected"]

    occupancy_entropy=entropy(holdings,possible=len(assets))
    route_edge_entropy=entropy(route_edges,possible=len(assets)*(len(assets)-1))
    conflict_dependence=float(conflict_selected/total_transitions) if total_transitions else 1.0

    return {
        "mean_abs_corr":mean_abs_corr,
        "pca1_share":pca1_share,
        "mean_relative_vol":mean_relative_vol,
        "signal_edge_entropy":signal_edge_entropy,
        "occupancy_entropy":occupancy_entropy,
        "route_edge_entropy":route_edge_entropy,
        "conflict_dependence":conflict_dependence,
        "training_transition_count":total_transitions,
        "training_signal_edge_count":len(signal_edges),
        "training_route_edge_count":len(route_edges),
    }


def rank_structural(df):
    x=df.copy()
    feature_dirs={
        "mean_abs_corr":False,
        "pca1_share":False,
        "mean_relative_vol":True,
        "signal_edge_entropy":True,
        "occupancy_entropy":True,
        "route_edge_entropy":True,
        "conflict_dependence":False,
    }
    pct_cols=[]
    for col,higher_better in feature_dirs.items():
        pcol=col+"_pct"
        # rank(pct=True) ascending=True => high values high percentile
        pct=x[col].rank(pct=True,ascending=True,method="average")
        x[pcol]=pct if higher_better else 1.0-pct+1.0/len(x)
        pct_cols.append(pcol)
    x["structural_score"]=x[pct_cols].mean(axis=1)
    return x.sort_values(["structural_score","key"],ascending=[False,True],kind="stable").reset_index(drop=True)


def evaluate_case(panel, events_by_date, confirmed, cfg, run_dir, name):
    train_start,train_end=utc(cfg["train_start"]),utc(cfg["train_end"])
    fut_start,fut_end=utc(cfg["future_start"]),utc(cfg["future_end"])

    sets=list(combinations_u8())
    basic=pd.DataFrame([{"key":key(s),"assets":"|".join(s),"niches":niche_count(s)} for s in sets])
    max_niches=int(basic["niches"].max())
    max_keys=set(basic[basic["niches"]==max_niches]["key"])

    rows=[]
    all_returns=[]
    for i,assets in enumerate(sets):
        k=key(assets)
        train=graph_summary(panel,events_by_date,assets,train_start,train_end)
        future=graph_summary(panel,events_by_date,assets,fut_start,fut_end)
        all_returns.append({
            "key":k,
            "assets":"|".join(assets),
            "niches":niche_count(assets),
            "train_median":train["median_return"],
            "future_median":future["median_return"],
            "future_atom":future["atom_return"],
            "future_dd":future["median_max_dd"],
        })
        if k in max_keys:
            f=structural_features(panel,events_by_date,confirmed,assets,train_start,train_end)
            rows.append({
                "key":k,
                "assets":"|".join(assets),
                "niches":niche_count(assets),
                "train_median":train["median_return"],
                "future_median":future["median_return"],
                "future_atom":future["atom_return"],
                "future_dd":future["median_max_dd"],
                **f,
            })

    all_df=pd.DataFrame(all_returns)
    all_df["future_rank_all"]=all_df["future_median"].rank(method="min",ascending=False).astype(int)
    all_df["train_rank_all"]=all_df["train_median"].rank(method="min",ascending=False).astype(int)
    all_df.to_csv(run_dir/f"{name.lower()}_all_sets.csv",index=False)

    struct=rank_structural(pd.DataFrame(rows))
    struct["structural_rank"]=range(1,len(struct)+1)
    struct["future_rank_max_niche"]=struct["future_median"].rank(method="min",ascending=False).astype(int)
    struct.to_csv(run_dir/f"{name.lower()}_max_niche_structural.csv",index=False)

    chosen=struct.iloc[0].to_dict()
    chosen_all=all_df[all_df["key"]==chosen["key"]].iloc[0].to_dict()
    original=all_df[all_df["key"]==key(ORIGINAL_U8)].iloc[0].to_dict()
    train_top=all_df.sort_values(["train_rank_all","key"]).iloc[0].to_dict()
    hindsight=all_df.sort_values(["future_rank_all","key"]).iloc[0].to_dict()
    max_future=struct.sort_values(["future_rank_max_niche","key"]).iloc[0].to_dict()

    chosen["future_rank_all"]=int(chosen_all["future_rank_all"])
    chosen["train_rank_all"]=int(chosen_all["train_rank_all"])

    return {
        "max_niches":max_niches,
        "max_niche_set_count":len(struct),
        "structural_selection":chosen,
        "historical_u8":original,
        "training_return_selection":train_top,
        "future_best_hindsight_only":hindsight,
        "max_niche_future_best_hindsight_only":max_future,
        "max_niche_future_median":float(struct["future_median"].median()),
        "max_niche_future_positive_rate":float((struct["future_median"]>0).mean()),
        "structural_score_future_spearman":float(
            struct["structural_score"].corr(struct["future_median"],method="spearman")
        ),
        "feature_future_spearman":{
            c:float(struct[c].corr(struct["future_median"],method="spearman"))
            for c in (
                "mean_abs_corr","pca1_share","mean_relative_vol",
                "signal_edge_entropy","occupancy_entropy",
                "route_edge_entropy","conflict_dependence",
            )
        },
    }


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cutoff",default="2026-09-27T00:00:00Z")
    args=ap.parse_args()

    panel,meta=download_panel(utc(args.cutoff))
    events_by_date,confirmed=build_events(panel)

    run_dir=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True,exist_ok=True)

    payload={
        "generated_at":pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit":source_sha(),
        "pool":list(POOL),
        "niches":NICHES,
        "data":meta,
        "cases":{},
    }
    for name,cfg in CASES.items():
        payload["cases"][name]=evaluate_case(
            panel,events_by_date,confirmed,cfg,run_dir,name
        )

    (run_dir/"results.json").write_text(
        json.dumps(payload,indent=2,sort_keys=True,allow_nan=False),encoding="utf-8"
    )

    lines=["# U8 Diversity Selection Engine v1","",
           "Mode: STRESS_TEST_ONLY","Live U8 unchanged.",""]
    for name,res in payload["cases"].items():
        s=res["structural_selection"]
        o=res["historical_u8"]
        t=res["training_return_selection"]
        lines += [
            f"## {name}","",
            f"Max niches: {res['max_niches']}; candidate sets: {res['max_niche_set_count']}",
            "",
            "| Selection | Assets | Future median | Future ATOM | Future rank/all |",
            "|---|---|---:|---:|---:|",
            f"| Structural | {s['assets']} | {100*s['future_median']:+.2f}% | {100*s['future_atom']:+.2f}% | {int(s['future_rank_all'])}/1716 |",
            f"| Historical U8 | {o['assets']} | {100*o['future_median']:+.2f}% | {100*o['future_atom']:+.2f}% | {int(o['future_rank_all'])}/1716 |",
            f"| Top trailing return | {t['assets']} | {100*t['future_median']:+.2f}% | {100*t['future_atom']:+.2f}% | {int(t['future_rank_all'])}/1716 |",
            "",
            f"Median future return among max-niche sets: {100*res['max_niche_future_median']:+.2f}%",
            f"Structural-score Spearman vs future among max-niche sets: {res['structural_score_future_spearman']:+.3f}",
            "",
        ]
    (run_dir/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")

    print("run_dir="+str(run_dir))
    for name,res in payload["cases"].items():
        s=res["structural_selection"]
        o=res["historical_u8"]
        print(name+"_selected="+s["assets"])
        print(name+"_selected_future=%.6f" % s["future_median"])
        print(name+"_selected_rank="+str(int(s["future_rank_all"])))
        print(name+"_historical_future=%.6f" % o["future_median"])
        print(name+"_historical_rank="+str(int(o["future_rank_all"])))
        print(name+"_score_corr=%.6f" % res["structural_score_future_spearman"])
    return 0


if __name__=="__main__":
    raise SystemExit(main())
