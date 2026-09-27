from __future__ import annotations

import argparse
import itertools
import json
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
OUT = ROOT / "research_artifacts" / "unconstrained_u8_u10_last_year_v1"

POOL = (
    "ATOM","TWT","PEPE","BNB","SOL","TRX","AAVE","LINK",
    "AVAX","FIL","ETH","ALGO","ADA","XRP","HBAR",
)
CANONICAL_U8 = ("ATOM","TWT","PEPE","BNB","SOL","TRX","AAVE","LINK")
RESEARCH_U10 = CANONICAL_U8 + ("FIL","HBAR")
U9_CLEANER = ("ATOM","TWT","PEPE","BNB","TRX","AAVE","LINK","FIL","HBAR")

LOOKBACK=180
ARM=0.15
REVERSAL=0.03
COST=0.001
DATA_START=pd.Timestamp("2023-05-05",tz="UTC")
YEAR_START=pd.Timestamp("2025-09-27",tz="UTC")
YEAR_END=pd.Timestamp("2026-09-26",tz="UTC")


def utc(x):
    t=pd.Timestamp(x)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def source_sha():
    v=os.environ.get("SOURCE_COMMIT_SHA")
    if v:
        return v
    return subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip()


def download_panel(cutoff):
    client=BinanceSpotRestClient()
    panel=None
    meta={}
    for a in POOL:
        result=download_historical_dataset(
            client,
            symbol=a+"USDT",
            start=DATA_START,
            end=cutoff,
            timeframe="1D",
            as_of=cutoff,
        )
        if result.dataset is None:
            raise RuntimeError(f"{a}: {result.metadata.status}")
        if result.dataset.quality.has_critical_issues:
            raise RuntimeError(f"{a}: critical data quality issues")
        f=result.dataset.candles[["timestamp","open","close"]].copy()
        f=f.rename(columns={"open":a+"_open","close":a+"_close"})
        meta[a]={
            "rows":len(f),
            "start":pd.Timestamp(f.iloc[0]["timestamp"]).isoformat(),
            "end":pd.Timestamp(f.iloc[-1]["timestamp"]).isoformat(),
        }
        panel=f if panel is None else panel.merge(f,on="timestamp",how="inner",validate="one_to_one")
    return panel.sort_values("timestamp").reset_index(drop=True),meta


def build_event_index(panel):
    cols=["timestamp"]+[a+"_close" for a in POOL]
    events,_=build_pair_monitor(
        panel[cols].copy(),
        assets=POOL,
        lookback=LOOKBACK,
        arm_threshold=ARM,
        reversal=REVERSAL,
    )
    idx=defaultdict(lambda:defaultdict(list))
    for e in events:
        if e["event"]!="CONFIRMED":
            continue
        d=utc(e["date"])
        idx[d][e["from_asset"]].append({
            "to_asset":e["to_asset"],
            "pair":e["pair"],
            "max_dislocation":float(e["max_dislocation"]),
        })
    for d in idx:
        for a in idx[d]:
            idx[d][a].sort(key=lambda e:(-e["max_dislocation"],e["to_asset"],e["pair"]))
    return idx


def prep_window(panel):
    w=panel[(panel["timestamp"]>=YEAR_START)&(panel["timestamp"]<=YEAR_END)].copy()
    if w.empty:
        raise RuntimeError("empty year window")
    dates=[utc(x) for x in w["timestamp"]]
    opens={a:w[a+"_open"].astype(float).to_numpy() for a in POOL}
    closes={a:w[a+"_close"].astype(float).to_numpy() for a in POOL}
    return dates,opens,closes


def run_one(dates,opens,closes,event_idx,assets,start_asset):
    aset=set(assets)
    current=start_asset
    qty=1.0/opens[current][0]
    pending=None
    equity=[]
    transitions=0
    conflicts=0

    n=len(dates)
    for i,ts in enumerate(dates):
        if pending is not None:
            value=qty*opens[current][i]
            current=pending
            qty=value*(1.0-COST)/opens[current][i]
            transitions+=1
            pending=None

        equity.append(qty*closes[current][i])

        if i==n-1:
            continue

        candidates=event_idx.get(ts,{}).get(current,[])
        if not candidates:
            continue

        eligible=[e for e in candidates if e["to_asset"] in aset]
        if not eligible:
            continue
        if len(eligible)>1:
            conflicts+=1
        pending=eligible[0]["to_asset"]

    arr=np.asarray(equity,dtype=float)
    peaks=np.maximum.accumulate(arr)
    dd=float(np.min(arr/peaks-1.0))
    return {
        "return":float(arr[-1]/arr[0]-1.0),
        "max_dd":dd,
        "transitions":transitions,
        "conflicts":conflicts,
    }


def evaluate_universe(dates,opens,closes,event_idx,assets):
    runs=[]
    for a in assets:
        r=run_one(dates,opens,closes,event_idx,assets,a)
        runs.append((a,r))
    returns=np.array([r["return"] for _,r in runs],dtype=float)
    dds=np.array([r["max_dd"] for _,r in runs],dtype=float)
    transitions=np.array([r["transitions"] for _,r in runs],dtype=float)
    conflicts=np.array([r["conflicts"] for _,r in runs],dtype=float)
    per={a:r for a,r in runs}

    out={
        "key":"|".join(sorted(assets)),
        "assets":"|".join(assets),
        "size":len(assets),
        "median_return":float(np.median(returns)),
        "worst_return":float(np.min(returns)),
        "best_return":float(np.max(returns)),
        "positive_starts":int(np.sum(returns>0)),
        "median_max_dd":float(np.median(dds)),
        "worst_max_dd":float(np.min(dds)),
        "median_transitions":float(np.median(transitions)),
        "median_conflicts":float(np.median(conflicts)),
    }
    for a in ("ATOM","TWT","PEPE"):
        out[a.lower()+"_return"]=float(per[a]["return"]) if a in per else None
    return out


def enumerate_size(size):
    yield from itertools.combinations(POOL,size)


def distribution(df):
    return {
        "count":len(df),
        "median":float(df["median_return"].median()),
        "q25":float(df["median_return"].quantile(.25)),
        "q75":float(df["median_return"].quantile(.75)),
        "q90":float(df["median_return"].quantile(.90)),
        "q95":float(df["median_return"].quantile(.95)),
        "q99":float(df["median_return"].quantile(.99)),
        "best":float(df["median_return"].max()),
        "worst":float(df["median_return"].min()),
        "positive_rate":float((df["median_return"]>0).mean()),
    }


def top_frequency(df,n):
    c=Counter()
    for assets in df.head(n)["assets"]:
        c.update(assets.split("|"))
    return dict(sorted(c.items(),key=lambda kv:(-kv[1],kv[0])))


def core_of_top(df,n):
    rows=[set(x.split("|")) for x in df.head(n)["assets"]]
    if not rows:
        return []
    return sorted(set.intersection(*rows))


def rank_of(df,key):
    hit=df[df["key"]==key]
    if hit.empty:
        return None
    return int(hit.iloc[0]["median_rank"])


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cutoff",default="2026-09-27T00:00:00Z")
    args=ap.parse_args()

    panel,meta=download_panel(utc(args.cutoff))
    event_idx=build_event_index(panel)
    dates,opens,closes=prep_window(panel)

    run_dir=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True,exist_ok=True)

    outputs={}
    for size in (8,10):
        rows=[]
        for assets in enumerate_size(size):
            rows.append(evaluate_universe(dates,opens,closes,event_idx,assets))
        df=pd.DataFrame(rows)
        df["median_rank"]=df["median_return"].rank(method="min",ascending=False).astype(int)
        df["dd_rank"]=df["median_max_dd"].rank(method="min",ascending=False).astype(int)
        df=df.sort_values(["median_rank","key"],kind="stable").reset_index(drop=True)
        df.to_csv(run_dir/f"all_u{size}_last_year.csv",index=False)
        outputs[f"U{size}"]={
            "distribution":distribution(df),
            "top10":json.loads(df.head(10).to_json(orient="records")),
            "top50_frequency":top_frequency(df,50),
            "top100_frequency":top_frequency(df,100),
            "top10_frequency":top_frequency(df,10),
            "top10_core":core_of_top(df,10),
        }
        if size==8:
            outputs[f"U{size}"]["canonical_u8_rank"]=rank_of(df,"|".join(sorted(CANONICAL_U8)))
        if size==10:
            outputs[f"U{size}"]["research_u10_rank"]=rank_of(df,"|".join(sorted(RESEARCH_U10)))

    # Context: U9 cleaner evaluated on same year, not ranked against U8/U10 because size differs.
    u9=evaluate_universe(dates,opens,closes,event_idx,U9_CLEANER)

    result={
        "generated_at":pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit":source_sha(),
        "window":{"start":YEAR_START.isoformat(),"end":YEAR_END.isoformat()},
        "data":meta,
        "results":outputs,
        "u9_cleaner_context":u9,
    }
    (run_dir/"results.json").write_text(
        json.dumps(result,indent=2,sort_keys=True,allow_nan=False),
        encoding="utf-8"
    )

    lines=[
        "# Unconstrained U8/U10 Last-Year Exhaustive Search v1","",
        "Mode: STRESS_TEST_ONLY",
        "Live unchanged.","",
        f"Window: {YEAR_START.date()} -> {YEAR_END.date()}","",
        "## Top 10 U8","",
        "| Rank | Assets | Median | Worst start | Median DD |",
        "|---:|---|---:|---:|---:|",
    ]
    for row in outputs["U8"]["top10"]:
        lines.append(
            f"| {row['median_rank']} | {row['assets']} | {100*row['median_return']:+.2f}% | "
            f"{100*row['worst_return']:+.2f}% | {100*row['median_max_dd']:+.2f}% |"
        )
    lines += ["","## Top 10 U10","",
              "| Rank | Assets | Median | Worst start | Median DD |",
              "|---:|---|---:|---:|---:|"]
    for row in outputs["U10"]["top10"]:
        lines.append(
            f"| {row['median_rank']} | {row['assets']} | {100*row['median_return']:+.2f}% | "
            f"{100*row['worst_return']:+.2f}% | {100*row['median_max_dd']:+.2f}% |"
        )
    (run_dir/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")

    print("run_dir="+str(run_dir))
    for size in ("U8","U10"):
        x=outputs[size]
        print(size+"_count="+str(x["distribution"]["count"]))
        print(size+"_median=%.6f" % x["distribution"]["median"])
        print(size+"_q95=%.6f" % x["distribution"]["q95"])
        print(size+"_best=%.6f" % x["distribution"]["best"])
        print(size+"_top10_core="+",".join(x["top10_core"]))
        print(size+"_top1="+x["top10"][0]["assets"])
        print(size+"_top1_return=%.6f" % x["top10"][0]["median_return"])
    print("canonical_u8_rank="+str(outputs["U8"]["canonical_u8_rank"]))
    print("research_u10_rank="+str(outputs["U10"]["research_u10_rank"]))
    print("u9_cleaner_median=%.6f" % u9["median_return"])
    return 0


if __name__=="__main__":
    raise SystemExit(main())
