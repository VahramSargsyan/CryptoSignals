from __future__ import annotations

import argparse
import itertools
import json
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

from scripts.research_algo_deep_audit_v1 import (
    ROOT,
    POOL,
    utc,
    source_sha,
    download_panel,
    build_event_index,
    prep_arrays,
    run_one,
    hodl_return,
    matched_marginal,
)

OUT = ROOT / "research_artifacts" / "algo_endpoint_sensitivity_v1"

ENDPOINTS = (
    pd.Timestamp("2026-05-31",tz="UTC"),
    pd.Timestamp("2026-06-30",tz="UTC"),
    pd.Timestamp("2026-07-31",tz="UTC"),
    pd.Timestamp("2026-08-31",tz="UTC"),
    pd.Timestamp("2026-09-26",tz="UTC"),
)


def eval_u8(dates,opens,closes,event_idx,assets,start,end):
    rets=[]
    for starter in assets:
        r=run_one(dates,opens,closes,event_idx,assets,start,end,starter)
        rets.append(r["return"])
    return float(np.median(np.asarray(rets,dtype=float)))


def top_frequency(df,n,asset):
    return int(sum(asset in x.split("|") for x in df.head(n)["assets"]))


def top_core(df,n):
    sets=[set(x.split("|")) for x in df.head(n)["assets"]]
    return sorted(set.intersection(*sets)) if sets else []


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cutoff",default="2026-09-27T00:00:00Z")
    args=ap.parse_args()

    panel,meta,_=download_panel(utc(args.cutoff))
    event_idx=build_event_index(panel)
    dates,opens,closes,_=prep_arrays(panel)

    result={
        "generated_at":pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit":source_sha(),
        "data":meta,
        "windows":[],
    }

    for end in ENDPOINTS:
        start=end-pd.Timedelta(days=364)
        rows=[]
        lookup={}
        for assets in itertools.combinations(POOL,8):
            med=eval_u8(dates,opens,closes,event_idx,assets,start,end)
            key="|".join(sorted(assets))
            rows.append({"key":key,"assets":"|".join(assets),"median_return":med})
            lookup[frozenset(assets)]=med
        df=pd.DataFrame(rows).sort_values(
            ["median_return","key"],ascending=[False,True],kind="stable"
        ).reset_index(drop=True)
        df["rank"]=np.arange(1,len(df)+1)

        algo_df=df[df["assets"].str.split("|",regex=False).apply(lambda xs:"ALGO" in xs)]
        _,marg=matched_marginal(lookup,8)

        win={
            "start":start.isoformat(),
            "end":end.isoformat(),
            "algo_hodl":hodl_return(dates,opens,closes,"ALGO",start,end),
            "algo_top10_frequency":top_frequency(df,10,"ALGO"),
            "algo_top50_frequency":top_frequency(df,50,"ALGO"),
            "algo_top100_frequency":top_frequency(df,100,"ALGO"),
            "algo_best_rank":int(algo_df.iloc[0]["rank"]),
            "algo_median_rank":float(algo_df["rank"].median()),
            "algo_pairwise_win_rate":marg["pairwise_win_rate"],
            "algo_median_delta_vs_median_alt":marg["median_delta_vs_median_alternative"],
            "algo_best_context_rate":marg["best_context_rate"],
            "top10_core":top_core(df,10),
            "top1_assets":df.iloc[0]["assets"],
            "top1_return":float(df.iloc[0]["median_return"]),
        }
        result["windows"].append(win)
        df.head(100).to_csv(
            OUT.name + "_tmp.csv",index=False
        ) if False else None

    run_dir=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True,exist_ok=True)
    (run_dir/"results.json").write_text(
        json.dumps(result,indent=2,sort_keys=True,allow_nan=False),
        encoding="utf-8"
    )

    lines=[
        "# ALGO Endpoint Sensitivity v1","",
        "Mode: STRESS_TEST_ONLY","Live unchanged.","",
        "| End | ALGO HODL | Top10 | Top50 | Top100 | Pairwise win | Median marginal delta |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for w in result["windows"]:
        lines.append(
            f"| {w['end'][:10]} | {100*w['algo_hodl']:+.2f}% | "
            f"{w['algo_top10_frequency']}/10 | {w['algo_top50_frequency']}/50 | "
            f"{w['algo_top100_frequency']}/100 | {100*w['algo_pairwise_win_rate']:.2f}% | "
            f"{100*w['algo_median_delta_vs_median_alt']:+.2f} pp |"
        )
    (run_dir/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")

    print("run_dir="+str(run_dir))
    for w in result["windows"]:
        tag=w["end"][:10]
        print(tag+"_algo_hodl=%.6f" % w["algo_hodl"])
        print(tag+"_top10="+str(w["algo_top10_frequency"]))
        print(tag+"_top50="+str(w["algo_top50_frequency"]))
        print(tag+"_top100="+str(w["algo_top100_frequency"]))
        print(tag+"_pairwise_win=%.6f" % w["algo_pairwise_win_rate"])
        print(tag+"_median_delta=%.6f" % w["algo_median_delta_vs_median_alt"])
        print(tag+"_best_context=%.6f" % w["algo_best_context_rate"])
        print(tag+"_top10_core="+",".join(w["top10_core"]))
    return 0


if __name__=="__main__":
    raise SystemExit(main())
