from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

from scripts.research_algo_deep_audit_v1 import (
    ROOT,
    U9_CLEANER,
    MANDATORY,
    MATURE_START,
    POST_BULL_START,
    TWO_YEAR_START,
    YEAR_START,
    END,
    utc,
    source_sha,
    download_panel,
    build_event_index,
    prep_arrays,
    run_one,
    summarize,
)

OUT = ROOT / "research_artifacts" / "algo_feeder_interaction_v1"
FEEDERS = ("AVAX","ETH","SOL","XRP")


def topology(dates,opens,closes,event_idx,assets,start,end):
    entries=exits=ends=0
    total_days=algo_days=0
    incoming=Counter()
    outgoing=Counter()
    for starter in MANDATORY:
        r=run_one(dates,opens,closes,event_idx,assets,start,end,starter,collect=True)
        total_days += len(r["holdings"])
        algo_days += sum(1 for x in r["holdings"] if x=="ALGO")
        if r["holdings"][-1]=="ALGO":
            ends += 1
        for (a,b),n in r["edges"].items():
            if b=="ALGO":
                entries += n
                incoming[a] += n
            if a=="ALGO":
                exits += n
                outgoing[b] += n
    return {
        "occupancy_share": algo_days/total_days if total_days else 0.0,
        "entries": entries,
        "exits": exits,
        "routes_ending_in_algo": ends,
        "incoming_edges": dict(incoming),
        "outgoing_edges": dict(outgoing),
    }


def eval_variant(dates,opens,closes,event_idx,assets):
    wins={
        "mature":(MATURE_START,END),
        "post_bull":(POST_BULL_START,END),
        "latest_2y":(TWO_YEAR_START,END),
        "latest_1y":(YEAR_START,END),
    }
    out={"assets":list(assets),"windows":{},"topology":{}}
    for label,(s,e) in wins.items():
        out["windows"][label]=summarize(
            dates,opens,closes,event_idx,assets,s,e,MANDATORY
        )
        if "ALGO" in assets:
            out["topology"][label]=topology(
                dates,opens,closes,event_idx,assets,s,e
            )
    return out


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cutoff",default="2026-09-27T00:00:00Z")
    args=ap.parse_args()

    panel,meta,_=download_panel(utc(args.cutoff))
    event_idx=build_event_index(panel)
    dates,opens,closes,_=prep_arrays(panel)

    variants={
        "BASE":U9_CLEANER,
        "BASE_PLUS_ALGO":U9_CLEANER+("ALGO",),
    }
    for f in FEEDERS:
        variants[f"BASE_PLUS_{f}"]=U9_CLEANER+(f,)
        variants[f"BASE_PLUS_{f}_ALGO"]=U9_CLEANER+(f,"ALGO")

    result={
        "generated_at":pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit":source_sha(),
        "data":meta,
        "variants":{},
        "interactions":{},
    }
    for name,assets in variants.items():
        result["variants"][name]=eval_variant(
            dates,opens,closes,event_idx,assets
        )

    base=result["variants"]["BASE"]
    base_algo=result["variants"]["BASE_PLUS_ALGO"]

    for f in FEEDERS:
        no_algo=result["variants"][f"BASE_PLUS_{f}"]
        with_algo=result["variants"][f"BASE_PLUS_{f}_ALGO"]
        x={}
        for w in ("mature","post_bull","latest_2y","latest_1y"):
            alone=(
                base_algo["windows"][w]["median_return"]
                - base["windows"][w]["median_return"]
            )
            with_f=(
                with_algo["windows"][w]["median_return"]
                - no_algo["windows"][w]["median_return"]
            )
            x[w]={
                "algo_increment_alone":alone,
                "algo_increment_with_feeder":with_f,
                "interaction_excess":with_f-alone,
                "base_plus_feeder":no_algo["windows"][w]["median_return"],
                "base_plus_feeder_algo":with_algo["windows"][w]["median_return"],
            }
        result["interactions"][f]=x

    run_dir=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True,exist_ok=True)
    (run_dir/"results.json").write_text(
        json.dumps(result,indent=2,sort_keys=True,allow_nan=False),
        encoding="utf-8"
    )

    lines=[
        "# ALGO Feeder Interaction v1","",
        "Mode: STRESS_TEST_ONLY","Live unchanged.","",
        "| Feeder | Base+F 1Y | Base+F+ALGO 1Y | ALGO incremental 1Y | Interaction excess 1Y |",
        "|---|---:|---:|---:|---:|",
    ]
    for f in FEEDERS:
        x=result["interactions"][f]["latest_1y"]
        lines.append(
            f"| {f} | {100*x['base_plus_feeder']:+.2f}% | "
            f"{100*x['base_plus_feeder_algo']:+.2f}% | "
            f"{100*x['algo_increment_with_feeder']:+.2f} pp | "
            f"{100*x['interaction_excess']:+.2f} pp |"
        )
    (run_dir/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")

    print("run_dir="+str(run_dir))
    print("algo_alone_1y_increment=%.6f" % (
        base_algo["windows"]["latest_1y"]["median_return"]
        - base["windows"]["latest_1y"]["median_return"]
    ))
    for f in FEEDERS:
        for w in ("post_bull","latest_2y","latest_1y"):
            x=result["interactions"][f][w]
            print(f"{f}_{w}_without_algo={x['base_plus_feeder']:.6f}")
            print(f"{f}_{w}_with_algo={x['base_plus_feeder_algo']:.6f}")
            print(f"{f}_{w}_algo_increment={x['algo_increment_with_feeder']:.6f}")
            print(f"{f}_{w}_interaction_excess={x['interaction_excess']:.6f}")
        topo=result["variants"][f"BASE_PLUS_{f}_ALGO"]["topology"]["latest_1y"]
        print(f"{f}_1y_algo_occupancy={topo['occupancy_share']:.6f}")
        print(f"{f}_1y_algo_entries={topo['entries']}")
        print(f"{f}_1y_algo_exits={topo['exits']}")
        print(f"{f}_1y_algo_incoming={json.dumps(topo['incoming_edges'],sort_keys=True)}")
        print(f"{f}_1y_algo_outgoing={json.dumps(topo['outgoing_edges'],sort_keys=True)}")
    return 0


if __name__=="__main__":
    raise SystemExit(main())
