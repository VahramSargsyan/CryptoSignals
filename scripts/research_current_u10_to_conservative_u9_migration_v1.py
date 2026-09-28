from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

from scripts.research_target_universe_champion_v1 import (
    ROOT, POOL, AIDX, CURRENT_U10,
    utc, source_sha, download_panel, build_events, arrays,
    bounds, mask_for, summary_daily,
)

OUT = ROOT / "research_artifacts" / "current_u10_to_conservative_u9_migration_v1"

TARGET = ("TWT","PEPE","BNB","TRX","AAVE","AVAX","FIL","ALGO","XRP")
SUNSET = ("ATOM","SOL","LINK","HBAR")
KEEP = ("TWT","PEPE","BNB","TRX","AAVE","FIL")
ADD = ("AVAX","ALGO","XRP")
END = pd.Timestamp("2026-09-26",tz="UTC")
COST = 0.001


def choose_event(by_day, date, src, allowed):
    cands = by_day.get(date,{}).get(src,[])
    elig=[x for x in cands if x[0] in allowed]
    return elig[0][0] if elig else None


def run_source(dates,opens,closes,by_day,start,end,starter):
    inds=tuple(AIDX[a] for a in CURRENT_U10)
    # reuse daily summary implementation logic by direct one-start wrapper
    from scripts.research_target_universe_champion_v1 import run_daily
    return run_daily(dates,opens,closes,by_day,inds,start,end,AIDX[starter],collect=True)


def run_transition(dates,opens,closes,by_day,start,end,starter):
    target_idx={AIDX[a] for a in TARGET}
    sunset_idx={AIDX[a] for a in SUNSET}
    lo,hi=bounds(dates,start,end)
    current=AIDX[starter]
    qty=1.0/opens[lo,current]
    pending=None
    equity=[]
    route=[current]
    first_exit=None
    first_dest=None
    transitions=0

    for i in range(lo,hi+1):
        d=dates[i]
        if pending is not None:
            value=qty*opens[i,current]
            old=current
            current=pending
            qty=value*(1.0-COST)/opens[i,current]
            transitions+=1
            route.append(current)
            if old in sunset_idx and first_exit is None:
                first_exit=dates[i]
                first_dest=POOL[current]
            pending=None

        equity.append(qty*closes[i,current])
        if i==hi:
            continue

        # If legacy/sunset is still held, only accept an exit into TARGET.
        # Once inside TARGET, only TARGET destinations remain eligible.
        allowed=target_idx
        pending=choose_event(by_day,d,current,allowed)

    arr=np.asarray(equity,dtype=float)
    peaks=np.maximum.accumulate(arr)
    return {
        "return":float(arr[-1]/arr[0]-1.0),
        "max_dd":float(np.min(arr/peaks-1.0)),
        "transitions":transitions,
        "route":[POOL[i] for i in route],
        "first_exit_date":None if first_exit is None else first_exit.isoformat(),
        "first_exit_destination":first_dest,
        "days_to_exit":None if first_exit is None else int((first_exit-start).days),
        "completed_exit":bool(starter not in SUNSET or first_exit is not None),
    }


def start_dates(dates):
    out=[]
    cursor=pd.Timestamp("2024-01-01",tz="UTC")
    last=pd.Timestamp("2026-03-01",tz="UTC")
    while cursor<=last:
        cand=[d for d in dates if d>=cursor and d<=END]
        if cand:
            out.append(cand[0])
        cursor+=pd.offsets.MonthBegin(1)
    return out


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cutoff",default="2026-09-28T00:00:00Z")
    args=ap.parse_args()

    panel,meta=download_panel(utc(args.cutoff))
    by_day=build_events(panel)
    dates,opens,closes,_=arrays(panel)

    runs=[]
    exit_records=[]
    for s in start_dates(dates):
        src_returns=[]
        tr_returns=[]
        src_dd=[]
        tr_dd=[]
        for starter in CURRENT_U10:
            src=run_source(dates,opens,closes,by_day,s,END,starter)
            tr=run_transition(dates,opens,closes,by_day,s,END,starter)
            src_returns.append(src["return"]); tr_returns.append(tr["return"])
            src_dd.append(src["max_dd"]); tr_dd.append(tr["max_dd"])
            if starter in SUNSET:
                exit_records.append({
                    "migration_start":s.isoformat(),
                    "starter":starter,
                    "completed_exit":tr["completed_exit"],
                    "days_to_exit":tr["days_to_exit"],
                    "destination":tr["first_exit_destination"],
                    "transition_return":tr["return"],
                    "source_return":src["return"],
                })
        runs.append({
            "migration_start":s.isoformat(),
            "source_median":float(np.median(src_returns)),
            "transition_median":float(np.median(tr_returns)),
            "source_worst":float(np.min(src_returns)),
            "transition_worst":float(np.min(tr_returns)),
            "source_median_dd":float(np.median(src_dd)),
            "transition_median_dd":float(np.median(tr_dd)),
            "transition_beats_source":bool(np.median(tr_returns)>np.median(src_returns)),
        })

    rdf=pd.DataFrame(runs)
    edf=pd.DataFrame(exit_records)

    per_asset={}
    for asset in SUNSET:
        g=edf[edf["starter"]==asset]
        dest=Counter(x for x in g["destination"].dropna())
        per_asset[asset]={
            "starts":int(len(g)),
            "completion_rate":float(g["completed_exit"].mean()),
            "median_days_to_exit":None if g["days_to_exit"].dropna().empty else float(g["days_to_exit"].dropna().median()),
            "max_days_to_exit":None if g["days_to_exit"].dropna().empty else int(g["days_to_exit"].dropna().max()),
            "destination_counts":dict(dest),
            "transition_beats_source_rate":float((g["transition_return"]>g["source_return"]).mean()),
        }

    summary={
        "start_count":int(len(rdf)),
        "transition_beats_source_rate":float(rdf["transition_beats_source"].mean()),
        "median_source_return":float(rdf["source_median"].median()),
        "median_transition_return":float(rdf["transition_median"].median()),
        "median_return_delta":float((rdf["transition_median"]-rdf["source_median"]).median()),
        "worst_transition_median":float(rdf["transition_median"].min()),
        "worst_source_median":float(rdf["source_median"].min()),
        "median_source_dd":float(rdf["source_median_dd"].median()),
        "median_transition_dd":float(rdf["transition_median_dd"].median()),
    }

    run_dir=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True,exist_ok=True)
    rdf.to_csv(run_dir/"migration_monthly_starts.csv",index=False)
    edf.to_csv(run_dir/"sunset_asset_exits.csv",index=False)

    result={
        "generated_at":pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit":source_sha(),
        "source_universe":list(CURRENT_U10),
        "target_universe":list(TARGET),
        "keep":list(KEEP),
        "sunset":list(SUNSET),
        "add":list(ADD),
        "summary":summary,
        "per_sunset_asset":per_asset,
    }
    (run_dir/"results.json").write_text(
        json.dumps(result,indent=2,sort_keys=True,allow_nan=False),encoding="utf-8"
    )

    lines=[
        "# Current U10 -> Conservative U9 Migration v1","",
        "Mode: STRESS_TEST_ONLY","Live unchanged.","",
        f"Historical monthly starts: {summary['start_count']}",
        f"Transition beats source: {100*summary['transition_beats_source_rate']:.1f}%",
        f"Median source return: {100*summary['median_source_return']:+.2f}%",
        f"Median transition return: {100*summary['median_transition_return']:+.2f}%",
        f"Median return delta: {100*summary['median_return_delta']:+.2f} pp",
        f"Median source DD: {100*summary['median_source_dd']:+.2f}%",
        f"Median transition DD: {100*summary['median_transition_dd']:+.2f}%",
        "",
        "## Sunset exits","",
    ]
    for a,x in per_asset.items():
        lines.append(
            f"- {a}: completion {100*x['completion_rate']:.1f}%; "
            f"median days {x['median_days_to_exit']}; max days {x['max_days_to_exit']}; "
            f"transition beats source {100*x['transition_beats_source_rate']:.1f}%"
        )
    (run_dir/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")

    print("run_dir="+str(run_dir))
    print("start_count="+str(summary["start_count"]))
    print("transition_beats_source_rate=%.6f" % summary["transition_beats_source_rate"])
    print("median_source_return=%.6f" % summary["median_source_return"])
    print("median_transition_return=%.6f" % summary["median_transition_return"])
    print("median_return_delta=%.6f" % summary["median_return_delta"])
    print("median_source_dd=%.6f" % summary["median_source_dd"])
    print("median_transition_dd=%.6f" % summary["median_transition_dd"])
    for a,x in per_asset.items():
        print(a+"_completion=%.6f" % x["completion_rate"])
        print(a+"_median_days="+str(x["median_days_to_exit"]))
        print(a+"_max_days="+str(x["max_days_to_exit"]))
        print(a+"_beats_source=%.6f" % x["transition_beats_source_rate"])
        print(a+"_destinations="+json.dumps(x["destination_counts"],sort_keys=True))
    return 0


if __name__=="__main__":
    raise SystemExit(main())
