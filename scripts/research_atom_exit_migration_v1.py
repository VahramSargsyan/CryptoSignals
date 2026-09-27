from __future__ import annotations

import argparse
import itertools
import json
import os
import subprocess
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import build_pair_monitor

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "atom_exit_migration_v1"

POOL = (
    "ATOM","TWT","PEPE","BNB","SOL","TRX","AAVE","LINK",
    "AVAX","FIL","ETH","ALGO","ADA","XRP","HBAR",
)
BASE_U10 = ("ATOM","TWT","PEPE","BNB","SOL","TRX","AAVE","LINK","FIL","HBAR")
DROP_ATOM_U9 = tuple(a for a in BASE_U10 if a!="ATOM")
BASE_U9_CLEANER = ("ATOM","TWT","PEPE","BNB","TRX","AAVE","LINK","FIL","HBAR")
DROP_ATOM_U8_CLEANER = tuple(a for a in BASE_U9_CLEANER if a!="ATOM")

U10_REPLACEMENTS = ("AVAX","ETH","ALGO","ADA","XRP")
U9_REPLACEMENTS = ("SOL","AVAX","ETH","ALGO","ADA","XRP")

LOOKBACK=180
ARM=0.15
REVERSAL=0.03
COST=0.001
DATA_START=pd.Timestamp("2023-05-05",tz="UTC")
MATURE_START=pd.Timestamp("2023-10-31",tz="UTC")
POST_PEPE_START=pd.Timestamp("2024-06-11",tz="UTC")
TWO_YEAR_START=pd.Timestamp("2024-09-27",tz="UTC")
YEAR_START=pd.Timestamp("2025-09-27",tz="UTC")
END=pd.Timestamp("2026-09-26",tz="UTC")


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
            raise RuntimeError(f"{a}: critical data quality")
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


def prep_arrays(panel):
    dates=[utc(x) for x in panel["timestamp"]]
    opens={a:panel[a+"_open"].astype(float).to_numpy() for a in POOL}
    closes={a:panel[a+"_close"].astype(float).to_numpy() for a in POOL}
    return dates,opens,closes


def bounds(dates,start,end):
    lo=next(i for i,d in enumerate(dates) if d>=start)
    hi=max(i for i,d in enumerate(dates) if d<=end)
    return lo,hi


def choose_event(event_idx,ts,current,eligible_targets):
    cands=event_idx.get(ts,{}).get(current,[])
    if not cands:
        return None,0
    eligible=[e for e in cands if e["to_asset"] in eligible_targets]
    if not eligible:
        return None,0
    return eligible[0],len(eligible)


def run_standard(dates,opens,closes,event_idx,assets,start,end,start_asset):
    aset=set(assets)
    lo,hi=bounds(dates,start,end)
    current=start_asset
    qty=1.0/opens[current][lo]
    pending=None
    equity=[]
    transitions=0
    conflicts=0
    route=[current]

    for i in range(lo,hi+1):
        ts=dates[i]
        if pending is not None:
            value=qty*opens[current][i]
            current=pending["to_asset"]
            qty=value*(1.0-COST)/opens[current][i]
            transitions+=1
            route.append(current)
            pending=None

        equity.append(qty*closes[current][i])

        if i==hi:
            continue
        ev,n=choose_event(event_idx,ts,current,aset)
        if ev is not None:
            if n>1:
                conflicts+=1
            pending=ev

    arr=np.asarray(equity,dtype=float)
    peaks=np.maximum.accumulate(arr)
    return {
        "return":float(arr[-1]/arr[0]-1.0),
        "max_dd":float(np.min(arr/peaks-1.0)),
        "transitions":transitions,
        "conflicts":conflicts,
        "route":route,
    }


def run_exit_only_atom(dates,opens,closes,event_idx,target_assets,start,end):
    target=set(target_assets)
    lo,hi=bounds(dates,start,end)
    current="ATOM"
    qty=1.0/opens[current][lo]
    pending=None
    equity=[]
    transitions=0
    conflicts=0
    route=[current]
    exited=False
    exit_signal_date=None
    exit_exec_date=None
    first_destination=None
    atom_days=0

    for i in range(lo,hi+1):
        ts=dates[i]

        if pending is not None:
            value=qty*opens[current][i]
            old=current
            current=pending["to_asset"]
            qty=value*(1.0-COST)/opens[current][i]
            transitions+=1
            route.append(current)
            if old=="ATOM" and not exited:
                exited=True
                exit_exec_date=ts
                first_destination=current
            pending=None

        if current=="ATOM":
            atom_days+=1

        equity.append(qty*closes[current][i])

        if i==hi:
            continue

        if current=="ATOM":
            eligible=target
        else:
            eligible=target  # ATOM intentionally absent after exit.

        ev,n=choose_event(event_idx,ts,current,eligible)
        if ev is not None:
            if n>1:
                conflicts+=1
            pending=ev
            if current=="ATOM" and exit_signal_date is None:
                exit_signal_date=ts

    arr=np.asarray(equity,dtype=float)
    peaks=np.maximum.accumulate(arr)
    return {
        "return":float(arr[-1]/arr[0]-1.0),
        "max_dd":float(np.min(arr/peaks-1.0)),
        "transitions":transitions,
        "conflicts":conflicts,
        "route":route,
        "exited_atom":bool(exited),
        "atom_days_before_exit":int(atom_days),
        "exit_signal_date":None if exit_signal_date is None else exit_signal_date.isoformat(),
        "exit_exec_date":None if exit_exec_date is None else exit_exec_date.isoformat(),
        "first_destination":first_destination,
    }


def summarize(dates,opens,closes,event_idx,assets,start,end,starters):
    runs=[]
    for s in starters:
        if s not in assets:
            continue
        r=run_standard(dates,opens,closes,event_idx,assets,start,end,s)
        runs.append({"start_asset":s,**r})
    df=pd.DataFrame(runs)
    return {
        "median_return":float(df["return"].median()),
        "worst_return":float(df["return"].min()),
        "best_return":float(df["return"].max()),
        "positive_starts":int((df["return"]>0).sum()),
        "median_max_dd":float(df["max_dd"].median()),
        "median_transitions":float(df["transitions"].median()),
    }


def all_member_summary(dates,opens,closes,event_idx,assets,start,end):
    return summarize(dates,opens,closes,event_idx,assets,start,end,assets)


def rolling_12m(dates,opens,closes,event_idx,assets,starters):
    rows=[]
    cursor=pd.Timestamp("2024-06-01",tz="UTC")
    while True:
        candidates=[d for d in dates if d>=cursor]
        if not candidates:
            break
        s=candidates[0]
        target=s+pd.DateOffset(months=12)-pd.Timedelta(days=1)
        if target>END:
            break
        e=max(d for d in dates if d<=target)
        x=summarize(dates,opens,closes,event_idx,assets,s,e,starters)
        rows.append(x["median_return"])
        cursor+=pd.offsets.MonthBegin(1)
    if not rows:
        return {"count":0}
    arr=np.asarray(rows,dtype=float)
    return {
        "count":len(arr),
        "median":float(np.median(arr)),
        "worst":float(np.min(arr)),
        "positive_rate":float(np.mean(arr>0)),
    }


def variant(name,assets):
    return {"name":name,"assets":tuple(assets)}


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cutoff",default="2026-09-27T00:00:00Z")
    args=ap.parse_args()

    panel,meta=download_panel(utc(args.cutoff))
    event_idx=build_event_index(panel)
    dates,opens,closes=prep_arrays(panel)

    variants=[
        variant("DROP_ATOM_U9",DROP_ATOM_U9),
        variant("DROP_ATOM_U8_CLEANER",DROP_ATOM_U8_CLEANER),
    ]
    for x in U10_REPLACEMENTS:
        variants.append(variant(f"U10_REPLACE_ATOM_WITH_{x}",DROP_ATOM_U9+(x,)))
    for x in U9_REPLACEMENTS:
        variants.append(variant(f"U9_CLEANER_REPLACE_ATOM_WITH_{x}",DROP_ATOM_U8_CLEANER+(x,)))

    windows={
        "mature":(MATURE_START,END),
        "post_pepe":(POST_PEPE_START,END),
        "latest_2y":(TWO_YEAR_START,END),
        "latest_1y":(YEAR_START,END),
    }

    results={}
    for v in variants:
        a=v["assets"]
        entry={"assets":list(a),"common_twt_pepe":{},"all_member":{}}
        for label,(s,e) in windows.items():
            entry["common_twt_pepe"][label]=summarize(
                dates,opens,closes,event_idx,a,s,e,("TWT","PEPE")
            )
            entry["all_member"][label]=all_member_summary(
                dates,opens,closes,event_idx,a,s,e
            )
        entry["rolling_12m_common_twt_pepe"]=rolling_12m(
            dates,opens,closes,event_idx,a,("TWT","PEPE")
        )
        results[v["name"]]=entry

    migration={}
    for target_name,target_assets in (
        ("TO_DROP_ATOM_U9",DROP_ATOM_U9),
        ("TO_DROP_ATOM_U8_CLEANER",DROP_ATOM_U8_CLEANER),
    ):
        migration[target_name]={}
        for label,(s,e) in windows.items():
            migration[target_name][label]=run_exit_only_atom(
                dates,opens,closes,event_idx,target_assets,s,e
            )

    # Legacy ATOM-start comparisons.
    legacy={}
    for base_name,base_assets in (
        ("BASE_U10",BASE_U10),
        ("BASE_U9_CLEANER",BASE_U9_CLEANER),
    ):
        legacy[base_name]={}
        for label,(s,e) in windows.items():
            legacy[base_name][label]=run_standard(
                dates,opens,closes,event_idx,base_assets,s,e,"ATOM"
            )

    # Current-state diagnostic: latest confirmed outbound events on the final closed candle.
    final_date=END
    outbound=event_idx.get(final_date,{}).get("ATOM",[])
    current_atom_outbound=[
        {
            "to_asset":e["to_asset"],
            "pair":e["pair"],
            "max_dislocation":e["max_dislocation"],
            "eligible_drop_u9":e["to_asset"] in set(DROP_ATOM_U9),
            "eligible_cleaner":e["to_asset"] in set(DROP_ATOM_U8_CLEANER),
        }
        for e in outbound
    ]

    payload={
        "generated_at":pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit":source_sha(),
        "data":meta,
        "variants":results,
        "migration":migration,
        "legacy_atom_start":legacy,
        "final_closed_candle":final_date.isoformat(),
        "confirmed_atom_outbound_on_final_candle":current_atom_outbound,
    }

    run_dir=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True,exist_ok=True)
    (run_dir/"results.json").write_text(
        json.dumps(payload,indent=2,sort_keys=True,allow_nan=False),
        encoding="utf-8"
    )

    lines=[
        "# ATOM Exit Migration v1","",
        "Mode: STRESS_TEST_ONLY","Live unchanged.","",
        "## No-ATOM targets — TWT/PEPE common starts","",
        "| Variant | Mature | Latest 2Y | Latest 1Y | Worst rolling 12m |",
        "|---|---:|---:|---:|---:|",
    ]
    for name,x in results.items():
        lines.append(
            f"| {name} | {100*x['common_twt_pepe']['mature']['median_return']:+.2f}% | "
            f"{100*x['common_twt_pepe']['latest_2y']['median_return']:+.2f}% | "
            f"{100*x['common_twt_pepe']['latest_1y']['median_return']:+.2f}% | "
            f"{100*x['rolling_12m_common_twt_pepe']['worst']:+.2f}% |"
        )
    lines += ["","## EXIT_ONLY_ATOM migration","",
              "| Target | Window | Return | DD | ATOM days | First destination |",
              "|---|---|---:|---:|---:|---|"]
    for target,bywin in migration.items():
        for label,x in bywin.items():
            lines.append(
                f"| {target} | {label} | {100*x['return']:+.2f}% | {100*x['max_dd']:+.2f}% | "
                f"{x['atom_days_before_exit']} | {x['first_destination'] or 'NONE'} |"
            )
    (run_dir/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")

    print("run_dir="+str(run_dir))
    for name in ("DROP_ATOM_U9","DROP_ATOM_U8_CLEANER"):
        x=results[name]
        print(name+"_mature=%.6f" % x["common_twt_pepe"]["mature"]["median_return"])
        print(name+"_2y=%.6f" % x["common_twt_pepe"]["latest_2y"]["median_return"])
        print(name+"_1y=%.6f" % x["common_twt_pepe"]["latest_1y"]["median_return"])
        print(name+"_12m_worst=%.6f" % x["rolling_12m_common_twt_pepe"]["worst"])
    for target,bywin in migration.items():
        for label,x in bywin.items():
            print(f"{target}_{label}_return={x['return']:.6f}")
            print(f"{target}_{label}_atom_days={x['atom_days_before_exit']}")
            print(f"{target}_{label}_destination={x['first_destination']}")
    for name,x in results.items():
        if "REPLACE_ATOM" in name:
            print(name+"_1y=%.6f" % x["common_twt_pepe"]["latest_1y"]["median_return"])
            print(name+"_2y=%.6f" % x["common_twt_pepe"]["latest_2y"]["median_return"])
    print("current_atom_outbound="+json.dumps(current_atom_outbound,sort_keys=True))
    return 0


if __name__=="__main__":
    raise SystemExit(main())
