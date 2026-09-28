from __future__ import annotations

import argparse
import itertools
import json
import math
from collections import deque
from pathlib import Path

import numpy as np
import pandas as pd

from scripts.research_target_universe_champion_v1 import (
    ROOT, POOL, AIDX, CURRENT_U10,
    MATURE_START, TWO_YEAR_START, YEAR_START, END, ENDPOINTS,
    utc, source_sha, download_panel, build_events, arrays, prep_event_lists,
    bounds, mask_for, choose_target, summary_daily, rolling_summary,
)

OUT = ROOT / "research_artifacts" / "bull_neutralized_target_champion_v1"


def max_drawup(series, days):
    prices=series.astype(float).to_numpy()
    q=deque()
    best=-math.inf
    bi=bj=0
    for j,p in enumerate(prices):
        while q and q[0] < j-days:
            q.popleft()
        while q and prices[q[-1]] >= p:
            q.pop()
        q.append(j)
        i=q[0]
        if i<j:
            gain=p/prices[i]-1.0
            if gain>best:
                best=gain; bi=i; bj=j
    return float(best),bi,bj


def primary_intervals(panel):
    dates=[utc(x) for x in panel["timestamp"]]
    out={}
    for i,a in enumerate(POOL):
        s=panel[a+"_close"].astype(float).reset_index(drop=True)
        d90,i90,j90=max_drawup(s,90)
        d180,i180,j180=max_drawup(s,180)
        if d90>=2.0 or d180>=4.0:
            out[i]={
                "asset":a,
                "drawup90":d90,
                "drawup180":d180,
                "start_i":i90,
                "end_i":j90,
                "start":dates[i90].isoformat(),
                "end":dates[j90].isoformat(),
            }
    return out


def build_clipped_cumlogs(dates, closes, primary):
    n=len(dates)
    m=len(POOL)
    raw=np.zeros((n,m),dtype=float)
    clip=np.zeros((n,m),dtype=float)
    primary_bounds={i:(x["start_i"],x["end_i"]) for i,x in primary.items()}

    for a in range(m):
        for j in range(1,n):
            r=closes[j,a]/closes[j-1,a]
            raw[j,a]=raw[j-1,a]+math.log(r)
            cr=r
            b=primary_bounds.get(a)
            if b is not None and b[0] <= j <= b[1] and r>1.0:
                cr=1.0
            clip[j,a]=clip[j-1,a]+math.log(cr)
    return raw,clip


def segment_factor(cum,start_i,end_i,asset):
    if end_i<=start_i:
        return 1.0
    return math.exp(cum[end_i,asset]-cum[start_i,asset])


def terminal_route_neutralized(opens,closes,per_src,indices,start_i,end_i,starter,raw_cum,clip_cum,primary):
    mask=mask_for(indices)
    current=starter
    day=start_i
    route_start=start_i
    raw_factor=1.0
    neutral_factor=1.0
    pos_by_src={}
    p_bounds={i:(x["start_i"],x["end_i"]) for i,x in primary.items()}

    while day < end_i:
        lst=per_src[current]
        p=pos_by_src.get(current,0)
        while p < len(lst) and lst[p][0] < day:
            p+=1

        chosen=None
        while p < len(lst):
            event_day,cands=lst[p]
            if event_day >= end_i:
                break
            target=choose_target(cands,mask)
            p+=1
            if target is None:
                continue
            chosen=(event_day,target,p)
            break

        if chosen is None:
            raw_factor *= segment_factor(raw_cum,route_start,end_i,current)
            neutral_factor *= segment_factor(clip_cum,route_start,end_i,current)
            break

        event_day,target,pnext=chosen
        exec_i=event_day+1

        raw_factor *= segment_factor(raw_cum,route_start,exec_i-1,current)
        neutral_factor *= segment_factor(clip_cum,route_start,exec_i-1,current)

        trans=(opens[exec_i,current]/closes[exec_i-1,current])*(1.0-0.001)*(closes[exec_i,target]/opens[exec_i,target])
        raw_factor *= trans
        ntrans=trans
        b=p_bounds.get(target)
        if b is not None and b[0] <= exec_i <= b[1] and trans>1.0:
            ntrans=1.0
        neutral_factor *= ntrans

        pos_by_src[current]=pnext
        current=target
        day=exec_i
        route_start=exec_i
    else:
        pass

    return raw_factor-1.0, neutral_factor-1.0


def universe_metrics(opens,closes,per_src,indices,start_i,end_i,raw_cum,clip_cum,primary):
    raw=[]
    neutral=[]
    for s in indices:
        r,n=terminal_route_neutralized(
            opens,closes,per_src,indices,start_i,end_i,s,raw_cum,clip_cum,primary
        )
        raw.append(r); neutral.append(n)
    return {
        "raw_mature_median":float(np.median(raw)),
        "neutral_mature_median":float(np.median(neutral)),
        "neutral_worst_start":float(np.min(neutral)),
        "neutral_best_start":float(np.max(neutral)),
    }


def key(indices):
    return "|".join(POOL[i] for i in indices)


def exhaustive(panel,dates,opens,closes,per_src,raw_cum,clip_cum,primary):
    lo,hi=bounds(dates,MATURE_START,END)
    rows=[]
    for size in range(6,16):
        for combo in itertools.combinations(range(len(POOL)),size):
            m=universe_metrics(opens,closes,per_src,combo,lo,hi,raw_cum,clip_cum,primary)
            rows.append({"key":key(combo),"size":size,**m})
    df=pd.DataFrame(rows)
    df=df.sort_values(
        ["neutral_mature_median","raw_mature_median","size","key"],
        ascending=[False,False,True,True],kind="stable"
    ).reset_index(drop=True)
    df["neutral_rank"]=np.arange(1,len(df)+1)
    return df


def shortlist(df):
    keys=set(df.head(100)["key"])
    for size in range(6,16):
        keys.update(df[df["size"]==size].head(5)["key"].tolist())
    keys.add("|".join(CURRENT_U10))
    keys.add("TWT|PEPE|BNB|TRX|AAVE|AVAX|FIL|HBAR")
    keys.add("TWT|PEPE|BNB|TRX|AAVE|LINK|AVAX|FIL|ALGO|HBAR")
    return df[df["key"].isin(keys)].copy()


def inds(k):
    return tuple(AIDX[a] for a in k.split("|"))


def robust_eval(dates,opens,closes,by_day,short):
    rows=[]
    for _,r in short.iterrows():
        ii=inds(r["key"])
        y1=summary_daily(dates,opens,closes,by_day,ii,YEAR_START,END)
        y2=summary_daily(dates,opens,closes,by_day,ii,TWO_YEAR_START,END)
        r12=rolling_summary(dates,opens,closes,by_day,ii,MATURE_START,12)
        r24=rolling_summary(dates,opens,closes,by_day,ii,MATURE_START,24)
        rows.append({
            "key":r["key"],"size":int(r["size"]),
            "raw_mature_median":float(r["raw_mature_median"]),
            "neutral_mature_median":float(r["neutral_mature_median"]),
            "neutral_rank":int(r["neutral_rank"]),
            "latest_1y":y1["median_return"],
            "latest_2y":y2["median_return"],
            "worst_12m":r12["worst"],
            "positive_12m":r12["positive_rate"],
            "worst_24m":r24["worst"],
            "positive_24m":r24["positive_rate"],
            "median_max_dd":summary_daily(dates,opens,closes,by_day,ii,MATURE_START,END)["median_max_dd"],
        })
    x=pd.DataFrame(rows)
    x["eligible"]=(
        (x["latest_1y"]>0) &
        (x["latest_2y"]>0) &
        (x["positive_12m"]>=0.60) &
        (x["positive_24m"]>=0.80)
    )
    x=x.sort_values(
        ["eligible","neutral_mature_median","latest_2y","latest_1y","size","key"],
        ascending=[False,False,False,False,True,True],kind="stable"
    ).reset_index(drop=True)
    return x


def endpoint_eval(dates,opens,closes,by_day,ii):
    rows=[]
    for e in ENDPOINTS:
        s=e-pd.Timedelta(days=364)
        x=summary_daily(dates,opens,closes,by_day,ii,s,e)
        rows.append({"start":s.isoformat(),"end":e.isoformat(),**x})
    return rows


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cutoff",default="2026-09-28T00:00:00Z")
    args=ap.parse_args()

    panel,meta=download_panel(utc(args.cutoff))
    by_day=build_events(panel)
    dates,opens,closes,_=arrays(panel)
    per_src=prep_event_lists(dates,by_day)
    primary=primary_intervals(panel)
    raw_cum,clip_cum=build_clipped_cumlogs(dates,closes,primary)

    run_dir=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True,exist_ok=True)

    all_df=exhaustive(panel,dates,opens,closes,per_src,raw_cum,clip_cum,primary)
    all_df.to_csv(run_dir/"all_u6_to_u15_bull_neutralized.csv",index=False)

    short=shortlist(all_df)
    robust=robust_eval(dates,opens,closes,by_day,short)
    robust.to_csv(run_dir/"bull_neutralized_robust_shortlist.csv",index=False)

    eligible=robust[robust["eligible"]]
    if eligible.empty:
        raise RuntimeError("No eligible conservative champion")
    champ=eligible.iloc[0]
    champ_key=champ["key"]
    champ_i=inds(champ_key)

    current_key="|".join(CURRENT_U10)
    current=robust[robust["key"]==current_key].iloc[0]

    cs=set(champ_key.split("|"))
    us=set(CURRENT_U10)
    migration={
        "keep":sorted(cs&us),
        "exit":sorted(us-cs),
        "add":sorted(cs-us),
        "target_size":len(cs),
        "current_size":len(us),
    }

    result={
        "generated_at":pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit":source_sha(),
        "data":meta,
        "primary_assets":{
            POOL[i]:{
                "drawup90":v["drawup90"],"drawup180":v["drawup180"],
                "start":v["start"],"end":v["end"]
            } for i,v in primary.items()
        },
        "search_space_count":int(len(all_df)),
        "conservative_champion":json.loads(pd.DataFrame([champ]).to_json(orient="records"))[0],
        "current_u10":json.loads(pd.DataFrame([current]).to_json(orient="records"))[0],
        "top20_neutralized":json.loads(all_df.head(20).to_json(orient="records")),
        "top20_eligible":json.loads(eligible.head(20).to_json(orient="records")),
        "endpoint_sensitivity":{
            "CONSERVATIVE_CHAMPION":endpoint_eval(dates,opens,closes,by_day,champ_i),
            "CURRENT_U10":endpoint_eval(dates,opens,closes,by_day,inds(current_key)),
        },
        "migration_map":migration,
    }
    (run_dir/"results.json").write_text(
        json.dumps(result,indent=2,sort_keys=True,allow_nan=False),encoding="utf-8"
    )

    lines=[
        "# Bull-Neutralized Target Champion v1","",
        "Mode: STRESS_TEST_ONLY","Live unchanged.","",
        f"Search space: {len(all_df)}","",
        "## CONSERVATIVE TARGET","",
        f"Assets: {champ_key}",
        f"Size: U{int(champ['size'])}",
        f"Bull-neutralized mature median: {100*float(champ['neutral_mature_median']):+.2f}%",
        f"Raw mature median: {100*float(champ['raw_mature_median']):+.2f}%",
        f"Latest 2Y: {100*float(champ['latest_2y']):+.2f}%",
        f"Latest 1Y: {100*float(champ['latest_1y']):+.2f}%",
        f"Worst 12m: {100*float(champ['worst_12m']):+.2f}%",
        f"Worst 24m: {100*float(champ['worst_24m']):+.2f}%",
        "",
        "KEEP: "+", ".join(migration["keep"]),
        "EXIT: "+", ".join(migration["exit"]),
        "ADD: "+", ".join(migration["add"]),
    ]
    (run_dir/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")

    print("run_dir="+str(run_dir))
    print("search_space_count="+str(len(all_df)))
    print("primary_assets="+",".join(POOL[i] for i in sorted(primary)))
    print("champ_key="+champ_key)
    print("champ_size="+str(int(champ["size"])))
    print("champ_neutral=%.6f" % float(champ["neutral_mature_median"]))
    print("champ_raw=%.6f" % float(champ["raw_mature_median"]))
    print("champ_2y=%.6f" % float(champ["latest_2y"]))
    print("champ_1y=%.6f" % float(champ["latest_1y"]))
    print("champ_worst12=%.6f" % float(champ["worst_12m"]))
    print("champ_worst24=%.6f" % float(champ["worst_24m"]))
    print("current_neutral=%.6f" % float(current["neutral_mature_median"]))
    print("current_raw=%.6f" % float(current["raw_mature_median"]))
    print("migration_keep="+",".join(migration["keep"]))
    print("migration_exit="+",".join(migration["exit"]))
    print("migration_add="+",".join(migration["add"]))
    return 0


if __name__=="__main__":
    raise SystemExit(main())
