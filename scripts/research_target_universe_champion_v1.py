from __future__ import annotations

import argparse
import itertools
import json
import math
import os
import subprocess
from collections import defaultdict, deque
from pathlib import Path

import numpy as np
import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import build_pair_monitor

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "target_universe_champion_v1"

POOL = (
    "ATOM","TWT","PEPE","BNB","SOL","TRX","AAVE","LINK",
    "AVAX","FIL","ETH","ALGO","ADA","XRP","HBAR",
)
AIDX = {a:i for i,a in enumerate(POOL)}
CURRENT_U10 = ("ATOM","TWT","PEPE","BNB","SOL","TRX","AAVE","LINK","FIL","HBAR")

LOOKBACK=180
ARM=0.15
REVERSAL=0.03
COST=0.001
DATA_START=pd.Timestamp("2023-05-05",tz="UTC")
MATURE_START=pd.Timestamp("2023-10-31",tz="UTC")
TWO_YEAR_START=pd.Timestamp("2024-09-27",tz="UTC")
YEAR_START=pd.Timestamp("2025-09-27",tz="UTC")
END=pd.Timestamp("2026-09-26",tz="UTC")
ENDPOINTS=(
    pd.Timestamp("2026-05-31",tz="UTC"),
    pd.Timestamp("2026-06-30",tz="UTC"),
    pd.Timestamp("2026-07-31",tz="UTC"),
    pd.Timestamp("2026-08-31",tz="UTC"),
    pd.Timestamp("2026-09-26",tz="UTC"),
)


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
        r=download_historical_dataset(
            client,symbol=a+"USDT",start=DATA_START,end=cutoff,timeframe="1D",as_of=cutoff
        )
        if r.dataset is None:
            raise RuntimeError(f"{a}: {r.metadata.status}")
        if r.dataset.quality.has_critical_issues:
            raise RuntimeError(f"{a}: critical data quality")
        f=r.dataset.candles[["timestamp","open","close"]].copy()
        meta[a]={
            "rows":len(f),
            "start":pd.Timestamp(f.iloc[0]["timestamp"]).isoformat(),
            "end":pd.Timestamp(f.iloc[-1]["timestamp"]).isoformat(),
        }
        f=f.rename(columns={"open":a+"_open","close":a+"_close"})
        panel=f if panel is None else panel.merge(f,on="timestamp",how="inner",validate="one_to_one")
    panel=panel.sort_values("timestamp").reset_index(drop=True)
    return panel,meta


def build_events(panel):
    cols=["timestamp"]+[a+"_close" for a in POOL]
    events,_=build_pair_monitor(
        panel[cols].copy(),assets=POOL,lookback=LOOKBACK,
        arm_threshold=ARM,reversal=REVERSAL,
    )
    by_day=defaultdict(lambda:defaultdict(list))
    for e in events:
        if e["event"]!="CONFIRMED":
            continue
        d=utc(e["date"])
        by_day[d][AIDX[e["from_asset"]]].append(
            (AIDX[e["to_asset"]],float(e["max_dislocation"]),e["pair"])
        )
    for d in by_day:
        for src in by_day[d]:
            by_day[d][src].sort(key=lambda x:(-x[1],POOL[x[0]],x[2]))
    return by_day


def arrays(panel):
    dates=[utc(x) for x in panel["timestamp"]]
    opens=np.column_stack([panel[a+"_open"].astype(float).to_numpy() for a in POOL])
    closes=np.column_stack([panel[a+"_close"].astype(float).to_numpy() for a in POOL])
    d2i={d:i for i,d in enumerate(dates)}
    return dates,opens,closes,d2i


def bounds(dates,start,end):
    lo=next(i for i,d in enumerate(dates) if d>=start)
    hi=max(i for i,d in enumerate(dates) if d<=end)
    return lo,hi


def mask_for(indices):
    m=0
    for i in indices:
        m |= 1<<i
    return m


def prep_event_lists(dates,by_day):
    per_src=[[] for _ in POOL]
    for day_i,d in enumerate(dates):
        srcmap=by_day.get(d,{})
        for src,cands in srcmap.items():
            per_src[src].append((day_i,cands))
    return per_src


def choose_target(cands,mask):
    for target,disl,pair in cands:
        if mask & (1<<target):
            return target
    return None


def terminal_route_fast(opens,closes,per_src,indices,start_i,end_i,starter):
    mask=mask_for(indices)
    current=starter
    qty=1.0/opens[start_i,current]
    pos_by_src={current:0}
    day=start_i

    while day < end_i:
        lst=per_src[current]
        p=pos_by_src.get(current,0)
        while p < len(lst) and lst[p][0] < day:
            p += 1
        found=False
        while p < len(lst):
            event_day,cands=lst[p]
            if event_day >= end_i:
                break
            target=choose_target(cands,mask)
            p += 1
            if target is None:
                continue
            exec_i=event_day+1
            value=qty*opens[exec_i,current]
            qty=value*(1.0-COST)/opens[exec_i,target]
            pos_by_src[current]=p
            current=target
            day=exec_i
            found=True
            break
        if not found:
            break
    return float(qty*closes[end_i,current]/(1.0))


def universe_terminal_median(opens,closes,per_src,indices,start_i,end_i):
    rets=[]
    for starter in indices:
        final=terminal_route_fast(opens,closes,per_src,indices,start_i,end_i,starter)
        initial_equity=closes[start_i,starter]/opens[start_i,starter]
        rets.append(final/initial_equity-1.0)
    return float(np.median(rets)), float(np.min(rets)), float(np.max(rets))


def run_daily(dates,opens,closes,by_day,indices,start,end,starter,collect=False):
    lo,hi=bounds(dates,start,end)
    mask=mask_for(indices)
    current=starter
    qty=1.0/opens[lo,current]
    pending=None
    equity=[]
    holdings=[]
    held_dates=[]
    transitions=0
    conflicts=0

    for i in range(lo,hi+1):
        d=dates[i]
        if pending is not None:
            value=qty*opens[i,current]
            current=pending
            qty=value*(1.0-COST)/opens[i,current]
            transitions+=1
            pending=None
        equity.append(qty*closes[i,current])
        holdings.append(current)
        held_dates.append(d)

        if i==hi:
            continue
        elig=[]
        for t,disl,pair in by_day.get(d,{}).get(current,[]):
            if mask & (1<<t):
                elig.append(t)
        if elig:
            if len(elig)>1:
                conflicts+=1
            pending=elig[0]

    arr=np.asarray(equity,dtype=float)
    peaks=np.maximum.accumulate(arr)
    out={
        "return":float(arr[-1]/arr[0]-1.0),
        "max_dd":float(np.min(arr/peaks-1.0)),
        "transitions":transitions,
        "conflicts":conflicts,
    }
    if collect:
        out.update({"equity":equity,"holdings":holdings,"dates":held_dates})
    return out


def summary_daily(dates,opens,closes,by_day,indices,start,end):
    rows=[run_daily(dates,opens,closes,by_day,indices,start,end,s) for s in indices]
    rets=np.asarray([r["return"] for r in rows],dtype=float)
    dds=np.asarray([r["max_dd"] for r in rows],dtype=float)
    trans=np.asarray([r["transitions"] for r in rows],dtype=float)
    conf=np.asarray([r["conflicts"] for r in rows],dtype=float)
    return {
        "median_return":float(np.median(rets)),
        "worst_return":float(np.min(rets)),
        "best_return":float(np.max(rets)),
        "positive_starts":int(np.sum(rets>0)),
        "median_max_dd":float(np.median(dds)),
        "worst_max_dd":float(np.min(dds)),
        "median_transitions":float(np.median(trans)),
        "median_conflicts":float(np.median(conf)),
    }


def monthly_windows(dates,start,months):
    cursor=pd.Timestamp(start.year,start.month,1,tz="UTC")
    if cursor<start:
        cursor+=pd.offsets.MonthBegin(1)
    out=[]
    while True:
        cand=[d for d in dates if d>=cursor]
        if not cand:
            break
        s=cand[0]
        target=s+pd.DateOffset(months=months)-pd.Timedelta("1D")
        if target>END:
            break
        e=max(d for d in dates if d<=target)
        out.append((s,e))
        cursor+=pd.offsets.MonthBegin(1)
    return out


def rolling_summary(dates,opens,closes,by_day,indices,start,months):
    vals=[]
    for s,e in monthly_windows(dates,start,months):
        vals.append(summary_daily(dates,opens,closes,by_day,indices,s,e)["median_return"])
    if not vals:
        return {"count":0}
    a=np.asarray(vals,dtype=float)
    return {
        "count":len(vals),
        "median":float(np.median(a)),
        "worst":float(np.min(a)),
        "positive_rate":float(np.mean(a>0)),
    }


def key(indices):
    return "|".join(POOL[i] for i in indices)


def exhaustive_mature(dates,opens,closes,per_src):
    lo,hi=bounds(dates,MATURE_START,END)
    rows=[]
    for size in range(6,16):
        for combo in itertools.combinations(range(len(POOL)),size):
            med,worst,best=universe_terminal_median(opens,closes,per_src,combo,lo,hi)
            rows.append({
                "key":key(combo),
                "size":size,
                "indices":combo,
                "mature_median":med,
                "mature_worst_start":worst,
                "mature_best_start":best,
            })
    df=pd.DataFrame(rows)
    df=df.sort_values(["mature_median","size","key"],ascending=[False,True,True],kind="stable").reset_index(drop=True)
    df["overall_rank"]=np.arange(1,len(df)+1)
    return df


def shortlist_from(df):
    keys=set(df.head(100)["key"])
    for size in range(6,16):
        for k in df[df["size"]==size].head(5)["key"]:
            keys.add(k)
    keys.add("|".join(CURRENT_U10))
    return df[df["key"].isin(keys)].copy()


def parse_indices(k):
    return tuple(AIDX[a] for a in k.split("|"))


def robust_eval(dates,opens,closes,by_day,short):
    rows=[]
    for _,r in short.iterrows():
        inds=parse_indices(r["key"])
        mature=summary_daily(dates,opens,closes,by_day,inds,MATURE_START,END)
        y2=summary_daily(dates,opens,closes,by_day,inds,TWO_YEAR_START,END)
        y1=summary_daily(dates,opens,closes,by_day,inds,YEAR_START,END)
        r12=rolling_summary(dates,opens,closes,by_day,inds,MATURE_START,12)
        r24=rolling_summary(dates,opens,closes,by_day,inds,MATURE_START,24)
        rows.append({
            "key":r["key"],"size":int(r["size"]),
            "mature_median":mature["median_return"],
            "latest_2y":y2["median_return"],
            "latest_1y":y1["median_return"],
            "worst_12m":r12["worst"],
            "positive_12m":r12["positive_rate"],
            "worst_24m":r24["worst"],
            "positive_24m":r24["positive_rate"],
            "median_max_dd":mature["median_max_dd"],
            "worst_start":mature["worst_return"],
            "median_transitions":mature["median_transitions"],
            "median_conflicts":mature["median_conflicts"],
        })
    x=pd.DataFrame(rows)
    eligible=(
        (x["latest_1y"]>0) &
        (x["latest_2y"]>0) &
        (x["positive_12m"]>=0.60) &
        (x["positive_24m"]>=0.80)
    )
    x["eligible"]=eligible
    metrics=["mature_median","latest_2y","latest_1y","worst_12m","worst_24m"]
    pct=[]
    for m in metrics:
        c=m+"_pct"
        x[c]=x[m].rank(pct=True,method="average")
        pct.append(c)
    x["robust_profit_score"]=x[pct].mean(axis=1)
    x=x.sort_values(
        ["eligible","robust_profit_score","mature_median","size","key"],
        ascending=[False,False,False,True,True],kind="stable"
    ).reset_index(drop=True)
    return x


def max_drawup(frame,days):
    prices=frame.astype(float).to_numpy()
    q=deque()
    best=-math.inf
    bi=bj=0
    for j,p in enumerate(prices):
        while q and q[0] < j-days:
            q.popleft()
        while q and prices[q[-1]]>=p:
            q.pop()
        q.append(j)
        i=q[0]
        if i<j:
            gain=p/prices[i]-1.0
            if gain>best:
                best=gain; bi=i; bj=j
    return float(best),bi,bj


def bull_intervals(panel,indices):
    out={}
    for i in indices:
        s=panel[POOL[i]+"_close"].astype(float).reset_index(drop=True)
        d90,i90,j90=max_drawup(s,90)
        d180,i180,j180=max_drawup(s,180)
        out[i]={
            "drawup90":d90,
            "start90":utc(panel.iloc[i90]["timestamp"]),
            "end90":utc(panel.iloc[j90]["timestamp"]),
            "drawup180":d180,
            "primary":bool(d90>=2.0 or d180>=4.0),
        }
    return out


def neutralized_result(dates,opens,closes,by_day,indices,neutral_map):
    vals=[]
    for starter in indices:
        r=run_daily(dates,opens,closes,by_day,indices,MATURE_START,END,starter,collect=True)
        value=1.0
        for j in range(1,len(r["equity"])):
            ratio=r["equity"][j]/r["equity"][j-1]
            held=r["holdings"][j]
            interval=neutral_map.get(held)
            if interval is not None:
                s,e=interval
                if s<=r["dates"][j]<=e and ratio>1.0:
                    ratio=1.0
            value*=ratio
        vals.append(value-1.0)
    return float(np.median(vals))


def bull_audit(panel,dates,opens,closes,by_day,indices):
    base=summary_daily(dates,opens,closes,by_day,indices,MATURE_START,END)["median_return"]
    ints=bull_intervals(panel,indices)
    per={}
    for i,x in ints.items():
        per[POOL[i]]={
            "primary":x["primary"],
            "drawup90":x["drawup90"],
            "drawup180":x["drawup180"],
            "start90":x["start90"].isoformat(),
            "end90":x["end90"].isoformat(),
            "neutralized_median":neutralized_result(
                dates,opens,closes,by_day,indices,{i:(x["start90"],x["end90"])}
            ),
        }
    primary_map={
        i:(x["start90"],x["end90"])
        for i,x in ints.items() if x["primary"]
    }
    all_primary=neutralized_result(dates,opens,closes,by_day,indices,primary_map)
    return {
        "raw_mature_median":base,
        "per_token":per,
        "all_primary_neutralized_median":all_primary,
        "wealth_retention_ratio":(1+all_primary)/(1+base) if 1+base>0 else None,
    }


def endpoint_eval(dates,opens,closes,by_day,indices):
    rows=[]
    for e in ENDPOINTS:
        s=e-pd.Timedelta(days=364)
        x=summary_daily(dates,opens,closes,by_day,indices,s,e)
        rows.append({"start":s.isoformat(),"end":e.isoformat(),**x})
    return rows


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cutoff",default="2026-09-28T00:00:00Z")
    args=ap.parse_args()

    panel,meta=download_panel(utc(args.cutoff))
    by_day=build_events(panel)
    dates,opens,closes,d2i=arrays(panel)
    per_src=prep_event_lists(dates,by_day)

    run_dir=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True,exist_ok=True)

    all_df=exhaustive_mature(dates,opens,closes,per_src)
    all_df.drop(columns=["indices"]).to_csv(run_dir/"all_u6_to_u15_mature.csv",index=False)

    short=shortlist_from(all_df)
    robust=robust_eval(dates,opens,closes,by_day,short)
    robust.to_csv(run_dir/"robust_shortlist.csv",index=False)

    raw_row=all_df.iloc[0]
    raw_key=raw_row["key"]
    raw_inds=parse_indices(raw_key)

    eligible=robust[robust["eligible"]]
    if eligible.empty:
        raise RuntimeError("No TARGET CHAMPION candidate passed fixed robustness gates")
    target_row=eligible.iloc[0]
    target_key=target_row["key"]
    target_inds=parse_indices(target_key)

    current_key="|".join(CURRENT_U10)
    current_inds=tuple(AIDX[a] for a in CURRENT_U10)
    current_rank=int(all_df[all_df["key"]==current_key].iloc[0]["overall_rank"])

    best_by_size=[]
    for size in range(6,16):
        r=all_df[all_df["size"]==size].iloc[0]
        best_by_size.append({
            "size":size,"key":r["key"],"mature_median":float(r["mature_median"]),
            "overall_rank":int(r["overall_rank"]),
        })

    raw_bull=bull_audit(panel,dates,opens,closes,by_day,raw_inds)
    target_bull=bull_audit(panel,dates,opens,closes,by_day,target_inds)

    endpoint={
        "TARGET_CHAMPION":endpoint_eval(dates,opens,closes,by_day,target_inds),
        "CURRENT_U10":endpoint_eval(dates,opens,closes,by_day,current_inds),
    }

    target_set=set(target_key.split("|"))
    current_set=set(CURRENT_U10)
    migration={
        "keep":sorted(current_set & target_set),
        "exit":sorted(current_set - target_set),
        "add":sorted(target_set - current_set),
        "current_size":len(current_set),
        "target_size":len(target_set),
    }

    result={
        "generated_at":pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit":source_sha(),
        "data":meta,
        "search_space_count":int(len(all_df)),
        "raw_champion":{
            "key":raw_key,
            "size":int(raw_row["size"]),
            "mature_median":float(raw_row["mature_median"]),
            "mature_worst_start":float(raw_row["mature_worst_start"]),
            "mature_best_start":float(raw_row["mature_best_start"]),
            "bull_audit":raw_bull,
        },
        "target_champion":{
            **{k:(bool(v) if isinstance(v,(np.bool_,)) else int(v) if isinstance(v,(np.integer,)) else float(v) if isinstance(v,(np.floating,)) else v)
               for k,v in target_row.to_dict().items()},
            "bull_audit":target_bull,
        },
        "current_u10":{
            "key":current_key,
            "overall_mature_rank":current_rank,
            "robust":json.loads(robust[robust["key"]==current_key].to_json(orient="records"))[0],
        },
        "best_by_size":best_by_size,
        "top20_raw":json.loads(all_df.head(20).drop(columns=["indices"]).to_json(orient="records")),
        "top20_robust":json.loads(robust.head(20).to_json(orient="records")),
        "endpoint_sensitivity":endpoint,
        "migration_map":migration,
    }

    (run_dir/"results.json").write_text(
        json.dumps(result,indent=2,sort_keys=True,allow_nan=False),encoding="utf-8"
    )

    lines=[
        "# Target Universe Champion v1","",
        "Mode: STRESS_TEST_ONLY","Live unchanged.","",
        f"Search space: {len(all_df)} universes (U6..U15)","",
        "## RAW CHAMPION","",
        f"Assets: {raw_key}",
        f"Size: U{int(raw_row['size'])}",
        f"Mature median: {100*float(raw_row['mature_median']):+.2f}%",
        "",
        "## TARGET CHAMPION","",
        f"Assets: {target_key}",
        f"Size: U{int(target_row['size'])}",
        f"Mature median: {100*float(target_row['mature_median']):+.2f}%",
        f"Latest 2Y: {100*float(target_row['latest_2y']):+.2f}%",
        f"Latest 1Y: {100*float(target_row['latest_1y']):+.2f}%",
        f"Worst rolling 12m: {100*float(target_row['worst_12m']):+.2f}%",
        f"Worst rolling 24m: {100*float(target_row['worst_24m']):+.2f}%",
        f"Robust score: {float(target_row['robust_profit_score']):.4f}",
        "",
        "## Migration from CURRENT_U10","",
        "KEEP: "+", ".join(migration["keep"]),
        "EXIT: "+", ".join(migration["exit"]),
        "ADD: "+", ".join(migration["add"]),
    ]
    (run_dir/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")

    print("run_dir="+str(run_dir))
    print("search_space_count="+str(len(all_df)))
    print("raw_key="+raw_key)
    print("raw_size="+str(int(raw_row["size"])))
    print("raw_mature=%.6f" % float(raw_row["mature_median"]))
    print("raw_primary_neutral=%.6f" % raw_bull["all_primary_neutralized_median"])
    print("target_key="+target_key)
    print("target_size="+str(int(target_row["size"])))
    print("target_mature=%.6f" % float(target_row["mature_median"]))
    print("target_2y=%.6f" % float(target_row["latest_2y"]))
    print("target_1y=%.6f" % float(target_row["latest_1y"]))
    print("target_worst12=%.6f" % float(target_row["worst_12m"]))
    print("target_worst24=%.6f" % float(target_row["worst_24m"]))
    print("target_score=%.6f" % float(target_row["robust_profit_score"]))
    print("target_primary_neutral=%.6f" % target_bull["all_primary_neutralized_median"])
    print("current_u10_rank="+str(current_rank))
    print("migration_keep="+",".join(migration["keep"]))
    print("migration_exit="+",".join(migration["exit"]))
    print("migration_add="+",".join(migration["add"]))
    return 0


if __name__=="__main__":
    raise SystemExit(main())
