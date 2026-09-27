from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
from collections import Counter, defaultdict, deque
from pathlib import Path

import numpy as np
import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import build_pair_monitor

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "atom_replacement_expanded_v1"

BASE = ("TWT","PEPE","BNB","SOL","TRX","AAVE","LINK","FIL","HBAR")
CANDIDATES = (
    "BTC","ETH","XRP","DOGE","ADA","AVAX","DOT","LTC","BCH","NEAR","UNI",
    "ICP","XLM","ETC","RUNE","CRV","SAND","MANA","OP","ARB","APE",
    "GALA","AXS","THETA","VET","ALGO","XTZ","CHZ","ENJ","COMP","LDO",
)

LOOKBACK = 180
ARM = 0.15
REVERSAL = 0.03
COST = 0.001

DATA_START = pd.Timestamp("2023-05-05",tz="UTC")
MATURE_START = pd.Timestamp("2023-10-31",tz="UTC")
TWO_YEAR_START = pd.Timestamp("2024-09-27",tz="UTC")
YEAR_START = pd.Timestamp("2025-09-27",tz="UTC")
END = pd.Timestamp("2026-09-26",tz="UTC")

ENDPOINTS = (
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


def download_one(client,asset,cutoff):
    result=download_historical_dataset(
        client,
        symbol=asset+"USDT",
        start=DATA_START,
        end=cutoff,
        timeframe="1D",
        as_of=cutoff,
    )
    if result.dataset is None:
        return None,{"status":"NO_DATASET","detail":str(result.metadata.status)}
    if result.dataset.quality.has_critical_issues:
        return None,{"status":"CRITICAL_DATA_QUALITY"}
    f=result.dataset.candles[["timestamp","open","close"]].copy()
    f=f.sort_values("timestamp").reset_index(drop=True)
    return f,{
        "status":"OK",
        "rows":len(f),
        "start":pd.Timestamp(f.iloc[0]["timestamp"]).isoformat(),
        "end":pd.Timestamp(f.iloc[-1]["timestamp"]).isoformat(),
    }


def download_base(client,cutoff):
    panel=None
    meta={}
    for a in BASE:
        f,m=download_one(client,a,cutoff)
        if f is None:
            raise RuntimeError(f"base asset {a} unavailable: {m}")
        meta[a]=m
        f=f.rename(columns={"open":a+"_open","close":a+"_close"})
        panel=f if panel is None else panel.merge(f,on="timestamp",how="inner",validate="one_to_one")
    panel=panel.sort_values("timestamp").reset_index(drop=True)
    return panel,meta


def build_event_index(panel,assets):
    cols=["timestamp"]+[a+"_close" for a in assets]
    events,_=build_pair_monitor(
        panel[cols].copy(),
        assets=assets,
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


def prep_arrays(panel,assets):
    dates=[utc(x) for x in panel["timestamp"]]
    opens={a:panel[a+"_open"].astype(float).to_numpy() for a in assets}
    closes={a:panel[a+"_close"].astype(float).to_numpy() for a in assets}
    return dates,opens,closes


def bounds(dates,start,end):
    lo=next(i for i,d in enumerate(dates) if d>=start)
    hi=max(i for i,d in enumerate(dates) if d<=end)
    return lo,hi


def run_one(dates,opens,closes,event_idx,assets,start,end,start_asset,collect=False):
    aset=set(assets)
    lo,hi=bounds(dates,start,end)
    current=start_asset
    qty=1.0/opens[current][lo]
    pending=None
    equity=[]
    holdings=[]
    held_dates=[]
    edges=Counter()
    transitions=0
    conflicts=0

    for i in range(lo,hi+1):
        ts=dates[i]
        if pending is not None:
            value=qty*opens[current][i]
            old=current
            current=pending
            qty=value*(1.0-COST)/opens[current][i]
            transitions+=1
            edges[(old,current)]+=1
            pending=None

        equity.append(qty*closes[current][i])
        holdings.append(current)
        held_dates.append(ts)

        if i==hi:
            continue
        cands=[
            e for e in event_idx.get(ts,{}).get(current,[])
            if e["to_asset"] in aset
        ]
        if not cands:
            continue
        if len(cands)>1:
            conflicts+=1
        pending=cands[0]["to_asset"]

    arr=np.asarray(equity,dtype=float)
    peaks=np.maximum.accumulate(arr)
    out={
        "start_asset":start_asset,
        "return":float(arr[-1]/arr[0]-1.0),
        "max_dd":float(np.min(arr/peaks-1.0)),
        "transitions":transitions,
        "conflicts":conflicts,
    }
    if collect:
        out.update({
            "equity":equity,
            "holdings":holdings,
            "dates":held_dates,
            "edges":edges,
        })
    return out


def summarize(dates,opens,closes,event_idx,assets,start,end):
    rows=[run_one(dates,opens,closes,event_idx,assets,start,end,a) for a in BASE]
    df=pd.DataFrame(rows)
    return {
        "median_return":float(df["return"].median()),
        "worst_return":float(df["return"].min()),
        "best_return":float(df["return"].max()),
        "positive_starts":int((df["return"]>0).sum()),
        "median_max_dd":float(df["max_dd"].median()),
        "worst_max_dd":float(df["max_dd"].min()),
        "median_transitions":float(df["transitions"].median()),
        "median_conflicts":float(df["conflicts"].median()),
    }


def max_drawup(panel,asset,days):
    dates=[utc(x) for x in panel["timestamp"]]
    prices=panel[asset+"_close"].astype(float).to_numpy()
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
                best=gain
                bi,bj=i,j
    return {
        "drawup":float(best),
        "start":dates[bi].isoformat(),
        "end":dates[bj].isoformat(),
    }


def bull_metrics(panel,asset):
    d90=max_drawup(panel,asset,90)
    d180=max_drawup(panel,asset,180)
    return {
        "max_90d_drawup":d90["drawup"],
        "max_90d_start":d90["start"],
        "max_90d_end":d90["end"],
        "max_180d_drawup":d180["drawup"],
        "max_180d_start":d180["start"],
        "max_180d_end":d180["end"],
        "primary_explosive":bool(d90["drawup"]>=2.0 or d180["drawup"]>=4.0),
    }


def neutralized_return(run,candidate,bull_start,bull_end):
    value=1.0
    for i in range(1,len(run["equity"])):
        ratio=run["equity"][i]/run["equity"][i-1]
        if (
            run["holdings"][i]==candidate
            and bull_start<=run["dates"][i]<=bull_end
            and ratio>1.0
        ):
            ratio=1.0
        value*=ratio
    return float(value-1.0)


def neutralized_summary(dates,opens,closes,event_idx,assets,candidate,bull):
    bs=utc(bull["max_90d_start"])
    be=utc(bull["max_90d_end"])
    vals=[]
    for starter in BASE:
        run=run_one(dates,opens,closes,event_idx,assets,MATURE_START,END,starter,collect=True)
        vals.append(neutralized_return(run,candidate,bs,be))
    return float(np.median(vals))


def topology(dates,opens,closes,event_idx,assets,candidate,start,end):
    total_days=candidate_days=entries=exits=ending=0
    incoming=Counter()
    outgoing=Counter()
    for starter in BASE:
        r=run_one(dates,opens,closes,event_idx,assets,start,end,starter,collect=True)
        total_days+=len(r["holdings"])
        candidate_days+=sum(1 for x in r["holdings"] if x==candidate)
        if r["holdings"][-1]==candidate:
            ending+=1
        for (a,b),n in r["edges"].items():
            if b==candidate:
                entries+=n
                incoming[a]+=n
            if a==candidate:
                exits+=n
                outgoing[b]+=n
    return {
        "occupancy_share":candidate_days/total_days if total_days else 0.0,
        "entries":entries,
        "exits":exits,
        "routes_ending_rate":ending/len(BASE),
        "incoming_edges":dict(incoming.most_common()),
        "outgoing_edges":dict(outgoing.most_common()),
    }


def monthly_windows(dates,months):
    cursor=pd.Timestamp(MATURE_START.year,MATURE_START.month,1,tz="UTC")
    if cursor<MATURE_START:
        cursor+=pd.offsets.MonthBegin(1)
    out=[]
    while True:
        starts=[d for d in dates if d>=cursor]
        if not starts:
            break
        start=starts[0]
        target=start+pd.DateOffset(months=months)-pd.Timedelta("1D")
        if target>END:
            break
        end=max(d for d in dates if d<=target)
        out.append((start,end))
        cursor+=pd.offsets.MonthBegin(1)
    return out


def rolling_summary(dates,opens,closes,event_idx,assets,months):
    vals=[]
    for start,end in monthly_windows(dates,months):
        vals.append(summarize(dates,opens,closes,event_idx,assets,start,end))
    df=pd.DataFrame(vals)
    return {
        "count":len(df),
        "median_window_return":float(df["median_return"].median()),
        "worst_window_return":float(df["median_return"].min()),
        "positive_window_rate":float((df["median_return"]>0).mean()),
    }


def endpoint_sensitivity(dates,opens,closes,event_idx,assets):
    rows=[]
    for end in ENDPOINTS:
        start=end-pd.Timedelta(days=364)
        rows.append({"end":end.isoformat(),**summarize(dates,opens,closes,event_idx,assets,start,end)})
    return rows


def data_eligibility(base_panel,candidate_frame,candidate):
    if candidate_frame is None or candidate_frame.empty:
        return False,"NO_DATA"
    cstart=utc(candidate_frame.iloc[0]["timestamp"])
    cend=utc(candidate_frame.iloc[-1]["timestamp"])
    if cend < END:
        return False,f"DATA_ENDS_EARLY:{cend.date()}"
    merged=base_panel[["timestamp"]].merge(candidate_frame[["timestamp"]],on="timestamp",how="inner")
    base_eval=base_panel[(base_panel["timestamp"]>=DATA_START)&(base_panel["timestamp"]<=END)]
    if len(merged) != len(base_eval):
        return False,f"TIMESTAMP_MISMATCH:{len(merged)}/{len(base_eval)}"
    warmup_rows=int((merged["timestamp"]<=MATURE_START).sum())
    if warmup_rows < LOOKBACK:
        return False,f"INSUFFICIENT_WARMUP:{warmup_rows}"
    return True,"OK"


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cutoff",default="2026-09-27T00:00:00Z")
    args=ap.parse_args()
    cutoff=utc(args.cutoff)

    client=BinanceSpotRestClient()
    base_panel,base_meta=download_base(client,cutoff)
    base_events=build_event_index(base_panel,BASE)
    base_dates,base_opens,base_closes=prep_arrays(base_panel,BASE)

    base={
        "mature":summarize(base_dates,base_opens,base_closes,base_events,BASE,MATURE_START,END),
        "latest_2y":summarize(base_dates,base_opens,base_closes,base_events,BASE,TWO_YEAR_START,END),
        "latest_1y":summarize(base_dates,base_opens,base_closes,base_events,BASE,YEAR_START,END),
        "rolling_12m":rolling_summary(base_dates,base_opens,base_closes,base_events,BASE,12),
        "rolling_24m":rolling_summary(base_dates,base_opens,base_closes,base_events,BASE,24),
        "endpoints":endpoint_sensitivity(base_dates,base_opens,base_closes,base_events,BASE),
    }

    result={
        "generated_at":pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit":source_sha(),
        "base_assets":list(BASE),
        "base_data":base_meta,
        "base":base,
        "candidates":{},
    }
    comparison=[]

    for candidate in CANDIDATES:
        frame,meta=download_one(client,candidate,cutoff)
        eligible,reason=data_eligibility(base_panel,frame,candidate)
        if not eligible:
            result["candidates"][candidate]={
                "eligible":False,
                "reason":reason,
                "data":meta,
            }
            comparison.append({
                "candidate":candidate,
                "eligible":False,
                "reason":reason,
            })
            print(candidate+"_eligible=FALSE:"+reason)
            continue

        renamed=frame.rename(columns={"open":candidate+"_open","close":candidate+"_close"})
        panel=base_panel.merge(renamed,on="timestamp",how="inner",validate="one_to_one")
        assets=BASE+(candidate,)
        events=build_event_index(panel,assets)
        dates,opens,closes=prep_arrays(panel,assets)

        mature=summarize(dates,opens,closes,events,assets,MATURE_START,END)
        y2=summarize(dates,opens,closes,events,assets,TWO_YEAR_START,END)
        y1=summarize(dates,opens,closes,events,assets,YEAR_START,END)
        rolling12=rolling_summary(dates,opens,closes,events,assets,12)
        rolling24=rolling_summary(dates,opens,closes,events,assets,24)
        endpoints=endpoint_sensitivity(dates,opens,closes,events,assets)
        endpoint_increments=[
            {
                "end":c["end"],
                "increment":c["median_return"]-b["median_return"],
            }
            for b,c in zip(base["endpoints"],endpoints)
        ]
        bull=bull_metrics(panel,candidate)
        neutral=neutralized_summary(dates,opens,closes,events,assets,candidate,bull)
        topo=topology(dates,opens,closes,events,assets,candidate,MATURE_START,END)

        dm=mature["median_return"]-base["mature"]["median_return"]
        d2=y2["median_return"]-base["latest_2y"]["median_return"]
        d1=y1["median_return"]-base["latest_1y"]["median_return"]
        nonneg=sum(1 for x in endpoint_increments if x["increment"]>=0)
        positive=sum(1 for x in endpoint_increments if x["increment"]>0)
        broad=bool(dm>0 and d2>=0 and d1>=0 and nonneg>=4)
        strict=bool(dm>0 and d2>0 and d1>0 and positive==5)

        payload={
            "eligible":True,
            "data":meta,
            "assets":list(assets),
            "mature":mature,
            "latest_2y":y2,
            "latest_1y":y1,
            "rolling_12m":rolling12,
            "rolling_24m":rolling24,
            "endpoints":endpoints,
            "endpoint_increments":endpoint_increments,
            "bull":bull,
            "bull_neutralized_mature":neutral,
            "topology":topo,
            "delta_mature":dm,
            "delta_2y":d2,
            "delta_1y":d1,
            "nonnegative_endpoints":nonneg,
            "positive_endpoints":positive,
            "broad_positive":broad,
            "strict_positive":strict,
        }
        result["candidates"][candidate]=payload
        comparison.append({
            "candidate":candidate,
            "eligible":True,
            "reason":"OK",
            "mature_return":mature["median_return"],
            "delta_mature":dm,
            "latest_2y_return":y2["median_return"],
            "delta_2y":d2,
            "latest_1y_return":y1["median_return"],
            "delta_1y":d1,
            "worst_12m":rolling12["worst_window_return"],
            "worst_24m":rolling24["worst_window_return"],
            "bull_neutralized_mature":neutral,
            "occupancy":topo["occupancy_share"],
            "entries":topo["entries"],
            "exits":topo["exits"],
            "nonnegative_endpoints":nonneg,
            "positive_endpoints":positive,
            "broad_positive":broad,
            "strict_positive":strict,
        })
        print(
            f"{candidate}_eligible=TRUE mature={mature['median_return']:.6f} "
            f"dm={dm:.6f} d2={d2:.6f} d1={d1:.6f} "
            f"endpoints={nonneg}/5 broad={broad} strict={strict}"
        )

    run_dir=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True,exist_ok=True)
    cdf=pd.DataFrame(comparison)
    if "eligible" in cdf.columns:
        cdf=cdf.sort_values(
            ["eligible","broad_positive","strict_positive","delta_mature"],
            ascending=[False,False,False,False],
            na_position="last",
        )
    cdf.to_csv(run_dir/"candidate_comparison.csv",index=False)
    (run_dir/"results.json").write_text(
        json.dumps(result,indent=2,sort_keys=True,allow_nan=False),
        encoding="utf-8"
    )

    eligible_rows=[x for x in comparison if x.get("eligible")]
    broad_rows=[x for x in eligible_rows if x.get("broad_positive")]
    strict_rows=[x for x in eligible_rows if x.get("strict_positive")]

    lines=[
        "# ATOM Replacement Expanded v1","",
        "Mode: STRESS_TEST_ONLY",
        "Live unchanged. PR #54 remains unmerged.","",
        f"Eligible candidates: {len(eligible_rows)}/{len(CANDIDATES)}",
        f"BROAD_POSITIVE: {len(broad_rows)}",
        f"STRICT_POSITIVE: {len(strict_rows)}",
        "",
        "| Candidate | Mature Δ | 2Y Δ | 1Y Δ | Endpoints nonneg | Worst 12m | Worst 24m | Broad | Strict |",
        "|---|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for row in eligible_rows:
        lines.append(
            f"| {row['candidate']} | {100*row['delta_mature']:+.2f} pp | "
            f"{100*row['delta_2y']:+.2f} pp | {100*row['delta_1y']:+.2f} pp | "
            f"{row['nonnegative_endpoints']}/5 | {100*row['worst_12m']:+.2f}% | "
            f"{100*row['worst_24m']:+.2f}% | {row['broad_positive']} | {row['strict_positive']} |"
        )
    (run_dir/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")

    print("run_dir="+str(run_dir))
    print("eligible_count="+str(len(eligible_rows)))
    print("broad_positive="+",".join(x["candidate"] for x in broad_rows))
    print("strict_positive="+",".join(x["candidate"] for x in strict_rows))
    return 0


if __name__=="__main__":
    raise SystemExit(main())
