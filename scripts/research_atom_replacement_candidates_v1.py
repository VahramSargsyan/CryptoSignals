from __future__ import annotations

import argparse
import itertools
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
OUT = ROOT / "research_artifacts" / "atom_replacement_candidates_v1"

BASE = ("TWT","PEPE","BNB","SOL","TRX","AAVE","LINK","FIL","HBAR")
CANDIDATES = ("AVAX","ETH","ALGO","ADA","XRP")
ALL_ASSETS = BASE + CANDIDATES
VARIANTS = {"U9_NO_ATOM": BASE}
VARIANTS.update({f"U10_{c}": BASE + (c,) for c in CANDIDATES})

LOOKBACK = 180
ARM = 0.15
REVERSAL = 0.03
COST = 0.001

DATA_START = pd.Timestamp("2023-05-05", tz="UTC")
MATURE_START = pd.Timestamp("2023-10-31", tz="UTC")
TWO_YEAR_START = pd.Timestamp("2024-09-27", tz="UTC")
YEAR_START = pd.Timestamp("2025-09-27", tz="UTC")
END = pd.Timestamp("2026-09-26", tz="UTC")

ENDPOINTS = (
    pd.Timestamp("2026-05-31",tz="UTC"),
    pd.Timestamp("2026-06-30",tz="UTC"),
    pd.Timestamp("2026-07-31",tz="UTC"),
    pd.Timestamp("2026-08-31",tz="UTC"),
    pd.Timestamp("2026-09-26",tz="UTC"),
)

WINDOW_CACHE = {}


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
    for asset in ALL_ASSETS:
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
            raise RuntimeError(f"{asset}: critical data quality issues")
        f = result.dataset.candles[["timestamp","open","close"]].copy()
        meta[asset] = {
            "rows": len(f),
            "start": pd.Timestamp(f.iloc[0]["timestamp"]).isoformat(),
            "end": pd.Timestamp(f.iloc[-1]["timestamp"]).isoformat(),
        }
        f = f.rename(columns={"open":asset+"_open","close":asset+"_close"})
        panel = f if panel is None else panel.merge(f,on="timestamp",how="inner",validate="one_to_one")
    panel = panel.sort_values("timestamp").reset_index(drop=True)
    return panel, meta


def build_event_index(panel):
    cols = ["timestamp"] + [a+"_close" for a in ALL_ASSETS]
    events,_ = build_pair_monitor(
        panel[cols].copy(),
        assets=ALL_ASSETS,
        lookback=LOOKBACK,
        arm_threshold=ARM,
        reversal=REVERSAL,
    )
    idx = defaultdict(lambda: defaultdict(list))
    for e in events:
        if e["event"] != "CONFIRMED":
            continue
        d = utc(e["date"])
        idx[d][e["from_asset"]].append({
            "to_asset": e["to_asset"],
            "pair": e["pair"],
            "max_dislocation": float(e["max_dislocation"]),
        })
    for d in idx:
        for a in idx[d]:
            idx[d][a].sort(key=lambda e:(-e["max_dislocation"],e["to_asset"],e["pair"]))
    return idx


def prep_arrays(panel):
    dates = [utc(x) for x in panel["timestamp"]]
    opens = {a: panel[a+"_open"].astype(float).to_numpy() for a in ALL_ASSETS}
    closes = {a: panel[a+"_close"].astype(float).to_numpy() for a in ALL_ASSETS}
    return dates,opens,closes


def bounds(dates,start,end):
    key=(start.isoformat(),end.isoformat())
    if key not in WINDOW_CACHE:
        lo = next(i for i,d in enumerate(dates) if d >= start)
        hi = max(i for i,d in enumerate(dates) if d <= end)
        WINDOW_CACHE[key]=(lo,hi)
    return WINDOW_CACHE[key]


def run_one(dates,opens,closes,event_idx,assets,start,end,start_asset,collect=False):
    aset=set(assets)
    lo,hi=bounds(dates,start,end)
    current=start_asset
    qty=1.0/opens[current][lo]
    pending=None
    equity=[]
    held=[]
    held_dates=[]
    route=[current]
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
            edges[(old,current)] += 1
            route.append(current)
            pending=None

        equity.append(qty*closes[current][i])
        held.append(current)
        held_dates.append(ts)

        if i==hi:
            continue
        candidates=[
            e for e in event_idx.get(ts,{}).get(current,[])
            if e["to_asset"] in aset
        ]
        if not candidates:
            continue
        if len(candidates)>1:
            conflicts+=1
        pending=candidates[0]["to_asset"]

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
            "holdings":held,
            "dates":held_dates,
            "route":route,
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


def candidate_start_summary(dates,opens,closes,event_idx,assets,candidate,start,end):
    r=run_one(dates,opens,closes,event_idx,assets,start,end,candidate)
    return r


def max_drawup_interval(panel,asset,days):
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
    d90=max_drawup_interval(panel,asset,90)
    d180=max_drawup_interval(panel,asset,180)
    return {
        "asset":asset,
        "max_90d_drawup":d90["drawup"],
        "max_90d_start":d90["start"],
        "max_90d_end":d90["end"],
        "max_180d_drawup":d180["drawup"],
        "max_180d_start":d180["start"],
        "max_180d_end":d180["end"],
        "primary_explosive":bool(d90["drawup"]>=2.0 or d180["drawup"]>=4.0),
    }


def neutralized_candidate_return(run,candidate,start,end):
    value=1.0
    for i in range(1,len(run["equity"])):
        ratio=run["equity"][i]/run["equity"][i-1]
        if (
            run["holdings"][i]==candidate
            and start<=run["dates"][i]<=end
            and ratio>1.0
        ):
            ratio=1.0
        value*=ratio
    return float(value-1.0)


def bull_neutralized_summary(dates,opens,closes,event_idx,assets,candidate,bull):
    bs=utc(bull["max_90d_start"])
    be=utc(bull["max_90d_end"])
    vals=[]
    for starter in BASE:
        r=run_one(dates,opens,closes,event_idx,assets,MATURE_START,END,starter,collect=True)
        vals.append(neutralized_candidate_return(r,candidate,bs,be))
    return {"median_return":float(np.median(vals))}


def holding_streaks(holdings,candidate):
    streaks=[]
    cur=0
    for a in holdings:
        if a==candidate:
            cur+=1
        elif cur:
            streaks.append(cur)
            cur=0
    if cur:
        streaks.append(cur)
    return streaks


def topology(dates,opens,closes,event_idx,assets,candidate,start,end):
    total_days=0
    cand_days=0
    entries=0
    exits=0
    ending=0
    streaks=[]
    incoming=Counter()
    outgoing=Counter()
    for starter in BASE:
        r=run_one(dates,opens,closes,event_idx,assets,start,end,starter,collect=True)
        total_days += len(r["holdings"])
        cand_days += sum(1 for x in r["holdings"] if x==candidate)
        if r["holdings"][-1]==candidate:
            ending += 1
        streaks.extend(holding_streaks(r["holdings"],candidate))
        for (a,b),n in r["edges"].items():
            if b==candidate:
                entries+=n
                incoming[a]+=n
            if a==candidate:
                exits+=n
                outgoing[b]+=n
    return {
        "occupancy_share":cand_days/total_days if total_days else 0.0,
        "entries":entries,
        "exits":exits,
        "routes_ending_in_candidate":ending,
        "routes_ending_rate":ending/len(BASE),
        "median_holding_streak_days":float(np.median(streaks)) if streaks else 0.0,
        "max_holding_streak_days":int(max(streaks)) if streaks else 0,
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
    rows=[]
    for start,end in monthly_windows(dates,months):
        x=summarize(dates,opens,closes,event_idx,assets,start,end)
        rows.append({"start":start.isoformat(),"end":end.isoformat(),**x})
    df=pd.DataFrame(rows)
    return {
        "count":len(df),
        "median_window_return":float(df["median_return"].median()),
        "worst_window_return":float(df["median_return"].min()),
        "positive_window_rate":float((df["median_return"]>0).mean()),
        "median_max_dd":float(df["median_max_dd"].median()),
        "worst_max_dd":float(df["worst_max_dd"].min()),
    }


def endpoint_sensitivity(dates,opens,closes,event_idx,assets):
    out=[]
    for end in ENDPOINTS:
        start=end-pd.Timedelta(days=364)
        x=summarize(dates,opens,closes,event_idx,assets,start,end)
        out.append({"start":start.isoformat(),"end":end.isoformat(),**x})
    return out


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cutoff",default="2026-09-27T00:00:00Z")
    args=ap.parse_args()

    panel,meta=download_panel(utc(args.cutoff))
    event_idx=build_event_index(panel)
    dates,opens,closes=prep_arrays(panel)

    run_dir=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True,exist_ok=True)

    bulls={c:bull_metrics(panel,c) for c in CANDIDATES}
    base_windows={
        "mature":summarize(dates,opens,closes,event_idx,BASE,MATURE_START,END),
        "latest_2y":summarize(dates,opens,closes,event_idx,BASE,TWO_YEAR_START,END),
        "latest_1y":summarize(dates,opens,closes,event_idx,BASE,YEAR_START,END),
        "rolling_12m":rolling_summary(dates,opens,closes,event_idx,BASE,12),
        "rolling_24m":rolling_summary(dates,opens,closes,event_idx,BASE,24),
        "endpoint_sensitivity":endpoint_sensitivity(dates,opens,closes,event_idx,BASE),
    }

    result={
        "generated_at":pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit":source_sha(),
        "data":meta,
        "base_assets":list(BASE),
        "base":base_windows,
        "bull_metrics":bulls,
        "candidates":{},
    }

    rows=[]
    for candidate in CANDIDATES:
        assets=BASE+(candidate,)
        mature=summarize(dates,opens,closes,event_idx,assets,MATURE_START,END)
        y2=summarize(dates,opens,closes,event_idx,assets,TWO_YEAR_START,END)
        y1=summarize(dates,opens,closes,event_idx,assets,YEAR_START,END)
        rolling12=rolling_summary(dates,opens,closes,event_idx,assets,12)
        rolling24=rolling_summary(dates,opens,closes,event_idx,assets,24)
        endpoints=endpoint_sensitivity(dates,opens,closes,event_idx,assets)
        topo=topology(dates,opens,closes,event_idx,assets,candidate,MATURE_START,END)
        topo_1y=topology(dates,opens,closes,event_idx,assets,candidate,YEAR_START,END)
        neutral=bull_neutralized_summary(dates,opens,closes,event_idx,assets,candidate,bulls[candidate])
        candidate_start=candidate_start_summary(dates,opens,closes,event_idx,assets,candidate,MATURE_START,END)

        endpoint_increments=[]
        for base_ep,cand_ep in zip(base_windows["endpoint_sensitivity"],endpoints):
            endpoint_increments.append({
                "end":cand_ep["end"],
                "candidate_increment":cand_ep["median_return"]-base_ep["median_return"],
                "base_return":base_ep["median_return"],
                "candidate_return":cand_ep["median_return"],
            })

        payload={
            "assets":list(assets),
            "mature":mature,
            "latest_2y":y2,
            "latest_1y":y1,
            "rolling_12m":rolling12,
            "rolling_24m":rolling24,
            "topology_mature":topo,
            "topology_latest_1y":topo_1y,
            "candidate_start_mature":candidate_start,
            "own_bull_neutralized_mature":neutral,
            "endpoint_sensitivity":endpoints,
            "endpoint_increments_vs_u9":endpoint_increments,
            "delta_mature_vs_u9":mature["median_return"]-base_windows["mature"]["median_return"],
            "delta_2y_vs_u9":y2["median_return"]-base_windows["latest_2y"]["median_return"],
            "delta_1y_vs_u9":y1["median_return"]-base_windows["latest_1y"]["median_return"],
            "delta_bull_neutralized_vs_u9_mature":neutral["median_return"]-base_windows["mature"]["median_return"],
        }
        result["candidates"][candidate]=payload
        rows.append({
            "candidate":candidate,
            "mature_return":mature["median_return"],
            "delta_mature_vs_u9":payload["delta_mature_vs_u9"],
            "latest_2y_return":y2["median_return"],
            "delta_2y_vs_u9":payload["delta_2y_vs_u9"],
            "latest_1y_return":y1["median_return"],
            "delta_1y_vs_u9":payload["delta_1y_vs_u9"],
            "worst_12m":rolling12["worst_window_return"],
            "worst_24m":rolling24["worst_window_return"],
            "bull_neutralized_mature":neutral["median_return"],
            "occupancy_mature":topo["occupancy_share"],
            "entries_mature":topo["entries"],
            "exits_mature":topo["exits"],
            "end_rate_mature":topo["routes_ending_rate"],
            "positive_endpoint_increments":sum(1 for x in endpoint_increments if x["candidate_increment"]>0),
            "negative_endpoint_increments":sum(1 for x in endpoint_increments if x["candidate_increment"]<0),
        })

    pd.DataFrame(rows).to_csv(run_dir/"candidate_comparison.csv",index=False)
    (run_dir/"results.json").write_text(
        json.dumps(result,indent=2,sort_keys=True,allow_nan=False),
        encoding="utf-8"
    )

    lines=[
        "# ATOM Replacement Candidates v1","",
        "Mode: STRESS_TEST_ONLY",
        "Live unchanged. PR #54 remains unmerged during this research.","",
        "## Base U9 without ATOM","",
        f"Mature: {100*base_windows['mature']['median_return']:+.2f}%",
        f"Latest 2Y: {100*base_windows['latest_2y']['median_return']:+.2f}%",
        f"Latest 1Y: {100*base_windows['latest_1y']['median_return']:+.2f}%",
        "",
        "| Candidate | Mature | Δ vs U9 | 2Y | Δ | 1Y | Δ | Worst 12m | Worst 24m | Endpoint +/5 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            f"| {row['candidate']} | {100*row['mature_return']:+.2f}% | {100*row['delta_mature_vs_u9']:+.2f} pp | "
            f"{100*row['latest_2y_return']:+.2f}% | {100*row['delta_2y_vs_u9']:+.2f} pp | "
            f"{100*row['latest_1y_return']:+.2f}% | {100*row['delta_1y_vs_u9']:+.2f} pp | "
            f"{100*row['worst_12m']:+.2f}% | {100*row['worst_24m']:+.2f}% | "
            f"{row['positive_endpoint_increments']}/5 |"
        )
    (run_dir/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")

    print("run_dir="+str(run_dir))
    print("u9_mature=%.6f" % base_windows["mature"]["median_return"])
    print("u9_2y=%.6f" % base_windows["latest_2y"]["median_return"])
    print("u9_1y=%.6f" % base_windows["latest_1y"]["median_return"])
    print("u9_worst12=%.6f" % base_windows["rolling_12m"]["worst_window_return"])
    print("u9_worst24=%.6f" % base_windows["rolling_24m"]["worst_window_return"])
    for c,p in result["candidates"].items():
        print(c+"_mature=%.6f" % p["mature"]["median_return"])
        print(c+"_delta_mature=%.6f" % p["delta_mature_vs_u9"])
        print(c+"_2y=%.6f" % p["latest_2y"]["median_return"])
        print(c+"_delta_2y=%.6f" % p["delta_2y_vs_u9"])
        print(c+"_1y=%.6f" % p["latest_1y"]["median_return"])
        print(c+"_delta_1y=%.6f" % p["delta_1y_vs_u9"])
        print(c+"_worst12=%.6f" % p["rolling_12m"]["worst_window_return"])
        print(c+"_worst24=%.6f" % p["rolling_24m"]["worst_window_return"])
        print(c+"_bullneutral=%.6f" % p["own_bull_neutralized_mature"]["median_return"])
        print(c+"_occupancy=%.6f" % p["topology_mature"]["occupancy_share"])
        print(c+"_entries="+str(p["topology_mature"]["entries"]))
        print(c+"_exits="+str(p["topology_mature"]["exits"]))
        print(c+"_end_rate=%.6f" % p["topology_mature"]["routes_ending_rate"])
        print(c+"_endpoint_increments="+",".join(f"{100*x['candidate_increment']:+.2f}" for x in p["endpoint_increments_vs_u9"]))
    return 0


if __name__=="__main__":
    raise SystemExit(main())
