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
OUT = ROOT / "research_artifacts" / "algo_deep_audit_v1"

POOL = (
    "ATOM","TWT","PEPE","BNB","SOL","TRX","AAVE","LINK",
    "AVAX","FIL","ETH","ALGO","ADA","XRP","HBAR",
)
MANDATORY = ("ATOM","TWT","PEPE")
U9_CLEANER = ("ATOM","TWT","PEPE","BNB","TRX","AAVE","LINK","FIL","HBAR")

LOOKBACK = 180
ARM = 0.15
REVERSAL = 0.03
COST = 0.001

DATA_START = pd.Timestamp("2023-05-05", tz="UTC")
FULL_ALGO_START = pd.Timestamp("2019-01-01", tz="UTC")
MATURE_START = pd.Timestamp("2023-10-31", tz="UTC")
ALGO_BULL_START = pd.Timestamp("2024-11-04", tz="UTC")
ALGO_BULL_END = pd.Timestamp("2024-12-07", tz="UTC")
POST_BULL_START = pd.Timestamp("2024-12-08", tz="UTC")
TWO_YEAR_START = pd.Timestamp("2024-09-27", tz="UTC")
YEAR_START = pd.Timestamp("2025-09-27", tz="UTC")
END = pd.Timestamp("2026-09-26", tz="UTC")

WINDOW_CACHE = {}


def utc(x):
    t = pd.Timestamp(x)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def source_sha():
    v = os.environ.get("SOURCE_COMMIT_SHA")
    if v:
        return v
    return subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip()


def download_dataset(client, asset, start, end):
    result = download_historical_dataset(
        client,
        symbol=asset+"USDT",
        start=start,
        end=end,
        timeframe="1D",
        as_of=end,
    )
    if result.dataset is None:
        raise RuntimeError(f"{asset}: {result.metadata.status}")
    if result.dataset.quality.has_critical_issues:
        raise RuntimeError(f"{asset}: critical data quality issues")
    return result.dataset.candles[["timestamp","open","close"]].copy()


def download_panel(cutoff):
    client = BinanceSpotRestClient()
    panel = None
    meta = {}
    for a in POOL:
        f = download_dataset(client,a,DATA_START,cutoff)
        meta[a] = {
            "rows": len(f),
            "start": pd.Timestamp(f.iloc[0]["timestamp"]).isoformat(),
            "end": pd.Timestamp(f.iloc[-1]["timestamp"]).isoformat(),
        }
        f = f.rename(columns={"open":a+"_open","close":a+"_close"})
        panel = f if panel is None else panel.merge(f,on="timestamp",how="inner",validate="one_to_one")
    panel = panel.sort_values("timestamp").reset_index(drop=True)
    algo_full = download_dataset(client,"ALGO",FULL_ALGO_START,cutoff).sort_values("timestamp").reset_index(drop=True)
    return panel,meta,algo_full


def build_event_index(panel):
    cols = ["timestamp"] + [a+"_close" for a in POOL]
    events,_ = build_pair_monitor(
        panel[cols].copy(),
        assets=POOL,
        lookback=LOOKBACK,
        arm_threshold=ARM,
        reversal=REVERSAL,
    )
    idx = defaultdict(lambda:defaultdict(list))
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
    opens = {a:panel[a+"_open"].astype(float).to_numpy() for a in POOL}
    closes = {a:panel[a+"_close"].astype(float).to_numpy() for a in POOL}
    date_to_i = {d:i for i,d in enumerate(dates)}
    return dates,opens,closes,date_to_i


def window_bounds(dates,start,end):
    key=(start.isoformat(),end.isoformat())
    if key in WINDOW_CACHE:
        return WINDOW_CACHE[key]
    lo = next(i for i,d in enumerate(dates) if d>=start)
    hi = max(i for i,d in enumerate(dates) if d<=end)
    WINDOW_CACHE[key]=(lo,hi)
    return lo,hi


def run_one(dates,opens,closes,event_idx,assets,start,end,start_asset,collect=False):
    aset=set(assets)
    lo,hi=window_bounds(dates,start,end)
    current=start_asset
    qty=1.0/opens[current][lo]
    pending=None
    equity=[]
    holdings=[]
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
            edges[(old,current)]+=1
            route.append(current)
            pending=None

        equity.append(qty*closes[current][i])
        holdings.append(current)
        held_dates.append(ts)

        if i==hi:
            continue
        cands=event_idx.get(ts,{}).get(current,[])
        if not cands:
            continue
        eligible=[e for e in cands if e["to_asset"] in aset]
        if not eligible:
            continue
        if len(eligible)>1:
            conflicts+=1
        pending=eligible[0]["to_asset"]

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
            "route":route,
            "edges":edges,
        })
    return out


def summarize(dates,opens,closes,event_idx,assets,start,end,starters):
    runs=[run_one(dates,opens,closes,event_idx,assets,start,end,a) for a in starters]
    df=pd.DataFrame(runs)
    atom=df[df["start_asset"]=="ATOM"]
    return {
        "median_return":float(df["return"].median()),
        "worst_return":float(df["return"].min()),
        "best_return":float(df["return"].max()),
        "positive_starts":int((df["return"]>0).sum()),
        "median_max_dd":float(df["max_dd"].median()),
        "worst_max_dd":float(df["max_dd"].min()),
        "median_transitions":float(df["transitions"].median()),
        "median_conflicts":float(df["conflicts"].median()),
        "atom_return":None if atom.empty else float(atom.iloc[0]["return"]),
        "atom_max_dd":None if atom.empty else float(atom.iloc[0]["max_dd"]),
    }


def hodl_return(dates,opens,closes,asset,start,end):
    lo,hi=window_bounds(dates,start,end)
    return float(closes[asset][hi]/opens[asset][lo]-1.0)


def max_drawup_interval(frame,days):
    dates=[utc(x) for x in frame["timestamp"]]
    prices=frame["close"].astype(float).to_numpy()
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


def algo_price_path(panel,algo_full,dates,opens,closes):
    common=panel[["timestamp","ALGO_close"]].rename(columns={"ALGO_close":"close"})
    common90=max_drawup_interval(common,90)
    common180=max_drawup_interval(common,180)
    full90=max_drawup_interval(algo_full[["timestamp","close"]],90)
    full180=max_drawup_interval(algo_full[["timestamp","close"]],180)
    return {
        "strategy_common_90d":common90,
        "strategy_common_180d":common180,
        "full_binance_90d_context_only":full90,
        "full_binance_180d_context_only":full180,
        "hodl":{
            "mature":hodl_return(dates,opens,closes,"ALGO",MATURE_START,END),
            "post_bull":hodl_return(dates,opens,closes,"ALGO",POST_BULL_START,END),
            "latest_2y":hodl_return(dates,opens,closes,"ALGO",TWO_YEAR_START,END),
            "latest_1y":hodl_return(dates,opens,closes,"ALGO",YEAR_START,END),
            "common_history":hodl_return(dates,opens,closes,"ALGO",utc(dates[0]),END),
        },
    }


def neutralized_algo_return(run):
    value=1.0
    for i in range(1,len(run["equity"])):
        ratio=run["equity"][i]/run["equity"][i-1]
        if (
            run["holdings"][i]=="ALGO"
            and ALGO_BULL_START<=run["dates"][i]<=ALGO_BULL_END
            and ratio>1.0
        ):
            ratio=1.0
        value*=ratio
    return float(value-1.0)


def neutralized_summary(dates,opens,closes,event_idx,assets,starters):
    vals=[]
    atom=None
    for a in starters:
        r=run_one(dates,opens,closes,event_idx,assets,MATURE_START,END,a,collect=True)
        ret=neutralized_algo_return(r)
        vals.append(ret)
        if a=="ATOM":
            atom=ret
    return {
        "median_return":float(np.median(vals)),
        "atom_return":atom,
    }


def evaluate_universe_year(dates,opens,closes,event_idx,assets):
    runs=[]
    for a in assets:
        r=run_one(dates,opens,closes,event_idx,assets,YEAR_START,END,a)
        runs.append(r)
    rets=np.asarray([r["return"] for r in runs],dtype=float)
    dds=np.asarray([r["max_dd"] for r in runs],dtype=float)
    return {
        "key":"|".join(sorted(assets)),
        "assets":"|".join(assets),
        "median_return":float(np.median(rets)),
        "worst_return":float(np.min(rets)),
        "best_return":float(np.max(rets)),
        "median_max_dd":float(np.median(dds)),
        "positive_starts":int(np.sum(rets>0)),
    }


def exhaustive_year(dates,opens,closes,event_idx,size):
    rows=[]
    lookup={}
    for assets in itertools.combinations(POOL,size):
        r=evaluate_universe_year(dates,opens,closes,event_idx,assets)
        rows.append(r)
        lookup[frozenset(assets)]=r["median_return"]
    df=pd.DataFrame(rows).sort_values(["median_return","key"],ascending=[False,True],kind="stable").reset_index(drop=True)
    df["rank"]=np.arange(1,len(df)+1)
    return df,lookup


def matched_marginal(lookup,size):
    algo_sets=[s for s in lookup if "ALGO" in s]
    rows=[]
    pair_wins=pair_losses=pair_ties=0
    best=top3=med_or_better=worst=0
    for s in algo_sets:
        base=set(s)-{"ALGO"}
        algo_ret=lookup[s]
        alternatives=[]
        for x in POOL:
            if x=="ALGO" or x in base:
                continue
            alt=frozenset(base|{x})
            alternatives.append((x,lookup[alt]))
        alt_vals=[v for _,v in alternatives]
        for _,v in alternatives:
            if algo_ret>v:
                pair_wins+=1
            elif algo_ret<v:
                pair_losses+=1
            else:
                pair_ties+=1
        all_vals=[algo_ret]+alt_vals
        rank=1+sum(v>algo_ret for v in alt_vals)
        max_rank=1+len(alt_vals)
        if rank==1:
            best+=1
        if rank<=3:
            top3+=1
        if rank<=math.ceil(max_rank/2):
            med_or_better+=1
        if all(algo_ret<=v for v in alt_vals):
            worst+=1
        rows.append({
            "base":"|".join(sorted(base)),
            "algo_return":algo_ret,
            "median_alternative":float(np.median(alt_vals)),
            "delta_vs_median_alt":float(algo_ret-np.median(alt_vals)),
            "extension_rank":rank,
            "extension_count":max_rank,
        })
    rdf=pd.DataFrame(rows)
    total_pairs=pair_wins+pair_losses+pair_ties
    return rdf,{
        "contexts":len(rows),
        "pairwise_win_rate":pair_wins/total_pairs if total_pairs else 0.0,
        "pairwise_loss_rate":pair_losses/total_pairs if total_pairs else 0.0,
        "pairwise_tie_rate":pair_ties/total_pairs if total_pairs else 0.0,
        "median_delta_vs_median_alternative":float(rdf["delta_vs_median_alt"].median()),
        "mean_delta_vs_median_alternative":float(rdf["delta_vs_median_alt"].mean()),
        "best_context_rate":best/len(rows),
        "top3_context_rate":top3/len(rows),
        "median_or_better_context_rate":med_or_better/len(rows),
        "worst_context_rate":worst/len(rows),
    }


def algo_streaks(holdings):
    streaks=[]
    cur=0
    for a in holdings:
        if a=="ALGO":
            cur+=1
        elif cur:
            streaks.append(cur)
            cur=0
    if cur:
        streaks.append(cur)
    return streaks


def topology_for_sets(dates,opens,closes,event_idx,df_top):
    total_days=0
    algo_days=0
    entries=0
    exits=0
    ends=0
    routes=0
    streaks=[]
    incoming=Counter()
    outgoing=Counter()
    for assets_txt in df_top["assets"]:
        assets=tuple(assets_txt.split("|"))
        for starter in assets:
            r=run_one(dates,opens,closes,event_idx,assets,YEAR_START,END,starter,collect=True)
            routes+=1
            total_days+=len(r["holdings"])
            algo_days+=sum(1 for x in r["holdings"] if x=="ALGO")
            if r["holdings"][-1]=="ALGO":
                ends+=1
            streaks.extend(algo_streaks(r["holdings"]))
            for (a,b),n in r["edges"].items():
                if b=="ALGO":
                    entries+=n
                    incoming[a]+=n
                if a=="ALGO":
                    exits+=n
                    outgoing[b]+=n
    return {
        "routes":routes,
        "occupancy_share":algo_days/total_days if total_days else 0.0,
        "entries":entries,
        "exits":exits,
        "routes_ending_in_algo":ends,
        "routes_ending_in_algo_rate":ends/routes if routes else 0.0,
        "holding_streak_count":len(streaks),
        "median_holding_streak_days":float(np.median(streaks)) if streaks else 0.0,
        "mean_holding_streak_days":float(np.mean(streaks)) if streaks else 0.0,
        "max_holding_streak_days":int(max(streaks)) if streaks else 0,
        "incoming_edges":dict(incoming.most_common()),
        "outgoing_edges":dict(outgoing.most_common()),
    }


def cleaner_variants():
    variants={
        "U9_CLEANER":U9_CLEANER,
        "U10_CLEANER_PLUS_ALGO":U9_CLEANER+("ALGO",),
    }
    for removed in ("BNB","TRX","AAVE","LINK","FIL","HBAR"):
        variants[f"REPLACE_{removed}_WITH_ALGO"]=tuple(
            "ALGO" if x==removed else x for x in U9_CLEANER
        )
    return variants


def evaluate_variant(dates,opens,closes,event_idx,assets):
    windows={
        "mature":(MATURE_START,END),
        "post_bull":(POST_BULL_START,END),
        "latest_2y":(TWO_YEAR_START,END),
        "latest_1y":(YEAR_START,END),
    }
    out={"assets":list(assets),"mandatory_starts":{},"all_member_starts":{}}
    for label,(start,end) in windows.items():
        out["mandatory_starts"][label]=summarize(
            dates,opens,closes,event_idx,assets,start,end,MANDATORY
        )
        out["all_member_starts"][label]=summarize(
            dates,opens,closes,event_idx,assets,start,end,assets
        )
    if "ALGO" in assets:
        out["algo_bull_neutralized_mature_mandatory"]=neutralized_summary(
            dates,opens,closes,event_idx,assets,MANDATORY
        )
    else:
        out["algo_bull_neutralized_mature_mandatory"]=None
    return out


def monthly_windows(dates,start,months):
    cursor=pd.Timestamp(start.year,start.month,1,tz="UTC")
    if cursor<start:
        cursor+=pd.offsets.MonthBegin(1)
    out=[]
    while True:
        candidate=[d for d in dates if d>=cursor]
        if not candidate:
            break
        s=candidate[0]
        target=s+pd.DateOffset(months=months)-pd.Timedelta("1D")
        if target>END:
            break
        e=max(d for d in dates if d<=target)
        out.append((s,e))
        cursor+=pd.offsets.MonthBegin(1)
    return out


def rolling_post_bull(dates,opens,closes,event_idx,assets,months):
    rows=[]
    for s,e in monthly_windows(dates,POST_BULL_START,months):
        x=summarize(dates,opens,closes,event_idx,assets,s,e,MANDATORY)
        rows.append({"start":s.isoformat(),"end":e.isoformat(),**x})
    if not rows:
        return {"count":0}
    df=pd.DataFrame(rows)
    return {
        "count":len(df),
        "median_window_return":float(df["median_return"].median()),
        "worst_window_return":float(df["median_return"].min()),
        "positive_window_rate":float((df["median_return"]>0).mean()),
        "worst_atom_return":float(df["atom_return"].min()),
    }


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cutoff",default="2026-09-27T00:00:00Z")
    args=ap.parse_args()

    panel,meta,algo_full=download_panel(utc(args.cutoff))
    event_idx=build_event_index(panel)
    dates,opens,closes,date_to_i=prep_arrays(panel)

    run_dir=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True,exist_ok=True)

    price_path=algo_price_path(panel,algo_full,dates,opens,closes)

    df8,lookup8=exhaustive_year(dates,opens,closes,event_idx,8)
    df10,lookup10=exhaustive_year(dates,opens,closes,event_idx,10)
    df8.to_csv(run_dir/"all_u8_last_year.csv",index=False)
    df10.to_csv(run_dir/"all_u10_last_year.csv",index=False)

    m8_df,m8=matched_marginal(lookup8,8)
    m10_df,m10=matched_marginal(lookup10,10)
    m8_df.to_csv(run_dir/"algo_matched_marginal_u8.csv",index=False)
    m10_df.to_csv(run_dir/"algo_matched_marginal_u10.csv",index=False)

    top8=df8.head(10).copy()
    top10=df10.head(10).copy()
    top8.to_csv(run_dir/"top10_u8.csv",index=False)
    top10.to_csv(run_dir/"top10_u10.csv",index=False)

    topology8=topology_for_sets(dates,opens,closes,event_idx,top8)
    topology10=topology_for_sets(dates,opens,closes,event_idx,top10)

    variants={}
    for name,assets in cleaner_variants().items():
        variants[name]=evaluate_variant(dates,opens,closes,event_idx,assets)

    rolling={}
    for name in ("U9_CLEANER","U10_CLEANER_PLUS_ALGO"):
        assets=tuple(variants[name]["assets"])
        rolling[name]={
            "rolling_12m_post_bull":rolling_post_bull(dates,opens,closes,event_idx,assets,12),
            "rolling_18m_post_bull":rolling_post_bull(dates,opens,closes,event_idx,assets,18),
        }

    # Direct ALGO-bull neutralization on top-1 U8/U10 from latest-year ranking.
    top1_neutral={}
    for label,row in (("TOP1_U8",top8.iloc[0]),("TOP1_U10",top10.iloc[0])):
        assets=tuple(row["assets"].split("|"))
        raw=summarize(dates,opens,closes,event_idx,assets,MATURE_START,END,assets)
        neutral=neutralized_summary(dates,opens,closes,event_idx,assets,assets)
        top1_neutral[label]={
            "assets":list(assets),
            "raw_mature":raw,
            "algo_bull_neutralized_mature":neutral,
        }

    result={
        "generated_at":pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit":source_sha(),
        "data":meta,
        "algo_price_path":price_path,
        "matched_marginal":{"U8":m8,"U10":m10},
        "top10_topology":{"U8":topology8,"U10":topology10},
        "top1_algo_bull_neutralization":top1_neutral,
        "cleaner_variants":variants,
        "post_bull_rolling":rolling,
        "top10_u8":json.loads(top8.to_json(orient="records")),
        "top10_u10":json.loads(top10.to_json(orient="records")),
    }
    (run_dir/"results.json").write_text(
        json.dumps(result,indent=2,sort_keys=True,allow_nan=False),
        encoding="utf-8"
    )

    lines=[
        "# ALGO Deep Audit v1","",
        "Mode: STRESS_TEST_ONLY",
        "Live unchanged.","",
        "## Price path","",
        f"Strategy-common strongest 90d drawup: {100*price_path['strategy_common_90d']['drawup']:+.2f}% "
        f"({price_path['strategy_common_90d']['start'][:10]} -> {price_path['strategy_common_90d']['end'][:10]})",
        f"ALGO HODL post-bull: {100*price_path['hodl']['post_bull']:+.2f}%",
        f"ALGO HODL latest 1Y: {100*price_path['hodl']['latest_1y']:+.2f}%",
        "",
        "## Matched marginal","",
        f"U8 ALGO pairwise win rate: {100*m8['pairwise_win_rate']:.2f}%",
        f"U8 ALGO best-context rate: {100*m8['best_context_rate']:.2f}%",
        f"U8 median delta vs median alternative: {100*m8['median_delta_vs_median_alternative']:+.2f} pp",
        f"U10 ALGO pairwise win rate: {100*m10['pairwise_win_rate']:.2f}%",
        f"U10 ALGO best-context rate: {100*m10['best_context_rate']:.2f}%",
        f"U10 median delta vs median alternative: {100*m10['median_delta_vs_median_alternative']:+.2f} pp",
        "",
        "## Cleaner baseline integration","",
        "| Variant | Post-bull median (mandatory starts) | Latest 1Y | Latest 2Y |",
        "|---|---:|---:|---:|",
    ]
    for name,v in variants.items():
        m=v["mandatory_starts"]
        lines.append(
            f"| {name} | {100*m['post_bull']['median_return']:+.2f}% | "
            f"{100*m['latest_1y']['median_return']:+.2f}% | "
            f"{100*m['latest_2y']['median_return']:+.2f}% |"
        )
    (run_dir/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")

    print("run_dir="+str(run_dir))
    print("algo_common_bull90=%.6f" % price_path["strategy_common_90d"]["drawup"])
    print("algo_common_bull90_interval="+price_path["strategy_common_90d"]["start"]+".."+price_path["strategy_common_90d"]["end"])
    print("algo_full_bull90=%.6f" % price_path["full_binance_90d_context_only"]["drawup"])
    print("algo_full_bull90_interval="+price_path["full_binance_90d_context_only"]["start"]+".."+price_path["full_binance_90d_context_only"]["end"])
    print("algo_hodl_post_bull=%.6f" % price_path["hodl"]["post_bull"])
    print("algo_hodl_1y=%.6f" % price_path["hodl"]["latest_1y"])
    print("u8_pairwise_win=%.6f" % m8["pairwise_win_rate"])
    print("u8_best_context=%.6f" % m8["best_context_rate"])
    print("u8_median_delta=%.6f" % m8["median_delta_vs_median_alternative"])
    print("u10_pairwise_win=%.6f" % m10["pairwise_win_rate"])
    print("u10_best_context=%.6f" % m10["best_context_rate"])
    print("u10_median_delta=%.6f" % m10["median_delta_vs_median_alternative"])
    print("top10_u8_algo_occupancy=%.6f" % topology8["occupancy_share"])
    print("top10_u8_algo_entries="+str(topology8["entries"]))
    print("top10_u8_algo_exits="+str(topology8["exits"]))
    print("top10_u8_algo_end_rate=%.6f" % topology8["routes_ending_in_algo_rate"])
    print("top10_u10_algo_occupancy=%.6f" % topology10["occupancy_share"])
    print("top10_u10_algo_entries="+str(topology10["entries"]))
    print("top10_u10_algo_exits="+str(topology10["exits"]))
    print("top10_u10_algo_end_rate=%.6f" % topology10["routes_ending_in_algo_rate"])
    for name,v in variants.items():
        print(name+"_postbull=%.6f" % v["mandatory_starts"]["post_bull"]["median_return"])
        print(name+"_1y=%.6f" % v["mandatory_starts"]["latest_1y"]["median_return"])
        print(name+"_2y=%.6f" % v["mandatory_starts"]["latest_2y"]["median_return"])
        print(name+"_atom1y=%.6f" % v["mandatory_starts"]["latest_1y"]["atom_return"])
        if v["algo_bull_neutralized_mature_mandatory"] is not None:
            print(name+"_algo_neutral=%.6f" % v["algo_bull_neutralized_mature_mandatory"]["median_return"])
    for name,x in top1_neutral.items():
        print(name+"_raw_mature=%.6f" % x["raw_mature"]["median_return"])
        print(name+"_algo_neutral_mature=%.6f" % x["algo_bull_neutralized_mature"]["median_return"])
    return 0


if __name__=="__main__":
    raise SystemExit(main())
