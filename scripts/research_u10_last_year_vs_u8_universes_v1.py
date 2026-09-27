from __future__ import annotations

import argparse
import itertools
import json
import os
import random
import subprocess
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import build_pair_monitor

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "u10_last_year_vs_u8_universes_v1"

MANDATORY = ("ATOM","TWT","PEPE")
POOL = (
    "ATOM","TWT","PEPE","BNB","SOL","TRX","AAVE","LINK",
    "AVAX","FIL","ETH","ALGO","ADA","XRP","HBAR",
)
CANONICAL_U8 = ("ATOM","TWT","PEPE","BNB","SOL","TRX","AAVE","LINK")
U10 = CANONICAL_U8 + ("FIL","HBAR")

LOOKBACK=180
ARM=0.15
REVERSAL=0.03
COST=0.001
DATA_START=pd.Timestamp("2023-05-05",tz="UTC")

YEAR_START=pd.Timestamp("2025-09-27",tz="UTC")
YEAR_END=pd.Timestamp("2026-09-26",tz="UTC")
TWO_YEAR_START=pd.Timestamp("2024-09-27",tz="UTC")
TWO_YEAR_END=pd.Timestamp("2026-09-26",tz="UTC")

WINDOW_CACHE={}


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


def build_events(panel):
    cols=["timestamp"]+[a+"_close" for a in POOL]
    events,_=build_pair_monitor(
        panel[cols].copy(),
        assets=POOL,
        lookback=LOOKBACK,
        arm_threshold=ARM,
        reversal=REVERSAL,
    )
    by_date=defaultdict(list)
    for e in events:
        if e["event"]=="CONFIRMED":
            by_date[utc(e["date"])].append(e)
    return by_date


def cached_rows(panel,start,end):
    cache_key=(start.isoformat(),end.isoformat())
    if cache_key not in WINDOW_CACHE:
        w=panel[(panel["timestamp"]>=start)&(panel["timestamp"]<=end)]
        if w.empty:
            raise ValueError("empty window")
        WINDOW_CACHE[cache_key]=list(w.itertuples(index=False,name="MarketRow"))
    return WINDOW_CACHE[cache_key]


def run_one(panel,events,assets,start,end,start_asset,collect=False):
    aset=set(assets)
    rows=cached_rows(panel,start,end)
    current=start_asset
    qty=1.0/float(getattr(rows[0],current+"_open"))
    pending=None
    equity=[]
    transitions=0
    conflicts=0
    holdings=Counter()
    route=[current]

    last_pos=len(rows)-1
    for pos,row in enumerate(rows):
        ts=utc(row.timestamp)

        if pending is not None:
            value=qty*float(getattr(row,current+"_open"))
            current=pending["to_asset"]
            qty=value*(1.0-COST)/float(getattr(row,current+"_open"))
            transitions+=1
            route.append(current)
            pending=None

        holdings[current]+=1
        equity.append(qty*float(getattr(row,current+"_close")))

        if pos==last_pos:
            continue

        candidates=[
            e for e in events.get(ts,[])
            if e["from_asset"]==current and e["to_asset"] in aset
        ]
        if not candidates:
            continue
        if len(candidates)>1:
            conflicts+=1
        pending=dict(sorted(
            candidates,
            key=lambda e:(-float(e["max_dislocation"]),e["to_asset"],e["pair"])
        )[0])

    peak=equity[0]
    worst_dd=0.0
    for value in equity:
        if value>peak:
            peak=value
        dd=value/peak-1.0
        if dd<worst_dd:
            worst_dd=dd

    out={
        "start_asset":start_asset,
        "return":float(equity[-1]/equity[0]-1.0),
        "max_dd":float(worst_dd),
        "transitions":transitions,
        "conflicts":conflicts,
    }
    if collect:
        out["holdings"]=dict(holdings)
        out["route"]=route
    return out


def summarize(panel,events,assets,start,end,collect_atom=False,starters=None):
    runs=[]
    atom_detail=None
    starters = assets if starters is None else starters
    for a in starters:
        collect=(collect_atom and a=="ATOM")
        r=run_one(panel,events,assets,start,end,a,collect=collect)
        runs.append(r)
        if collect:
            atom_detail=r
    df=pd.DataFrame(runs)
    atom=df[df["start_asset"]=="ATOM"].iloc[0]
    out={
        "median_return":float(df["return"].median()),
        "worst_return":float(df["return"].min()),
        "best_return":float(df["return"].max()),
        "positive_starts":int((df["return"]>0).sum()),
        "median_max_dd":float(df["max_dd"].median()),
        "worst_max_dd":float(df["max_dd"].min()),
        "median_transitions":float(df["transitions"].median()),
        "median_conflicts":float(df["conflicts"].median()),
        "atom_return":float(atom["return"]),
        "atom_max_dd":float(atom["max_dd"]),
        "atom_transitions":int(atom["transitions"]),
        "atom_conflicts":int(atom["conflicts"]),
    }
    if atom_detail is not None:
        out["atom_route"]=atom_detail["route"]
        out["atom_holdings"]=atom_detail["holdings"]
    return out


def universe_key(assets):
    return "|".join(sorted(assets))


def all_u8():
    rest=[a for a in POOL if a not in MANDATORY]
    return [tuple(MANDATORY+extra) for extra in itertools.combinations(rest,5)]


def percentile_ge(series,value):
    # Percentile as fraction of universe values <= value.
    return float((series<=value).mean())


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cutoff",default="2026-09-27T00:00:00Z")
    args=ap.parse_args()

    panel,meta=download_panel(utc(args.cutoff))
    events=build_events(panel)

    run_dir=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True,exist_ok=True)

    u8s=all_u8()
    rows=[]
    for assets in u8s:
        s=summarize(panel,events,assets,YEAR_START,YEAR_END)
        rows.append({
            "key":universe_key(assets),
            "assets":"|".join(assets),
            **s,
        })

    df=pd.DataFrame(rows)
    df["median_rank"]=df["median_return"].rank(method="min",ascending=False).astype(int)
    df["atom_rank"]=df["atom_return"].rank(method="min",ascending=False).astype(int)
    df["dd_rank"]=df["median_max_dd"].rank(method="min",ascending=False).astype(int)
    df=df.sort_values(["median_rank","key"],kind="stable").reset_index(drop=True)
    df.to_csv(run_dir/"all_792_u8_last_year.csv",index=False)

    canonical_key=universe_key(CANONICAL_U8)
    canonical_row=df[df["key"]==canonical_key].iloc[0].to_dict()

    u8_detail=summarize(panel,events,CANONICAL_U8,YEAR_START,YEAR_END,collect_atom=True)
    u10_detail=summarize(panel,events,U10,YEAR_START,YEAR_END,collect_atom=True,starters=CANONICAL_U8)

    u8_2y=summarize(panel,events,CANONICAL_U8,TWO_YEAR_START,TWO_YEAR_END,collect_atom=True)
    u10_2y=summarize(panel,events,U10,TWO_YEAR_START,TWO_YEAR_END,collect_atom=True,starters=CANONICAL_U8)

    rng=random.Random(20260927)
    idxs=rng.sample(range(len(df)),12)
    random_rows=df.iloc[idxs].copy()
    random_rows.to_csv(run_dir/"random_12_examples.csv",index=False)

    u10_occ=u10_detail["atom_holdings"]
    atom_days=sum(u10_occ.values())

    result={
        "generated_at":pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit":source_sha(),
        "data":meta,
        "year":{"start":YEAR_START.isoformat(),"end":YEAR_END.isoformat()},
        "two_year":{"start":TWO_YEAR_START.isoformat(),"end":TWO_YEAR_END.isoformat()},
        "u8_count":len(df),
        "canonical_u8":{
            **u8_detail,
            "median_rank_792":int(canonical_row["median_rank"]),
            "atom_rank_792":int(canonical_row["atom_rank"]),
            "median_percentile":percentile_ge(df["median_return"],u8_detail["median_return"]),
            "atom_percentile":percentile_ge(df["atom_return"],u8_detail["atom_return"]),
        },
        "u10":{
            **u10_detail,
            "median_percentile_equivalent_vs_792_u8":percentile_ge(df["median_return"],u10_detail["median_return"]),
            "atom_percentile_equivalent_vs_792_u8":percentile_ge(df["atom_return"],u10_detail["atom_return"]),
            "fil_atom_route_occupancy_share":u10_occ.get("FIL",0)/atom_days if atom_days else 0.0,
            "hbar_atom_route_occupancy_share":u10_occ.get("HBAR",0)/atom_days if atom_days else 0.0,
        },
        "two_year_context":{
            "canonical_u8":u8_2y,
            "u10":u10_2y,
        },
        "distribution":{
            "median_of_u8_medians":float(df["median_return"].median()),
            "q25":float(df["median_return"].quantile(0.25)),
            "q75":float(df["median_return"].quantile(0.75)),
            "q90":float(df["median_return"].quantile(0.90)),
            "q95":float(df["median_return"].quantile(0.95)),
            "best_u8":df.iloc[0].to_dict(),
            "worst_u8":df.iloc[-1].to_dict(),
            "positive_u8_rate":float((df["median_return"]>0).mean()),
        },
        "random_12":random_rows.to_dict(orient="records"),
    }

    (run_dir/"results.json").write_text(
        json.dumps(result,indent=2,sort_keys=True,allow_nan=False),
        encoding="utf-8"
    )

    lines=[
        "# U10 Last-Year vs U8 Universe Distribution v1","",
        "Mode: STRESS_TEST_ONLY",
        "Live U8 unchanged.","",
        f"Window: {YEAR_START.date()} -> {YEAR_END.date()}","",
        "| Metric | Canonical U8 | U10 |",
        "|---|---:|---:|",
        f"| Median return | {100*u8_detail['median_return']:+.2f}% | {100*u10_detail['median_return']:+.2f}% |",
        f"| ATOM-start | {100*u8_detail['atom_return']:+.2f}% | {100*u10_detail['atom_return']:+.2f}% |",
        f"| Median max DD | {100*u8_detail['median_max_dd']:+.2f}% | {100*u10_detail['median_max_dd']:+.2f}% |",
        f"| Median transitions | {u8_detail['median_transitions']:.1f} | {u10_detail['median_transitions']:.1f} |",
        "",
        f"Canonical U8 median rank among 792 mandatory ATOM/TWT/PEPE U8s: {int(canonical_row['median_rank'])}/792",
        f"U10 median percentile-equivalent vs 792 U8s: {100*result['u10']['median_percentile_equivalent_vs_792_u8']:.2f}%",
        f"Median of all 792 U8 medians: {100*result['distribution']['median_of_u8_medians']:+.2f}%",
        "",
        "## Two-year context","",
        f"U8 median: {100*u8_2y['median_return']:+.2f}%",
        f"U10 median: {100*u10_2y['median_return']:+.2f}%",
    ]
    (run_dir/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")

    print("run_dir="+str(run_dir))
    print("u8_median=%.6f" % u8_detail["median_return"])
    print("u8_atom=%.6f" % u8_detail["atom_return"])
    print("u8_dd=%.6f" % u8_detail["median_max_dd"])
    print("u8_rank_792="+str(int(canonical_row["median_rank"])))
    print("u10_median=%.6f" % u10_detail["median_return"])
    print("u10_atom=%.6f" % u10_detail["atom_return"])
    print("u10_dd=%.6f" % u10_detail["median_max_dd"])
    print("u10_percentile_equiv=%.6f" % result["u10"]["median_percentile_equivalent_vs_792_u8"])
    print("u8_2y=%.6f" % u8_2y["median_return"])
    print("u10_2y=%.6f" % u10_2y["median_return"])
    print("dist_median=%.6f" % result["distribution"]["median_of_u8_medians"])
    print("dist_q95=%.6f" % result["distribution"]["q95"])
    return 0


if __name__=="__main__":
    raise SystemExit(main())
