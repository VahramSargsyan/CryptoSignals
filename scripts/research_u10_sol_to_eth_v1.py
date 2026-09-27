from __future__ import annotations

import argparse
import json
import os
import subprocess
from collections import defaultdict
from pathlib import Path

import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import build_pair_monitor

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/"research_artifacts"/"u10_sol_to_eth_v1"

POOL=("ATOM","TWT","PEPE","BNB","SOL","TRX","AAVE","LINK","FIL","HBAR","ETH")
COMMON_STARTERS=("ATOM","TWT","PEPE","BNB","TRX","AAVE","LINK")
UNIVERSES={
    "BASE_U10":("ATOM","TWT","PEPE","BNB","SOL","TRX","AAVE","LINK","FIL","HBAR"),
    "DROP_SOL_U9":("ATOM","TWT","PEPE","BNB","TRX","AAVE","LINK","FIL","HBAR"),
    "SOL_TO_ETH_U10":("ATOM","TWT","PEPE","BNB","ETH","TRX","AAVE","LINK","FIL","HBAR"),
    "DROP_HBAR_U9":("ATOM","TWT","PEPE","BNB","SOL","TRX","AAVE","LINK","FIL"),
}
LOOKBACK=180
ARM=0.15
REVERSAL=0.03
COST=0.001
DATA_START=pd.Timestamp("2023-05-05",tz="UTC")
MATURE_START=pd.Timestamp("2023-10-31",tz="UTC")
END=pd.Timestamp("2026-09-26",tz="UTC")
YEAR_START=pd.Timestamp("2025-09-27",tz="UTC")
TWO_YEAR_START=pd.Timestamp("2024-09-27",tz="UTC")
PEPE_BULL_START=pd.Timestamp("2024-02-23",tz="UTC")
PEPE_BULL_END=pd.Timestamp("2024-05-23",tz="UTC")
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
            client,symbol=a+"USDT",start=DATA_START,end=cutoff,
            timeframe="1D",as_of=cutoff,
        )
        if result.dataset is None:
            raise RuntimeError(f"{a}: {result.metadata.status}")
        if result.dataset.quality.has_critical_issues:
            raise RuntimeError(f"{a}: critical data")
        f=result.dataset.candles[["timestamp","open","close"]].copy()
        f=f.rename(columns={"open":a+"_open","close":a+"_close"})
        meta[a]={"rows":len(f),"start":pd.Timestamp(f.iloc[0].timestamp).isoformat(),"end":pd.Timestamp(f.iloc[-1].timestamp).isoformat()}
        panel=f if panel is None else panel.merge(f,on="timestamp",how="inner",validate="one_to_one")
    return panel.sort_values("timestamp").reset_index(drop=True),meta


def build_events(panel):
    cols=["timestamp"]+[a+"_close" for a in POOL]
    events,_=build_pair_monitor(
        panel[cols].copy(),assets=POOL,lookback=LOOKBACK,
        arm_threshold=ARM,reversal=REVERSAL,
    )
    by_date=defaultdict(list)
    for e in events:
        if e["event"]=="CONFIRMED":
            by_date[utc(e["date"])].append(e)
    return by_date


def rows_for(panel,start,end):
    key=(start.isoformat(),end.isoformat())
    if key not in WINDOW_CACHE:
        w=panel[(panel.timestamp>=start)&(panel.timestamp<=end)]
        WINDOW_CACHE[key]=list(w.itertuples(index=False,name="MarketRow"))
    return WINDOW_CACHE[key]


def run_one(panel,events,assets,start,end,start_asset,collect=False):
    aset=set(assets)
    rows=rows_for(panel,start,end)
    current=start_asset
    qty=1.0/float(getattr(rows[0],current+"_open"))
    pending=None
    equity=[]
    dates=[]
    holdings=[]
    transitions=0
    conflicts=0

    for pos,row in enumerate(rows):
        ts=utc(row.timestamp)
        if pending is not None:
            value=qty*float(getattr(row,current+"_open"))
            current=pending["to_asset"]
            qty=value*(1.0-COST)/float(getattr(row,current+"_open"))
            transitions+=1
            pending=None

        equity.append(qty*float(getattr(row,current+"_close")))
        dates.append(ts)
        holdings.append(current)

        if pos==len(rows)-1:
            continue
        cands=[e for e in events.get(ts,[]) if e["from_asset"]==current and e["to_asset"] in aset]
        if not cands:
            continue
        if len(cands)>1:
            conflicts+=1
        pending=dict(sorted(cands,key=lambda e:(-float(e["max_dislocation"]),e["to_asset"],e["pair"]))[0])

    peak=equity[0]
    dd=0.0
    for v in equity:
        peak=max(peak,v)
        dd=min(dd,v/peak-1.0)

    out={
        "start_asset":start_asset,
        "return":float(equity[-1]/equity[0]-1.0),
        "max_dd":float(dd),
        "transitions":transitions,
        "conflicts":conflicts,
    }
    if collect:
        out.update({"equity":equity,"dates":dates,"holdings":holdings})
    return out


def summarize(panel,events,assets,start,end):
    runs=[run_one(panel,events,assets,start,end,a) for a in COMMON_STARTERS]
    df=pd.DataFrame(runs)
    atom=df[df.start_asset=="ATOM"].iloc[0]
    return {
        "median_return":float(df["return"].median()),
        "worst_return":float(df["return"].min()),
        "positive_starts":int((df["return"]>0).sum()),
        "median_max_dd":float(df["max_dd"].median()),
        "atom_return":float(atom["return"]),
        "atom_max_dd":float(atom["max_dd"]),
        "median_transitions":float(df["transitions"].median()),
        "median_conflicts":float(df["conflicts"].median()),
    }


def neutralized_pepe_summary(panel,events,assets):
    vals=[]
    atom=None
    for a in COMMON_STARTERS:
        r=run_one(panel,events,assets,MATURE_START,END,a,collect=True)
        value=1.0
        for i in range(1,len(r["equity"])):
            ratio=r["equity"][i]/r["equity"][i-1]
            if r["holdings"][i]=="PEPE" and PEPE_BULL_START<=r["dates"][i]<=PEPE_BULL_END and ratio>1.0:
                ratio=1.0
            value*=ratio
        ret=float(value-1.0)
        vals.append(ret)
        if a=="ATOM":
            atom=ret
    return {"median_return":float(pd.Series(vals).median()),"atom_return":float(atom)}


def monthly_windows(panel,months):
    cursor=pd.Timestamp(MATURE_START.year,MATURE_START.month,1,tz="UTC")
    if cursor<MATURE_START:
        cursor+=pd.offsets.MonthBegin(1)
    out=[]
    while True:
        starts=panel[panel.timestamp>=cursor]
        if starts.empty:
            break
        start=utc(starts.iloc[0].timestamp)
        target=start+pd.DateOffset(months=months)-pd.Timedelta("1D")
        if target>END:
            break
        end=utc(panel[panel.timestamp<=target].iloc[-1].timestamp)
        out.append((start,end))
        cursor+=pd.offsets.MonthBegin(1)
    return out


def rolling(panel,events,assets,months):
    rows=[]
    for start,end in monthly_windows(panel,months):
        s=summarize(panel,events,assets,start,end)
        rows.append({"start":start.isoformat(),"end":end.isoformat(),**s})
    df=pd.DataFrame(rows)
    return {
        "count":len(df),
        "median_return":float(df.median_return.median()),
        "worst_return":float(df.median_return.min()),
        "positive_rate":float((df.median_return>0).mean()),
        "worst_atom":float(df.atom_return.min()),
        "atom_positive_rate":float((df.atom_return>0).mean()),
    }


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cutoff",default="2026-09-27T00:00:00Z")
    args=ap.parse_args()
    panel,meta=download_panel(utc(args.cutoff))
    events=build_events(panel)
    run_dir=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True,exist_ok=True)

    result={"generated_at":pd.Timestamp.now(tz="UTC").isoformat(),"source_commit":source_sha(),"data":meta,"universes":{}}
    for name,assets in UNIVERSES.items():
        result["universes"][name]={
            "assets":list(assets),
            "mature":summarize(panel,events,assets,MATURE_START,END),
            "latest_year":summarize(panel,events,assets,YEAR_START,END),
            "latest_two_year":summarize(panel,events,assets,TWO_YEAR_START,END),
            "rolling_12m":rolling(panel,events,assets,12),
            "rolling_24m":rolling(panel,events,assets,24),
            "pepe_bull_neutralized_mature":neutralized_pepe_summary(panel,events,assets),
        }

    (run_dir/"results.json").write_text(json.dumps(result,indent=2,sort_keys=True,allow_nan=False),encoding="utf-8")

    lines=["# U10 SOL-to-ETH substitution v1","","Mode: STRESS_TEST_ONLY","Live unchanged.","",
           "| Universe | Mature | Latest 1Y | Latest 2Y | Mature DD | PEPE-neutralized mature |",
           "|---|---:|---:|---:|---:|---:|"]
    for name,u in result["universes"].items():
        lines.append(
            f"| {name} | {100*u['mature']['median_return']:+.2f}% | {100*u['latest_year']['median_return']:+.2f}% | "
            f"{100*u['latest_two_year']['median_return']:+.2f}% | {100*u['mature']['median_max_dd']:+.2f}% | "
            f"{100*u['pepe_bull_neutralized_mature']['median_return']:+.2f}% |"
        )
    (run_dir/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")

    print("run_dir="+str(run_dir))
    for name,u in result["universes"].items():
        print(name+"_mature=%.6f" % u["mature"]["median_return"])
        print(name+"_year=%.6f" % u["latest_year"]["median_return"])
        print(name+"_2y=%.6f" % u["latest_two_year"]["median_return"])
        print(name+"_dd=%.6f" % u["mature"]["median_max_dd"])
        print(name+"_pepe_neutral=%.6f" % u["pepe_bull_neutralized_mature"]["median_return"])
        print(name+"_24m_worst=%.6f" % u["rolling_24m"]["worst_return"])
    return 0


if __name__=="__main__":
    raise SystemExit(main())
