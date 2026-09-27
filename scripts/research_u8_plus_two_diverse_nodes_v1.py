from __future__ import annotations

import argparse
import json
import os
import subprocess
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import build_pair_monitor

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "u8_plus_two_diverse_nodes_v1"

U8 = ("ATOM","TWT","PEPE","BNB","SOL","TRX","AAVE","LINK")
FIL = "FIL"
HBAR = "HBAR"
UNIVERSES = {
    "U8": U8,
    "U9_FIL": U8 + (FIL,),
    "U9_HBAR": U8 + (HBAR,),
    "U10_FIL_HBAR": U8 + (FIL,HBAR),
}
ALL_ASSETS = U8 + (FIL,HBAR)

LOOKBACK = 180
ARM = 0.15
REVERSAL = 0.03
COST = 0.001
DATA_START = pd.Timestamp("2023-05-05", tz="UTC")

ROUTERS = ("strongest","skip_conflict","weakest","first")
COSTS = (0.001,0.005,0.01)


def utc(x):
    t = pd.Timestamp(x)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def source_sha():
    v = os.environ.get("SOURCE_COMMIT_SHA")
    if v:
        return v
    return subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip()


def download_panel(cutoff):
    client=BinanceSpotRestClient()
    panel=None
    meta={}
    for a in ALL_ASSETS:
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


def event_map(panel,assets):
    cols=["timestamp"]+[a+"_close" for a in assets]
    events,_=build_pair_monitor(
        panel[cols].copy(),
        assets=assets,
        lookback=LOOKBACK,
        arm_threshold=ARM,
        reversal=REVERSAL,
    )
    by_date=defaultdict(list)
    for e in events:
        if e["event"]=="CONFIRMED":
            by_date[utc(e["date"])].append(e)
    return by_date


def choose(candidates,router):
    if not candidates:
        return None
    if router=="skip_conflict":
        return None if len(candidates)>1 else candidates[0]
    if router=="strongest":
        return sorted(candidates,key=lambda e:(-float(e["max_dislocation"]),e["to_asset"],e["pair"]))[0]
    if router=="weakest":
        return sorted(candidates,key=lambda e:(float(e["max_dislocation"]),e["to_asset"],e["pair"]))[0]
    if router=="first":
        return sorted(candidates,key=lambda e:(e["to_asset"],e["pair"],-float(e["max_dislocation"])))[0]
    raise ValueError(router)


def max_dd(values):
    s=pd.Series(values,dtype=float)
    return float((s/s.cummax()-1.0).min())


def run_one(panel,emap,assets,start,end,start_asset,router="strongest",cost=COST,collect=False):
    aset=set(assets)
    w=panel[(panel["timestamp"]>=start)&(panel["timestamp"]<=end)].copy()
    if w.empty:
        raise ValueError("empty window")
    current=start_asset
    qty=1.0/float(w.iloc[0][current+"_open"])
    pending=None
    equity=[]
    transitions=0
    conflicts=0
    holdings=Counter()
    edges=Counter()
    route=[current]

    for pos,(_,row) in enumerate(w.iterrows()):
        ts=utc(row["timestamp"])

        if pending is not None:
            value=qty*float(row[current+"_open"])
            old=current
            current=pending["to_asset"]
            qty=value*(1.0-cost)/float(row[current+"_open"])
            transitions+=1
            edges[(old,current)]+=1
            route.append(current)
            pending=None

        holdings[current]+=1
        equity.append(qty*float(row[current+"_close"]))

        if pos==len(w)-1:
            continue
        candidates=[
            e for e in emap.get(ts,[])
            if e["from_asset"]==current and e["to_asset"] in aset
        ]
        if not candidates:
            continue
        if len(candidates)>1:
            conflicts+=1
        selected=choose(candidates,router)
        if selected is not None:
            pending=dict(selected)

    out={
        "start_asset":start_asset,
        "return":float(equity[-1]/equity[0]-1.0),
        "max_dd":max_dd(equity),
        "transitions":transitions,
        "conflicts":conflicts,
    }
    if collect:
        out["holdings"]=dict(holdings)
        out["edges"]={f"{a}->{b}":n for (a,b),n in edges.items()}
        out["route"]=route
    return out


def aggregate(runs):
    df=pd.DataFrame(runs)
    atom=df[df["start_asset"]=="ATOM"].iloc[0]
    return {
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
    }


def full_eval(panel,emap,assets,start,end,router="strongest",cost=COST):
    return aggregate([
        run_one(panel,emap,assets,start,end,a,router=router,cost=cost)
        for a in U8
    ])


def monthly_windows(panel,mature_start,months):
    last=utc(panel.iloc[-1]["timestamp"])
    cursor=pd.Timestamp(mature_start.year,mature_start.month,1,tz="UTC")
    if cursor<mature_start:
        cursor=cursor+pd.offsets.MonthBegin(1)
    out=[]
    while True:
        starts=panel[panel["timestamp"]>=cursor]
        if starts.empty:
            break
        start=utc(starts.iloc[0]["timestamp"])
        target=start+pd.DateOffset(months=months)-pd.Timedelta("1D")
        if target>last:
            break
        ends=panel[panel["timestamp"]<=target]
        if ends.empty:
            break
        end=utc(ends.iloc[-1]["timestamp"])
        out.append((start,end))
        cursor=cursor+pd.offsets.MonthBegin(1)
    return out


def rolling_eval(panel,emap,assets,mature_start,months):
    rows=[]
    for start,end in monthly_windows(panel,mature_start,months):
        agg=full_eval(panel,emap,assets,start,end)
        rows.append({"start":start.isoformat(),"end":end.isoformat(),**agg})
    df=pd.DataFrame(rows)
    return rows,{
        "window_count":len(df),
        "median_window_median_return":float(df["median_return"].median()),
        "worst_window_median_return":float(df["median_return"].min()),
        "positive_window_rate":float((df["median_return"]>0).mean()),
        "median_atom_return":float(df["atom_return"].median()),
        "worst_atom_return":float(df["atom_return"].min()),
        "atom_positive_window_rate":float((df["atom_return"]>0).mean()),
        "median_max_dd":float(df["median_max_dd"].median()),
        "worst_max_dd":float(df["worst_max_dd"].min()),
        "median_transitions":float(df["median_transitions"].median()),
        "median_conflicts":float(df["median_conflicts"].median()),
    }


def occupancy_eval(panel,emap,assets,start,end):
    holdings=Counter()
    edges=Counter()
    route_samples={}
    for a in U8:
        r=run_one(panel,emap,assets,start,end,a,collect=True)
        holdings.update(r["holdings"])
        for edge,n in r["edges"].items():
            edges[edge]+=n
        route_samples[a]=r["route"]
    total=sum(holdings.values())
    return {
        "occupancy_days":dict(holdings),
        "occupancy_share":{a:holdings[a]/total for a in assets},
        "edge_counts":dict(edges),
        "atom_route":route_samples["ATOM"],
        "fil_entries":sum(n for edge,n in edges.items() if edge.endswith("->FIL")),
        "fil_exits":sum(n for edge,n in edges.items() if edge.startswith("FIL->")),
        "hbar_entries":sum(n for edge,n in edges.items() if edge.endswith("->HBAR")),
        "hbar_exits":sum(n for edge,n in edges.items() if edge.startswith("HBAR->")),
    }


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cutoff",default="2026-09-27T00:00:00Z")
    args=ap.parse_args()
    panel,meta=download_panel(utc(args.cutoff))

    common_start=utc(panel.iloc[0]["timestamp"])
    mature_start=utc(panel.iloc[LOOKBACK-1]["timestamp"])
    end=utc(panel.iloc[-1]["timestamp"])

    run_dir=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True,exist_ok=True)

    payload={
        "generated_at":pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit":source_sha(),
        "data":meta,
        "common_start":common_start.isoformat(),
        "mature_start":mature_start.isoformat(),
        "end":end.isoformat(),
        "mature_days":int((end-mature_start).days+1),
        "universes":{},
        "u10_cost_sensitivity":{},
        "u10_router_sensitivity":{},
    }

    maps={}
    for name,assets in UNIVERSES.items():
        emap=event_map(panel,assets)
        maps[name]=emap
        full=full_eval(panel,emap,assets,mature_start,end)
        r12_rows,r12=rolling_eval(panel,emap,assets,mature_start,12)
        r24_rows,r24=rolling_eval(panel,emap,assets,mature_start,24)
        occ=occupancy_eval(panel,emap,assets,mature_start,end)
        payload["universes"][name]={
            "assets":list(assets),
            "full_mature":full,
            "rolling_12m":r12,
            "rolling_24m":r24,
            "occupancy":occ,
        }
        pd.DataFrame(r12_rows).to_csv(run_dir/f"{name.lower()}_rolling_12m.csv",index=False)
        pd.DataFrame(r24_rows).to_csv(run_dir/f"{name.lower()}_rolling_24m.csv",index=False)

    u10=UNIVERSES["U10_FIL_HBAR"]
    u10map=maps["U10_FIL_HBAR"]
    for cost in COSTS:
        payload["u10_cost_sensitivity"][f"{100*cost:.2f}%"]=full_eval(
            panel,u10map,u10,mature_start,end,cost=cost
        )
    for router in ROUTERS:
        payload["u10_router_sensitivity"][router]=full_eval(
            panel,u10map,u10,mature_start,end,router=router
        )

    base=payload["universes"]["U8"]["full_mature"]
    for name in ("U9_FIL","U9_HBAR","U10_FIL_HBAR"):
        x=payload["universes"][name]["full_mature"]
        x["delta_median_vs_u8"]=x["median_return"]-base["median_return"]
        x["delta_atom_vs_u8"]=x["atom_return"]-base["atom_return"]
        x["delta_dd_vs_u8"]=x["median_max_dd"]-base["median_max_dd"]

    (run_dir/"results.json").write_text(
        json.dumps(payload,indent=2,sort_keys=True,allow_nan=False),
        encoding="utf-8"
    )

    lines=[
        "# U8 + Two Diverse Nodes v1","",
        "Mode: STRESS_TEST_ONLY",
        "Live U8 unchanged.","",
        f"Mature window: {mature_start.date()} -> {end.date()}",
        "",
        "| Universe | Mature median | ATOM | Median DD | 12m worst | 24m worst |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for name in UNIVERSES:
        u=payload["universes"][name]
        lines.append(
            f"| {name} | {100*u['full_mature']['median_return']:+.2f}% | "
            f"{100*u['full_mature']['atom_return']:+.2f}% | "
            f"{100*u['full_mature']['median_max_dd']:+.2f}% | "
            f"{100*u['rolling_12m']['worst_window_median_return']:+.2f}% | "
            f"{100*u['rolling_24m']['worst_window_median_return']:+.2f}% |"
        )
    (run_dir/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")

    print("run_dir="+str(run_dir))
    print("mature_start="+mature_start.isoformat())
    for name in UNIVERSES:
        u=payload["universes"][name]
        print(name+"_median=%.6f" % u["full_mature"]["median_return"])
        print(name+"_atom=%.6f" % u["full_mature"]["atom_return"])
        print(name+"_dd=%.6f" % u["full_mature"]["median_max_dd"])
        print(name+"_12m_worst=%.6f" % u["rolling_12m"]["worst_window_median_return"])
        print(name+"_24m_worst=%.6f" % u["rolling_24m"]["worst_window_median_return"])
    return 0


if __name__=="__main__":
    raise SystemExit(main())
