from __future__ import annotations

import argparse
import itertools
import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "rr_u10_vs_u11_link_v1"

U10 = ("TWT","PEPE","BNB","TRX","AAVE","AVAX","FIL","ALGO","XRP","HBAR")
U11 = U10 + ("LINK",)
LOOKBACK=180
ARM=0.15
REVERSAL=0.03
DD_RATIO=1.50
COST=0.001
DATA_START=pd.Timestamp("2023-05-05",tz="UTC")


@dataclass
class PairState:
    mode:str="NONE"
    extreme:float|None=None
    max_dislocation:float=0.0


def utc(value):
    t=pd.Timestamp(value)
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
    for asset in U11:
        result=download_historical_dataset(
            client,symbol=asset+"USDT",start=DATA_START,end=cutoff,
            timeframe="1D",as_of=cutoff
        )
        if result.dataset is None:
            raise RuntimeError(f"{asset}: {result.metadata.status}")
        if result.dataset.quality.has_critical_issues:
            raise RuntimeError(f"{asset}: critical data quality")
        f=result.dataset.candles[["timestamp","open","close"]].copy()
        f["timestamp"]=pd.to_datetime(f["timestamp"],utc=True)
        f=f.rename(columns={"open":asset+"_open","close":asset+"_close"})
        meta[asset]={
            "rows":int(len(f)),
            "start":pd.Timestamp(f.iloc[0]["timestamp"]).isoformat(),
            "end":pd.Timestamp(f.iloc[-1]["timestamp"]).isoformat(),
        }
        panel=f if panel is None else panel.merge(f,on="timestamp",how="inner",validate="one_to_one")
    panel=panel.sort_values("timestamp",kind="stable").reset_index(drop=True)
    return panel,meta


def pair_rows_for_day(panel, assets):
    timestamps=pd.DatetimeIndex(pd.to_datetime(panel["timestamp"],utc=True))
    states={(a,b):PairState() for a,b in itertools.combinations(assets,2)}
    hist={(a,b):[] for a,b in itertools.combinations(assets,2)}
    events=[[] for _ in range(len(panel))]
    snapshots=[]

    for i,ts in enumerate(timestamps):
        snap={}
        for left,right in itertools.combinations(assets,2):
            key=(left,right)
            ratio=float(panel.iloc[i][right+"_close"])/float(panel.iloc[i][left+"_close"])
            h=hist[key]
            h.append(ratio)
            st=states[key]
            if len(h)<LOOKBACK:
                snap[key]={"pair":f"{left}/{right}","mode":st.mode,"from_asset":None,"to_asset":None,
                           "deviation":None,"max_dislocation":st.max_dislocation,"reversal_from_extreme":None}
                continue
            med=float(np.median(h[-LOOKBACK:]))
            dev=ratio/med-1.0
            if st.mode=="NONE":
                if dev>=ARM:
                    st=PairState("HIGH",ratio,abs(dev))
                    events[i].append({"date":ts.isoformat(),"event":"ARMED","pair":f"{left}/{right}",
                                      "from_asset":right,"to_asset":left,"deviation":dev,
                                      "max_dislocation":abs(dev),"reversal_from_extreme":0.0})
                elif dev<=-ARM:
                    st=PairState("LOW",ratio,abs(dev))
                    events[i].append({"date":ts.isoformat(),"event":"ARMED","pair":f"{left}/{right}",
                                      "from_asset":left,"to_asset":right,"deviation":dev,
                                      "max_dislocation":abs(dev),"reversal_from_extreme":0.0})
            elif st.mode=="HIGH":
                if ratio>float(st.extreme):
                    st.extreme=ratio
                    st.max_dislocation=max(st.max_dislocation,abs(dev))
                retr=1.0-ratio/float(st.extreme)
                if retr>=REVERSAL:
                    events[i].append({"date":ts.isoformat(),"event":"CONFIRMED","pair":f"{left}/{right}",
                                      "from_asset":right,"to_asset":left,"deviation":dev,
                                      "max_dislocation":st.max_dislocation,"reversal_from_extreme":retr})
                    st=PairState()
            elif st.mode=="LOW":
                if ratio<float(st.extreme):
                    st.extreme=ratio
                    st.max_dislocation=max(st.max_dislocation,abs(dev))
                retr=ratio/float(st.extreme)-1.0
                if retr>=REVERSAL:
                    events[i].append({"date":ts.isoformat(),"event":"CONFIRMED","pair":f"{left}/{right}",
                                      "from_asset":left,"to_asset":right,"deviation":dev,
                                      "max_dislocation":st.max_dislocation,"reversal_from_extreme":retr})
                    st=PairState()

            states[key]=st
            pf=pt=None
            rr=None
            if st.mode=="HIGH" and st.extreme is not None:
                pf,pt=right,left
                rr=max(0.0,1.0-ratio/float(st.extreme))
            elif st.mode=="LOW" and st.extreme is not None:
                pf,pt=left,right
                rr=max(0.0,ratio/float(st.extreme)-1.0)
            snap[key]={"pair":f"{left}/{right}","mode":st.mode,"from_asset":pf,"to_asset":pt,
                       "deviation":dev,"max_dislocation":st.max_dislocation,"reversal_from_extreme":rr}
        snapshots.append(snap)
    return timestamps,events,snapshots


def better(a,b):
    if a is None:return b
    ac=str(a.get("event") or "").upper()=="CONFIRMED"
    bc=str(b.get("event") or "").upper()=="CONFIRMED"
    if bc!=ac:return b if bc else a
    return b if float(b.get("max_dislocation") or 0)>float(a.get("max_dislocation") or 0) else a


def state_candidate(row,date_iso):
    if str(row.get("mode") or "NONE").upper() not in {"HIGH","LOW"}:
        return None
    fa=str(row.get("from_asset") or "")
    ta=str(row.get("to_asset") or "")
    if not fa or not ta:return None
    return {"date":date_iso,"event":"ARMED","pair":row.get("pair"),"from_asset":fa,"to_asset":ta,
            "deviation":row.get("deviation"),"max_dislocation":float(row.get("max_dislocation") or 0),
            "reversal_from_extreme":row.get("reversal_from_extreme")}


def route_for_day(current,day_i,assets,timestamps,events,snapshots):
    aset=set(assets)
    today=events[day_i]
    confirmed=[e for e in today if e["event"]=="CONFIRMED" and e["from_asset"]==current and e["to_asset"] in aset]
    confirmed.sort(key=lambda e:(-float(e["max_dislocation"]),e["to_asset"],e["pair"]))
    if not confirmed:return None
    primary=dict(confirmed[0])
    pto=primary["to_asset"]
    pstr=float(primary["max_dislocation"])
    date_iso=timestamps[day_i].isoformat()

    outbound={}
    for e in today:
        if e["from_asset"]==current and e["to_asset"] in aset and e["to_asset"]!=pto:
            outbound[e["to_asset"]]=better(outbound.get(e["to_asset"]),dict(e))
    for row in snapshots[day_i].values():
        c=state_candidate(row,date_iso)
        if c and c["from_asset"]==current and c["to_asset"] in aset and c["to_asset"]!=pto:
            outbound[c["to_asset"]]=better(outbound.get(c["to_asset"]),c)

    def relation(target):
        rel=None
        for e in today:
            if e["from_asset"]==pto and e["to_asset"]==target:
                rel=better(rel,dict(e))
        for row in snapshots[day_i].values():
            c=state_candidate(row,date_iso)
            if c and c["from_asset"]==pto and c["to_asset"]==target:
                rel=better(rel,c)
        return rel

    qs=[]
    for target,c in outbound.items():
        strength=float(c.get("max_dislocation") or 0)
        if strength+1e-12<pstr*DD_RATIO:continue
        rel=relation(target)
        if rel is not None:
            qs.append((strength,target,c,rel))
    if not qs:return primary
    qs.sort(key=lambda x:(x[0],x[1]),reverse=True)
    _,target,c,rel=qs[0]
    out=dict(primary)
    out.update({"to_asset":target,"pair":c.get("pair"),"deviation":c.get("deviation"),
                "max_dislocation":c.get("max_dislocation"),"reversal_from_extreme":c.get("reversal_from_extreme"),
                "route_override":True,"destination_relation":rel})
    return out


def bounds(timestamps,start,end):
    s=int(timestamps.searchsorted(utc(start),side="left"))
    e=int(timestamps.searchsorted(utc(end),side="right"))-1
    if e<s:raise RuntimeError("empty window")
    return s,e


def simulate(panel,timestamps,events,snapshots,assets,start_i,end_i,start_asset):
    opens={a:panel[a+"_open"].astype(float).to_numpy() for a in U11}
    closes={a:panel[a+"_close"].astype(float).to_numpy() for a in U11}
    current=start_asset
    qty=1.0/float(opens[current][start_i])
    initial=qty*float(closes[current][start_i])
    equity=[]
    transitions=0
    overrides=0
    pending=None
    for i in range(start_i,end_i+1):
        if pending is not None:
            value=qty*float(opens[current][i])
            current=pending["to_asset"]
            qty=value*(1.0-COST)/float(opens[current][i])
            transitions+=1
            overrides+=int(bool(pending.get("route_override")))
            pending=None
        equity.append(qty*float(closes[current][i]))
        if i<end_i:
            pending=route_for_day(current,i,assets,timestamps,events,snapshots)
    arr=np.asarray(equity,dtype=float)
    peak=np.maximum.accumulate(arr)
    dd=float(np.min(arr/peak-1.0))
    return {"return":float(arr[-1]/initial-1.0),"max_dd":dd,"transitions":transitions,"overrides":overrides}


def evaluate(panel,timestamps,events,snapshots,assets,start_assets,start,end):
    si,ei=bounds(timestamps,start,end)
    rows=[]
    for s in start_assets:
        r=simulate(panel,timestamps,events,snapshots,assets,si,ei,s)
        rows.append({"start_asset":s,**r})
    df=pd.DataFrame(rows)
    return df,{
        "median_return":float(df["return"].median()),
        "worst_return":float(df["return"].min()),
        "best_return":float(df["return"].max()),
        "median_max_dd":float(df["max_dd"].median()),
        "worst_max_dd":float(df["max_dd"].min()),
        "median_transitions":float(df["transitions"].median()),
        "median_overrides":float(df["overrides"].median()),
    }


def rolling(panel,timestamps,events,snapshots,assets,start_assets,months,end_cap):
    first=pd.Timestamp("2023-11-01",tz="UTC")
    vals=[]
    for start in pd.date_range(first,end_cap,freq="MS",tz="UTC"):
        end=utc(start+pd.DateOffset(months=months)-pd.Timedelta(days=1))
        if end>end_cap:continue
        _,m=evaluate(panel,timestamps,events,snapshots,assets,start_assets,start,end)
        vals.append(m)
    d=pd.DataFrame(vals)
    return {
        "count":int(len(d)),
        "median_return":float(d["median_return"].median()),
        "worst_return":float(d["median_return"].min()),
        "positive_rate":float((d["median_return"]>0).mean()),
        "median_max_dd":float(d["median_max_dd"].median()),
        "worst_max_dd":float(d["median_max_dd"].min()),
    }


def pct(x):return f"{100*x:+.1f}%"


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cutoff")
    args=ap.parse_args()
    cutoff=utc(args.cutoff) if args.cutoff else pd.Timestamp.now(tz="UTC").floor("D")
    panel,meta=download_panel(cutoff)
    timestamps,events,snapshots=pair_rows_for_day(panel,U11)
    mature_start=pd.Timestamp("2023-10-31",tz="UTC")
    end=timestamps[-1]
    windows={
        "last_1y":(utc(end-pd.DateOffset(years=1)+pd.Timedelta(days=1)),end),
        "last_2y":(utc(end-pd.DateOffset(years=2)+pd.Timedelta(days=1)),end),
        "mature":(mature_start,end),
    }

    summary={"experiment":"RR_U10_VS_U11_LINK_V1","source_commit_sha":source_sha(),
             "production_changes":"NONE","parameters":{"lookback":LOOKBACK,"arm":ARM,"reversal":REVERSAL,
             "destination_dominance_ratio":DD_RATIO,"cost":COST,"execution":"signal close T -> next daily open"},
             "data":{"start":timestamps[0].isoformat(),"end":end.isoformat(),"meta":meta},"windows":{}}
    all_details=[]
    for name,(s,e) in windows.items():
        d10,m10=evaluate(panel,timestamps,events,snapshots,U10,U10,s,e)
        d11_common,m11_common=evaluate(panel,timestamps,events,snapshots,U11,U10,s,e)
        d11_all,m11_all=evaluate(panel,timestamps,events,snapshots,U11,U11,s,e)
        merged=d10.merge(d11_common,on="start_asset",suffixes=("_u10","_u11"))
        merged["return_delta_pp"]=100*(merged["return_u11"]-merged["return_u10"])
        merged["dd_delta_pp"]=100*(merged["max_dd_u11"]-merged["max_dd_u10"])
        merged["window"]=name
        all_details.append(merged)
        link_row=d11_all[d11_all["start_asset"]=="LINK"].iloc[0].to_dict()
        summary["windows"][name]={
            "u10":m10,"u11_common10":m11_common,"u11_all11":m11_all,
            "common10_median_return_delta_pp":100*(m11_common["median_return"]-m10["median_return"]),
            "common10_median_dd_delta_pp":100*(m11_common["median_max_dd"]-m10["median_max_dd"]),
            "u11_beats_u10_starts":int((merged["return_u11"]>merged["return_u10"]).sum()),
            "common_start_count":10,
            "link_start":{k:(float(v) if isinstance(v,(np.floating,float)) else int(v) if isinstance(v,(np.integer,int)) else v)
                          for k,v in link_row.items() if k!="start_asset"},
        }

    summary["rolling_12m"]={
        "u10":rolling(panel,timestamps,events,snapshots,U10,U10,12,end),
        "u11_common10":rolling(panel,timestamps,events,snapshots,U11,U10,12,end),
    }
    summary["rolling_24m"]={
        "u10":rolling(panel,timestamps,events,snapshots,U10,U10,24,end),
        "u11_common10":rolling(panel,timestamps,events,snapshots,U11,U10,24,end),
    }

    run_dir=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True,exist_ok=True)
    pd.concat(all_details,ignore_index=True).to_csv(run_dir/"common10_start_details.csv",index=False)
    (run_dir/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True,default=str),encoding="utf-8")

    lines=["# Current U10 vs U11 = U10 + LINK V1","",
           "Mode: STRESS_TEST_ONLY — production/live unchanged.","",
           f"Data end: {end.isoformat()}","",
           "|Window|U10 median|U11 median (same 10 starts)|delta pp|U11 wins starts|U10 DD|U11 DD|",
           "|---|---:|---:|---:|---:|---:|---:|"]
    for name in ("last_1y","last_2y","mature"):
        x=summary["windows"][name]
        lines.append(f"|{name}|{pct(x['u10']['median_return'])}|{pct(x['u11_common10']['median_return'])}|"
                     f"{x['common10_median_return_delta_pp']:+.1f}|{x['u11_beats_u10_starts']}/10|"
                     f"{pct(x['u10']['median_max_dd'])}|{pct(x['u11_common10']['median_max_dd'])}|")
    lines+=["","## Rolling windows","",
            f"- 12m median: U10 {pct(summary['rolling_12m']['u10']['median_return'])}; "
            f"U11 {pct(summary['rolling_12m']['u11_common10']['median_return'])}.",
            f"- 12m worst: U10 {pct(summary['rolling_12m']['u10']['worst_return'])}; "
            f"U11 {pct(summary['rolling_12m']['u11_common10']['worst_return'])}.",
            f"- 24m median: U10 {pct(summary['rolling_24m']['u10']['median_return'])}; "
            f"U11 {pct(summary['rolling_24m']['u11_common10']['median_return'])}.",
            f"- 24m worst: U10 {pct(summary['rolling_24m']['u10']['worst_return'])}; "
            f"U11 {pct(summary['rolling_24m']['u11_common10']['worst_return'])}.","",
            "## Boundary","",
            "- This test was requested after U10 had already been selected and LINK was specifically proposed; historical improvement is post-selection evidence.",
            "- No production universe or Telegram behavior changed.",""]
    (run_dir/"report.md").write_text("\n".join(lines),encoding="utf-8")
    print(f"run_dir={run_dir}")
    for name in ("last_1y","last_2y","mature"):
        x=summary["windows"][name]
        print(name,x["u10"]["median_return"],x["u11_common10"]["median_return"],x["common10_median_return_delta_pp"],x["u11_beats_u10_starts"])


if __name__=="__main__":
    raise SystemExit(main())
