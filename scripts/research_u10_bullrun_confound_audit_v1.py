from __future__ import annotations

import argparse
import itertools
import json
import math
import os
import subprocess
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import build_pair_monitor

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "u10_bullrun_confound_audit_v1"

POOL = (
    "ATOM","TWT","PEPE","BNB","SOL","TRX","AAVE","LINK",
    "AVAX","FIL","ETH","ALGO","ADA","XRP","HBAR",
)
MANDATORY = ("ATOM","TWT","PEPE")
U8 = ("ATOM","TWT","PEPE","BNB","SOL","TRX","AAVE","LINK")
U10 = U8 + ("FIL","HBAR")
NICHES = {
    "ATOM":"INTEROPERABILITY","TWT":"WALLET","PEPE":"MEME",
    "BNB":"EXCHANGE_PLATFORM","SOL":"SMART_CONTRACT_L1","TRX":"PAYMENTS",
    "AAVE":"LENDING","LINK":"ORACLE","AVAX":"SMART_CONTRACT_L1",
    "FIL":"DECENTRALIZED_STORAGE","ETH":"SMART_CONTRACT_L1",
    "ALGO":"SMART_CONTRACT_L1","ADA":"SMART_CONTRACT_L1","XRP":"PAYMENTS",
    "HBAR":"ENTERPRISE_DLT",
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
TRAIN_START=pd.Timestamp("2024-03-29",tz="UTC")
TRAIN_END=pd.Timestamp("2025-03-28",tz="UTC")

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
        panel[cols].copy(),assets=POOL,lookback=LOOKBACK,
        arm_threshold=ARM,reversal=REVERSAL,
    )
    by_date=defaultdict(list)
    for e in events:
        if e["event"]=="CONFIRMED":
            by_date[utc(e["date"])].append(e)
    return by_date


def cached_rows(panel,start,end):
    k=(start.isoformat(),end.isoformat())
    if k not in WINDOW_CACHE:
        w=panel[(panel["timestamp"]>=start)&(panel["timestamp"]<=end)]
        WINDOW_CACHE[k]=list(w.itertuples(index=False,name="MarketRow"))
    return WINDOW_CACHE[k]


def run_one(panel,events,assets,start,end,start_asset,collect=False):
    aset=set(assets)
    rows=cached_rows(panel,start,end)
    current=start_asset
    qty=1.0/float(getattr(rows[0],current+"_open"))
    pending=None
    equity=[]
    dates=[]
    holdings=[]
    route=[current]
    transitions=0
    conflicts=0

    for pos,row in enumerate(rows):
        ts=utc(row.timestamp)
        if pending is not None:
            value=qty*float(getattr(row,current+"_open"))
            current=pending["to_asset"]
            qty=value*(1.0-COST)/float(getattr(row,current+"_open"))
            transitions+=1
            route.append(current)
            pending=None

        equity.append(qty*float(getattr(row,current+"_close")))
        dates.append(ts)
        holdings.append(current)

        if pos==len(rows)-1:
            continue
        cands=[
            e for e in events.get(ts,[])
            if e["from_asset"]==current and e["to_asset"] in aset
        ]
        if not cands:
            continue
        if len(cands)>1:
            conflicts+=1
        pending=dict(sorted(
            cands,key=lambda e:(-float(e["max_dislocation"]),e["to_asset"],e["pair"])
        )[0])

    peak=equity[0]
    worst=0.0
    for v in equity:
        peak=max(peak,v)
        worst=min(worst,v/peak-1.0)

    out={
        "start_asset":start_asset,
        "return":float(equity[-1]/equity[0]-1.0),
        "max_dd":float(worst),
        "transitions":transitions,
        "conflicts":conflicts,
    }
    if collect:
        out.update({"equity":equity,"dates":dates,"holdings":holdings,"route":route})
    return out


def summarize(panel,events,assets,start,end,starters=U8):
    runs=[run_one(panel,events,assets,start,end,a) for a in starters if a in assets]
    df=pd.DataFrame(runs)
    atom=df[df["start_asset"]=="ATOM"].iloc[0]
    return {
        "median_return":float(df["return"].median()),
        "worst_return":float(df["return"].min()),
        "positive_starts":int((df["return"]>0).sum()),
        "median_max_dd":float(df["max_dd"].median()),
        "atom_return":float(atom["return"]),
        "atom_max_dd":float(atom["max_dd"]),
        "median_transitions":float(df["transitions"].median()),
    }


def max_close_return(s,days):
    return float((s/s.shift(days)-1.0).max())


def max_drawup_interval(dates,prices,days):
    best=-1.0
    best_i=0
    best_j=0
    dq=[]
    from collections import deque
    q=deque()
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
                best_i=i
                best_j=j
    return float(best),dates[best_i],dates[best_j]


def bull_metrics(panel,asset):
    dates=[utc(x) for x in panel["timestamp"]]
    p=panel[asset+"_close"].astype(float).reset_index(drop=True)
    vals=p.to_numpy()
    d90,s90,e90=max_drawup_interval(dates,vals,90)
    d180,s180,e180=max_drawup_interval(dates,vals,180)
    r={
        "asset":asset,
        "max_30d_return":max_close_return(p,30),
        "max_90d_return":max_close_return(p,90),
        "max_180d_return":max_close_return(p,180),
        "max_90d_drawup":d90,
        "max_180d_drawup":d180,
        "strongest_90_start":s90.isoformat(),
        "strongest_90_end":e90.isoformat(),
        "strongest_180_start":s180.isoformat(),
        "strongest_180_end":e180.isoformat(),
        "common_history_return":float(vals[-1]/vals[0]-1.0),
    }
    r["strict_flag"]=bool(d90>=1.0 or d180>=2.0)
    r["primary_flag"]=bool(d90>=2.0 or d180>=4.0)
    r["lenient_flag"]=bool(d90>=3.0 or d180>=6.0)
    return r


def neutralized_return(run,intervals):
    eq=run["equity"]
    dates=run["dates"]
    holds=run["holdings"]
    value=1.0
    for i in range(1,len(eq)):
        ratio=eq[i]/eq[i-1]
        asset=holds[i]
        interval=intervals.get(asset)
        if interval is not None:
            start,end=interval
            if start<=dates[i]<=end and ratio>1.0:
                ratio=1.0
        value*=ratio
    return float(value-1.0)


def neutralized_summary(panel,events,intervals):
    vals=[]
    atom=None
    for a in U8:
        r=run_one(panel,events,U10,MATURE_START,END,a,collect=True)
        ret=neutralized_return(r,intervals)
        vals.append(ret)
        if a=="ATOM":
            atom=ret
    return {"median_return":float(pd.Series(vals).median()),"atom_return":float(atom)}


def all_u8():
    rest=[a for a in POOL if a not in MANDATORY]
    return [tuple(MANDATORY+x) for x in itertools.combinations(rest,5)]


def structural_score(panel,assets):
    w=panel[(panel["timestamp"]>=TRAIN_START)&(panel["timestamp"]<=TRAIN_END)]
    rets=pd.DataFrame({
        a:np.log(w[a+"_close"].astype(float)).diff()
        for a in assets
    }).dropna()
    corr=rets.corr().to_numpy()
    tri=np.triu_indices(len(assets),k=1)
    mean_abs_corr=float(np.mean(np.abs(corr[tri])))
    cov=np.cov(rets.to_numpy(),rowvar=False)
    eig=np.linalg.eigvalsh(cov)
    pca=float(eig[-1]/eig.sum()) if eig.sum()>0 else 1.0
    rel=[]
    for a,b in itertools.combinations(assets,2):
        rel.append(float((rets[b]-rets[a]).std(ddof=1)))
    relv=float(np.mean(rel))
    niches=len({NICHES[a] for a in assets})
    return {
        "niches":niches,
        "mean_abs_corr":mean_abs_corr,
        "pca1_share":pca,
        "mean_relative_vol":relv,
    }


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cutoff",default="2026-09-27T00:00:00Z")
    args=ap.parse_args()
    panel,meta=download_panel(utc(args.cutoff))
    events=build_events(panel)

    run_dir=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True,exist_ok=True)

    metrics=[bull_metrics(panel,a) for a in POOL]
    mdf=pd.DataFrame(metrics).sort_values("max_90d_drawup",ascending=False)
    mdf.to_csv(run_dir/"token_bull_metrics.csv",index=False)
    by_asset={x["asset"]:x for x in metrics}
    primary={a for a,x in by_asset.items() if x["primary_flag"]}

    baseline=summarize(panel,events,U10,MATURE_START,END)
    ablation=[]
    for a in U10:
        if a in MANDATORY:
            continue
        assets=tuple(x for x in U10 if x!=a)
        s=summarize(panel,events,assets,MATURE_START,END,starters=tuple(x for x in U8 if x!=a))
        ablation.append({
            "removed":a,
            "primary_explosive":a in primary,
            **s,
            "delta_median_vs_u10":s["median_return"]-baseline["median_return"],
            "delta_atom_vs_u10":s["atom_return"]-baseline["atom_return"],
        })
    pd.DataFrame(ablation).to_csv(run_dir/"u10_node_ablation.csv",index=False)

    intervals_all={}
    per_token_neutral=[]
    for a in U10:
        x=by_asset[a]
        interval=(utc(x["strongest_90_start"]),utc(x["strongest_90_end"]))
        intervals_all[a]=interval
        n=neutralized_summary(panel,events,{a:interval})
        per_token_neutral.append({
            "asset":a,
            "primary_explosive":a in primary,
            "neutralized_median_return":n["median_return"],
            "neutralized_atom_return":n["atom_return"],
            "delta_median_vs_baseline":n["median_return"]-baseline["median_return"],
        })
    pd.DataFrame(per_token_neutral).to_csv(run_dir/"u10_bull_interval_neutralization.csv",index=False)

    primary_intervals={a:intervals_all[a] for a in U10 if a in primary}
    multi_neutral=neutralized_summary(panel,events,primary_intervals)

    dist_rows=[]
    for assets in all_u8():
        last=summarize(panel,events,assets,YEAR_START,END,starters=assets)
        two=summarize(panel,events,assets,TWO_YEAR_START,END,starters=assets)
        dist_rows.append({
            "assets":"|".join(assets),
            "primary_explosive_count":sum(1 for a in assets if a in primary),
            "optional_primary_explosive_count":sum(1 for a in assets if a in primary and a not in MANDATORY),
            "last_year_median":last["median_return"],
            "two_year_median":two["median_return"],
        })
    dist=pd.DataFrame(dist_rows)
    dist.to_csv(run_dir/"u8_distribution_by_bull_count.csv",index=False)
    groups=[]
    for cnt,g in dist.groupby("optional_primary_explosive_count"):
        groups.append({
            "optional_primary_explosive_count":int(cnt),
            "sets":len(g),
            "last_year_median":float(g["last_year_median"].median()),
            "last_year_positive_rate":float((g["last_year_median"]>0).mean()),
            "two_year_median":float(g["two_year_median"].median()),
        })

    optional_clean=[a for a in POOL if a not in MANDATORY and a not in primary]
    bull_clean_sets=[]
    if len(optional_clean)>=5:
        for extra in itertools.combinations(optional_clean,5):
            assets=tuple(MANDATORY+extra)
            f=structural_score(panel,assets)
            bull_clean_sets.append({"assets":"|".join(assets),**f})
        bc=pd.DataFrame(bull_clean_sets)
        # Fixed simple structural score: max niches, then relative vol rank + inverse corr + inverse PCA equal weights.
        bc["rel_pct"]=bc["mean_relative_vol"].rank(pct=True)
        bc["corr_pct"]=1.0-bc["mean_abs_corr"].rank(pct=True)+1.0/len(bc)
        bc["pca_pct"]=1.0-bc["pca1_share"].rank(pct=True)+1.0/len(bc)
        bc["score"]=(bc["rel_pct"]+bc["corr_pct"]+bc["pca_pct"])/3.0
        bc=bc.sort_values(["niches","score","assets"],ascending=[False,False,True])
        bc.to_csv(run_dir/"bull_clean_structural_candidates.csv",index=False)
        bull_clean_top=bc.head(10).to_dict(orient="records")
    else:
        bull_clean_top=[]

    result={
        "generated_at":pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit":source_sha(),
        "data":meta,
        "primary_explosive_assets":sorted(primary),
        "u10_primary_explosive_assets":[a for a in U10 if a in primary],
        "mandatory_primary_explosive_assets":[a for a in MANDATORY if a in primary],
        "optional_clean_assets":optional_clean,
        "u10_baseline":baseline,
        "ablation":ablation,
        "per_token_neutralization":per_token_neutral,
        "multi_primary_neutralization":multi_neutral,
        "u8_distribution_groups":groups,
        "bull_clean_structural_top10":bull_clean_top,
        "bull_clean_set_count":len(bull_clean_sets),
        "token_metrics":metrics,
    }
    (run_dir/"results.json").write_text(json.dumps(result,indent=2,sort_keys=True,allow_nan=False),encoding="utf-8")

    lines=[
        "# U10 Bull-Run Confound Audit v1","",
        "Mode: STRESS_TEST_ONLY","Live unchanged.","",
        "## Primary explosive assets","",
        ", ".join(sorted(primary)) if primary else "None",
        "",
        "## U10 primary explosive members","",
        ", ".join(result["u10_primary_explosive_assets"]) if result["u10_primary_explosive_assets"] else "None",
        "",
        f"U10 mature median baseline: {100*baseline['median_return']:+.2f}%",
        f"U10 mature ATOM baseline: {100*baseline['atom_return']:+.2f}%",
        f"After simultaneous strongest-90d positive-gain neutralization for all PRIMARY U10 explosive tokens: {100*multi_neutral['median_return']:+.2f}% median / {100*multi_neutral['atom_return']:+.2f}% ATOM",
        "",
        f"Bull-clean optional assets available: {len(optional_clean)}",
        f"Bull-clean U8 combinations with mandatory ATOM/TWT/PEPE: {len(bull_clean_sets)}",
    ]
    (run_dir/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")

    print("run_dir="+str(run_dir))
    print("primary_explosive="+",".join(sorted(primary)))
    print("u10_primary="+",".join(result["u10_primary_explosive_assets"]))
    print("mandatory_primary="+",".join(result["mandatory_primary_explosive_assets"]))
    print("u10_baseline_median=%.6f" % baseline["median_return"])
    print("u10_baseline_atom=%.6f" % baseline["atom_return"])
    print("multi_neutral_median=%.6f" % multi_neutral["median_return"])
    print("multi_neutral_atom=%.6f" % multi_neutral["atom_return"])
    print("bull_clean_optional_count="+str(len(optional_clean)))
    print("bull_clean_set_count="+str(len(bull_clean_sets)))
    return 0


if __name__=="__main__":
    raise SystemExit(main())
