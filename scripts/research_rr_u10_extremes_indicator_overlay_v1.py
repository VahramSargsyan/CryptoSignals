from __future__ import annotations

import argparse, calendar, json, os, subprocess
from pathlib import Path
import numpy as np
import pandas as pd

import scripts.research_rr_u10_combined_total_btceth_regime_v1 as jointmod
import scripts.research_robust_exhaustive_u9_u10_atom_replacement_v1 as core

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/"research_artifacts"/"rr_u10_extremes_indicator_overlay_v1"
START=pd.Timestamp("2023-10-31",tz="UTC")
CUTOFF=pd.Timestamp("2026-09-26",tz="UTC")
EXPLOSIVE=0.40
SEVERE=-0.15
MILESTONES=[
 ("2024-04-12","LOSS_BULL_TO_MIXED"),
 ("2024-10-26","RETURN_BULL_BUILDING"),
 ("2024-11-24","FULL_BULL_CONFIRM"),
 ("2025-10-14","EXIT_FULL_BULL"),
 ("2025-11-03","TOTAL_BELOW_SMA200"),
 ("2026-02-19","FULL_BEAR_CONFIRM"),
 ("2026-05-02","TEMP_BULL_BUILDING"),
 ("2026-05-22","RETURN_MIXED"),
 ("2026-08-17","EXIT_FULL_BEAR"),
 ("2026-08-19","TOTAL_REGAINS_SMA200"),
 ("2026-08-21","TOTAL_REGAINS_SMA300"),
 ("2026-08-27","BULL_BUILDING"),
 ("2026-09-26","CURRENT_CUTOFF"),
]

def source_sha():
    return os.environ.get("SOURCE_COMMIT_SHA") or subprocess.check_output(
        ["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip()

def month_dates():
    rows=[]
    cur=pd.Timestamp("2023-10-01",tz="UTC")
    while cur<=CUTOFF:
        if cur.year==CUTOFF.year and cur.month==CUTOFF.month:
            rows.append(CUTOFF); break
        day=calendar.monthrange(cur.year,cur.month)[1]
        rows.append(pd.Timestamp(year=cur.year,month=cur.month,day=day,tz="UTC"))
        cur=cur+pd.offsets.MonthBegin(1)
    return rows

def idx_on_or_before(ts, when):
    return int(ts.searchsorted(when,side="right")-1)

def sample(ts, values, when):
    i=idx_on_or_before(ts,when)
    return np.nan if i<0 else float(values[i])

def build_ethbtc(btc,eth):
    b=btc[["date","close"]].rename(columns={"close":"btc_close"})
    e=eth[["date","close"]].rename(columns={"close":"eth_close"})
    x=b.merge(e,on="date")
    x["eth_btc"]=x["eth_close"]/x["btc_close"]
    for n in (25,50,100,200):
        x[f"ethbtc_sma{n}"]=x["eth_btc"].rolling(n,min_periods=n).mean()
        x[f"ethbtc_vs{n}"]=x["eth_btc"]/x[f"ethbtc_sma{n}"]-1
    return x

def build_monthly(total,ethbtc,u10_ts,equities):
    rows=[]
    total_ts=pd.DatetimeIndex(total["timestamp"])
    eth_ts=pd.DatetimeIndex(ethbtc["date"])
    for d in month_dates():
        tr=total.iloc[idx_on_or_before(total_ts,d)]
        er=ethbtc.iloc[idx_on_or_before(eth_ts,d)]
        row={"date":d,"month":d.strftime("%Y-%m"),
             "total_close":float(tr["close"]),
             "total_state":str(tr["total_state_calc"]),
             "ethbtc":float(er["eth_btc"])}
        for n in (50,100,200,300):
            row[f"total_vs_sma{n}"]=float(tr["close"]/tr[f"sma{n}_calc"]-1)
        for n in (25,50,100,200):
            row[f"ethbtc_vs_sma{n}"]=float(er[f"ethbtc_vs{n}"])
        for asset,eq in equities.items():
            row[f"eq_{asset}"]=sample(u10_ts,eq,d)
        rows.append(row)
    m=pd.DataFrame(rows)
    m["total_mom"]=m["total_close"].pct_change(fill_method=None)
    m["ethbtc_mom"]=m["ethbtc"].pct_change(fill_method=None)
    u10=[np.nan]
    for i in range(1,len(m)):
        vals=[m.iloc[i][f"eq_{a}"]/m.iloc[i-1][f"eq_{a}"]-1 for a in jointmod.U10]
        u10.append(float(np.median(vals)))
    m["u10_mom"]=u10
    m["class"]=np.where(m["u10_mom"]>=EXPLOSIVE,"EXPLOSIVE_MONTH",
                 np.where(m["u10_mom"]<=SEVERE,"SEVERE_DIP_MONTH","OTHER"))
    return m

def milestone_table(total,ethbtc):
    tt=pd.DatetimeIndex(total["timestamp"]); et=pd.DatetimeIndex(ethbtc["date"]); rows=[]
    for ds,label in MILESTONES:
        d=pd.Timestamp(ds,tz="UTC")
        tr=total.iloc[idx_on_or_before(tt,d)]
        er=ethbtc.iloc[idx_on_or_before(et,d)]
        row={"date":d,"label":label,"total_state":str(tr["total_state_calc"]),
             "total_close":float(tr["close"]),"ethbtc":float(er["eth_btc"])}
        for n in (25,50,100,200,300):
            row[f"total_vs_sma{n}"]=float(tr["close"]/tr[f"sma{n}_calc"]-1)
        for n in (25,50,100,200):
            row[f"ethbtc_vs_sma{n}"]=float(er[f"ethbtc_vs{n}"])
        rows.append(row)
    return pd.DataFrame(rows)

def pattern_summary(extremes):
    rows=[]
    for cls,g in extremes.groupby("class",sort=False):
        row={
          "class":cls,"months":int(len(g)),
          "total_positive_mom_rate":float((g["total_mom"]>0).mean()),
          "total_bull_state_rate":float(g["total_state"].isin(["BULL_BUILDING","FULL_BULL_ALIGNMENT"]).mean()),
          "ethbtc_positive_mom_rate":float((g["ethbtc_mom"]>0).mean()),
        }
        for n in (25,50,100,200):
            row[f"ethbtc_above_sma{n}_rate"]=float((g[f"ethbtc_vs_sma{n}"]>0).mean())
        rows.append(row)
    return pd.DataFrame(rows)

def pct(x):
    return "—" if pd.isna(x) else f"{100*float(x):+.1f}%"

def tval(x):
    return "$"+f"{float(x)/1e12:.2f}T"

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--cutoff",default="2026-09-26T00:00:00Z")
    args=ap.parse_args()
    if core.utc(args.cutoff).normalize()!=CUTOFF: raise RuntimeError("wrong cutoff")

    total=jointmod.load_total_cache()
    btc=jointmod.download_symbol("BTCUSDT",CUTOFF)
    eth=jointmod.download_symbol("ETHUSDT",CUTOFF)
    ethbtc=build_ethbtc(btc,eth)
    u10_ts,equities,data_meta=jointmod.build_u10_equities()
    monthly=build_monthly(total,ethbtc,u10_ts,equities)
    extremes=monthly[monthly["class"]!="OTHER"].copy()
    milestones=milestone_table(total,ethbtc)
    patterns=pattern_summary(extremes)

    run=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run.mkdir(parents=True,exist_ok=True)
    extremes.to_csv(run/"extreme_months.csv",index=False)
    milestones.to_csv(run/"cycle_milestones.csv",index=False)
    patterns.to_csv(run/"pattern_summary.csv",index=False)
    summary={
      "experiment":"RR_U10_EXTREMES_INDICATOR_OVERLAY_V1",
      "workflow_mode":"STRESS_TEST_ONLY","source_commit_sha":source_sha(),
      "cutoff":CUTOFF.isoformat(),
      "thresholds":{"explosive_month":EXPLOSIVE,"severe_dip_month":SEVERE},
      "explosive_count":int((extremes["class"]=="EXPLOSIVE_MONTH").sum()),
      "severe_count":int((extremes["class"]=="SEVERE_DIP_MONTH").sum()),
      "production_changes":"NONE",
      "test_level":"GITHUB_ACTIONS_REPOSITORY_CACHE_PLUS_LIVE_PUBLIC_DATA_STRESS_TEST",
      "data_metadata":data_meta
    }
    (run/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True),encoding="utf-8")

    lines=["# RR U10 EXTREMES + INDICATOR OVERLAY V1","","Mode: STRESS_TEST_ONLY","","## Extreme months","",
    "|Month|Class|U10|TOTAL MoM|TOTAL state|ETH/BTC MoM|ETH/BTC vs 25|vs 50|vs 100|vs 200|",
    "|---|---|---:|---:|---|---:|---:|---:|---:|---:|"]
    for _,r in extremes.sort_values("date").iterrows():
        lines.append(f"|{r['month']}|{r['class']}|{pct(r['u10_mom'])}|{pct(r['total_mom'])}|{r['total_state']}|"
                     f"{pct(r['ethbtc_mom'])}|{pct(r['ethbtc_vs_sma25'])}|{pct(r['ethbtc_vs_sma50'])}|"
                     f"{pct(r['ethbtc_vs_sma100'])}|{pct(r['ethbtc_vs_sma200'])}|")
    lines+=["","## Pattern summary","",
    "|Class|Months|TOTAL +MoM|TOTAL bull state|ETH/BTC +MoM|ETH/BTC >25|>50|>100|>200|",
    "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for _,r in patterns.iterrows():
        lines.append(f"|{r['class']}|{int(r['months'])}|{100*r['total_positive_mom_rate']:.1f}%|"
                     f"{100*r['total_bull_state_rate']:.1f}%|{100*r['ethbtc_positive_mom_rate']:.1f}%|"
                     f"{100*r['ethbtc_above_sma25_rate']:.1f}%|{100*r['ethbtc_above_sma50_rate']:.1f}%|"
                     f"{100*r['ethbtc_above_sma100_rate']:.1f}%|{100*r['ethbtc_above_sma200_rate']:.1f}%|")
    lines+=["","## Cycle milestones","",
    "|Date|Label|TOTAL state|TOTAL|T vs50|vs100|vs200|vs300|ETH/BTC vs25|vs50|vs100|vs200|",
    "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for _,r in milestones.iterrows():
        lines.append(f"|{pd.Timestamp(r['date']).date()}|{r['label']}|{r['total_state']}|{tval(r['total_close'])}|"
                     f"{pct(r['total_vs_sma50'])}|{pct(r['total_vs_sma100'])}|{pct(r['total_vs_sma200'])}|{pct(r['total_vs_sma300'])}|"
                     f"{pct(r['ethbtc_vs_sma25'])}|{pct(r['ethbtc_vs_sma50'])}|{pct(r['ethbtc_vs_sma100'])}|{pct(r['ethbtc_vs_sma200'])}|")
    lines+=["","TEST_LEVEL: GITHUB_ACTIONS_REPOSITORY_CACHE_PLUS_LIVE_PUBLIC_DATA_STRESS_TEST",""]
    report="\n".join(lines)
    (run/"report.md").write_text(report,encoding="utf-8")
    print(report)

if __name__=="__main__":
    main()

# workflow trigger: canonical extreme-month overlay v1
