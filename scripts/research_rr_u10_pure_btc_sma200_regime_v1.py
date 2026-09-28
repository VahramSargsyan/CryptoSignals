from __future__ import annotations

import argparse, json, os, subprocess
from pathlib import Path
import numpy as np
import pandas as pd

import scripts.research_robust_exhaustive_u9_u10_atom_replacement_v1 as core
from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "rr_u10_pure_btc_sma200_regime_v1"
U10 = ("TWT","PEPE","BNB","TRX","AAVE","AVAX","FIL","ALGO","XRP","HBAR")
MATURE_START = pd.Timestamp("2023-10-31", tz="UTC")
EVAL_END = pd.Timestamp("2026-09-26", tz="UTC")
BTC_WARMUP_START = pd.Timestamp("2022-01-01", tz="UTC")

def source_sha():
    return os.environ.get("SOURCE_COMMIT_SHA") or subprocess.check_output(
        ["git","rev-parse","HEAD"], cwd=ROOT, text=True
    ).strip()

def download_btc(cutoff):
    result = download_historical_dataset(
        BinanceSpotRestClient(), symbol="BTCUSDT",
        start=BTC_WARMUP_START, end=cutoff, timeframe="1D", as_of=cutoff
    )
    if result.dataset is None:
        raise RuntimeError(f"BTCUSDT: {result.metadata.status}")
    if result.dataset.quality.has_critical_issues:
        raise RuntimeError("BTCUSDT: critical data quality issues")
    f=result.dataset.candles[["timestamp","close"]].copy()
    f["timestamp"]=pd.to_datetime(f["timestamp"],utc=True)
    f["btc_close"]=f["close"].astype(float)
    f=f.drop(columns=["close"]).sort_values("timestamp").reset_index(drop=True)
    return f

def build_regime(panel, btc):
    ts=pd.DatetimeIndex(pd.to_datetime(panel["timestamp"],utc=True))
    b=btc.set_index("timestamp").copy()
    b["btc_sma200"]=b["btc_close"].rolling(200).mean()
    b["btc_daily_return"]=b["btc_close"].pct_change(fill_method=None)
    b["regime"]="UNKNOWN"
    ready=b["btc_sma200"].notna()
    b.loc[ready & (b["btc_close"] >= b["btc_sma200"]),"regime"]="BTC_ABOVE_SMA200"
    b.loc[ready & (b["btc_close"] < b["btc_sma200"]),"regime"]="BTC_BELOW_SMA200"
    b=b.reindex(ts)
    return pd.DataFrame({
        "timestamp":ts,
        "btc_close":b["btc_close"].to_numpy(),
        "btc_sma200":b["btc_sma200"].to_numpy(),
        "btc_daily_return":b["btc_daily_return"].to_numpy(),
        "regime":b["regime"].fillna("UNKNOWN").to_numpy(),
    })

def conditioned(daily, mask):
    daily=np.asarray(daily,float); mask=np.asarray(mask,bool)
    vals=daily[mask]; vals=vals[np.isfinite(vals)]
    if not len(vals):
        return dict(day_count=0, conditioned_return=np.nan, conditioned_dd=np.nan,
                    avg_daily_return=np.nan, median_daily_return=np.nan,
                    positive_day_rate=np.nan)
    c=np.where(mask,daily,0.0); c=np.where(np.isfinite(c),c,0.0)
    eq=np.cumprod(1+c); peak=np.maximum.accumulate(eq)
    return dict(
        day_count=int(len(vals)),
        conditioned_return=float(np.prod(1+vals)-1),
        conditioned_dd=float(np.min(eq/peak-1)),
        avg_daily_return=float(np.mean(vals)),
        median_daily_return=float(np.median(vals)),
        positive_day_rate=float(np.mean(vals>0)),
    )

def episodes(daily, labels, target):
    daily=np.asarray(daily,float); labels=np.asarray(labels,object)
    rets=[]; lengths=[]; i=0
    while i<len(labels):
        if labels[i]!=target:
            i+=1; continue
        j=i
        while j+1<len(labels) and labels[j+1]==target:
            j+=1
        vals=daily[i:j+1]; vals=vals[np.isfinite(vals)]
        if len(vals):
            rets.append(float(np.prod(1+vals)-1)); lengths.append(j-i+1)
        i=j+1
    if not rets:
        return dict(episode_count=0, median_episode_return=np.nan,
                    worst_episode_return=np.nan, best_episode_return=np.nan,
                    positive_episode_rate=np.nan, longest_episode_days=0)
    a=np.asarray(rets,float)
    return dict(episode_count=len(rets), median_episode_return=float(np.median(a)),
                worst_episode_return=float(np.min(a)), best_episode_return=float(np.max(a)),
                positive_episode_rate=float(np.mean(a>0)), longest_episode_days=max(lengths))

def compound(vals):
    a=np.asarray(vals,float); a=a[np.isfinite(a)]
    return float(np.prod(1+a)-1) if len(a) else np.nan

def pct(x):
    return "n/a" if pd.isna(x) else f"{100*float(x):+.1f}%"

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--cutoff",default="2026-09-27T00:00:00Z")
    args=ap.parse_args()
    core.POOL=U10
    panel,data_meta=core.download_panel(core.utc(args.cutoff))
    timestamps,opens,closes=core.prepare_arrays(panel)
    sigpos,sigcand=core.build_signal_index(panel)
    regime=build_regime(panel,download_btc(core.utc(args.cutoff)))
    start_i,end_i=core.bounds(timestamps,MATURE_START,EVAL_END)
    local=regime.iloc[start_i:end_i+1].reset_index(drop=True)
    labels=local["regime"].astype(str).to_numpy()
    rows=[]; daily_rows=[]
    for start_asset in U10:
        res=core.simulate_one(opens,closes,sigpos,sigcand,U10,start_i,end_i,start_asset,collect_equity=True)
        eq=np.asarray(res["equity"],float)
        dr=np.zeros(len(eq),float)
        if len(eq)>1: dr[1:]=eq[1:]/eq[:-1]-1
        for rg in ("BTC_ABOVE_SMA200","BTC_BELOW_SMA200"):
            mask=labels==rg
            row={"start_asset":start_asset,"regime":rg,**conditioned(dr,mask),**episodes(dr,labels,rg)}
            row["btc_same_days_return"]=compound(local.loc[mask,"btc_daily_return"].to_numpy())
            rows.append(row)
            for idx,flag in enumerate(mask):
                daily_rows.append({
                    "timestamp":pd.Timestamp(local.iloc[idx]["timestamp"]).isoformat(),
                    "year":int(pd.Timestamp(local.iloc[idx]["timestamp"]).year),
                    "start_asset":start_asset,"regime":rg,"is_regime":bool(flag),
                    "strategy_daily_return":float(dr[idx]),
                })
    by_start=pd.DataFrame(rows)
    agg=[]
    for rg,g in by_start.groupby("regime",sort=False):
        agg.append({
            "regime":rg,
            "day_count":int(g["day_count"].iloc[0]),
            "episode_count":int(g["episode_count"].iloc[0]),
            "median_start_return":float(g["conditioned_return"].median()),
            "worst_start_return":float(g["conditioned_return"].min()),
            "best_start_return":float(g["conditioned_return"].max()),
            "positive_start_rate":float((g["conditioned_return"]>0).mean()),
            "median_start_dd":float(g["conditioned_dd"].median()),
            "median_episode_return":float(g["median_episode_return"].median()),
            "worst_episode_return":float(g["worst_episode_return"].min()),
            "best_episode_return":float(g["best_episode_return"].max()),
            "positive_episode_rate":float(g["positive_episode_rate"].median()),
            "longest_episode_days":int(g["longest_episode_days"].max()),
            "btc_same_days_return":float(g["btc_same_days_return"].iloc[0]),
        })
    aggregate=pd.DataFrame(agg)

    daily_df=pd.DataFrame(daily_rows)
    yearly=[]
    for (rg,year),g in daily_df.groupby(["regime","year"],sort=False):
        vals=[]
        for _,sg in g.groupby("start_asset"):
            x=sg.loc[sg["is_regime"],"strategy_daily_return"].to_numpy(float)
            if len(x): vals.append(float(np.prod(1+x)-1))
        if vals:
            a=np.asarray(vals,float)
            yearly.append({"regime":rg,"year":int(year),"median_start_return":float(np.median(a)),
                           "worst_start_return":float(np.min(a)),"best_start_return":float(np.max(a)),
                           "positive_start_rate":float(np.mean(a>0))})
    yearly=pd.DataFrame(yearly)

    run_dir=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ"); run_dir.mkdir(parents=True,exist_ok=True)
    by_start.to_csv(run_dir/"regime_by_start.csv",index=False)
    aggregate.to_csv(run_dir/"regime_aggregate.csv",index=False)
    yearly.to_csv(run_dir/"regime_by_year.csv",index=False)
    summary={
        "experiment":"RR_U10_PURE_BTC_SMA200_REGIME_V1",
        "workflow_mode":"STRESS_TEST_ONLY",
        "source_commit_sha":source_sha(),
        "frozen_universe_id":"RR_TARGET_U10_CANDIDATE_HBAR_V1",
        "assets":list(U10),
        "mature_start":MATURE_START.isoformat(),"end":EVAL_END.isoformat(),
        "regime_definition":"BTC close >=/< trailing SMA200",
        "aggregate":json.loads(aggregate.to_json(orient="records")),
        "yearly":json.loads(yearly.to_json(orient="records")),
        "data_metadata":data_meta,
        "production_changes":"NONE",
        "test_level":"GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    }
    (run_dir/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True),encoding="utf-8")
    out=[
        "# RR U10 PURE BTC SMA200 REGIME V1","",
        "Mode: STRESS_TEST_ONLY","",
        "|regime|days|episodes|strategy median|worst start|positive starts|conditioned DD|median episode|positive episodes|BTC same days|",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
    ]
    for _,r in aggregate.iterrows():
        out.append(f"|{r.regime}|{int(r.day_count)}|{int(r.episode_count)}|{pct(r.median_start_return)}|"
                   f"{pct(r.worst_start_return)}|{100*r.positive_start_rate:.1f}%|{pct(r.median_start_dd)}|"
                   f"{pct(r.median_episode_return)}|{100*r.positive_episode_rate:.1f}%|{pct(r.btc_same_days_return)}|")
    out += ["","## Safety","",
            "- Pure BTC SMA200 only; no SMA50 confirmation.",
            "- Historical regime attribution, not a forecast.",
            "- No production/paper-live/Telegram/exchange changes.","",
            "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",""]
    report="\n".join(out); (run_dir/"report.md").write_text(report,encoding="utf-8"); print(report)

if __name__=="__main__":
    main()
