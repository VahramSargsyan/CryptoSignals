from __future__ import annotations

import hashlib
from pathlib import Path
import numpy as np
import pandas as pd

SRC = Path("data/market/cryptocap_total_d1.csv")
OUT = Path("research_artifacts/total_mcap_trend_shape_v2")
EXPECTED = "d0c9b3bac915e484069c6302ddce1a0b050abbb8de155e4106e39cf6e519d3ee"
THRESHOLDS = (0.10,0.15,0.20,0.25,0.30,0.40)

def median(s):
    s=pd.Series(s).dropna()
    return float(s.median()) if len(s) else np.nan

def pct(g,label):
    return float((g["outcome"]==label).mean()*100) if len(g) else np.nan

def main():
    if hashlib.sha256(SRC.read_bytes()).hexdigest()!=EXPECTED:
        raise RuntimeError("dataset hash mismatch")
    f=pd.read_csv(SRC)
    f["timestamp"]=pd.to_datetime(f["timestamp"],utc=True)
    f=f.sort_values("timestamp").reset_index(drop=True)
    for w in (25,50,100):
        f[f"sma{w}"]=f["close"].rolling(w,min_periods=w).mean()
        f[f"slope{w}"]=f[f"sma{w}"]/f[f"sma{w}"].shift(5)-1
    f["dist100"]=f["close"]/f["sma100"]-1
    f["fan"]=(f["sma25"]-f["sma100"])/f["sma100"]
    f["fan_delta5"]=f["fan"]-f["fan"].shift(5)

    def regime(r):
        bull=r["sma25"]>r["sma50"]>r["sma100"]
        rising=all(r[f"slope{w}"]>0 for w in (25,50,100))
        if bull and rising and r["fan_delta5"]>0:
            return "POWERED_TREND"
        if bull and rising and r["fan_delta5"]<=0:
            return "CATCHUP_PHASE"
        return "TRANSITION_MIXED"

    episodes=[]
    first=int(f["sma100"].first_valid_index())
    for th in THRESHOLDS:
        active=False
        prev=first
        for i in range(first+1,len(f)):
            gap=float(f.at[i,"dist100"])
            prev_gap=float(f.at[prev,"dist100"])
            if active and gap<=th*0.5:
                active=False
            if (not active) and prev_gap<th<=gap:
                active=True
                t0=f.at[i,"timestamp"]
                end=t0+pd.Timedelta(days=90)
                future=f[(f["timestamp"]>t0)&(f["timestamp"]<=end)]
                ret=br=touch=None
                max_gap=gap
                max_ts=t0
                for j,r in future.iterrows():
                    g=float(r["dist100"])
                    if g>max_gap:
                        max_gap=g; max_ts=r["timestamp"]
                    if ret is None and g<=gap*0.5: ret=r["timestamp"]
                    if br is None and g>=gap*1.25: br=r["timestamp"]
                    if touch is None and g<=0: touch=r["timestamp"]
                if ret is not None and br is not None:
                    outcome="RETURN_FIRST" if ret<br else "BREAKOUT_FIRST" if br<ret else "SAME_DAY"
                elif ret is not None: outcome="RETURN_FIRST"
                elif br is not None: outcome="BREAKOUT_FIRST"
                else: outcome="CENSORED" if f.iloc[-1]["timestamp"]<end else "UNRESOLVED_90D"
                row=f.iloc[i]
                episodes.append({
                    "threshold_pct":th*100,
                    "trigger_date":t0.date().isoformat(),
                    "trigger_distance_pct":gap*100,
                    "regime":regime(row),
                    "sma25_slope5_pct":row["slope25"]*100,
                    "sma50_slope5_pct":row["slope50"]*100,
                    "sma100_slope5_pct":row["slope100"]*100,
                    "fan_spread_pct":row["fan"]*100,
                    "fan_delta5_pp":row["fan_delta5"]*100,
                    "outcome":outcome,
                    "extra_extension_pp":(max_gap-gap)*100,
                    "days_to_max":(max_ts-t0).days,
                    "days_to_return50":(ret-t0).days if ret is not None else np.nan,
                    "days_to_touch":(touch-t0).days if touch is not None else np.nan,
                })
            prev=i

    e=pd.DataFrame(episodes)
    rows=[]
    for (th,reg),g in e.groupby(["threshold_pct","regime"],sort=True):
        rows.append({
            "threshold_pct":th,"regime":reg,"n":len(g),
            "return_first_pct":pct(g,"RETURN_FIRST"),
            "breakout_first_pct":pct(g,"BREAKOUT_FIRST"),
            "unresolved_pct":float(g["outcome"].isin(["UNRESOLVED_90D","CENSORED"]).mean()*100),
            "median_extra_extension_pp":median(g["extra_extension_pp"]),
            "median_days_to_return50":median(g["days_to_return50"]),
            "median_days_to_touch":median(g["days_to_touch"]),
            "median_fan_delta5_pp":median(g["fan_delta5_pp"]),
        })
    summary=pd.DataFrame(rows)

    a=e[e["threshold_pct"]<=30]
    rows=[]
    for reg,g in a.groupby("regime",sort=True):
        rows.append({
            "regime":reg,"n":len(g),
            "return_first_pct":pct(g,"RETURN_FIRST"),
            "breakout_first_pct":pct(g,"BREAKOUT_FIRST"),
            "unresolved_pct":float(g["outcome"].isin(["UNRESOLVED_90D","CENSORED"]).mean()*100),
            "median_extra_extension_pp":median(g["extra_extension_pp"]),
            "median_days_to_return50":median(g["days_to_return50"]),
            "median_days_to_touch":median(g["days_to_touch"]),
        })
    aggregate=pd.DataFrame(rows)

    latest=f.iloc[-1]
    current=pd.DataFrame([{
        "date":latest["timestamp"].date().isoformat(),
        "distance100_pct":latest["dist100"]*100,
        "sma25_slope5_pct":latest["slope25"]*100,
        "sma50_slope5_pct":latest["slope50"]*100,
        "sma100_slope5_pct":latest["slope100"]*100,
        "fan_spread_pct":latest["fan"]*100,
        "fan_delta5_pp":latest["fan_delta5"]*100,
        "regime":regime(latest),
    }])

    OUT.mkdir(parents=True,exist_ok=True)
    e.to_csv(OUT/"episodes.csv",index=False)
    summary.to_csv(OUT/"summary.csv",index=False)
    aggregate.to_csv(OUT/"aggregate.csv",index=False)
    current.to_csv(OUT/"current.csv",index=False)
    print("EPISODES",len(e))
    print("SUMMARY_BEGIN")
    print(summary.to_csv(index=False))
    print("SUMMARY_END")
    print("AGGREGATE_BEGIN")
    print(aggregate.to_csv(index=False))
    print("AGGREGATE_END")
    print("CURRENT_BEGIN")
    print(current.to_csv(index=False))
    print("CURRENT_END")

if __name__=="__main__":
    main()
