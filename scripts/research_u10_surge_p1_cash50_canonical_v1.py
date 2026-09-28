from __future__ import annotations
import argparse, json
from pathlib import Path
import pandas as pd
import scripts.research_u10_monthly_surge_pullback_v1 as base

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/"research_artifacts"/"u10_surge_p1_cash50_canonical_v1"

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cutoff",default="2026-09-27T00:00:00Z")
    args=ap.parse_args()
    panel,meta=base.download_panel(base.utc(args.cutoff))
    events=base.build_events(panel)
    start=base.utc(panel.iloc[base.LOOKBACK-1]["timestamp"])
    baseline,ref=base.run_reference(panel,events,base.CANONICAL_U10,start,base.INITIAL_USDT)
    _,anchors=base.monthly_state(ref,base.INITIAL_USDT)
    control,cy30=base.run_overlay(ref,anchors,surge_threshold=1.0,pullback_threshold=0.05,reentry_threshold=0.25,cash_fraction=0.30,initial_usdt=base.INITIAL_USDT)
    cand,cy50=base.run_overlay(ref,anchors,surge_threshold=1.0,pullback_threshold=0.05,reentry_threshold=0.25,cash_fraction=0.50,initial_usdt=base.INITIAL_USDT)
    d={
      "baseline":baseline,
      "cash30":control,
      "cash50":cand,
      "delta_usdt":cand["final_equity_usdt"]-control["final_equity_usdt"],
      "delta_pct":cand["final_equity_usdt"]/control["final_equity_usdt"]-1,
      "cash50_vs_baseline_pct":cand["final_equity_usdt"]/baseline["final_equity_usdt"]-1,
      "data_metadata":meta
    }
    out=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    out.mkdir(parents=True,exist_ok=True)
    (out/"results.json").write_text(json.dumps(d,indent=2,default=str),encoding="utf-8")
    pd.concat([
      cy30.assign(variant="cash30"),
      cy50.assign(variant="cash50")
    ],ignore_index=True).to_csv(out/"cycles.csv",index=False)
    print(json.dumps({
      "baseline":baseline["final_equity_usdt"],
      "cash30":control["final_equity_usdt"],
      "cash50":cand["final_equity_usdt"],
      "delta_usdt":d["delta_usdt"],
      "delta_pct":d["delta_pct"],
      "baseline_dd":baseline["max_drawdown"],
      "cash30_dd":control["max_drawdown"],
      "cash50_dd":cand["max_drawdown"],
      "cash30_days":control["days_in_cash"],
      "cash50_days":cand["days_in_cash"],
      "cash30_cashouts":control["cashouts"],
      "cash50_cashouts":cand["cashouts"],
      "cash30_reentries":control["reentries"],
      "cash50_reentries":cand["reentries"]
    },indent=2))

if __name__=="__main__":
    main()
