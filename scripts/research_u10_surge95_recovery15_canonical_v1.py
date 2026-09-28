from __future__ import annotations
import argparse, json
from pathlib import Path
import pandas as pd

import scripts.research_u10_monthly_surge_fractal_buystop_v1 as fractal
from scripts.research_u10_surge95_recovery15_v1 import run_recovery_overlay, SURGE_THRESHOLD, P1_PULLBACK
from scripts.research_u10_monthly_surge_pullback_v1 import CANONICAL_U10, INITIAL_USDT, monthly_state, run_reference, source_sha
from scripts.research_u10_last_year_vs_u8_universes_v1 import LOOKBACK, utc

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/"research_artifacts"/"u10_surge95_recovery15_canonical_v1"

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--cutoff",default="2026-09-27T00:00:00Z"); args=ap.parse_args()
    panel,meta=fractal.download_ohlc_panel(utc(args.cutoff))
    event_map=fractal.build_events(panel)
    mature=utc(panel.iloc[LOOKBACK-1]["timestamp"])
    panel_eval=panel[panel["timestamp"]>=mature].copy().reset_index(drop=True)
    baseline,reference=run_reference(panel,event_map,CANONICAL_U10,mature,INITIAL_USDT)
    reference=reference.reset_index(drop=True)
    _,anchors=monthly_state(reference,INITIAL_USDT)
    control,cc=fractal.run_fractal_overlay(panel_eval,reference,anchors,surge_threshold=SURGE_THRESHOLD,cashout_pullback=P1_PULLBACK,initial_usdt=INITIAL_USDT)
    candidate,cy=run_recovery_overlay(panel_eval,reference,anchors)
    out={
      "source_commit":source_sha(),"baseline":baseline,"control":control,"candidate":candidate,
      "delta_candidate_vs_control_pct":candidate["final_equity_usdt"]/control["final_equity_usdt"]-1,
      "delta_candidate_vs_baseline_pct":candidate["final_equity_usdt"]/baseline["final_equity_usdt"]-1,
      "cycles":cy.where(pd.notna(cy), None).to_dict(orient="records")
    }
    run_dir=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ"); run_dir.mkdir(parents=True,exist_ok=True)
    (run_dir/"results.json").write_text(json.dumps(out,indent=2,sort_keys=True,allow_nan=True),encoding="utf-8")
    if not cy.empty: cy.to_csv(run_dir/"candidate_cycles.csv",index=False)
    if not cc.empty: cc.to_csv(run_dir/"control_cycles.csv",index=False)
    print("run_dir="+str(run_dir))
    print("baseline=%.8f control=%.8f candidate=%.8f dcontrol=%.8f dbaseline=%.8f dd=%.8f cash=%d fractal=%d recovery=%d immediate=%d cross=%d unfinished=%d"%(
      baseline["final_equity_usdt"],control["final_equity_usdt"],candidate["final_equity_usdt"],out["delta_candidate_vs_control_pct"],out["delta_candidate_vs_baseline_pct"],candidate["max_drawdown"],candidate["days_in_cash"],candidate["fractal_reentries"],candidate["recovery_reentries"],candidate["recovery_immediate"],candidate["recovery_cross"],candidate["unfinished_cycles"]))
    return 0
if __name__=="__main__": raise SystemExit(main())
