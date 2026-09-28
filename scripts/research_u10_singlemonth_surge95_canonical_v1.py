from __future__ import annotations
import argparse, json
from pathlib import Path
import pandas as pd
import scripts.research_u10_monthly_surge_fractal_buystop_v1 as fractal
from scripts.research_u10_monthly_surge_pullback_v1 import CANONICAL_U10, INITIAL_USDT, monthly_state, run_reference, source_sha
from scripts.research_u10_singlemonth_surge95_v1 import event_rows, summarize_events
from scripts.research_u10_last_year_vs_u8_universes_v1 import LOOKBACK, utc

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/"research_artifacts"/"u10_singlemonth_surge95_canonical_v1"

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--cutoff",default="2026-09-27T00:00:00Z"); args=ap.parse_args()
    panel,meta=fractal.download_ohlc_panel(utc(args.cutoff))
    events=fractal.build_events(panel)
    mature=utc(panel.iloc[LOOKBACK-1]["timestamp"])
    panel_eval=panel[panel["timestamp"]>=mature].copy().reset_index(drop=True)
    baseline,reference=run_reference(panel,events,CANONICAL_U10,mature,INITIAL_USDT)
    reference=reference.reset_index(drop=True)
    monthly,anchors=monthly_state(reference,INITIAL_USDT)
    e95=event_rows(reference,monthly,.95); e100=event_rows(reference,monthly,1.0)
    inc=e95[e95["selected_change"]<1.0].copy()
    payload={"source_commit":source_sha(),"baseline":baseline,"events95":e95.to_dict(orient="records"),"events100":e100.to_dict(orient="records"),"incremental":inc.to_dict(orient="records"),"event_summary":{"ge95":summarize_events(e95),"ge100":summarize_events(e100),"incremental":summarize_events(inc)},"variants":{}}
    frames=[]
    for label,pb in (("P1",.05),("P2",.10)):
        s100,_=fractal.run_fractal_overlay(panel_eval,reference,anchors,surge_threshold=1.0,cashout_pullback=pb,initial_usdt=INITIAL_USDT)
        s95,cy=fractal.run_fractal_overlay(panel_eval,reference,anchors,surge_threshold=.95,cashout_pullback=pb,initial_usdt=INITIAL_USDT)
        payload["variants"][label]={"s100":s100,"s95":s95,"delta95_vs_baseline_pct":s95["final_equity_usdt"]/baseline["final_equity_usdt"]-1,"delta95_vs100_pct":s95["final_equity_usdt"]/s100["final_equity_usdt"]-1}
        if not cy.empty:
            cc=cy.copy(); cc.insert(0,"variant",label); frames.append(cc)
    run_dir=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ"); run_dir.mkdir(parents=True,exist_ok=True)
    (run_dir/"results.json").write_text(json.dumps(payload,indent=2,sort_keys=True,allow_nan=False),encoding="utf-8")
    if frames: pd.concat(frames,ignore_index=True).to_csv(run_dir/"cycles.csv",index=False)
    print("run_dir="+str(run_dir)); print("EVENTS",payload["event_summary"])
    for label in ("P1","P2"):
        v=payload["variants"][label]
        print("%s base=%.8f s100=%.8f s95=%.8f db=%.8f d100=%.8f dd95=%.8f cash95=%d re95=%d"%(
            label,baseline["final_equity_usdt"],v["s100"]["final_equity_usdt"],v["s95"]["final_equity_usdt"],v["delta95_vs_baseline_pct"],v["delta95_vs100_pct"],v["s95"]["max_drawdown"],v["s95"]["days_in_cash"],v["s95"]["reentries"]))
    return 0
if __name__=="__main__": raise SystemExit(main())
