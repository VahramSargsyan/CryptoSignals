from __future__ import annotations
import argparse, json
from pathlib import Path
import pandas as pd

import scripts.research_u10_monthly_surge_fractal_buystop_v1 as fractal
from scripts.research_u10_streak95_trendbreak_v1 import attach_breaks, run_trendbreak_overlay
from scripts.research_u10_multimonth_streak95_v1 import P1, P2, build_streak_events, run_streak_overlay
from scripts.research_u10_monthly_surge_pullback_v1 import CANONICAL_U10, INITIAL_USDT, monthly_state, run_reference, source_sha
from scripts.research_u10_last_year_vs_u8_universes_v1 import LOOKBACK, utc

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/"research_artifacts"/"u10_streak95_trendbreak_canonical_v1"

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cutoff",default="2026-09-27T00:00:00Z")
    args=ap.parse_args()
    panel,meta=fractal.download_ohlc_panel(utc(args.cutoff))
    event_map=fractal.build_events(panel)
    mature=utc(panel.iloc[LOOKBACK-1]["timestamp"])
    panel_eval=panel[panel["timestamp"]>=mature].copy().reset_index(drop=True)
    baseline,reference=run_reference(panel,event_map,CANONICAL_U10,mature,INITIAL_USDT)
    reference=reference.reset_index(drop=True)
    monthly,_=monthly_state(reference,INITIAL_USDT)
    raw=build_streak_events(reference,monthly)
    events=attach_breaks(reference,monthly,raw)

    payload={"source_commit":source_sha(),"baseline":baseline,"events":events,"variants":{}}
    frames=[]
    for label,cfg in (("P1",P1),("P2",P2)):
        orig,_=run_streak_overlay(panel_eval,reference,raw,cashout_pullback=cfg["pullback"])
        tb,cycles=run_trendbreak_overlay(panel_eval,reference,events,cashout_pullback=cfg["pullback"])
        payload["variants"][label]={"original":orig,"trendbreak":tb,
            "tb_delta_vs_baseline_pct":tb["final_equity_usdt"]/baseline["final_equity_usdt"]-1,
            "tb_delta_vs_original_pct":tb["final_equity_usdt"]/orig["final_equity_usdt"]-1}
        if not cycles.empty:
            cc=cycles.copy(); cc.insert(0,"variant",label); frames.append(cc)

    run_dir=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True,exist_ok=True)
    (run_dir/"results.json").write_text(json.dumps(payload,indent=2,sort_keys=True,allow_nan=False),encoding="utf-8")
    if frames: pd.concat(frames,ignore_index=True).to_csv(run_dir/"cycles.csv",index=False)

    print("run_dir="+str(run_dir))
    print("baseline=%.8f dd=%.8f"%(baseline["final_equity_usdt"],baseline["max_drawdown"]))
    for label in ("P1","P2"):
        v=payload["variants"][label]
        t=v["trendbreak"]
        print("%s orig=%.8f tb=%.8f db=%.8f do=%.8f dd=%.8f cashouts=%d re=%d watch=%d cash=%d uw=%d uc=%d"%(
            label,v["original"]["final_equity_usdt"],t["final_equity_usdt"],v["tb_delta_vs_baseline_pct"],v["tb_delta_vs_original_pct"],t["max_drawdown"],t["cashouts"],t["reentries"],t["watch_days"],t["days_in_cash"],t["unfinished_watch"],t["unfinished_cash_cycles"]))
    return 0

if __name__=="__main__": raise SystemExit(main())
