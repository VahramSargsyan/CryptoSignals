from __future__ import annotations
import argparse
import json
from pathlib import Path
import pandas as pd

from scripts.research_u10_monthly_surge_fractal_buystop_v1 import (
    P1, P2, ROOT, download_ohlc_panel, build_events, run_fractal_overlay
)
from scripts.research_u10_last_year_vs_u8_universes_v1 import LOOKBACK, utc
from scripts.research_u10_monthly_surge_pullback_v1 import (
    CANONICAL_U10, CASH_FRACTION, INITIAL_USDT, monthly_state,
    run_overlay, run_reference, source_sha
)
from scripts.research_u10_monthly_surge_trailing_reentry_peak_v1 import (
    run_overlay_trailing_peak
)

OUT = ROOT / "research_artifacts" / "u10_fractal_canonical_check_v1"

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cutoff",default="2026-09-27T00:00:00Z")
    args=ap.parse_args()

    panel,meta=download_ohlc_panel(utc(args.cutoff))
    events=build_events(panel)
    mature_start=utc(panel.iloc[LOOKBACK-1]["timestamp"])
    panel_eval=panel[panel["timestamp"]>=mature_start].copy().reset_index(drop=True)

    baseline,reference=run_reference(
        panel,events,CANONICAL_U10,mature_start,INITIAL_USDT
    )
    reference=reference.reset_index(drop=True)
    _,anchors=monthly_state(reference,INITIAL_USDT)

    out={"source_commit":source_sha(),"mature_start":mature_start.isoformat(),"baseline":baseline,"variants":{}}
    cycle_frames=[]

    for label,cfg in (("P1",P1),("P2",P2)):
        fixed25,_=run_overlay(
            reference,anchors,
            surge_threshold=cfg["surge"],
            pullback_threshold=cfg["pullback"],
            reentry_threshold=0.25,
            cash_fraction=CASH_FRACTION,
            initial_usdt=INITIAL_USDT,
        )
        trailing25,_=run_overlay_trailing_peak(
            reference,anchors,
            surge_threshold=cfg["surge"],
            pullback_threshold=cfg["pullback"],
            reentry_threshold=0.25,
            cash_fraction=CASH_FRACTION,
            initial_usdt=INITIAL_USDT,
        )
        fractal,cycles=run_fractal_overlay(
            panel_eval,reference,anchors,
            surge_threshold=cfg["surge"],
            cashout_pullback=cfg["pullback"],
            initial_usdt=INITIAL_USDT,
        )
        out["variants"][label]={
            "fixed25":fixed25,
            "trailing25":trailing25,
            "fractal":fractal,
            "fractal_delta_vs_baseline_pct":fractal["final_equity_usdt"]/baseline["final_equity_usdt"]-1,
            "fractal_delta_vs_fixed25_pct":fractal["final_equity_usdt"]/fixed25["final_equity_usdt"]-1,
            "fractal_delta_vs_trailing25_pct":fractal["final_equity_usdt"]/trailing25["final_equity_usdt"]-1,
        }
        if not cycles.empty:
            cc=cycles.copy()
            cc.insert(0,"variant",label)
            cycle_frames.append(cc)

    run_dir=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True,exist_ok=True)
    (run_dir/"results.json").write_text(json.dumps(out,indent=2,sort_keys=True,allow_nan=False),encoding="utf-8")
    if cycle_frames:
        pd.concat(cycle_frames,ignore_index=True).to_csv(run_dir/"cycles.csv",index=False)

    print("run_dir="+str(run_dir))
    print("baseline_final=%.8f baseline_dd=%.8f"%(baseline["final_equity_usdt"],baseline["max_drawdown"]))
    for label in ("P1","P2"):
        v=out["variants"][label]
        f=v["fractal"]
        print("%s fractal=%.8f delta_base=%.8f delta_fixed=%.8f delta_trailing=%.8f dd=%.8f cash=%d re=%d unfinished=%d pivots=%d repl=%d gap=%d stop=%d"%(
            label,f["final_equity_usdt"],v["fractal_delta_vs_baseline_pct"],v["fractal_delta_vs_fixed25_pct"],v["fractal_delta_vs_trailing25_pct"],f["max_drawdown"],f["days_in_cash"],f["reentries"],f["unfinished_cycles"],f["confirmed_pivots"],f["stop_replacements"],f["gap_open_fills"],f["stop_price_fills"]
        ))
    return 0

if __name__=="__main__":
    raise SystemExit(main())
