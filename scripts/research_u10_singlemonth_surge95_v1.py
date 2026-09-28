from __future__ import annotations
import argparse, json
from pathlib import Path
import pandas as pd

import scripts.research_u10_monthly_surge_fractal_buystop_v1 as fractal
from scripts.research_u10_monthly_surge_pullback_v1 import (
    CANONICAL_U10, INITIAL_USDT, all_u10_universes, monthly_state,
    run_reference, source_sha, universe_key,
)
from scripts.research_u10_last_year_vs_u8_universes_v1 import LOOKBACK, utc

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/"research_artifacts"/"u10_singlemonth_surge95_v1"
TH95=0.95
TH100=1.00

def event_rows(reference, monthly, threshold):
    rows=[]
    for _,e in monthly[monthly["selected_change"]>=threshold].iterrows():
        start=utc(e["selected_date"])
        initial_peak=float(e["selected_equity_usdt"])
        row={
            "month":str(e["month"]),
            "selected_change":float(e["selected_change"]),
            "event_date":start.isoformat(),
            "event_equity_usdt":initial_peak,
        }
        for h in (31,62,93):
            end=start+pd.Timedelta(days=h)
            w=reference[(reference["timestamp"]>=start)&(reference["timestamp"]<=end)]
            peak=initial_peak; worst=0.0
            for _,r in w.iterrows():
                v=float(r["equity_usdt"])
                if v>peak: peak=v
                dd=v/peak-1.0
                if dd<worst: worst=dd
            row[f"worst_dd_{h}d"]=worst
            row[f"hit25_{h}d"]=bool(worst<=-0.25)
        rows.append(row)
    return pd.DataFrame(rows)

def summarize_events(df):
    if df.empty:
        return {"count":0}
    out={"count":int(len(df))}
    for h in (31,62,93):
        out[f"hit25_{h}d"]=float(df[f"hit25_{h}d"].mean())
        out[f"median_dd_{h}d"]=float(df[f"worst_dd_{h}d"].median())
    return out

def summarize_alt(df):
    a=df[~df["is_canonical"]].copy()
    return {
        "count":int(len(a)),
        "final_gt_baseline_rate":float((a["s95_delta_vs_baseline_usdt"]>0).mean()),
        "final_gt_s100_rate":float((a["s95_delta_vs_s100_usdt"]>0).mean()),
        "dd_better_rate":float((a["s95_dd_improvement_pp"]>0).mean()),
        "both_better_rate":float(((a["s95_delta_vs_baseline_usdt"]>0)&(a["s95_dd_improvement_pp"]>0)).mean()),
        "unfinished_rate":float((a["s95_unfinished_cycles"]>0).mean()),
        "median_delta_vs_baseline_pct":float(a["s95_delta_vs_baseline_pct"].median()),
        "q25_delta_vs_baseline_pct":float(a["s95_delta_vs_baseline_pct"].quantile(.25)),
        "q75_delta_vs_baseline_pct":float(a["s95_delta_vs_baseline_pct"].quantile(.75)),
        "median_delta_vs_s100_pct":float(a["s95_delta_vs_s100_pct"].median()),
        "q25_delta_vs_s100_pct":float(a["s95_delta_vs_s100_pct"].quantile(.25)),
        "q75_delta_vs_s100_pct":float(a["s95_delta_vs_s100_pct"].quantile(.75)),
    }

def pct(x):
    return "n/a" if x is None else f"{100*x:+.2f}%"

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cutoff",default="2026-09-27T00:00:00Z")
    args=ap.parse_args()

    panel,meta=fractal.download_ohlc_panel(utc(args.cutoff))
    events=fractal.build_events(panel)
    mature=utc(panel.iloc[LOOKBACK-1]["timestamp"])
    panel_eval=panel[panel["timestamp"]>=mature].copy().reset_index(drop=True)

    canonical_key=universe_key(CANONICAL_U10)
    rows=[]; event_frames=[]; cycles=[]; canonical={}

    for idx,assets in enumerate(all_u10_universes(),start=1):
        key=universe_key(assets); is_can=key==canonical_key
        baseline,reference=run_reference(panel,events,assets,mature,INITIAL_USDT)
        reference=reference.reset_index(drop=True)
        monthly,anchors=monthly_state(reference,INITIAL_USDT)

        e95=event_rows(reference,monthly,TH95)
        e100=event_rows(reference,monthly,TH100)
        einc=e95[(e95["selected_change"]<TH100)].copy() if not e95.empty else pd.DataFrame()
        for name,edf in (("GE95",e95),("GE100",e100),("INC95_100",einc)):
            if not edf.empty:
                x=edf.copy(); x.insert(0,"group",name); x.insert(0,"is_canonical",is_can); x.insert(0,"universe_key",key); event_frames.append(x)

        for label,pullback in (("P1",0.05),("P2",0.10)):
            s100,_=fractal.run_fractal_overlay(panel_eval,reference,anchors,surge_threshold=TH100,cashout_pullback=pullback,initial_usdt=INITIAL_USDT)
            s95,cy=fractal.run_fractal_overlay(panel_eval,reference,anchors,surge_threshold=TH95,cashout_pullback=pullback,initial_usdt=INITIAL_USDT)
            row={
                "universe_key":key,"variant":label,"is_canonical":is_can,
                "baseline_final_equity_usdt":baseline["final_equity_usdt"],
                "baseline_max_drawdown":baseline["max_drawdown"],
                "s100_final_equity_usdt":s100["final_equity_usdt"],
                "s100_max_drawdown":s100["max_drawdown"],
                "s95_final_equity_usdt":s95["final_equity_usdt"],
                "s95_max_drawdown":s95["max_drawdown"],
                "s95_cashouts":s95["cashouts"],"s95_reentries":s95["reentries"],
                "s95_unfinished_cycles":s95["unfinished_cycles"],"s95_days_in_cash":s95["days_in_cash"],
                "s95_delta_vs_baseline_usdt":s95["final_equity_usdt"]-baseline["final_equity_usdt"],
                "s95_delta_vs_baseline_pct":s95["final_equity_usdt"]/baseline["final_equity_usdt"]-1,
                "s95_delta_vs_s100_usdt":s95["final_equity_usdt"]-s100["final_equity_usdt"],
                "s95_delta_vs_s100_pct":s95["final_equity_usdt"]/s100["final_equity_usdt"]-1,
                "s95_dd_improvement_pp":(s95["max_drawdown"]-baseline["max_drawdown"])*100,
            }
            rows.append(row)
            if is_can: canonical[label]=dict(row)
            if not cy.empty:
                cc=cy.copy(); cc.insert(0,"variant",label); cc.insert(0,"is_canonical",is_can); cc.insert(0,"universe_key",key); cycles.append(cc)

        if idx%100==0: print(f"processed_universes={idx}")

    df=pd.DataFrame(rows)
    ev=pd.concat(event_frames,ignore_index=True) if event_frames else pd.DataFrame()
    cy=pd.concat(cycles,ignore_index=True) if cycles else pd.DataFrame()

    can_events={}
    alt_events={}
    for g in ("GE95","GE100","INC95_100"):
        can_events[g]=summarize_events(ev[(ev["group"]==g)&(ev["is_canonical"]==True)] if not ev.empty else pd.DataFrame())
        alt_events[g]=summarize_events(ev[(ev["group"]==g)&(ev["is_canonical"]==False)] if not ev.empty else pd.DataFrame())

    alt_summary={label:summarize_alt(df[df["variant"]==label]) for label in ("P1","P2")}

    run_dir=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True,exist_ok=True)
    df.to_csv(run_dir/"all_792_surge95_vs100.csv",index=False)
    if not ev.empty: ev.to_csv(run_dir/"all_surge95_events.csv",index=False)
    if not cy.empty: cy.to_csv(run_dir/"all_surge95_cycles.csv",index=False)

    payload={
        "generated_at":pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit":source_sha(),
        "mature_start":mature.isoformat(),
        "parameters":{"candidate_threshold":TH95,"control_threshold":TH100,"pivot_left":2,"pivot_right":2,"search_drawdown":fractal.SEARCH_DRAWDOWN},
        "canonical":canonical,
        "canonical_event_summary":can_events,
        "alternative_event_summary":alt_events,
        "alternative_summary":alt_summary,
        "data_metadata":meta,
    }
    (run_dir/"results.json").write_text(json.dumps(payload,indent=2,sort_keys=True,allow_nan=False),encoding="utf-8")

    lines=["# U10 Single-Month Surge 95% v1","","Mode: STRESS_TEST_ONLY","Live/paper logic: UNCHANGED","",
           "Only change: one-month surge trigger 100% -> 95%. 5-bar fractal cash/re-entry mechanics unchanged.","",
           "## Canonical events"]
    for g in ("GE100","GE95","INC95_100"):
        s=can_events[g]
        lines.append(f"- {g}: count={s.get('count',0)}, hit25_31={pct(s.get('hit25_31d'))}, hit25_62={pct(s.get('hit25_62d'))}, hit25_93={pct(s.get('hit25_93d'))}")
    lines+=["","## Canonical portfolio","","| Variant | Baseline | Surge100 | Surge95 | 95 vs baseline | 95 vs 100 | Max DD 95 |","|---|---:|---:|---:|---:|---:|---:|"]
    for label in ("P1","P2"):
        c=canonical[label]
        lines.append(f"| {label} | {c['baseline_final_equity_usdt']:,.2f} | {c['s100_final_equity_usdt']:,.2f} | {c['s95_final_equity_usdt']:,.2f} | {pct(c['s95_delta_vs_baseline_pct'])} | {pct(c['s95_delta_vs_s100_pct'])} | {pct(c['s95_max_drawdown'])} |")
    lines+=["","## 791 alternative U10s","","| Variant | 95 final > baseline | 95 final > 100 | DD better | Both better | Unfinished | Median 95 vs baseline | Median 95 vs 100 |","|---|---:|---:|---:|---:|---:|---:|---:|"]
    for label in ("P1","P2"):
        s=alt_summary[label]
        lines.append(f"| {label} | {100*s['final_gt_baseline_rate']:.2f}% | {100*s['final_gt_s100_rate']:.2f}% | {100*s['dd_better_rate']:.2f}% | {100*s['both_better_rate']:.2f}% | {100*s['unfinished_rate']:.2f}% | {pct(s['median_delta_vs_baseline_pct'])} | {pct(s['median_delta_vs_s100_pct'])} |")
    lines+=["","## Alternative event groups"]
    for g in ("GE100","GE95","INC95_100"):
        s=alt_events[g]
        lines.append(f"- {g}: count={s.get('count',0)}, hit25_31={pct(s.get('hit25_31d'))}, hit25_62={pct(s.get('hit25_62d'))}, hit25_93={pct(s.get('hit25_93d'))}")
    lines+=["","TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST"]
    (run_dir/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")

    print("run_dir="+str(run_dir))
    print("CAN_EVENTS",can_events)
    print("ALT_EVENTS",alt_events)
    for label in ("P1","P2"):
        c=canonical[label]; s=alt_summary[label]
        print("%s CAN base=%.8f s100=%.8f s95=%.8f dbase=%.8f d100=%.8f dd=%.8f cash=%d re=%d"%(
            label,c["baseline_final_equity_usdt"],c["s100_final_equity_usdt"],c["s95_final_equity_usdt"],c["s95_delta_vs_baseline_pct"],c["s95_delta_vs_s100_pct"],c["s95_max_drawdown"],c["s95_cashouts"],c["s95_reentries"]))
        print("%s ALT base=%.6f beat100=%.6f both=%.6f medbase=%.6f med100=%.6f"%(
            label,s["final_gt_baseline_rate"],s["final_gt_s100_rate"],s["both_better_rate"],s["median_delta_vs_baseline_pct"],s["median_delta_vs_s100_pct"]))
    return 0

if __name__=="__main__":
    raise SystemExit(main())
