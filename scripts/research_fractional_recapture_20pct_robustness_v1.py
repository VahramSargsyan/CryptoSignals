from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

import scripts.research_negative_trade_forensics_v1 as fx
import scripts.research_sequential_limit_recapture_v1 as sl
import scripts.research_sequential_limit_7d_fallback_v1 as base
import scripts.research_fractional_recapture_grid_v1 as fr

OUT=Path("research_artifacts/fractional_recapture_20pct_robustness_v1")
CANDIDATES=((0.20,3),(0.20,6),(0.20,12))
COST=0.001


def month_starts(first,last):
    return list(pd.date_range(first,last,freq="MS",tz="UTC"))


def simulate_set(panel,hourly,events,active,start,end,fraction,timeout,cache):
    return [fr.simulate_policy(panel,hourly,events,active,start,end,a,fraction,timeout,cache) for a in fx.U10]


def summary(runs):
    rr=np.array([r["return"] for r in runs],float)
    dd=np.array([r["max_dd"] for r in runs],float)
    return {
        "median_return":float(np.median(rr)),
        "worst_return":float(rr.min()),
        "best_return":float(rr.max()),
        "median_dd":float(np.median(dd)),
        "worst_dd":float(dd.min()),
    }


def main():
    OUT.mkdir(parents=True,exist_ok=True)
    panel,_=fx.download_panel()
    panel["timestamp"]=pd.to_datetime(panel["timestamp"],utc=True)
    events,active=sl.pair_tape_with_extremes(panel)

    hstart=fx.START
    hend=fx.CUTOFF+pd.Timedelta(days=2)
    hourly={}
    for asset in fx.U10:
        hourly[asset]=sl.download_hourly(asset,hstart,hend).set_index("timestamp")

    cache={}
    rows=[]
    start_rows=[]

    # Monthly-stepped rolling 12m and 24m, using exact calendar endpoints.
    for months in (12,24):
        starts=month_starts(pd.Timestamp("2023-11-01",tz="UTC"),pd.Timestamp("2026-09-01",tz="UTC"))
        for st in starts:
            en=st+pd.DateOffset(months=months)-pd.Timedelta(days=1)
            if en>fx.CUTOFF:
                continue

            canonical=base.baseline_one_fee(panel,st,en)
            market2=[base.simulate_market_two_fee(panel,events,active,st,en,a) for a in fx.U10]
            cs=summary(canonical); ms=summary(market2)

            for f,th in CANDIDATES:
                runs=simulate_set(panel,hourly,events,active,st,en,f,th,cache)
                ss=summary(runs)
                row={
                    "months":months,"start":st.date().isoformat(),"end":pd.Timestamp(en).date().isoformat(),
                    "fraction":f,"timeout_h":th,
                    **ss,
                    "canonical_median_return":cs["median_return"],
                    "canonical_median_dd":cs["median_dd"],
                    "market2_median_return":ms["median_return"],
                    "factor_vs_canonical":(1+ss["median_return"])/(1+cs["median_return"]),
                    "factor_vs_market2":(1+ss["median_return"])/(1+ms["median_return"]),
                    "dd_delta_vs_canonical":ss["median_dd"]-cs["median_dd"],
                }
                rows.append(row)
                for r,c,m in zip(runs,canonical,market2):
                    start_rows.append({
                        "months":months,"start":st.date().isoformat(),"end":pd.Timestamp(en).date().isoformat(),
                        "fraction":f,"timeout_h":th,"start_asset":r["start_asset"],
                        "factor_vs_canonical":(1+r["return"])/(1+c["return"]),
                        "factor_vs_market2":(1+r["return"])/(1+m["return"]),
                        "return":r["return"],"canonical_return":c["return"],
                        "max_dd":r["max_dd"],"canonical_dd":c["max_dd"],
                    })

    d=pd.DataFrame(rows)
    d.to_csv(OUT/"rolling_windows.csv",index=False)
    sr=pd.DataFrame(start_rows)
    sr.to_csv(OUT/"rolling_start_assets.csv",index=False)

    agg=[]
    for (months,f,th),g in d.groupby(["months","fraction","timeout_h"]):
        agg.append({
            "months":int(months),"fraction":f,"timeout_h":int(th),"n_windows":len(g),
            "median_factor_vs_canonical":float(g["factor_vs_canonical"].median()),
            "worst_factor_vs_canonical":float(g["factor_vs_canonical"].min()),
            "best_factor_vs_canonical":float(g["factor_vs_canonical"].max()),
            "share_windows_beats_canonical":float((g["factor_vs_canonical"]>1).mean()),
            "share_windows_beats_market2":float((g["factor_vs_market2"]>1).mean()),
            "median_dd_delta_pp":float(100*g["dd_delta_vs_canonical"].median()),
            "worst_dd_delta_pp":float(100*g["dd_delta_vs_canonical"].min()),
        })
    agg=pd.DataFrame(agg)
    agg.to_csv(OUT/"rolling_summary.csv",index=False)

    # Across every start-asset/window observation.
    sa=[]
    for (months,f,th),g in sr.groupby(["months","fraction","timeout_h"]):
        sa.append({
            "months":int(months),"fraction":f,"timeout_h":int(th),
            "n_start_window_obs":len(g),
            "median_factor_vs_canonical":float(g["factor_vs_canonical"].median()),
            "worst_factor_vs_canonical":float(g["factor_vs_canonical"].min()),
            "share_obs_beats_canonical":float((g["factor_vs_canonical"]>1).mean()),
        })
    sa=pd.DataFrame(sa)
    sa.to_csv(OUT/"start_asset_robustness.csv",index=False)

    # Event-level candidate comparison from mature direct attempts.
    evrows=[]
    for f,th in CANDIDATES:
        # One mature run family is enough to collect path-converged events; dedupe after all starts.
        runs=simulate_set(panel,hourly,events,active,fx.START,fx.CUTOFF,f,th,cache)
        led=[]
        for r in runs:
            for e in r["ledger"]:
                led.append({"start_asset":r["start_asset"],**e})
        le=pd.DataFrame(led)
        if len(le)==0: continue
        u=le.sort_values(["signal_date","source","destination","kind"]).drop_duplicates(
            ["signal_date","source","destination","kind"]
        )
        direct=u[u["applicable"]==True].copy()
        for _,e in direct.iterrows():
            si=int(e["signal_i"])
            q0=float(panel.loc[si+1,e["source"]+"_open"])/float(panel.loc[si+1,e["destination"]+"_open"])
            q1=float(e["sell_price"])/float(e["buy_price"])
            evrows.append({
                "fraction":f,"timeout_h":th,
                "signal_date":e["signal_date"],"source":e["source"],"destination":e["destination"],
                "kind":e["kind"],"exchange_improvement":q1/q0-1.0,
                "full_limit":e["kind"]=="FULL_LIMIT",
                "completion_h":float((pd.Timestamp(e["complete_ts"])-pd.Timestamp(e["start_ts"]))/pd.Timedelta(hours=1)),
            })
    ev=pd.DataFrame(evrows)
    ev.to_csv(OUT/"mature_candidate_events.csv",index=False)

    # Bootstrap by route event: descriptive uncertainty only; event dependence remains.
    boots=[]
    rng=np.random.default_rng(20261001)
    for (f,th),g in ev.groupby(["fraction","timeout_h"]):
        vals=g["exchange_improvement"].to_numpy(float)
        meds=[]
        for _ in range(5000):
            meds.append(float(np.median(rng.choice(vals,size=len(vals),replace=True))))
        boots.append({
            "fraction":f,"timeout_h":int(th),"n_events":len(g),
            "median_improvement":float(np.median(vals)),
            "median_boot_lo95":float(np.quantile(meds,.025)),
            "median_boot_hi95":float(np.quantile(meds,.975)),
            "full_limit_rate":float(g["full_limit"].mean()),
        })
    boots=pd.DataFrame(boots)
    boots.to_csv(OUT/"event_bootstrap.csv",index=False)

    payload={
        "experiment":"FRACTIONAL_RECAPTURE_20PCT_ROBUSTNESS_V1",
        "workflow_mode":"STRESS_TEST_ONLY",
        "production_changes":"NONE",
        "candidates":[{"fraction":f,"timeout_h":th} for f,th in CANDIDATES],
        "selection_note":"20pct family emerged from preregistered grid; this is post-selection robustness, not clean OOS",
        "rolling_summary":json.loads(agg.to_json(orient="records")),
        "start_asset_robustness":json.loads(sa.to_json(orient="records")),
        "event_bootstrap":json.loads(boots.to_json(orient="records")),
        "test_level":"GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    }
    (OUT/"summary.json").write_text(json.dumps(payload,indent=2,sort_keys=True,default=str)+"\n",encoding="utf-8")

    lines=[
        "# FRACTIONAL RECAPTURE 20% ROBUSTNESS V1","",
        "Mode: STRESS_TEST_ONLY","",
        "Post-selection robustness only: 20% family emerged from the fixed 20-cell grid.",
        "Candidates frozen for this pass: 20% with 3h, 6h, 12h fallback.","",
        "## Rolling windows","",
        "|Window|Timeout|N|Median capital factor vs canonical|Worst factor|Win share vs canonical|Win share vs two-fee|Median DD delta|",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _,r in agg.sort_values(["months","timeout_h"]).iterrows():
        lines.append(
            f"|{int(r['months'])}m|{int(r['timeout_h'])}h|{int(r['n_windows'])}|"
            f"{r['median_factor_vs_canonical']:.3f}x|{r['worst_factor_vs_canonical']:.3f}x|"
            f"{100*r['share_windows_beats_canonical']:.1f}%|{100*r['share_windows_beats_market2']:.1f}%|"
            f"{r['median_dd_delta_pp']:+.2f}pp|"
        )

    lines += ["","## Start-asset/window observations","",
              "|Window|Timeout|N obs|Median factor|Worst factor|Share beats canonical|",
              "|---:|---:|---:|---:|---:|---:|"]
    for _,r in sa.sort_values(["months","timeout_h"]).iterrows():
        lines.append(
            f"|{int(r['months'])}m|{int(r['timeout_h'])}h|{int(r['n_start_window_obs'])}|"
            f"{r['median_factor_vs_canonical']:.3f}x|{r['worst_factor_vs_canonical']:.3f}x|"
            f"{100*r['share_obs_beats_canonical']:.1f}%|"
        )

    lines += ["","## Event-level exchange improvement","",
              "|Timeout|Events|Median improvement|Bootstrap 95%|Full limit rate|",
              "|---:|---:|---:|---:|---:|"]
    for _,r in boots.sort_values("timeout_h").iterrows():
        lines.append(
            f"|{int(r['timeout_h'])}h|{int(r['n_events'])}|{100*r['median_improvement']:+.2f}%|"
            f"{100*r['median_boot_lo95']:+.2f}% to {100*r['median_boot_hi95']:+.2f}%|"
            f"{100*r['full_limit_rate']:.1f}%|"
        )

    lines += ["","## Guardrails","",
              "- This is not clean OOS: 20% was selected after seeing the grid.",
              "- No tuning inside 15/20/25% or 4/6/8h was performed.",
              "- Candidate deadlines all finish before the next daily RR decision, preserving path timing.",
              "- Hourly wick touch remains an optimistic fill proxy.",
              "- No production/live/paper/Telegram/exchange behavior changed.",
              "",
              "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST"]
    (OUT/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")
    print("\n".join(lines))


if __name__=="__main__":
    main()
