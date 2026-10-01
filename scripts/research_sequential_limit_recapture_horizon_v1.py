from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

import scripts.research_negative_trade_forensics_v1 as fx
import scripts.research_sequential_limit_recapture_v1 as base

OUT=Path("research_artifacts/sequential_limit_recapture_horizon_v1")
HORIZONS_H=(168,336,720,1440,2160)
MAX_H=max(HORIZONS_H)


def extract_path_occurrences(panel,events,active):
    ts=pd.DatetimeIndex(panel["timestamp"])
    si=int(ts.searchsorted(fx.START,"left"))
    ei=int(ts.searchsorted(fx.CUTOFF,"right"))-1
    rows=[]
    for start in fx.U10:
        cur=start
        ledger=[]
        for i in range(si,ei):
            route=base.effective_route(events[i],active[i],cur)
            if route is None:
                continue
            eff=route["effective"]
            ledger.append({
                "start_asset":start,
                "signal_i":i,
                "available_ts":ts[i+1],
                "source":cur,
                "destination":eff["to_asset"],
                "ddg_override":bool(route["override"]),
                "effective_event":eff["event"],
                "mode":eff["mode"],
                "extreme_i":int(eff["extreme_i"]),
            })
            cur=eff["to_asset"]
        for j,row in enumerate(ledger):
            nxt=ledger[j+1] if j+1<len(ledger) else None
            x=dict(row)
            x["next_signal_available_ts"]=None if nxt is None else nxt["available_ts"]
            rows.append(x)
    return pd.DataFrame(rows)


def download_hourly_long():
    data={}
    end=fx.CUTOFF+pd.Timedelta(days=1)
    for asset in fx.U10:
        data[asset]=base.download_hourly(asset,fx.START,end).set_index("timestamp")
    return data


def evaluate_occurrences(panel,hourly,occ):
    rows=[]
    for _,r in occ.iterrows():
        # This horizon study is deliberately limited to direct confirmed routes.
        if bool(r["ddg_override"]) or r["effective_event"]!="CONFIRMED":
            continue
        geom=base.target_geometry(panel,r.to_dict())
        if not geom.get("exact3_applicable"):
            continue
        start_ts=pd.Timestamp(r["available_ts"])
        fill=base.fill_sequential(
            hourly,r["source"],r["destination"],start_ts,
            float(geom["src_target_exact3"]),float(geom["dst_target_exact3"]),MAX_H
        )
        row={**r.to_dict(),**geom,**fill}
        next_ts=r["next_signal_available_ts"]
        if pd.notna(next_ts) and next_ts is not None:
            next_ts=pd.Timestamp(next_ts)
            row["hours_until_next_signal"]=float((next_ts-start_ts)/pd.Timedelta(hours=1))
            row["complete_before_next_signal"]=bool(
                fill.get("buy_filled")
                and pd.Timestamp(fill["buy_fill_hour"])+pd.Timedelta(hours=1) <= next_ts
            )
            row["sell_before_next_signal"]=bool(
                fill.get("sell_filled")
                and pd.Timestamp(fill["sell_fill_hour"])+pd.Timedelta(hours=1) <= next_ts
            )
        else:
            row["hours_until_next_signal"]=np.nan
            row["complete_before_next_signal"]=np.nan
            row["sell_before_next_signal"]=np.nan
        for h in HORIZONS_H:
            # To avoid right-censoring, only count a horizon if full horizon fits inside frozen data.
            observable=start_ts+pd.Timedelta(hours=h) <= fx.CUTOFF+pd.Timedelta(days=1)
            row[f"observable_{h}h"]=observable
            row[f"sell_by_{h}h"]=bool(observable and fill.get("sell_filled") and fill.get("sell_wait_h",1e9)<=h)
            row[f"complete_by_{h}h"]=bool(observable and fill.get("buy_filled") and fill.get("complete_wait_h",1e9)<=h)
        rows.append(row)
    return pd.DataFrame(rows)


def dedupe_for_horizon(df,h):
    g=df[df[f"observable_{h}h"]==True].copy()
    return g.sort_values(["available_ts","source","destination"]).drop_duplicates(
        ["available_ts","source","destination"]
    )


def main():
    OUT.mkdir(parents=True,exist_ok=True)
    panel,_=fx.download_panel()
    panel["timestamp"]=pd.to_datetime(panel["timestamp"],utc=True)
    events,active=base.pair_tape_with_extremes(panel)
    occ=extract_path_occurrences(panel,events,active)
    hourly=download_hourly_long()
    d=evaluate_occurrences(panel,hourly,occ)
    d.to_csv(OUT/"path_occurrence_horizon_results.csv",index=False)

    rows=[]
    for h in HORIZONS_H:
        g=dedupe_for_horizon(d,h)
        rows.append({
            "horizon_h":h,
            "horizon_days":h/24,
            "unique_routes_observable":len(g),
            "sell_fill_rate":float(g[f"sell_by_{h}h"].mean()) if len(g) else np.nan,
            "complete_fill_rate":float(g[f"complete_by_{h}h"].mean()) if len(g) else np.nan,
            "sell_filled_buy_missed":int((g[f"sell_by_{h}h"] & ~g[f"complete_by_{h}h"]).sum()) if len(g) else 0,
        })
    hs=pd.DataFrame(rows)
    hs.to_csv(OUT/"long_horizon_summary.csv",index=False)

    # Next-signal integrity on unique route + next-signal combinations.
    n=d[d["next_signal_available_ts"].notna()].copy()
    n=n.sort_values(["available_ts","source","destination","next_signal_available_ts"]).drop_duplicates(
        ["available_ts","source","destination","next_signal_available_ts"]
    )
    next_summary={
        "n":int(len(n)),
        "median_hours_until_next_signal":float(n["hours_until_next_signal"].median()) if len(n) else None,
        "p25_hours_until_next_signal":float(n["hours_until_next_signal"].quantile(.25)) if len(n) else None,
        "complete_before_next_signal_n":int(n["complete_before_next_signal"].fillna(False).sum()),
        "complete_before_next_signal_rate":float(n["complete_before_next_signal"].fillna(False).mean()) if len(n) else None,
        "sell_before_next_signal_rate":float(n["sell_before_next_signal"].fillna(False).mean()) if len(n) else None,
        "sold_but_not_bought_before_next_signal_n":int(
            (n["sell_before_next_signal"].fillna(False)&~n["complete_before_next_signal"].fillna(False)).sum()
        ),
    }
    pd.DataFrame([next_summary]).to_csv(OUT/"next_signal_integrity_summary.csv",index=False)

    # Cases for audit.
    n[~n["complete_before_next_signal"].fillna(False)].to_csv(
        OUT/"would_change_path_cases.csv",index=False
    )

    payload={
        "experiment":"SEQUENTIAL_LIMIT_RECAPTURE_HORIZON_V1",
        "workflow_mode":"STRESS_TEST_ONLY",
        "production_changes":"NONE",
        "target":"EXACT_3PCT_LOG",
        "scope":"PATH_VISITED_DIRECT_CONFIRMED",
        "long_horizons":json.loads(hs.to_json(orient="records")),
        "next_signal_integrity":next_summary,
        "test_level":"GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    }
    (OUT/"summary.json").write_text(json.dumps(payload,indent=2,sort_keys=True,default=str)+"\n",encoding="utf-8")

    lines=[
        "# SEQUENTIAL LIMIT RECAPTURE — LONG HORIZON / PATH INTEGRITY","",
        "Mode: STRESS_TEST_ONLY","",
        "Target: EXACT_3PCT_LOG",
        "Scope: path-visited direct confirmed rotations only.","",
        "## Long horizon fill","",
        "|Horizon|Observable unique routes|Sell fill|Complete sell->buy|Sold but buy missed|",
        "|---:|---:|---:|---:|---:|",
    ]
    for _,r in hs.iterrows():
        lines.append(
            f"|{r['horizon_days']:.0f}d|{int(r['unique_routes_observable'])}|"
            f"{100*r['sell_fill_rate']:.1f}%|{100*r['complete_fill_rate']:.1f}%|"
            f"{int(r['sell_filled_buy_missed'])}|"
        )
    lines += [
        "",
        "## Before next original RR signal","",
        f"Unique route/next-signal cases: {next_summary['n']}",
        f"Median time to next RR signal: {next_summary['median_hours_until_next_signal']:.1f}h",
        f"Complete recapture before next RR signal: {100*next_summary['complete_before_next_signal_rate']:.1f}%",
        f"Sell filled before next signal: {100*next_summary['sell_before_next_signal_rate']:.1f}%",
        f"Sold but still not rebought before next RR signal: {next_summary['sold_but_not_bought_before_next_signal_n']}",
        "",
        "If recapture is not complete before the next original signal, this is no longer a pure execution optimization; it changes the RR path.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    ]
    (OUT/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")
    print("\n".join(lines))

if __name__=="__main__":
    main()
