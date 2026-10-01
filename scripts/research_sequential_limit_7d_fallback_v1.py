from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

import scripts.research_negative_trade_forensics_v1 as fx
import scripts.research_sequential_limit_recapture_v1 as sl

OUT=Path("research_artifacts/sequential_limit_7d_fallback_v1")
COST=0.001
TIMEOUT_H=168

WINDOWS={
    "VALIDATION_1Y":(pd.Timestamp("2025-09-27",tz="UTC"),pd.Timestamp("2026-09-26",tz="UTC")),
    "LAST_2Y":(pd.Timestamp("2024-09-27",tz="UTC"),pd.Timestamp("2026-09-26",tz="UTC")),
    "MATURE":(pd.Timestamp("2023-10-31",tz="UTC"),pd.Timestamp("2026-09-26",tz="UTC")),
}


def get_open(hourly,asset,ts):
    f=hourly[asset]
    if ts in f.index:
        return float(f.loc[ts,"open"])
    ix=f.index.searchsorted(ts,side="left")
    if ix>=len(f):
        raise RuntimeError(f"no hourly open for {asset} at {ts}")
    return float(f.iloc[ix]["open"])


def build_plan(panel,hourly,events,active,signal_i,source,route):
    ts=pd.DatetimeIndex(panel["timestamp"])
    start_ts=ts[signal_i+1]
    dest=route["effective"]["to_asset"]

    # DDG / non-direct confirmations have no canonical exact-3 target between actual source and effective destination.
    if bool(route["override"]) or route["effective"].get("event")!="CONFIRMED":
        return {
            "kind":"IMMEDIATE_MARKET_DDG",
            "source":source,"destination":dest,
            "signal_i":signal_i,
            "start_ts":start_ts,
            "sell_ts":start_ts,"buy_ts":start_ts,"complete_ts":start_ts,
            "sell_price":float(panel.loc[signal_i+1,source+"_open"]),
            "buy_price":float(panel.loc[signal_i+1,dest+"_open"]),
            "sell_limit_filled":False,"buy_limit_filled":False,
            "sell_fallback":True,"buy_fallback":True,
        }

    eff=route["effective"]
    geom=sl.target_geometry(panel,{
        "source":source,"destination":dest,
        "signal_i":signal_i,"extreme_i":int(eff["extreme_i"]),"mode":eff["mode"],
    })
    if not geom.get("exact3_applicable"):
        return {
            "kind":"IMMEDIATE_MARKET_NONAPPLICABLE",
            "source":source,"destination":dest,
            "signal_i":signal_i,
            "start_ts":start_ts,
            "sell_ts":start_ts,"buy_ts":start_ts,"complete_ts":start_ts,
            "sell_price":float(panel.loc[signal_i+1,source+"_open"]),
            "buy_price":float(panel.loc[signal_i+1,dest+"_open"]),
            "sell_limit_filled":False,"buy_limit_filled":False,
            "sell_fallback":True,"buy_fallback":True,
        }

    sell_target=float(geom["src_target_exact3"])
    buy_target=float(geom["dst_target_exact3"])
    deadline=start_ts+pd.Timedelta(hours=TIMEOUT_H)

    hs=hourly[source]
    sf=hs[(hs.index>=start_ts)&(hs.index<deadline)]
    sell_ts=None
    for t,r in sf.iterrows():
        if float(r["high"])+1e-12>=sell_target:
            sell_ts=t; break

    if sell_ts is None:
        sp=get_open(hourly,source,deadline)
        bp=get_open(hourly,dest,deadline)
        return {
            "kind":"SELL_TIMEOUT_MARKET_BOTH",
            "source":source,"destination":dest,
            "signal_i":signal_i,"start_ts":start_ts,
            "deadline":deadline,
            "sell_ts":deadline,"buy_ts":deadline,"complete_ts":deadline,
            "sell_price":sp,"buy_price":bp,
            "sell_target":sell_target,"buy_target":buy_target,
            "sell_limit_filled":False,"buy_limit_filled":False,
            "sell_fallback":True,"buy_fallback":True,
        }

    buy_start=sell_ts+pd.Timedelta(hours=1)
    hd=hourly[dest]
    bf=hd[(hd.index>=buy_start)&(hd.index<deadline)]
    buy_ts=None
    for t,r in bf.iterrows():
        if float(r["low"])-1e-12<=buy_target:
            buy_ts=t; break

    if buy_ts is None:
        bp=get_open(hourly,dest,deadline)
        return {
            "kind":"SELL_LIMIT_BUY_TIMEOUT_MARKET",
            "source":source,"destination":dest,
            "signal_i":signal_i,"start_ts":start_ts,
            "deadline":deadline,
            "sell_ts":sell_ts,"buy_ts":deadline,"complete_ts":deadline,
            "sell_price":sell_target,"buy_price":bp,
            "sell_target":sell_target,"buy_target":buy_target,
            "sell_limit_filled":True,"buy_limit_filled":False,
            "sell_fallback":False,"buy_fallback":True,
        }

    return {
        "kind":"FULL_LIMIT_RECAPTURE",
        "source":source,"destination":dest,
        "signal_i":signal_i,"start_ts":start_ts,
        "deadline":deadline,
        "sell_ts":sell_ts,"buy_ts":buy_ts,"complete_ts":buy_ts+pd.Timedelta(hours=1),
        "sell_price":sell_target,"buy_price":buy_target,
        "sell_target":sell_target,"buy_target":buy_target,
        "sell_limit_filled":True,"buy_limit_filled":True,
        "sell_fallback":False,"buy_fallback":False,
    }


def simulate_policy(panel,hourly,events,active,start,end,start_asset):
    ts=pd.DatetimeIndex(panel["timestamp"])
    si=int(ts.searchsorted(start,"left"))
    ei=int(ts.searchsorted(end,"right"))-1
    if ei<=si:
        raise RuntimeError("bad window")

    cur=start_asset
    qty=1.0/float(panel.loc[si,cur+"_open"])
    cash=None
    pending=None
    ledger=[]
    equity=[]
    skipped_signal_days=0

    for i in range(si,ei+1):
        day_open=ts[i]
        day_close=ts[i]+pd.Timedelta(days=1)

        # Apply pending execution events that occurred by this daily close.
        if pending is not None:
            if pending.get("sell_applied") is not True and pending["sell_ts"] < day_close:
                cash=qty*float(pending["sell_price"])*(1-COST)
                qty=0.0
                pending["sell_applied"]=True

            if pending.get("sell_applied") is True and pending.get("buy_applied") is not True and pending["buy_ts"] < day_close:
                qty=float(cash)*(1-COST)/float(pending["buy_price"])
                cash=None
                cur=pending["destination"]
                pending["buy_applied"]=True
                pending["completed_day_i"]=i
                ledger.append({
                    **{k:v for k,v in pending.items() if k not in {"sell_applied","buy_applied"}},
                    "complete_day_index":i,
                })
                pending=None

        # Daily close equity.
        if pending is not None and pending.get("sell_applied") is True:
            value=float(cash)
        else:
            value=qty*float(panel.loc[i,cur+"_close"])
        equity.append(value)

        if i>=ei:
            continue

        # Signals while execution is pending are intentionally ignored.
        if pending is not None:
            skipped_signal_days += 1
            continue

        route=sl.effective_route(events[i],active[i],cur)
        if route is None:
            continue

        plan=build_plan(panel,hourly,events,active,i,cur,route)
        plan["signal_date"]=ts[i].date().isoformat()
        plan["ddg_override"]=bool(route["override"])
        plan["primary_destination"]=route["primary"]["to_asset"]
        pending=plan

    eq=np.asarray(equity,float)
    initial=float(eq[0])
    peak=np.maximum.accumulate(eq)
    ret=float(eq[-1]/initial-1)
    dd=float(np.min(eq/peak-1))

    return {
        "start_asset":start_asset,
        "return":ret,"max_dd":dd,
        "ledger":ledger,
        "skipped_signal_days":skipped_signal_days,
        "final_state":"CASH_PENDING" if pending is not None and pending.get("sell_applied") else ("SOURCE_PENDING" if pending is not None else cur),
        "equity":eq,
    }


def simulate_market_two_fee(panel,events,active,start,end,start_asset):
    ts=pd.DatetimeIndex(panel["timestamp"])
    si=int(ts.searchsorted(start,"left")); ei=int(ts.searchsorted(end,"right"))-1
    cur=start_asset
    qty=1.0/float(panel.loc[si,cur+"_open"])
    eq=[]
    pending_dest=None
    transitions=0
    for i in range(si,ei+1):
        if pending_dest is not None:
            cash=qty*float(panel.loc[i,cur+"_open"])*(1-COST)
            qty=cash*(1-COST)/float(panel.loc[i,pending_dest+"_open"])
            cur=pending_dest
            pending_dest=None
            transitions+=1
        eq.append(qty*float(panel.loc[i,cur+"_close"]))
        if i>=ei: continue
        route=sl.effective_route(events[i],active[i],cur)
        if route is not None:
            pending_dest=route["effective"]["to_asset"]
    eq=np.asarray(eq,float); peak=np.maximum.accumulate(eq)
    return {
        "start_asset":start_asset,
        "return":float(eq[-1]/eq[0]-1),
        "max_dd":float(np.min(eq/peak-1)),
        "transitions":transitions,
    }


def baseline_one_fee(panel,start,end):
    # Use current canonical RR implementation for one-fee comparison.
    ev,ac=fx.pair_daily_tape(panel,fx.REVERSAL,1)
    ts=pd.DatetimeIndex(panel["timestamp"])
    si=int(ts.searchsorted(start,"left")); ei=int(ts.searchsorted(end,"right"))-1
    runs=[]
    for start_asset in fx.U10:
        cur=start_asset
        qty=1.0/float(panel.loc[si,cur+"_open"])
        eq=[]
        pending=None
        transitions=0
        for i in range(si,ei+1):
            if pending is not None:
                value=qty*float(panel.loc[i,cur+"_open"])
                qty=value*(1-fx.COST)/float(panel.loc[i,pending+"_open"])
                cur=pending; pending=None; transitions+=1
            eq.append(qty*float(panel.loc[i,cur+"_close"]))
            if i>=ei: continue
            rr=fx.effective_route(ev[i],ac[i],cur)
            if rr is not None:
                pending=rr["effective"]["to_asset"]
        eq=np.asarray(eq,float); peak=np.maximum.accumulate(eq)
        runs.append({
            "start_asset":start_asset,
            "return":float(eq[-1]/eq[0]-1),
            "max_dd":float(np.min(eq/peak-1)),
            "transitions":transitions,
        })
    return runs


def summarize_runs(label,window,runs):
    return {
        "policy":label,"window":window,
        "median_return":float(np.median([r["return"] for r in runs])),
        "worst_return":float(np.min([r["return"] for r in runs])),
        "best_return":float(np.max([r["return"] for r in runs])),
        "median_dd":float(np.median([r["max_dd"] for r in runs])),
        "worst_dd":float(np.min([r["max_dd"] for r in runs])),
    }


def main():
    OUT.mkdir(parents=True,exist_ok=True)
    panel,_=fx.download_panel()
    panel["timestamp"]=pd.to_datetime(panel["timestamp"],utc=True)
    events,active=sl.pair_tape_with_extremes(panel)

    hourly={}
    hstart=min(v[0] for v in WINDOWS.values())
    hend=fx.CUTOFF+pd.Timedelta(days=8)
    for asset in fx.U10:
        hourly[asset]=sl.download_hourly(asset,hstart,hend).set_index("timestamp")

    summary_rows=[]
    all_ledgers=[]
    start_rows=[]

    for w,(start,end) in WINDOWS.items():
        policy_runs=[simulate_policy(panel,hourly,events,active,start,end,a) for a in fx.U10]
        market2_runs=[simulate_market_two_fee(panel,events,active,start,end,a) for a in fx.U10]
        base1_runs=baseline_one_fee(panel,start,end)

        summary_rows.append(summarize_runs("CANONICAL_NEXT_OPEN_ONE_FEE",w,base1_runs))
        summary_rows.append(summarize_runs("NEXT_OPEN_TWO_FEE_CONTROL",w,market2_runs))
        summary_rows.append(summarize_runs("LIMIT_7D_FALLBACK_TWO_FEE",w,policy_runs))

        for p,m,b in zip(policy_runs,market2_runs,base1_runs):
            start_rows.append({
                "window":w,"start_asset":p["start_asset"],
                "policy_return":p["return"],"policy_dd":p["max_dd"],
                "market2_return":m["return"],"market2_dd":m["max_dd"],
                "canonical_return":b["return"],"canonical_dd":b["max_dd"],
                "policy_vs_market2_factor":(1+p["return"])/(1+m["return"]),
                "policy_vs_canonical_factor":(1+p["return"])/(1+b["return"]),
                "skipped_signal_days":p["skipped_signal_days"],
                "policy_rotations":len(p["ledger"]),
            })
            if w=="MATURE":
                for e in p["ledger"]:
                    all_ledgers.append({
                        "start_asset":p["start_asset"],
                        **e,
                    })

    summary=pd.DataFrame(summary_rows)
    summary.to_csv(OUT/"window_summary.csv",index=False)
    starts=pd.DataFrame(start_rows)
    starts.to_csv(OUT/"start_level_results.csv",index=False)
    led=pd.DataFrame(all_ledgers)
    led.to_csv(OUT/"mature_execution_ledger.csv",index=False)

    # Deduplicate execution opportunities after path convergence.
    if len(led):
        unique=led.sort_values(["signal_date","source","destination"]).drop_duplicates(
            ["signal_date","source","destination","kind"]
        )
    else:
        unique=led
    unique.to_csv(OUT/"mature_unique_execution_events.csv",index=False)

    kinds=unique.groupby("kind",dropna=False).size().reset_index(name="n") if len(unique) else pd.DataFrame(columns=["kind","n"])
    kinds.to_csv(OUT/"execution_kind_counts.csv",index=False)

    # Fill stats only for direct exact3 attempts.
    direct=unique[unique["kind"].isin(["FULL_LIMIT_RECAPTURE","SELL_TIMEOUT_MARKET_BOTH","SELL_LIMIT_BUY_TIMEOUT_MARKET"])] if len(unique) else unique
    fill_stats={
        "unique_direct_attempts":int(len(direct)),
        "full_limit_n":int((direct["kind"]=="FULL_LIMIT_RECAPTURE").sum()) if len(direct) else 0,
        "full_limit_rate":float((direct["kind"]=="FULL_LIMIT_RECAPTURE").mean()) if len(direct) else None,
        "sell_timeout_n":int((direct["kind"]=="SELL_TIMEOUT_MARKET_BOTH").sum()) if len(direct) else 0,
        "buy_timeout_after_sell_n":int((direct["kind"]=="SELL_LIMIT_BUY_TIMEOUT_MARKET").sum()) if len(direct) else 0,
        "median_completion_hours":float(
            ((pd.to_datetime(direct["complete_ts"],utc=True)-pd.to_datetime(direct["start_ts"],utc=True))/pd.Timedelta(hours=1)).median()
        ) if len(direct) else None,
    }

    # Median start-level capital factor impact by window.
    impact=[]
    for w,g in starts.groupby("window"):
        impact.append({
            "window":w,
            "median_policy_vs_market2_factor":float(g["policy_vs_market2_factor"].median()),
            "median_policy_vs_canonical_factor":float(g["policy_vs_canonical_factor"].median()),
            "median_skipped_signal_days":float(g["skipped_signal_days"].median()),
            "median_policy_rotations":float(g["policy_rotations"].median()),
        })
    impact=pd.DataFrame(impact)
    impact.to_csv(OUT/"capital_impact.csv",index=False)

    payload={
        "experiment":"SEQUENTIAL_LIMIT_7D_FALLBACK_V1",
        "workflow_mode":"STRESS_TEST_ONLY",
        "production_changes":"NONE",
        "policy":{
            "timeout_hours":TIMEOUT_H,
            "direct_route":"exact3 sell limit; after sell exact3 buy limit; at 168h force remaining leg(s) at hourly open market proxy",
            "ddg_or_nonapplicable":"immediate next-open market",
            "pending_signals":"ignored until transfer completes",
            "fees":"0.1% sell + 0.1% buy",
        },
        "fill_stats":fill_stats,
        "window_summary":json.loads(summary.to_json(orient="records")),
        "capital_impact":json.loads(impact.to_json(orient="records")),
        "test_level":"GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    }
    (OUT/"summary.json").write_text(json.dumps(payload,indent=2,sort_keys=True,default=str)+"\n",encoding="utf-8")

    lines=[
        "# SEQUENTIAL LIMIT 7D + FORCED FALLBACK V1","",
        "Mode: STRESS_TEST_ONLY","",
        "Policy: try exact-3% sequential limit execution for 168h; if unfinished, force remaining transfer at market proxy.",
        "Signals arriving while capital is still transferring are ignored.","",
        "## Full path-dependent return","",
        "|Window|Policy|Median return|Worst start|Median DD|Worst DD|",
        "|---|---|---:|---:|---:|---:|",
    ]
    for _,r in summary.iterrows():
        lines.append(
            f"|{r['window']}|{r['policy']}|{100*r['median_return']:+.1f}%|"
            f"{100*r['worst_return']:+.1f}%|{100*r['median_dd']:+.1f}%|{100*r['worst_dd']:+.1f}%|"
        )

    lines += ["","## Capital factor impact of 7d policy","",
              "|Window|vs same two-fee next-open control|vs canonical one-fee|Median skipped signal-days|Median completed rotations|",
              "|---|---:|---:|---:|---:|"]
    for _,r in impact.iterrows():
        lines.append(
            f"|{r['window']}|{r['median_policy_vs_market2_factor']:.3f}x|"
            f"{r['median_policy_vs_canonical_factor']:.3f}x|"
            f"{r['median_skipped_signal_days']:.1f}|{r['median_policy_rotations']:.1f}|"
        )

    lines += ["","## MATURE unique direct execution attempts","",
              f"- attempts: {fill_stats['unique_direct_attempts']}",
              f"- full limit recapture: {fill_stats['full_limit_n']} ({100*fill_stats['full_limit_rate']:.1f}%)" if fill_stats["full_limit_rate"] is not None else "- no attempts",
              f"- source SELL timeout -> force both: {fill_stats['sell_timeout_n']}",
              f"- SELL filled but BUY timeout -> force buy: {fill_stats['buy_timeout_after_sell_n']}",
              "",
              "## Guardrails",
              "- Hourly wick touch is an optimistic fill proxy; queue/partial fills are not modeled.",
              "- Fallback uses the hourly open at the 168h deadline as market-price proxy.",
              "- Ignoring RR signals while transfer is pending makes the path effect explicit.",
              "- DDG overrides are executed immediately at next open because exact 3% is not canonically defined for the effective override destination.",
              "- No production/live/paper/Telegram/exchange behavior changed.",
              "",
              "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST"]
    (OUT/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")
    print("\n".join(lines))

if __name__=="__main__":
    main()
