from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

import scripts.research_negative_trade_forensics_v1 as fx
import scripts.research_sequential_limit_recapture_v1 as sl
import scripts.research_sequential_limit_7d_fallback_v1 as base

OUT=Path("research_artifacts/fractional_recapture_grid_v1")
COST=0.001
FRACTIONS=(0.20,0.40,0.60,0.80,1.00)
TIMEOUTS_H=(3,6,12,24)
WINDOWS=base.WINDOWS


def get_open(hourly,asset,ts):
    f=hourly[asset]
    if ts in f.index:
        return float(f.loc[ts,"open"])
    ix=f.index.searchsorted(ts,side="left")
    if ix>=len(f):
        raise RuntimeError(f"no hourly open {asset} {ts}")
    return float(f.iloc[ix]["open"])


def marketable_sell_target(current,ideal,f):
    # Only ask for favorable improvement. If ideal is already no better than current,
    # a marketable limit at current is used.
    if ideal<=current:
        return current
    return current + f*(ideal-current)


def marketable_buy_target(current,ideal,f):
    # Only ask for favorable improvement. If ideal or better is already available,
    # a marketable limit at current is used.
    if ideal>=current:
        return current
    return current + f*(ideal-current)


def sequential_fraction_fill(hourly,source,dest,start_ts,ideal_sell,ideal_buy,fraction,timeout_h):
    deadline=start_ts+pd.Timedelta(hours=timeout_h)

    src_now=get_open(hourly,source,start_ts)
    sell_target=marketable_sell_target(src_now,float(ideal_sell),fraction)

    hs=hourly[source]
    sf=hs[(hs.index>=start_ts)&(hs.index<deadline)]
    sell_ts=None
    for t,r in sf.iterrows():
        if float(r["high"])+1e-12>=sell_target:
            sell_ts=t
            break

    if sell_ts is None:
        sp=get_open(hourly,source,deadline)
        bp=get_open(hourly,dest,deadline)
        return {
            "kind":"SELL_TIMEOUT_MARKET_BOTH",
            "sell_ts":deadline,"buy_ts":deadline,"complete_ts":deadline,
            "sell_price":sp,"buy_price":bp,
            "initial_source_open":src_now,
            "sell_target":sell_target,
            "buy_reference_open":bp,
            "buy_target":bp,
            "sell_limit_filled":False,"buy_limit_filled":False,
            "sell_fallback":True,"buy_fallback":True,
        }

    # Destination order is created only after source sale; conservatively from next 1H candle.
    buy_start=sell_ts+pd.Timedelta(hours=1)
    if buy_start>=deadline:
        bp=get_open(hourly,dest,deadline)
        return {
            "kind":"SELL_LIMIT_BUY_TIMEOUT_MARKET",
            "sell_ts":sell_ts,"buy_ts":deadline,"complete_ts":deadline,
            "sell_price":sell_target,"buy_price":bp,
            "initial_source_open":src_now,
            "sell_target":sell_target,
            "buy_reference_open":bp,
            "buy_target":bp,
            "sell_limit_filled":True,"buy_limit_filled":False,
            "sell_fallback":False,"buy_fallback":True,
        }

    buy_now=get_open(hourly,dest,buy_start)
    buy_target=marketable_buy_target(buy_now,float(ideal_buy),fraction)

    hd=hourly[dest]
    bf=hd[(hd.index>=buy_start)&(hd.index<deadline)]
    buy_ts=None
    for t,r in bf.iterrows():
        if float(r["low"])-1e-12<=buy_target:
            buy_ts=t
            break

    if buy_ts is None:
        bp=get_open(hourly,dest,deadline)
        return {
            "kind":"SELL_LIMIT_BUY_TIMEOUT_MARKET",
            "sell_ts":sell_ts,"buy_ts":deadline,"complete_ts":deadline,
            "sell_price":sell_target,"buy_price":bp,
            "initial_source_open":src_now,
            "sell_target":sell_target,
            "buy_reference_open":buy_now,
            "buy_target":buy_target,
            "sell_limit_filled":True,"buy_limit_filled":False,
            "sell_fallback":False,"buy_fallback":True,
        }

    return {
        "kind":"FULL_LIMIT",
        "sell_ts":sell_ts,"buy_ts":buy_ts,"complete_ts":buy_ts+pd.Timedelta(hours=1),
        "sell_price":sell_target,"buy_price":buy_target,
        "initial_source_open":src_now,
        "sell_target":sell_target,
        "buy_reference_open":buy_now,
        "buy_target":buy_target,
        "sell_limit_filled":True,"buy_limit_filled":True,
        "sell_fallback":False,"buy_fallback":False,
    }


def build_plan(panel,hourly,signal_i,source,route,fraction,timeout_h):
    ts=pd.DatetimeIndex(panel["timestamp"])
    start_ts=ts[signal_i+1]
    dest=route["effective"]["to_asset"]

    if bool(route["override"]) or route["effective"].get("event")!="CONFIRMED":
        return {
            "kind":"IMMEDIATE_MARKET_DDG",
            "source":source,"destination":dest,"signal_i":signal_i,
            "start_ts":start_ts,"sell_ts":start_ts,"buy_ts":start_ts,"complete_ts":start_ts,
            "sell_price":float(panel.loc[signal_i+1,source+"_open"]),
            "buy_price":float(panel.loc[signal_i+1,dest+"_open"]),
            "sell_limit_filled":False,"buy_limit_filled":False,
            "sell_fallback":True,"buy_fallback":True,
            "applicable":False,
        }

    eff=route["effective"]
    geom=sl.target_geometry(panel,{
        "source":source,"destination":dest,
        "signal_i":signal_i,"extreme_i":int(eff["extreme_i"]),"mode":eff["mode"],
    })
    if not geom.get("exact3_applicable"):
        return {
            "kind":"IMMEDIATE_MARKET_NONAPPLICABLE",
            "source":source,"destination":dest,"signal_i":signal_i,
            "start_ts":start_ts,"sell_ts":start_ts,"buy_ts":start_ts,"complete_ts":start_ts,
            "sell_price":float(panel.loc[signal_i+1,source+"_open"]),
            "buy_price":float(panel.loc[signal_i+1,dest+"_open"]),
            "sell_limit_filled":False,"buy_limit_filled":False,
            "sell_fallback":True,"buy_fallback":True,
            "applicable":False,
        }

    fill=sequential_fraction_fill(
        hourly,source,dest,start_ts,
        float(geom["src_target_exact3"]),float(geom["dst_target_exact3"]),
        float(fraction),int(timeout_h)
    )
    return {
        "source":source,"destination":dest,"signal_i":signal_i,
        "start_ts":start_ts,"deadline":start_ts+pd.Timedelta(hours=timeout_h),
        "ideal_sell_target":float(geom["src_target_exact3"]),
        "ideal_buy_target":float(geom["dst_target_exact3"]),
        "fraction":float(fraction),"timeout_h":int(timeout_h),
        "applicable":True,
        **fill,
    }


def simulate_policy(panel,hourly,events,active,start,end,start_asset,fraction,timeout_h,plan_cache):
    ts=pd.DatetimeIndex(panel["timestamp"])
    si=int(ts.searchsorted(start,"left")); ei=int(ts.searchsorted(end,"right"))-1

    cur=start_asset
    qty=1.0/float(panel.loc[si,cur+"_open"])
    cash=None
    pending=None
    ledger=[]
    equity=[]
    pending_days=0
    rr_opportunities_while_pending=0

    for i in range(si,ei+1):
        day_close=ts[i]+pd.Timedelta(days=1)

        if pending is not None:
            if pending.get("sell_applied") is not True and pending["sell_ts"]<=day_close:
                cash=qty*float(pending["sell_price"])*(1-COST)
                qty=0.0
                pending["sell_applied"]=True

            if pending.get("sell_applied") is True and pending.get("buy_applied") is not True and pending["buy_ts"]<=day_close:
                qty=float(cash)*(1-COST)/float(pending["buy_price"])
                cash=None
                cur=pending["destination"]
                pending["buy_applied"]=True
                ledger.append({
                    **{k:v for k,v in pending.items() if k not in {"sell_applied","buy_applied"}},
                    "complete_day_index":i,
                })
                pending=None

        if pending is not None and pending.get("sell_applied") is True:
            value=float(cash)
        else:
            value=qty*float(panel.loc[i,cur+"_close"])
        equity.append(value)

        if i>=ei:
            continue

        if pending is not None:
            pending_days+=1
            state_asset=pending["destination"] if pending.get("sell_applied") else pending["source"]
            if sl.effective_route(events[i],active[i],state_asset) is not None:
                rr_opportunities_while_pending+=1
            continue

        route=sl.effective_route(events[i],active[i],cur)
        if route is None:
            continue

        key=(i,cur,route["effective"]["to_asset"],float(fraction),int(timeout_h),bool(route["override"]),route["effective"].get("event"))
        if key not in plan_cache:
            plan_cache[key]=build_plan(panel,hourly,i,cur,route,fraction,timeout_h)
        plan=dict(plan_cache[key])
        plan["signal_date"]=ts[i].date().isoformat()
        plan["ddg_override"]=bool(route["override"])
        pending=plan

    eq=np.asarray(equity,float)
    peak=np.maximum.accumulate(eq)
    return {
        "start_asset":start_asset,
        "return":float(eq[-1]/eq[0]-1),
        "max_dd":float(np.min(eq/peak-1)),
        "ledger":ledger,
        "pending_days":pending_days,
        "rr_opportunities_while_pending":rr_opportunities_while_pending,
    }


def summarize_runs(policy,window,runs,fraction=None,timeout_h=None):
    return {
        "policy":policy,"window":window,
        "fraction":fraction,"timeout_h":timeout_h,
        "median_return":float(np.median([r["return"] for r in runs])),
        "worst_return":float(np.min([r["return"] for r in runs])),
        "best_return":float(np.max([r["return"] for r in runs])),
        "median_dd":float(np.median([r["max_dd"] for r in runs])),
        "worst_dd":float(np.min([r["max_dd"] for r in runs])),
        "median_pending_days":float(np.median([r["pending_days"] for r in runs])),
        "median_rr_opportunities_while_pending":float(np.median([r["rr_opportunities_while_pending"] for r in runs])),
        "median_rotations":float(np.median([len(r["ledger"]) for r in runs])),
    }


def main():
    OUT.mkdir(parents=True,exist_ok=True)
    panel,_=fx.download_panel()
    panel["timestamp"]=pd.to_datetime(panel["timestamp"],utc=True)
    events,active=sl.pair_tape_with_extremes(panel)

    hstart=min(v[0] for v in WINDOWS.values())
    hend=fx.CUTOFF+pd.Timedelta(days=2)
    hourly={}
    for asset in fx.U10:
        hourly[asset]=sl.download_hourly(asset,hstart,hend).set_index("timestamp")

    plan_cache={}
    summary_rows=[]
    start_rows=[]
    mature_ledgers=[]

    for w,(start,end) in WINDOWS.items():
        canonical=base.baseline_one_fee(panel,start,end)
        market2=[base.simulate_market_two_fee(panel,events,active,start,end,a) for a in fx.U10]
        summary_rows.append(summarize_runs("CANONICAL_NEXT_OPEN_ONE_FEE",w,[
            {**r,"pending_days":0,"rr_opportunities_while_pending":0,"ledger":[None]*r["transitions"]} for r in canonical
        ]))
        summary_rows.append(summarize_runs("NEXT_OPEN_TWO_FEE_CONTROL",w,[
            {**r,"pending_days":0,"rr_opportunities_while_pending":0,"ledger":[None]*r["transitions"]} for r in market2
        ]))

        for f in FRACTIONS:
            for th in TIMEOUTS_H:
                runs=[simulate_policy(panel,hourly,events,active,start,end,a,f,th,plan_cache) for a in fx.U10]
                summary_rows.append(summarize_runs("FRACTIONAL_RECAPTURE",w,runs,f,th))
                for r,cn,m2 in zip(runs,canonical,market2):
                    start_rows.append({
                        "window":w,"fraction":f,"timeout_h":th,
                        "start_asset":r["start_asset"],
                        "return":r["return"],"max_dd":r["max_dd"],
                        "canonical_return":cn["return"],"market2_return":m2["return"],
                        "factor_vs_canonical":(1+r["return"])/(1+cn["return"]),
                        "factor_vs_market2":(1+r["return"])/(1+m2["return"]),
                        "pending_days":r["pending_days"],
                        "rr_opportunities_while_pending":r["rr_opportunities_while_pending"],
                        "rotations":len(r["ledger"]),
                    })
                    if w=="MATURE":
                        for e in r["ledger"]:
                            if e is not None:
                                mature_ledgers.append({
                                    "start_asset":r["start_asset"],"fraction":f,"timeout_h":th,**e
                                })

    summary=pd.DataFrame(summary_rows)
    summary.to_csv(OUT/"grid_window_summary.csv",index=False)
    starts=pd.DataFrame(start_rows)
    starts.to_csv(OUT/"grid_start_results.csv",index=False)
    led=pd.DataFrame(mature_ledgers)
    led.to_csv(OUT/"mature_execution_ledger.csv",index=False)

    # Unique MATURE execution events per grid cell.
    fill_rows=[]
    if len(led):
        for (f,th),g0 in led.groupby(["fraction","timeout_h"]):
            g=g0.sort_values(["signal_date","source","destination","kind"]).drop_duplicates(
                ["signal_date","source","destination","kind"]
            )
            direct=g[g["applicable"]==True].copy()
            if len(direct)==0:
                continue
            full=direct["kind"]=="FULL_LIMIT"
            improvements=[]
            for _,r in direct.iterrows():
                si=int(r["signal_i"])
                q0=float(panel.loc[si+1,r["source"]+"_open"])/float(panel.loc[si+1,r["destination"]+"_open"])
                q1=float(r["sell_price"])/float(r["buy_price"])
                improvements.append(q1/q0-1.0)
            waits=(pd.to_datetime(direct["complete_ts"],utc=True)-pd.to_datetime(direct["start_ts"],utc=True))/pd.Timedelta(hours=1)
            fill_rows.append({
                "fraction":float(f),"timeout_h":int(th),
                "direct_attempts":int(len(direct)),
                "full_limit_n":int(full.sum()),
                "full_limit_rate":float(full.mean()),
                "sell_timeout_n":int((direct["kind"]=="SELL_TIMEOUT_MARKET_BOTH").sum()),
                "buy_timeout_n":int((direct["kind"]=="SELL_LIMIT_BUY_TIMEOUT_MARKET").sum()),
                "median_completion_h":float(waits.median()),
                "p90_completion_h":float(waits.quantile(.90)),
                "median_exchange_improvement":float(np.median(improvements)),
                "p25_exchange_improvement":float(np.quantile(improvements,.25)),
                "p75_exchange_improvement":float(np.quantile(improvements,.75)),
            })
    fills=pd.DataFrame(fill_rows)
    fills.to_csv(OUT/"mature_fill_and_execution_summary.csv",index=False)

    # Join MATURE path metrics with execution metrics to provide Pareto table; do not declare a winner.
    mature=summary[(summary["window"]=="MATURE")&(summary["policy"]=="FRACTIONAL_RECAPTURE")].copy()
    frontier=mature.merge(fills,on=["fraction","timeout_h"],how="left")
    c=summary[(summary.window=="MATURE")&(summary.policy=="CANONICAL_NEXT_OPEN_ONE_FEE")].iloc[0]
    m=summary[(summary.window=="MATURE")&(summary.policy=="NEXT_OPEN_TWO_FEE_CONTROL")].iloc[0]
    frontier["capital_factor_vs_canonical"]=(1+frontier["median_return"])/(1+c["median_return"])
    frontier["capital_factor_vs_market2"]=(1+frontier["median_return"])/(1+m["median_return"])
    frontier=frontier.sort_values(["timeout_h","fraction"])
    frontier.to_csv(OUT/"mature_tradeoff_frontier.csv",index=False)

    # Best cells by each descriptive axis only; no production selection.
    descriptive={}
    if len(frontier):
        rret=frontier.loc[frontier["median_return"].idxmax()]
        rdd=frontier.loc[frontier["median_dd"].idxmax()]
        rfill=frontier.loc[frontier["full_limit_rate"].idxmax()]
        descriptive={
            "highest_mature_return":{"fraction":float(rret.fraction),"timeout_h":int(rret.timeout_h),"median_return":float(rret.median_return)},
            "least_negative_mature_dd":{"fraction":float(rdd.fraction),"timeout_h":int(rdd.timeout_h),"median_dd":float(rdd.median_dd)},
            "highest_full_limit_rate":{"fraction":float(rfill.fraction),"timeout_h":int(rfill.timeout_h),"full_limit_rate":float(rfill.full_limit_rate)},
        }

    payload={
        "experiment":"FRACTIONAL_RECAPTURE_GRID_V1",
        "workflow_mode":"STRESS_TEST_ONLY",
        "production_changes":"NONE",
        "fractions":list(FRACTIONS),
        "timeouts_h":list(TIMEOUTS_H),
        "formula":"target = current + fraction*(ideal-current); buy current is sampled only after source sell, from next eligible 1H candle",
        "ordering":"SELL FIRST; BUY FROM NEXT 1H CANDLE",
        "fallback":"force remaining transfer at timeout hourly-open market proxy",
        "fees":"0.1% sell + 0.1% buy",
        "descriptive_extrema_not_selection":descriptive,
        "test_level":"GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    }
    (OUT/"summary.json").write_text(json.dumps(payload,indent=2,sort_keys=True,default=str)+"\n",encoding="utf-8")

    lines=[
        "# FRACTIONAL RECAPTURE GRID V1","",
        "Mode: STRESS_TEST_ONLY","",
        "Frozen grid: 20/40/60/80/100% of the price distance to the exact-3% ideal target.",
        "Hard fallback grid: 3h / 6h / 12h / 24h.",
        "SELL must fill first. BUY target is calculated only after SELL, from the next eligible 1H candle.","",
        "## MATURE trade-off frontier","",
        "|Fraction|Timeout|Median return|Median DD|Full limit fill|Median execution improvement|Pending days|RR opps while pending|",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _,r in frontier.iterrows():
        lines.append(
            f"|{100*r['fraction']:.0f}%|{int(r['timeout_h'])}h|{100*r['median_return']:+.1f}%|"
            f"{100*r['median_dd']:+.1f}%|{100*r['full_limit_rate']:.1f}%|"
            f"{100*r['median_exchange_improvement']:+.2f}%|"
            f"{r['median_pending_days']:.1f}|{r['median_rr_opportunities_while_pending']:.1f}|"
        )

    lines += ["","## Controls","",
              f"- Canonical MATURE: {100*c['median_return']:+.1f}%, DD {100*c['median_dd']:+.1f}%",
              f"- Two-fee next-open MATURE: {100*m['median_return']:+.1f}%, DD {100*m['median_dd']:+.1f}%",
              "",
              "## Guardrails",
              "- Grid was fixed before results; no winner is production-selected from this same history.",
              "- Fraction is literal leg-price interpolation, matching the user example 14.50 -> 14.00; 60% = 14.20.",
              "- BUY fraction is measured from the destination price after source SELL, not from a future-known price.",
              "- Hourly wick touch is an optimistic fill proxy.",
              "- DDG/non-applicable transitions remain immediate next-open.",
              "- No production/live/paper/Telegram/exchange behavior changed.",
              "",
              "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST"]
    (OUT/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")
    print("\n".join(lines))


if __name__=="__main__":
    main()
