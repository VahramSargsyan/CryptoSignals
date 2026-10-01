from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

import scripts.research_negative_trade_forensics_v1 as fx
import scripts.research_sequential_limit_recapture_v1 as sl
import scripts.research_sequential_limit_7d_fallback_v1 as fixed

OUT=Path("research_artifacts/sequential_limit_trailing_3bar_v1")
COST=0.001
TIMEOUT_H=168
TRAIL_DAYS=3

WINDOWS=fixed.WINDOWS


def recent_three_day_extreme(hourly_df, ts, side):
    """Use only the previous 3 fully completed UTC days."""
    start=ts-pd.Timedelta(days=TRAIL_DAYS)
    g=hourly_df[(hourly_df.index>=start)&(hourly_df.index<ts)]
    if len(g)==0:
        return None
    if side=="SELL":
        return float(g["high"].max())
    if side=="BUY":
        return float(g["low"].min())
    raise ValueError(side)


def get_open(hourly,asset,ts):
    return fixed.get_open(hourly,asset,ts)


def simulate_trailing_fill(hourly,source,destination,start_ts,sell_target,buy_target):
    deadline=start_ts+pd.Timedelta(hours=TIMEOUT_H)
    hs=hourly[source]
    hd=hourly[destination]

    cur_sell=float(sell_target)
    sell_updates=[]
    sell_ts=None
    sell_px=None

    sf=hs[(hs.index>=start_ts)&(hs.index<deadline)]
    for t,r in sf.iterrows():
        # At each new UTC day, tighten using the previous 3 complete days.
        if t>start_ts and t.hour==0:
            ext=recent_three_day_extreme(hs,t,"SELL")
            if ext is not None:
                new=min(cur_sell,float(ext))
                if new < cur_sell-1e-12:
                    sell_updates.append({
                        "ts":t.isoformat(),
                        "old":cur_sell,
                        "new":new,
                        "three_day_high":float(ext),
                    })
                    cur_sell=new

        if float(r["high"])+1e-12>=cur_sell:
            sell_ts=t
            sell_px=cur_sell
            break

    if sell_ts is None:
        return {
            "kind":"TRAIL_SELL_TIMEOUT_MARKET_BOTH",
            "sell_ts":deadline,"buy_ts":deadline,"complete_ts":deadline,
            "sell_price":get_open(hourly,source,deadline),
            "buy_price":get_open(hourly,destination,deadline),
            "sell_limit_filled":False,"buy_limit_filled":False,
            "sell_fallback":True,"buy_fallback":True,
            "sell_updates_n":len(sell_updates),"buy_updates_n":0,
            "sell_updates_json":json.dumps(sell_updates,sort_keys=True),
            "buy_updates_json":"[]",
            "final_sell_limit":cur_sell,
            "final_buy_limit":float(buy_target),
        }

    # BUY order exists only from the next 1H candle after SELL fill.
    buy_start=sell_ts+pd.Timedelta(hours=1)
    cur_buy=float(buy_target)
    buy_updates=[]
    buy_ts=None
    buy_px=None

    bf=hd[(hd.index>=buy_start)&(hd.index<deadline)]
    for t,r in bf.iterrows():
        if t.hour==0:
            ext=recent_three_day_extreme(hd,t,"BUY")
            if ext is not None:
                new=max(cur_buy,float(ext))
                if new > cur_buy+1e-12:
                    buy_updates.append({
                        "ts":t.isoformat(),
                        "old":cur_buy,
                        "new":new,
                        "three_day_low":float(ext),
                    })
                    cur_buy=new

        if float(r["low"])-1e-12<=cur_buy:
            buy_ts=t
            buy_px=cur_buy
            break

    if buy_ts is None:
        return {
            "kind":"TRAIL_SELL_LIMIT_BUY_TIMEOUT_MARKET",
            "sell_ts":sell_ts,"buy_ts":deadline,"complete_ts":deadline,
            "sell_price":sell_px,
            "buy_price":get_open(hourly,destination,deadline),
            "sell_limit_filled":True,"buy_limit_filled":False,
            "sell_fallback":False,"buy_fallback":True,
            "sell_updates_n":len(sell_updates),"buy_updates_n":len(buy_updates),
            "sell_updates_json":json.dumps(sell_updates,sort_keys=True),
            "buy_updates_json":json.dumps(buy_updates,sort_keys=True),
            "final_sell_limit":cur_sell,
            "final_buy_limit":cur_buy,
        }

    return {
        "kind":"TRAIL_FULL_LIMIT",
        "sell_ts":sell_ts,"buy_ts":buy_ts,"complete_ts":buy_ts+pd.Timedelta(hours=1),
        "sell_price":sell_px,"buy_price":buy_px,
        "sell_limit_filled":True,"buy_limit_filled":True,
        "sell_fallback":False,"buy_fallback":False,
        "sell_updates_n":len(sell_updates),"buy_updates_n":len(buy_updates),
        "sell_updates_json":json.dumps(sell_updates,sort_keys=True),
        "buy_updates_json":json.dumps(buy_updates,sort_keys=True),
        "final_sell_limit":cur_sell,
        "final_buy_limit":cur_buy,
    }


def build_plan(panel,hourly,events,active,signal_i,source,route):
    ts=pd.DatetimeIndex(panel["timestamp"])
    start_ts=ts[signal_i+1]
    dest=route["effective"]["to_asset"]

    if bool(route["override"]) or route["effective"].get("event")!="CONFIRMED":
        return {
            "kind":"IMMEDIATE_MARKET_DDG",
            "source":source,"destination":dest,"signal_i":signal_i,
            "start_ts":start_ts,
            "sell_ts":start_ts,"buy_ts":start_ts,"complete_ts":start_ts,
            "sell_price":float(panel.loc[signal_i+1,source+"_open"]),
            "buy_price":float(panel.loc[signal_i+1,dest+"_open"]),
            "sell_limit_filled":False,"buy_limit_filled":False,
            "sell_fallback":True,"buy_fallback":True,
            "sell_updates_n":0,"buy_updates_n":0,
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
            "start_ts":start_ts,
            "sell_ts":start_ts,"buy_ts":start_ts,"complete_ts":start_ts,
            "sell_price":float(panel.loc[signal_i+1,source+"_open"]),
            "buy_price":float(panel.loc[signal_i+1,dest+"_open"]),
            "sell_limit_filled":False,"buy_limit_filled":False,
            "sell_fallback":True,"buy_fallback":True,
            "sell_updates_n":0,"buy_updates_n":0,
        }

    fill=simulate_trailing_fill(
        hourly,source,dest,start_ts,
        float(geom["src_target_exact3"]),float(geom["dst_target_exact3"])
    )
    return {
        "source":source,"destination":dest,"signal_i":signal_i,
        "start_ts":start_ts,"deadline":start_ts+pd.Timedelta(hours=TIMEOUT_H),
        "initial_sell_target":float(geom["src_target_exact3"]),
        "initial_buy_target":float(geom["dst_target_exact3"]),
        **fill,
    }


def simulate_policy(panel,hourly,events,active,start,end,start_asset):
    ts=pd.DatetimeIndex(panel["timestamp"])
    si=int(ts.searchsorted(start,"left"))
    ei=int(ts.searchsorted(end,"right"))-1

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
            if pending.get("sell_applied") is not True and pending["sell_ts"] < day_close:
                cash=qty*float(pending["sell_price"])*(1-COST)
                qty=0.0
                pending["sell_applied"]=True

            if pending.get("sell_applied") is True and pending.get("buy_applied") is not True and pending["buy_ts"] < day_close:
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
            pending_days += 1
            # Count a causal RR opportunity from the intended state:
            # source before sell; destination after sell.
            state_asset=pending["destination"] if pending.get("sell_applied") is True else pending["source"]
            rr_pending=sl.effective_route(events[i],active[i],state_asset)
            if rr_pending is not None:
                rr_opportunities_while_pending += 1
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
    peak=np.maximum.accumulate(eq)
    return {
        "start_asset":start_asset,
        "return":float(eq[-1]/eq[0]-1),
        "max_dd":float(np.min(eq/peak-1)),
        "ledger":ledger,
        "pending_days":pending_days,
        "rr_opportunities_while_pending":rr_opportunities_while_pending,
    }


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
    hstart=min(v[0] for v in WINDOWS.values())-pd.Timedelta(days=3)
    hend=fx.CUTOFF+pd.Timedelta(days=8)
    for asset in fx.U10:
        hourly[asset]=sl.download_hourly(asset,hstart,hend).set_index("timestamp")

    summary_rows=[]
    start_rows=[]
    mature_ledgers=[]

    for w,(start,end) in WINDOWS.items():
        trail=[simulate_policy(panel,hourly,events,active,start,end,a) for a in fx.U10]
        fixed7=[fixed.simulate_policy(panel,hourly,events,active,start,end,a) for a in fx.U10]
        market2=[fixed.simulate_market_two_fee(panel,events,active,start,end,a) for a in fx.U10]
        canonical=fixed.baseline_one_fee(panel,start,end)

        summary_rows += [
            summarize_runs("CANONICAL_NEXT_OPEN_ONE_FEE",w,canonical),
            summarize_runs("NEXT_OPEN_TWO_FEE_CONTROL",w,market2),
            summarize_runs("FIXED_EXACT3_7D_FALLBACK",w,fixed7),
            summarize_runs("TRAIL_3DAILY_EXTREME_7D_FALLBACK",w,trail),
        ]

        for tr,fx7,m2,cn in zip(trail,fixed7,market2,canonical):
            start_rows.append({
                "window":w,"start_asset":tr["start_asset"],
                "trail_return":tr["return"],"trail_dd":tr["max_dd"],
                "fixed7_return":fx7["return"],"fixed7_dd":fx7["max_dd"],
                "market2_return":m2["return"],"market2_dd":m2["max_dd"],
                "canonical_return":cn["return"],"canonical_dd":cn["max_dd"],
                "trail_vs_fixed7_factor":(1+tr["return"])/(1+fx7["return"]),
                "trail_vs_market2_factor":(1+tr["return"])/(1+m2["return"]),
                "trail_vs_canonical_factor":(1+tr["return"])/(1+cn["return"]),
                "trail_pending_days":tr["pending_days"],
                "trail_rr_opportunities_while_pending":tr["rr_opportunities_while_pending"],
                "trail_rotations":len(tr["ledger"]),
            })
            if w=="MATURE":
                for e in tr["ledger"]:
                    mature_ledgers.append({"start_asset":tr["start_asset"],**e})

    summary=pd.DataFrame(summary_rows)
    summary.to_csv(OUT/"window_summary.csv",index=False)
    starts=pd.DataFrame(start_rows)
    starts.to_csv(OUT/"start_level_results.csv",index=False)

    led=pd.DataFrame(mature_ledgers)
    led.to_csv(OUT/"mature_execution_ledger.csv",index=False)

    if len(led):
        unique=led.sort_values(["signal_date","source","destination","kind"]).drop_duplicates(
            ["signal_date","source","destination","kind"]
        )
    else:
        unique=led
    unique.to_csv(OUT/"mature_unique_execution_events.csv",index=False)

    direct=unique[unique["kind"].isin([
        "TRAIL_FULL_LIMIT","TRAIL_SELL_TIMEOUT_MARKET_BOTH","TRAIL_SELL_LIMIT_BUY_TIMEOUT_MARKET"
    ])] if len(unique) else unique

    fill_stats={
        "unique_direct_attempts":int(len(direct)),
        "full_limit_n":int((direct["kind"]=="TRAIL_FULL_LIMIT").sum()) if len(direct) else 0,
        "full_limit_rate":float((direct["kind"]=="TRAIL_FULL_LIMIT").mean()) if len(direct) else None,
        "sell_timeout_n":int((direct["kind"]=="TRAIL_SELL_TIMEOUT_MARKET_BOTH").sum()) if len(direct) else 0,
        "buy_timeout_after_sell_n":int((direct["kind"]=="TRAIL_SELL_LIMIT_BUY_TIMEOUT_MARKET").sum()) if len(direct) else 0,
        "median_sell_updates":float(direct["sell_updates_n"].median()) if len(direct) else None,
        "median_buy_updates":float(direct["buy_updates_n"].median()) if len(direct) else None,
        "median_completion_hours":float(
            ((pd.to_datetime(direct["complete_ts"],utc=True)-pd.to_datetime(direct["start_ts"],utc=True))/pd.Timedelta(hours=1)).median()
        ) if len(direct) else None,
    }

    # Actual realized exchange-rate improvement per direct execution vs immediate next-open.
    improvements=[]
    for _,r in direct.iterrows():
        si=int(r["signal_i"])
        source=r["source"]; dest=r["destination"]
        baseline_q=float(panel.loc[si+1,source+"_open"])/float(panel.loc[si+1,dest+"_open"])
        actual_q=float(r["sell_price"])/float(r["buy_price"])
        improvements.append(actual_q/baseline_q-1.0)
    if improvements:
        fill_stats["median_realized_exchange_improvement_vs_next_open"]=float(np.median(improvements))
        fill_stats["p25_realized_exchange_improvement"]=float(np.quantile(improvements,.25))
        fill_stats["p75_realized_exchange_improvement"]=float(np.quantile(improvements,.75))

    impact=[]
    for w,g in starts.groupby("window"):
        impact.append({
            "window":w,
            "median_trail_vs_fixed7_factor":float(g["trail_vs_fixed7_factor"].median()),
            "median_trail_vs_market2_factor":float(g["trail_vs_market2_factor"].median()),
            "median_trail_vs_canonical_factor":float(g["trail_vs_canonical_factor"].median()),
            "median_pending_days":float(g["trail_pending_days"].median()),
            "median_rr_opportunities_while_pending":float(g["trail_rr_opportunities_while_pending"].median()),
            "median_rotations":float(g["trail_rotations"].median()),
        })
    impact=pd.DataFrame(impact)
    impact.to_csv(OUT/"capital_impact.csv",index=False)

    payload={
        "experiment":"SEQUENTIAL_LIMIT_TRAILING_3BAR_V1",
        "workflow_mode":"STRESS_TEST_ONLY",
        "production_changes":"NONE",
        "policy":{
            "initial_target":"EXACT_3PCT_LOG",
            "sell_tightening":"once per UTC day: min(previous sell limit, highest hourly high from previous 3 complete UTC days)",
            "buy_tightening":"after sell, once per UTC day: max(previous buy limit, lowest hourly low from previous 3 complete UTC days)",
            "monotonic":"sell only down; buy only up",
            "same_hour_ordering":"buy eligible from hour after sell",
            "timeout_hours":168,
            "fallback":"force remaining transfer at hourly-open market proxy",
            "fees":"0.1% sell + 0.1% buy",
        },
        "fill_stats":fill_stats,
        "window_summary":json.loads(summary.to_json(orient="records")),
        "capital_impact":json.loads(impact.to_json(orient="records")),
        "test_level":"GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    }
    (OUT/"summary.json").write_text(json.dumps(payload,indent=2,sort_keys=True,default=str)+"\n",encoding="utf-8")

    lines=[
        "# SEQUENTIAL LIMIT TRAILING 3-BAR V1","",
        "Mode: STRESS_TEST_ONLY","",
        "Frozen rule: start at exact-3% targets, tighten once daily using extrema of the previous 3 fully closed UTC days.",
        "SELL can only move down; BUY can only move up. Hard fallback remains 168h.","",
        "## Full path-dependent return","",
        "|Window|Policy|Median return|Worst start|Median DD|Worst DD|",
        "|---|---|---:|---:|---:|---:|",
    ]
    for _,r in summary.iterrows():
        lines.append(
            f"|{r['window']}|{r['policy']}|{100*r['median_return']:+.1f}%|"
            f"{100*r['worst_return']:+.1f}%|{100*r['median_dd']:+.1f}%|{100*r['worst_dd']:+.1f}%|"
        )

    lines += ["","## Trailing capital impact","",
              "|Window|vs fixed-7d|vs two-fee next-open|vs canonical|Pending days|RR opportunities while pending|Rotations|",
              "|---|---:|---:|---:|---:|---:|---:|"]
    for _,r in impact.iterrows():
        lines.append(
            f"|{r['window']}|{r['median_trail_vs_fixed7_factor']:.3f}x|"
            f"{r['median_trail_vs_market2_factor']:.3f}x|{r['median_trail_vs_canonical_factor']:.3f}x|"
            f"{r['median_pending_days']:.1f}|{r['median_rr_opportunities_while_pending']:.1f}|"
            f"{r['median_rotations']:.1f}|"
        )

    lines += ["","## MATURE direct execution","",
              f"- direct attempts: {fill_stats['unique_direct_attempts']}",
              f"- full limit completion: {fill_stats['full_limit_n']} ({100*fill_stats['full_limit_rate']:.1f}%)" if fill_stats["full_limit_rate"] is not None else "- none",
              f"- sell timeout: {fill_stats['sell_timeout_n']}",
              f"- buy timeout after sell: {fill_stats['buy_timeout_after_sell_n']}",
              f"- median sell tightenings: {fill_stats['median_sell_updates']}",
              f"- median buy tightenings: {fill_stats['median_buy_updates']}",
              f"- median completion: {fill_stats['median_completion_hours']:.1f}h" if fill_stats["median_completion_hours"] is not None else "- n/a",
              f"- median realized exchange-rate improvement vs immediate next-open: {100*fill_stats.get('median_realized_exchange_improvement_vs_next_open',float('nan')):+.2f}%",
              "",
              "## Guardrails",
              "- Three-bar rule was frozen before results; no 2/5-bar tuning in this pass.",
              "- Daily extrema use only fully completed UTC days.",
              "- Hourly wick touch is still an optimistic fill proxy.",
              "- Market fallback uses hourly open at 168h.",
              "- DDG/non-applicable transitions remain immediate next-open.",
              "- No production/live/paper/Telegram/exchange behavior changed.",
              "",
              "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST"]
    (OUT/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")
    print("\n".join(lines))


if __name__=="__main__":
    main()
