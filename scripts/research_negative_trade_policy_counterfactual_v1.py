from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

import scripts.research_negative_trade_forensics_v1 as fx

OUT = Path("research_artifacts/negative_trade_policy_counterfactual_v1")

WINDOWS = {
    "DISCOVERY": (pd.Timestamp("2023-10-31",tz="UTC"),pd.Timestamp("2025-09-26",tz="UTC")),
    "VALIDATION_1Y": (pd.Timestamp("2025-09-27",tz="UTC"),pd.Timestamp("2026-09-26",tz="UTC")),
    "LAST_2Y": (pd.Timestamp("2024-09-27",tz="UTC"),pd.Timestamp("2026-09-26",tz="UTC")),
    "MATURE": (pd.Timestamp("2023-10-31",tz="UTC"),pd.Timestamp("2026-09-26",tz="UTC")),
}

POLICIES = (
    "BASE_DDG",
    "SCORE_GE3_HOLD",
    "SCORE_GE4_HOLD",
    "RR_NETWORK_MARKET_HOLD",
    "LT20_TOTAL_NOT_BULL_HOLD",
    "LT20_BROAD_BEAR_HOLD",
    "LOW_BREADTH_TOTAL_NOT_BULL_HOLD",
)


def signal_context(panel, market, i, events, active, route):
    ts = pd.Timestamp(panel.loc[i,"timestamp"])
    f = fx.route_features(panel,i,events,active,route)
    flags = fx.add_hold_rule_flags(f)
    row={**f,**{f"rule_{k}":bool(v) for k,v in flags.items()}}
    if ts in market.index:
        mr=market.loc[ts]
        for col in [
            "btc_above_sma200","btc_trend","broad_crypto_trend",
            "defensive_low_breadth_persist3","total_bull","total_bear",
            "btceth_above200_persist3","risk_total_notbull_ethbtc_below100",
            "combo_risk_200","eth_btc_vs_sma100",
        ]:
            row[col]=mr.get(col)

    row["flag_reversal_lt5"]=float(row.get("primary_reversal",0))<0.05
    row["flag_btc_below_sma200"]=not bool(row.get("btc_above_sma200",False))
    row["flag_btc_bear"]=str(row.get("btc_trend"))=="BTC_BEAR"
    row["flag_broad_bear"]=str(row.get("broad_crypto_trend"))=="BROAD_BEAR"
    row["flag_defensive_low_breadth"]=bool(row.get("defensive_low_breadth_persist3",False))
    row["flag_total_not_bull"]=not bool(row.get("total_bull",False))
    row["flag_total_bear"]=bool(row.get("total_bear",False))
    row["flag_btceth_long200"]=bool(row.get("btceth_above200_persist3",False))
    row["flag_ethbtc_below_sma100"]=float(row.get("eth_btc_vs_sma100",np.nan))<0 if pd.notna(row.get("eth_btc_vs_sma100",np.nan)) else False
    row["flag_high_attention"]=bool(row.get("risk_total_notbull_ethbtc_below100",False))
    row["flag_combo_risk200"]=bool(row.get("combo_risk_200",False))

    row["domain_rr_caution"]=row["flag_reversal_lt5"] or row["rule_S2_SIGNAL_LT20"] or row["rule_S3_SIGNAL_LT25"]
    row["domain_network_caution"]=(
        row["rule_C1_NO_INDEPENDENT_SUPPORT"]
        or row["rule_M1_DEST_WORSE_SOURCE_30_60_90"]
        or row["rule_M2_DEST_BOTTOM_HALF_30_60_90"]
        or row["rule_M3_DEST_RANK_WORSE_SOURCE_ALL"]
    )
    row["domain_market_caution"]=row["flag_btc_below_sma200"] or row["flag_broad_bear"] or row["flag_defensive_low_breadth"]
    row["domain_total_ratio_caution"]=row["flag_high_attention"] or row["flag_combo_risk200"] or row["flag_btceth_long200"]
    row["warning_domain_count"]=sum(bool(row[x]) for x in (
        "domain_rr_caution","domain_network_caution","domain_market_caution","domain_total_ratio_caution"
    ))
    return row


def veto(policy,c):
    if policy=="BASE_DDG":
        return False
    if policy=="SCORE_GE3_HOLD":
        return int(c["warning_domain_count"])>=3
    if policy=="SCORE_GE4_HOLD":
        return int(c["warning_domain_count"])>=4
    if policy=="RR_NETWORK_MARKET_HOLD":
        return bool(c["domain_rr_caution"] and c["domain_network_caution"] and c["domain_market_caution"])
    if policy=="LT20_TOTAL_NOT_BULL_HOLD":
        return bool(c["rule_S2_SIGNAL_LT20"] and c["flag_total_not_bull"])
    if policy=="LT20_BROAD_BEAR_HOLD":
        return bool(c["rule_S2_SIGNAL_LT20"] and c["flag_broad_bear"])
    if policy=="LOW_BREADTH_TOTAL_NOT_BULL_HOLD":
        return bool(c["flag_defensive_low_breadth"] and c["flag_total_not_bull"])
    raise ValueError(policy)


def bounds(ts,start,end):
    return int(ts.searchsorted(start,"left")),int(ts.searchsorted(end,"right"))-1


def simulate(panel,events,active,market,start,end,start_asset,policy):
    ts=pd.DatetimeIndex(panel["timestamp"])
    si,ei=bounds(ts,start,end)
    cur=start_asset
    qty=1.0/float(panel.loc[si,cur+"_open"])
    initial=qty*float(panel.loc[si,cur+"_close"])
    eq=np.empty(ei-si+1,float)
    pending=None
    transitions=0
    vetoes=0
    veto_ledger=[]

    for i in range(si,ei+1):
        if pending is not None:
            value=qty*float(panel.loc[i,cur+"_open"])
            nxt=pending["to_asset"]
            qty=value*(1-fx.COST)/float(panel.loc[i,nxt+"_open"])
            cur=nxt
            transitions+=1
            pending=None

        eq[i-si]=qty*float(panel.loc[i,cur+"_close"])
        if i>=ei:
            continue
        route=fx.effective_route(events[i],active[i],cur)
        if route is None:
            continue
        c=signal_context(panel,market,i,events[i],active[i],route)
        if veto(policy,c):
            vetoes+=1
            veto_ledger.append({
                "signal_date":ts[i].date().isoformat(),
                "source":cur,"destination":route["effective"]["to_asset"],
                "warning_domain_count":c["warning_domain_count"],
            })
            continue
        pending={"to_asset":route["effective"]["to_asset"]}

    peak=np.maximum.accumulate(eq)
    return {
        "return":float(eq[-1]/initial-1),
        "max_dd":float(np.min(eq/peak-1)),
        "transitions":transitions,
        "vetoes":vetoes,
        "veto_ledger":veto_ledger,
    }


def main():
    OUT.mkdir(parents=True,exist_ok=True)
    panel,_=fx.download_panel()
    panel["timestamp"]=pd.to_datetime(panel["timestamp"],utc=True)
    events,active=fx.pair_daily_tape(panel,fx.REVERSAL,1)
    market,_=fx.market_state(panel)

    rows=[]
    ledgers=[]
    for policy in POLICIES:
        for w,(start,end) in WINDOWS.items():
            runs=[simulate(panel,events,active,market,start,end,a,policy) for a in fx.U10]
            arr=np.array([r["return"] for r in runs],float)
            dds=np.array([r["max_dd"] for r in runs],float)
            rows.append({
                "policy":policy,"window":w,
                "median_return":float(np.median(arr)),
                "worst_return":float(arr.min()),
                "best_return":float(arr.max()),
                "median_dd":float(np.median(dds)),
                "worst_dd":float(dds.min()),
                "median_transitions":float(np.median([r["transitions"] for r in runs])),
                "median_vetoes":float(np.median([r["vetoes"] for r in runs])),
            })
            if w=="MATURE":
                seen=Counter()
                for start_asset,r in zip(fx.U10,runs):
                    for e in r["veto_ledger"]:
                        key=(e["signal_date"],e["source"],e["destination"])
                        seen[key]+=1
                for key,count in seen.items():
                    ledgers.append({
                        "policy":policy,"signal_date":key[0],"source":key[1],"destination":key[2],"start_count":count
                    })

    df=pd.DataFrame(rows)
    df.to_csv(OUT/"policy_windows.csv",index=False)
    pd.DataFrame(ledgers).to_csv(OUT/"policy_veto_ledger.csv",index=False)

    base=df[df.policy=="BASE_DDG"].set_index("window")
    comparisons=[]
    for policy in POLICIES:
        if policy=="BASE_DDG": continue
        sub=df[df.policy==policy].set_index("window")
        for w in WINDOWS:
            comparisons.append({
                "policy":policy,"window":w,
                "return_delta_pp":100*(sub.loc[w,"median_return"]-base.loc[w,"median_return"]),
                "dd_delta_pp":100*(sub.loc[w,"median_dd"]-base.loc[w,"median_dd"]),
                "median_return":sub.loc[w,"median_return"],
                "median_dd":sub.loc[w,"median_dd"],
                "median_vetoes":sub.loc[w,"median_vetoes"],
            })
    cmp=pd.DataFrame(comparisons)
    cmp.to_csv(OUT/"policy_vs_base.csv",index=False)

    summary={
        "experiment":"NEGATIVE_TRADE_POLICY_COUNTERFACTUAL_V1",
        "workflow_mode":"STRESS_TEST_ONLY",
        "classification":"POST_SELECTION_EXPLORATORY_COUNTERFACTUAL",
        "production_changes":"NONE",
        "results":json.loads(df.to_json(orient="records")),
    }
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True)+"\n",encoding="utf-8")

    lines=[
        "# NEGATIVE TRADE COMPOSITE POLICY COUNTERFACTUAL V1","",
        "POST-SELECTION EXPLORATORY ONLY — NOT OOS / NOT PRODUCTION APPROVAL","",
        "|Policy|Discovery|Validation 1Y|2Y|Mature|Mature DD|Median vetoes Mature|",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for policy in POLICIES:
        s=df[df.policy==policy].set_index("window")
        lines.append(
            f"|{policy}|{100*s.loc['DISCOVERY','median_return']:+.1f}%|{100*s.loc['VALIDATION_1Y','median_return']:+.1f}%|"
            f"{100*s.loc['LAST_2Y','median_return']:+.1f}%|{100*s.loc['MATURE','median_return']:+.1f}%|"
            f"{100*s.loc['MATURE','median_dd']:+.1f}%|{s.loc['MATURE','median_vetoes']:.1f}|"
        )
    lines += ["","No production/live/Telegram changes.","","TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST"]
    (OUT/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")
    print("\n".join(lines))


if __name__=="__main__":
    from collections import Counter
    main()
