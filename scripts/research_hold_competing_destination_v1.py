from __future__ import annotations

import itertools
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient

OUT = Path("research_artifacts/hold_competing_destination_v1")

ASSETS = ("TWT","PEPE","BNB","TRX","AAVE","AVAX","FIL","ALGO","XRP","HBAR")
LOOKBACK = 180
ARM = 0.15
REVERSAL = 0.03
COST = 0.001
DDG_RATIO = 1.50

DATA_START = pd.Timestamp("2023-05-05", tz="UTC")
CUTOFF = pd.Timestamp("2026-09-27", tz="UTC")
WINDOWS = {
    "DISCOVERY": (pd.Timestamp("2023-10-31", tz="UTC"), pd.Timestamp("2025-09-26", tz="UTC")),
    "VALIDATION_1Y": (pd.Timestamp("2025-09-27", tz="UTC"), pd.Timestamp("2026-09-26", tz="UTC")),
    "MATURE": (pd.Timestamp("2023-10-31", tz="UTC"), pd.Timestamp("2026-09-26", tz="UTC")),
}

EXPECTED_DDG_MATURE = 35.8836

@dataclass
class State:
    mode: str = "NONE"
    extreme: float | None = None
    max_dislocation: float = 0.0
    armed_at: int | None = None


def utc(v):
    t = pd.Timestamp(v)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def download_panel():
    client = BinanceSpotRestClient()
    panel = None
    meta = {}
    for asset in ASSETS:
        result = download_historical_dataset(
            client,
            symbol=asset+"USDT",
            start=DATA_START,
            end=CUTOFF,
            timeframe="1D",
            as_of=CUTOFF,
        )
        if result.dataset is None:
            raise RuntimeError(f"{asset}: no dataset ({result.metadata.status})")
        if result.dataset.quality.has_critical_issues:
            raise RuntimeError(f"{asset}: critical candle quality issue")
        f = result.dataset.candles[["timestamp","open","close"]].copy()
        f["timestamp"] = pd.to_datetime(f["timestamp"], utc=True)
        f = f.rename(columns={"open":asset+"_open","close":asset+"_close"})
        meta[asset] = {
            "rows": int(len(f)),
            "start": pd.Timestamp(f.iloc[0]["timestamp"]).isoformat(),
            "end": pd.Timestamp(f.iloc[-1]["timestamp"]).isoformat(),
        }
        panel = f if panel is None else panel.merge(f,on="timestamp",how="inner",validate="one_to_one")
    panel = panel.sort_values("timestamp").reset_index(drop=True)
    return panel, meta


def pair_daily_tape(panel):
    n = len(panel)
    daily_events = [[] for _ in range(n)]
    daily_active = [[] for _ in range(n)]

    for left,right in itertools.combinations(ASSETS,2):
        ratio = panel[right+"_close"].astype(float) / panel[left+"_close"].astype(float)
        median = ratio.rolling(LOOKBACK,min_periods=LOOKBACK).median()
        dev = ratio/median - 1.0
        st = State()

        for i in range(n):
            if pd.isna(median.iloc[i]) or pd.isna(dev.iloc[i]):
                continue
            r = float(ratio.iloc[i])
            d = float(dev.iloc[i])

            if st.mode=="NONE":
                if d>=ARM:
                    st=State("HIGH",r,abs(d),i)
                    daily_events[i].append({
                        "event":"ARMED","pair":f"{left}/{right}",
                        "from_asset":right,"to_asset":left,
                        "max_dislocation":abs(d),"deviation":d,
                    })
                elif d<=-ARM:
                    st=State("LOW",r,abs(d),i)
                    daily_events[i].append({
                        "event":"ARMED","pair":f"{left}/{right}",
                        "from_asset":left,"to_asset":right,
                        "max_dislocation":abs(d),"deviation":d,
                    })
            elif st.mode=="HIGH":
                if r>float(st.extreme):
                    st.extreme=r
                    st.max_dislocation=max(st.max_dislocation,abs(d))
                retr=1.0-r/float(st.extreme)
                if retr>=REVERSAL:
                    daily_events[i].append({
                        "event":"CONFIRMED","pair":f"{left}/{right}",
                        "from_asset":right,"to_asset":left,
                        "max_dislocation":st.max_dislocation,"deviation":d,
                        "reversal_from_extreme":retr,
                    })
                    st=State()
            elif st.mode=="LOW":
                if r<float(st.extreme):
                    st.extreme=r
                    st.max_dislocation=max(st.max_dislocation,abs(d))
                retr=r/float(st.extreme)-1.0
                if retr>=REVERSAL:
                    daily_events[i].append({
                        "event":"CONFIRMED","pair":f"{left}/{right}",
                        "from_asset":left,"to_asset":right,
                        "max_dislocation":st.max_dislocation,"deviation":d,
                        "reversal_from_extreme":retr,
                    })
                    st=State()

            if st.mode=="HIGH":
                daily_active[i].append({
                    "event":"ARMED","pair":f"{left}/{right}",
                    "from_asset":right,"to_asset":left,
                    "max_dislocation":st.max_dislocation,
                })
            elif st.mode=="LOW":
                daily_active[i].append({
                    "event":"ARMED","pair":f"{left}/{right}",
                    "from_asset":left,"to_asset":right,
                    "max_dislocation":st.max_dislocation,
                })

    return daily_events, daily_active


def better(existing, candidate):
    if existing is None:
        return candidate
    ex_conf = str(existing.get("event")).upper()=="CONFIRMED"
    ca_conf = str(candidate.get("event")).upper()=="CONFIRMED"
    if ca_conf != ex_conf:
        return candidate if ca_conf else existing
    if float(candidate.get("max_dislocation",0)) > float(existing.get("max_dislocation",0)):
        return candidate
    return existing


def merged_relations(events, active):
    rel = {}
    for row in list(events)+list(active):
        key=(row["from_asset"],row["to_asset"])
        rel[key]=better(rel.get(key),dict(row))
    return rel


def effective_route(day_events, day_active, source):
    confirmed = [
        dict(e) for e in day_events
        if e.get("event")=="CONFIRMED" and e.get("from_asset")==source and e.get("to_asset") in ASSETS
    ]
    confirmed.sort(key=lambda e:(-float(e["max_dislocation"]),str(e["to_asset"]),str(e["pair"])))
    if not confirmed:
        return None

    primary = confirmed[0]
    rel = merged_relations(day_events,day_active)
    pto = primary["to_asset"]
    pstr = float(primary["max_dislocation"])

    candidates=[]
    for (frm,to),row in rel.items():
        if frm != source or to == pto:
            continue
        cstr=float(row.get("max_dislocation",0))
        if cstr + 1e-12 < pstr*DDG_RATIO:
            continue
        relation=rel.get((pto,to))
        if relation is None:
            continue
        candidates.append((cstr,str(to),row,relation))

    if not candidates:
        return {
            "source":source,"primary":primary,"effective":dict(primary),
            "override":False,"override_detail":None
        }

    candidates.sort(key=lambda x:(-x[0],x[1]))
    cstr,to,row,relation=candidates[0]
    eff=dict(primary)
    eff.update({
        "to_asset":to,
        "pair":row.get("pair"),
        "max_dislocation":cstr,
        "route_override":True,
    })
    return {
        "source":source,"primary":primary,"effective":eff,
        "override":True,
        "override_detail":{
            "competing":dict(row),
            "destination_relation":dict(relation),
            "strength_ratio":cstr/pstr if pstr else None,
        }
    }


def returns_and_ranks(panel, i, lookback):
    if i < lookback:
        return {},{}
    vals={}
    for a in ASSETS:
        now=float(panel.loc[i,a+"_close"])
        prev=float(panel.loc[i-lookback,a+"_close"])
        vals[a]=now/prev-1.0
    ordered=sorted(vals,key=lambda a:(-vals[a],a))
    ranks={a:j+1 for j,a in enumerate(ordered)}
    return vals,ranks


def route_features(panel, i, day_events, day_active, route):
    source=route["source"]
    dest=route["effective"]["to_asset"]
    rel=merged_relations(day_events,day_active)

    inbound_active_sources=sorted({
        frm for (frm,to),r in rel.items()
        if to==dest and frm!=source
    })
    inbound_confirmed_sources=sorted({
        e["from_asset"] for e in day_events
        if e.get("event")=="CONFIRMED" and e.get("to_asset")==dest and e.get("from_asset")!=source
    })

    outbound_alt=[
        r for (frm,to),r in rel.items()
        if frm==source and to!=dest
    ]
    alt_best=max((float(r.get("max_dislocation",0)) for r in outbound_alt),default=0.0)

    r30,k30=returns_and_ranks(panel,i,30)
    r60,k60=returns_and_ranks(panel,i,60)
    r90,k90=returns_and_ranks(panel,i,90)

    return {
        "signal_index":int(i),
        "source":source,
        "destination":dest,
        "primary_destination":route["primary"]["to_asset"],
        "ddg_override":bool(route["override"]),
        "effective_strength":float(route["effective"]["max_dislocation"]),
        "primary_strength":float(route["primary"]["max_dislocation"]),
        "best_other_outbound_strength":float(alt_best),
        "inbound_active_ex_source":len(inbound_active_sources),
        "inbound_confirmed_ex_source":len(inbound_confirmed_sources),
        "inbound_active_sources":"|".join(inbound_active_sources),
        "inbound_confirmed_sources":"|".join(inbound_confirmed_sources),
        "source_ret30":r30.get(source),
        "dest_ret30":r30.get(dest),
        "source_rank30":k30.get(source),
        "dest_rank30":k30.get(dest),
        "rel30":None if source not in r30 or dest not in r30 else r30[dest]-r30[source],
        "source_ret60":r60.get(source),
        "dest_ret60":r60.get(dest),
        "source_rank60":k60.get(source),
        "dest_rank60":k60.get(dest),
        "rel60":None if source not in r60 or dest not in r60 else r60[dest]-r60[source],
        "source_ret90":r90.get(source),
        "dest_ret90":r90.get(dest),
        "source_rank90":k90.get(source),
        "dest_rank90":k90.get(dest),
        "rel90":None if source not in r90 or dest not in r90 else r90[dest]-r90[source],
    }


def hold_rule(rule_id, f):
    if rule_id=="BASE_DDG":
        return False
    if rule_id=="C1_NO_INDEPENDENT_SUPPORT":
        return int(f["inbound_active_ex_source"])==0
    if rule_id=="C2_LOW_SUPPORT":
        return int(f["inbound_active_ex_source"])<=1
    if rule_id=="C3_NO_SUPPORT_AND_DEST_BOTTOM_HALF_30D":
        return int(f["inbound_active_ex_source"])==0 and int(f["dest_rank30"])>5
    if rule_id=="C4_NO_SUPPORT_AND_DEST_NOT_BEATING_SOURCE_30D":
        return int(f["inbound_active_ex_source"])==0 and float(f["rel30"])<=0.0
    if rule_id=="C5_NO_SUPPORT_AND_SIGNAL_LT_20PCT":
        return int(f["inbound_active_ex_source"])==0 and float(f["effective_strength"])<0.20
    if rule_id=="C6_LOW_SUPPORT_AND_DEST_NOT_BEATING_SOURCE_30D":
        return int(f["inbound_active_ex_source"])<=1 and float(f["rel30"])<=0.0
    if rule_id=="C7_NO_CONFIRMED_SUPPORT_AND_DEST_NOT_BEATING_SOURCE_60D":
        return int(f["inbound_confirmed_ex_source"])==0 and float(f["rel60"])<=0.0
    if rule_id=="M1_DEST_WORSE_THAN_SOURCE_30_60_90":
        return float(f["rel30"])<=0.0 and float(f["rel60"])<=0.0 and float(f["rel90"])<=0.0
    if rule_id=="M2_DEST_BOTTOM_HALF_30_60_90":
        return int(f["dest_rank30"])>5 and int(f["dest_rank60"])>5 and int(f["dest_rank90"])>5
    if rule_id=="M3_DEST_RANK_WORSE_THAN_SOURCE_ALL":
        return int(f["dest_rank30"])>int(f["source_rank30"]) and int(f["dest_rank60"])>int(f["source_rank60"]) and int(f["dest_rank90"])>int(f["source_rank90"])
    if rule_id=="S1_SIGNAL_STRENGTH_LT_18PCT":
        return float(f["effective_strength"])<0.18
    if rule_id=="S2_SIGNAL_STRENGTH_LT_20PCT":
        return float(f["effective_strength"])<0.20
    if rule_id=="S3_SIGNAL_STRENGTH_LT_25PCT":
        return float(f["effective_strength"])<0.25
    if rule_id=="MS1_STRENGTH_LT_20_AND_DEST_WORSE_30_60":
        return float(f["effective_strength"])<0.20 and float(f["rel30"])<=0.0 and float(f["rel60"])<=0.0
    raise ValueError(rule_id)


RULES=(
    "BASE_DDG",
    "C1_NO_INDEPENDENT_SUPPORT",
    "C2_LOW_SUPPORT",
    "C3_NO_SUPPORT_AND_DEST_BOTTOM_HALF_30D",
    "C4_NO_SUPPORT_AND_DEST_NOT_BEATING_SOURCE_30D",
    "C5_NO_SUPPORT_AND_SIGNAL_LT_20PCT",
    "C6_LOW_SUPPORT_AND_DEST_NOT_BEATING_SOURCE_30D",
    "C7_NO_CONFIRMED_SUPPORT_AND_DEST_NOT_BEATING_SOURCE_60D",
    "M1_DEST_WORSE_THAN_SOURCE_30_60_90",
    "M2_DEST_BOTTOM_HALF_30_60_90",
    "M3_DEST_RANK_WORSE_THAN_SOURCE_ALL",
    "S1_SIGNAL_STRENGTH_LT_18PCT",
    "S2_SIGNAL_STRENGTH_LT_20PCT",
    "S3_SIGNAL_STRENGTH_LT_25PCT",
    "MS1_STRENGTH_LT_20_AND_DEST_WORSE_30_60",
)


def bounds(ts,start,end):
    si=int(ts.searchsorted(utc(start),side="left"))
    ei=int(ts.searchsorted(utc(end),side="right"))-1
    return si,ei


def simulate(panel, ts, daily_events, daily_active, start_i, end_i, start_asset, rule_id, force_veto_key=None):
    cur=start_asset
    qty=1.0/float(panel.loc[start_i,cur+"_open"])
    initial=qty*float(panel.loc[start_i,cur+"_close"])
    equity=np.empty(end_i-start_i+1,dtype=float)
    ledger=[]
    pending=None
    veto_count=0

    for i in range(start_i,end_i+1):
        if pending is not None:
            value=qty*float(panel.loc[i,cur+"_open"])
            new_asset=pending["to_asset"]
            qty=value*(1.0-COST)/float(panel.loc[i,new_asset+"_open"])
            ledger.append({
                "signal_index":pending["signal_index"],
                "signal_date":ts[pending["signal_index"]].isoformat(),
                "execute_index":i,
                "execute_date":ts[i].isoformat(),
                "from_asset":cur,
                "to_asset":new_asset,
                "rule_id":rule_id,
                "ddg_override":pending["features"]["ddg_override"],
                "hold_veto":False,
                **pending["features"],
            })
            cur=new_asset
            pending=None

        equity[i-start_i]=qty*float(panel.loc[i,cur+"_close"])
        if i>=end_i:
            continue

        route=effective_route(daily_events[i],daily_active[i],cur)
        if route is None:
            continue
        f=route_features(panel,i,daily_events[i],daily_active[i],route)
        key=(ts[i].date().isoformat(),cur,route["effective"]["to_asset"])

        forced = force_veto_key is not None and key==force_veto_key
        veto = forced or hold_rule(rule_id,f)
        if veto:
            veto_count += 1
            ledger.append({
                "signal_index":i,
                "signal_date":ts[i].isoformat(),
                "execute_index":None,
                "execute_date":None,
                "from_asset":cur,
                "to_asset":route["effective"]["to_asset"],
                "rule_id":rule_id,
                "ddg_override":f["ddg_override"],
                "hold_veto":True,
                "forced_veto":forced,
                **f,
            })
            continue

        pending={"signal_index":i,"to_asset":route["effective"]["to_asset"],"features":f}

    peak=np.maximum.accumulate(equity)
    dd=float(np.min(equity/peak-1.0))
    return {
        "return":float(equity[-1]/initial-1.0),
        "final_equity":float(equity[-1]),
        "max_dd":dd,
        "transitions":sum(1 for x in ledger if not x.get("hold_veto")),
        "vetoes":veto_count,
        "ledger":ledger,
    }


def evaluate(panel,ts,daily_events,daily_active,start,end,rule_id):
    si,ei=bounds(ts,start,end)
    runs=[simulate(panel,ts,daily_events,daily_active,si,ei,a,rule_id) for a in ASSETS]
    rets=np.array([r["return"] for r in runs],dtype=float)
    dds=np.array([r["max_dd"] for r in runs],dtype=float)
    return {
        "median_return":float(np.median(rets)),
        "worst_return":float(np.min(rets)),
        "best_return":float(np.max(rets)),
        "median_max_dd":float(np.median(dds)),
        "worst_max_dd":float(np.min(dds)),
        "median_transitions":float(np.median([r["transitions"] for r in runs])),
        "median_vetoes":float(np.median([r["vetoes"] for r in runs])),
        "_runs":runs,
    }


def direct_horizon(panel,i,source,dest,days):
    j=min(i+1+days,len(panel)-1)
    entry=i+1
    if entry>=len(panel) or j<=entry:
        return None
    s0=float(panel.loc[entry,source+"_open"])
    d0=float(panel.loc[entry,dest+"_open"])
    s1=float(panel.loc[j,source+"_close"])
    d1=float(panel.loc[j,dest+"_close"])
    return (d1/d0-1.0)-(s1/s0-1.0)


def one_time_veto_audit(panel,ts,daily_events,daily_active,baseline_runs,mature_si,mature_ei):
    decisions={}
    for r in baseline_runs:
        for x in r["ledger"]:
            if x.get("hold_veto"):
                continue
            key=(x["signal_date"][:10],x["from_asset"],x["to_asset"])
            decisions[key]=x

    rows=[]
    for key,x in sorted(decisions.items()):
        i=int(x["signal_index"])
        source=x["from_asset"]
        dest=x["to_asset"]
        base=simulate(panel,ts,daily_events,daily_active,i,mature_ei,source,"BASE_DDG")
        veto=simulate(panel,ts,daily_events,daily_active,i,mature_ei,source,"BASE_DDG",force_veto_key=key)
        row={
            "signal_date":key[0],"source":source,"destination":dest,
            "baseline_final_from_1":base["final_equity"],
            "one_time_hold_final_from_1":veto["final_equity"],
            "hold_to_switch_final_ratio":veto["final_equity"]/base["final_equity"] if base["final_equity"] else None,
            "hold_better_final":veto["final_equity"]>base["final_equity"],
            "direct_dest_minus_source_30d":direct_horizon(panel,i,source,dest,30),
            "direct_dest_minus_source_60d":direct_horizon(panel,i,source,dest,60),
            "direct_dest_minus_source_90d":direct_horizon(panel,i,source,dest,90),
        }
        for k,v in x.items():
            if k in {
                "inbound_active_ex_source","inbound_confirmed_ex_source","effective_strength",
                "primary_strength","dest_rank30","source_rank30","rel30","dest_rank60",
                "source_rank60","rel60","dest_rank90","source_rank90","rel90","ddg_override"
            }:
                row[k]=v
        rows.append(row)
    return pd.DataFrame(rows)


def main():
    OUT.mkdir(parents=True,exist_ok=True)
    panel,data_meta=download_panel()
    ts=pd.DatetimeIndex(pd.to_datetime(panel["timestamp"],utc=True))
    daily_events,daily_active=pair_daily_tape(panel)

    summary_rows=[]
    detailed={}
    for rule in RULES:
        detailed[rule]={}
        for w,(start,end) in WINDOWS.items():
            m=evaluate(panel,ts,daily_events,daily_active,start,end,rule)
            detailed[rule][w]={k:v for k,v in m.items() if k!="_runs"}
            summary_rows.append({
                "rule_id":rule,"window":w,
                **{k:v for k,v in m.items() if k!="_runs"}
            })

    summary_df=pd.DataFrame(summary_rows)
    summary_df.to_csv(OUT/"rule_window_summary.csv",index=False)

    mature_base=evaluate(panel,ts,daily_events,daily_active,*WINDOWS["MATURE"],"BASE_DDG")
    mature_ret=mature_base["median_return"]
    if abs(mature_ret-EXPECTED_DDG_MATURE)>0.02:
        raise RuntimeError(
            f"DDG baseline mismatch: got {mature_ret:.6f}, expected about {EXPECTED_DDG_MATURE:.6f}"
        )

    mature_si,mature_ei=bounds(ts,*WINDOWS["MATURE"])
    event_df=one_time_veto_audit(
        panel,ts,daily_events,daily_active,mature_base["_runs"],mature_si,mature_ei
    )
    event_df.to_csv(OUT/"transition_hold_counterfactuals.csv",index=False)

    # Focus on the known January 2024 killer event without using it to tune rules.
    focus=event_df[
        (event_df["signal_date"]=="2024-01-14") &
        (event_df["source"]=="TRX") &
        (event_df["destination"]=="XRP")
    ]
    if focus.empty:
        raise RuntimeError("Expected TRX->XRP 2024-01-14 decision not found under DDG baseline")

    # Rule diagnostics: which fixed rules block the focus event and how they behave on validation.
    diagnostics=[]
    focus_row=focus.iloc[0].to_dict()
    f_for_rule={
        "inbound_active_ex_source":focus_row["inbound_active_ex_source"],
        "inbound_confirmed_ex_source":focus_row["inbound_confirmed_ex_source"],
        "source_rank30":focus_row["source_rank30"],
        "dest_rank30":focus_row["dest_rank30"],
        "source_rank60":focus_row["source_rank60"],
        "dest_rank60":focus_row["dest_rank60"],
        "source_rank90":focus_row["source_rank90"],
        "dest_rank90":focus_row["dest_rank90"],
        "rel30":focus_row["rel30"],
        "rel60":focus_row["rel60"],
        "rel90":focus_row["rel90"],
        "effective_strength":focus_row["effective_strength"],
    }
    base_val=detailed["BASE_DDG"]["VALIDATION_1Y"]["median_return"]
    base_disc=detailed["BASE_DDG"]["DISCOVERY"]["median_return"]
    for rule in RULES[1:]:
        diagnostics.append({
            "rule_id":rule,
            "blocks_TRX_XRP_2024_01_14":bool(hold_rule(rule,f_for_rule)),
            "discovery_return":detailed[rule]["DISCOVERY"]["median_return"],
            "discovery_delta_vs_base":detailed[rule]["DISCOVERY"]["median_return"]-base_disc,
            "validation_return":detailed[rule]["VALIDATION_1Y"]["median_return"],
            "validation_delta_vs_base":detailed[rule]["VALIDATION_1Y"]["median_return"]-base_val,
            "mature_return":detailed[rule]["MATURE"]["median_return"],
            "mature_delta_vs_base":detailed[rule]["MATURE"]["median_return"]-mature_ret,
            "mature_vetoes":detailed[rule]["MATURE"]["median_vetoes"],
        })
    diag_df=pd.DataFrame(diagnostics)
    diag_df.to_csv(OUT/"fixed_rule_diagnostics.csv",index=False)

    # No automatic production winner. Research-only descriptive shortlist.
    non_destructive=diag_df[
        (diag_df["blocks_TRX_XRP_2024_01_14"]) &
        (diag_df["validation_delta_vs_base"]>=0)
    ].copy()

    summary={
        "experiment":"HOLD_COMPETING_DESTINATION_V1",
        "workflow_mode":"STRESS_TEST_ONLY",
        "test_level":"GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
        "production_changes":"NONE",
        "parameters":{"lookback":LOOKBACK,"arm":ARM,"reversal":REVERSAL,"cost":COST,"ddg_ratio":DDG_RATIO},
        "windows":{k:{"start":v[0].isoformat(),"end":v[1].isoformat()} for k,v in WINDOWS.items()},
        "ddg_baseline_validation":{"observed_mature_return":mature_ret,"expected_approx":EXPECTED_DDG_MATURE},
        "focus_transition":focus_row,
        "fixed_rules":json.loads(diag_df.to_json(orient="records")),
        "descriptive_non_destructive_shortlist":non_destructive["rule_id"].tolist(),
        "data_metadata":data_meta,
    }
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True,default=str)+"\n",encoding="utf-8")

    lines=[
        "# HOLD AS COMPETING DESTINATION V1","",
        "Mode: STRESS_TEST_ONLY","",
        f"DDG baseline Mature validation: {100*mature_ret:+.1f}% (expected ~+3588.4%)","",
        "## Fixed rule results","",
        "|Rule|Blocks TRX->XRP 2024-01-14|Discovery delta|Validation delta|Mature delta|Median vetoes Mature|",
        "|---|---|---:|---:|---:|---:|",
    ]
    for _,r in diag_df.iterrows():
        lines.append(
            f"|{r['rule_id']}|{bool(r['blocks_TRX_XRP_2024_01_14'])}|"
            f"{100*r['discovery_delta_vs_base']:+.1f} pp|"
            f"{100*r['validation_delta_vs_base']:+.1f} pp|"
            f"{100*r['mature_delta_vs_base']:+.1f} pp|"
            f"{r['mature_vetoes']:.1f}|"
        )
    lines += ["","## Focus transition features",""]
    for k in [
        "signal_date","source","destination","effective_strength","inbound_active_ex_source",
        "inbound_confirmed_ex_source","source_rank30","dest_rank30","rel30",
        "source_rank60","dest_rank60","rel60","hold_to_switch_final_ratio",
        "direct_dest_minus_source_30d","direct_dest_minus_source_60d","direct_dest_minus_source_90d"
    ]:
        lines.append(f"- {k}: {focus_row.get(k)}")
    lines += ["","No live/paper/Telegram/execution configuration changed.","",
              "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST"]
    (OUT/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")
    print("\n".join(lines))


if __name__=="__main__":
    main()
