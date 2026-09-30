from __future__ import annotations

import itertools
import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient

OUT = Path("research_artifacts/followthrough_equal10_v1")

U10 = ("TWT","PEPE","BNB","TRX","AAVE","AVAX","FIL","ALGO","XRP","HBAR")
U9 = tuple(a for a in U10 if a != "XRP")
LOOKBACK = 180
ARM = 0.15
COST = 0.001
DDG_RATIO = 1.50

DATA_START = pd.Timestamp("2023-05-05", tz="UTC")
CUTOFF = pd.Timestamp("2026-09-27", tz="UTC")
WINDOWS = {
    "DISCOVERY": (pd.Timestamp("2023-10-31", tz="UTC"), pd.Timestamp("2025-09-26", tz="UTC")),
    "VALIDATION_1Y": (pd.Timestamp("2025-09-27", tz="UTC"), pd.Timestamp("2026-09-26", tz="UTC")),
    "LAST_2Y": (pd.Timestamp("2024-09-27", tz="UTC"), pd.Timestamp("2026-09-26", tz="UTC")),
    "MATURE": (pd.Timestamp("2023-10-31", tz="UTC"), pd.Timestamp("2026-09-26", tz="UTC")),
}
PORTFOLIO_WINDOWS = ("VALIDATION_1Y","LAST_2Y","MATURE")

VARIANTS = {
    "BASE_3PCT_1CLOSE": {"reversal":0.03,"confirm_closes":1},
    "PERSIST_3PCT_2CLOSES": {"reversal":0.03,"confirm_closes":2},
    "PERSIST_3PCT_3CLOSES": {"reversal":0.03,"confirm_closes":3},
    "THRESHOLD_4PCT_1CLOSE": {"reversal":0.04,"confirm_closes":1},
    "THRESHOLD_5PCT_1CLOSE": {"reversal":0.05,"confirm_closes":1},
}
EXPECTED_BASE_MATURE = 35.8836

@dataclass
class PairState:
    mode: str = "NONE"
    extreme: float | None = None
    max_dislocation: float = 0.0
    confirm_streak: int = 0


def utc(v):
    t = pd.Timestamp(v)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def download_panel():
    client = BinanceSpotRestClient()
    panel = None
    meta = {}
    for asset in U10:
        result = download_historical_dataset(
            client,
            symbol=asset+"USDT",
            start=DATA_START,
            end=CUTOFF,
            timeframe="1D",
            as_of=CUTOFF,
        )
        if result.dataset is None:
            raise RuntimeError(f"{asset}: {result.metadata.status}")
        if result.dataset.quality.has_critical_issues:
            raise RuntimeError(f"{asset}: critical candle quality issue")
        f = result.dataset.candles[["timestamp","open","close"]].copy()
        f["timestamp"] = pd.to_datetime(f["timestamp"], utc=True)
        f = f.rename(columns={"open":asset+"_open","close":asset+"_close"})
        meta[asset] = {
            "rows":int(len(f)),
            "start":pd.Timestamp(f.iloc[0]["timestamp"]).isoformat(),
            "end":pd.Timestamp(f.iloc[-1]["timestamp"]).isoformat(),
        }
        panel = f if panel is None else panel.merge(f,on="timestamp",how="inner",validate="one_to_one")
    return panel.sort_values("timestamp").reset_index(drop=True), meta


def pair_daily_tape(panel, reversal, confirm_closes):
    n=len(panel)
    daily_events=[[] for _ in range(n)]
    daily_active=[[] for _ in range(n)]

    for left,right in itertools.combinations(U10,2):
        ratio=panel[right+"_close"].astype(float)/panel[left+"_close"].astype(float)
        median=ratio.rolling(LOOKBACK,min_periods=LOOKBACK).median()
        dev=ratio/median-1.0
        st=PairState()

        for i in range(n):
            if pd.isna(median.iloc[i]) or pd.isna(dev.iloc[i]):
                continue
            r=float(ratio.iloc[i])
            d=float(dev.iloc[i])

            if st.mode=="NONE":
                if d>=ARM:
                    st=PairState("HIGH",r,abs(d),0)
                    daily_events[i].append({
                        "event":"ARMED","pair":f"{left}/{right}",
                        "from_asset":right,"to_asset":left,
                        "max_dislocation":abs(d),"deviation":d,
                    })
                elif d<=-ARM:
                    st=PairState("LOW",r,abs(d),0)
                    daily_events[i].append({
                        "event":"ARMED","pair":f"{left}/{right}",
                        "from_asset":left,"to_asset":right,
                        "max_dislocation":abs(d),"deviation":d,
                    })
            elif st.mode=="HIGH":
                if r>float(st.extreme):
                    st.extreme=r
                    st.max_dislocation=max(st.max_dislocation,abs(d))
                    st.confirm_streak=0
                retr=1.0-r/float(st.extreme)
                if retr>=reversal:
                    st.confirm_streak += 1
                    if st.confirm_streak>=confirm_closes:
                        daily_events[i].append({
                            "event":"CONFIRMED","pair":f"{left}/{right}",
                            "from_asset":right,"to_asset":left,
                            "max_dislocation":st.max_dislocation,"deviation":d,
                            "reversal_from_extreme":retr,
                            "confirm_closes":confirm_closes,
                        })
                        st=PairState()
                else:
                    st.confirm_streak=0
            elif st.mode=="LOW":
                if r<float(st.extreme):
                    st.extreme=r
                    st.max_dislocation=max(st.max_dislocation,abs(d))
                    st.confirm_streak=0
                retr=r/float(st.extreme)-1.0
                if retr>=reversal:
                    st.confirm_streak += 1
                    if st.confirm_streak>=confirm_closes:
                        daily_events[i].append({
                            "event":"CONFIRMED","pair":f"{left}/{right}",
                            "from_asset":left,"to_asset":right,
                            "max_dislocation":st.max_dislocation,"deviation":d,
                            "reversal_from_extreme":retr,
                            "confirm_closes":confirm_closes,
                        })
                        st=PairState()
                else:
                    st.confirm_streak=0

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
    return daily_events,daily_active


def better(existing,candidate):
    if existing is None:
        return candidate
    ec=str(existing.get("event")).upper()=="CONFIRMED"
    cc=str(candidate.get("event")).upper()=="CONFIRMED"
    if cc!=ec:
        return candidate if cc else existing
    if float(candidate.get("max_dislocation",0))>float(existing.get("max_dislocation",0)):
        return candidate
    return existing


def merged_relations(events,active,allowed):
    allowed=set(allowed)
    rel={}
    for row in list(events)+list(active):
        if row["from_asset"] not in allowed or row["to_asset"] not in allowed:
            continue
        key=(row["from_asset"],row["to_asset"])
        rel[key]=better(rel.get(key),dict(row))
    return rel


def effective_route(day_events,day_active,source,allowed):
    allowed=set(allowed)
    confirmed=[
        dict(e) for e in day_events
        if e.get("event")=="CONFIRMED"
        and e.get("from_asset")==source
        and e.get("to_asset") in allowed
    ]
    confirmed.sort(key=lambda e:(-float(e["max_dislocation"]),str(e["to_asset"]),str(e["pair"])))
    if not confirmed:
        return None

    primary=confirmed[0]
    rel=merged_relations(day_events,day_active,allowed)
    pto=primary["to_asset"]
    pstr=float(primary["max_dislocation"])
    candidates=[]
    for (frm,to),row in rel.items():
        if frm!=source or to==pto:
            continue
        cstr=float(row.get("max_dislocation",0))
        if cstr+1e-12 < pstr*DDG_RATIO:
            continue
        relation=rel.get((pto,to))
        if relation is None:
            continue
        candidates.append((cstr,str(to),row,relation))

    if not candidates:
        return {
            "from_asset":source,
            "to_asset":pto,
            "pair":primary["pair"],
            "max_dislocation":pstr,
            "ddg_override":False,
            "primary_to":pto,
        }

    candidates.sort(key=lambda x:(-x[0],x[1]))
    cstr,to,row,relation=candidates[0]
    return {
        "from_asset":source,
        "to_asset":to,
        "pair":row.get("pair"),
        "max_dislocation":cstr,
        "ddg_override":True,
        "primary_to":pto,
        "ddg_strength_ratio":cstr/pstr if pstr else None,
    }


def bounds(ts,start,end):
    si=int(ts.searchsorted(utc(start),side="left"))
    ei=int(ts.searchsorted(utc(end),side="right"))-1
    if ei<si:
        raise RuntimeError("empty bounds")
    return si,ei


def simulate(panel,ts,daily_events,daily_active,allowed,start_i,end_i,start_asset,initial_capital=None):
    cur=start_asset
    if initial_capital is None:
        qty=1.0/float(panel.loc[start_i,cur+"_open"])
        initial=qty*float(panel.loc[start_i,cur+"_close"])
    else:
        initial=float(initial_capital)
        qty=initial/float(panel.loc[start_i,cur+"_close"])

    n=end_i-start_i+1
    equity=np.empty(n,dtype=float)
    holdings=np.empty(n,dtype=object)
    pending=None
    ledger=[]

    for i in range(start_i,end_i+1):
        if pending is not None:
            value=qty*float(panel.loc[i,cur+"_open"])
            nxt=pending["to_asset"]
            qty=value*(1.0-COST)/float(panel.loc[i,nxt+"_open"])
            ledger.append({
                "signal_date":ts[pending["signal_i"]].isoformat(),
                "execute_date":ts[i].isoformat(),
                "from_asset":cur,
                "to_asset":nxt,
                "pair":pending["pair"],
                "max_dislocation":pending["max_dislocation"],
                "ddg_override":pending["ddg_override"],
                "primary_to":pending["primary_to"],
            })
            cur=nxt
            pending=None

        equity[i-start_i]=qty*float(panel.loc[i,cur+"_close"])
        holdings[i-start_i]=cur
        if i>=end_i:
            continue

        route=effective_route(daily_events[i],daily_active[i],cur,allowed)
        if route is not None:
            pending={"signal_i":i,**route}

    peak=np.maximum.accumulate(equity)
    dd=float(np.min(equity/peak-1.0))
    return {
        "initial_equity":initial,
        "final_equity":float(equity[-1]),
        "return":float(equity[-1]/initial-1.0),
        "max_dd":dd,
        "transitions":len(ledger),
        "equity":equity,
        "holdings":holdings,
        "ledger":ledger,
    }


def evaluate_single(panel,ts,daily_events,daily_active,allowed,start,end):
    si,ei=bounds(ts,start,end)
    runs=[simulate(panel,ts,daily_events,daily_active,allowed,si,ei,a) for a in allowed]
    rets=np.array([r["return"] for r in runs],dtype=float)
    dds=np.array([r["max_dd"] for r in runs],dtype=float)
    return {
        "median_return":float(np.median(rets)),
        "mean_return":float(np.mean(rets)),
        "worst_return":float(np.min(rets)),
        "best_return":float(np.max(rets)),
        "median_max_dd":float(np.median(dds)),
        "worst_max_dd":float(np.min(dds)),
        "median_transitions":float(np.median([r["transitions"] for r in runs])),
        "_runs":runs,
    }


def equal_books(panel,ts,daily_events,daily_active,allowed,start,end):
    si,ei=bounds(ts,start,end)
    weight=1.0/len(allowed)
    runs=[
        simulate(panel,ts,daily_events,daily_active,allowed,si,ei,a,initial_capital=weight)
        for a in allowed
    ]
    port=np.sum(np.vstack([r["equity"] for r in runs]),axis=0)
    initial=1.0
    peak=np.maximum.accumulate(port)
    dd=float(np.min(port/peak-1.0))

    largest=[]
    unique_counts=[]
    full_convergence=[]
    for d in range(len(port)):
        by_asset={}
        for r in runs:
            a=str(r["holdings"][d])
            by_asset[a]=by_asset.get(a,0.0)+float(r["equity"][d])
        shares=[v/port[d] for v in by_asset.values()]
        largest.append(max(shares))
        unique_counts.append(len(by_asset))
        full_convergence.append(len(by_asset)==1)

    full_idx=[i for i,x in enumerate(full_convergence) if x]
    first_full=None if not full_idx else ts[si+full_idx[0]].date().isoformat()
    return {
        "return":float(port[-1]/initial-1.0),
        "max_dd":dd,
        "total_transitions":int(sum(r["transitions"] for r in runs)),
        "max_largest_asset_share":float(np.max(largest)),
        "median_largest_asset_share":float(np.median(largest)),
        "days_over_50pct":float(np.mean(np.array(largest)>0.50)),
        "days_over_80pct":float(np.mean(np.array(largest)>0.80)),
        "days_over_95pct":float(np.mean(np.array(largest)>=0.95)),
        "collision_day_share":float(np.mean(np.array(unique_counts)<len(allowed))),
        "full_convergence_day_share":float(np.mean(full_convergence)),
        "first_full_convergence_date":first_full,
        "ending_assets":"|".join(str(r["holdings"][-1]) for r in runs),
        "_equity":port,
        "_runs":runs,
    }


def static_equal_weight(panel,ts,allowed,start,end):
    si,ei=bounds(ts,start,end)
    weight=1.0/len(allowed)
    port=np.zeros(ei-si+1,dtype=float)
    for a in allowed:
        qty=weight/float(panel.loc[si,a+"_close"])
        port += qty*panel.loc[si:ei,a+"_close"].astype(float).to_numpy()
    peak=np.maximum.accumulate(port)
    return {
        "return":float(port[-1]-1.0),
        "max_dd":float(np.min(port/peak-1.0)),
    }


def rolling_12m(panel,ts,daily_events,daily_active,allowed):
    rows=[]
    first=pd.Timestamp("2023-11-01",tz="UTC")
    end_limit=WINDOWS["MATURE"][1]
    for start in pd.date_range(first,end_limit,freq="MS"):
        end=start+pd.DateOffset(months=12)-pd.Timedelta(days=1)
        if end>end_limit:
            continue
        eq=equal_books(panel,ts,daily_events,daily_active,allowed,start,end)
        bh=static_equal_weight(panel,ts,allowed,start,end)
        rows.append({
            "start":start.date().isoformat(),
            "end":end.date().isoformat(),
            "equal_books_return":eq["return"],
            "equal_books_max_dd":eq["max_dd"],
            "static_equal_return":bh["return"],
            "static_equal_max_dd":bh["max_dd"],
            "full_convergence_day_share":eq["full_convergence_day_share"],
        })
    return pd.DataFrame(rows)


def focus_transition(runs):
    hits=[]
    for r in runs:
        for e in r["ledger"]:
            sd=e["signal_date"][:10]
            if sd>="2024-01-10" and sd<="2024-02-15" and e["from_asset"]=="TRX":
                hits.append(e)
    if not hits:
        return []
    seen={}
    for h in hits:
        key=(h["signal_date"][:10],h["from_asset"],h["to_asset"])
        seen[key]=h
    return list(seen.values())


def main():
    OUT.mkdir(parents=True,exist_ok=True)
    panel,data_meta=download_panel()
    ts=pd.DatetimeIndex(pd.to_datetime(panel["timestamp"],utc=True))

    variant_rows=[]
    variant_summary={}
    baseline_tape=None

    for vid,cfg in VARIANTS.items():
        events,active=pair_daily_tape(panel,cfg["reversal"],cfg["confirm_closes"])
        if vid=="BASE_3PCT_1CLOSE":
            baseline_tape=(events,active)

        variant_summary[vid]={}
        for w,(start,end) in WINDOWS.items():
            m=evaluate_single(panel,ts,events,active,U10,start,end)
            row={
                "variant":vid,"window":w,
                "reversal":cfg["reversal"],
                "confirm_closes":cfg["confirm_closes"],
                **{k:v for k,v in m.items() if k!="_runs"},
            }
            variant_rows.append(row)
            variant_summary[vid][w]={k:v for k,v in row.items() if k not in {"variant","window"}}

        mature=evaluate_single(panel,ts,events,active,U10,*WINDOWS["MATURE"])
        variant_summary[vid]["focus_TRX_Jan2024"]=focus_transition(mature["_runs"])

    variant_df=pd.DataFrame(variant_rows)
    variant_df.to_csv(OUT/"followthrough_variant_windows.csv",index=False)

    base_mature=variant_summary["BASE_3PCT_1CLOSE"]["MATURE"]["median_return"]
    if abs(base_mature-EXPECTED_BASE_MATURE)>0.02:
        raise RuntimeError(f"baseline mismatch {base_mature:.6f} vs expected ~{EXPECTED_BASE_MATURE:.6f}")

    base_events,base_active=baseline_tape

    portfolio_rows=[]
    for w in PORTFOLIO_WINDOWS:
        start,end=WINDOWS[w]
        for label,allowed in (("U10",U10),("U9_NO_XRP",U9)):
            eq=equal_books(panel,ts,base_events,base_active,allowed,start,end)
            bh=static_equal_weight(panel,ts,allowed,start,end)
            single=evaluate_single(panel,ts,base_events,base_active,allowed,start,end)
            portfolio_rows.append({
                "window":w,
                "universe":label,
                "book_count":len(allowed),
                "equal_books_return":eq["return"],
                "equal_books_max_dd":eq["max_dd"],
                "static_equal_buyhold_return":bh["return"],
                "static_equal_buyhold_max_dd":bh["max_dd"],
                "single_start_mean_return":single["mean_return"],
                "single_start_median_return":single["median_return"],
                "max_largest_asset_share":eq["max_largest_asset_share"],
                "median_largest_asset_share":eq["median_largest_asset_share"],
                "days_over_50pct":eq["days_over_50pct"],
                "days_over_80pct":eq["days_over_80pct"],
                "days_over_95pct":eq["days_over_95pct"],
                "collision_day_share":eq["collision_day_share"],
                "full_convergence_day_share":eq["full_convergence_day_share"],
                "first_full_convergence_date":eq["first_full_convergence_date"],
                "total_transitions":eq["total_transitions"],
                "ending_assets":eq["ending_assets"],
            })

    port_df=pd.DataFrame(portfolio_rows)
    port_df.to_csv(OUT/"equal_capital_portfolios.csv",index=False)

    roll10=rolling_12m(panel,ts,base_events,base_active,U10)
    roll9=rolling_12m(panel,ts,base_events,base_active,U9)
    roll10["universe"]="U10"
    roll9["universe"]="U9_NO_XRP"
    rolling=pd.concat([roll10,roll9],ignore_index=True)
    rolling.to_csv(OUT/"equal_capital_rolling12m.csv",index=False)

    rolling_summary=[]
    for label,g in rolling.groupby("universe"):
        rolling_summary.append({
            "universe":label,
            "window_count":int(len(g)),
            "median_equal_books_return":float(g["equal_books_return"].median()),
            "worst_equal_books_return":float(g["equal_books_return"].min()),
            "positive_equal_books_rate":float((g["equal_books_return"]>0).mean()),
            "median_static_equal_return":float(g["static_equal_return"].median()),
            "worst_static_equal_return":float(g["static_equal_return"].min()),
            "median_full_convergence_share":float(g["full_convergence_day_share"].median()),
        })
    rolling_summary_df=pd.DataFrame(rolling_summary)
    rolling_summary_df.to_csv(OUT/"equal_capital_rolling12m_summary.csv",index=False)

    summary={
        "experiment":"FOLLOWTHROUGH_EQUAL10_V1",
        "workflow_mode":"STRESS_TEST_ONLY",
        "test_level":"GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
        "production_changes":"NONE",
        "parameters":{"lookback":LOOKBACK,"arm":ARM,"cost":COST,"ddg_ratio":DDG_RATIO},
        "followthrough_variants":variant_summary,
        "portfolio_results":json.loads(port_df.to_json(orient="records")),
        "rolling12m_summary":json.loads(rolling_summary_df.to_json(orient="records")),
        "data_metadata":data_meta,
    }
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True,default=str)+"\n",encoding="utf-8")

    lines=[
        "# FOLLOW-THROUGH + EQUAL-CAPITAL U10 V1","",
        "Mode: STRESS_TEST_ONLY","",
        f"Baseline Mature validation: {100*base_mature:+.1f}% (expected ~+3588.4%)","",
        "## Follow-through variants","",
        "|Variant|Discovery|Validation 1Y|Mature|Mature DD|Transitions|Jan-2024 TRX route|",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for vid in VARIANTS:
        d=variant_summary[vid]["DISCOVERY"]
        v=variant_summary[vid]["VALIDATION_1Y"]
        m=variant_summary[vid]["MATURE"]
        focus=variant_summary[vid]["focus_TRX_Jan2024"]
        focus_txt="; ".join(f"{x['signal_date'][:10]} {x['from_asset']}->{x['to_asset']}" for x in focus) or "none"
        lines.append(
            f"|{vid}|{100*d['median_return']:+.1f}%|{100*v['median_return']:+.1f}%|"
            f"{100*m['median_return']:+.1f}%|{100*m['median_max_dd']:+.1f}%|"
            f"{m['median_transitions']:.1f}|{focus_txt}|"
        )

    lines += ["","## Equal-capital portfolios","",
              "|Window|Universe|Books|RR equal-books|DD|Static equal B&H|DD|Median largest share|Full convergence days|",
              "|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    for _,r in port_df.iterrows():
        lines.append(
            f"|{r['window']}|{r['universe']}|{int(r['book_count'])}|"
            f"{100*r['equal_books_return']:+.1f}%|{100*r['equal_books_max_dd']:+.1f}%|"
            f"{100*r['static_equal_buyhold_return']:+.1f}%|{100*r['static_equal_buyhold_max_dd']:+.1f}%|"
            f"{100*r['median_largest_asset_share']:.1f}%|{100*r['full_convergence_day_share']:.1f}%|"
        )

    lines += ["","No production/live/Telegram/execution configuration changed.","",
              "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST"]
    (OUT/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")
    print("\n".join(lines))


if __name__=="__main__":
    main()
