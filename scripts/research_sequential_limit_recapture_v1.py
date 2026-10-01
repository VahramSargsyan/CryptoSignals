from __future__ import annotations

import bisect
import itertools
import json
import math
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
import scripts.research_negative_trade_forensics_v1 as fx

OUT = Path("research_artifacts/sequential_limit_recapture_v1")
HORIZONS_H = (24, 48, 72, 120, 168)
MAX_H = max(HORIZONS_H)
REV = 0.03
COST = 0.001

@dataclass
class State:
    mode: str = "NONE"
    extreme_ratio: float | None = None
    extreme_i: int | None = None
    max_dislocation: float = 0.0


def pair_tape_with_extremes(panel):
    n=len(panel)
    events=[[] for _ in range(n)]
    active=[[] for _ in range(n)]

    for left,right in itertools.combinations(fx.U10,2):
        ratio=panel[right+"_close"].astype(float)/panel[left+"_close"].astype(float)
        med=ratio.rolling(fx.LOOKBACK,min_periods=fx.LOOKBACK).median()
        dev=ratio/med-1.0
        st=State()

        for i in range(n):
            if pd.isna(med.iloc[i]) or pd.isna(dev.iloc[i]):
                continue
            r=float(ratio.iloc[i]); d=float(dev.iloc[i])

            if st.mode=="NONE":
                if d>=fx.ARM:
                    st=State("HIGH",r,i,abs(d))
                elif d<=-fx.ARM:
                    st=State("LOW",r,i,abs(d))

            elif st.mode=="HIGH":
                if r>float(st.extreme_ratio):
                    st.extreme_ratio=r
                    st.extreme_i=i
                    st.max_dislocation=max(st.max_dislocation,abs(d))
                retr=1.0-r/float(st.extreme_ratio)
                if retr>=REV:
                    events[i].append({
                        "event":"CONFIRMED","mode":"HIGH",
                        "pair":f"{left}/{right}","left":left,"right":right,
                        "from_asset":right,"to_asset":left,
                        "max_dislocation":st.max_dislocation,
                        "reversal_from_extreme":retr,
                        "extreme_ratio":float(st.extreme_ratio),
                        "current_pair_ratio":r,
                        "extreme_i":int(st.extreme_i),
                        "signal_i":i,
                    })
                    st=State()

            elif st.mode=="LOW":
                if r<float(st.extreme_ratio):
                    st.extreme_ratio=r
                    st.extreme_i=i
                    st.max_dislocation=max(st.max_dislocation,abs(d))
                retr=r/float(st.extreme_ratio)-1.0
                if retr>=REV:
                    events[i].append({
                        "event":"CONFIRMED","mode":"LOW",
                        "pair":f"{left}/{right}","left":left,"right":right,
                        "from_asset":left,"to_asset":right,
                        "max_dislocation":st.max_dislocation,
                        "reversal_from_extreme":retr,
                        "extreme_ratio":float(st.extreme_ratio),
                        "current_pair_ratio":r,
                        "extreme_i":int(st.extreme_i),
                        "signal_i":i,
                    })
                    st=State()

            if st.mode!="NONE":
                if st.mode=="HIGH":
                    retr=max(0.0,1.0-r/float(st.extreme_ratio))
                    frm,to=right,left
                else:
                    retr=max(0.0,r/float(st.extreme_ratio)-1.0)
                    frm,to=left,right
                active[i].append({
                    "event":"ARMED","mode":st.mode,
                    "pair":f"{left}/{right}","left":left,"right":right,
                    "from_asset":frm,"to_asset":to,
                    "max_dislocation":st.max_dislocation,
                    "reversal_from_extreme":retr,
                    "extreme_ratio":float(st.extreme_ratio),
                    "current_pair_ratio":r,
                    "extreme_i":int(st.extreme_i),
                    "signal_i":i,
                })
    return events,active


def better(existing,candidate):
    if existing is None:
        return dict(candidate)
    ec=existing.get("event")=="CONFIRMED"
    cc=candidate.get("event")=="CONFIRMED"
    if ec!=cc:
        return dict(candidate) if cc else existing
    if float(candidate.get("max_dislocation",0))>float(existing.get("max_dislocation",0)):
        return dict(candidate)
    return existing


def merged(events,active):
    rel={}
    for row in list(events)+list(active):
        if row["from_asset"] not in fx.U10 or row["to_asset"] not in fx.U10:
            continue
        k=(row["from_asset"],row["to_asset"])
        rel[k]=better(rel.get(k),row)
    return rel


def effective_route(events,active,source):
    confirmed=[dict(e) for e in events if e["event"]=="CONFIRMED" and e["from_asset"]==source]
    confirmed.sort(key=lambda e:(-float(e["max_dislocation"]),e["to_asset"],e["pair"]))
    if not confirmed:
        return None
    primary=confirmed[0]
    rel=merged(events,active)
    pto=primary["to_asset"]; ps=float(primary["max_dislocation"])
    cands=[]
    for (frm,to),row in rel.items():
        if frm!=source or to==pto:
            continue
        cs=float(row.get("max_dislocation",0))
        if cs+1e-12<ps*fx.DDG_RATIO:
            continue
        direct=rel.get((pto,to))
        if direct is None:
            continue
        cands.append((cs,to,row,direct))
    if not cands:
        return {"source":source,"primary":primary,"effective":dict(primary),"override":False}
    cands.sort(key=lambda x:(-x[0],x[1]))
    cs,to,row,direct=cands[0]
    eff=dict(row)
    return {
        "source":source,"primary":primary,"effective":eff,"override":True,
        "override_detail":{"strength_ratio":cs/ps if ps else None,"destination_relation":direct}
    }


def download_hourly(asset,start,end):
    result=download_historical_dataset(
        BinanceSpotRestClient(),symbol=asset+"USDT",
        start=start,end=end,timeframe="1H",as_of=end+pd.Timedelta(hours=2)
    )
    if result.dataset is None:
        raise RuntimeError(f"{asset} 1H: {result.metadata.status}")
    if result.dataset.quality.has_critical_issues:
        raise RuntimeError(f"{asset} 1H critical quality")
    f=result.dataset.candles[["timestamp","open","high","low","close"]].copy()
    f["timestamp"]=pd.to_datetime(f["timestamp"],utc=True)
    return f.sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)


def build_hourly():
    start=fx.START
    end=fx.CUTOFF+pd.Timedelta(days=8)
    data={}
    meta={}
    for asset in fx.U10:
        f=download_hourly(asset,start,end)
        data[asset]=f.set_index("timestamp")
        meta[asset]={
            "rows":len(f),
            "start":f.iloc[0]["timestamp"].isoformat(),
            "end":f.iloc[-1]["timestamp"].isoformat(),
        }
    return data,meta


def target_geometry(panel,row):
    src=row["source"]; dst=row["destination"]
    si=int(row["signal_i"]); ei=int(row["extreme_i"])

    src_conf=float(panel.loc[si,src+"_close"])
    dst_conf=float(panel.loc[si,dst+"_close"])
    src_ext=float(panel.loc[ei,src+"_close"])
    dst_ext=float(panel.loc[ei,dst+"_close"])

    q_conf=src_conf/dst_conf
    q_ext=src_ext/dst_ext
    if not (q_ext>q_conf):
        # Effective DDG armed relation or numerical edge can violate the direct-confirm geometry.
        return {
            "q_conf":q_conf,"q_ext":q_ext,"src_conf":src_conf,"dst_conf":dst_conf,
            "src_ext":src_ext,"dst_ext":dst_ext,"exact3_applicable":False,
        }

    mode=row["mode"]
    if mode=="HIGH":
        target_factor=1.0/(1.0-REV)
    elif mode=="LOW":
        target_factor=1.0+REV
    else:
        target_factor=1.0+REV

    q_target=q_conf*target_factor
    applicable=q_target<=q_ext*(1+1e-10)

    out={
        "q_conf":q_conf,"q_ext":q_ext,
        "src_conf":src_conf,"dst_conf":dst_conf,
        "src_ext":src_ext,"dst_ext":dst_ext,
        "target_factor_exact3":target_factor,
        "q_target_exact3":q_target,
        "exact3_applicable":bool(applicable),
    }
    if applicable:
        den=math.log(q_ext/q_conf)
        lam=math.log(q_target/q_conf)/den if den>1e-15 else 0.0
        lam=min(max(lam,0.0),1.0)
        src_t=math.exp(math.log(src_conf)+lam*(math.log(src_ext)-math.log(src_conf)))
        dst_t=math.exp(math.log(dst_conf)+lam*(math.log(dst_ext)-math.log(dst_conf)))
        out.update({
            "lambda_exact3":lam,
            "src_target_exact3":src_t,
            "dst_target_exact3":dst_t,
            "exact3_ratio_check":src_t/dst_t,
        })
    return out


def fill_sequential(hourly,source,destination,start_ts,sell_target,buy_target,max_h=MAX_H):
    hs=hourly[source]
    hd=hourly[destination]

    sell_frame=hs[(hs.index>=start_ts)&(hs.index<start_ts+pd.Timedelta(hours=max_h))]
    sell_ts=None
    for ts,r in sell_frame.iterrows():
        if float(r["high"])+1e-12>=sell_target:
            sell_ts=ts
            break
    if sell_ts is None:
        return {"sell_filled":False,"buy_filled":False}

    # Conservative ordering: destination order is active only from the NEXT 1H candle.
    buy_start=sell_ts+pd.Timedelta(hours=1)
    buy_frame=hd[(hd.index>=buy_start)&(hd.index<start_ts+pd.Timedelta(hours=max_h))]
    buy_ts=None
    for ts,r in buy_frame.iterrows():
        if float(r["low"])-1e-12<=buy_target:
            buy_ts=ts
            break

    out={
        "sell_filled":True,
        "sell_fill_hour":sell_ts.isoformat(),
        "sell_wait_h":float((sell_ts-start_ts)/pd.Timedelta(hours=1)+1),
        "buy_filled":buy_ts is not None,
    }
    if buy_ts is not None:
        out.update({
            "buy_fill_hour":buy_ts.isoformat(),
            "complete_wait_h":float((buy_ts-start_ts)/pd.Timedelta(hours=1)+1),
            "post_sell_buy_wait_h":float((buy_ts-sell_ts)/pd.Timedelta(hours=1)),
        })
    return out


def extract_all_routes(panel,events,active):
    ts=pd.DatetimeIndex(panel["timestamp"])
    si=int(ts.searchsorted(fx.START,"left"))
    ei=int(ts.searchsorted(fx.CUTOFF,"right"))-1
    rows=[]
    for i in range(si,ei):
        for source in fx.U10:
            route=effective_route(events[i],active[i],source)
            if route is None:
                continue
            eff=dict(route["effective"])
            rows.append({
                "signal_i":i,
                "signal_date":ts[i].date().isoformat(),
                "available_ts":ts[i+1],
                "source":source,
                "destination":eff["to_asset"],
                "ddg_override":bool(route["override"]),
                "effective_event":eff["event"],
                "pair":eff["pair"],
                "mode":eff["mode"],
                "max_dislocation":float(eff["max_dislocation"]),
                "actual_reversal":float(eff.get("reversal_from_extreme") or 0),
                "extreme_i":int(eff["extreme_i"]),
                "primary_destination":route["primary"]["to_asset"],
            })
    return pd.DataFrame(rows)


def extract_path_hits(panel,events,active):
    ts=pd.DatetimeIndex(panel["timestamp"])
    si=int(ts.searchsorted(fx.START,"left"))
    ei=int(ts.searchsorted(fx.CUTOFF,"right"))-1
    counts={}
    for start in fx.U10:
        cur=start
        for i in range(si,ei):
            route=effective_route(events[i],active[i],cur)
            if route is None:
                continue
            eff=route["effective"]
            key=(i,cur,eff["to_asset"])
            counts[key]=counts.get(key,0)+1
            cur=eff["to_asset"]
    return counts


def evaluate(panel,hourly,routes,path_hits):
    ts=pd.DatetimeIndex(panel["timestamp"])
    rows=[]

    for _,r in routes.iterrows():
        base={
            **r.to_dict(),
            "path_hit_count":int(path_hits.get((int(r["signal_i"]),r["source"],r["destination"]),0)),
        }
        geom=target_geometry(panel,base)
        base.update(geom)

        i=int(r["signal_i"])
        baseline_src=float(panel.loc[i+1,r["source"]+"_open"])
        baseline_dst=float(panel.loc[i+1,r["destination"]+"_open"])
        baseline_q=baseline_src/baseline_dst
        base["baseline_next_open_q"]=baseline_q

        variants=[("EXTREME_REPLAY",geom["src_ext"],geom["dst_ext"],True)]
        if geom.get("exact3_applicable"):
            variants.append(("EXACT_3PCT_LOG",geom["src_target_exact3"],geom["dst_target_exact3"],True))
        else:
            variants.append(("EXACT_3PCT_LOG",np.nan,np.nan,False))

        for name,sell_t,buy_t,app in variants:
            row=dict(base)
            row["variant"]=name
            row["applicable"]=bool(app)
            row["sell_target"]=sell_t
            row["buy_target"]=buy_t
            if not app:
                rows.append(row); continue

            fill=fill_sequential(
                hourly,r["source"],r["destination"],r["available_ts"],
                float(sell_t),float(buy_t),MAX_H
            )
            row.update(fill)
            planned_q=float(sell_t)/float(buy_t)
            row["planned_q"]=planned_q
            row["gross_improvement_vs_next_open"]=planned_q/baseline_q-1.0
            # Real spot two-leg baseline: both strategies pay same two fees, so ratio improvement is unchanged.
            row["net_improvement_real_two_fee_vs_two_fee"]=planned_q/baseline_q-1.0
            # Compare against current research engine's one-cost transition convention.
            seq_units=planned_q*(1-COST)*(1-COST)
            synthetic_units=baseline_q*(1-COST)
            row["net_improvement_vs_current_one_fee_model"]=seq_units/synthetic_units-1.0
            for h in HORIZONS_H:
                row[f"sell_by_{h}h"]=bool(fill.get("sell_filled") and fill.get("sell_wait_h",1e9)<=h)
                row[f"complete_by_{h}h"]=bool(fill.get("buy_filled") and fill.get("complete_wait_h",1e9)<=h)
            rows.append(row)
    return pd.DataFrame(rows)


def summarize(df,scope_name,scope_mask):
    d=df[scope_mask].copy()
    rows=[]
    for variant in ("EXTREME_REPLAY","EXACT_3PCT_LOG"):
        g=d[(d["variant"]==variant)&(d["applicable"]==True)].copy()
        if len(g)==0: continue
        base={
            "scope":scope_name,"variant":variant,
            "n":len(g),
            "ddg_overrides":int(g["ddg_override"].sum()),
            "median_planned_gross_improvement":float(g["gross_improvement_vs_next_open"].median()),
            "median_net_improvement_vs_current_one_fee_model":float(g["net_improvement_vs_current_one_fee_model"].median()),
            "sell_filled_7d":int(g["sell_filled"].fillna(False).sum()),
            "complete_7d":int(g["buy_filled"].fillna(False).sum()),
            "sell_fill_rate_7d":float(g["sell_filled"].fillna(False).mean()),
            "complete_rate_7d":float(g["buy_filled"].fillna(False).mean()),
            "sell_filled_buy_missed_7d":int((g["sell_filled"].fillna(False)&~g["buy_filled"].fillna(False)).sum()),
            "median_complete_wait_h":float(g.loc[g["buy_filled"]==True,"complete_wait_h"].median()) if g["buy_filled"].any() else np.nan,
            "p95_complete_wait_h":float(g.loc[g["buy_filled"]==True,"complete_wait_h"].quantile(.95)) if g["buy_filled"].any() else np.nan,
            "max_complete_wait_h":float(g.loc[g["buy_filled"]==True,"complete_wait_h"].max()) if g["buy_filled"].any() else np.nan,
        }
        for h in HORIZONS_H:
            base[f"sell_rate_{h}h"]=float(g[f"sell_by_{h}h"].mean())
            base[f"complete_rate_{h}h"]=float(g[f"complete_by_{h}h"].mean())
        rows.append(base)
    return rows


def main():
    OUT.mkdir(parents=True,exist_ok=True)
    panel,_=fx.download_panel()
    panel["timestamp"]=pd.to_datetime(panel["timestamp"],utc=True)

    events,active=pair_tape_with_extremes(panel)
    routes=extract_all_routes(panel,events,active)
    path_hits=extract_path_hits(panel,events,active)
    hourly,hourly_meta=build_hourly()

    result=evaluate(panel,hourly,routes,path_hits)
    result.to_csv(OUT/"sequential_limit_route_results.csv",index=False)

    summary_rows=[]
    summary_rows += summarize(result,"ALL_EXECUTABLE_ROUTES",result.index==result.index)
    summary_rows += summarize(result,"DIRECT_CONFIRMED_ONLY",(~result["ddg_override"])&(result["effective_event"]=="CONFIRMED"))
    summary_rows += summarize(result,"PATH_VISITED",result["path_hit_count"]>0)
    summary_rows += summarize(result,"PATH_VISITED_DIRECT", (result["path_hit_count"]>0)&(~result["ddg_override"])&(result["effective_event"]=="CONFIRMED"))
    summary=pd.DataFrame(summary_rows)
    summary.to_csv(OUT/"fill_summary.csv",index=False)

    failures=result[
        (result["applicable"]==True)
        & (
            (~result["buy_filled"].fillna(False))
            | (result["complete_wait_h"].fillna(1e9)>48)
        )
    ].copy()
    failures.to_csv(OUT/"slow_or_failed_cases.csv",index=False)

    ddg=result[result["ddg_override"]==True].copy()
    ddg.to_csv(OUT/"ddg_override_cases.csv",index=False)

    exact=result[(result["variant"]=="EXACT_3PCT_LOG")&(result["applicable"]==True)].copy()
    exact_app_rate=float(
        result[result["variant"]=="EXACT_3PCT_LOG"]["applicable"].mean()
    )

    payload={
        "experiment":"SEQUENTIAL_LIMIT_RECAPTURE_V1",
        "workflow_mode":"STRESS_TEST_ONLY",
        "production_changes":"NONE",
        "universe":list(fx.U10),
        "route_opportunities":int(len(routes)),
        "path_visited_unique":int(sum(1 for v in path_hits.values() if v>0)),
        "exact3_applicability_rate":exact_app_rate,
        "hourly_meta":hourly_meta,
        "ordering_rule":"SELL FIRST; BUY ELIGIBLE FROM NEXT 1H CANDLE ONLY",
        "fill_rule":"SELL if hourly HIGH>=limit; BUY if hourly LOW<=limit",
        "targets":{
            "EXTREME_REPLAY":"source and destination daily close prices at pair-ratio extreme",
            "EXACT_3PCT_LOG":"log-price interpolation from confirmation closes toward extreme closes until exact 3% reversal threshold is recovered",
        },
        "summary":json.loads(summary.to_json(orient="records")),
        "test_level":"GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    }
    (OUT/"summary.json").write_text(json.dumps(payload,indent=2,sort_keys=True,default=str)+"\n",encoding="utf-8")

    lines=[
        "# SEQUENTIAL LIMIT RECAPTURE V1","",
        "Mode: STRESS_TEST_ONLY","",
        f"Current U10 route opportunities: {len(routes)}",
        f"Unique path-visited route episodes: {payload['path_visited_unique']}",
        f"EXACT_3PCT applicability: {100*exact_app_rate:.1f}%","",
        "Sell must fill first. Buy is not allowed until the next 1H candle.",
        "No same-hour ordering assumptions are accepted.","",
        "## Fill feasibility","",
        "|Scope|Variant|N|24h complete|48h|72h|5d|7d|Sell 7d|Sell→buy missed 7d|Median wait|P95 wait|",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _,r in summary.iterrows():
        lines.append(
            f"|{r['scope']}|{r['variant']}|{int(r['n'])}|"
            f"{100*r['complete_rate_24h']:.1f}%|{100*r['complete_rate_48h']:.1f}%|"
            f"{100*r['complete_rate_72h']:.1f}%|{100*r['complete_rate_120h']:.1f}%|"
            f"{100*r['complete_rate_168h']:.1f}%|{100*r['sell_fill_rate_7d']:.1f}%|"
            f"{int(r['sell_filled_buy_missed_7d'])}|"
            f"{r['median_complete_wait_h']:.1f}h|{r['p95_complete_wait_h']:.1f}h|"
        )

    lines += ["","## Planned exchange-rate improvement","",
              "|Scope|Variant|Median gross vs next-open|Median net vs current one-fee research model|",
              "|---|---|---:|---:|"]
    for _,r in summary.iterrows():
        lines.append(
            f"|{r['scope']}|{r['variant']}|"
            f"{100*r['median_planned_gross_improvement']:+.2f}%|"
            f"{100*r['median_net_improvement_vs_current_one_fee_model']:+.2f}%|"
        )

    lines += [
        "",
        "## Guardrails",
        "",
        "- 1H high/low proves target price existed during the hour, not queue priority or full exchange fill.",
        "- Same-hour sell->buy order is deliberately NOT credited.",
        "- EXTREME_REPLAY can recover more than 3% when the daily confirmation overshot the 3% threshold.",
        "- EXACT_3PCT_LOG is the strict 3%-threshold recovery construction.",
        "- DDG overrides are separated because their effective destination relation may be ARMED rather than the confirming pair.",
        "- This is execution feasibility only; it does not yet change the full RR path or capital curve.",
        "- No production/live/Telegram/exchange behavior changed.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    ]
    (OUT/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")
    print("\n".join(lines))

if __name__=="__main__":
    main()
