from __future__ import annotations

import bisect
import json
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import (
    build_pair_monitor,
    choose_held_events,
    find_route_conflicts,
    choose_destination_dominance_override,
)

OUT = Path("research_artifacts/xrp_node_interference_fork_v1")

U10 = ("TWT","PEPE","BNB","TRX","AAVE","AVAX","FIL","ALGO","XRP","HBAR")
U9 = tuple(a for a in U10 if a != "XRP")
COMMON_STARTS = U9
LOOKBACK = 180
ARM = 0.15
REVERSAL = 0.03
COST = 0.001
DATA_START = pd.Timestamp("2023-05-05", tz="UTC")
MATURE_START = pd.Timestamp("2023-10-31", tz="UTC")
EVAL_END = pd.Timestamp("2026-09-26", tz="UTC")
CUTOFF = pd.Timestamp("2026-09-27", tz="UTC")


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
            raise RuntimeError(f"{asset}: critical candle-quality issue")
        f = result.dataset.candles[["timestamp","open","close"]].copy()
        f["timestamp"] = pd.to_datetime(f["timestamp"], utc=True)
        f = f.rename(columns={"open":asset+"_open","close":asset+"_close"})
        meta[asset] = {
            "rows": int(len(f)),
            "start": pd.Timestamp(f.iloc[0]["timestamp"]).isoformat(),
            "end": pd.Timestamp(f.iloc[-1]["timestamp"]).isoformat(),
        }
        panel = f if panel is None else panel.merge(f,on="timestamp",how="inner",validate="one_to_one")
    return panel.sort_values("timestamp").reset_index(drop=True), meta


def arrays(panel):
    ts = pd.DatetimeIndex(pd.to_datetime(panel["timestamp"], utc=True))
    opens = {a: panel[a+"_open"].astype(float).to_numpy() for a in U10}
    closes = {a: panel[a+"_close"].astype(float).to_numpy() for a in U10}
    return ts, opens, closes


def bounds(ts, start, end):
    si = int(ts.searchsorted(utc(start), side="left"))
    ei = int(ts.searchsorted(utc(end), side="right")) - 1
    if ei < si:
        raise RuntimeError("empty bounds")
    return si, ei


def build_signal_index(panel, ts):
    events, _ = build_pair_monitor(
        panel[["timestamp"]+[a+"_close" for a in U10]].copy(),
        assets=U10,
        lookback=LOOKBACK,
        arm_threshold=ARM,
        reversal=REVERSAL,
    )
    ts_to_i = {utc(t):i for i,t in enumerate(ts)}
    grouped = defaultdict(list)
    for e in events:
        if e.get("event") != "CONFIRMED":
            continue
        i = ts_to_i.get(utc(e["date"]))
        if i is None:
            continue
        grouped[(e["from_asset"],i)].append({
            "date":e["date"],
            "event":"CONFIRMED",
            "from_asset":e["from_asset"],
            "to_asset":e["to_asset"],
            "pair":e["pair"],
            "max_dislocation":float(e["max_dislocation"]),
            "deviation":float(e["deviation"]),
            "reversal_from_extreme":float(e["reversal_from_extreme"]),
        })
    positions = {a:[] for a in U10}
    candidates = {a:[] for a in U10}
    for (frm,i),items in grouped.items():
        items.sort(key=lambda x:(-x["max_dislocation"],x["to_asset"],x["pair"]))
        positions[frm].append(i)
        candidates[frm].append(items)
    for a in U10:
        if positions[a]:
            order = np.argsort(positions[a]).tolist()
            positions[a] = [positions[a][j] for j in order]
            candidates[a] = [candidates[a][j] for j in order]
    return events, positions, candidates


def simulate(
    ts, opens, closes, positions, candidates, assets,
    start_i, end_i, start_asset,
    initial_capital=None, allow_signal_on_start=True,
):
    allowed = set(assets)
    cur = start_asset
    if initial_capital is None:
        qty = 1.0/float(opens[cur][start_i])
        initial_equity = qty*float(closes[cur][start_i])
    else:
        initial_equity = float(initial_capital)
        qty = initial_equity/float(closes[cur][start_i])

    n = end_i-start_i+1
    equity = np.empty(n,dtype=float)
    holdings = np.empty(n,dtype=object)
    ledger = []
    seg = start_i
    search_from = start_i if allow_signal_on_start else start_i+1

    while seg <= end_i:
        pl = positions[cur]
        ca = candidates[cur]
        p = bisect.bisect_left(pl, search_from)
        transitioned = False
        while p < len(pl):
            signal_i = pl[p]
            if signal_i >= end_i:
                break
            eligible = [e for e in ca[p] if e["to_asset"] in allowed]
            if not eligible:
                p += 1
                continue
            chosen = eligible[0]
            lo, hi = seg-start_i, signal_i-start_i+1
            equity[lo:hi] = qty*closes[cur][seg:signal_i+1]
            holdings[lo:hi] = cur

            execute_i = signal_i+1
            value_before = qty*float(opens[cur][execute_i])
            nxt = chosen["to_asset"]
            qty_after = value_before*(1.0-COST)/float(opens[nxt][execute_i])
            ledger.append({
                "signal_i":int(signal_i),
                "execute_i":int(execute_i),
                "signal_date":ts[signal_i].isoformat(),
                "execute_date":ts[execute_i].isoformat(),
                "from_asset":cur,
                "to_asset":nxt,
                "pair":chosen["pair"],
                "max_dislocation":chosen["max_dislocation"],
                "equity_close_signal":float(qty*closes[cur][signal_i]),
                "value_before_cost_at_execution":float(value_before),
            })
            cur = nxt
            qty = qty_after
            seg = execute_i
            search_from = execute_i
            transitioned = True
            break

        if transitioned:
            continue
        lo = seg-start_i
        equity[lo:] = qty*closes[cur][seg:end_i+1]
        holdings[lo:] = cur
        break

    return {
        "initial_equity":float(initial_equity),
        "final_equity":float(equity[-1]),
        "return":float(equity[-1]/initial_equity-1.0),
        "equity":equity,
        "holdings":holdings,
        "ledger":ledger,
        "final_asset":str(holdings[-1]),
    }


def eval_median(ts, opens, closes, positions, candidates, assets, si, ei):
    runs = [simulate(ts,opens,closes,positions,candidates,assets,si,ei,a) for a in assets]
    return {
        "median_return":float(np.median([r["return"] for r in runs])),
        "median_final_equity":float(np.median([r["final_equity"] for r in runs])),
        "median_transitions":float(np.median([len(r["ledger"]) for r in runs])),
    }


def difference_segments(ts, start_i, h9, h10):
    diff = np.asarray(h9) != np.asarray(h10)
    rows = []
    if len(diff)==0:
        return rows
    s = 0
    state = bool(diff[0])
    for i in range(1,len(diff)+1):
        if i==len(diff) or bool(diff[i]) != state:
            rows.append({
                "different":state,
                "start_date":ts[start_i+s].date().isoformat(),
                "end_date":ts[start_i+i-1].date().isoformat(),
                "days":i-s,
                "u9_asset_start":str(h9[s]),
                "u10_asset_start":str(h10[s]),
                "u9_asset_end":str(h9[i-1]),
                "u10_asset_end":str(h10[i-1]),
            })
            if i < len(diff):
                s=i
                state=bool(diff[i])
    return rows


def first_stable_reconvergence(diff, min_days=30):
    i=0
    while i<len(diff):
        if diff[i]:
            i+=1
            continue
        j=i
        while j<len(diff) and not diff[j]:
            j+=1
        if j-i>=min_days:
            return i
        i=j
    return None


def latest_relation_rows(events, states, date_iso):
    latest_events = [
        dict(e) for e in events
        if str(e.get("date"))==date_iso and str(e.get("event")).upper() in {"ARMED","CONFIRMED"}
    ]
    rows = latest_events[:]
    for s in states:
        mode = str(s.get("mode") or "NONE").upper()
        frm = str(s.get("from_asset") or "").upper()
        to = str(s.get("to_asset") or "").upper()
        if mode in {"HIGH","LOW"} and frm and to:
            rows.append({
                "date":date_iso,
                "event":"ARMED",
                "pair":s.get("pair"),
                "from_asset":frm,
                "to_asset":to,
                "max_dislocation":float(s.get("max_dislocation") or 0.0),
                "deviation":s.get("deviation"),
                "reversal_from_extreme":s.get("reversal_from_extreme"),
                "armed_at":s.get("armed_at"),
            })
    return rows


def best_relation(rows, frm, to):
    hits=[r for r in rows if str(r.get("from_asset")).upper()==frm and str(r.get("to_asset")).upper()==to]
    if not hits:
        return None
    hits.sort(key=lambda r:(
        0 if str(r.get("event")).upper()=="CONFIRMED" else 1,
        -float(r.get("max_dislocation") or 0.0),
    ))
    return hits[0]


def audit_xrp_entry(panel, ts, entry):
    signal_i = entry["signal_i"]
    date = ts[signal_i]
    prefix = panel.iloc[:signal_i+1][["timestamp"]+[a+"_close" for a in U10]].copy()
    ev, states = build_pair_monitor(
        prefix, assets=U10, lookback=LOOKBACK, arm_threshold=ARM, reversal=REVERSAL
    )
    source = entry["from_asset"]
    held10 = choose_held_events(ev,held_asset=source,latest_date=date,allowed_to_assets=U10)
    held9 = choose_held_events(ev,held_asset=source,latest_date=date,allowed_to_assets=U9)
    primary10 = held10["primary_confirmed"]
    primary9 = held9["primary_confirmed"]
    conflicts = find_route_conflicts(
        ev, states,
        primary_confirmed=primary10,
        latest_date=date,
        allowed_to_assets=U10,
    )
    effective, chosen_conflict = choose_destination_dominance_override(primary10,conflicts)
    rows = latest_relation_rows(ev,states,date.isoformat())

    alt = None if primary9 is None else str(primary9["to_asset"]).upper()
    xrp_strength = None if primary10 is None else float(primary10.get("max_dislocation") or 0.0)
    alt_strength = None
    direct = None
    if alt:
        rel = best_relation(rows,source,alt)
        if rel:
            alt_strength=float(rel.get("max_dislocation") or 0.0)
        direct = best_relation(rows,"XRP",alt)

    if primary9 is None:
        reason = "U9_HAS_NO_SAME_DAY_CONFIRMED_EXIT__ITS_ALTERNATIVE_IS_HOLD"
    elif alt_strength is None or xrp_strength in (None,0):
        reason = "ALTERNATIVE_STRENGTH_UNAVAILABLE"
    elif alt_strength/xrp_strength < 1.5:
        reason = "ALTERNATIVE_BELOW_1P5X_STRENGTH"
    elif direct is None:
        reason = "NO_XRP_TO_ALTERNATIVE_ARMED_OR_CONFIRMED_RELATION"
    elif effective and str(effective.get("to_asset")).upper() != "XRP":
        reason = "DDG_1P5X_BLOCKS_XRP"
    else:
        reason = "QUALIFIED_BUT_NOT_OVERRIDDEN_CHECK"

    return {
        "signal_date":entry["signal_date"],
        "execute_date":entry["execute_date"],
        "source_asset":source,
        "baseline_u10_to":None if primary10 is None else primary10.get("to_asset"),
        "baseline_u10_strength":xrp_strength,
        "u9_same_day_to":"HOLD" if primary9 is None else primary9.get("to_asset"),
        "u9_same_day_strength":alt_strength,
        "alt_to_xrp_strength_ratio":None if alt_strength is None or not xrp_strength else alt_strength/xrp_strength,
        "xrp_to_u9_alt_relation_event":None if direct is None else direct.get("event"),
        "xrp_to_u9_alt_relation_strength":None if direct is None else float(direct.get("max_dislocation") or 0.0),
        "ddg_conflict_count":len(conflicts),
        "ddg_effective_to":None if effective is None else effective.get("to_asset"),
        "ddg_blocks_xrp":bool(effective is not None and str(effective.get("to_asset")).upper()!="XRP"),
        "diagnosis":reason,
        "all_confirmed_u10":json.dumps([
            {"to":e["to_asset"],"strength":float(e["max_dislocation"])}
            for e in held10["confirmed"]
        ],sort_keys=True),
        "all_armed_u10":json.dumps([
            {"to":e["to_asset"],"strength":float(e["max_dislocation"])}
            for e in held10["armed"]
        ],sort_keys=True),
        "ddg_conflicts":json.dumps(conflicts,sort_keys=True,default=str),
    }


def main():
    OUT.mkdir(parents=True,exist_ok=True)
    panel, data_meta = download_panel()
    ts, opens, closes = arrays(panel)
    full_events, positions, candidates = build_signal_index(panel,ts)
    si, ei = bounds(ts,MATURE_START,EVAL_END)

    validation9 = eval_median(ts,opens,closes,positions,candidates,U9,si,ei)
    validation10 = eval_median(ts,opens,closes,positions,candidates,U10,si,ei)

    all_segments=[]
    all_entry_keys={}
    start_rows=[]
    representative=None

    for start in COMMON_STARTS:
        r9=simulate(ts,opens,closes,positions,candidates,U9,si,ei,start)
        r10=simulate(ts,opens,closes,positions,candidates,U10,si,ei,start)
        segs=difference_segments(ts,si,r9["holdings"],r10["holdings"])
        for row in segs:
            row["start_asset"]=start
            all_segments.append(row)

        diff=np.asarray(r9["holdings"])!=np.asarray(r10["holdings"])
        first=np.where(diff)[0][0] if diff.any() else None
        reconv=first_stable_reconvergence(diff,30) if first is not None else None
        first_global=None if first is None else si+int(first)
        reconv_global=None if reconv is None else si+int(reconv)

        xrp_entries=[x for x in r10["ledger"] if x["to_asset"]=="XRP"]
        for e in xrp_entries:
            all_entry_keys[(e["signal_date"],e["from_asset"],e["to_asset"])]=e

        start_rows.append({
            "start_asset":start,
            "u9_return":r9["return"],
            "u10_return":r10["return"],
            "final_capital_ratio_u9_to_u10":r9["final_equity"]/r10["final_equity"],
            "first_divergence_date":None if first_global is None else ts[first_global].date().isoformat(),
            "first_divergence_u9_asset":None if first is None else str(r9["holdings"][first]),
            "first_divergence_u10_asset":None if first is None else str(r10["holdings"][first]),
            "stable_reconvergence_date":None if reconv_global is None else ts[reconv_global].date().isoformat(),
            "reconvergence_asset":None if reconv is None else str(r9["holdings"][reconv]),
            "xrp_entry_count":len(xrp_entries),
        })
        if start=="TWT":
            representative=(r9,r10,first_global,reconv_global)

    pd.DataFrame(all_segments).to_csv(OUT/"divergence_segments.csv",index=False)
    pd.DataFrame(start_rows).to_csv(OUT/"start_path_comparison.csv",index=False)

    # Audit each unique baseline transition into XRP.
    audits=[audit_xrp_entry(panel,ts,e) for e in sorted(all_entry_keys.values(),key=lambda x:x["signal_date"])]
    pd.DataFrame(audits).to_csv(OUT/"xrp_entry_audit.csv",index=False)

    r9,r10,first_global,reconv_global=representative
    fork_rows=[]

    # First divergence fork: same source, same 1.0 capital at signal close.
    first_xrp=next(e for e in r10["ledger"] if e["to_asset"]=="XRP")
    fork_signal_i=int(first_xrp["signal_i"])
    fork_source=first_xrp["from_asset"]
    f9=simulate(ts,opens,closes,positions,candidates,U9,fork_signal_i,ei,fork_source,initial_capital=1.0,allow_signal_on_start=True)
    f10=simulate(ts,opens,closes,positions,candidates,U10,fork_signal_i,ei,fork_source,initial_capital=1.0,allow_signal_on_start=True)
    fork_rows.append({
        "test":"EQUAL_CAPITAL_AT_FIRST_XRP_SIGNAL",
        "date":ts[fork_signal_i].date().isoformat(),
        "asset":fork_source,
        "u9_final":f9["final_equity"],
        "u10_final":f10["final_equity"],
        "u9_to_u10_ratio":f9["final_equity"]/f10["final_equity"],
        "interpretation":"future route quality from the exact fork, with past capital erased",
    })

    # Immediately after first XRP exit: remove XRP and see whether future U9 road can recover.
    first_exit=next(e for e in r10["ledger"] if e["from_asset"]=="XRP" and e["execute_i"]>first_xrp["execute_i"])
    switch_i=int(first_exit["execute_i"])
    switch_asset=first_exit["to_asset"]
    s9=simulate(ts,opens,closes,positions,candidates,U9,switch_i,ei,switch_asset,initial_capital=1.0,allow_signal_on_start=False)
    s10=simulate(ts,opens,closes,positions,candidates,U10,switch_i,ei,switch_asset,initial_capital=1.0,allow_signal_on_start=False)
    actual_u10_cap=float(r10["equity"][switch_i-si])
    switched_final=actual_u10_cap*s9["final_equity"]
    natural_u10_final=float(r10["final_equity"])
    natural_u9_final=float(r9["final_equity"])
    fork_rows.append({
        "test":"EQUAL_CAPITAL_AFTER_FIRST_XRP_EXIT",
        "date":ts[switch_i].date().isoformat(),
        "asset":switch_asset,
        "u9_final":s9["final_equity"],
        "u10_final":s10["final_equity"],
        "u9_to_u10_ratio":s9["final_equity"]/s10["final_equity"],
        "interpretation":"does U9 continue to have a structural edge after the damaging XRP holding episode?",
    })
    fork_rows.append({
        "test":"ACTUAL_U10_CAPITAL_SWITCH_TO_U9_AFTER_XRP_EXIT",
        "date":ts[switch_i].date().isoformat(),
        "asset":switch_asset,
        "u9_final":switched_final,
        "u10_final":natural_u10_final,
        "u9_to_u10_ratio":switched_final/natural_u10_final,
        "interpretation":"how much final capital would be recovered if XRP were removed only after the damage already occurred?",
        "natural_u9_final_reference":natural_u9_final,
    })

    # Stable reconvergence: erase accumulated x2 and compare only the remaining road.
    if reconv_global is not None:
        reconv_asset=str(r9["holdings"][reconv_global-si])
        q9=simulate(ts,opens,closes,positions,candidates,U9,reconv_global,ei,reconv_asset,initial_capital=1.0,allow_signal_on_start=False)
        q10=simulate(ts,opens,closes,positions,candidates,U10,reconv_global,ei,reconv_asset,initial_capital=1.0,allow_signal_on_start=False)
        actual_cap=float(r10["equity"][reconv_global-si])
        switch_final=actual_cap*q9["final_equity"]
        fork_rows.append({
            "test":"EQUAL_CAPITAL_AT_STABLE_RECONVERGENCE",
            "date":ts[reconv_global].date().isoformat(),
            "asset":reconv_asset,
            "u9_final":q9["final_equity"],
            "u10_final":q10["final_equity"],
            "u9_to_u10_ratio":q9["final_equity"]/q10["final_equity"],
            "interpretation":"tests whether U9 keeps driving faster after both routes are back on the same road",
        })
        fork_rows.append({
            "test":"ACTUAL_U10_CAPITAL_SWITCH_TO_U9_AT_RECONVERGENCE",
            "date":ts[reconv_global].date().isoformat(),
            "asset":reconv_asset,
            "u9_final":switch_final,
            "u10_final":natural_u10_final,
            "u9_to_u10_ratio":switch_final/natural_u10_final,
            "interpretation":"tests recoverable value after the early x2 gap has already been locked in",
            "natural_u9_final_reference":natural_u9_final,
        })

    fork_df=pd.DataFrame(fork_rows)
    fork_df.to_csv(OUT/"fork_tests.csv",index=False)

    summary={
        "experiment":"XRP_NODE_INTERFERENCE_FORK_V1",
        "workflow_mode":"STRESS_TEST_ONLY",
        "test_level":"GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
        "production_changes":"NONE",
        "parameters":{"lookback":LOOKBACK,"arm":ARM,"reversal":REVERSAL,"cost":COST},
        "window":{"start":MATURE_START.isoformat(),"end":EVAL_END.isoformat()},
        "u9":list(U9),"u10":list(U10),
        "validation":{"u9":validation9,"u10":validation10},
        "unique_xrp_entries":audits,
        "fork_tests":json.loads(fork_df.to_json(orient="records")),
        "data_metadata":data_meta,
    }
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True,default=str)+"\n",encoding="utf-8")

    lines=[
        "# XRP NODE INTERFERENCE + U9/U10 FORK V1","",
        "Mode: STRESS_TEST_ONLY","",
        f"Validation U9 Mature: {100*validation9['median_return']:+.1f}%",
        f"Validation U10 Mature: {100*validation10['median_return']:+.1f}%","",
        "## XRP entry audit","",
        "|signal|source|U10 target|U9 same-day action|DDG effective|blocks XRP|diagnosis|",
        "|---|---|---|---|---|---|---|",
    ]
    for a in audits:
        lines.append(f"|{a['signal_date'][:10]}|{a['source_asset']}|{a['baseline_u10_to']}|{a['u9_same_day_to']}|{a['ddg_effective_to']}|{a['ddg_blocks_xrp']}|{a['diagnosis']}|")
    lines += ["","## Fork tests","",
              "|test|date|asset|U9 final from 1|U10 final from 1|ratio|",
              "|---|---|---|---:|---:|---:|"]
    for _,r in fork_df.iterrows():
        lines.append(f"|{r['test']}|{r['date']}|{r['asset']}|{r['u9_final']:.4f}|{r['u10_final']:.4f}|{r['u9_to_u10_ratio']:.4f}x|")
    lines += ["","No production/live configuration changed.","",
              "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST"]
    (OUT/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")
    print("\n".join(lines))


if __name__=="__main__":
    main()
