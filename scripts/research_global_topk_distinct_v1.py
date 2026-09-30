from __future__ import annotations

import itertools
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient

OUT = Path("research_artifacts/global_topk_distinct_v1")

U10 = ("TWT","PEPE","BNB","TRX","AAVE","AVAX","FIL","ALGO","XRP","HBAR")
LOOKBACK = 180
ARM = 0.15
REVERSAL = 0.03
DDG_RATIO = 1.50
COST = 0.001
KS = (1,2,3,4,5)

DATA_START = pd.Timestamp("2023-05-05", tz="UTC")
CUTOFF = pd.Timestamp("2026-09-27", tz="UTC")
WINDOWS = {
    "DISCOVERY": (pd.Timestamp("2023-10-31", tz="UTC"), pd.Timestamp("2025-09-26", tz="UTC")),
    "VALIDATION_1Y": (pd.Timestamp("2025-09-27", tz="UTC"), pd.Timestamp("2026-09-26", tz="UTC")),
    "LAST_2Y": (pd.Timestamp("2024-09-27", tz="UTC"), pd.Timestamp("2026-09-26", tz="UTC")),
    "MATURE": (pd.Timestamp("2023-10-31", tz="UTC"), pd.Timestamp("2026-09-26", tz="UTC")),
}
EXPECTED_PATH_DDG_MATURE = 35.8836

@dataclass
class PairState:
    mode: str = "NONE"
    extreme: float | None = None
    max_dislocation: float = 0.0


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
            raise RuntimeError(f"{asset}: critical candle quality")
        f = result.dataset.candles[["timestamp","open","close"]].copy()
        f["timestamp"] = pd.to_datetime(f["timestamp"], utc=True)
        f = f.rename(columns={"open":asset+"_open","close":asset+"_close"})
        meta[asset] = {
            "rows": int(len(f)),
            "start": pd.Timestamp(f.iloc[0]["timestamp"]).isoformat(),
            "end": pd.Timestamp(f.iloc[-1]["timestamp"]).isoformat(),
        }
        panel = f if panel is None else panel.merge(f, on="timestamp", how="inner", validate="one_to_one")
    return panel.sort_values("timestamp").reset_index(drop=True), meta


def build_tape(panel):
    n = len(panel)
    daily_events = [[] for _ in range(n)]
    daily_active = [[] for _ in range(n)]

    for left,right in itertools.combinations(U10,2):
        ratio = panel[right+"_close"].astype(float) / panel[left+"_close"].astype(float)
        median = ratio.rolling(LOOKBACK, min_periods=LOOKBACK).median()
        dev = ratio/median - 1.0
        st = PairState()

        for i in range(n):
            if pd.isna(median.iloc[i]) or pd.isna(dev.iloc[i]):
                continue
            r = float(ratio.iloc[i])
            d = float(dev.iloc[i])

            if st.mode == "NONE":
                if d >= ARM:
                    st = PairState("HIGH", r, abs(d))
                    daily_events[i].append({
                        "event":"ARMED","pair":f"{left}/{right}",
                        "from_asset":right,"to_asset":left,
                        "max_dislocation":abs(d),"deviation":d,
                    })
                elif d <= -ARM:
                    st = PairState("LOW", r, abs(d))
                    daily_events[i].append({
                        "event":"ARMED","pair":f"{left}/{right}",
                        "from_asset":left,"to_asset":right,
                        "max_dislocation":abs(d),"deviation":d,
                    })
            elif st.mode == "HIGH":
                if r > float(st.extreme):
                    st.extreme = r
                    st.max_dislocation = max(st.max_dislocation, abs(d))
                retr = 1.0 - r/float(st.extreme)
                if retr >= REVERSAL:
                    daily_events[i].append({
                        "event":"CONFIRMED","pair":f"{left}/{right}",
                        "from_asset":right,"to_asset":left,
                        "max_dislocation":st.max_dislocation,
                        "deviation":d,"reversal_from_extreme":retr,
                    })
                    st = PairState()
            elif st.mode == "LOW":
                if r < float(st.extreme):
                    st.extreme = r
                    st.max_dislocation = max(st.max_dislocation, abs(d))
                retr = r/float(st.extreme) - 1.0
                if retr >= REVERSAL:
                    daily_events[i].append({
                        "event":"CONFIRMED","pair":f"{left}/{right}",
                        "from_asset":left,"to_asset":right,
                        "max_dislocation":st.max_dislocation,
                        "deviation":d,"reversal_from_extreme":retr,
                    })
                    st = PairState()

            if st.mode == "HIGH":
                daily_active[i].append({
                    "event":"ARMED","pair":f"{left}/{right}",
                    "from_asset":right,"to_asset":left,
                    "max_dislocation":st.max_dislocation,
                })
            elif st.mode == "LOW":
                daily_active[i].append({
                    "event":"ARMED","pair":f"{left}/{right}",
                    "from_asset":left,"to_asset":right,
                    "max_dislocation":st.max_dislocation,
                })
    return daily_events, daily_active


def better(existing, candidate):
    if existing is None:
        return dict(candidate)
    ex_conf = str(existing.get("event")).upper() == "CONFIRMED"
    ca_conf = str(candidate.get("event")).upper() == "CONFIRMED"
    if ex_conf != ca_conf:
        return dict(candidate) if ca_conf else existing
    if float(candidate.get("max_dislocation",0)) > float(existing.get("max_dislocation",0)):
        return dict(candidate)
    return existing


def merged_rel(day_events, day_active):
    rel = {}
    for row in list(day_events)+list(day_active):
        key = (row["from_asset"],row["to_asset"])
        rel[key] = better(rel.get(key), row)
    return rel


def effective_route_for_source(day_events, day_active, source):
    confirmed = [
        dict(e) for e in day_events
        if e.get("event")=="CONFIRMED" and e.get("from_asset")==source
    ]
    confirmed.sort(key=lambda e:(-float(e["max_dislocation"]),str(e["to_asset"]),str(e["pair"])))
    if not confirmed:
        return None

    primary = confirmed[0]
    pto = primary["to_asset"]
    pstr = float(primary["max_dislocation"])
    rel = merged_rel(day_events, day_active)

    conflicts = []
    for (frm,to), row in rel.items():
        if frm != source or to == pto:
            continue
        strength = float(row.get("max_dislocation",0))
        if strength + 1e-12 < pstr*DDG_RATIO:
            continue
        destination_relation = rel.get((pto,to))
        if destination_relation is None:
            continue
        conflicts.append((strength,str(to),row,destination_relation))

    if not conflicts:
        return {
            "source":source,
            "destination":pto,
            "pair":primary["pair"],
            "strength":pstr,
            "primary_destination":pto,
            "ddg_override":False,
        }

    conflicts.sort(key=lambda x:(-x[0],x[1]))
    strength,to,row,dest_rel = conflicts[0]
    return {
        "source":source,
        "destination":to,
        "pair":row.get("pair"),
        "strength":float(strength),
        "primary_destination":pto,
        "ddg_override":True,
    }


def global_packet(day_events, day_active):
    routes = []
    for source in U10:
        row = effective_route_for_source(day_events, day_active, source)
        if row is not None:
            routes.append(row)

    # One independent bet per destination: keep strongest route into that destination.
    by_destination = {}
    for row in routes:
        d = row["destination"]
        cur = by_destination.get(d)
        key = (float(row["strength"]), -U10.index(row["source"]))
        if cur is None:
            by_destination[d] = row
        else:
            oldkey = (float(cur["strength"]), -U10.index(cur["source"]))
            if key > oldkey:
                by_destination[d] = row

    distinct = list(by_destination.values())
    distinct.sort(key=lambda r:(-float(r["strength"]),str(r["destination"]),str(r["source"])))
    return distinct


def build_packets(daily_events,daily_active):
    packets = {}
    for i,(ev,act) in enumerate(zip(daily_events,daily_active)):
        p = global_packet(ev,act)
        if p:
            packets[i] = p
    return packets


def bounds(ts,start,end):
    si = int(ts.searchsorted(utc(start),side="left"))
    ei = int(ts.searchsorted(utc(end),side="right")) - 1
    if ei < si:
        raise RuntimeError("empty bounds")
    return si,ei


def rebalance_at_open(qty,panel,idx,targets,cash=0.0):
    current_value = {
        a: float(qty.get(a,0.0))*float(panel.loc[idx,a+"_open"])
        for a in U10
    }
    total = float(sum(current_value.values()) + cash)
    if total <= 0:
        raise RuntimeError("non-positive portfolio value")

    weight = 1.0/len(targets)
    target_before_fee = {a:(total*weight if a in targets else 0.0) for a in U10}
    turnover = 0.5*sum(abs(current_value[a]-target_before_fee[a]) for a in U10)
    fee = COST*turnover
    after = total-fee
    newqty = {}
    for a in U10:
        tv = after*weight if a in targets else 0.0
        newqty[a] = tv/float(panel.loc[idx,a+"_open"]) if tv>0 else 0.0
    return newqty,fee,turnover,0.0


def latest_packet_before(packets, idx):
    keys = [k for k in packets.keys() if k < idx]
    if not keys:
        return None,None
    k = max(keys)
    return k,packets[k]


def selected_from_packet(packet,k):
    return packet[:min(k,len(packet))]


def simulate_global_topk(panel,ts,packets,start,end,k):
    si,ei = bounds(ts,start,end)
    prev_i,prev_packet = latest_packet_before(packets,si)

    qty = {a:0.0 for a in U10}
    cash = 0.0
    if prev_packet is None:
        # No confirmed global packet exists before this window.
        # Stay flat until the first causal packet inside the window,
        # then execute it at the next open.
        cash = 1.0
    else:
        selected = selected_from_packet(prev_packet,k)
        targets = [r["destination"] for r in selected]
        weight = 1.0/len(targets)
        for a in targets:
            qty[a] = weight/float(panel.loc[si,a+"_open"])

    equity = []
    largest = []
    held_counts = []
    pending = None
    costs = 0.0
    turnover = 0.0
    rebalances = 0
    packet_updates = 0
    target_changes = 0
    jan_snapshot = None
    update_ledger = []

    for idx in range(si,ei+1):
        if pending is not None:
            old_targets = set(a for a,q in qty.items() if q>0)
            new_targets = [r["destination"] for r in pending["selected"]]
            qty,fee,moved,cash = rebalance_at_open(qty,panel,idx,new_targets,cash)
            costs += fee
            turnover += moved
            rebalances += 1
            packet_updates += 1
            if set(new_targets) != old_targets:
                target_changes += 1

            update_ledger.append({
                "signal_date":ts[pending["signal_i"]].date().isoformat(),
                "execute_date":ts[idx].date().isoformat(),
                "targets":"|".join(new_targets),
                "routes":";".join(
                    f"{r['source']}->{r['destination']}:{r['strength']:.6f}"
                    for r in pending["selected"]
                ),
                "fee":fee,
                "turnover":moved,
            })

            if ts[idx].date().isoformat()=="2024-01-15":
                xrp_weight = 1.0/len(new_targets) if "XRP" in new_targets else 0.0
                jan_snapshot = {
                    "signal_date":pending["signal_date"],
                    "execute_date":ts[idx].date().isoformat(),
                    "targets":new_targets,
                    "xrp_weight":xrp_weight,
                    "routes":pending["selected"],
                }
            pending = None

        values = {
            a:float(qty.get(a,0.0))*float(panel.loc[idx,a+"_close"])
            for a in U10
        }
        total = float(sum(values.values()) + cash)
        equity.append(total)
        positive = [v for v in values.values() if v>0]
        largest.append(max(positive)/total if positive else 0.0)
        held_counts.append(len(positive))

        if idx>=ei:
            continue
        packet = packets.get(idx)
        if packet:
            sel = selected_from_packet(packet,k)
            pending = {
                "signal_i":idx,
                "signal_date":ts[idx].date().isoformat(),
                "selected":sel,
            }

    arr = np.asarray(equity,float)
    peak = np.maximum.accumulate(arr)
    shares = np.asarray(largest,float)
    counts = np.asarray(held_counts,int)
    return {
        "k":k,
        "return":float(arr[-1]/arr[0]-1.0),
        "max_dd":float(np.min(arr/peak-1.0)),
        "max_largest_share":float(shares.max()),
        "median_largest_share":float(np.median(shares)),
        "days_gt_50":float(np.mean(shares>0.50)),
        "days_gt_80":float(np.mean(shares>0.80)),
        "median_held_assets":float(np.median(counts)),
        "min_held_assets":int(counts.min()),
        "max_held_assets":int(counts.max()),
        "rebalances":int(rebalances),
        "packet_updates":int(packet_updates),
        "target_changes":int(target_changes),
        "cost_paid":float(costs),
        "turnover_notional":float(turnover),
        "jan2024_xrp_weight":None if jan_snapshot is None else float(jan_snapshot["xrp_weight"]),
        "jan2024_targets":None if jan_snapshot is None else "|".join(jan_snapshot["targets"]),
        "jan2024_routes":None if jan_snapshot is None else ";".join(
            f"{r['source']}->{r['destination']}:{r['strength']:.6f}"
            for r in jan_snapshot["routes"]
        ),
        "ending_assets":"|".join(a for a,q in qty.items() if q>0),
        "_ledger":update_ledger,
    }


def simulate_path_rr(panel,ts,daily_events,daily_active,start,end,start_asset):
    si,ei = bounds(ts,start,end)
    cur = start_asset
    qty = 1.0/float(panel.loc[si,cur+"_open"])
    eq = []
    pending = None
    transitions = 0
    for idx in range(si,ei+1):
        if pending is not None:
            value = qty*float(panel.loc[idx,cur+"_open"])
            cur = pending["destination"]
            qty = value*(1.0-COST)/float(panel.loc[idx,cur+"_open"])
            transitions += 1
            pending = None
        eq.append(qty*float(panel.loc[idx,cur+"_close"]))
        if idx < ei:
            r = effective_route_for_source(daily_events[idx],daily_active[idx],cur)
            if r is not None:
                pending = r
    arr = np.asarray(eq,float)
    peak = np.maximum.accumulate(arr)
    return {
        "return":float(arr[-1]/arr[0]-1.0),
        "max_dd":float(np.min(arr/peak-1.0)),
        "transitions":transitions,
    }


def rolling(panel,ts,packets,k,months):
    rows = []
    first = pd.Timestamp("2023-11-01",tz="UTC")
    limit = WINDOWS["MATURE"][1]
    for start in pd.date_range(first,limit,freq="MS"):
        end = start+pd.DateOffset(months=months)-pd.Timedelta(days=1)
        if end > limit:
            continue
        r = simulate_global_topk(panel,ts,packets,start,end,k)
        rows.append({
            "k":k,"months":months,
            "start":start.date().isoformat(),"end":end.date().isoformat(),
            "return":r["return"],"max_dd":r["max_dd"],
            "median_largest_share":r["median_largest_share"],
            "max_largest_share":r["max_largest_share"],
        })
    return pd.DataFrame(rows)


def main():
    OUT.mkdir(parents=True,exist_ok=True)
    panel,meta = download_panel()
    ts = pd.DatetimeIndex(pd.to_datetime(panel["timestamp"],utc=True))
    daily_events,daily_active = build_tape(panel)
    packets = build_packets(daily_events,daily_active)

    # Canonical path-dependent DDG validation.
    path_runs = [
        simulate_path_rr(panel,ts,daily_events,daily_active,*WINDOWS["MATURE"],a)
        for a in U10
    ]
    path_med = float(np.median([r["return"] for r in path_runs]))
    if abs(path_med-EXPECTED_PATH_DDG_MATURE)>0.02:
        raise RuntimeError(
            f"path DDG baseline mismatch {path_med:.6f} vs expected ~{EXPECTED_PATH_DDG_MATURE:.6f}"
        )

    rows = []
    for k in KS:
        for w,(start,end) in WINDOWS.items():
            r = simulate_global_topk(panel,ts,packets,start,end,k)
            rows.append({
                "k":k,"window":w,
                **{kk:vv for kk,vv in r.items() if kk not in {"k","_ledger"}},
            })

    results = pd.DataFrame(rows)
    results.to_csv(OUT/"global_topk_distinct_results.csv",index=False)

    # Full Jan-2024 packet for causal inspection.
    jan_idx = int(ts.searchsorted(pd.Timestamp("2024-01-14",tz="UTC"),side="left"))
    jan_packet = packets.get(jan_idx,[])
    jan_rows = []
    for rank,row in enumerate(jan_packet,1):
        jan_rows.append({"rank":rank,**row})
    pd.DataFrame(jan_rows).to_csv(OUT/"jan2024_global_packet.csv",index=False)

    rolling_frames = []
    for k in KS:
        rolling_frames.append(rolling(panel,ts,packets,k,12))
        rolling_frames.append(rolling(panel,ts,packets,k,24))
    roll = pd.concat(rolling_frames,ignore_index=True)
    roll.to_csv(OUT/"rolling_global_topk.csv",index=False)

    roll_summary = []
    for (k,months),g in roll.groupby(["k","months"]):
        roll_summary.append({
            "k":int(k),"months":int(months),"windows":int(len(g)),
            "median_return":float(g["return"].median()),
            "worst_return":float(g["return"].min()),
            "positive_rate":float((g["return"]>0).mean()),
            "median_dd":float(g["max_dd"].median()),
            "worst_dd":float(g["max_dd"].min()),
            "median_peak_share":float(g["max_largest_share"].median()),
        })
    roll_summary_df = pd.DataFrame(roll_summary)
    roll_summary_df.to_csv(OUT/"rolling_global_topk_summary.csv",index=False)

    summary = {
        "experiment":"GLOBAL_TOPK_DISTINCT_V1",
        "workflow_mode":"STRESS_TEST_ONLY",
        "test_level":"GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
        "production_changes":"NONE",
        "mechanics":{
            "lookback":LOOKBACK,"arm":ARM,"reversal":REVERSAL,
            "ddg_ratio":DDG_RATIO,"cost":COST,
            "selection":"GLOBAL_EFFECTIVE_DDG_ROUTES__ONE_STRONGEST_ROUTE_PER_DESTINATION",
            "ranking":"MAX_DISLOCATION_DESC",
            "weights":"EQUAL_ACROSS_SELECTED_DISTINCT_DESTINATIONS",
            "execution":"SIGNAL_CLOSE_T_TO_NEXT_OPEN",
            "between_updates":"HOLD_LAST_SELECTED_DESTINATIONS",
        },
        "canonical_path_ddg_mature_median_return":path_med,
        "packet_count":len(packets),
        "results":json.loads(results.to_json(orient="records")),
        "rolling_summary":json.loads(roll_summary_df.to_json(orient="records")),
        "jan2024_packet":jan_rows,
        "data_metadata":meta,
    }
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True,default=str)+"\n",encoding="utf-8")

    lines = [
        "# GLOBAL TOP-K DISTINCT V1","",
        "Mode: STRESS_TEST_ONLY","",
        f"Canonical path DDG Mature validation: {100*path_med:+.1f}%","",
        "## Primary results","",
        "|K|Discovery|Validation 1Y|2Y|Mature|Mature DD|Median largest share|Peak share|Jan-2024 XRP weight|",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for k in KS:
        sub = results[results.k==k].set_index("window")
        d=sub.loc["DISCOVERY"]; v=sub.loc["VALIDATION_1Y"]; y2=sub.loc["LAST_2Y"]; m=sub.loc["MATURE"]
        jw=m["jan2024_xrp_weight"]
        lines.append(
            f"|{k}|{100*d['return']:+.1f}%|{100*v['return']:+.1f}%|{100*y2['return']:+.1f}%|"
            f"{100*m['return']:+.1f}%|{100*m['max_dd']:+.1f}%|"
            f"{100*m['median_largest_share']:.1f}%|{100*m['max_largest_share']:.1f}%|"
            f"{'' if pd.isna(jw) else f'{100*jw:.1f}%'}|"
        )

    lines += ["","## Jan 14 2024 global distinct packet","",
              "|Rank|Route|Destination|Strength|DDG override|",
              "|---:|---|---|---:|---|"]
    for row in jan_rows:
        lines.append(
            f"|{row['rank']}|{row['source']}->{row['destination']}|{row['destination']}|"
            f"{100*row['strength']:.2f}%|{row['ddg_override']}|"
        )
    lines += ["","No production/live/Telegram/execution configuration changed.","",
              "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST"]
    (OUT/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")
    print("\n".join(lines))


if __name__=="__main__":
    main()
