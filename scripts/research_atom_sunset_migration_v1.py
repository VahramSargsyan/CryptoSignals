from __future__ import annotations

import argparse
import json
import os
import subprocess
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import build_pair_monitor

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "atom_sunset_migration_v1"

U10 = ("ATOM","TWT","PEPE","BNB","SOL","TRX","AAVE","LINK","FIL","HBAR")
U9 = tuple(a for a in U10 if a != "ATOM")
COMMON_STARTERS = U9

LOOKBACK = 180
ARM = 0.15
REVERSAL = 0.03
COST = 0.001

DATA_START = pd.Timestamp("2023-05-05",tz="UTC")
MATURE_START = pd.Timestamp("2023-10-31",tz="UTC")
TWO_YEAR_START = pd.Timestamp("2024-09-27",tz="UTC")
YEAR_START = pd.Timestamp("2025-09-27",tz="UTC")
END = pd.Timestamp("2026-09-26",tz="UTC")


def utc(x):
    t = pd.Timestamp(x)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def source_sha():
    v = os.environ.get("SOURCE_COMMIT_SHA")
    if v:
        return v
    return subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip()


def download_panel(cutoff):
    client = BinanceSpotRestClient()
    panel = None
    meta = {}
    for a in U10:
        result = download_historical_dataset(
            client,
            symbol=a+"USDT",
            start=DATA_START,
            end=cutoff,
            timeframe="1D",
            as_of=cutoff,
        )
        if result.dataset is None:
            raise RuntimeError(f"{a}: {result.metadata.status}")
        if result.dataset.quality.has_critical_issues:
            raise RuntimeError(f"{a}: critical data quality")
        f = result.dataset.candles[["timestamp","open","close"]].copy()
        f = f.rename(columns={"open":a+"_open","close":a+"_close"})
        meta[a] = {
            "rows":len(f),
            "start":pd.Timestamp(f.iloc[0]["timestamp"]).isoformat(),
            "end":pd.Timestamp(f.iloc[-1]["timestamp"]).isoformat(),
        }
        panel = f if panel is None else panel.merge(f,on="timestamp",how="inner",validate="one_to_one")
    return panel.sort_values("timestamp").reset_index(drop=True),meta


def build_event_index(panel):
    cols = ["timestamp"] + [a+"_close" for a in U10]
    events,_ = build_pair_monitor(
        panel[cols].copy(),
        assets=U10,
        lookback=LOOKBACK,
        arm_threshold=ARM,
        reversal=REVERSAL,
    )
    idx = defaultdict(lambda:defaultdict(list))
    for e in events:
        if e["event"] != "CONFIRMED":
            continue
        d = utc(e["date"])
        idx[d][e["from_asset"]].append({
            "to_asset":e["to_asset"],
            "pair":e["pair"],
            "max_dislocation":float(e["max_dislocation"]),
        })
    for d in idx:
        for a in idx[d]:
            idx[d][a].sort(key=lambda e:(-e["max_dislocation"],e["to_asset"],e["pair"]))
    return idx


def prep_arrays(panel):
    dates = [utc(x) for x in panel["timestamp"]]
    opens = {a:panel[a+"_open"].astype(float).to_numpy() for a in U10}
    closes = {a:panel[a+"_close"].astype(float).to_numpy() for a in U10}
    return dates,opens,closes


def bounds(dates,start,end):
    lo = next(i for i,d in enumerate(dates) if d >= start)
    hi = max(i for i,d in enumerate(dates) if d <= end)
    return lo,hi


def choose(cands,allowed_to):
    eligible = [e for e in cands if e["to_asset"] in allowed_to]
    if not eligible:
        return None,0
    return eligible[0],len(eligible)


def run_route(dates,opens,closes,event_idx,assets,start,end,start_asset,allow_atom_incoming=True,collect=False):
    aset = set(assets)
    lo,hi = bounds(dates,start,end)
    current = start_asset
    qty = 1.0/opens[current][lo]
    pending = None
    equity = []
    transitions = 0
    conflicts = 0
    route = [current]

    for i in range(lo,hi+1):
        ts = dates[i]
        if pending is not None:
            value = qty*opens[current][i]
            current = pending
            qty = value*(1.0-COST)/opens[current][i]
            transitions += 1
            route.append(current)
            pending = None

        equity.append(qty*closes[current][i])

        if i == hi:
            continue
        allowed_to = set(aset)
        if not allow_atom_incoming and current != "ATOM":
            allowed_to.discard("ATOM")
        selected,n = choose(event_idx.get(ts,{}).get(current,[]),allowed_to)
        if selected is None:
            continue
        if n > 1:
            conflicts += 1
        pending = selected["to_asset"]

    arr = np.asarray(equity,dtype=float)
    peaks = np.maximum.accumulate(arr)
    out = {
        "return":float(arr[-1]/arr[0]-1.0),
        "max_dd":float(np.min(arr/peaks-1.0)),
        "transitions":transitions,
        "conflicts":conflicts,
    }
    if collect:
        out["route"] = route
    return out


def summarize(dates,opens,closes,event_idx,variant,start,end,starters):
    rows=[]
    for a in starters:
        if variant=="BASE_U10":
            r=run_route(dates,opens,closes,event_idx,U10,start,end,a,True)
        elif variant=="NO_ATOM_U9":
            r=run_route(dates,opens,closes,event_idx,U9,start,end,a,False)
        elif variant=="ATOM_EXIT_ONLY":
            r=run_route(dates,opens,closes,event_idx,U10,start,end,a,False)
        else:
            raise ValueError(variant)
        rows.append({"start_asset":a,**r})
    df=pd.DataFrame(rows)
    atom=df[df["start_asset"]=="ATOM"]
    return {
        "median_return":float(df["return"].median()),
        "worst_return":float(df["return"].min()),
        "best_return":float(df["return"].max()),
        "positive_starts":int((df["return"]>0).sum()),
        "median_max_dd":float(df["max_dd"].median()),
        "median_transitions":float(df["transitions"].median()),
        "atom_return":None if atom.empty else float(atom.iloc[0]["return"]),
        "atom_max_dd":None if atom.empty else float(atom.iloc[0]["max_dd"]),
    }


def migration_start_dates(dates):
    out=[]
    cursor=pd.Timestamp("2024-01-01",tz="UTC")
    end_cursor=pd.Timestamp("2026-03-01",tz="UTC")
    while cursor <= end_cursor:
        candidates=[d for d in dates if d>=cursor and d<=END]
        if candidates:
            out.append(candidates[0])
        cursor += pd.offsets.MonthBegin(1)
    return out


def outbound_atom_signals(dates,event_idx,start,end):
    lo,hi=bounds(dates,start,end)
    signals=[]
    for i in range(lo,hi+1):
        ts=dates[i]
        if i==hi:
            continue
        selected,n=choose(event_idx.get(ts,{}).get("ATOM",[]),set(U9))
        if selected is not None:
            signals.append({
                "signal_date":ts,
                "execution_index":i+1,
                "execution_date":dates[i+1],
                "to_asset":selected["to_asset"],
                "candidate_count":n,
            })
    return signals


def run_tranche_after_exit(dates,opens,closes,event_idx,start_index,initial_asset,initial_value,end):
    hi=max(i for i,d in enumerate(dates) if d<=end)
    current=initial_asset
    qty=initial_value/opens[current][start_index]
    pending=None
    value_series=[]

    for i in range(start_index,hi+1):
        ts=dates[i]
        if pending is not None:
            value=qty*opens[current][i]
            current=pending
            qty=value*(1.0-COST)/opens[current][i]
            pending=None
        value_series.append(qty*closes[current][i])
        if i==hi:
            continue
        selected,_=choose(event_idx.get(ts,{}).get(current,[]),set(U9))
        if selected is not None:
            pending=selected["to_asset"]
    return float(value_series[-1])


def staged_migration(dates,opens,closes,event_idx,start,end):
    lo,hi=bounds(dates,start,end)
    signals=outbound_atom_signals(dates,event_idx,start,end)
    tranche_records=[]
    final_values=[]
    checkpoints={}

    for tranche in range(4):
        initial_value=0.25
        if tranche < len(signals):
            s=signals[tranche]
            exec_i=s["execution_index"]
            atom_qty=initial_value/opens["ATOM"][lo]
            value_at_exec=atom_qty*opens["ATOM"][exec_i]
            target=s["to_asset"]
            target_qty=value_at_exec*(1.0-COST)/opens[target][exec_i]
            # continue inside U9 from target after execution day
            current=target
            qty=target_qty
            pending=None
            for i in range(exec_i,hi+1):
                ts=dates[i]
                if i>exec_i and pending is not None:
                    value=qty*opens[current][i]
                    current=pending
                    qty=value*(1.0-COST)/opens[current][i]
                    pending=None
                if i==hi:
                    break
                selected,_=choose(event_idx.get(ts,{}).get(current,[]),set(U9))
                if selected is not None:
                    pending=selected["to_asset"]
            final_value=qty*closes[current][hi]
            final_values.append(final_value)
            days=(s["execution_date"]-start).days
            checkpoints[str((tranche+1)*25)]=days
            tranche_records.append({
                "tranche":tranche+1,
                "signal_date":s["signal_date"].isoformat(),
                "execution_date":s["execution_date"].isoformat(),
                "destination":target,
                "days_from_start":days,
                "completed":True,
            })
        else:
            atom_qty=initial_value/opens["ATOM"][lo]
            final_value=atom_qty*closes["ATOM"][hi]
            final_values.append(final_value)
            tranche_records.append({
                "tranche":tranche+1,
                "signal_date":None,
                "execution_date":None,
                "destination":None,
                "days_from_start":None,
                "completed":False,
            })
    terminal=sum(final_values)
    return {
        "return":float(terminal-1.0),
        "completed_tranches":sum(1 for x in tranche_records if x["completed"]),
        "full_migration":all(x["completed"] for x in tranche_records),
        "days_to_25":checkpoints.get("25"),
        "days_to_50":checkpoints.get("50"),
        "days_to_75":checkpoints.get("75"),
        "days_to_100":checkpoints.get("100"),
        "tranches":tranche_records,
    }


def migration_comparison(dates,opens,closes,event_idx,start,end):
    base=run_route(dates,opens,closes,event_idx,U10,start,end,"ATOM",True,True)
    exit_only=run_route(dates,opens,closes,event_idx,U10,start,end,"ATOM",False,True)
    staged=staged_migration(dates,opens,closes,event_idx,start,end)
    return {
        "start":start.isoformat(),
        "base_u10_atom":base,
        "exit_only_atom":exit_only,
        "staged_4x25":staged,
    }


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cutoff",default="2026-09-27T00:00:00Z")
    args=ap.parse_args()

    panel,meta=download_panel(utc(args.cutoff))
    event_idx=build_event_index(panel)
    dates,opens,closes=prep_arrays(panel)

    run_dir=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True,exist_ok=True)

    windows={
        "mature":(MATURE_START,END),
        "latest_2y":(TWO_YEAR_START,END),
        "latest_1y":(YEAR_START,END),
    }
    fixed={}
    for name in ("BASE_U10","NO_ATOM_U9","ATOM_EXIT_ONLY"):
        fixed[name]={}
        for label,(s,e) in windows.items():
            starters=COMMON_STARTERS
            fixed[name][label]=summarize(dates,opens,closes,event_idx,name,s,e,starters)
        if name in ("BASE_U10","ATOM_EXIT_ONLY"):
            fixed[name]["atom_start"]={}
            for label,(s,e) in windows.items():
                fixed[name]["atom_start"][label]=summarize(
                    dates,opens,closes,event_idx,name,s,e,("ATOM",)
                )

    migrations=[]
    for start in migration_start_dates(dates):
        migrations.append(migration_comparison(dates,opens,closes,event_idx,start,END))
    pd.DataFrame([{
        "start":x["start"],
        "base_return":x["base_u10_atom"]["return"],
        "exit_only_return":x["exit_only_atom"]["return"],
        "staged_return":x["staged_4x25"]["return"],
        "completed_tranches":x["staged_4x25"]["completed_tranches"],
        "full_migration":x["staged_4x25"]["full_migration"],
        "days_to_25":x["staged_4x25"]["days_to_25"],
        "days_to_50":x["staged_4x25"]["days_to_50"],
        "days_to_75":x["staged_4x25"]["days_to_75"],
        "days_to_100":x["staged_4x25"]["days_to_100"],
    } for x in migrations]).to_csv(run_dir/"migration_starts_summary.csv",index=False)

    mdf=pd.DataFrame([{
        "base_return":x["base_u10_atom"]["return"],
        "exit_only_return":x["exit_only_atom"]["return"],
        "staged_return":x["staged_4x25"]["return"],
        "completed_tranches":x["staged_4x25"]["completed_tranches"],
        "full_migration":x["staged_4x25"]["full_migration"],
        "days_to_25":x["staged_4x25"]["days_to_25"],
        "days_to_50":x["staged_4x25"]["days_to_50"],
        "days_to_75":x["staged_4x25"]["days_to_75"],
        "days_to_100":x["staged_4x25"]["days_to_100"],
    } for x in migrations])

    summary={
        "start_count":len(mdf),
        "full_migration_rate":float(mdf["full_migration"].mean()),
        "median_days_to_25":None if mdf["days_to_25"].dropna().empty else float(mdf["days_to_25"].dropna().median()),
        "median_days_to_50":None if mdf["days_to_50"].dropna().empty else float(mdf["days_to_50"].dropna().median()),
        "median_days_to_75":None if mdf["days_to_75"].dropna().empty else float(mdf["days_to_75"].dropna().median()),
        "median_days_to_100":None if mdf["days_to_100"].dropna().empty else float(mdf["days_to_100"].dropna().median()),
        "median_base_return":float(mdf["base_return"].median()),
        "median_exit_only_return":float(mdf["exit_only_return"].median()),
        "median_staged_return":float(mdf["staged_return"].median()),
        "staged_beats_base_rate":float((mdf["staged_return"]>mdf["base_return"]).mean()),
        "exit_only_beats_base_rate":float((mdf["exit_only_return"]>mdf["base_return"]).mean()),
        "staged_beats_exit_only_rate":float((mdf["staged_return"]>mdf["exit_only_return"]).mean()),
    }

    result={
        "generated_at":pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit":source_sha(),
        "data":meta,
        "fixed_window_comparison":fixed,
        "migration_summary":summary,
        "migration_runs":migrations,
    }
    (run_dir/"results.json").write_text(
        json.dumps(result,indent=2,sort_keys=True,allow_nan=False),
        encoding="utf-8"
    )

    lines=[
        "# ATOM Sunset Migration v1","",
        "Mode: STRESS_TEST_ONLY",
        "Live unchanged.","",
        "## Fixed windows","",
        "| Variant | Mature median | Latest 2Y | Latest 1Y |",
        "|---|---:|---:|---:|",
    ]
    for name in ("BASE_U10","NO_ATOM_U9","ATOM_EXIT_ONLY"):
        x=fixed[name]
        lines.append(
            f"| {name} | {100*x['mature']['median_return']:+.2f}% | "
            f"{100*x['latest_2y']['median_return']:+.2f}% | "
            f"{100*x['latest_1y']['median_return']:+.2f}% |"
        )
    lines += [
        "",
        "## 4x25 migration",
        "",
        f"Monthly migration starts: {summary['start_count']}",
        f"Full migration rate: {100*summary['full_migration_rate']:.1f}%",
        f"Median days to 100%: {summary['median_days_to_100']}",
        f"Median staged return to 2026-09-26: {100*summary['median_staged_return']:+.2f}%",
        f"Median BASE_U10 ATOM return: {100*summary['median_base_return']:+.2f}%",
        f"Median EXIT_ONLY return: {100*summary['median_exit_only_return']:+.2f}%",
    ]
    (run_dir/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")

    print("run_dir="+str(run_dir))
    for name in ("BASE_U10","NO_ATOM_U9","ATOM_EXIT_ONLY"):
        print(name+"_mature=%.6f" % fixed[name]["mature"]["median_return"])
        print(name+"_2y=%.6f" % fixed[name]["latest_2y"]["median_return"])
        print(name+"_1y=%.6f" % fixed[name]["latest_1y"]["median_return"])
        if "atom_start" in fixed[name]:
            print(name+"_atom_1y=%.6f" % fixed[name]["atom_start"]["latest_1y"]["median_return"])
    print("migration_start_count="+str(summary["start_count"]))
    print("full_migration_rate=%.6f" % summary["full_migration_rate"])
    print("median_days_to_25="+str(summary["median_days_to_25"]))
    print("median_days_to_50="+str(summary["median_days_to_50"]))
    print("median_days_to_75="+str(summary["median_days_to_75"]))
    print("median_days_to_100="+str(summary["median_days_to_100"]))
    print("median_base_return=%.6f" % summary["median_base_return"])
    print("median_exit_only_return=%.6f" % summary["median_exit_only_return"])
    print("median_staged_return=%.6f" % summary["median_staged_return"])
    print("staged_beats_base_rate=%.6f" % summary["staged_beats_base_rate"])
    print("exit_only_beats_base_rate=%.6f" % summary["exit_only_beats_base_rate"])
    print("staged_beats_exit_only_rate=%.6f" % summary["staged_beats_exit_only_rate"])
    return 0


if __name__=="__main__":
    raise SystemExit(main())
