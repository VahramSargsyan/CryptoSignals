from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path

import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import build_pair_monitor
from scripts.research_u10_last_year_vs_u8_universes_v1 import ARM, COST, LOOKBACK, REVERSAL, utc
from scripts.research_u10_monthly_surge_pullback_v1 import INITIAL_USDT, monthly_state, run_reference, source_sha
from scripts.research_u10_monthly_surge_fractal_buystop_v1 import P1, P2, run_fractal_overlay
from scripts.research_u10_surge_old2020_2022_holdout_v1 import OLD10, DOWNLOAD_START, EVAL_START, EVAL_END

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "u10_fractal_old2020_2022_secondary_v1"


def download_old_ohlc(cutoff):
    client=BinanceSpotRestClient()
    panel=None
    meta={}
    for asset in OLD10:
        result=download_historical_dataset(
            client,
            symbol=asset+"USDT",
            start=DOWNLOAD_START,
            end=cutoff,
            timeframe="1D",
            as_of=cutoff,
        )
        if result.dataset is None:
            raise RuntimeError(f"{asset}: {result.metadata.status}")
        if result.dataset.quality.has_critical_issues:
            raise RuntimeError(f"{asset}: critical data quality issues")
        f=result.dataset.candles[["timestamp","open","high","low","close"]].copy()
        f["timestamp"]=pd.to_datetime(f["timestamp"],utc=True)
        f=f.rename(columns={
            "open":asset+"_open",
            "high":asset+"_high",
            "low":asset+"_low",
            "close":asset+"_close",
        })
        meta[asset]={
            "rows":int(len(f)),
            "start":utc(f.iloc[0]["timestamp"]).isoformat(),
            "end":utc(f.iloc[-1]["timestamp"]).isoformat(),
        }
        panel=f if panel is None else panel.merge(f,on="timestamp",how="inner",validate="one_to_one")
    panel=panel.sort_values("timestamp",kind="stable").reset_index(drop=True)
    pre=int((panel["timestamp"]<EVAL_START).sum())
    if pre<LOOKBACK:
        raise RuntimeError(f"only {pre} common prehistory rows")
    panel=panel[panel["timestamp"]<=EVAL_END].copy().reset_index(drop=True)
    return panel,meta,pre


def build_events(panel):
    cols=["timestamp"]+[a+"_close" for a in OLD10]
    events,_=build_pair_monitor(
        panel[cols].copy(),
        assets=OLD10,
        lookback=LOOKBACK,
        arm_threshold=ARM,
        reversal=REVERSAL,
    )
    out=defaultdict(list)
    for e in events:
        if e["event"]=="CONFIRMED":
            out[utc(e["date"])].append(e)
    return out


def pct(x):
    return f"{100*x:+.2f}%"


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cutoff",default="2026-09-27T00:00:00Z")
    args=ap.parse_args()

    panel,meta,pre=download_old_ohlc(utc(args.cutoff))
    events=build_events(panel)
    baseline,reference=run_reference(panel,events,OLD10,EVAL_START,INITIAL_USDT)
    reference=reference.reset_index(drop=True)
    panel_eval=panel[panel["timestamp"]>=EVAL_START].copy().reset_index(drop=True)
    if len(panel_eval)!=len(reference):
        raise RuntimeError("old panel/reference mismatch")
    _,anchors=monthly_state(reference,INITIAL_USDT)

    variants={}
    cycles_all=[]
    for label,cfg in (("P1",P1),("P2",P2)):
        fractal,cycles=run_fractal_overlay(
            panel_eval,
            reference,
            anchors,
            surge_threshold=cfg["surge"],
            cashout_pullback=cfg["pullback"],
            initial_usdt=INITIAL_USDT,
        )
        fractal["delta_vs_baseline_usdt"]=fractal["final_equity_usdt"]-baseline["final_equity_usdt"]
        fractal["delta_vs_baseline_pct"]=fractal["final_equity_usdt"]/baseline["final_equity_usdt"]-1
        fractal["max_dd_improvement_pp"]=(fractal["max_drawdown"]-baseline["max_drawdown"])*100
        variants[label]=fractal
        if not cycles.empty:
            cc=cycles.copy()
            cc.insert(0,"variant",label)
            cycles_all.append(cc)

    run_dir=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True,exist_ok=True)
    if cycles_all:
        pd.concat(cycles_all,ignore_index=True).to_csv(run_dir/"cycles.csv",index=False)

    payload={
        "generated_at":pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit":source_sha(),
        "history_status":"REUSED_HISTORY_SECONDARY_EVIDENCE",
        "old10":list(OLD10),
        "prehistory_rows":pre,
        "baseline":baseline,
        "variants":variants,
        "data_metadata":meta,
    }
    (run_dir/"results.json").write_text(json.dumps(payload,indent=2,sort_keys=True,allow_nan=False),encoding="utf-8")

    lines=[
        "# OLD10 2020-2022 Fractal Buy-Stop Secondary Evidence",
        "",
        "History status: REUSED_HISTORY_SECONDARY_EVIDENCE",
        "This is NOT untouched OOS validation.",
        "",
        f"Baseline final: {baseline['final_equity_usdt']:,.2f} USDT",
        f"Baseline return: {pct(baseline['total_return'])}",
        f"Baseline max DD: {pct(baseline['max_drawdown'])}",
        "",
        "| Variant | Final | Delta vs baseline | Max DD | DD change | Reentries | Unfinished | Cash days |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for label in ("P1","P2"):
        v=variants[label]
        lines.append(
            f"| {label}-FRACTAL | {v['final_equity_usdt']:,.2f} | "
            f"{pct(v['delta_vs_baseline_pct'])} | {pct(v['max_drawdown'])} | "
            f"{v['max_dd_improvement_pp']:+.2f} pp | {v['reentries']} | "
            f"{v['unfinished_cycles']} | {v['days_in_cash']} |"
        )
    lines += [
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_REUSED_HISTORY_SECONDARY",
    ]
    (run_dir/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")

    print("run_dir="+str(run_dir))
    print("baseline final=%.8f dd=%.8f"%(baseline["final_equity_usdt"],baseline["max_drawdown"]))
    for label in ("P1","P2"):
        v=variants[label]
        print("%s final=%.8f delta=%.8f dd=%.8f ddpp=%.8f re=%d unfinished=%d cash=%d"%(
            label,v["final_equity_usdt"],v["delta_vs_baseline_pct"],v["max_drawdown"],v["max_dd_improvement_pp"],v["reentries"],v["unfinished_cycles"],v["days_in_cash"]
        ))
    return 0

if __name__=="__main__":
    raise SystemExit(main())
