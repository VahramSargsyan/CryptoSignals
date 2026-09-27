from __future__ import annotations

import argparse
import itertools
import json
import os
import subprocess
from collections import defaultdict
from pathlib import Path

import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import build_pair_monitor

from scripts.research_u10_monthly_surge_pullback_v1 import (
    INITIAL_USDT,
    monthly_state,
    event_study,
    run_overlay,
    run_reference,
    universe_key,
)
from scripts.research_u10_monthly_surge_trailing_reentry_v1 import (
    run_overlay_trailing_reentry,
)

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "u10_old_history_2020_2022_v1"

SUPERSET = (
    "ATOM","BTC","ETH","BNB","SOL","XRP","TRX","DOGE",
    "ADA","LINK","XLM","LTC","HBAR","AVAX","BCH","UNI",
)

DATA_START = pd.Timestamp("2019-01-01", tz="UTC")
ELIGIBILITY_CUTOFF = pd.Timestamp("2020-01-01", tz="UTC")
EVAL_END = pd.Timestamp("2022-12-31", tz="UTC")
DOWNLOAD_END = pd.Timestamp("2023-01-01", tz="UTC")

LOOKBACK = 180
ARM = 0.15
REVERSAL = 0.03
COST = 0.001
CASH_FRACTION = 0.30

VARIANTS = {
    "P1_ORIGINAL": (1.00, 0.05, 0.25, "original"),
    "P1_TRAILING": (1.00, 0.05, 0.25, "trailing"),
    "P2_ORIGINAL": (1.00, 0.10, 0.25, "original"),
    "P2_TRAILING": (1.00, 0.10, 0.25, "trailing"),
}


def utc(x):
    t=pd.Timestamp(x)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def source_sha():
    v=os.environ.get("SOURCE_COMMIT_SHA")
    if v:
        return v
    return subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip()


def download_assets():
    client=BinanceSpotRestClient()
    raw={}
    meta={}
    for asset in SUPERSET:
        result=download_historical_dataset(
            client,
            symbol=asset+"USDT",
            start=DATA_START,
            end=DOWNLOAD_END,
            timeframe="1D",
            as_of=DOWNLOAD_END,
        )
        if result.dataset is None:
            meta[asset]={"status":str(result.metadata.status),"eligible":False}
            continue
        if result.dataset.quality.has_critical_issues:
            meta[asset]={"status":"CRITICAL_DATA_QUALITY","eligible":False}
            continue

        f=result.dataset.candles[["timestamp","open","close"]].copy()
        f["timestamp"]=pd.to_datetime(f["timestamp"],utc=True)
        f=f.sort_values("timestamp",kind="stable").reset_index(drop=True)
        first=utc(f.iloc[0]["timestamp"])
        last=utc(f.iloc[-1]["timestamp"])
        eligible=(first <= ELIGIBILITY_CUTOFF and last >= EVAL_END)

        raw[asset]=f
        meta[asset]={
            "status":"OK",
            "rows":len(f),
            "first":first.isoformat(),
            "last":last.isoformat(),
            "eligible":bool(eligible),
            "listing_truncated":bool(result.metadata.listing_truncated),
        }
    return raw,meta


def build_common_panel(raw,eligible):
    panel=None
    for asset in eligible:
        f=raw[asset].rename(columns={"open":asset+"_open","close":asset+"_close"})
        panel=f if panel is None else panel.merge(f,on="timestamp",how="inner",validate="one_to_one")
    panel=panel[panel["timestamp"]<=EVAL_END].sort_values("timestamp",kind="stable").reset_index(drop=True)
    if len(panel)<LOOKBACK:
        raise RuntimeError(f"common panel only {len(panel)} rows")
    return panel


def build_events(panel,assets):
    cols=["timestamp"]+[a+"_close" for a in assets]
    events,_=build_pair_monitor(
        panel[cols].copy(),
        assets=assets,
        lookback=LOOKBACK,
        arm_threshold=ARM,
        reversal=REVERSAL,
    )
    by_date=defaultdict(list)
    for e in events:
        if e["event"]=="CONFIRMED":
            by_date[utc(e["date"])].append(e)
    return by_date


def all_old_u10(eligible):
    if "ATOM" not in eligible:
        raise RuntimeError("ATOM is not old-history eligible")
    others=[a for a in eligible if a!="ATOM"]
    if len(others)<9:
        raise RuntimeError(f"need at least 9 non-ATOM eligible assets; have {len(others)}")
    return [tuple(["ATOM"]+list(c)) for c in itertools.combinations(others,9)]


def summarize_variant(df):
    return {
        "universe_count":int(len(df)),
        "final_gt_baseline_rate":float((df["delta_vs_baseline_usdt"]>0).mean()),
        "dd_better_rate":float((df["dd_improvement_pp"]>0).mean()),
        "both_rate":float(((df["delta_vs_baseline_usdt"]>0)&(df["dd_improvement_pp"]>0)).mean()),
        "median_delta_pct":float(df["delta_vs_baseline_pct"].median()),
        "q25_delta_pct":float(df["delta_vs_baseline_pct"].quantile(.25)),
        "q75_delta_pct":float(df["delta_vs_baseline_pct"].quantile(.75)),
        "median_dd_improvement_pp":float(df["dd_improvement_pp"].median()),
        "median_cashouts":float(df["cashouts"].median()),
        "median_reentries":float(df["reentries"].median()),
        "unfinished_rate":float((df["unfinished_cycles"]>0).mean()),
        "median_days_in_cash":float(df["days_in_cash"].median()),
    }


def pct(x): return f"{100*x:+.2f}%"


def write_report(payload,run_dir):
    lines=[
        "# U10-like Old-History Validation 2020-2022 v1","",
        "Mode: STRESS_TEST_ONLY",
        "Rules frozen before old data was opened.","",
        f"Eligible old survivor assets ({len(payload['eligible_assets'])}): "+", ".join(payload["eligible_assets"]),
        f"Excluded by listing/data cutoff: "+", ".join(payload["excluded_assets"]),
        f"Old U10-like universe count: {payload['universe_count']}",
        f"Shared mature start: {pd.Timestamp(payload['mature_start']).date()}",
        f"Evaluation end: {EVAL_END.date()}","",
        "## Direct +100% surge event study","",
        f"Total +100% events: {payload['event_summary']['event_count']}",
        f"Universes with at least one event: {payload['event_summary']['universes_with_event']}/{payload['universe_count']}",
        f"-25% running pullback within 31d: {100*payload['event_summary']['hit31']:.2f}%",
        f"-25% running pullback within 62d: {100*payload['event_summary']['hit62']:.2f}%",
        f"Median worst 31d running DD: {pct(payload['event_summary']['median_dd31'])}",
        f"Median worst 62d running DD: {pct(payload['event_summary']['median_dd62'])}","",
        "## Frozen overlay validation","",
        "| Variant | Final > baseline | DD better | Both better | Median terminal delta | Q25 / Q75 | Median DD improvement | Unfinished |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name in ("P1_ORIGINAL","P1_TRAILING","P2_ORIGINAL","P2_TRAILING"):
        s=payload["variant_summary"][name]
        lines.append(
            f"| {name} | {100*s['final_gt_baseline_rate']:.2f}% | {100*s['dd_better_rate']:.2f}% | "
            f"{100*s['both_rate']:.2f}% | {pct(s['median_delta_pct'])} | "
            f"{pct(s['q25_delta_pct'])} / {pct(s['q75_delta_pct'])} | "
            f"{s['median_dd_improvement_pp']:+.2f} pp | {100*s['unfinished_rate']:.2f}% |"
        )
    lines += ["","## Guardrail","",
        "- No parameter was changed using 2020-2022 outcomes.",
        "- Old universes differ from current U10 because TWT/PEPE and other later assets were unavailable.",
        "- This is temporal validation of the overlay mechanism on old survivor topologies, not a reconstruction of canonical U10.",
        "","TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST"]
    (run_dir/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--end",default="2023-01-01T00:00:00Z")
    args=ap.parse_args()

    raw,meta=download_assets()
    eligible=tuple(a for a in SUPERSET if meta.get(a,{}).get("eligible"))
    excluded=tuple(a for a in SUPERSET if a not in eligible)
    panel=build_common_panel(raw,eligible)
    mature_start=utc(panel.iloc[LOOKBACK-1]["timestamp"])
    events=build_events(panel,eligible)
    universes=all_old_u10(eligible)

    baseline_rows=[]
    overlay_rows=[]
    event_rows=[]

    for idx,assets in enumerate(universes,1):
        key=universe_key(assets)
        baseline,ref=run_reference(panel,events,assets,mature_start,INITIAL_USDT)
        monthly,anchors=monthly_state(ref,INITIAL_USDT)
        ev=event_study(ref,monthly,surge_threshold=1.0)
        if not ev.empty:
            ev=ev.copy(); ev.insert(0,"universe_key",key); event_rows.append(ev)

        baseline_rows.append({
            "universe_key":key,
            "assets":"|".join(assets),
            "final_equity_usdt":baseline["final_equity_usdt"],
            "total_return":baseline["total_return"],
            "max_drawdown":baseline["max_drawdown"],
            "transitions":baseline["transitions"],
        })

        for name,(surge,pullback,reentry,kind) in VARIANTS.items():
            if kind=="original":
                out,_=run_overlay(
                    ref,anchors,
                    surge_threshold=surge,
                    pullback_threshold=pullback,
                    reentry_threshold=reentry,
                    cash_fraction=CASH_FRACTION,
                    initial_usdt=INITIAL_USDT,
                )
                reentries=out["reentries"]
            else:
                out,_=run_overlay_trailing_reentry(
                    ref,anchors,
                    surge_threshold=surge,
                    pullback_threshold=pullback,
                    reentry_threshold=reentry,
                    cash_fraction=CASH_FRACTION,
                    initial_usdt=INITIAL_USDT,
                )
                reentries=out["reentries"]

            overlay_rows.append({
                "universe_key":key,
                "variant":name,
                "baseline_final_equity_usdt":baseline["final_equity_usdt"],
                "baseline_max_drawdown":baseline["max_drawdown"],
                "final_equity_usdt":out["final_equity_usdt"],
                "total_return":out["total_return"],
                "max_drawdown":out["max_drawdown"],
                "delta_vs_baseline_usdt":out["final_equity_usdt"]-baseline["final_equity_usdt"],
                "delta_vs_baseline_pct":out["final_equity_usdt"]/baseline["final_equity_usdt"]-1,
                "dd_improvement_pp":(out["max_drawdown"]-baseline["max_drawdown"])*100,
                "cashouts":out["cashouts"],
                "reentries":reentries,
                "unfinished_cycles":int(out.get("unfinished_cycles", 1 if out.get("terminal_cash_usdt", 0.0) > 0 else 0)),
                "days_in_cash":out["days_in_cash"],
                "terminal_cash_usdt":out["terminal_cash_usdt"],
            })
        if idx%50==0:
            print(f"processed_universes={idx}")

    baseline_df=pd.DataFrame(baseline_rows)
    overlay_df=pd.DataFrame(overlay_rows)
    events_df=pd.concat(event_rows,ignore_index=True) if event_rows else pd.DataFrame()

    variant_summary={
        name:summarize_variant(overlay_df[overlay_df.variant==name])
        for name in VARIANTS
    }

    if len(events_df):
        event_summary={
            "event_count":int(len(events_df)),
            "universes_with_event":int(events_df.universe_key.nunique()),
            "hit31":float(events_df.hit_minus25_31d.mean()),
            "hit62":float(events_df.hit_minus25_62d.mean()),
            "median_dd31":float(events_df.worst_running_dd_31d.median()),
            "median_dd62":float(events_df.worst_running_dd_62d.median()),
        }
        month_summary=events_df.groupby("surge_month").agg(
            events=("universe_key","size"),
            hit31=("hit_minus25_31d","mean"),
            hit62=("hit_minus25_62d","mean"),
            median_dd31=("worst_running_dd_31d","median"),
            median_dd62=("worst_running_dd_62d","median"),
        ).reset_index()
    else:
        event_summary={"event_count":0,"universes_with_event":0,"hit31":None,"hit62":None,"median_dd31":None,"median_dd62":None}
        month_summary=pd.DataFrame()

    run_dir=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True,exist_ok=True)
    baseline_df.to_csv(run_dir/"old_u10_baselines.csv",index=False)
    overlay_df.to_csv(run_dir/"old_u10_overlay_results.csv",index=False)
    if len(events_df): events_df.to_csv(run_dir/"old_surge_events.csv",index=False)
    if len(month_summary): month_summary.to_csv(run_dir/"old_event_month_summary.csv",index=False)

    payload={
        "generated_at":pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit":source_sha(),
        "asset_metadata":meta,
        "eligible_assets":list(eligible),
        "excluded_assets":list(excluded),
        "universe_count":len(universes),
        "common_start":utc(panel.iloc[0]["timestamp"]).isoformat(),
        "mature_start":mature_start.isoformat(),
        "evaluation_end":EVAL_END.isoformat(),
        "event_summary":event_summary,
        "variant_summary":variant_summary,
    }
    (run_dir/"results.json").write_text(json.dumps(payload,indent=2,sort_keys=True,allow_nan=False),encoding="utf-8")
    write_report(payload,run_dir)

    print("run_dir="+str(run_dir))
    print("eligible="+",".join(eligible))
    print("excluded="+",".join(excluded))
    print("universe_count="+str(len(universes)))
    print("mature_start="+mature_start.isoformat())
    print("event_count="+str(event_summary["event_count"]))
    if event_summary["event_count"]:
        print("hit31=%.6f hit62=%.6f med31=%.6f med62=%.6f" % (
            event_summary["hit31"],event_summary["hit62"],event_summary["median_dd31"],event_summary["median_dd62"]))
    for name in VARIANTS:
        s=variant_summary[name]
        print("%s final=%.6f dd=%.6f both=%.6f meddelta=%.6f unfinished=%.6f" % (
            name,s["final_gt_baseline_rate"],s["dd_better_rate"],s["both_rate"],s["median_delta_pct"],s["unfinished_rate"]))
    return 0

if __name__=="__main__":
    raise SystemExit(main())
