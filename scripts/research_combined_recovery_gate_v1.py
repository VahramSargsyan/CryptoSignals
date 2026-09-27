from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd

import scripts.research_btc_sma_reentry_development_v1 as dev
import scripts.research_btc_sma_legacy7_holdout_v1 as leg
from scripts.research_official_m2_vs_crypto_stress_v1 import RELEASE_DATES as RELEASE_DATES_2022_2026

REPO_ROOT = Path(__file__).resolve().parents[1]
M2_PATH = REPO_ROOT / "research" / "reference_data" / "official_h6_m2_monthly_2020_2026.csv"
M2_SHA256 = "5d0448c4e6fb376a4dd47ff9e8da9e96b7ed0089c7defa549e54ef358234faf7"
BTC_CANDIDATES = ((25, 100), (30, 100), (12, 100))
ROBUSTNESS_DAYS = (180, 120)

# First monthly H.6 release was 2021-02-23; thereafter fourth Tuesday.
RELEASE_DATES_2021 = tuple(pd.Timestamp(x) for x in (
    "2021-02-23","2021-03-23","2021-04-27","2021-05-25","2021-06-22",
    "2021-07-27","2021-08-24","2021-09-28","2021-10-26","2021-11-23","2021-12-28",
))
ALL_RELEASE_DATES = RELEASE_DATES_2021 + tuple(RELEASE_DATES_2022_2026)

CRITICAL_2022_START = pd.Timestamp("2022-02-10", tz="UTC")
CRITICAL_2022_END = pd.Timestamp("2022-08-08", tz="UTC")


def _source_commit() -> str:
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(["git","rev-parse","HEAD"], cwd=REPO_ROOT, text=True).strip()


def _json_safe(value):
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, np.generic):
        return _json_safe(value.item())
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_safe(payload), indent=2, sort_keys=True, allow_nan=False)+"\n", encoding="utf-8")


def load_m2() -> pd.DataFrame:
    payload = M2_PATH.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    if digest != M2_SHA256:
        raise RuntimeError(f"M2 snapshot SHA mismatch: expected={M2_SHA256} actual={digest}")
    x = pd.read_csv(M2_PATH)
    x["period_date"] = pd.to_datetime(x["period_date"], errors="raise")
    x["observation_month_end"] = x["period_date"].dt.normalize()
    x = x.sort_values("observation_month_end").reset_index(drop=True)
    for months in (3, 6, 12):
        x[f"m2_change_{months}m"] = x["m2_value"] / x["m2_value"].shift(months) - 1.0

    rows=[]
    for release in ALL_RELEASE_DATES:
        rows.append({
            "observation_month_end": (release - pd.offsets.MonthEnd(1)).normalize(),
            "release_date": release.normalize(),
            "available_date": release.normalize() + pd.Timedelta(days=1),
        })
    releases=pd.DataFrame(rows)
    x=x.merge(releases,on="observation_month_end",how="left",validate="one_to_one")
    return x


def add_causal_m2(panel: pd.DataFrame) -> pd.DataFrame:
    m2=load_m2()
    right=m2.dropna(subset=["available_date"])[[
        "observation_month_end","release_date","available_date","m2_value",
        "m2_change_3m","m2_change_6m","m2_change_12m"
    ]].sort_values("available_date")

    out=panel.copy()
    out["crypto_date"]=pd.to_datetime(out["timestamp"],utc=True).dt.tz_localize(None).dt.normalize()
    out=pd.merge_asof(
        out.sort_values("crypto_date"),
        right,
        left_on="crypto_date",
        right_on="available_date",
        direction="backward",
    )
    out["m2_expansion_all"]=(
        (out["m2_change_3m"]>0)
        & (out["m2_change_6m"]>0)
        & (out["m2_change_12m"]>0)
    )
    return out.sort_values("timestamp").reset_index(drop=True)


def build_gate_map(
    panel: pd.DataFrame,
    btc_cross_by_date: dict[pd.Timestamp,bool],
) -> tuple[dict[pd.Timestamp,bool],dict]:
    gate={}
    cross_days=blocked_m2=blocked_breadth=passed=0
    for _,row in panel.iterrows():
        ts=pd.Timestamp(row["timestamp"])
        cross=bool(btc_cross_by_date.get(ts,False))
        if not cross:
            gate[ts]=False
            continue
        cross_days+=1
        m2_ok=bool(row["m2_expansion_all"]) if pd.notna(row["m2_expansion_all"]) else False
        breadth_ok=int(row["breadth_sma200"])>=4
        if not m2_ok:
            blocked_m2+=1
            gate[ts]=False
        elif not breadth_ok:
            blocked_breadth+=1
            gate[ts]=False
        else:
            passed+=1
            gate[ts]=True
    return gate,{
        "btc_cross_days":cross_days,
        "blocked_by_m2":blocked_m2,
        "blocked_by_breadth_after_m2_pass":blocked_breadth,
        "passed_all_gates":passed,
    }


def current_cross_map(panel: pd.DataFrame, fast:int, slow:int) -> dict[pd.Timestamp,bool]:
    s=dev.btc_cross_series(panel,fast,slow)
    return {pd.Timestamp(ts):bool(flag) for ts,flag in zip(panel["timestamp"],s,strict=True)}


def summarize_current_period(panel,signals,cross_map,start,end):
    return dev.summarize(dev.evaluate_pair(panel,signals,cross_map,start=start,end=end))


def current_low(panel,signals,start,end):
    return dev.summarize_ablation(
        dev.evaluate_frozen_destination(panel,signals,start=start,end=end,with_daily=False)
    )["LOW_VOL_CRYPTO"]


def current_robustness(panel,signals,anchor,end,fast,slow,standalone_map,combined_map):
    rows=[]
    for days in ROBUSTNESS_DAYS:
        for idx,(start,stop) in enumerate(dev.complete_windows(anchor,end,days),start=1):
            low=current_low(panel,signals,start,stop)
            standalone=summarize_current_period(panel,signals,standalone_map,start,stop)
            combined=summarize_current_period(panel,signals,combined_map,start,stop)
            rows.append({
                "window_days":days,"window_index":idx,"start":start.isoformat(),"end":stop.isoformat(),
                "fast_sma":fast,"slow_sma":slow,
                "low_return":low["median_return"],"low_dd":low["median_max_drawdown"],
                "standalone_return":standalone["median_return"],"standalone_dd":standalone["median_max_drawdown"],
                "combined_return":combined["median_return"],"combined_dd":combined["median_max_drawdown"],
                "combined_beats_low_return":combined["median_return"]>low["median_return"],
                "combined_beats_low_dd":combined["median_max_drawdown"]>low["median_max_drawdown"],
                "combined_beats_low_both":(
                    combined["median_return"]>low["median_return"]
                    and combined["median_max_drawdown"]>low["median_max_drawdown"]
                ),
                "combined_beats_standalone_return":combined["median_return"]>standalone["median_return"],
                "combined_beats_standalone_dd":combined["median_max_drawdown"]>standalone["median_max_drawdown"],
            })
    return pd.DataFrame(rows)


def legacy_summary(panel,signals,start,end,cross_map,fast,slow):
    return leg.summarize(leg.evaluate_variant(
        "BTC",panel,signals,start=start,end=end,cross_map=cross_map,fast=fast,slow=slow
    ))


def legacy_low(panel,signals,start,end):
    return leg.summarize(leg.evaluate_variant("LOW_VOL",panel,signals,start=start,end=end))


def legacy_robustness(panel,signals,anchor,end,fast,slow,standalone_map,combined_map):
    rows=[]
    for days in ROBUSTNESS_DAYS:
        for idx,(start,stop) in enumerate(leg.complete_windows(anchor,end,days),start=1):
            low=legacy_low(panel,signals,start,stop)
            standalone=legacy_summary(panel,signals,start,stop,standalone_map,fast,slow)
            combined=legacy_summary(panel,signals,start,stop,combined_map,fast,slow)
            rows.append({
                "window_days":days,"window_index":idx,"start":start.isoformat(),"end":stop.isoformat(),
                "fast_sma":fast,"slow_sma":slow,
                "low_return":low["median_return"],"low_dd":low["median_max_drawdown"],
                "standalone_return":standalone["median_return"],"standalone_dd":standalone["median_max_drawdown"],
                "combined_return":combined["median_return"],"combined_dd":combined["median_max_drawdown"],
                "combined_beats_low_return":combined["median_return"]>low["median_return"],
                "combined_beats_low_dd":combined["median_max_drawdown"]>low["median_max_drawdown"],
                "combined_beats_low_both":(
                    combined["median_return"]>low["median_return"]
                    and combined["median_max_drawdown"]>low["median_max_drawdown"]
                ),
            })
    return pd.DataFrame(rows)


def robustness_summary(df:pd.DataFrame)->dict:
    out={}
    for days,g in df.groupby("window_days"):
        out[f"{int(days)}d"]={
            "windows":int(len(g)),
            "return_wins_vs_low":int(g["combined_beats_low_return"].sum()),
            "dd_wins_vs_low":int(g["combined_beats_low_dd"].sum()),
            "both_wins_vs_low":int(g["combined_beats_low_both"].sum()),
        }
    out["all"]={
        "windows":int(len(df)),
        "return_wins_vs_low":int(df["combined_beats_low_return"].sum()),
        "dd_wins_vs_low":int(df["combined_beats_low_dd"].sum()),
        "both_wins_vs_low":int(df["combined_beats_low_both"].sum()),
    }
    return out


def build_parser():
    p=argparse.ArgumentParser(description="Combined M2 + BTC + breadth recovery gate V1.")
    p.add_argument("--output-root",type=Path,default=REPO_ROOT/"research_artifacts"/"combined_recovery_gate_v1")
    return p


def main(argv=None)->int:
    args=build_parser().parse_args(argv)

    # Current 8-asset history.
    panel,crypto_meta=dev.download_panel(dev.DEV_START,dev.DEV_END)
    panel=dev.add_defensive_features(panel)
    btc,btc_meta=dev.download_btc_close(dev.DEV_START,dev.DEV_END)
    panel=dev.add_btc_to_panel(panel,btc)
    panel=add_causal_m2(panel)
    signals,_=dev.build_pair_context(panel)
    full_start=dev.first_fully_eligible_date(panel)

    current_results={}
    current_windows=[]

    for fast,slow in BTC_CANDIDATES:
        standalone_map=current_cross_map(panel,fast,slow)
        combined_map,diag=build_gate_map(panel,standalone_map)

        periods={
            "reproduction":(dev.REPRO_START,dev.REPRO_END),
            "opened_2026":(dev.UNTOUCHED_START,dev.UNTOUCHED_END),
            "full":(full_start,dev.UNTOUCHED_END),
        }
        pdata={}
        for name,(start,end) in periods.items():
            pdata[name]={
                "low_vol":current_low(panel,signals,start,end),
                "standalone_btc":summarize_current_period(panel,signals,standalone_map,start,end),
                "combined":summarize_current_period(panel,signals,combined_map,start,end),
            }

        rw=current_robustness(
            panel,signals,full_start,dev.UNTOUCHED_END,fast,slow,standalone_map,combined_map
        )
        current_windows.append(rw)
        current_results[f"{fast}_{slow}"]={
            "gate_diagnostics_full_panel":diag,
            "periods":pdata,
            "robustness":robustness_summary(rw),
        }

    current_windows_df=pd.concat(current_windows,ignore_index=True)

    # Retrospective legacy-7 robustness.
    lpanel,_,legacy_meta=leg.download_legacy_panel()
    lpanel=leg.add_features(lpanel)
    lpanel=add_causal_m2(lpanel)
    lsignals=leg.build_pair_signals(lpanel)
    lanchor=leg.eligible_anchor(lpanel)

    legacy_results={}
    legacy_windows=[]
    critical={}

    for fast,slow in BTC_CANDIDATES:
        standalone_map=leg.btc_cross_map(lpanel,fast,slow)
        combined_map,diag=build_gate_map(lpanel,standalone_map)

        low_full=legacy_low(lpanel,lsignals,lanchor,leg.HOLDOUT_END)
        standalone_full=legacy_summary(lpanel,lsignals,lanchor,leg.HOLDOUT_END,standalone_map,fast,slow)
        combined_full=legacy_summary(lpanel,lsignals,lanchor,leg.HOLDOUT_END,combined_map,fast,slow)

        low_critical=legacy_low(lpanel,lsignals,CRITICAL_2022_START,CRITICAL_2022_END)
        standalone_critical=legacy_summary(
            lpanel,lsignals,CRITICAL_2022_START,CRITICAL_2022_END,standalone_map,fast,slow
        )
        combined_critical=legacy_summary(
            lpanel,lsignals,CRITICAL_2022_START,CRITICAL_2022_END,combined_map,fast,slow
        )

        rw=legacy_robustness(
            lpanel,lsignals,lanchor,leg.HOLDOUT_END,fast,slow,standalone_map,combined_map
        )
        legacy_windows.append(rw)

        legacy_results[f"{fast}_{slow}"]={
            "gate_diagnostics_full_panel":diag,
            "full_old_period":{
                "low_vol":low_full,
                "standalone_btc":standalone_full,
                "combined":combined_full,
            },
            "critical_2022":{
                "start":CRITICAL_2022_START.date().isoformat(),
                "end":CRITICAL_2022_END.date().isoformat(),
                "low_vol":low_critical,
                "standalone_btc":standalone_critical,
                "combined":combined_critical,
            },
            "robustness":robustness_summary(rw),
        }

    legacy_windows_df=pd.concat(legacy_windows,ignore_index=True)

    run_id=f"COMBINED_RECOVERY_GATE_V1_{_source_commit()[:12]}"
    run_dir=args.output_root/run_id
    run_dir.mkdir(parents=True,exist_ok=True)

    current_windows_df.to_csv(run_dir/"current8_robustness_windows.csv",index=False)
    legacy_windows_df.to_csv(run_dir/"legacy7_robustness_windows.csv",index=False)

    report={
        "run_id":run_id,
        "source_commit_sha":_source_commit(),
        "status":"COMBINED_RECOVERY_GATE_V1_EXECUTED",
        "rule":{
            "m2":"3m>0 AND 6m>0 AND 12m>0",
            "btc_candidates":[f"{f}/{s}" for f,s in BTC_CANDIDATES],
            "breadth":"breadth>=4",
            "entry":"breadth<=3 x3 -> next open CASH",
            "exit":"all recovery gates true on close -> next open CASH to current shadow target",
            "rearm":"breadth>=5 x3",
        },
        "current8":{
            "full_start":full_start,
            "end":dev.UNTOUCHED_END,
            "results":current_results,
            "crypto_dataset":crypto_meta,
            "btc_dataset":btc_meta,
        },
        "legacy7_retrospective":{
            "label":"RETROSPECTIVE_LEGACY7_ROBUSTNESS_NOT_VALIDATION",
            "anchor":lanchor,
            "end":leg.HOLDOUT_END,
            "results":legacy_results,
            "data":legacy_meta,
        },
        "m2_snapshot":{
            "path":str(M2_PATH.relative_to(REPO_ROOT)),
            "sha256":M2_SHA256,
            "source_probe_run":36306932451,
            "official_source":"Federal Reserve H.6 M2.M",
        },
        "interpretation_boundary":[
            "Retrospective combination test informed by prior component research.",
            "No new BTC SMA lengths were searched.",
            "No alternative M2 horizons were searched.",
            "Breadth>=4 is the complement of the frozen <=3 stress zone, not an optimized threshold.",
            "Legacy-7 period is already known and is not untouched validation.",
            "No production or paper-live change is authorized.",
        ],
    }
    _write_json(run_dir/"summary.json",report)
    print(json.dumps(_json_safe(report),indent=2,sort_keys=True))
    print(f"output={run_dir}")
    return 0


if __name__=="__main__":
    raise SystemExit(main())
