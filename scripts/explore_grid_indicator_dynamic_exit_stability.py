from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
from pathlib import Path

import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.link_level_grid.strategy import (
    GridBacktestConfig,
    RollingRangePolicy,
    run_grid_backtest,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
SYMBOLS = ("LINKUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "BTCUSDT")
POLICIES = (
    "NONE_WIDE",
    "RSI_OVERSOLD_WIDE",
    "STOCH_OVERSOLD_WIDE",
    "BOLLINGER_LOWER_WIDE",
    "SMA200_BULL_WIDE",
    "MACD_BULL_WIDE",
    "STOCH_BULL_WIDE",
    "CORE_BULL_WIDE",
    "V2_CORE_BULL_WIDE",
    "SMA200_OR_MACD_WIDE",
)


def _source_commit() -> str:
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, text=True).strip()


def _geomean_return(values: list[float]) -> float:
    wealth = [1.0 + value for value in values]
    if any(value <= 0 for value in wealth):
        return -1.0
    return math.exp(sum(math.log(value) for value in wealth) / len(wealth)) - 1.0


def _config(policy: str) -> GridBacktestConfig:
    dynamic = "NONE" if policy == "NONE_WIDE" else policy
    return GridBacktestConfig(
        micro_capital=1000.0,
        mid_capital=1000.0,
        allocation_preset="linear_depth_reserved",
        micro_allocation_power=1.0,
        mid_allocation_power=1.0,
        micro_exit_sublevels=6 if policy == "NONE_WIDE" else 1,
        mid_recovery_sublevels=18 if policy == "NONE_WIDE" else 10,
        mid_target_scale=1.0,
        profit_reinvest_fraction=1.0,
        runner_fraction=0.0,
        entry_filter="NONE",
        dynamic_exit_policy=dynamic,
        dynamic_wide_micro_exit_sublevels=6,
        dynamic_wide_mid_recovery_sublevels=18,
        fee_bps=10.0,
        slippage_bps=5.0,
        rolling_range=RollingRangePolicy(
            lookback_candles=1095,
            min_history_candles=1095,
            refresh_candles=30,
        ),
        ten_sublevel_from_main=7,
        liquidate_at_end=False,
    )


def build_parser() -> argparse.ArgumentParser:
    p=argparse.ArgumentParser(description="Temporal stability test for indicator-driven grid exits.")
    p.add_argument("--output-root", type=Path, default=REPO_ROOT/"research_artifacts"/"grid_indicator_dynamic_exit_stability")
    return p


def main(argv: list[str] | None = None) -> int:
    args=build_parser().parse_args(argv)
    cutoff=pd.Timestamp.now(tz="UTC").floor("D")
    window_ends=[cutoff-pd.DateOffset(years=2), cutoff-pd.DateOffset(years=1), cutoff]
    windows=[]
    for end in window_ends:
        start=end-pd.DateOffset(years=1)
        windows.append((start,end))

    data_start=min(start for start,_ in windows)-pd.DateOffset(years=3)-pd.Timedelta(days=7)
    client=BinanceSpotRestClient()
    datasets={}
    for symbol in SYMBOLS:
        result=download_historical_dataset(client,symbol=symbol,start=data_start,end=cutoff,timeframe="1D",as_of=cutoff)
        if result.dataset is None:
            raise RuntimeError(f"{symbol}: no dataset")
        if result.dataset.quality.has_critical_issues:
            raise RuntimeError(f"{symbol}: critical data quality")
        datasets[symbol]=result.dataset

    source=_source_commit()
    rows=[]
    for widx,(start,end) in enumerate(windows, start=1):
        for pidx,policy in enumerate(POLICIES, start=1):
            cfg=_config(policy)
            for symbol in SYMBOLS:
                ds=datasets[symbol]
                sliced=ds.candles[pd.to_datetime(ds.candles["timestamp"], utc=True) < end].copy()
                result=run_grid_backtest(
                    sliced,
                    dataset_id=f"{ds.dataset_id}:W{widx}",
                    source_commit_sha=source,
                    config=cfg,
                    evaluation_start=start,
                )
                rows.append({
                    "window":widx,
                    "window_start":start.isoformat(),
                    "window_end_exclusive":end.isoformat(),
                    "policy":policy,
                    "symbol":symbol,
                    "total_return":float(result.summary["total_return"]),
                    "max_drawdown":float(result.summary["max_drawdown"]),
                    "closed_trade_count":int(result.summary["closed_trade_count"]),
                })
        print(f"window {widx}/{len(windows)}")

    df=pd.DataFrame(rows)
    aggregates=[]
    for (window,policy),frame in df.groupby(["window","policy"], sort=False):
        aggregates.append({
            "window":int(window),
            "policy":policy,
            "geomean_return":_geomean_return(frame["total_return"].astype(float).tolist()),
            "median_return":float(frame["total_return"].median()),
            "median_max_drawdown":float(frame["max_drawdown"].median()),
            "closed_trade_count_total":int(frame["closed_trade_count"].sum()),
        })
    agg=pd.DataFrame(aggregates)
    baseline=agg[agg["policy"]=="NONE_WIDE"][["window","geomean_return","median_max_drawdown"]].rename(
        columns={"geomean_return":"wide_geomean","median_max_drawdown":"wide_median_dd"}
    )
    comp=agg.merge(baseline,on="window")
    comp["geomean_delta_vs_wide"]=comp["geomean_return"]-comp["wide_geomean"]
    comp["dd_delta_vs_wide"]=comp["median_max_drawdown"]-comp["wide_median_dd"]

    symbol_base=df[df["policy"]=="NONE_WIDE"][["window","symbol","total_return"]].rename(columns={"total_return":"wide_return"})
    sym=df.merge(symbol_base,on=["window","symbol"])
    sym["return_delta_vs_wide"]=sym["total_return"]-sym["wide_return"]

    stability=(
        comp[comp["policy"]!="NONE_WIDE"]
        .groupby("policy",as_index=False)
        .agg(
            windows_beating_wide=("geomean_delta_vs_wide",lambda s:int((s>0).sum())),
            mean_geomean_delta_vs_wide=("geomean_delta_vs_wide","mean"),
            worst_window_delta_vs_wide=("geomean_delta_vs_wide","min"),
            best_window_delta_vs_wide=("geomean_delta_vs_wide","max"),
        )
    )
    asset_stability=(
        sym[sym["policy"]!="NONE_WIDE"]
        .groupby(["policy","symbol"],as_index=False)
        .agg(
            windows_beating_wide=("return_delta_vs_wide",lambda s:int((s>0).sum())),
            mean_return_delta_vs_wide=("return_delta_vs_wide","mean"),
        )
    )

    run_dir=args.output_root/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True,exist_ok=True)
    df.to_csv(run_dir/"per_asset_window.csv",index=False)
    comp.to_csv(run_dir/"aggregate_window_comparison.csv",index=False)
    stability.to_csv(run_dir/"policy_stability.csv",index=False)
    asset_stability.to_csv(run_dir/"asset_policy_stability.csv",index=False)

    summary={
        "status":"TEMPORAL_STABILITY_DIAGNOSTIC_NONBLIND",
        "source_commit_sha":source,
        "windows":[{"window":i+1,"start":s.isoformat(),"end_exclusive":e.isoformat()} for i,(s,e) in enumerate(windows)],
        "baseline":"NONE_WIDE",
        "policy_stability":stability.sort_values(["windows_beating_wide","mean_geomean_delta_vs_wide"],ascending=[False,False]).to_dict(orient="records"),
        "asset_policy_stability":asset_stability.sort_values(["symbol","windows_beating_wide","mean_return_delta_vs_wide"],ascending=[True,False,False]).to_dict(orient="records"),
    }
    (run_dir/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True,default=str,allow_nan=False)+"\n",encoding="utf-8")
    print(f"output={run_dir}")
    print(json.dumps(summary,indent=2,sort_keys=True,default=str,allow_nan=False))
    return 0


if __name__=="__main__":
    raise SystemExit(main())
