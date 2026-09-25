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
    "STATIC_BASE",
    "STATIC_WIDE",
    "M25_100",
    "M25_200",
    "M50_100",
    "M50_200",
    "NEAREST_PAIRS",
    "FARTHEST_PAIRS",
)


def _source_commit() -> str:
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO_ROOT,
        text=True,
    ).strip()


def _geomean_return(values: list[float]) -> float:
    wealth = [1.0 + value for value in values]
    if any(value <= 0 for value in wealth):
        return -1.0
    return math.exp(sum(math.log(value) for value in wealth) / len(wealth)) - 1.0


def _config(policy: str) -> GridBacktestConfig:
    if policy == "STATIC_BASE":
        micro_exit, mid_recovery, ma_policy = 1, 10, "NONE"
    elif policy == "STATIC_WIDE":
        micro_exit, mid_recovery, ma_policy = 6, 18, "NONE"
    else:
        micro_exit, mid_recovery, ma_policy = 6, 18, policy

    return GridBacktestConfig(
        micro_capital=1000.0,
        mid_capital=1000.0,
        allocation_preset="linear_depth_reserved",
        micro_allocation_power=1.0,
        mid_allocation_power=1.0,
        micro_exit_sublevels=micro_exit,
        mid_recovery_sublevels=mid_recovery,
        mid_target_scale=1.0,
        profit_reinvest_fraction=1.0,
        runner_fraction=0.0,
        entry_filter="NONE",
        dynamic_exit_policy="NONE",
        ma_exit_policy=ma_policy,
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Test moving averages as exit targets while preserving all grid buys."
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=REPO_ROOT / "research_artifacts" / "grid_ma_exit_targets",
    )
    args = parser.parse_args(argv)

    cutoff = pd.Timestamp.now(tz="UTC").floor("D")
    data_start = cutoff - pd.DateOffset(years=6)
    evaluation_start = cutoff - pd.DateOffset(years=3)
    client = BinanceSpotRestClient()

    datasets = {}
    for symbol in SYMBOLS:
        result = download_historical_dataset(
            client,
            symbol=symbol,
            start=data_start,
            end=cutoff,
            timeframe="1D",
            as_of=cutoff,
        )
        if result.dataset is None:
            raise RuntimeError(f"{symbol}: download failed")
        if result.dataset.quality.has_critical_issues:
            raise RuntimeError(f"{symbol}: critical data quality")
        datasets[symbol] = result.dataset

    source = _source_commit()
    rows: list[dict] = []
    for index, policy in enumerate(POLICIES, start=1):
        cfg = _config(policy)
        for symbol in SYMBOLS:
            ds = datasets[symbol]
            result = run_grid_backtest(
                ds.candles,
                dataset_id=ds.dataset_id,
                source_commit_sha=source,
                config=cfg,
                evaluation_start=evaluation_start,
            )
            rows.append(
                {
                    "candidate_id": f"GMA-{index:03d}",
                    "policy": policy,
                    "symbol": symbol,
                    "total_return": float(result.summary["total_return"]),
                    "micro_return": float(result.summary["micro_total_return"]),
                    "mid_return": float(result.summary["mid_total_return"]),
                    "benchmark_return": float(result.summary["benchmark_buy_hold_return"]),
                    "max_drawdown": float(result.summary["max_drawdown"]),
                    "closed_trade_count": int(result.summary["closed_trade_count"]),
                }
            )
        print(f"policy {index}/{len(POLICIES)}")

    per_asset = pd.DataFrame(rows)
    aggregates = []
    for candidate_id, frame in per_asset.groupby("candidate_id", sort=False):
        row0 = frame.iloc[0]
        aggregates.append(
            {
                "candidate_id": candidate_id,
                "policy": row0["policy"],
                "geomean_return": _geomean_return(frame["total_return"].astype(float).tolist()),
                "median_return": float(frame["total_return"].median()),
                "worst_asset_return": float(frame["total_return"].min()),
                "median_max_drawdown": float(frame["max_drawdown"].median()),
                "max_max_drawdown": float(frame["max_drawdown"].max()),
                "benchmark_wins": int((frame["total_return"] > frame["benchmark_return"]).sum()),
                "closed_trade_count_total": int(frame["closed_trade_count"].sum()),
            }
        )
    aggregate = pd.DataFrame(aggregates).sort_values(
        ["geomean_return", "median_max_drawdown"],
        ascending=[False, True],
    )

    base = aggregate[aggregate["policy"] == "STATIC_WIDE"].iloc[0]
    aggregate["geomean_delta_vs_static_wide"] = aggregate["geomean_return"] - float(base["geomean_return"])
    aggregate["median_dd_delta_vs_static_wide"] = aggregate["median_max_drawdown"] - float(base["median_max_drawdown"])

    per_symbol_base = per_asset[per_asset["policy"] == "STATIC_WIDE"][
        ["symbol", "total_return", "max_drawdown"]
    ].rename(
        columns={
            "total_return": "static_wide_return",
            "max_drawdown": "static_wide_dd",
        }
    )
    detail = per_asset.merge(per_symbol_base, on="symbol")
    detail["return_delta_vs_static_wide"] = detail["total_return"] - detail["static_wide_return"]
    detail["dd_delta_vs_static_wide"] = detail["max_drawdown"] - detail["static_wide_dd"]

    run_dir = args.output_root / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)
    aggregate.to_csv(run_dir / "aggregate.csv", index=False)
    detail.to_csv(run_dir / "per_asset_comparison.csv", index=False)

    summary = {
        "status": "EXPLORATORY_DEVELOPMENT_ONLY",
        "source_commit_sha": source,
        "evaluation_start": evaluation_start.isoformat(),
        "evaluation_end_exclusive": cutoff.isoformat(),
        "causal_semantics": (
            "All grid buys are unchanged. For MA variants, the previous completed daily candle "
            "sets today's reclaim target. Micro exits at SMA25/SMA50 variants instead of a fixed "
            "sublevel distance. Mid keeps its percentage target, while SMA100/SMA200 replaces "
            "the fixed +18 recovery alternative."
        ),
        "best_cross_asset": aggregate.iloc[0].to_dict(),
        "static_wide": base.to_dict(),
        "aggregate": aggregate.to_dict(orient="records"),
        "per_asset": detail.to_dict(orient="records"),
    }
    (run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    print(f"output={run_dir}")
    print(json.dumps(summary, indent=2, sort_keys=True, default=str, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
