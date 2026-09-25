from __future__ import annotations

import argparse
import itertools
import json
import math
import os
import subprocess
from dataclasses import dataclass
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
DEFAULT_SYMBOLS = ("LINKUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "BTCUSDT")


@dataclass(frozen=True)
class DatasetBundle:
    symbol: str
    dataset_id: str
    candles: pd.DataFrame
    evaluation_start: pd.Timestamp


def _source_commit() -> str:
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO_ROOT,
        text=True,
    ).strip()


def _cutoff() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC").floor("D")


def _geomean_return(values: list[float]) -> float:
    wealth = [1.0 + value for value in values]
    if any(value <= 0 for value in wealth):
        return -1.0
    return math.exp(sum(math.log(value) for value in wealth) / len(wealth)) - 1.0


def _parse_floats(value: str) -> tuple[float, ...]:
    return tuple(float(item.strip()) for item in value.split(",") if item.strip())


def _download(symbols: tuple[str, ...], history_years: int, trade_years: int) -> list[DatasetBundle]:
    end = _cutoff()
    start = end - pd.DateOffset(years=history_years)
    evaluation_start = end - pd.DateOffset(years=trade_years)
    client = BinanceSpotRestClient()
    bundles: list[DatasetBundle] = []
    for symbol in symbols:
        result = download_historical_dataset(
            client,
            symbol=symbol,
            start=start,
            end=end,
            timeframe="1D",
            as_of=end,
        )
        if result.dataset is None:
            raise RuntimeError(f"{symbol}: download failed: {result.metadata.status}")
        if result.dataset.quality.has_critical_issues:
            raise RuntimeError(f"{symbol}: critical data quality: {result.dataset.quality}")
        bundles.append(
            DatasetBundle(
                symbol=symbol,
                dataset_id=result.dataset.dataset_id,
                candles=result.dataset.candles,
                evaluation_start=evaluation_start,
            )
        )
    return bundles


def _config(
    *,
    exit_profile: str,
    reinvest_fraction: float,
    runner_fraction: float,
) -> GridBacktestConfig:
    if exit_profile == "BASE":
        micro_exit = 1
        mid_recovery = 10
    elif exit_profile == "WIDE":
        micro_exit = 6
        mid_recovery = 18
    else:
        raise ValueError(f"Unknown exit profile: {exit_profile}")

    return GridBacktestConfig(
        micro_capital=1000.0,
        mid_capital=1000.0,
        allocation_preset="linear_depth_reserved",
        micro_allocation_power=1.0,
        mid_allocation_power=1.0,
        micro_exit_sublevels=micro_exit,
        mid_recovery_sublevels=mid_recovery,
        mid_target_scale=1.0,
        profit_reinvest_fraction=reinvest_fraction,
        runner_fraction=runner_fraction,
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
    parser = argparse.ArgumentParser(
        description="Explore profit reinvestment and partial token runners."
    )
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--history-years", type=int, default=6)
    parser.add_argument("--trade-years", type=int, default=3)
    parser.add_argument("--reinvest-fractions", default="0,0.5,1")
    parser.add_argument("--runner-fractions", default="0,0.1,0.25,0.5")
    parser.add_argument("--exit-profiles", default="BASE,WIDE")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=REPO_ROOT / "research_artifacts" / "grid_reinvest_runner",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.history_years <= args.trade_years:
        raise ValueError("history-years must be greater than trade-years")

    symbols = tuple(item.strip().upper() for item in args.symbols.split(",") if item.strip())
    reinvest_values = _parse_floats(args.reinvest_fractions)
    runner_values = _parse_floats(args.runner_fractions)
    exit_profiles = tuple(item.strip().upper() for item in args.exit_profiles.split(",") if item.strip())

    bundles = _download(symbols, args.history_years, args.trade_years)
    source_commit_sha = _source_commit()

    rows: list[dict] = []
    candidate_index = 0
    for exit_profile, reinvest_fraction, runner_fraction in itertools.product(
        exit_profiles,
        reinvest_values,
        runner_values,
    ):
        candidate_index += 1
        candidate_id = f"RR-{candidate_index:03d}"
        cfg = _config(
            exit_profile=exit_profile,
            reinvest_fraction=reinvest_fraction,
            runner_fraction=runner_fraction,
        )
        for bundle in bundles:
            result = run_grid_backtest(
                bundle.candles,
                dataset_id=bundle.dataset_id,
                source_commit_sha=source_commit_sha,
                config=cfg,
                evaluation_start=bundle.evaluation_start,
            )
            rows.append(
                {
                    "candidate_id": candidate_id,
                    "symbol": bundle.symbol,
                    "exit_profile": exit_profile,
                    "profit_reinvest_fraction": reinvest_fraction,
                    "runner_fraction": runner_fraction,
                    "total_return": float(result.summary["total_return"]),
                    "micro_return": float(result.summary["micro_total_return"]),
                    "mid_return": float(result.summary["mid_total_return"]),
                    "benchmark_return": float(result.summary["benchmark_buy_hold_return"]),
                    "max_drawdown": float(result.summary["max_drawdown"]),
                    "closed_trade_count": int(result.summary["closed_trade_count"]),
                    "runner_value_end": float(
                        result.summary["micro_runner_value_end"]
                        + result.summary["mid_runner_value_end"]
                    ),
                    "profit_reserve_end": float(
                        result.summary["micro_profit_reserve_end"]
                        + result.summary["mid_profit_reserve_end"]
                    ),
                }
            )
        print(f"candidate {candidate_index}/{len(exit_profiles) * len(reinvest_values) * len(runner_values)}")

    per_asset = pd.DataFrame(rows)
    aggregates: list[dict] = []
    for candidate_id, frame in per_asset.groupby("candidate_id", sort=False):
        returns = frame["total_return"].astype(float).tolist()
        row0 = frame.iloc[0]
        aggregates.append(
            {
                "candidate_id": candidate_id,
                "exit_profile": row0["exit_profile"],
                "profit_reinvest_fraction": float(row0["profit_reinvest_fraction"]),
                "runner_fraction": float(row0["runner_fraction"]),
                "geomean_return": _geomean_return(returns),
                "median_return": float(frame["total_return"].median()),
                "worst_asset_return": float(frame["total_return"].min()),
                "median_max_drawdown": float(frame["max_drawdown"].median()),
                "benchmark_wins": int(
                    (frame["total_return"] > frame["benchmark_return"]).sum()
                ),
                "closed_trade_count_total": int(frame["closed_trade_count"].sum()),
                "runner_value_end_total": float(frame["runner_value_end"].sum()),
                "profit_reserve_end_total": float(frame["profit_reserve_end"].sum()),
            }
        )
    aggregate = pd.DataFrame(aggregates).sort_values(
        ["geomean_return", "median_max_drawdown"],
        ascending=[False, True],
    )

    per_symbol_best = (
        per_asset.sort_values("total_return", ascending=False)
        .groupby("symbol", as_index=False)
        .first()
        .sort_values("symbol")
    )

    run_key = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir = args.output_root / run_key
    run_dir.mkdir(parents=True, exist_ok=True)
    per_asset.to_csv(run_dir / "per_asset.csv", index=False)
    aggregate.to_csv(run_dir / "aggregate.csv", index=False)
    per_symbol_best.to_csv(run_dir / "per_symbol_best.csv", index=False)

    summary = {
        "status": "EXPLORATORY_ONLY_NOT_VALIDATION",
        "source_commit_sha": source_commit_sha,
        "symbols": list(symbols),
        "candidate_count": int(len(aggregate)),
        "best_cross_asset": aggregate.iloc[0].to_dict(),
        "baseline_full_reinvest_no_runner": aggregate[
            (aggregate["exit_profile"] == "BASE")
            & (aggregate["profit_reinvest_fraction"] == 1.0)
            & (aggregate["runner_fraction"] == 0.0)
        ].iloc[0].to_dict(),
        "per_symbol_best": per_symbol_best.to_dict(orient="records"),
        "interpretation": (
            "Current strategy already compounds 100% of positive realized profit "
            "inside each slot. runner_fraction leaves that share of every exited "
            "position unsold and marked to market through the end of the test."
        ),
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
