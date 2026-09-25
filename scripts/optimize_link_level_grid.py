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


def _utc(value) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _default_cutoff() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC").floor("D")


def _source_commit() -> str:
    env_sha = os.environ.get("SOURCE_COMMIT_SHA")
    if env_sha:
        return env_sha
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO_ROOT,
        text=True,
    ).strip()


def _parse_floats(value: str) -> tuple[float, ...]:
    return tuple(float(item.strip()) for item in value.split(",") if item.strip())


def _parse_ints(value: str) -> tuple[int, ...]:
    return tuple(int(item.strip()) for item in value.split(",") if item.strip())


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def _geometric_mean_return(returns: list[float]) -> float:
    if not returns:
        raise ValueError("returns cannot be empty")
    wealth = [1.0 + value for value in returns]
    if any(value <= 0 for value in wealth):
        return -1.0
    return math.exp(sum(math.log(value) for value in wealth) / len(wealth)) - 1.0


def _period_return(equity: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp | None) -> float:
    frame = equity.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    scoped = frame[frame["timestamp"] >= start]
    if end is not None:
        scoped = scoped[scoped["timestamp"] < end]
    if scoped.empty:
        raise ValueError("No equity rows for requested period")
    start_equity = float(scoped.iloc[0]["total_equity"])
    end_equity = float(scoped.iloc[-1]["total_equity"])
    return (end_equity / start_equity) - 1.0


def _drawdown(equity: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp | None) -> float:
    frame = equity.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    scoped = frame[frame["timestamp"] >= start]
    if end is not None:
        scoped = scoped[scoped["timestamp"] < end]
    values = scoped["total_equity"].astype(float)
    if values.empty:
        raise ValueError("No equity rows for requested period")
    running_peak = values.cummax()
    dd = (values / running_peak) - 1.0
    return abs(float(dd.min()))


def _download(
    *,
    symbols: tuple[str, ...],
    cutoff: pd.Timestamp,
    history_years: int,
    trade_years: int,
) -> list[DatasetBundle]:
    client = BinanceSpotRestClient()
    start = cutoff - pd.DateOffset(years=history_years)
    evaluation_start = cutoff - pd.DateOffset(years=trade_years)
    bundles: list[DatasetBundle] = []
    for symbol in symbols:
        result = download_historical_dataset(
            client,
            symbol=symbol,
            start=start,
            end=cutoff,
            timeframe="1D",
            as_of=cutoff,
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
    micro_power: float,
    mid_power: float,
    micro_exit: int,
    mid_recovery: int,
    mid_target_scale: float,
    refresh_candles: int,
) -> GridBacktestConfig:
    return GridBacktestConfig(
        micro_capital=1000.0,
        mid_capital=1000.0,
        allocation_preset="linear_depth_reserved",
        micro_allocation_power=micro_power,
        mid_allocation_power=mid_power,
        micro_exit_sublevels=micro_exit,
        mid_recovery_sublevels=mid_recovery,
        mid_target_scale=mid_target_scale,
        fee_bps=10.0,
        slippage_bps=5.0,
        rolling_range=RollingRangePolicy(
            lookback_candles=1095,
            min_history_candles=1095,
            refresh_candles=refresh_candles,
        ),
        ten_sublevel_from_main=7,
        liquidate_at_end=False,
    )


def _evaluate_candidate(
    *,
    candidate_id: str,
    phase: str,
    bundles: list[DatasetBundle],
    source_commit_sha: str,
    config: GridBacktestConfig,
    validation_days: int,
) -> tuple[dict, list[dict]]:
    asset_rows: list[dict] = []
    for bundle in bundles:
        result = run_grid_backtest(
            bundle.candles,
            dataset_id=bundle.dataset_id,
            source_commit_sha=source_commit_sha,
            config=config,
            evaluation_start=bundle.evaluation_start,
        )
        validation_start = pd.Timestamp(result.summary["period_end"]) - pd.DateOffset(
            days=int(validation_days - 1)
        )
        train_return = _period_return(
            result.equity_curve,
            bundle.evaluation_start,
            validation_start,
        )
        validation_return = _period_return(
            result.equity_curve,
            validation_start,
            None,
        )
        train_drawdown = _drawdown(
            result.equity_curve,
            bundle.evaluation_start,
            validation_start,
        )
        validation_drawdown = _drawdown(
            result.equity_curve,
            validation_start,
            None,
        )
        asset_rows.append(
            {
                "candidate_id": candidate_id,
                "phase": phase,
                "symbol": bundle.symbol,
                "dataset_id": bundle.dataset_id,
                "micro_power": config.micro_allocation_power,
                "mid_power": config.mid_allocation_power,
                "micro_exit_sublevels": config.micro_exit_sublevels,
                "mid_recovery_sublevels": config.mid_recovery_sublevels,
                "mid_target_scale": config.mid_target_scale,
                "train_return": train_return,
                "validation_return": validation_return,
                "full_return": float(result.summary["total_return"]),
                "full_benchmark_return": float(
                    result.summary["benchmark_buy_hold_return"]
                ),
                "full_max_drawdown": float(result.summary["max_drawdown"]),
                "train_max_drawdown": train_drawdown,
                "validation_max_drawdown": validation_drawdown,
                "closed_trade_count": int(result.summary["closed_trade_count"]),
            }
        )

    train_returns = [row["train_return"] for row in asset_rows]
    validation_returns = [row["validation_return"] for row in asset_rows]
    full_returns = [row["full_return"] for row in asset_rows]
    train_drawdowns = [row["train_max_drawdown"] for row in asset_rows]
    full_drawdowns = [row["full_max_drawdown"] for row in asset_rows]

    aggregate = {
        "candidate_id": candidate_id,
        "phase": phase,
        "micro_power": config.micro_allocation_power,
        "mid_power": config.mid_allocation_power,
        "micro_exit_sublevels": config.micro_exit_sublevels,
        "mid_recovery_sublevels": config.mid_recovery_sublevels,
        "mid_target_scale": config.mid_target_scale,
        "train_geomean_return": _geometric_mean_return(train_returns),
        "validation_geomean_return": _geometric_mean_return(validation_returns),
        "full_geomean_return": _geometric_mean_return(full_returns),
        "train_median_return": float(pd.Series(train_returns).median()),
        "validation_median_return": float(pd.Series(validation_returns).median()),
        "full_median_return": float(pd.Series(full_returns).median()),
        "worst_full_return": min(full_returns),
        "median_train_drawdown": float(pd.Series(train_drawdowns).median()),
        "median_full_drawdown": float(pd.Series(full_drawdowns).median()),
        "benchmark_wins_full": sum(
            row["full_return"] > row["full_benchmark_return"] for row in asset_rows
        ),
    }
    aggregate["train_robust_score"] = (
        aggregate["train_geomean_return"]
        - 0.50 * aggregate["median_train_drawdown"]
    )
    return aggregate, asset_rows


def _run_phase(
    *,
    phase: str,
    candidates: list[GridBacktestConfig],
    bundles: list[DatasetBundle],
    source_commit_sha: str,
    validation_days: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    aggregate_rows: list[dict] = []
    asset_rows: list[dict] = []
    total = len(candidates)
    for index, config in enumerate(candidates, start=1):
        candidate_id = f"{phase.upper()}-{index:04d}"
        aggregate, per_asset = _evaluate_candidate(
            candidate_id=candidate_id,
            phase=phase,
            bundles=bundles,
            source_commit_sha=source_commit_sha,
            config=config,
            validation_days=validation_days,
        )
        aggregate_rows.append(aggregate)
        asset_rows.extend(per_asset)
        if index == 1 or index % 10 == 0 or index == total:
            print(f"{phase}: {index}/{total}")
    aggregate_df = pd.DataFrame(aggregate_rows).sort_values(
        ["train_geomean_return", "train_robust_score"],
        ascending=False,
    )
    asset_df = pd.DataFrame(asset_rows)
    return aggregate_df, asset_df


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Research optimizer for VAHRAM_LINK_LEVEL_GRID_V1."
    )
    parser.add_argument(
        "--symbols",
        default=",".join(DEFAULT_SYMBOLS),
        help="Comma-separated Binance Spot symbols.",
    )
    parser.add_argument("--history-years", type=int, default=6)
    parser.add_argument("--trade-years", type=int, default=3)
    parser.add_argument("--validation-days", type=int, default=365)
    parser.add_argument("--range-refresh-candles", type=int, default=30)
    parser.add_argument(
        "--phases",
        default="capital,exit",
        help="Comma-separated phases: capital,exit",
    )
    parser.add_argument(
        "--allocation-powers",
        default="0,0.5,1,1.5,2,2.5,3",
    )
    parser.add_argument("--micro-exits", default="1,2,3,4")
    parser.add_argument("--mid-recoveries", default="6,8,10,12,14")
    parser.add_argument("--mid-target-scales", default="0.75,1,1.25")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=REPO_ROOT / "research_artifacts" / "link_level_grid_optimizer",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    symbols = tuple(item.strip().upper() for item in args.symbols.split(",") if item.strip())
    phases = tuple(item.strip().lower() for item in args.phases.split(",") if item.strip())
    unknown = set(phases).difference({"capital", "exit"})
    if unknown:
        raise ValueError(f"Unknown phases: {sorted(unknown)}")
    if args.history_years <= args.trade_years:
        raise ValueError("history-years must be greater than trade-years")
    if args.validation_days <= 0:
        raise ValueError("validation-days must be positive")

    cutoff = _default_cutoff()
    bundles = _download(
        symbols=symbols,
        cutoff=cutoff,
        history_years=args.history_years,
        trade_years=args.trade_years,
    )
    source_commit_sha = _source_commit()

    run_key = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir = args.output_root / run_key
    run_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "status": "RESEARCH_ONLY_NOT_CANONICAL",
        "source_commit_sha": source_commit_sha,
        "symbols": symbols,
        "cutoff": cutoff.isoformat(),
        "history_years": args.history_years,
        "trade_years": args.trade_years,
        "validation_days": args.validation_days,
        "selection_rule": (
            "Select on first ~2 years only using cross-asset geometric mean return; "
            "report final 365 days as temporal holdout. Holdout is not used to rank."
        ),
        "range_refresh_candles": args.range_refresh_candles,
    }

    summary: dict[str, dict] = {}
    if "capital" in phases:
        powers = _parse_floats(args.allocation_powers)
        candidates = [
            _config(
                micro_power=micro_power,
                mid_power=mid_power,
                micro_exit=1,
                mid_recovery=10,
                mid_target_scale=1.0,
                refresh_candles=args.range_refresh_candles,
            )
            for micro_power, mid_power in itertools.product(powers, powers)
        ]
        aggregate, per_asset = _run_phase(
            phase="capital",
            candidates=candidates,
            bundles=bundles,
            source_commit_sha=source_commit_sha,
            validation_days=args.validation_days,
        )
        aggregate.to_csv(run_dir / "capital_candidates.csv", index=False)
        per_asset.to_csv(run_dir / "capital_per_asset.csv", index=False)
        best_profit = aggregate.iloc[0].to_dict()
        best_robust = aggregate.sort_values(
            "train_robust_score", ascending=False
        ).iloc[0].to_dict()
        summary["capital"] = {
            "candidate_count": len(aggregate),
            "best_train_profit": best_profit,
            "best_train_robust": best_robust,
        }

    if "exit" in phases:
        micro_exits = _parse_ints(args.micro_exits)
        recoveries = _parse_ints(args.mid_recoveries)
        scales = _parse_floats(args.mid_target_scales)
        candidates = [
            _config(
                micro_power=1.0,
                mid_power=1.0,
                micro_exit=micro_exit,
                mid_recovery=recovery,
                mid_target_scale=scale,
                refresh_candles=args.range_refresh_candles,
            )
            for micro_exit, recovery, scale in itertools.product(
                micro_exits, recoveries, scales
            )
        ]
        aggregate, per_asset = _run_phase(
            phase="exit",
            candidates=candidates,
            bundles=bundles,
            source_commit_sha=source_commit_sha,
            validation_days=args.validation_days,
        )
        aggregate.to_csv(run_dir / "exit_candidates.csv", index=False)
        per_asset.to_csv(run_dir / "exit_per_asset.csv", index=False)
        best_profit = aggregate.iloc[0].to_dict()
        best_robust = aggregate.sort_values(
            "train_robust_score", ascending=False
        ).iloc[0].to_dict()
        summary["exit"] = {
            "candidate_count": len(aggregate),
            "best_train_profit": best_profit,
            "best_train_robust": best_robust,
        }

    _write_json(run_dir / "manifest.json", manifest)
    _write_json(run_dir / "summary.json", summary)

    print(f"optimizer_output={run_dir}")
    print(json.dumps(summary, indent=2, sort_keys=True, default=str, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
