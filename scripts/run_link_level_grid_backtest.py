from __future__ import annotations

import argparse
import json
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


def _resolve_source_commit(explicit: str | None) -> str:
    if explicit:
        return explicit
    env_sha = os.environ.get("SOURCE_COMMIT_SHA")
    if env_sha:
        return env_sha
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO_ROOT,
        text=True,
    ).strip()


def _utc(value) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _default_cutoff(now=None) -> pd.Timestamp:
    current = pd.Timestamp.now(tz="UTC") if now is None else _utc(now)
    return current.floor("D")


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, default=str, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run experimental VAHRAM_LINK_LEVEL_GRID_V1 research backtest."
    )
    parser.add_argument("--symbol", default="LINKUSDT")
    parser.add_argument(
        "--history-years",
        type=int,
        default=6,
        help="Total historical years to download (default: 6).",
    )
    parser.add_argument(
        "--trade-years",
        type=int,
        default=3,
        help="Final years used for trading/evaluation (default: 3).",
    )
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--source-commit")
    parser.add_argument("--micro-capital", type=float, default=1000.0)
    parser.add_argument("--mid-capital", type=float, default=1000.0)
    parser.add_argument(
        "--allocation-preset",
        default="linear_depth_reserved",
        choices=[
            "linear_depth_reserved",
            "equal_reserved",
            "historical_observed",
        ],
    )
    parser.add_argument("--fee-bps", type=float, default=10.0)
    parser.add_argument("--slippage-bps", type=float, default=5.0)
    parser.add_argument("--range-lookback-candles", type=int, default=1095)
    parser.add_argument("--range-min-history-candles", type=int, default=1095)
    parser.add_argument("--range-refresh-candles", type=int, default=30)
    parser.add_argument("--ten-sublevel-from-main", type=int, default=7)
    parser.add_argument("--liquidate-at-end", action="store_true")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=REPO_ROOT / "research_artifacts" / "link_level_grid",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.history_years <= 0:
        raise ValueError("history-years must be positive")
    if args.trade_years <= 0:
        raise ValueError("trade-years must be positive")
    if args.history_years <= args.trade_years:
        raise ValueError("history-years must be greater than trade-years")

    cutoff = _utc(args.end) if args.end else _default_cutoff()
    trade_start = cutoff - pd.DateOffset(years=args.trade_years)
    start = (
        _utc(args.start)
        if args.start
        else cutoff - pd.DateOffset(years=args.history_years)
    )
    if start >= trade_start:
        raise ValueError(
            "download start must be earlier than trade_start so H/L has prehistory"
        )

    source_commit_sha = _resolve_source_commit(args.source_commit)
    download = download_historical_dataset(
        BinanceSpotRestClient(),
        symbol=args.symbol,
        start=start,
        end=cutoff,
        timeframe="1D",
        as_of=cutoff,
    )
    if download.dataset is None:
        print(f"Download failed: {download.metadata.status}")
        return 2
    if download.dataset.quality.has_critical_issues:
        print(f"Data-quality block: {download.dataset.quality}")
        return 3

    config = GridBacktestConfig(
        micro_capital=args.micro_capital,
        mid_capital=args.mid_capital,
        allocation_preset=args.allocation_preset,
        fee_bps=args.fee_bps,
        slippage_bps=args.slippage_bps,
        rolling_range=RollingRangePolicy(
            lookback_candles=args.range_lookback_candles,
            min_history_candles=args.range_min_history_candles,
            refresh_candles=args.range_refresh_candles,
        ),
        ten_sublevel_from_main=args.ten_sublevel_from_main,
        liquidate_at_end=args.liquidate_at_end,
    )

    result = run_grid_backtest(
        download.dataset.candles,
        dataset_id=download.dataset.dataset_id,
        source_commit_sha=source_commit_sha,
        config=config,
        evaluation_start=trade_start,
    )

    run_dir = args.output_root / result.run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    download.dataset.candles.to_csv(run_dir / "canonical_candles.csv", index=False)
    result.trades.to_csv(run_dir / "trades.csv", index=False)
    result.equity_curve.to_csv(run_dir / "equity_curve.csv", index=False)
    result.range_history.to_csv(run_dir / "range_history.csv", index=False)
    result.allocation_table.to_csv(run_dir / "allocation_table.csv", index=False)
    _write_json(run_dir / "summary.json", result.summary)
    _write_json(
        run_dir / "download_metadata.json",
        {
            "download": download.metadata.__dict__,
            "quality": download.dataset.quality.__dict__,
        },
    )

    summary = result.summary
    print(f"run_id={result.run_id}")
    print(f"dataset_id={summary['dataset_id']}")
    print(f"dataset_candles={summary['dataset_candles']}")
    print(f"prehistory_candles={summary['prehistory_candles']}")
    print(f"trading_candles={summary['candles']}")
    print(f"trading_period_start={summary['period_start']}")
    print(f"allocation_preset={summary['allocation_preset']}")
    print(f"total_return={summary['total_return']:.6f}")
    print(f"micro_total_return={summary['micro_total_return']:.6f}")
    print(f"mid_total_return={summary['mid_total_return']:.6f}")
    print(f"benchmark_buy_hold_return={summary['benchmark_buy_hold_return']:.6f}")
    print(f"max_drawdown={summary['max_drawdown']:.6f}")
    print(f"closed_trade_count={summary['closed_trade_count']}")
    print(f"open_micro_lots_end={summary['open_micro_lots_end']}")
    print(f"open_mid_lots_end={summary['open_mid_lots_end']}")
    print(f"output={run_dir}")
    print("status=RESEARCH_ONLY_NON_CANONICAL_ASSUMPTIONS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
