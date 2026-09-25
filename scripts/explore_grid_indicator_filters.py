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
DEFAULT_FILTERS = (
    "NONE",
    "RSI_OVERSOLD_30",
    "STOCH_OVERSOLD_20",
    "BOLLINGER_LOWER",
    "SMA200_BULL",
    "MACD_BULL",
    "STOCH_BULL",
    "CORE_BULL",
    "DIP_IN_BULL",
    "V2_CORE_BULL",
)


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


def _download(
    symbols: tuple[str, ...],
    *,
    history_years: int,
    trade_years: int,
) -> list[DatasetBundle]:
    cutoff = _cutoff()
    start = cutoff - pd.DateOffset(years=history_years)
    evaluation_start = cutoff - pd.DateOffset(years=trade_years)
    client = BinanceSpotRestClient()
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


def _config(*, exit_profile: str, entry_filter: str) -> GridBacktestConfig:
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
        profit_reinvest_fraction=1.0,
        runner_fraction=0.0,
        entry_filter=entry_filter,
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
        description="Explore causal indicator filters layered on the working level grid."
    )
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--history-years", type=int, default=6)
    parser.add_argument("--trade-years", type=int, default=3)
    parser.add_argument("--filters", default=",".join(DEFAULT_FILTERS))
    parser.add_argument("--exit-profiles", default="BASE,WIDE")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=REPO_ROOT / "research_artifacts" / "grid_indicator_filters",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.history_years <= args.trade_years:
        raise ValueError("history-years must be greater than trade-years")

    symbols = tuple(item.strip().upper() for item in args.symbols.split(",") if item.strip())
    filters = tuple(item.strip().upper() for item in args.filters.split(",") if item.strip())
    exit_profiles = tuple(item.strip().upper() for item in args.exit_profiles.split(",") if item.strip())

    bundles = _download(
        symbols,
        history_years=args.history_years,
        trade_years=args.trade_years,
    )
    source_commit_sha = _source_commit()

    rows: list[dict] = []
    candidates = list(itertools.product(exit_profiles, filters))
    for index, (exit_profile, entry_filter) in enumerate(candidates, start=1):
        cfg = _config(exit_profile=exit_profile, entry_filter=entry_filter)
        candidate_id = f"GIF-{index:03d}"
        for bundle in bundles:
            result = run_grid_backtest(
                bundle.candles,
                dataset_id=bundle.dataset_id,
                source_commit_sha=source_commit_sha,
                config=cfg,
                evaluation_start=bundle.evaluation_start,
            )
            buy_events = 0
            sell_events = 0
            if not result.events.empty:
                buy_events = int((result.events["event_type"] == "BUY").sum())
                sell_events = int((result.events["event_type"] == "SELL").sum())
            rows.append(
                {
                    "candidate_id": candidate_id,
                    "exit_profile": exit_profile,
                    "entry_filter": entry_filter,
                    "symbol": bundle.symbol,
                    "dataset_id": bundle.dataset_id,
                    "total_return": float(result.summary["total_return"]),
                    "micro_return": float(result.summary["micro_total_return"]),
                    "mid_return": float(result.summary["mid_total_return"]),
                    "benchmark_return": float(result.summary["benchmark_buy_hold_return"]),
                    "max_drawdown": float(result.summary["max_drawdown"]),
                    "closed_trade_count": int(result.summary["closed_trade_count"]),
                    "buy_events": buy_events,
                    "sell_events": sell_events,
                }
            )
        print(f"candidate {index}/{len(candidates)}")

    per_asset = pd.DataFrame(rows)
    aggregates: list[dict] = []
    for candidate_id, frame in per_asset.groupby("candidate_id", sort=False):
        returns = frame["total_return"].astype(float).tolist()
        row0 = frame.iloc[0]
        aggregates.append(
            {
                "candidate_id": candidate_id,
                "exit_profile": row0["exit_profile"],
                "entry_filter": row0["entry_filter"],
                "geomean_return": _geomean_return(returns),
                "median_return": float(frame["total_return"].median()),
                "worst_asset_return": float(frame["total_return"].min()),
                "median_max_drawdown": float(frame["max_drawdown"].median()),
                "max_max_drawdown": float(frame["max_drawdown"].max()),
                "benchmark_wins": int(
                    (frame["total_return"] > frame["benchmark_return"]).sum()
                ),
                "closed_trade_count_total": int(frame["closed_trade_count"].sum()),
                "buy_events_total": int(frame["buy_events"].sum()),
                "sell_events_total": int(frame["sell_events"].sum()),
            }
        )
    aggregate = pd.DataFrame(aggregates).sort_values(
        ["geomean_return", "median_max_drawdown"],
        ascending=[False, True],
    )

    per_symbol_best = (
        per_asset.sort_values(["symbol", "total_return"], ascending=[True, False])
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

    def pick(exit_profile: str, entry_filter: str) -> dict:
        hit = aggregate[
            (aggregate["exit_profile"] == exit_profile)
            & (aggregate["entry_filter"] == entry_filter)
        ]
        if hit.empty:
            raise RuntimeError(f"Missing candidate {exit_profile}/{entry_filter}")
        return hit.iloc[0].to_dict()

    summary = {
        "status": "EXPLORATORY_DEVELOPMENT_ONLY",
        "source_commit_sha": source_commit_sha,
        "symbols": list(symbols),
        "candidate_count": int(len(aggregate)),
        "causal_rule": (
            "Indicator filters use only the previous completed daily candle; "
            "the current candle can touch grid levels only after the filter was already known."
        ),
        "control_base": pick("BASE", "NONE"),
        "control_wide": pick("WIDE", "NONE"),
        "best_cross_asset": aggregate.iloc[0].to_dict(),
        "per_symbol_best": per_symbol_best.to_dict(orient="records"),
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
