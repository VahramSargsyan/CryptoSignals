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
from scripts.research_u10_ledger_entry_stress_v1 import (
    LOOKBACK as U10_LOOKBACK,
    U10,
    build_events as build_u10_events,
    download_panel as download_u10_panel,
    run_window as run_u10_window,
    utc,
)
from strategies.crypto.link_level_grid.strategy import (
    GridBacktestConfig,
    RollingRangePolicy,
    run_grid_backtest,
)

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "grid_vs_u10_universe_portfolios_v1"

INITIAL_USDT = 10000.0
PRIMARY_START = pd.Timestamp("2023-10-31", tz="UTC")
GRID_DATA_START = pd.Timestamp("2019-01-01", tz="UTC")
GRID_LOOKBACK = 1095

GRID_UNIVERSES = {
    "GRID_TIER_A_5": ("LINK", "SOL", "ETH", "ADA", "XLM"),
    "GRID_TIER_A_B_10": (
        "LINK", "SOL", "ETH", "ADA", "XLM",
        "HBAR", "UNI", "DOGE", "AVAX", "LTC",
    ),
    "GRID_FULL_15": (
        "BTC", "ETH", "BNB", "SOL", "XRP",
        "TRX", "DOGE", "ADA", "LINK", "XLM",
        "LTC", "HBAR", "AVAX", "BCH", "UNI",
    ),
    "GRID_U10_MATURE_8": (
        "ATOM", "BNB", "SOL", "TRX", "AAVE", "LINK", "FIL", "HBAR",
    ),
}

PROFILES = {
    "BASE": {"micro_exit_sublevels": 1, "mid_recovery_sublevels": 10},
    "WIDE": {"micro_exit_sublevels": 6, "mid_recovery_sublevels": 18},
}


def source_sha() -> str:
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def download_grid_candles(
    client: BinanceSpotRestClient,
    asset: str,
    cutoff: pd.Timestamp,
) -> tuple[pd.DataFrame, dict]:
    result = download_historical_dataset(
        client,
        symbol=asset + "USDT",
        start=GRID_DATA_START,
        end=cutoff,
        timeframe="1D",
        as_of=cutoff,
    )
    if result.dataset is None:
        raise RuntimeError(f"{asset}: {result.metadata.status}")
    if result.dataset.quality.has_critical_issues:
        raise RuntimeError(f"{asset}: critical data quality issues")

    frame = result.dataset.candles[
        ["timestamp", "open", "high", "low", "close", "volume"]
    ].copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame = frame.sort_values("timestamp", kind="stable").reset_index(drop=True)

    meta = {
        "rows": len(frame),
        "start": utc(frame.iloc[0]["timestamp"]).isoformat(),
        "end": utc(frame.iloc[-1]["timestamp"]).isoformat(),
        "listing_truncated": bool(result.metadata.listing_truncated),
    }
    return frame, meta


def assert_grid_eligible(
    candles: pd.DataFrame,
    asset: str,
    evaluation_start: pd.Timestamp,
) -> None:
    prior = int((candles["timestamp"] < evaluation_start).sum())
    if prior < GRID_LOOKBACK:
        raise RuntimeError(
            f"{asset}: only {prior} daily candles before {evaluation_start.date()}, "
            f"need {GRID_LOOKBACK}"
        )


def first_grid_eligible_date(candles: pd.DataFrame, asset: str) -> pd.Timestamp:
    if len(candles) <= GRID_LOOKBACK:
        raise RuntimeError(
            f"{asset}: only {len(candles)} candles, cannot satisfy {GRID_LOOKBACK} prehistory"
        )
    return utc(candles.iloc[GRID_LOOKBACK]["timestamp"])


def build_grid_config(token_capital: float, profile: str) -> GridBacktestConfig:
    params = PROFILES[profile]
    return GridBacktestConfig(
        micro_capital=token_capital / 2.0,
        mid_capital=token_capital / 2.0,
        allocation_preset="linear_depth_reserved",
        micro_exit_sublevels=params["micro_exit_sublevels"],
        mid_recovery_sublevels=params["mid_recovery_sublevels"],
        profit_reinvest_fraction=1.0,
        runner_fraction=0.0,
        fee_bps=10.0,
        slippage_bps=5.0,
        rolling_range=RollingRangePolicy(
            lookback_candles=GRID_LOOKBACK,
            min_history_candles=GRID_LOOKBACK,
            refresh_candles=30,
        ),
        liquidate_at_end=False,
    )


def analyze_equity(
    timestamps: pd.Series,
    values: pd.Series,
    initial_usdt: float,
) -> dict:
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(timestamps, utc=True),
            "equity": values.astype(float),
        }
    ).reset_index(drop=True)

    if frame.empty:
        raise RuntimeError("empty portfolio equity")

    synthetic_ts = frame.iloc[0]["timestamp"] - pd.Timedelta(days=1)
    augmented = pd.concat(
        [
            pd.DataFrame(
                [{"timestamp": synthetic_ts, "equity": float(initial_usdt)}]
            ),
            frame,
        ],
        ignore_index=True,
    )

    eq = augmented["equity"].astype(float)
    running_peak = eq.cummax()
    drawdown = eq / running_peak - 1.0

    dd_idx = int(drawdown.idxmin())
    peak_idx = int(eq.loc[:dd_idx].idxmax())
    min_idx = int(eq.idxmin())

    def date_label(idx: int) -> str:
        if idx == 0:
            return "INITIAL"
        return pd.Timestamp(augmented.loc[idx, "timestamp"]).isoformat()

    return {
        "final_equity_usdt": float(eq.iloc[-1]),
        "total_return": float(eq.iloc[-1] / initial_usdt - 1.0),
        "minimum_equity_usdt": float(eq.loc[min_idx]),
        "minimum_equity_date": date_label(min_idx),
        "minimum_vs_initial": float(eq.loc[min_idx] / initial_usdt - 1.0),
        "max_drawdown": float(drawdown.loc[dd_idx]),
        "max_drawdown_peak_equity_usdt": float(eq.loc[peak_idx]),
        "max_drawdown_peak_date": date_label(peak_idx),
        "max_drawdown_trough_equity_usdt": float(eq.loc[dd_idx]),
        "max_drawdown_trough_date": date_label(dd_idx),
    }


def run_grid_portfolio(
    *,
    candles_by_asset: dict[str, pd.DataFrame],
    universe_name: str,
    assets: tuple[str, ...],
    profile: str,
    evaluation_start: pd.Timestamp,
    initial_usdt: float,
    source_commit_sha: str,
) -> tuple[dict, pd.DataFrame, pd.DataFrame]:
    token_capital = initial_usdt / len(assets)
    curves: list[pd.DataFrame] = []
    token_rows: list[dict] = []

    for asset in assets:
        candles = candles_by_asset[asset]
        assert_grid_eligible(candles, asset, evaluation_start)

        cfg = build_grid_config(token_capital, profile)
        result = run_grid_backtest(
            candles,
            dataset_id=f"BINANCE:{asset}USDT:1D:GRID_VS_U10_V1",
            source_commit_sha=source_commit_sha,
            config=cfg,
            evaluation_start=evaluation_start,
        )

        curve = result.equity_curve[["timestamp", "total_equity"]].copy()
        curve["timestamp"] = pd.to_datetime(curve["timestamp"], utc=True)
        curve = curve.rename(columns={"total_equity": asset})
        curves.append(curve)

        final_equity = float(curve.iloc[-1][asset])
        token_return = final_equity / token_capital - 1.0
        summary = result.summary

        token_rows.append(
            {
                "universe": universe_name,
                "profile": profile,
                "asset": asset,
                "initial_usdt": token_capital,
                "final_equity_usdt": final_equity,
                "total_return": token_return,
                "max_drawdown": -abs(float(summary["max_drawdown"])),
                "closed_trade_count": int(summary["closed_trade_count"]),
                "open_micro_lots_end": int(summary["open_micro_lots_end"]),
                "open_mid_lots_end": int(summary["open_mid_lots_end"]),
            }
        )

    merged = curves[0]
    for curve in curves[1:]:
        merged = merged.merge(curve, on="timestamp", how="inner", validate="one_to_one")

    if merged.empty:
        raise RuntimeError(f"{universe_name}/{profile}: no common Grid equity dates")

    merged["portfolio_equity_usdt"] = merged[list(assets)].sum(axis=1)

    risk = analyze_equity(
        merged["timestamp"],
        merged["portfolio_equity_usdt"],
        initial_usdt,
    )

    token_df = pd.DataFrame(token_rows)
    portfolio_profit = risk["final_equity_usdt"] - initial_usdt
    token_df["profit_usdt"] = token_df["final_equity_usdt"] - token_df["initial_usdt"]
    if not math.isclose(portfolio_profit, 0.0, abs_tol=1e-12):
        token_df["profit_contribution_share"] = (
            token_df["profit_usdt"] / portfolio_profit
        )
    else:
        token_df["profit_contribution_share"] = float("nan")

    result = {
        "strategy": "GRID",
        "universe": universe_name,
        "assets": list(assets),
        "profile": profile,
        "start": pd.Timestamp(merged.iloc[0]["timestamp"]).isoformat(),
        "end": pd.Timestamp(merged.iloc[-1]["timestamp"]).isoformat(),
        "elapsed_days": int(
            (
                pd.Timestamp(merged.iloc[-1]["timestamp"])
                - pd.Timestamp(merged.iloc[0]["timestamp"])
            ).days
        ),
        "initial_usdt": initial_usdt,
        "token_count": len(assets),
        "token_initial_usdt": token_capital,
        **risk,
        "closed_trade_count": int(token_df["closed_trade_count"].sum()),
        "profitable_tokens": int((token_df["total_return"] > 0).sum()),
        "median_token_return": float(token_df["total_return"].median()),
        "worst_token_return": float(token_df["total_return"].min()),
        "best_token_return": float(token_df["total_return"].max()),
        "largest_profit_contributor": str(
            token_df.sort_values("profit_usdt", ascending=False).iloc[0]["asset"]
        ),
        "largest_profit_contribution_share": float(
            token_df.sort_values("profit_usdt", ascending=False).iloc[0][
                "profit_contribution_share"
            ]
        ),
    }
    return result, merged, token_df


def run_u10_comparison(
    panel: pd.DataFrame,
    events,
    evaluation_start: pd.Timestamp,
    initial_usdt: float,
) -> tuple[dict, pd.DataFrame]:
    result, _, equity = run_u10_window(
        panel,
        events,
        evaluation_start,
        initial_usdt,
        collect_ledger=False,
        shadow_cost=False,
    )

    risk = analyze_equity(
        pd.to_datetime(equity["timestamp"], utc=True),
        equity["equity_usdt"],
        initial_usdt,
    )

    out = {
        "strategy": "U10",
        "universe": "U10",
        "assets": list(U10),
        "profile": "RELATIVE_ROTATION_GRAPH_10_RESEARCH",
        "start": str(equity.iloc[0]["timestamp"]),
        "end": str(equity.iloc[-1]["timestamp"]),
        "elapsed_days": int(
            (
                utc(equity.iloc[-1]["timestamp"])
                - utc(equity.iloc[0]["timestamp"])
            ).days
        ),
        "initial_usdt": initial_usdt,
        **risk,
        "transitions": int(result["transitions"]),
        "route": result["route"],
    }
    return out, equity


def pct(value: float) -> str:
    return f"{100*value:+.2f}%"


def money(value: float) -> str:
    return f"{value:,.2f}"


def write_report(payload: dict, run_dir: Path) -> None:
    primary_u10 = payload["primary_u10"]
    primary_grid = payload["primary_grid"]
    exact = payload["exact_u10_diagnostic"]

    lines = [
        "# Grid vs U10 Universe Portfolios v1",
        "",
        "Mode: STRESS_TEST_ONLY",
        "Live/paper strategy logic: UNCHANGED",
        "",
        "## Primary common window",
        "",
        f"{pd.Timestamp(primary_u10['start']).date()} -> {pd.Timestamp(primary_u10['end']).date()}",
        "",
        "Starting capital for every portfolio: 10,000 USDT.",
        "",
        "Grid portfolio = equal initial allocation across tokens; each token runs an independent 50/50 Micro/Mid Grid sleeve.",
        "",
        "| Strategy / universe | Profile | Final equity | Return | Min equity | Min vs initial | Max DD | Activity |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
        f"| U10 | rotation | {money(primary_u10['final_equity_usdt'])} | {pct(primary_u10['total_return'])} | {money(primary_u10['minimum_equity_usdt'])} | {pct(primary_u10['minimum_vs_initial'])} | {pct(primary_u10['max_drawdown'])} | {primary_u10['transitions']} transitions |",
    ]

    for row in primary_grid:
        lines.append(
            f"| {row['universe']} | {row['profile']} | "
            f"{money(row['final_equity_usdt'])} | {pct(row['total_return'])} | "
            f"{money(row['minimum_equity_usdt'])} | {pct(row['minimum_vs_initial'])} | "
            f"{pct(row['max_drawdown'])} | {row['closed_trade_count']} closed lots |"
        )

    lines += [
        "",
        "## Grid universe comparison",
        "",
        "| Universe | Profile | Profitable tokens | Median token return | Worst token | Best token | Largest profit contributor |",
        "|---|---|---:|---:|---:|---:|---|",
    ]
    for row in primary_grid:
        lines.append(
            f"| {row['universe']} | {row['profile']} | "
            f"{row['profitable_tokens']}/{row['token_count']} | "
            f"{pct(row['median_token_return'])} | {pct(row['worst_token_return'])} | "
            f"{pct(row['best_token_return'])} | {row['largest_profit_contributor']} |"
        )

    lines += [
        "",
        "## Exact U10-token Grid diagnostic",
        "",
        f"Exact ten-token Grid start: {pd.Timestamp(exact['start']).date()}",
        f"Exact diagnostic end: {pd.Timestamp(exact['end']).date()}",
        f"Elapsed days: {exact['elapsed_days']}",
        "",
        "| Strategy | Profile | Final equity | Return | Min equity | Max DD |",
        "|---|---|---:|---:|---:|---:|",
        f"| U10 exact-window | rotation | {money(exact['u10']['final_equity_usdt'])} | {pct(exact['u10']['total_return'])} | {money(exact['u10']['minimum_equity_usdt'])} | {pct(exact['u10']['max_drawdown'])} |",
    ]
    for row in exact["grid"]:
        lines.append(
            f"| Grid exact U10 | {row['profile']} | {money(row['final_equity_usdt'])} | "
            f"{pct(row['total_return'])} | {money(row['minimum_equity_usdt'])} | "
            f"{pct(row['max_drawdown'])} |"
        )

    lines += [
        "",
        "## Interpretation boundaries",
        "",
        "- U10 concentrates capital in one selected asset; Grid diversifies capital across independent token sleeves.",
        "- Grid universes Tier A and Tier A+B were selected using earlier development evidence; they are not untouched OOS universes.",
        "- Full 15 is survivor-conditioned and therefore still exposed to survivorship bias.",
        "- U10 and Grid use different frozen friction models: U10 uses 0.1% per rotation; Grid uses 10 bps fee plus 5 bps adverse slippage on fills.",
        "- Exact-U10 Grid is a short diagnostic because TWT/PEPE cannot satisfy the 1095-day Grid prehistory until much later than the primary comparison start.",
        "- No result here authorizes a live promotion.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    ]

    (run_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff", default="2026-09-27T00:00:00Z")
    args = parser.parse_args()

    cutoff = utc(args.cutoff)
    sha = source_sha()

    # Frozen U10 market panel and monitor state.
    u10_panel, u10_meta = download_u10_panel(cutoff)
    u10_events = build_u10_events(u10_panel)
    u10_mature_start = utc(u10_panel.iloc[U10_LOOKBACK - 1]["timestamp"])
    if u10_mature_start != PRIMARY_START:
        raise AssertionError(
            f"expected U10 mature start {PRIMARY_START}, got {u10_mature_start}"
        )

    primary_u10, primary_u10_equity = run_u10_comparison(
        u10_panel, u10_events, PRIMARY_START, INITIAL_USDT
    )

    all_grid_assets = sorted(
        set(asset for assets in GRID_UNIVERSES.values() for asset in assets).union(U10)
    )

    client = BinanceSpotRestClient()
    candles_by_asset: dict[str, pd.DataFrame] = {}
    grid_meta: dict[str, dict] = {}

    for asset in all_grid_assets:
        candles, meta = download_grid_candles(client, asset, cutoff)
        candles_by_asset[asset] = candles
        grid_meta[asset] = meta

    # Primary eligibility must be strict.
    for universe_name, assets in GRID_UNIVERSES.items():
        for asset in assets:
            assert_grid_eligible(candles_by_asset[asset], asset, PRIMARY_START)

    primary_grid_results = []
    primary_grid_curves = []
    primary_token_tables = []

    for universe_name, assets in GRID_UNIVERSES.items():
        for profile in ("BASE", "WIDE"):
            result, curve, tokens = run_grid_portfolio(
                candles_by_asset=candles_by_asset,
                universe_name=universe_name,
                assets=assets,
                profile=profile,
                evaluation_start=PRIMARY_START,
                initial_usdt=INITIAL_USDT,
                source_commit_sha=sha,
            )
            primary_grid_results.append(result)

            curve_out = curve[["timestamp", "portfolio_equity_usdt"]].copy()
            curve_out.insert(0, "profile", profile)
            curve_out.insert(0, "universe", universe_name)
            primary_grid_curves.append(curve_out)

            primary_token_tables.append(tokens)

    # Exact-U10 short diagnostic: wait until every U10 token has 1095 prior candles.
    eligible_dates = {
        asset: first_grid_eligible_date(candles_by_asset[asset], asset)
        for asset in U10
    }
    exact_start = max(eligible_dates.values())

    if exact_start >= cutoff:
        raise RuntimeError(
            f"exact U10 Grid eligible start {exact_start} is not before cutoff {cutoff}"
        )

    exact_u10, exact_u10_equity = run_u10_comparison(
        u10_panel, u10_events, exact_start, INITIAL_USDT
    )

    exact_grid_results = []
    exact_grid_curves = []
    exact_token_tables = []

    for profile in ("BASE", "WIDE"):
        result, curve, tokens = run_grid_portfolio(
            candles_by_asset=candles_by_asset,
            universe_name="GRID_EXACT_U10_10",
            assets=tuple(U10),
            profile=profile,
            evaluation_start=exact_start,
            initial_usdt=INITIAL_USDT,
            source_commit_sha=sha,
        )
        exact_grid_results.append(result)

        curve_out = curve[["timestamp", "portfolio_equity_usdt"]].copy()
        curve_out.insert(0, "profile", profile)
        curve_out.insert(0, "universe", "GRID_EXACT_U10_10")
        exact_grid_curves.append(curve_out)

        exact_token_tables.append(tokens)

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(primary_grid_results).to_csv(
        run_dir / "primary_grid_portfolios.csv", index=False
    )
    pd.concat(primary_grid_curves, ignore_index=True).to_csv(
        run_dir / "primary_grid_equity.csv", index=False
    )
    pd.concat(primary_token_tables, ignore_index=True).to_csv(
        run_dir / "primary_grid_token_results.csv", index=False
    )

    primary_u10_equity.to_csv(run_dir / "primary_u10_equity.csv", index=False)

    pd.DataFrame(exact_grid_results).to_csv(
        run_dir / "exact_u10_grid_portfolios.csv", index=False
    )
    pd.concat(exact_grid_curves, ignore_index=True).to_csv(
        run_dir / "exact_u10_grid_equity.csv", index=False
    )
    pd.concat(exact_token_tables, ignore_index=True).to_csv(
        run_dir / "exact_u10_grid_token_results.csv", index=False
    )
    exact_u10_equity.to_csv(run_dir / "exact_u10_rotation_equity.csv", index=False)

    payload = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit": sha,
        "cutoff": cutoff.isoformat(),
        "initial_usdt": INITIAL_USDT,
        "primary_start": PRIMARY_START.isoformat(),
        "primary_u10": primary_u10,
        "primary_grid": primary_grid_results,
        "grid_universes": {
            name: list(assets) for name, assets in GRID_UNIVERSES.items()
        },
        "grid_profiles": PROFILES,
        "u10_data_metadata": u10_meta,
        "grid_data_metadata": grid_meta,
        "exact_u10_diagnostic": {
            "start": exact_start.isoformat(),
            "end": exact_u10["end"],
            "elapsed_days": exact_u10["elapsed_days"],
            "per_token_grid_eligible_dates": {
                asset: date.isoformat() for asset, date in eligible_dates.items()
            },
            "u10": exact_u10,
            "grid": exact_grid_results,
        },
    }

    (run_dir / "results.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False),
        encoding="utf-8",
    )
    write_report(payload, run_dir)

    print("run_dir=" + str(run_dir))
    print(
        "primary_u10 final=%.2f return=%.6f dd=%.6f transitions=%d"
        % (
            primary_u10["final_equity_usdt"],
            primary_u10["total_return"],
            primary_u10["max_drawdown"],
            primary_u10["transitions"],
        )
    )
    for row in primary_grid_results:
        print(
            "%s %s final=%.2f return=%.6f dd=%.6f profitable=%d/%d trades=%d"
            % (
                row["universe"],
                row["profile"],
                row["final_equity_usdt"],
                row["total_return"],
                row["max_drawdown"],
                row["profitable_tokens"],
                row["token_count"],
                row["closed_trade_count"],
            )
        )

    print("exact_u10_start=" + exact_start.isoformat())
    print(
        "exact_u10 final=%.2f return=%.6f dd=%.6f"
        % (
            exact_u10["final_equity_usdt"],
            exact_u10["total_return"],
            exact_u10["max_drawdown"],
        )
    )
    for row in exact_grid_results:
        print(
            "exact_grid %s final=%.2f return=%.6f dd=%.6f"
            % (
                row["profile"],
                row["final_equity_usdt"],
                row["total_return"],
                row["max_drawdown"],
            )
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
