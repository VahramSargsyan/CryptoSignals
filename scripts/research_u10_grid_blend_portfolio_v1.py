from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path

import pandas as pd

from integrations.binance.rest_client import BinanceSpotRestClient
from scripts.research_grid_vs_u10_universe_portfolios_v1 import (
    GRID_UNIVERSES,
    INITIAL_USDT,
    PRIMARY_START,
    analyze_equity,
    build_u10_events,
    download_grid_candles,
    download_u10_panel,
    run_grid_portfolio,
    run_u10_comparison,
    utc,
)

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "u10_grid_blend_portfolio_v1"

GRID_UNIVERSE_NAMES = (
    "GRID_TIER_A_5",
    "GRID_TIER_A_B_10",
    "GRID_FULL_15",
)
U10_WEIGHTS = (0.80, 0.70, 0.60, 0.50)
GRID_PROFILE = "BASE"


def source_sha() -> str:
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def normalize_curve(
    timestamps: pd.Series,
    values: pd.Series,
    initial_usdt: float,
    value_name: str,
) -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(timestamps, utc=True),
            value_name: values.astype(float),
        }
    ).sort_values("timestamp", kind="stable").reset_index(drop=True)

    if frame.empty:
        raise RuntimeError("empty equity curve")

    first_ts = pd.Timestamp(frame.iloc[0]["timestamp"])
    synthetic = pd.DataFrame(
        [{
            "timestamp": first_ts - pd.Timedelta(days=1),
            value_name: float(initial_usdt),
        }]
    )
    return pd.concat([synthetic, frame], ignore_index=True)


def sleeve_relationships(
    u10_curve: pd.DataFrame,
    grid_curve: pd.DataFrame,
) -> dict:
    merged = u10_curve.merge(
        grid_curve,
        on="timestamp",
        how="inner",
        validate="one_to_one",
    )
    if len(merged) < 3:
        raise RuntimeError("insufficient common equity rows")

    u10_ret = merged["u10_equity"].pct_change()
    grid_ret = merged["grid_equity"].pct_change()
    correlation = float(u10_ret.corr(grid_ret))

    u10_peak = merged["u10_equity"].cummax()
    grid_peak = merged["grid_equity"].cummax()
    u10_dd = merged["u10_equity"] < u10_peak
    grid_dd = merged["grid_equity"] < grid_peak

    comparable = merged.index >= 1
    both = (u10_dd & grid_dd & comparable).sum()
    either = ((u10_dd | grid_dd) & comparable).sum()
    days = int(comparable.sum())

    return {
        "daily_close_return_correlation": correlation,
        "both_in_drawdown_days": int(both),
        "either_in_drawdown_days": int(either),
        "common_days": days,
        "both_in_drawdown_fraction": float(both / days) if days else None,
        "conditional_overlap_fraction": float(both / either) if either else None,
    }


def value_on_or_before(
    curve: pd.DataFrame,
    date: pd.Timestamp,
    value_col: str,
) -> dict:
    ts = utc(date)
    sub = curve[curve["timestamp"] <= ts]
    if sub.empty:
        return {"date": None, "equity_usdt": None}
    row = sub.iloc[-1]
    return {
        "date": pd.Timestamp(row["timestamp"]).isoformat(),
        "equity_usdt": float(row[value_col]),
    }


def build_blend(
    *,
    u10_curve_10k: pd.DataFrame,
    grid_curve_10k: pd.DataFrame,
    u10_weight: float,
    initial_usdt: float,
    u10_control: dict,
    grid_control: dict,
    relationships: dict,
) -> tuple[dict, pd.DataFrame]:
    grid_weight = 1.0 - u10_weight

    merged = u10_curve_10k.merge(
        grid_curve_10k,
        on="timestamp",
        how="inner",
        validate="one_to_one",
    )
    if merged.empty:
        raise RuntimeError("blend has no common daily equity")

    merged["u10_weight"] = u10_weight
    merged["grid_weight"] = grid_weight
    merged["u10_sleeve_equity_usdt"] = (
        merged["u10_equity"] * u10_weight
    )
    merged["grid_sleeve_equity_usdt"] = (
        merged["grid_equity"] * grid_weight
    )
    merged["blend_equity_usdt"] = (
        merged["u10_sleeve_equity_usdt"]
        + merged["grid_sleeve_equity_usdt"]
    )

    risk = analyze_equity(
        merged.loc[merged.index >= 1, "timestamp"],
        merged.loc[merged.index >= 1, "blend_equity_usdt"],
        initial_usdt,
    )

    final_equity = risk["final_equity_usdt"]
    retained = final_equity / u10_control["final_equity_usdt"]
    dd_reduction_pp = (
        abs(u10_control["max_drawdown"]) - abs(risk["max_drawdown"])
    ) * 100.0

    u10_trough_2024 = value_on_or_before(
        merged,
        pd.Timestamp("2024-09-07", tz="UTC"),
        "blend_equity_usdt",
    )
    u10_trough_2026 = value_on_or_before(
        merged,
        pd.Timestamp("2026-06-06", tz="UTC"),
        "blend_equity_usdt",
    )

    result = {
        "u10_weight": u10_weight,
        "grid_weight": grid_weight,
        "initial_usdt": initial_usdt,
        **risk,
        "terminal_equity_retained_vs_u10": retained,
        "terminal_equity_delta_vs_u10_usdt": (
            final_equity - u10_control["final_equity_usdt"]
        ),
        "max_drawdown_reduction_pp_vs_u10": dd_reduction_pp,
        "max_drawdown_ratio_vs_u10": (
            abs(risk["max_drawdown"]) / abs(u10_control["max_drawdown"])
        ),
        "terminal_equity_vs_grid_ratio": (
            final_equity / grid_control["final_equity_usdt"]
        ),
        "u10_reference_2024_09_07": u10_trough_2024,
        "u10_reference_2026_06_06": u10_trough_2026,
        **relationships,
    }
    return result, merged


def pct(value: float) -> str:
    return f"{100 * value:+.2f}%"


def money(value: float) -> str:
    return f"{value:,.2f}"


def write_report(payload: dict, run_dir: Path) -> None:
    u10 = payload["u10_control"]
    grid_controls = payload["grid_controls"]
    blends = payload["blends"]

    lines = [
        "# U10 + Grid Blend Portfolio v1",
        "",
        "Mode: STRESS_TEST_ONLY",
        "Live/paper strategy logic: UNCHANGED",
        "",
        "## Construction",
        "",
        "Start: 10,000 USDT.",
        "",
        "The portfolio is split once at the beginning. U10 and Grid then compound independently with no rebalancing and no transfers between sleeves.",
        "",
        "Combined daily equity = U10 sleeve equity + Grid sleeve equity.",
        "",
        f"Evaluation: {pd.Timestamp(payload['start']).date()} -> {pd.Timestamp(payload['end']).date()}",
        "",
        "## Controls",
        "",
        "| Control | Final equity | Return | Max DD |",
        "|---|---:|---:|---:|",
        f"| 100% U10 | {money(u10['final_equity_usdt'])} | {pct(u10['total_return'])} | {pct(u10['max_drawdown'])} |",
    ]

    for name, grid in grid_controls.items():
        lines.append(
            f"| 100% {name} BASE | {money(grid['final_equity_usdt'])} | "
            f"{pct(grid['total_return'])} | {pct(grid['max_drawdown'])} |"
        )

    lines += [
        "",
        "## Fixed-split blends",
        "",
        "| Grid sleeve | U10 / Grid | Final equity | Return | Max DD | DD reduction vs U10 | Terminal retained vs U10 | Daily return corr |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]

    for row in blends:
        lines.append(
            f"| {row['grid_universe']} | "
            f"{100*row['u10_weight']:.0f}/{100*row['grid_weight']:.0f} | "
            f"{money(row['final_equity_usdt'])} | {pct(row['total_return'])} | "
            f"{pct(row['max_drawdown'])} | "
            f"{row['max_drawdown_reduction_pp_vs_u10']:+.2f} pp | "
            f"{100*row['terminal_equity_retained_vs_u10']:.2f}% | "
            f"{row['daily_close_return_correlation']:+.3f} |"
        )

    best_dd = max(blends, key=lambda x: x["max_drawdown"])
    best_equity = max(blends, key=lambda x: x["final_equity_usdt"])

    lines += [
        "",
        "## Descriptive extremes",
        "",
        f"- Highest blend terminal equity: {best_equity['grid_universe']} "
        f"{100*best_equity['u10_weight']:.0f}/{100*best_equity['grid_weight']:.0f} -> "
        f"{money(best_equity['final_equity_usdt'])} USDT.",
        f"- Shallowest blend max DD: {best_dd['grid_universe']} "
        f"{100*best_dd['u10_weight']:.0f}/{100*best_dd['grid_weight']:.0f} -> "
        f"{pct(best_dd['max_drawdown'])}.",
        "",
        "These are descriptive results on one historical path, not an optimization decision.",
        "",
        "## Interpretation boundaries",
        "",
        "- No rebalancing is used; the result is not receiving an artificial periodic buy-low/sell-high effect.",
        "- Tier A and Tier A+B Grid universes remain development-selected.",
        "- Full 15 remains survivor-conditioned.",
        "- Grid and U10 preserve their different existing friction models.",
        "- A lower blend drawdown does not prove future diversification.",
        "- No live allocation change is authorized.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    ]

    (run_dir / "report.md").write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff", default="2026-09-27T00:00:00Z")
    args = parser.parse_args()

    cutoff = utc(args.cutoff)
    sha = source_sha()

    u10_panel, _ = download_u10_panel(cutoff)
    u10_events = build_u10_events(u10_panel)
    u10_control, u10_eq = run_u10_comparison(
        u10_panel, u10_events, PRIMARY_START, INITIAL_USDT
    )

    u10_curve = normalize_curve(
        pd.to_datetime(u10_eq["timestamp"], utc=True),
        u10_eq["equity_usdt"],
        INITIAL_USDT,
        "u10_equity",
    )

    all_grid_assets = sorted(
        set(
            asset
            for name in GRID_UNIVERSE_NAMES
            for asset in GRID_UNIVERSES[name]
        )
    )
    client = BinanceSpotRestClient()
    candles_by_asset = {}
    for asset in all_grid_assets:
        candles, _ = download_grid_candles(client, asset, cutoff)
        candles_by_asset[asset] = candles

    grid_controls = {}
    grid_curves = {}
    grid_token_results = []

    for universe_name in GRID_UNIVERSE_NAMES:
        grid_result, grid_eq, token_df = run_grid_portfolio(
            candles_by_asset=candles_by_asset,
            universe_name=universe_name,
            assets=GRID_UNIVERSES[universe_name],
            profile=GRID_PROFILE,
            evaluation_start=PRIMARY_START,
            initial_usdt=INITIAL_USDT,
            source_commit_sha=sha,
        )
        grid_controls[universe_name] = grid_result

        curve = normalize_curve(
            grid_eq["timestamp"],
            grid_eq["portfolio_equity_usdt"],
            INITIAL_USDT,
            "grid_equity",
        )
        grid_curves[universe_name] = curve
        grid_token_results.append(token_df)

    blend_results = []
    blend_curves = []

    for universe_name in GRID_UNIVERSE_NAMES:
        grid_curve = grid_curves[universe_name]
        relationships = sleeve_relationships(u10_curve, grid_curve)

        for u10_weight in U10_WEIGHTS:
            result, curve = build_blend(
                u10_curve_10k=u10_curve,
                grid_curve_10k=grid_curve,
                u10_weight=u10_weight,
                initial_usdt=INITIAL_USDT,
                u10_control=u10_control,
                grid_control=grid_controls[universe_name],
                relationships=relationships,
            )
            result["grid_universe"] = universe_name
            result["grid_profile"] = GRID_PROFILE
            blend_results.append(result)

            out = curve[
                [
                    "timestamp",
                    "u10_sleeve_equity_usdt",
                    "grid_sleeve_equity_usdt",
                    "blend_equity_usdt",
                ]
            ].copy()
            out.insert(0, "grid_weight", 1.0 - u10_weight)
            out.insert(0, "u10_weight", u10_weight)
            out.insert(0, "grid_universe", universe_name)
            blend_curves.append(out)

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(blend_results).to_csv(
        run_dir / "blend_summary.csv",
        index=False,
    )
    pd.concat(blend_curves, ignore_index=True).to_csv(
        run_dir / "blend_equity.csv",
        index=False,
    )
    pd.DataFrame(list(grid_controls.values())).to_csv(
        run_dir / "grid_controls.csv",
        index=False,
    )
    pd.concat(grid_token_results, ignore_index=True).to_csv(
        run_dir / "grid_token_results.csv",
        index=False,
    )
    u10_eq.to_csv(run_dir / "u10_control_equity.csv", index=False)

    payload = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit": sha,
        "start": PRIMARY_START.isoformat(),
        "end": u10_control["end"],
        "initial_usdt": INITIAL_USDT,
        "u10_weights": list(U10_WEIGHTS),
        "grid_profile": GRID_PROFILE,
        "grid_universe_names": list(GRID_UNIVERSE_NAMES),
        "u10_control": u10_control,
        "grid_controls": grid_controls,
        "blends": blend_results,
    }

    (run_dir / "results.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False),
        encoding="utf-8",
    )
    write_report(payload, run_dir)

    print("run_dir=" + str(run_dir))
    print(
        "U10 control final=%.2f return=%.6f dd=%.6f"
        % (
            u10_control["final_equity_usdt"],
            u10_control["total_return"],
            u10_control["max_drawdown"],
        )
    )

    for universe_name in GRID_UNIVERSE_NAMES:
        grid = grid_controls[universe_name]
        print(
            "%s control final=%.2f return=%.6f dd=%.6f"
            % (
                universe_name,
                grid["final_equity_usdt"],
                grid["total_return"],
                grid["max_drawdown"],
            )
        )

    for row in sorted(
        blend_results,
        key=lambda x: (x["grid_universe"], -x["u10_weight"]),
    ):
        print(
            "%s %02d/%02d final=%.2f return=%.6f dd=%.6f retain=%.6f corr=%.4f overlap=%.4f"
            % (
                row["grid_universe"],
                int(round(100 * row["u10_weight"])),
                int(round(100 * row["grid_weight"])),
                row["final_equity_usdt"],
                row["total_return"],
                row["max_drawdown"],
                row["terminal_equity_retained_vs_u10"],
                row["daily_close_return_correlation"],
                row["both_in_drawdown_fraction"],
            )
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
