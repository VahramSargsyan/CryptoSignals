from __future__ import annotations

import argparse
import json
import os
import subprocess
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

from scripts.research_rr_u10_vs_u11_link_v1 import (
    U10,
    bounds,
    download_panel,
    pair_rows_for_day,
    route_for_day,
    utc,
)

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "rr_u10_binance_flexible_earn_v1"

LOOKBACK = 180
ARM = 0.15
REVERSAL = 0.03
DD_RATIO = 1.50
COST = 0.001

# Frozen APR assumptions for this first-pass sensitivity test.
# Nine assets come from the latest machine-readable Binance Flexible Earn
# snapshot available in research at 2026-04-21T13:04:50Z.
# TRX is overridden by the user's Binance screenshot on 2026-10-04: 3.15%.
APR = {
    "TWT": 0.00633661,
    "PEPE": 0.00143683,
    "BNB": 0.00045651,
    "TRX": 0.0315,
    "AAVE": 0.00189944,
    "AVAX": 0.00369051,
    "FIL": 0.00329868,
    "ALGO": 0.01301053,
    "XRP": 0.00043053,
    "HBAR": 0.00068730,
}
APR_SOURCE = {
    "TRX": "user Binance screenshot 2026-10-04, Flexible 3.15%",
    "others": "machine-readable Binance Flexible Earn snapshot updated 2026-04-21T13:04:50Z",
}


def source_sha():
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def simulate_with_earn(
    panel,
    timestamps,
    events,
    snapshots,
    start_i,
    end_i,
    start_asset,
):
    opens = {a: panel[a + "_open"].astype(float).to_numpy() for a in U10}
    closes = {a: panel[a + "_close"].astype(float).to_numpy() for a in U10}

    current = start_asset
    qty_base = 1.0 / float(opens[current][start_i])
    qty_earn = qty_base
    initial = qty_base * float(closes[current][start_i])

    exposure_days = Counter()
    transitions = 0
    pending = None

    base_equity = []
    earn_equity = []

    for i in range(start_i, end_i + 1):
        if pending is not None:
            base_value = qty_base * float(opens[current][i])
            earn_value = qty_earn * float(opens[current][i])
            current = pending["to_asset"]
            qty_base = base_value * (1.0 - COST) / float(opens[current][i])
            qty_earn = earn_value * (1.0 - COST) / float(opens[current][i])
            transitions += 1
            pending = None

        exposure_days[current] += 1

        # Approximation: one day of Flexible Earn accrual for each calendar
        # day the RR path holds the asset from open through close.
        qty_earn *= 1.0 + APR[current] / 365.0

        base_equity.append(qty_base * float(closes[current][i]))
        earn_equity.append(qty_earn * float(closes[current][i]))

        if i < end_i:
            pending = route_for_day(
                current, i, U10, timestamps, events, snapshots
            )

    base_arr = np.asarray(base_equity, dtype=float)
    earn_arr = np.asarray(earn_equity, dtype=float)
    days = int(end_i - start_i + 1)

    base_return = float(base_arr[-1] / initial - 1.0)
    earn_return = float(earn_arr[-1] / initial - 1.0)
    lift = float(earn_arr[-1] / base_arr[-1] - 1.0)
    annualized_lift = float((1.0 + lift) ** (365.0 / days) - 1.0)
    monthly_30d_lift = float((1.0 + annualized_lift) ** (30.0 / 365.0) - 1.0)

    weighted_apr = sum(
        exposure_days[a] * APR[a] for a in U10
    ) / days

    return {
        "start_asset": start_asset,
        "days": days,
        "transitions": transitions,
        "baseline_return": base_return,
        "earn_overlay_return": earn_return,
        "earn_lift_total": lift,
        "earn_lift_annualized": annualized_lift,
        "earn_lift_30d_equiv": monthly_30d_lift,
        "time_weighted_apr": float(weighted_apr),
        "exposure_days": dict(exposure_days),
    }


def summarize_window(rows):
    df = pd.DataFrame(
        [
            {
                k: v
                for k, v in row.items()
                if k != "exposure_days"
            }
            for row in rows
        ]
    )

    pooled = Counter()
    for row in rows:
        pooled.update(row["exposure_days"])
    total_asset_days = sum(pooled.values())

    allocation = []
    for asset in U10:
        days = int(pooled.get(asset, 0))
        share = days / total_asset_days if total_asset_days else 0.0
        allocation.append(
            {
                "asset": asset,
                "apr": APR[asset],
                "pooled_days": days,
                "pooled_share": share,
                "apr_contribution": share * APR[asset],
            }
        )

    allocation_df = pd.DataFrame(allocation).sort_values(
        ["pooled_share", "apr"], ascending=[False, False], kind="stable"
    )

    return df, allocation_df, {
        "start_count": int(len(df)),
        "median_baseline_return": float(df["baseline_return"].median()),
        "median_earn_overlay_return": float(df["earn_overlay_return"].median()),
        "median_total_earn_lift": float(df["earn_lift_total"].median()),
        "min_total_earn_lift": float(df["earn_lift_total"].min()),
        "max_total_earn_lift": float(df["earn_lift_total"].max()),
        "median_annualized_earn_lift": float(df["earn_lift_annualized"].median()),
        "median_30d_equivalent_lift": float(df["earn_lift_30d_equiv"].median()),
        "median_time_weighted_apr": float(df["time_weighted_apr"].median()),
        "pooled_time_weighted_apr": float(allocation_df["apr_contribution"].sum()),
        "median_transitions": float(df["transitions"].median()),
    }


def pct(value, decimals=2):
    return f"{100.0 * value:+.{decimals}f}%"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff")
    args = parser.parse_args()

    cutoff = utc(args.cutoff) if args.cutoff else pd.Timestamp.now(tz="UTC").floor("D")
    panel, data_meta = download_panel(cutoff)
    timestamps, events, snapshots = pair_rows_for_day(panel, U10)

    end = timestamps[-1]
    windows = {
        "last_1y": (
            utc(end - pd.DateOffset(years=1) + pd.Timedelta(days=1)),
            end,
        ),
        "last_2y": (
            utc(end - pd.DateOffset(years=2) + pd.Timedelta(days=1)),
            end,
        ),
        "mature": (pd.Timestamp("2023-10-31", tz="UTC"), end),
    }

    summary = {
        "experiment": "RR_U10_BINANCE_FLEXIBLE_EARN_V1",
        "workflow_mode": "STRESS_TEST_ONLY",
        "source_commit_sha": source_sha(),
        "production_changes": "NONE",
        "apr": APR,
        "apr_source": APR_SOURCE,
        "assumptions": {
            "apr_constant_over_history": True,
            "daily_accrual_factor": "1 + APR/365 for each held calendar day",
            "all_rewards_reinvested_in_same_asset": True,
            "earn_does_not_change_rr_signals": True,
            "subscription_or_redemption_delay": 0,
            "bnb_extra_launchpool_hodler_rewards_included": False,
            "historical_binance_apr_series_used": False,
            "transition_cost": COST,
            "rr_rules": {
                "lookback_days": LOOKBACK,
                "arm": ARM,
                "reversal": REVERSAL,
                "destination_dominance_ratio": DD_RATIO,
            },
        },
        "data": {
            "panel_start": timestamps[0].isoformat(),
            "panel_end": end.isoformat(),
            "metadata": data_meta,
        },
        "windows": {},
    }

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    report_lines = [
        "# RR U10 + Binance Flexible Earn sensitivity V1",
        "",
        "Mode: STRESS_TEST_ONLY — production/live unchanged.",
        "",
        "This is a constant-APR sensitivity test, NOT a historical Binance APR backtest.",
        "",
        "## Frozen APR assumptions",
        "",
        "|Asset|APR|",
        "|---|---:|",
    ]
    for asset in sorted(U10, key=lambda a: APR[a], reverse=True):
        report_lines.append(f"|{asset}|{100*APR[asset]:.4f}%|")

    for name, (start, stop) in windows.items():
        start_i, end_i = bounds(timestamps, start, stop)
        rows = [
            simulate_with_earn(
                panel,
                timestamps,
                events,
                snapshots,
                start_i,
                end_i,
                start_asset,
            )
            for start_asset in U10
        ]
        details_df, allocation_df, metrics = summarize_window(rows)

        details_df.to_csv(run_dir / f"{name}_start_details.csv", index=False)
        allocation_df.to_csv(run_dir / f"{name}_asset_allocation.csv", index=False)

        summary["windows"][name] = {
            "start": start.isoformat(),
            "end": stop.isoformat(),
            "days": int(end_i - start_i + 1),
            "metrics": metrics,
            "asset_allocation": allocation_df.to_dict("records"),
        }

        report_lines += [
            "",
            f"## {name}",
            "",
            f"- Period: {start.date()} -> {stop.date()} ({end_i-start_i+1} days)",
            f"- Pure RR median return: {pct(metrics['median_baseline_return'],1)}",
            f"- RR + Earn median return: {pct(metrics['median_earn_overlay_return'],1)}",
            f"- Median multiplicative Earn lift over whole window: {pct(metrics['median_total_earn_lift'],2)}",
            f"- Median annualized Earn-only lift: {pct(metrics['median_annualized_earn_lift'],3)}",
            f"- 30-day equivalent Earn-only lift: {pct(metrics['median_30d_equivalent_lift'],3)}",
            f"- Median time-weighted APR from actual RR holdings: {pct(metrics['median_time_weighted_apr'],3)}",
            f"- Pooled time-weighted APR across 10 starts: {pct(metrics['pooled_time_weighted_apr'],3)}",
            "",
            "|Asset|Pooled holding share|APR|APR contribution|",
            "|---|---:|---:|---:|",
        ]
        for row in allocation_df.to_dict("records"):
            report_lines.append(
                f"|{row['asset']}|{100*row['pooled_share']:.2f}%|"
                f"{100*row['apr']:.4f}%|{100*row['apr_contribution']:.4f}%|"
            )

    report_lines += [
        "",
        "## Interpretation boundary",
        "",
        "- Current/frozen APRs were applied backward as a sensitivity layer; actual historical Binance Earn rates were not reconstructed.",
        "- TRX uses the user's 3.15% screenshot; other assets use the 2026-04-21 machine snapshot available to this research run.",
        "- BNB Launchpool/HODLer/Megadrop-style extra rewards are excluded.",
        "- Zero subscription/redemption delay is assumed.",
        "- No production configuration, Telegram logic, or trading behavior was changed.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_PRICE_DATA + FROZEN_APR_SENSITIVITY",
        "",
    ]

    (run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (run_dir / "report.md").write_text("\n".join(report_lines), encoding="utf-8")

    print(f"run_dir={run_dir}")
    for name in ("last_1y", "last_2y", "mature"):
        m = summary["windows"][name]["metrics"]
        print(
            name,
            f"weighted_apr={m['median_time_weighted_apr']:.6f}",
            f"annualized_lift={m['median_annualized_earn_lift']:.6f}",
            f"total_lift={m['median_total_earn_lift']:.6f}",
        )


if __name__ == "__main__":
    raise SystemExit(main())
