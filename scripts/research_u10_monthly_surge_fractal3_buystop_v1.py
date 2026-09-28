from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

import scripts.research_u10_monthly_surge_fractal_buystop_v1 as fractal
from scripts.research_u10_monthly_surge_pullback_v1 import (
    CANONICAL_U10,
    CASH_FRACTION,
    INITIAL_USDT,
    all_u10_universes,
    monthly_state,
    run_overlay,
    run_reference,
    source_sha,
    universe_key,
)
from scripts.research_u10_monthly_surge_trailing_reentry_peak_v1 import (
    run_overlay_trailing_peak,
)
from scripts.research_u10_last_year_vs_u8_universes_v1 import LOOKBACK, utc

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "u10_monthly_surge_fractal3_buystop_v1"

P1 = {"name": "P1", "surge": 1.00, "pullback": 0.05}
P2 = {"name": "P2", "surge": 1.00, "pullback": 0.10}


def run_fractal_with_shape(
    left: int,
    right: int,
    panel_eval: pd.DataFrame,
    reference: pd.DataFrame,
    anchors: dict[str, float],
    *,
    surge_threshold: float,
    cashout_pullback: float,
):
    old_left = fractal.PIVOT_LEFT
    old_right = fractal.PIVOT_RIGHT
    try:
        fractal.PIVOT_LEFT = left
        fractal.PIVOT_RIGHT = right
        return fractal.run_fractal_overlay(
            panel_eval,
            reference,
            anchors,
            surge_threshold=surge_threshold,
            cashout_pullback=cashout_pullback,
            initial_usdt=INITIAL_USDT,
        )
    finally:
        fractal.PIVOT_LEFT = old_left
        fractal.PIVOT_RIGHT = old_right


def summarize_alts(df: pd.DataFrame, cycles: pd.DataFrame) -> dict:
    alt = df[~df["is_canonical"]].copy()
    alt_cycles = cycles[cycles["is_canonical"] == False].copy() if not cycles.empty else pd.DataFrame()

    out = {
        "count": int(len(alt)),
        "final_gt_baseline_rate": float((alt["f3_delta_vs_baseline_usdt"] > 0).mean()),
        "dd_better_baseline_rate": float((alt["f3_dd_improvement_pp"] > 0).mean()),
        "both_better_baseline_rate": float(
            (
                (alt["f3_delta_vs_baseline_usdt"] > 0)
                & (alt["f3_dd_improvement_pp"] > 0)
            ).mean()
        ),
        "f3_gt_f5_rate": float((alt["f3_delta_vs_f5_usdt"] > 0).mean()),
        "f3_gt_fixed25_rate": float((alt["f3_delta_vs_fixed25_usdt"] > 0).mean()),
        "f3_gt_trailing25_rate": float((alt["f3_delta_vs_trailing25_usdt"] > 0).mean()),
        "unfinished_rate": float((alt["f3_unfinished_cycles"] > 0).mean()),
        "median_delta_vs_baseline_pct": float(alt["f3_delta_vs_baseline_pct"].median()),
        "q25_delta_vs_baseline_pct": float(alt["f3_delta_vs_baseline_pct"].quantile(0.25)),
        "q75_delta_vs_baseline_pct": float(alt["f3_delta_vs_baseline_pct"].quantile(0.75)),
        "median_delta_vs_f5_pct": float(alt["f3_delta_vs_f5_pct"].median()),
        "q25_delta_vs_f5_pct": float(alt["f3_delta_vs_f5_pct"].quantile(0.25)),
        "q75_delta_vs_f5_pct": float(alt["f3_delta_vs_f5_pct"].quantile(0.75)),
        "median_days_in_cash": float(alt["f3_days_in_cash"].median()),
        "median_reentries": float(alt["f3_reentries"].median()),
    }

    if not alt_cycles.empty:
        completed = alt_cycles[alt_cycles["reentry_execution_date"].notna()].copy()
        if not completed.empty:
            depths = completed["reference_drawdown_at_reentry_close"].astype(float)
            durations = completed["cash_cycle_days"].astype(float)
            out.update(
                {
                    "cycle_count": int(len(completed)),
                    "reentry_depth_min": float(depths.min()),
                    "reentry_depth_q10": float(depths.quantile(0.10)),
                    "reentry_depth_q25": float(depths.quantile(0.25)),
                    "reentry_depth_median": float(depths.median()),
                    "reentry_depth_q75": float(depths.quantile(0.75)),
                    "reentry_depth_max": float(depths.max()),
                    "cash_days_min": float(durations.min()),
                    "cash_days_q25": float(durations.quantile(0.25)),
                    "cash_days_median": float(durations.median()),
                    "cash_days_q75": float(durations.quantile(0.75)),
                    "cash_days_max": float(durations.max()),
                }
            )
    return out


def pct(x: float) -> str:
    return f"{100*x:+.2f}%"


def write_report(payload: dict, run_dir: Path) -> None:
    lines = [
        "# U10 Monthly Surge 3-Bar Fractal Buy-Stop v1",
        "",
        "Mode: STRESS_TEST_ONLY",
        "Live/paper logic: UNCHANGED",
        "",
        "Only change versus the prior fractal study: pivot confirmation is 1-left / center / 1-right instead of 2-left / center / 2-right.",
        "",
        "## Canonical U10",
        "",
        "| Variant | Baseline | Fixed -25% | Moving -25% | 5-bar | 3-bar | 3-bar vs 5-bar | Max DD 3-bar | Cash days 3-bar |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for label in ("P1", "P2"):
        row = payload["canonical"][label]
        lines.append(
            f"| {label} | {row['baseline_final_equity_usdt']:,.2f} | "
            f"{row['fixed25_final_equity_usdt']:,.2f} | "
            f"{row['trailing25_final_equity_usdt']:,.2f} | "
            f"{row['f5_final_equity_usdt']:,.2f} | "
            f"{row['f3_final_equity_usdt']:,.2f} | "
            f"{pct(row['f3_delta_vs_f5_pct'])} | "
            f"{pct(row['f3_max_drawdown'])} | "
            f"{row['f3_days_in_cash']} |"
        )

    lines += [
        "",
        "## 791 alternative U10s",
        "",
        "| Variant | Final > baseline | DD better | Both better | 3-bar > 5-bar | 3-bar > moving -25 | 3-bar > fixed -25 | Unfinished | Median delta vs baseline | Median delta vs 5-bar |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for label in ("P1", "P2"):
        s = payload["alternative_summary"][label]
        lines.append(
            f"| {label} | {100*s['final_gt_baseline_rate']:.2f}% | "
            f"{100*s['dd_better_baseline_rate']:.2f}% | "
            f"{100*s['both_better_baseline_rate']:.2f}% | "
            f"{100*s['f3_gt_f5_rate']:.2f}% | "
            f"{100*s['f3_gt_trailing25_rate']:.2f}% | "
            f"{100*s['f3_gt_fixed25_rate']:.2f}% | "
            f"{100*s['unfinished_rate']:.2f}% | "
            f"{pct(s['median_delta_vs_baseline_pct'])} | "
            f"{pct(s['median_delta_vs_f5_pct'])} |"
        )

    lines += [
        "",
        "## Guardrail",
        "",
        "- No parameter sweep was performed.",
        "- 15% search activation remained unchanged.",
        "- Cash-out and portfolio rules remained unchanged.",
        "- 2020-2022 was not used to choose the 3-bar rule.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    ]

    (run_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff", default="2026-09-27T00:00:00Z")
    args = parser.parse_args()

    panel, metadata = fractal.download_ohlc_panel(utc(args.cutoff))
    events = fractal.build_events(panel)
    mature_start = utc(panel.iloc[LOOKBACK - 1]["timestamp"])
    panel_eval = panel[panel["timestamp"] >= mature_start].copy().reset_index(drop=True)

    canonical_key = universe_key(CANONICAL_U10)
    rows = []
    cycle_frames = []
    canonical = {}

    for idx, assets in enumerate(all_u10_universes(), start=1):
        key = universe_key(assets)
        is_canonical = key == canonical_key

        baseline, reference = run_reference(
            panel, events, assets, mature_start, INITIAL_USDT
        )
        reference = reference.reset_index(drop=True)
        if len(reference) != len(panel_eval):
            raise RuntimeError("reference/panel mismatch")

        _, anchors = monthly_state(reference, INITIAL_USDT)

        for label, cfg in (("P1", P1), ("P2", P2)):
            fixed25, _ = run_overlay(
                reference,
                anchors,
                surge_threshold=cfg["surge"],
                pullback_threshold=cfg["pullback"],
                reentry_threshold=0.25,
                cash_fraction=CASH_FRACTION,
                initial_usdt=INITIAL_USDT,
            )
            trailing25, _ = run_overlay_trailing_peak(
                reference,
                anchors,
                surge_threshold=cfg["surge"],
                pullback_threshold=cfg["pullback"],
                reentry_threshold=0.25,
                cash_fraction=CASH_FRACTION,
                initial_usdt=INITIAL_USDT,
            )
            f5, _ = run_fractal_with_shape(
                2, 2, panel_eval, reference, anchors,
                surge_threshold=cfg["surge"],
                cashout_pullback=cfg["pullback"],
            )
            f3, cycles = run_fractal_with_shape(
                1, 1, panel_eval, reference, anchors,
                surge_threshold=cfg["surge"],
                cashout_pullback=cfg["pullback"],
            )

            row = {
                "universe_key": key,
                "variant": label,
                "is_canonical": is_canonical,
                "baseline_final_equity_usdt": baseline["final_equity_usdt"],
                "baseline_max_drawdown": baseline["max_drawdown"],
                "fixed25_final_equity_usdt": fixed25["final_equity_usdt"],
                "trailing25_final_equity_usdt": trailing25["final_equity_usdt"],
                "f5_final_equity_usdt": f5["final_equity_usdt"],
                "f5_max_drawdown": f5["max_drawdown"],
                "f3_final_equity_usdt": f3["final_equity_usdt"],
                "f3_max_drawdown": f3["max_drawdown"],
                "f3_days_in_cash": f3["days_in_cash"],
                "f3_reentries": f3["reentries"],
                "f3_confirmed_pivots": f3["confirmed_pivots"],
                "f3_stop_replacements": f3["stop_replacements"],
                "f3_gap_open_fills": f3["gap_open_fills"],
                "f3_stop_price_fills": f3["stop_price_fills"],
                "f3_unfinished_cycles": f3["unfinished_cycles"],
                "f3_delta_vs_baseline_usdt": f3["final_equity_usdt"] - baseline["final_equity_usdt"],
                "f3_delta_vs_baseline_pct": f3["final_equity_usdt"] / baseline["final_equity_usdt"] - 1.0,
                "f3_dd_improvement_pp": (f3["max_drawdown"] - baseline["max_drawdown"]) * 100.0,
                "f3_delta_vs_f5_usdt": f3["final_equity_usdt"] - f5["final_equity_usdt"],
                "f3_delta_vs_f5_pct": f3["final_equity_usdt"] / f5["final_equity_usdt"] - 1.0,
                "f3_delta_vs_fixed25_usdt": f3["final_equity_usdt"] - fixed25["final_equity_usdt"],
                "f3_delta_vs_fixed25_pct": f3["final_equity_usdt"] / fixed25["final_equity_usdt"] - 1.0,
                "f3_delta_vs_trailing25_usdt": f3["final_equity_usdt"] - trailing25["final_equity_usdt"],
                "f3_delta_vs_trailing25_pct": f3["final_equity_usdt"] / trailing25["final_equity_usdt"] - 1.0,
            }
            rows.append(row)
            if is_canonical:
                canonical[label] = dict(row)

            if not cycles.empty:
                cc = cycles.copy()
                cc.insert(0, "is_canonical", is_canonical)
                cc.insert(0, "variant", label)
                cc.insert(0, "universe_key", key)
                cycle_frames.append(cc)

        if idx % 100 == 0:
            print(f"processed_universes={idx}")

    df = pd.DataFrame(rows)
    cycles_df = pd.concat(cycle_frames, ignore_index=True) if cycle_frames else pd.DataFrame()

    alt_summary = {
        label: summarize_alts(df[df["variant"] == label].copy(),
                              cycles_df[cycles_df["variant"] == label].copy() if not cycles_df.empty else pd.DataFrame())
        for label in ("P1", "P2")
    }

    canonical_cycles = (
        cycles_df[cycles_df["is_canonical"] == True].copy()
        if not cycles_df.empty else pd.DataFrame()
    )

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    df.to_csv(run_dir / "all_792_fractal3_comparison.csv", index=False)
    if not cycles_df.empty:
        cycles_df.to_csv(run_dir / "all_fractal3_cycles.csv", index=False)
    if not canonical_cycles.empty:
        canonical_cycles.to_csv(run_dir / "canonical_fractal3_cycles.csv", index=False)

    payload = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit": source_sha(),
        "data_metadata": metadata,
        "mature_start": mature_start.isoformat(),
        "parameters": {
            "surge_threshold": 1.0,
            "p1_cashout_pullback": 0.05,
            "p2_cashout_pullback": 0.10,
            "cash_fraction": CASH_FRACTION,
            "search_drawdown": fractal.SEARCH_DRAWDOWN,
            "pivot_left": 1,
            "pivot_right": 1,
        },
        "canonical": canonical,
        "alternative_summary": alt_summary,
        "canonical_cycles": canonical_cycles.to_dict(orient="records"),
    }

    (run_dir / "results.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False),
        encoding="utf-8",
    )
    write_report(payload, run_dir)

    print("run_dir=" + str(run_dir))
    for label in ("P1", "P2"):
        c = canonical[label]
        s = alt_summary[label]
        print(
            "%s CAN base=%.8f f5=%.8f f3=%.8f d5=%.8f dd=%.8f cash=%d re=%d unfinished=%d"
            % (
                label,
                c["baseline_final_equity_usdt"],
                c["f5_final_equity_usdt"],
                c["f3_final_equity_usdt"],
                c["f3_delta_vs_f5_pct"],
                c["f3_max_drawdown"],
                c["f3_days_in_cash"],
                c["f3_reentries"],
                c["f3_unfinished_cycles"],
            )
        )
        print(
            "%s ALT base=%.6f both=%.6f beat5=%.6f beattrail=%.6f beatfixed=%.6f unfinished=%.6f medbase=%.6f med5=%.6f"
            % (
                label,
                s["final_gt_baseline_rate"],
                s["both_better_baseline_rate"],
                s["f3_gt_f5_rate"],
                s["f3_gt_trailing25_rate"],
                s["f3_gt_fixed25_rate"],
                s["unfinished_rate"],
                s["median_delta_vs_baseline_pct"],
                s["median_delta_vs_f5_pct"],
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
