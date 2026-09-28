from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path

import pandas as pd

import scripts.research_u10_monthly_surge_pullback_v1 as base


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "u10_surge_p1_cash50_reentry25_v1"

SURGE = 1.00
PULLBACK = 0.05
REENTRY = 0.25
CONTROL_CASH = 0.30
CANDIDATE_CASH = 0.50


def source_sha() -> str:
    return os.environ.get("SOURCE_COMMIT_SHA") or subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def pct(x: float) -> str:
    return f"{100.0 * float(x):+.2f}%"


def summarize_alt(df: pd.DataFrame) -> dict:
    alt = df[~df["is_canonical"]].copy()
    return {
        "count": int(len(alt)),
        "candidate_gt_baseline_rate": float(
            (alt["candidate_delta_vs_baseline_usdt"] > 0).mean()
        ),
        "candidate_gt_control_rate": float(
            (alt["candidate_delta_vs_control_usdt"] > 0).mean()
        ),
        "candidate_eq_control_rate": float(
            (alt["candidate_delta_vs_control_usdt"].abs() < 1e-8).mean()
        ),
        "candidate_lt_control_rate": float(
            (alt["candidate_delta_vs_control_usdt"] < -1e-8).mean()
        ),
        "candidate_dd_better_control_rate": float(
            (alt["candidate_dd_vs_control_pp"] > 0).mean()
        ),
        "candidate_both_better_control_rate": float(
            (
                (alt["candidate_delta_vs_control_usdt"] > 0)
                & (alt["candidate_dd_vs_control_pp"] > 0)
            ).mean()
        ),
        "candidate_unfinished_rate": float(
            (alt["candidate_terminal_cash_usdt"] > 1e-8).mean()
        ),
        "median_candidate_delta_vs_control_pct": float(
            alt["candidate_delta_vs_control_pct"].median()
        ),
        "q25_candidate_delta_vs_control_pct": float(
            alt["candidate_delta_vs_control_pct"].quantile(0.25)
        ),
        "q75_candidate_delta_vs_control_pct": float(
            alt["candidate_delta_vs_control_pct"].quantile(0.75)
        ),
        "median_candidate_delta_vs_baseline_pct": float(
            alt["candidate_delta_vs_baseline_pct"].median()
        ),
        "median_candidate_dd_vs_control_pp": float(
            alt["candidate_dd_vs_control_pp"].median()
        ),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cutoff", default="2026-09-27T00:00:00Z")
    args = ap.parse_args()

    cutoff = base.utc(args.cutoff)
    panel, metadata = base.download_panel(cutoff)
    event_map = base.build_events(panel)
    mature_start = base.utc(panel.iloc[base.LOOKBACK - 1]["timestamp"])

    universes = base.all_u10_universes()
    if len(universes) != 792:
        raise AssertionError(f"expected 792 universes, got {len(universes)}")

    canonical_key = base.universe_key(base.CANONICAL_U10)
    rows = []
    control_cycles_all = []
    candidate_cycles_all = []
    canonical = None

    for idx, assets in enumerate(universes, start=1):
        key = base.universe_key(assets)
        is_canonical = key == canonical_key

        baseline, reference = base.run_reference(
            panel, event_map, assets, mature_start, base.INITIAL_USDT
        )
        _, anchors = base.monthly_state(reference, base.INITIAL_USDT)

        control, control_cycles = base.run_overlay(
            reference,
            anchors,
            surge_threshold=SURGE,
            pullback_threshold=PULLBACK,
            reentry_threshold=REENTRY,
            cash_fraction=CONTROL_CASH,
            initial_usdt=base.INITIAL_USDT,
        )
        candidate, candidate_cycles = base.run_overlay(
            reference,
            anchors,
            surge_threshold=SURGE,
            pullback_threshold=PULLBACK,
            reentry_threshold=REENTRY,
            cash_fraction=CANDIDATE_CASH,
            initial_usdt=base.INITIAL_USDT,
        )

        row = {
            "universe_key": key,
            "is_canonical": is_canonical,
            "baseline_final_equity_usdt": baseline["final_equity_usdt"],
            "baseline_max_drawdown": baseline["max_drawdown"],
            "control_final_equity_usdt": control["final_equity_usdt"],
            "control_max_drawdown": control["max_drawdown"],
            "control_cashouts": control["cashouts"],
            "control_reentries": control["reentries"],
            "control_days_in_cash": control["days_in_cash"],
            "control_terminal_cash_usdt": control["terminal_cash_usdt"],
            "control_cashout_fees_usdt": control["cashout_fees_usdt"],
            "control_reentry_fees_usdt": control["reentry_fees_usdt"],
            "candidate_final_equity_usdt": candidate["final_equity_usdt"],
            "candidate_max_drawdown": candidate["max_drawdown"],
            "candidate_cashouts": candidate["cashouts"],
            "candidate_reentries": candidate["reentries"],
            "candidate_days_in_cash": candidate["days_in_cash"],
            "candidate_terminal_cash_usdt": candidate["terminal_cash_usdt"],
            "candidate_cashout_fees_usdt": candidate["cashout_fees_usdt"],
            "candidate_reentry_fees_usdt": candidate["reentry_fees_usdt"],
            "candidate_delta_vs_control_usdt": (
                candidate["final_equity_usdt"] - control["final_equity_usdt"]
            ),
            "candidate_delta_vs_control_pct": (
                candidate["final_equity_usdt"] / control["final_equity_usdt"] - 1.0
            ),
            "candidate_delta_vs_baseline_usdt": (
                candidate["final_equity_usdt"] - baseline["final_equity_usdt"]
            ),
            "candidate_delta_vs_baseline_pct": (
                candidate["final_equity_usdt"] / baseline["final_equity_usdt"] - 1.0
            ),
            "candidate_dd_vs_control_pp": (
                candidate["max_drawdown"] - control["max_drawdown"]
            ) * 100.0,
            "candidate_dd_vs_baseline_pp": (
                candidate["max_drawdown"] - baseline["max_drawdown"]
            ) * 100.0,
        }
        rows.append(row)

        if is_canonical:
            canonical = dict(row)
            if not control_cycles.empty:
                x = control_cycles.copy()
                x.insert(0, "cash_fraction", CONTROL_CASH)
                x.insert(0, "variant", "CONTROL_CASH30")
                control_cycles_all.append(x)
            if not candidate_cycles.empty:
                x = candidate_cycles.copy()
                x.insert(0, "cash_fraction", CANDIDATE_CASH)
                x.insert(0, "variant", "CANDIDATE_CASH50")
                candidate_cycles_all.append(x)

        if idx % 100 == 0:
            print(f"processed={idx}", flush=True)

    df = pd.DataFrame(rows)
    if canonical is None:
        raise RuntimeError("canonical U10 not found")

    alt = summarize_alt(df)

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(run_dir / "all_792_cash30_vs_cash50.csv", index=False)

    cycles = []
    if control_cycles_all:
        cycles.extend(control_cycles_all)
    if candidate_cycles_all:
        cycles.extend(candidate_cycles_all)
    if cycles:
        pd.concat(cycles, ignore_index=True).to_csv(
            run_dir / "canonical_cycles_cash30_vs_cash50.csv",
            index=False,
        )

    payload = {
        "experiment": "U10_SURGE_P1_CASH50_REENTRY25_V1",
        "source_commit": source_sha(),
        "mature_start": mature_start.isoformat(),
        "parameters": {
            "surge_threshold": SURGE,
            "cashout_pullback": PULLBACK,
            "reentry_threshold": REENTRY,
            "control_cash_fraction": CONTROL_CASH,
            "candidate_cash_fraction": CANDIDATE_CASH,
            "transaction_cost": base.COST,
        },
        "canonical": canonical,
        "alternative_summary": alt,
        "data_metadata": metadata,
        "production_changes": "NONE",
        "test_level": "GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    }
    (run_dir / "results.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False),
        encoding="utf-8",
    )

    c = canonical
    lines = [
        "# U10 P1: 30% vs 50% cash sleeve, re-entry at -25%",
        "",
        "Mode: STRESS_TEST_ONLY",
        "",
        "Only cash fraction changed: 30% -> 50%.",
        "",
        "## Canonical U10",
        "",
        "|Metric|Baseline|Cash 30% control|Cash 50% candidate|",
        "|---|---:|---:|---:|",
        f"|Final equity|{c['baseline_final_equity_usdt']:,.2f}|"
        f"{c['control_final_equity_usdt']:,.2f}|"
        f"{c['candidate_final_equity_usdt']:,.2f}|",
        f"|Max DD|{pct(c['baseline_max_drawdown'])}|"
        f"{pct(c['control_max_drawdown'])}|"
        f"{pct(c['candidate_max_drawdown'])}|",
        f"|Cash-outs|—|{int(c['control_cashouts'])}|"
        f"{int(c['candidate_cashouts'])}|",
        f"|Re-entries|—|{int(c['control_reentries'])}|"
        f"{int(c['candidate_reentries'])}|",
        f"|Days in cash|—|{int(c['control_days_in_cash'])}|"
        f"{int(c['candidate_days_in_cash'])}|",
        "",
        f"Cash50 vs cash30 final-equity delta: "
        f"{c['candidate_delta_vs_control_usdt']:+,.2f} USDT "
        f"({pct(c['candidate_delta_vs_control_pct'])})",
        "",
        f"Cash50 vs ordinary baseline: "
        f"{c['candidate_delta_vs_baseline_usdt']:+,.2f} USDT "
        f"({pct(c['candidate_delta_vs_baseline_pct'])})",
        "",
        f"Cash50 DD change vs cash30: "
        f"{c['candidate_dd_vs_control_pp']:+.2f} pp",
        "",
        "## 791 alternative U10s",
        "",
        f"- cash50 > ordinary baseline: "
        f"{100*alt['candidate_gt_baseline_rate']:.2f}%",
        f"- cash50 > cash30: "
        f"{100*alt['candidate_gt_control_rate']:.2f}%",
        f"- cash50 = cash30: "
        f"{100*alt['candidate_eq_control_rate']:.2f}%",
        f"- cash50 < cash30: "
        f"{100*alt['candidate_lt_control_rate']:.2f}%",
        f"- cash50 DD better than cash30: "
        f"{100*alt['candidate_dd_better_control_rate']:.2f}%",
        f"- both final and DD better than cash30: "
        f"{100*alt['candidate_both_better_control_rate']:.2f}%",
        f"- median cash50 delta vs cash30: "
        f"{pct(alt['median_candidate_delta_vs_control_pct'])}",
        f"- q25/q75 cash50 delta vs cash30: "
        f"{pct(alt['q25_candidate_delta_vs_control_pct'])} / "
        f"{pct(alt['q75_candidate_delta_vs_control_pct'])}",
        f"- unfinished candidate cash-cycle rate: "
        f"{100*alt['candidate_unfinished_rate']:.2f}%",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
        "",
    ]
    report = "\n".join(lines)
    (run_dir / "report.md").write_text(report, encoding="utf-8")
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
