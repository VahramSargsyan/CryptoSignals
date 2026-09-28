from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

import scripts.research_u10_monthly_surge_fractal_buystop_v1 as fractal
from scripts.research_u10_fractal_old2020_2022_secondary_v1 import (
    download_old_ohlc,
    build_events,
)
from scripts.research_u10_surge_old2020_2022_holdout_v1 import (
    OLD10,
    EVAL_START,
)
from scripts.research_u10_monthly_surge_pullback_v1 import (
    INITIAL_USDT,
    monthly_state,
    run_reference,
    source_sha,
)
from scripts.research_u10_last_year_vs_u8_universes_v1 import utc

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "u10_fractal3_old2020_2022_secondary_v1"


def run_f3(panel_eval, reference, anchors, cfg):
    old_left, old_right = fractal.PIVOT_LEFT, fractal.PIVOT_RIGHT
    try:
        fractal.PIVOT_LEFT = 1
        fractal.PIVOT_RIGHT = 1
        return fractal.run_fractal_overlay(
            panel_eval,
            reference,
            anchors,
            surge_threshold=cfg["surge"],
            cashout_pullback=cfg["pullback"],
            initial_usdt=INITIAL_USDT,
        )
    finally:
        fractal.PIVOT_LEFT, fractal.PIVOT_RIGHT = old_left, old_right


def pct(x):
    return f"{100*x:+.2f}%"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cutoff", default="2026-09-27T00:00:00Z")
    args = ap.parse_args()

    panel, meta, pre = download_old_ohlc(utc(args.cutoff))
    events = build_events(panel)
    baseline, reference = run_reference(
        panel, events, OLD10, EVAL_START, INITIAL_USDT
    )
    reference = reference.reset_index(drop=True)
    panel_eval = panel[panel["timestamp"] >= EVAL_START].copy().reset_index(drop=True)
    if len(panel_eval) != len(reference):
        raise RuntimeError("old panel/reference mismatch")

    _, anchors = monthly_state(reference, INITIAL_USDT)
    variants = {}
    cycles_all = []

    for label, cfg in (("P1", fractal.P1), ("P2", fractal.P2)):
        f3, cycles = run_f3(panel_eval, reference, anchors, cfg)
        f3["delta_vs_baseline_usdt"] = (
            f3["final_equity_usdt"] - baseline["final_equity_usdt"]
        )
        f3["delta_vs_baseline_pct"] = (
            f3["final_equity_usdt"] / baseline["final_equity_usdt"] - 1.0
        )
        f3["max_dd_improvement_pp"] = (
            f3["max_drawdown"] - baseline["max_drawdown"]
        ) * 100.0
        variants[label] = f3

        if not cycles.empty:
            cc = cycles.copy()
            cc.insert(0, "variant", label)
            cycles_all.append(cc)

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    if cycles_all:
        pd.concat(cycles_all, ignore_index=True).to_csv(
            run_dir / "cycles.csv", index=False
        )

    payload = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit": source_sha(),
        "history_status": "REUSED_HISTORY_SECONDARY_EVIDENCE",
        "pivot_left": 1,
        "pivot_right": 1,
        "old10": list(OLD10),
        "prehistory_rows": pre,
        "baseline": baseline,
        "variants": variants,
        "data_metadata": meta,
    }
    (run_dir / "results.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False),
        encoding="utf-8",
    )

    lines = [
        "# OLD10 2020-2022 3-Bar Fractal Secondary Evidence",
        "",
        "History status: REUSED_HISTORY_SECONDARY_EVIDENCE",
        "This is NOT untouched OOS validation.",
        "",
        f"Baseline final: {baseline['final_equity_usdt']:,.2f} USDT",
        f"Baseline max DD: {pct(baseline['max_drawdown'])}",
        "",
        "| Variant | Final | Delta vs baseline | Max DD | DD change | Reentries | Unfinished | Cash days |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for label in ("P1", "P2"):
        v = variants[label]
        lines.append(
            f"| {label}-FRACTAL3 | {v['final_equity_usdt']:,.2f} | "
            f"{pct(v['delta_vs_baseline_pct'])} | {pct(v['max_drawdown'])} | "
            f"{v['max_dd_improvement_pp']:+.2f} pp | {v['reentries']} | "
            f"{v['unfinished_cycles']} | {v['days_in_cash']} |"
        )
    lines += [
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_REUSED_HISTORY_SECONDARY",
    ]
    (run_dir / "report.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )

    print("run_dir=" + str(run_dir))
    print(
        "baseline final=%.8f dd=%.8f"
        % (baseline["final_equity_usdt"], baseline["max_drawdown"])
    )
    for label in ("P1", "P2"):
        v = variants[label]
        print(
            "%s final=%.8f delta=%.8f dd=%.8f ddpp=%.8f re=%d unfinished=%d cash=%d"
            % (
                label,
                v["final_equity_usdt"],
                v["delta_vs_baseline_pct"],
                v["max_drawdown"],
                v["max_dd_improvement_pp"],
                v["reentries"],
                v["unfinished_cycles"],
                v["days_in_cash"],
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
