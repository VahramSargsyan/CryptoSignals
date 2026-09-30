from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path

import pandas as pd

from scripts.research_u10_ledger_entry_stress_v1 import (
    COST,
    LOOKBACK,
    START_ASSET,
    U10,
    build_events,
    choose_candidate,
    download_panel,
    run_window,
    utc,
)

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "u10_20pct_cash_reentry_v1"

INITIAL_USDT = 10000.0
TRIGGER_MULTIPLE = 10.0
CASH_FRACTION = 0.20

SCENARIOS = (
    ("CASH_FOREVER", None),
    ("REENTER_6M", 6),
    ("REENTER_12M", 12),
)


def source_sha():
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def analyze_after_cashout(eq: pd.DataFrame, execution_date: str, initial_usdt: float):
    start_ts = utc(execution_date)
    sub = eq[pd.to_datetime(eq["timestamp"], utc=True) >= start_ts].copy()
    if sub.empty:
        raise RuntimeError("empty post-cashout equity segment")

    values = sub["total_equity_usdt"].astype(float).reset_index(drop=True)
    timestamps = sub["timestamp"].reset_index(drop=True)

    running_peak = values.cummax()
    drawdown = values / running_peak - 1.0
    dd_idx = int(drawdown.idxmin())
    peak_idx = int(values.loc[:dd_idx].idxmax())
    min_idx = int(values.idxmin())

    return {
        "post_cashout_min_equity_usdt": float(values.loc[min_idx]),
        "post_cashout_min_date": str(timestamps.loc[min_idx]),
        "post_cashout_min_vs_initial": float(values.loc[min_idx]) / initial_usdt - 1.0,
        "post_cashout_max_drawdown": float(drawdown.loc[dd_idx]),
        "post_cashout_dd_peak_equity_usdt": float(values.loc[peak_idx]),
        "post_cashout_dd_peak_date": str(timestamps.loc[peak_idx]),
        "post_cashout_dd_trough_equity_usdt": float(values.loc[dd_idx]),
        "post_cashout_dd_trough_date": str(timestamps.loc[dd_idx]),
    }


def run_overlay(
    panel,
    event_map,
    start,
    initial_usdt,
    *,
    scenario,
    reentry_months,
):
    w = panel[panel["timestamp"] >= utc(start)].copy().reset_index(drop=True)
    if w.empty:
        raise RuntimeError("empty U10 overlay window")

    current = START_ASSET
    first_open = float(w.iloc[0][current + "_open"])
    qty = initial_usdt / first_open
    cash = 0.0

    pending = None
    cashout_pending = False
    cashout_done = False
    cashout_trigger_date = None
    cashout_record = None
    reentry_record = None
    reentry_target = None

    strategy_transitions = 0
    conflicts = 0
    route = [current]
    rows = []

    for pos, row in w.iterrows():
        ts = utc(row["timestamp"])

        # Frozen U10 strategy transition executes first at the open.
        if pending is not None:
            value = qty * float(row[current + "_open"])
            current = pending["to_asset"]
            qty = value * (1.0 - COST) / float(row[current + "_open"])
            strategy_transitions += 1
            route.append(current)
            pending = None

        open_price = float(row[current + "_open"])

        # Capital-management overlay executes after any U10 transition.
        if cashout_pending and not cashout_done:
            pre_cashout_value = qty * open_price + cash
            sold_qty = qty * CASH_FRACTION
            gross_withdraw = sold_qty * open_price
            cashout_fee = gross_withdraw * COST
            net_cash = gross_withdraw - cashout_fee

            qty -= sold_qty
            cash += net_cash

            cashout_done = True
            cashout_pending = False

            if reentry_months is not None:
                reentry_target = ts + pd.DateOffset(months=reentry_months)

            cashout_record = {
                "trigger_date": cashout_trigger_date.isoformat(),
                "execution_date": ts.isoformat(),
                "asset": current,
                "asset_open_usdt": open_price,
                "pre_cashout_total_usdt": pre_cashout_value,
                "sold_qty": sold_qty,
                "gross_withdraw_usdt": gross_withdraw,
                "cashout_fee_usdt": cashout_fee,
                "net_cash_parked_usdt": net_cash,
                "post_cashout_total_at_open_usdt": qty * open_price + cash,
                "reentry_target_date": (
                    reentry_target.isoformat() if reentry_target is not None else None
                ),
            }

        # Re-enter entire cash sleeve on first daily open on/after target.
        if (
            cashout_done
            and cash > 0.0
            and reentry_target is not None
            and reentry_record is None
            and ts >= reentry_target
        ):
            reentry_gross = cash
            reentry_fee = reentry_gross * COST
            reentry_net = reentry_gross - reentry_fee
            buy_qty = reentry_net / open_price

            qty += buy_qty
            cash = 0.0

            reentry_record = {
                "target_date": reentry_target.isoformat(),
                "execution_date": ts.isoformat(),
                "asset": current,
                "asset_open_usdt": open_price,
                "gross_cash_usdt": reentry_gross,
                "reentry_fee_usdt": reentry_fee,
                "net_reinvested_usdt": reentry_net,
                "bought_qty": buy_qty,
            }

        close_price = float(row[current + "_close"])
        invested_close = qty * close_price
        total_close = invested_close + cash

        rows.append(
            {
                "timestamp": ts.isoformat(),
                "scenario": scenario,
                "asset": current,
                "asset_qty": qty,
                "asset_close_usdt": close_price,
                "invested_equity_usdt": invested_close,
                "cash_usdt": cash,
                "total_equity_usdt": total_close,
            }
        )

        if (
            not cashout_done
            and not cashout_pending
            and total_close >= initial_usdt * TRIGGER_MULTIPLE
            and pos < len(w) - 1
        ):
            cashout_trigger_date = ts
            cashout_pending = True

        if pos == len(w) - 1:
            continue

        candidates = [
            event
            for event in event_map.get(ts, [])
            if event["from_asset"] == current and event["to_asset"] in U10
        ]
        if candidates:
            if len(candidates) > 1:
                conflicts += 1
            selected = sorted(
                candidates,
                key=lambda event: (
                    -float(event["max_dislocation"]),
                    event["to_asset"],
                    event["pair"],
                ),
            )[0]
            pending = dict(selected)

    eq = pd.DataFrame(rows)
    if cashout_record is None:
        raise RuntimeError(
            f"{scenario}: U10 never reached {TRIGGER_MULTIPLE:.1f}x trigger"
        )

    final_total = float(eq.iloc[-1]["total_equity_usdt"])
    final_invested = float(eq.iloc[-1]["invested_equity_usdt"])
    final_cash = float(eq.iloc[-1]["cash_usdt"])

    post = analyze_after_cashout(
        eq, cashout_record["execution_date"], initial_usdt
    )

    result = {
        "scenario": scenario,
        "start": str(eq.iloc[0]["timestamp"]),
        "end": str(eq.iloc[-1]["timestamp"]),
        "initial_usdt": initial_usdt,
        "trigger_multiple": TRIGGER_MULTIPLE,
        "trigger_threshold_usdt": initial_usdt * TRIGGER_MULTIPLE,
        "cash_fraction": CASH_FRACTION,
        "cashout": cashout_record,
        "reentry": reentry_record,
        "strategy_transitions": strategy_transitions,
        "conflicts": conflicts,
        "route": route,
        "final_asset": current,
        "final_asset_qty": float(qty),
        "final_invested_equity_usdt": final_invested,
        "final_cash_usdt": final_cash,
        "final_total_equity_usdt": final_total,
        "total_return": final_total / initial_usdt - 1.0,
        **post,
    }

    return result, eq


def pct(value):
    return f"{100 * value:+.2f}%"


def write_report(payload, run_dir):
    baseline = payload["baseline"]
    lines = [
        "# U10 20% Cash-Out + Re-entry v1",
        "",
        "Mode: PATCH_FIX / research-only capital-management experiment",
        "Live U10/U8 behavior: UNCHANGED",
        "",
        "## Rule",
        "",
        "Start with 10,000 USDT on the first mature U10 date.",
        "After the first daily close at or above 100,000 USDT, sell 20% into USDT at the next daily open.",
        "The remaining 80% continues frozen U10. Parked cash earns 0%.",
        "Re-entry variants buy the asset U10 is holding after 6 or 12 calendar months.",
        "Cash-out and re-entry each pay the same modeled 0.1% transaction cost.",
        "",
        "## Baseline",
        "",
        f"- No cash-out terminal equity: {baseline['final_equity_usdt']:,.2f} USDT",
        f"- No cash-out return: {pct(baseline['total_return'])}",
        "",
        "## Results",
        "",
        "| Scenario | Cash-out execution | Net cash parked | Re-entry | Post-cashout minimum | Post-cashout max DD | Final equity | Delta vs baseline |",
        "|---|---|---:|---|---:|---:|---:|---:|",
    ]

    for s in payload["scenarios"]:
        reentry = (
            pd.Timestamp(s["reentry"]["execution_date"]).date()
            if s["reentry"] is not None
            else "never"
        )
        lines.append(
            f"| {s['scenario']} | {pd.Timestamp(s['cashout']['execution_date']).date()} | "
            f"{s['cashout']['net_cash_parked_usdt']:,.2f} | {reentry} | "
            f"{s['post_cashout_min_equity_usdt']:,.2f} | "
            f"{pct(s['post_cashout_max_drawdown'])} | "
            f"{s['final_total_equity_usdt']:,.2f} | "
            f"{s['terminal_delta_vs_baseline_usdt']:+,.2f} |"
        )

    lines += [
        "",
        "## Scenario details",
    ]

    for s in payload["scenarios"]:
        lines += [
            "",
            f"### {s['scenario']}",
            "",
            f"- Trigger close: {pd.Timestamp(s['cashout']['trigger_date']).date()}",
            f"- Cash-out execution: {pd.Timestamp(s['cashout']['execution_date']).date()}",
            f"- Asset sold: {s['cashout']['asset']}",
            f"- Portfolio at cash-out open before sale: {s['cashout']['pre_cashout_total_usdt']:,.2f} USDT",
            f"- Gross 20% sleeve: {s['cashout']['gross_withdraw_usdt']:,.2f} USDT",
            f"- Cash-out cost: {s['cashout']['cashout_fee_usdt']:,.2f} USDT",
            f"- Net cash parked: {s['cashout']['net_cash_parked_usdt']:,.2f} USDT",
            f"- Post-cashout minimum total equity: {s['post_cashout_min_equity_usdt']:,.2f} USDT on {pd.Timestamp(s['post_cashout_min_date']).date()}",
            f"- Post-cashout max drawdown: {pct(s['post_cashout_max_drawdown'])}",
        ]

        if s["reentry"] is not None:
            lines += [
                f"- Re-entry target: {pd.Timestamp(s['reentry']['target_date']).date()}",
                f"- Re-entry execution: {pd.Timestamp(s['reentry']['execution_date']).date()}",
                f"- Re-entry asset: {s['reentry']['asset']}",
                f"- Re-entry gross cash: {s['reentry']['gross_cash_usdt']:,.2f} USDT",
                f"- Re-entry cost: {s['reentry']['reentry_fee_usdt']:,.2f} USDT",
            ]
        else:
            lines.append(
                f"- Cash remaining at end: {s['final_cash_usdt']:,.2f} USDT"
            )

        lines += [
            f"- Final total equity: {s['final_total_equity_usdt']:,.2f} USDT ({pct(s['total_return'])})",
            f"- Delta vs no-cash baseline: {s['terminal_delta_vs_baseline_usdt']:+,.2f} USDT ({pct(s['terminal_delta_vs_baseline_pct'])})",
        ]

    lines += [
        "",
        "## Interpretation boundary",
        "",
        "- The 20% cash sleeve is a capital-management overlay, not a change to U10 signal logic.",
        "- Cash is modeled as non-yielding USDT.",
        "- Results depend strongly on the historical timing of the 10x trigger and re-entry dates.",
        "- Additional real-world spread/slippage and stablecoin risk are not separately modeled.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_BACKTEST",
    ]

    (run_dir / "report.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff", default="2026-09-27T00:00:00Z")
    args = parser.parse_args()

    cutoff = utc(args.cutoff)
    panel, metadata = download_panel(cutoff)
    event_map = build_events(panel)
    mature_start = utc(panel.iloc[LOOKBACK - 1]["timestamp"])

    baseline, _, baseline_eq = run_window(
        panel,
        event_map,
        mature_start,
        INITIAL_USDT,
        collect_ledger=False,
        shadow_cost=False,
    )

    scenario_results = []
    equity_frames = []

    for name, months in SCENARIOS:
        result, eq = run_overlay(
            panel,
            event_map,
            mature_start,
            INITIAL_USDT,
            scenario=name,
            reentry_months=months,
        )
        result["terminal_delta_vs_baseline_usdt"] = (
            result["final_total_equity_usdt"] - baseline["final_equity_usdt"]
        )
        result["terminal_delta_vs_baseline_pct"] = (
            result["final_total_equity_usdt"] / baseline["final_equity_usdt"] - 1.0
        )
        scenario_results.append(result)
        equity_frames.append(eq)

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    pd.concat(equity_frames, ignore_index=True).to_csv(
        run_dir / "scenario_equity.csv", index=False
    )

    pd.DataFrame(
        [
            {
                "scenario": s["scenario"],
                "trigger_date": s["cashout"]["trigger_date"],
                "cashout_execution_date": s["cashout"]["execution_date"],
                "cashout_asset": s["cashout"]["asset"],
                "pre_cashout_total_usdt": s["cashout"]["pre_cashout_total_usdt"],
                "gross_withdraw_usdt": s["cashout"]["gross_withdraw_usdt"],
                "cashout_fee_usdt": s["cashout"]["cashout_fee_usdt"],
                "net_cash_parked_usdt": s["cashout"]["net_cash_parked_usdt"],
                "reentry_execution_date": (
                    s["reentry"]["execution_date"] if s["reentry"] else None
                ),
                "reentry_asset": (
                    s["reentry"]["asset"] if s["reentry"] else None
                ),
                "reentry_fee_usdt": (
                    s["reentry"]["reentry_fee_usdt"] if s["reentry"] else None
                ),
                "post_cashout_min_equity_usdt": s["post_cashout_min_equity_usdt"],
                "post_cashout_min_date": s["post_cashout_min_date"],
                "post_cashout_max_drawdown": s["post_cashout_max_drawdown"],
                "final_total_equity_usdt": s["final_total_equity_usdt"],
                "terminal_delta_vs_baseline_usdt": s[
                    "terminal_delta_vs_baseline_usdt"
                ],
                "terminal_delta_vs_baseline_pct": s[
                    "terminal_delta_vs_baseline_pct"
                ],
            }
            for s in scenario_results
        ]
    ).to_csv(run_dir / "summary.csv", index=False)

    payload = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit": source_sha(),
        "u10": list(U10),
        "parameters": {
            "initial_usdt": INITIAL_USDT,
            "trigger_multiple": TRIGGER_MULTIPLE,
            "cash_fraction": CASH_FRACTION,
            "transition_cost": COST,
            "cash_yield": 0.0,
            "execution": "next_open",
        },
        "data_metadata": metadata,
        "mature_start": mature_start.isoformat(),
        "baseline": baseline,
        "scenarios": scenario_results,
    }

    (run_dir / "results.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False),
        encoding="utf-8",
    )
    write_report(payload, run_dir)

    print("run_dir=" + str(run_dir))
    print("baseline_final=%.8f" % baseline["final_equity_usdt"])
    for s in scenario_results:
        print(
            f"{s['scenario']}: trigger={s['cashout']['trigger_date']} "
            f"cashout={s['cashout']['execution_date']} "
            f"cash={s['cashout']['net_cash_parked_usdt']:.2f} "
            f"reentry={s['reentry']['execution_date'] if s['reentry'] else 'never'} "
            f"min={s['post_cashout_min_equity_usdt']:.2f} "
            f"maxdd={s['post_cashout_max_drawdown']:.6f} "
            f"final={s['final_total_equity_usdt']:.2f} "
            f"delta={s['terminal_delta_vs_baseline_usdt']:.2f}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
