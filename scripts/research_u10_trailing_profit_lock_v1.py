from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path

import pandas as pd

from scripts.research_u10_20pct_cash_reentry_v1 import run_overlay as run_immediate_overlay
from scripts.research_u10_ledger_entry_stress_v1 import (
    COST,
    LOOKBACK,
    START_ASSET,
    U10,
    build_events,
    download_panel,
    run_window,
    utc,
)

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "u10_trailing_profit_lock_v1"

INITIAL_USDT = 10000.0
ACTIVATION_MULTIPLE = 10.0
TRAIL_DECLINE_MULTIPLE = 2.0
CASH_FRACTION = 0.20
REENTRY_DRAWDOWN = 0.50


def source_sha():
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def analyze_from(eq: pd.DataFrame, start_date: str):
    start_ts = utc(start_date)
    ts_series = pd.to_datetime(eq["timestamp"], utc=True)
    sub = eq[ts_series >= start_ts].copy().reset_index(drop=True)
    if sub.empty:
        raise RuntimeError("empty post-trigger segment")

    values = sub["total_equity_usdt"].astype(float)
    running_peak = values.cummax()
    dd = values / running_peak - 1.0

    min_idx = int(values.idxmin())
    dd_idx = int(dd.idxmin())
    peak_idx = int(values.loc[:dd_idx].idxmax())

    return {
        "minimum_total_equity_usdt": float(values.loc[min_idx]),
        "minimum_date": str(sub.loc[min_idx, "timestamp"]),
        "max_drawdown": float(dd.loc[dd_idx]),
        "drawdown_peak_usdt": float(values.loc[peak_idx]),
        "drawdown_peak_date": str(sub.loc[peak_idx, "timestamp"]),
        "drawdown_trough_usdt": float(values.loc[dd_idx]),
        "drawdown_trough_date": str(sub.loc[dd_idx, "timestamp"]),
    }


def run_trailing_overlay(panel, baseline_eq, mature_start):
    w = panel[panel["timestamp"] >= utc(mature_start)].copy().reset_index(drop=True)
    ref = baseline_eq.copy().reset_index(drop=True)

    if len(w) != len(ref):
        raise RuntimeError(
            f"panel/reference row mismatch: panel={len(w)} ref={len(ref)}"
        )

    current = START_ASSET
    first_open = float(w.iloc[0][current + "_open"])
    qty = INITIAL_USDT / first_open
    cash = 0.0

    activated = False
    activation_date = None
    running_peak_usdt = None
    running_peak_date = None

    locked_peak_usdt = None
    locked_peak_date = None

    cashout_pending = False
    cashout_trigger = None
    cashout_record = None

    reentry_pending = False
    reentry_trigger = None
    reentry_record = None

    route = [current]
    strategy_transitions = 0
    rows = []

    for pos in range(len(w)):
        row = w.iloc[pos]
        ref_row = ref.iloc[pos]
        ts = utc(row["timestamp"])
        ref_ts = utc(ref_row["timestamp"])
        if ts != ref_ts:
            raise RuntimeError(f"timestamp mismatch at row {pos}: {ts} != {ref_ts}")

        # The baseline U10 asset tells us whether frozen U10 rotated at this open.
        target_asset = str(ref_row["asset"])
        if target_asset != current:
            from_open = float(row[current + "_open"])
            to_open = float(row[target_asset + "_open"])
            invested_value = qty * from_open
            qty = invested_value * (1.0 - COST) / to_open
            current = target_asset
            route.append(current)
            strategy_transitions += 1

        open_price = float(row[current + "_open"])

        # Capital overlay executes after any U10 rotation at this open.
        if cashout_pending and cashout_record is None:
            pre_total = qty * open_price + cash
            sold_qty = qty * CASH_FRACTION
            gross_cash = sold_qty * open_price
            sell_fee = gross_cash * COST
            net_cash = gross_cash - sell_fee

            qty -= sold_qty
            cash += net_cash
            cashout_pending = False

            cashout_record = {
                "trigger_date": cashout_trigger["date"],
                "execution_date": ts.isoformat(),
                "asset": current,
                "asset_open_usdt": open_price,
                "locked_peak_usdt": locked_peak_usdt,
                "locked_peak_date": locked_peak_date,
                "reference_equity_at_trigger_usdt": cashout_trigger[
                    "reference_equity_usdt"
                ],
                "absolute_decline_at_trigger_usdt": cashout_trigger[
                    "absolute_decline_usdt"
                ],
                "decline_pct_from_locked_peak": cashout_trigger[
                    "decline_pct_from_peak"
                ],
                "pre_cashout_total_usdt": pre_total,
                "sold_qty": sold_qty,
                "gross_cash_usdt": gross_cash,
                "cashout_fee_usdt": sell_fee,
                "net_cash_parked_usdt": net_cash,
                "post_cashout_total_at_open_usdt": qty * open_price + cash,
            }

        if reentry_pending and reentry_record is None:
            gross_cash = cash
            reentry_fee = gross_cash * COST
            net_cash = gross_cash - reentry_fee
            bought_qty = net_cash / open_price

            qty += bought_qty
            cash = 0.0
            reentry_pending = False

            reentry_record = {
                "trigger_date": reentry_trigger["date"],
                "execution_date": ts.isoformat(),
                "asset": current,
                "asset_open_usdt": open_price,
                "locked_peak_usdt": locked_peak_usdt,
                "reference_equity_at_trigger_usdt": reentry_trigger[
                    "reference_equity_usdt"
                ],
                "reference_drawdown_from_locked_peak": reentry_trigger[
                    "drawdown_from_locked_peak"
                ],
                "gross_cash_usdt": gross_cash,
                "reentry_fee_usdt": reentry_fee,
                "net_reinvested_usdt": net_cash,
                "bought_qty": bought_qty,
            }

        close_price = float(row[current + "_close"])
        invested_close = qty * close_price
        total_close = invested_close + cash
        reference_close = float(ref_row["equity_usdt"])

        rows.append(
            {
                "timestamp": ts.isoformat(),
                "asset": current,
                "asset_qty": qty,
                "asset_close_usdt": close_price,
                "invested_equity_usdt": invested_close,
                "cash_usdt": cash,
                "total_equity_usdt": total_close,
                "reference_u10_equity_usdt": reference_close,
            }
        )

        # 10x only activates monitoring; it does not sell anything.
        if not activated:
            if reference_close >= INITIAL_USDT * ACTIVATION_MULTIPLE:
                activated = True
                activation_date = ts.isoformat()
                running_peak_usdt = reference_close
                running_peak_date = ts.isoformat()
        elif cashout_record is None and not cashout_pending:
            if reference_close > running_peak_usdt:
                running_peak_usdt = reference_close
                running_peak_date = ts.isoformat()

            absolute_decline = running_peak_usdt - reference_close
            if (
                absolute_decline >= INITIAL_USDT * TRAIL_DECLINE_MULTIPLE
                and pos < len(w) - 1
            ):
                locked_peak_usdt = running_peak_usdt
                locked_peak_date = running_peak_date
                cashout_trigger = {
                    "date": ts.isoformat(),
                    "reference_equity_usdt": reference_close,
                    "absolute_decline_usdt": absolute_decline,
                    "decline_pct_from_peak": reference_close
                    / running_peak_usdt
                    - 1.0,
                }
                cashout_pending = True

        # Re-entry starts only after the cash-out has actually happened.
        if (
            cashout_record is not None
            and reentry_record is None
            and not reentry_pending
            and ts > utc(cashout_record["execution_date"])
            and reference_close <= locked_peak_usdt * (1.0 - REENTRY_DRAWDOWN)
            and pos < len(w) - 1
        ):
            reentry_trigger = {
                "date": ts.isoformat(),
                "reference_equity_usdt": reference_close,
                "drawdown_from_locked_peak": reference_close
                / locked_peak_usdt
                - 1.0,
            }
            reentry_pending = True

    eq = pd.DataFrame(rows)

    if not activated:
        raise RuntimeError("U10 never reached the 10x activation threshold")
    if cashout_record is None:
        raise RuntimeError("trailing 2x decline never triggered a cash-out")

    post = analyze_from(eq, cashout_record["execution_date"])
    final_total = float(eq.iloc[-1]["total_equity_usdt"])

    result = {
        "start": str(eq.iloc[0]["timestamp"]),
        "end": str(eq.iloc[-1]["timestamp"]),
        "initial_usdt": INITIAL_USDT,
        "activation_threshold_usdt": INITIAL_USDT * ACTIVATION_MULTIPLE,
        "activation_date": activation_date,
        "trail_decline_usdt": INITIAL_USDT * TRAIL_DECLINE_MULTIPLE,
        "cash_fraction": CASH_FRACTION,
        "reentry_drawdown": REENTRY_DRAWDOWN,
        "cashout": cashout_record,
        "reentry": reentry_record,
        "final_asset": current,
        "final_asset_qty": float(qty),
        "final_cash_usdt": float(cash),
        "final_total_equity_usdt": final_total,
        "total_return": final_total / INITIAL_USDT - 1.0,
        "strategy_transitions": strategy_transitions,
        "route": route,
        **post,
    }
    return result, eq


def pct(x):
    return f"{100*x:+.2f}%"


def write_report(payload, run_dir):
    trailing = payload["trailing"]
    baseline = payload["baseline"]
    immediate = payload["immediate_cash_forever"]

    lines = [
        "# U10 Trailing Profit Lock v1",
        "",
        "Mode: PATCH_FIX / research-only capital-management overlay",
        "Live U10/U8 logic: UNCHANGED",
        "",
        "## Rule",
        "",
        "- 10x original capital (100,000 USDT) only activates trailing monitoring.",
        "- Keep updating the frozen-U10 running peak after activation.",
        "- When frozen-U10 equity closes at least 20,000 USDT below that peak, sell 20% on the next open.",
        "- Lock that peak.",
        "- Re-enter parked cash only after frozen-U10 equity closes 50% below the locked peak; buy on the next open.",
        "- Cash-out and re-entry each pay 0.1% modeled transaction cost.",
        "",
        "## Observed path",
        "",
        f"- 10x activation close: {pd.Timestamp(trailing['activation_date']).date()}",
        f"- Locked peak: {trailing['cashout']['locked_peak_usdt']:,.2f} USDT on {pd.Timestamp(trailing['cashout']['locked_peak_date']).date()}",
        f"- Cash-out trigger close: {trailing['cashout']['reference_equity_at_trigger_usdt']:,.2f} USDT on {pd.Timestamp(trailing['cashout']['trigger_date']).date()}",
        f"- Decline at trigger: {trailing['cashout']['absolute_decline_at_trigger_usdt']:,.2f} USDT ({pct(trailing['cashout']['decline_pct_from_locked_peak'])})",
        f"- Cash-out execution: {pd.Timestamp(trailing['cashout']['execution_date']).date()} while holding {trailing['cashout']['asset']}",
        f"- Actual overlay equity at execution open: {trailing['cashout']['pre_cashout_total_usdt']:,.2f} USDT",
        f"- Gross 20% sleeve sold: {trailing['cashout']['gross_cash_usdt']:,.2f} USDT",
        f"- Cash-out cost: {trailing['cashout']['cashout_fee_usdt']:,.2f} USDT",
        f"- Net cash parked: {trailing['cashout']['net_cash_parked_usdt']:,.2f} USDT",
        "",
        "## Re-entry",
        "",
    ]

    if trailing["reentry"] is None:
        lines += [
            "- The 50%-from-locked-peak re-entry trigger was not reached before the test ended.",
            f"- Cash still parked at end: {trailing['final_cash_usdt']:,.2f} USDT",
        ]
    else:
        lines += [
            f"- Re-entry trigger close: {trailing['reentry']['reference_equity_at_trigger_usdt']:,.2f} USDT on {pd.Timestamp(trailing['reentry']['trigger_date']).date()}",
            f"- Reference drawdown from locked peak: {pct(trailing['reentry']['reference_drawdown_from_locked_peak'])}",
            f"- Re-entry execution: {pd.Timestamp(trailing['reentry']['execution_date']).date()} into {trailing['reentry']['asset']}",
            f"- Gross cash re-entered: {trailing['reentry']['gross_cash_usdt']:,.2f} USDT",
            f"- Re-entry cost: {trailing['reentry']['reentry_fee_usdt']:,.2f} USDT",
        ]

    lines += [
        "",
        "## Comparison",
        "",
        "| Scenario | Final equity | Return | Minimum after its cash-out | Max DD after its cash-out |",
        "|---|---:|---:|---:|---:|",
        f"| NO_OVERLAY | {baseline['final_equity_usdt']:,.2f} | {pct(baseline['total_return'])} | n/a | n/a |",
        f"| IMMEDIATE_20PCT_AT_10X / cash forever | {immediate['final_total_equity_usdt']:,.2f} | {pct(immediate['total_return'])} | {immediate['post_cashout_min_equity_usdt']:,.2f} | {pct(immediate['post_cashout_max_drawdown'])} |",
        f"| TRAILING_2X_THEN_REENTER_50PCT | {trailing['final_total_equity_usdt']:,.2f} | {pct(trailing['total_return'])} | {trailing['minimum_total_equity_usdt']:,.2f} | {pct(trailing['max_drawdown'])} |",
        "",
        f"- Trailing overlay delta vs no-overlay baseline: {trailing['terminal_delta_vs_baseline_usdt']:+,.2f} USDT ({pct(trailing['terminal_delta_vs_baseline_pct'])})",
        f"- Trailing overlay delta vs immediate-cash-forever: {trailing['terminal_delta_vs_immediate_usdt']:+,.2f} USDT ({pct(trailing['terminal_delta_vs_immediate_pct'])})",
        "",
        "## Interpretation boundary",
        "",
        "- The frozen U10 reference equity controls activation, peak tracking, cash-out trigger and re-entry trigger.",
        "- The parked cash sleeve cannot move its own trigger thresholds.",
        "- This is one historical path and does not establish optimal thresholds.",
        "- Additional spread/slippage and stablecoin risk are not separately modeled.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_BACKTEST",
    ]

    (run_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff", default="2026-09-27T00:00:00Z")
    args = parser.parse_args()

    panel, metadata = download_panel(utc(args.cutoff))
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

    immediate, _ = run_immediate_overlay(
        panel,
        event_map,
        mature_start,
        INITIAL_USDT,
        scenario="CASH_FOREVER",
        reentry_months=None,
    )

    trailing, eq = run_trailing_overlay(panel, baseline_eq, mature_start)

    trailing["terminal_delta_vs_baseline_usdt"] = (
        trailing["final_total_equity_usdt"] - baseline["final_equity_usdt"]
    )
    trailing["terminal_delta_vs_baseline_pct"] = (
        trailing["final_total_equity_usdt"] / baseline["final_equity_usdt"] - 1.0
    )
    trailing["terminal_delta_vs_immediate_usdt"] = (
        trailing["final_total_equity_usdt"] - immediate["final_total_equity_usdt"]
    )
    trailing["terminal_delta_vs_immediate_pct"] = (
        trailing["final_total_equity_usdt"]
        / immediate["final_total_equity_usdt"]
        - 1.0
    )

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    eq.to_csv(run_dir / "equity.csv", index=False)

    payload = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit": source_sha(),
        "u10": list(U10),
        "parameters": {
            "initial_usdt": INITIAL_USDT,
            "activation_multiple": ACTIVATION_MULTIPLE,
            "trail_decline_multiple": TRAIL_DECLINE_MULTIPLE,
            "trail_decline_usdt": INITIAL_USDT * TRAIL_DECLINE_MULTIPLE,
            "cash_fraction": CASH_FRACTION,
            "reentry_drawdown": REENTRY_DRAWDOWN,
            "transaction_cost": COST,
        },
        "data_metadata": metadata,
        "mature_start": mature_start.isoformat(),
        "baseline": baseline,
        "immediate_cash_forever": immediate,
        "trailing": trailing,
    }

    (run_dir / "results.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False),
        encoding="utf-8",
    )
    write_report(payload, run_dir)

    print("run_dir=" + str(run_dir))
    print("activation=" + trailing["activation_date"])
    print(
        "locked_peak=%.8f %s"
        % (
            trailing["cashout"]["locked_peak_usdt"],
            trailing["cashout"]["locked_peak_date"],
        )
    )
    print(
        "cashout_trigger=%.8f %s decline=%.8f"
        % (
            trailing["cashout"]["reference_equity_at_trigger_usdt"],
            trailing["cashout"]["trigger_date"],
            trailing["cashout"]["absolute_decline_at_trigger_usdt"],
        )
    )
    print("cashout_execution=" + trailing["cashout"]["execution_date"])
    if trailing["reentry"] is None:
        print("reentry=NOT_REACHED")
    else:
        print(
            "reentry_trigger=%.8f %s"
            % (
                trailing["reentry"]["reference_equity_at_trigger_usdt"],
                trailing["reentry"]["trigger_date"],
            )
        )
        print("reentry_execution=" + trailing["reentry"]["execution_date"])

    print("baseline_final=%.8f" % baseline["final_equity_usdt"])
    print("immediate_final=%.8f" % immediate["final_total_equity_usdt"])
    print("trailing_final=%.8f" % trailing["final_total_equity_usdt"])
    print("trailing_min=%.8f" % trailing["minimum_total_equity_usdt"])
    print("trailing_max_dd=%.8f" % trailing["max_drawdown"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
