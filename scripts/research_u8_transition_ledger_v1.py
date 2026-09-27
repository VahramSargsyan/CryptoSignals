from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
from collections import defaultdict
from pathlib import Path

import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import build_pair_monitor

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "u8_transition_ledger_v1"

ASSETS = ("ATOM", "TWT", "PEPE", "BNB", "SOL", "TRX", "AAVE", "LINK")
LOOKBACK = 180
ARM = 0.15
REVERSAL = 0.03
COST = 0.001
DATA_START = pd.Timestamp("2023-01-01", tz="UTC")
START_ASSET = "ATOM"


def utc(value):
    ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def source_sha():
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()


def download_panel(cutoff):
    client = BinanceSpotRestClient()
    panel = None
    metadata = {}
    for asset in ASSETS:
        result = download_historical_dataset(
            client,
            symbol=asset + "USDT",
            start=DATA_START,
            end=cutoff,
            timeframe="1D",
            as_of=cutoff,
        )
        if result.dataset is None:
            raise RuntimeError(f"{asset}: {result.metadata.status}")
        if result.dataset.quality.has_critical_issues:
            raise RuntimeError(f"{asset}: critical data quality issues")
        frame = result.dataset.candles[["timestamp", "open", "close"]].copy()
        frame = frame.rename(columns={"open": asset + "_open", "close": asset + "_close"})
        metadata[asset] = {
            "rows": len(frame),
            "start": pd.Timestamp(frame.iloc[0]["timestamp"]).isoformat(),
            "end": pd.Timestamp(frame.iloc[-1]["timestamp"]).isoformat(),
            "listing_truncated": bool(result.metadata.listing_truncated),
        }
        panel = frame if panel is None else panel.merge(
            frame,
            on="timestamp",
            how="inner",
            validate="one_to_one",
        )

    panel = panel.sort_values("timestamp", kind="stable").reset_index(drop=True)
    if len(panel) < LOOKBACK:
        raise RuntimeError(f"common panel has only {len(panel)} rows; need at least {LOOKBACK}")
    return panel, metadata


def make_confirmed_event_map(panel):
    close_cols = ["timestamp"] + [asset + "_close" for asset in ASSETS]
    events, _ = build_pair_monitor(
        panel[close_cols].copy(),
        assets=ASSETS,
        lookback=LOOKBACK,
        arm_threshold=ARM,
        reversal=REVERSAL,
    )
    by_date = defaultdict(list)
    for event in events:
        if event["event"] == "CONFIRMED":
            by_date[utc(event["date"])].append(event)
    return by_date


def choose_candidate(event_map, ts, current):
    candidates = [
        event
        for event in event_map.get(ts, [])
        if event["from_asset"] == current and event["to_asset"] in ASSETS
    ]
    if not candidates:
        return None, 0
    selected = sorted(
        candidates,
        key=lambda event: (
            -float(event["max_dislocation"]),
            event["to_asset"],
            event["pair"],
        ),
    )[0]
    return selected, len(candidates)


def run_ledger(panel, event_map, initial_usdt):
    mature_start = utc(panel.iloc[LOOKBACK - 1]["timestamp"])
    eval_frame = panel[panel["timestamp"] >= mature_start].copy().reset_index(drop=True)
    if eval_frame.empty:
        raise RuntimeError("mature evaluation frame is empty")

    current = START_ASSET
    first_open = float(eval_frame.iloc[0][current + "_open"])
    qty = float(initial_usdt) / first_open
    shadow_qty = qty
    pending = None
    ledger = []
    equity = []
    shadow_equity = []
    route = [current]
    conflicts = 0

    for pos, row in eval_frame.iterrows():
        ts = utc(row["timestamp"])

        if pending is not None:
            from_asset = current
            to_asset = pending["to_asset"]
            from_open = float(row[from_asset + "_open"])
            to_open = float(row[to_asset + "_open"])

            from_qty_before = qty
            shadow_from_qty_before = shadow_qty

            gross_value = from_qty_before * from_open
            shadow_gross_value = shadow_from_qty_before * from_open
            cost_usdt = gross_value * COST
            net_value = gross_value - cost_usdt

            qty = net_value / to_open
            shadow_qty = shadow_gross_value / to_open
            current = to_asset
            route.append(current)

            actual_value_after = qty * to_open
            no_cost_value_after = shadow_qty * to_open
            cumulative_drag_usdt = no_cost_value_after - actual_value_after
            cumulative_drag_pct = (
                cumulative_drag_usdt / no_cost_value_after if no_cost_value_after else 0.0
            )

            transition_no = len(ledger) + 1
            expected_ratio = (1.0 - COST) ** transition_no
            actual_ratio = actual_value_after / no_cost_value_after
            if not math.isclose(actual_ratio, expected_ratio, rel_tol=1e-11, abs_tol=1e-11):
                raise AssertionError(
                    f"fee accounting invariant failed at transition {transition_no}: "
                    f"actual/no-cost={actual_ratio}, expected={expected_ratio}"
                )

            ledger.append(
                {
                    "transition_no": transition_no,
                    "signal_date": pending["signal_date"].isoformat(),
                    "execution_date": ts.isoformat(),
                    "pair": pending["pair"],
                    "from_asset": from_asset,
                    "from_qty_before": from_qty_before,
                    "from_open_usdt": from_open,
                    "gross_value_usdt": gross_value,
                    "transition_cost_rate": COST,
                    "transition_cost_usdt": cost_usdt,
                    "net_value_after_cost_usdt": net_value,
                    "to_asset": to_asset,
                    "to_open_usdt": to_open,
                    "to_qty_after": qty,
                    "shadow_no_cost_to_qty": shadow_qty,
                    "no_cost_value_usdt": no_cost_value_after,
                    "cumulative_fee_drag_usdt": cumulative_drag_usdt,
                    "cumulative_fee_drag_pct_vs_no_cost": cumulative_drag_pct,
                    "max_dislocation": float(pending["max_dislocation"]),
                    "candidate_count_on_signal_day": int(pending["candidate_count"]),
                }
            )
            pending = None

        close_price = float(row[current + "_close"])
        equity.append(
            {
                "timestamp": ts.isoformat(),
                "asset": current,
                "qty": qty,
                "close": close_price,
                "equity_usdt": qty * close_price,
            }
        )
        shadow_equity.append(
            {
                "timestamp": ts.isoformat(),
                "asset": current,
                "qty": shadow_qty,
                "close": close_price,
                "equity_usdt": shadow_qty * close_price,
            }
        )

        if pos == len(eval_frame) - 1:
            continue

        selected, candidate_count = choose_candidate(event_map, ts, current)
        if selected is None:
            continue
        if candidate_count > 1:
            conflicts += 1
        pending = {
            **selected,
            "signal_date": ts,
            "candidate_count": candidate_count,
        }

    equity_df = pd.DataFrame(equity)
    shadow_df = pd.DataFrame(shadow_equity)
    ledger_df = pd.DataFrame(ledger)

    initial_equity = float(initial_usdt)
    first_close_equity = float(equity_df.iloc[0]["equity_usdt"])
    final_equity = float(equity_df.iloc[-1]["equity_usdt"])
    final_shadow = float(shadow_df.iloc[-1]["equity_usdt"])
    total_return = final_equity / initial_equity - 1.0
    legacy_compatible_return = final_equity / first_close_equity - 1.0
    no_cost_return = final_shadow / initial_equity - 1.0
    final_fee_drag = final_shadow - final_equity
    final_drag_pct = final_fee_drag / final_shadow

    expected_final_ratio = (1.0 - COST) ** len(ledger_df)
    actual_final_ratio = final_equity / final_shadow
    if not math.isclose(actual_final_ratio, expected_final_ratio, rel_tol=1e-11, abs_tol=1e-11):
        raise AssertionError(
            f"terminal fee accounting invariant failed: actual/no-cost={actual_final_ratio}, "
            f"expected={expected_final_ratio}"
        )

    return {
        "mature_start": mature_start,
        "mature_end": utc(eval_frame.iloc[-1]["timestamp"]),
        "common_start": utc(panel.iloc[0]["timestamp"]),
        "common_end": utc(panel.iloc[-1]["timestamp"]),
        "initial_usdt": float(initial_usdt),
        "initial_asset": START_ASSET,
        "initial_asset_open": first_open,
        "initial_qty": float(initial_usdt) / first_open,
        "first_close_equity_usdt": first_close_equity,
        "legacy_compatible_return": legacy_compatible_return,
        "transitions": int(len(ledger_df)),
        "conflict_signal_days": int(conflicts),
        "route": route,
        "final_asset": current,
        "final_qty": float(qty),
        "final_equity_usdt": final_equity,
        "total_return": total_return,
        "no_cost_final_qty": float(shadow_qty),
        "no_cost_final_equity_usdt": final_shadow,
        "no_cost_total_return": no_cost_return,
        "terminal_fee_drag_usdt": final_fee_drag,
        "terminal_fee_drag_pct_vs_no_cost": final_drag_pct,
        "fee_multiplier_actual_vs_no_cost": actual_final_ratio,
        "expected_fee_multiplier": expected_final_ratio,
        "ledger": ledger_df,
        "equity": equity_df,
        "shadow_equity": shadow_df,
    }


def fmt_qty(value):
    if abs(value) >= 1000:
        return f"{value:,.4f}"
    if abs(value) >= 1:
        return f"{value:,.6f}"
    return f"{value:,.10f}"


def write_report(result, metadata, run_dir):
    ledger = result["ledger"]
    days = (result["mature_end"] - result["mature_start"]).days + 1
    months = days / 30.4375

    lines = [
        "# U8 Transition Ledger v1",
        "",
        "Mode: PATCH_FIX / research reporting only",
        "Live U8 logic and live configuration: UNCHANGED",
        "",
        "## Run identity",
        "",
        f"- Source commit: {source_sha()}",
        "- Data: Binance Spot 1D closed candles",
        f"- Common panel: {result['common_start'].date()} -> {result['common_end'].date()}",
        f"- Mature evaluation: {result['mature_start'].date()} -> {result['mature_end'].date()}",
        f"- Mature span: {days} days / {months:.2f} months",
        f"- Initial normalized capital: {result['initial_usdt']:,.2f} USDT",
        f"- Initial position: {fmt_qty(result['initial_qty'])} {START_ASSET} @ {result['initial_asset_open']:.8f} USDT",
        f"- Transition cost: {100*COST:.3f}% on every executed transition",
        f"- Executed transitions: {result['transitions']}",
        "",
        "## Terminal result",
        "",
        f"- Final position: {fmt_qty(result['final_qty'])} {result['final_asset']}",
        f"- Final equity after transition costs: {result['final_equity_usdt']:,.2f} USDT",
        f"- Return from initial 10,000 USDT at first open: {100*result['total_return']:+.2f}%",
        f"- First-day close equity: {result['first_close_equity_usdt']:,.2f} USDT",
        f"- Legacy-compatible return (final / first close - 1): {100*result['legacy_compatible_return']:+.2f}%",
        f"- Same route with zero transition cost: {result['no_cost_final_equity_usdt']:,.2f} USDT",
        f"- Zero-cost return: {100*result['no_cost_total_return']:+.2f}%",
        f"- Terminal fee drag: {result['terminal_fee_drag_usdt']:,.2f} USDT",
        f"- Terminal fee drag vs no-cost shadow: {100*result['terminal_fee_drag_pct_vs_no_cost']:.4f}%",
        f"- Accounting identity: actual/no-cost = {result['fee_multiplier_actual_vs_no_cost']:.12f}; "
        f"(1-cost)^N = {result['expected_fee_multiplier']:.12f}",
        "",
        "## Every executed transition",
        "",
        "| # | Signal | Execute | From | From qty | Gross USDT | Fee USDT | To | To qty | Net USDT | Cum. fee drag % |",
        "|---:|---|---|---|---:|---:|---:|---|---:|---:|---:|",
    ]

    for _, row in ledger.iterrows():
        lines.append(
            f"| {int(row['transition_no'])} | {pd.Timestamp(row['signal_date']).date()} | "
            f"{pd.Timestamp(row['execution_date']).date()} | {row['from_asset']} | "
            f"{fmt_qty(float(row['from_qty_before']))} | {float(row['gross_value_usdt']):,.2f} | "
            f"{float(row['transition_cost_usdt']):,.2f} | {row['to_asset']} | "
            f"{fmt_qty(float(row['to_qty_after']))} | {float(row['net_value_after_cost_usdt']):,.2f} | "
            f"{100*float(row['cumulative_fee_drag_pct_vs_no_cost']):.4f}% |"
        )

    lines += [
        "",
        "## Route",
        "",
        " -> ".join(result["route"]),
        "",
        "## Interpretation boundary",
        "",
        "- Token quantities use a normalized 10,000 USDT initial notional; scale them linearly for another starting capital.",
        "- Fee drag is measured against a shadow portfolio following the exact same route with zero transition cost.",
        "- Legacy U8 research runners report return as final equity / first-day close equity - 1; this report shows that value separately from return on the 10,000 USDT first-open starting capital.",
        "- This patch adds reporting only. It does not change U8 selection, signals, routing, parameters, or live held_asset.",
        "- Slippage beyond the frozen 0.1% transition-cost model is not separately modeled in this runner.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_BACKTEST",
    ]

    (run_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff", default="2026-09-27T00:00:00Z")
    parser.add_argument("--initial-usdt", type=float, default=10000.0)
    args = parser.parse_args()

    if args.initial_usdt <= 0:
        raise ValueError("--initial-usdt must be > 0")

    cutoff = utc(args.cutoff)
    panel, metadata = download_panel(cutoff)
    event_map = make_confirmed_event_map(panel)
    result = run_ledger(panel, event_map, args.initial_usdt)

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    result["ledger"].to_csv(run_dir / "transitions.csv", index=False)
    result["equity"].to_csv(run_dir / "equity.csv", index=False)
    result["shadow_equity"].to_csv(run_dir / "equity_no_cost_shadow.csv", index=False)

    payload = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit": source_sha(),
        "data_metadata": metadata,
        "assets": list(ASSETS),
        "parameters": {
            "lookback": LOOKBACK,
            "arm": ARM,
            "reversal": REVERSAL,
            "transition_cost": COST,
            "execution": "next_open",
            "router": "strongest_confirmed_max_dislocation",
        },
        "common_start": result["common_start"].isoformat(),
        "common_end": result["common_end"].isoformat(),
        "mature_start": result["mature_start"].isoformat(),
        "mature_end": result["mature_end"].isoformat(),
        "initial_usdt": result["initial_usdt"],
        "initial_asset": result["initial_asset"],
        "initial_asset_open": result["initial_asset_open"],
        "initial_qty": result["initial_qty"],
        "first_close_equity_usdt": result["first_close_equity_usdt"],
        "legacy_compatible_return": result["legacy_compatible_return"],
        "transitions": result["transitions"],
        "conflict_signal_days": result["conflict_signal_days"],
        "route": result["route"],
        "final_asset": result["final_asset"],
        "final_qty": result["final_qty"],
        "final_equity_usdt": result["final_equity_usdt"],
        "total_return": result["total_return"],
        "no_cost_final_qty": result["no_cost_final_qty"],
        "no_cost_final_equity_usdt": result["no_cost_final_equity_usdt"],
        "no_cost_total_return": result["no_cost_total_return"],
        "terminal_fee_drag_usdt": result["terminal_fee_drag_usdt"],
        "terminal_fee_drag_pct_vs_no_cost": result["terminal_fee_drag_pct_vs_no_cost"],
        "fee_multiplier_actual_vs_no_cost": result["fee_multiplier_actual_vs_no_cost"],
        "expected_fee_multiplier": result["expected_fee_multiplier"],
    }
    (run_dir / "summary.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False),
        encoding="utf-8",
    )
    write_report(result, metadata, run_dir)

    print("run_dir=" + str(run_dir))
    print("mature_start=" + result["mature_start"].isoformat())
    print("mature_end=" + result["mature_end"].isoformat())
    print("transitions=" + str(result["transitions"]))
    print("route=" + "->".join(result["route"]))
    print("final_equity_usdt=%.8f" % result["final_equity_usdt"])
    print("total_return=%.8f" % result["total_return"])
    print("legacy_compatible_return=%.8f" % result["legacy_compatible_return"])
    print("no_cost_final_equity_usdt=%.8f" % result["no_cost_final_equity_usdt"])
    print("terminal_fee_drag_usdt=%.8f" % result["terminal_fee_drag_usdt"])
    print("terminal_fee_drag_pct=%.8f" % result["terminal_fee_drag_pct_vs_no_cost"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
