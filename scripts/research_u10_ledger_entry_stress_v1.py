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
OUT = ROOT / "research_artifacts" / "u10_ledger_entry_stress_v1"

U10 = ("ATOM", "TWT", "PEPE", "BNB", "SOL", "TRX", "AAVE", "LINK", "FIL", "HBAR")
LOOKBACK = 180
ARM = 0.15
REVERSAL = 0.03
COST = 0.001
DATA_START = pd.Timestamp("2023-01-01", tz="UTC")
START_ASSET = "ATOM"


def utc(value):
    t = pd.Timestamp(value)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def source_sha():
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def download_panel(cutoff):
    client = BinanceSpotRestClient()
    panel = None
    metadata = {}

    for asset in U10:
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
        frame = frame.rename(
            columns={"open": asset + "_open", "close": asset + "_close"}
        )
        metadata[asset] = {
            "rows": len(frame),
            "start": utc(frame.iloc[0]["timestamp"]).isoformat(),
            "end": utc(frame.iloc[-1]["timestamp"]).isoformat(),
            "listing_truncated": bool(result.metadata.listing_truncated),
        }
        panel = (
            frame
            if panel is None
            else panel.merge(frame, on="timestamp", how="inner", validate="one_to_one")
        )

    panel = panel.sort_values("timestamp", kind="stable").reset_index(drop=True)
    if len(panel) < LOOKBACK:
        raise RuntimeError(
            f"common U10 panel has only {len(panel)} rows; need at least {LOOKBACK}"
        )
    return panel, metadata


def build_events(panel):
    close_cols = ["timestamp"] + [asset + "_close" for asset in U10]
    events, _ = build_pair_monitor(
        panel[close_cols].copy(),
        assets=U10,
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
        if event["from_asset"] == current and event["to_asset"] in U10
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


def analyze_equity(eq, initial_usdt):
    values = eq["equity_usdt"].astype(float)
    running_peak = values.cummax()
    drawdown = values / running_peak - 1.0

    max_dd_idx = int(drawdown.idxmin())
    peak_idx = int(values.loc[:max_dd_idx].idxmax())
    min_idx = int(values.idxmin())

    below = values < float(initial_usdt)
    first_recovery_date = None
    recovered_after_first_below = False

    if below.any():
        first_below_idx = int(below[below].index[0])
        recovered = values.loc[first_below_idx + 1 :]
        recovered = recovered[recovered >= float(initial_usdt)]
        if not recovered.empty:
            recovered_after_first_below = True
            recovery_idx = int(recovered.index[0])
            first_recovery_date = str(eq.loc[recovery_idx, "timestamp"])

    return {
        "max_drawdown": float(drawdown.loc[max_dd_idx]),
        "max_drawdown_peak_equity_usdt": float(values.loc[peak_idx]),
        "max_drawdown_peak_date": str(eq.loc[peak_idx, "timestamp"]),
        "max_drawdown_peak_asset": str(eq.loc[peak_idx, "asset"]),
        "max_drawdown_trough_equity_usdt": float(values.loc[max_dd_idx]),
        "max_drawdown_trough_date": str(eq.loc[max_dd_idx, "timestamp"]),
        "max_drawdown_trough_asset": str(eq.loc[max_dd_idx, "asset"]),
        "max_drawdown_trough_vs_initial": float(values.loc[max_dd_idx])
        / float(initial_usdt)
        - 1.0,
        "min_equity_usdt": float(values.loc[min_idx]),
        "min_equity_date": str(eq.loc[min_idx, "timestamp"]),
        "min_equity_asset": str(eq.loc[min_idx, "asset"]),
        "min_vs_initial_return": float(values.loc[min_idx]) / float(initial_usdt) - 1.0,
        "below_initial_days": int(below.sum()),
        "recovered_after_first_below": recovered_after_first_below,
        "first_recovery_date": first_recovery_date,
    }


def run_window(
    panel,
    event_map,
    start,
    initial_usdt,
    *,
    collect_ledger=False,
    shadow_cost=False,
):
    start = utc(start)
    w = panel[panel["timestamp"] >= start].copy().reset_index(drop=True)
    if w.empty:
        raise RuntimeError(f"no U10 rows from {start.isoformat()}")

    current = START_ASSET
    first_open = float(w.iloc[0][current + "_open"])
    qty = float(initial_usdt) / first_open
    shadow_qty = qty
    pending = None
    route = [current]
    equity_rows = []
    ledger_rows = []
    conflicts = 0

    for pos, row in w.iterrows():
        ts = utc(row["timestamp"])

        if pending is not None:
            from_asset = current
            to_asset = pending["to_asset"]
            from_open = float(row[from_asset + "_open"])
            to_open = float(row[to_asset + "_open"])

            from_qty = qty
            gross = from_qty * from_open
            fee = gross * COST
            net = gross - fee
            qty = net / to_open

            shadow_from_qty = shadow_qty
            shadow_gross = shadow_from_qty * from_open
            shadow_qty = (
                shadow_gross / to_open if shadow_cost else qty
            )

            current = to_asset
            route.append(current)

            if collect_ledger:
                transition_no = len(ledger_rows) + 1
                no_cost_value = shadow_qty * to_open if shadow_cost else None
                actual_value = qty * to_open

                cumulative_drag_usdt = (
                    no_cost_value - actual_value if shadow_cost else None
                )
                cumulative_drag_pct = (
                    cumulative_drag_usdt / no_cost_value
                    if shadow_cost and no_cost_value
                    else None
                )

                if shadow_cost:
                    expected_ratio = (1.0 - COST) ** transition_no
                    actual_ratio = actual_value / no_cost_value
                    if not math.isclose(
                        actual_ratio,
                        expected_ratio,
                        rel_tol=1e-11,
                        abs_tol=1e-11,
                    ):
                        raise AssertionError(
                            "U10 fee accounting invariant failed "
                            f"at transition {transition_no}: "
                            f"actual/no-cost={actual_ratio}, expected={expected_ratio}"
                        )

                ledger_rows.append(
                    {
                        "transition_no": transition_no,
                        "signal_date": pending["signal_date"].isoformat(),
                        "execution_date": ts.isoformat(),
                        "pair": pending["pair"],
                        "from_asset": from_asset,
                        "from_qty_before": from_qty,
                        "from_open_usdt": from_open,
                        "gross_value_usdt": gross,
                        "transition_cost_rate": COST,
                        "transition_cost_usdt": fee,
                        "net_value_after_cost_usdt": net,
                        "to_asset": to_asset,
                        "to_open_usdt": to_open,
                        "to_qty_after": qty,
                        "shadow_no_cost_to_qty": shadow_qty if shadow_cost else None,
                        "cumulative_fee_drag_usdt": cumulative_drag_usdt,
                        "cumulative_fee_drag_pct_vs_no_cost": cumulative_drag_pct,
                        "max_dislocation": float(pending["max_dislocation"]),
                        "candidate_count_on_signal_day": int(
                            pending["candidate_count"]
                        ),
                    }
                )

            pending = None

        close_price = float(row[current + "_close"])
        equity_rows.append(
            {
                "timestamp": ts.isoformat(),
                "asset": current,
                "qty": qty,
                "close": close_price,
                "equity_usdt": qty * close_price,
            }
        )

        if pos == len(w) - 1:
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

    eq = pd.DataFrame(equity_rows)
    ledger = pd.DataFrame(ledger_rows)

    risk = analyze_equity(eq, initial_usdt)
    final_equity = float(eq.iloc[-1]["equity_usdt"])

    result = {
        "start": str(eq.iloc[0]["timestamp"]),
        "end": str(eq.iloc[-1]["timestamp"]),
        "initial_usdt": float(initial_usdt),
        "initial_asset": START_ASSET,
        "initial_open_usdt": first_open,
        "initial_qty": float(initial_usdt) / first_open,
        "transitions": int(len(route) - 1),
        "conflicts": int(conflicts),
        "route": route,
        "final_asset": current,
        "final_qty": float(qty),
        "final_equity_usdt": final_equity,
        "total_return": final_equity / float(initial_usdt) - 1.0,
        **risk,
    }

    if shadow_cost:
        final_shadow_equity = shadow_qty * float(
            w.iloc[-1][current + "_close"]
        )
        expected_ratio = (1.0 - COST) ** result["transitions"]
        actual_ratio = final_equity / final_shadow_equity

        if not math.isclose(
            actual_ratio,
            expected_ratio,
            rel_tol=1e-11,
            abs_tol=1e-11,
        ):
            raise AssertionError(
                "U10 terminal fee accounting invariant failed: "
                f"actual/no-cost={actual_ratio}, expected={expected_ratio}"
            )

        result.update(
            {
                "no_cost_final_qty": float(shadow_qty),
                "no_cost_final_equity_usdt": float(final_shadow_equity),
                "no_cost_total_return": float(final_shadow_equity)
                / float(initial_usdt)
                - 1.0,
                "terminal_fee_drag_usdt": float(final_shadow_equity)
                - final_equity,
                "terminal_fee_drag_pct_vs_no_cost": (
                    float(final_shadow_equity) - final_equity
                )
                / float(final_shadow_equity),
                "fee_multiplier_actual_vs_no_cost": actual_ratio,
                "expected_fee_multiplier": expected_ratio,
            }
        )

    return result, ledger, eq


def scenario_start_dates(long_result, mature_start):
    peak = utc(long_result["max_drawdown_peak_date"])
    trough = utc(long_result["max_drawdown_trough_date"])

    candidates = [
        (
            "ONE_YEAR_BEFORE_PEAK",
            peak - pd.DateOffset(years=1),
        ),
        (
            "PEAK_YEAR_START",
            pd.Timestamp(year=peak.year, month=1, day=1, tz="UTC"),
        ),
        (
            "AT_PEAK_DATE",
            peak,
        ),
        (
            "TROUGH_YEAR_START",
            pd.Timestamp(year=trough.year, month=1, day=1, tz="UTC"),
        ),
    ]

    out = []
    for name, requested in candidates:
        actual = max(utc(requested), utc(mature_start))
        out.append((name, utc(requested), actual))
    return out


def pct(value):
    return f"{100 * value:+.2f}%"


def write_report(payload, run_dir):
    long_result = payload["long_path"]
    lines = [
        "# U10 Ledger + Entry Stress v1",
        "",
        "Mode: PATCH_FIX / research reporting only",
        "Live strategy logic: UNCHANGED",
        "",
        "## U10 definition",
        "",
        ", ".join(U10),
        "",
        "## Long mature path",
        "",
        f"- Common panel: {pd.Timestamp(payload['common_start']).date()} -> {pd.Timestamp(payload['common_end']).date()}",
        f"- Mature capital start: {pd.Timestamp(long_result['start']).date()}",
        f"- Initial capital: {long_result['initial_usdt']:,.2f} USDT",
        f"- Initial position: {long_result['initial_qty']:,.8f} ATOM @ {long_result['initial_open_usdt']:.8f} USDT",
        f"- Final equity: {long_result['final_equity_usdt']:,.2f} USDT ({pct(long_result['total_return'])})",
        f"- Executed transitions: {long_result['transitions']}",
        f"- Lowest equity vs initial: {long_result['min_equity_usdt']:,.2f} USDT on {pd.Timestamp(long_result['min_equity_date']).date()} = {pct(long_result['min_vs_initial_return'])}",
        f"- Daily closes below initial capital: {long_result['below_initial_days']}",
        f"- Max drawdown from prior peak: {pct(long_result['max_drawdown'])}",
        f"- Max-DD peak: {long_result['max_drawdown_peak_equity_usdt']:,.2f} USDT on {pd.Timestamp(long_result['max_drawdown_peak_date']).date()} while holding {long_result['max_drawdown_peak_asset']}",
        f"- Max-DD trough: {long_result['max_drawdown_trough_equity_usdt']:,.2f} USDT on {pd.Timestamp(long_result['max_drawdown_trough_date']).date()} while holding {long_result['max_drawdown_trough_asset']}",
        f"- Max-DD trough vs original 10,000: {pct(long_result['max_drawdown_trough_vs_initial'])}",
        "",
        "## Transition-cost shadow",
        "",
        f"- Cost-adjusted final equity: {long_result['final_equity_usdt']:,.2f} USDT",
        f"- Same route with zero modeled transition cost: {long_result['no_cost_final_equity_usdt']:,.2f} USDT",
        f"- Terminal fee drag: {long_result['terminal_fee_drag_usdt']:,.2f} USDT",
        f"- Drag vs zero-cost shadow: {100*long_result['terminal_fee_drag_pct_vs_no_cost']:.4f}%",
        f"- Accounting identity actual/no-cost: {long_result['fee_multiplier_actual_vs_no_cost']:.12f}",
        f"- Expected (1-cost)^N: {long_result['expected_fee_multiplier']:.12f}",
        "",
        "## Fresh-entry stress around U10's own strongest drawdown",
        "",
        "| Scenario | Start | Final equity | Return | Minimum equity | Min vs initial | Days below 10k | Max DD | DD trough vs initial | Transitions |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]

    for s in payload["fresh_entry_scenarios"]:
        lines.append(
            f"| {s['scenario']} | {pd.Timestamp(s['start']).date()} | "
            f"{s['final_equity_usdt']:,.2f} | {pct(s['total_return'])} | "
            f"{s['min_equity_usdt']:,.2f} | {pct(s['min_vs_initial_return'])} | "
            f"{s['below_initial_days']} | {pct(s['max_drawdown'])} | "
            f"{pct(s['max_drawdown_trough_vs_initial'])} | {s['transitions']} |"
        )

    lines += [
        "",
        "## Long-path route",
        "",
        " -> ".join(long_result["route"]),
        "",
        "## Interpretation boundary",
        "",
        "- MAX DRAWDOWN FROM PRIOR PEAK is not the same as loss versus original capital.",
        "- Fresh-entry scenarios each restart at 10,000 USDT in ATOM.",
        "- Historical monitor warm-up/state is retained before each fresh capital start.",
        "- The drawdown anchor is discovered from U10 itself, not copied from U8.",
        "- Transition cost is 0.1% on each executed rotation.",
        "- Additional real-world spread/slippage is not separately modeled.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_BACKTEST",
    ]

    (run_dir / "report.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff", default="2026-09-27T00:00:00Z")
    parser.add_argument("--initial-usdt", type=float, default=10000.0)
    args = parser.parse_args()

    if args.initial_usdt <= 0:
        raise ValueError("--initial-usdt must be > 0")

    cutoff = utc(args.cutoff)
    panel, metadata = download_panel(cutoff)
    event_map = build_events(panel)

    mature_start = utc(panel.iloc[LOOKBACK - 1]["timestamp"])

    long_result, long_ledger, long_equity = run_window(
        panel,
        event_map,
        mature_start,
        args.initial_usdt,
        collect_ledger=True,
        shadow_cost=True,
    )

    scenarios = []
    scenario_equities = []
    scenario_transitions = []

    for name, requested_start, actual_start in scenario_start_dates(
        long_result, mature_start
    ):
        result, transitions, equity = run_window(
            panel,
            event_map,
            actual_start,
            args.initial_usdt,
            collect_ledger=True,
            shadow_cost=False,
        )
        result["scenario"] = name
        result["requested_start"] = requested_start.isoformat()
        scenarios.append(result)

        eq = equity.copy()
        eq.insert(0, "scenario", name)
        scenario_equities.append(eq)

        if not transitions.empty:
            tr = transitions.copy()
            tr.insert(0, "scenario", name)
            scenario_transitions.append(tr)

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    long_ledger.to_csv(run_dir / "long_path_transitions.csv", index=False)
    long_equity.to_csv(run_dir / "long_path_equity.csv", index=False)

    pd.DataFrame(
        [
            {
                "scenario": s["scenario"],
                "requested_start": s["requested_start"],
                "actual_start": s["start"],
                "end": s["end"],
                "final_equity_usdt": s["final_equity_usdt"],
                "total_return": s["total_return"],
                "min_equity_usdt": s["min_equity_usdt"],
                "min_equity_date": s["min_equity_date"],
                "min_vs_initial_return": s["min_vs_initial_return"],
                "below_initial_days": s["below_initial_days"],
                "max_drawdown": s["max_drawdown"],
                "max_drawdown_peak_equity_usdt": s[
                    "max_drawdown_peak_equity_usdt"
                ],
                "max_drawdown_peak_date": s["max_drawdown_peak_date"],
                "max_drawdown_trough_equity_usdt": s[
                    "max_drawdown_trough_equity_usdt"
                ],
                "max_drawdown_trough_date": s["max_drawdown_trough_date"],
                "max_drawdown_trough_vs_initial": s[
                    "max_drawdown_trough_vs_initial"
                ],
                "transitions": s["transitions"],
            }
            for s in scenarios
        ]
    ).to_csv(run_dir / "fresh_entry_summary.csv", index=False)

    if scenario_equities:
        pd.concat(scenario_equities, ignore_index=True).to_csv(
            run_dir / "fresh_entry_equity.csv", index=False
        )
    if scenario_transitions:
        pd.concat(scenario_transitions, ignore_index=True).to_csv(
            run_dir / "fresh_entry_transitions.csv", index=False
        )

    payload = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit": source_sha(),
        "u10": list(U10),
        "parameters": {
            "lookback": LOOKBACK,
            "arm": ARM,
            "reversal": REVERSAL,
            "transition_cost": COST,
            "execution": "next_open",
            "router": "strongest_confirmed_max_dislocation",
            "initial_usdt": float(args.initial_usdt),
        },
        "data_metadata": metadata,
        "common_start": utc(panel.iloc[0]["timestamp"]).isoformat(),
        "common_end": utc(panel.iloc[-1]["timestamp"]).isoformat(),
        "mature_start": mature_start.isoformat(),
        "long_path": long_result,
        "fresh_entry_scenarios": scenarios,
    }

    (run_dir / "results.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False),
        encoding="utf-8",
    )
    write_report(payload, run_dir)

    print("run_dir=" + str(run_dir))
    print("mature_start=" + long_result["start"])
    print("mature_end=" + long_result["end"])
    print("long_final=%.8f" % long_result["final_equity_usdt"])
    print("long_return=%.8f" % long_result["total_return"])
    print("long_min=%.8f" % long_result["min_equity_usdt"])
    print("long_min_vs_initial=%.8f" % long_result["min_vs_initial_return"])
    print("long_max_dd=%.8f" % long_result["max_drawdown"])
    print("long_dd_peak=" + long_result["max_drawdown_peak_date"])
    print("long_dd_trough=" + long_result["max_drawdown_trough_date"])
    print("long_transitions=" + str(long_result["transitions"]))
    print(
        "long_fee_drag_pct=%.8f"
        % long_result["terminal_fee_drag_pct_vs_no_cost"]
    )
    print("long_route=" + "->".join(long_result["route"]))

    for s in scenarios:
        print(
            f"{s['scenario']}: start={s['start']} "
            f"final={s['final_equity_usdt']:.2f} "
            f"return={s['total_return']:.6f} "
            f"min={s['min_equity_usdt']:.2f} "
            f"min_vs_initial={s['min_vs_initial_return']:.6f} "
            f"max_dd={s['max_drawdown']:.6f} "
            f"dd_trough_vs_initial={s['max_drawdown_trough_vs_initial']:.6f} "
            f"transitions={s['transitions']}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
