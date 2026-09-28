from __future__ import annotations

import argparse
import itertools
import json
from collections import defaultdict
from pathlib import Path

import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import build_pair_monitor
from scripts.research_u10_last_year_vs_u8_universes_v1 import (
    ARM, COST, DATA_START, LOOKBACK, MANDATORY, POOL, REVERSAL, utc,
)
from scripts.research_u10_monthly_surge_pullback_v1 import (
    CANONICAL_U10, CASH_FRACTION, INITIAL_USDT,
    all_u10_universes, analyze_values, monthly_state,
    run_overlay, run_reference, source_sha, universe_key,
)
from scripts.research_u10_monthly_surge_trailing_reentry_peak_v1 import (
    run_overlay_trailing_peak,
)

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "u10_monthly_surge_fractal_buystop_v1"

SEARCH_DRAWDOWN = 0.15
PIVOT_LEFT = 2
PIVOT_RIGHT = 2
P1 = {"name": "P1_FRACTAL", "surge": 1.00, "pullback": 0.05}
P2 = {"name": "P2_FRACTAL", "surge": 1.00, "pullback": 0.10}


def download_ohlc_panel(cutoff: pd.Timestamp):
    client = BinanceSpotRestClient()
    panel = None
    meta = {}
    for asset in POOL:
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
        f = result.dataset.candles[
            ["timestamp", "open", "high", "low", "close"]
        ].copy()
        f["timestamp"] = pd.to_datetime(f["timestamp"], utc=True)
        f = f.rename(
            columns={
                "open": asset + "_open",
                "high": asset + "_high",
                "low": asset + "_low",
                "close": asset + "_close",
            }
        )
        meta[asset] = {
            "rows": int(len(f)),
            "start": utc(f.iloc[0]["timestamp"]).isoformat(),
            "end": utc(f.iloc[-1]["timestamp"]).isoformat(),
        }
        panel = (
            f if panel is None
            else panel.merge(f, on="timestamp", how="inner", validate="one_to_one")
        )
    panel = panel.sort_values("timestamp", kind="stable").reset_index(drop=True)
    return panel, meta


def build_events(panel: pd.DataFrame):
    cols = ["timestamp"] + [a + "_close" for a in POOL]
    events, _ = build_pair_monitor(
        panel[cols].copy(),
        assets=POOL,
        lookback=LOOKBACK,
        arm_threshold=ARM,
        reversal=REVERSAL,
    )
    by_date = defaultdict(list)
    for event in events:
        if event["event"] == "CONFIRMED":
            by_date[utc(event["date"])].append(event)
    return by_date


def is_confirmed_pivot_high(
    panel: pd.DataFrame,
    reference: pd.DataFrame,
    confirm_idx: int,
    asset: str,
    segment_start_idx: int,
    search_start_idx: int,
):
    center = confirm_idx - PIVOT_RIGHT
    left0 = center - PIVOT_LEFT
    right2 = center + PIVOT_RIGHT

    if left0 < 0 or right2 != confirm_idx:
        return None
    if left0 < segment_start_idx:
        return None
    if center < search_start_idx:
        return None

    held = reference.loc[left0:right2, "asset"].tolist()
    if any(x != asset for x in held):
        return None

    highs = [
        float(panel.loc[k, asset + "_high"])
        for k in range(left0, right2 + 1)
    ]
    center_high = highs[PIVOT_LEFT]
    if not all(
        center_high > highs[k]
        for k in range(len(highs))
        if k != PIVOT_LEFT
    ):
        return None

    return {
        "pivot_center_idx": center,
        "pivot_center_date": utc(reference.loc[center, "timestamp"]).isoformat(),
        "confirmation_idx": confirm_idx,
        "confirmation_date": utc(reference.loc[confirm_idx, "timestamp"]).isoformat(),
        "pivot_high": center_high,
    }


def run_fractal_overlay(
    panel_eval: pd.DataFrame,
    reference: pd.DataFrame,
    anchor_by_month: dict[str, float],
    *,
    surge_threshold: float,
    cashout_pullback: float,
    initial_usdt: float,
):
    n = len(reference)
    if n != len(panel_eval):
        raise RuntimeError(f"panel/reference mismatch {n} != {len(panel_eval)}")
    if n == 0:
        raise RuntimeError("empty reference")

    timestamps = [utc(x) for x in reference["timestamp"]]
    months = [x.strftime("%Y-%m") for x in timestamps]
    ref_open_equity = reference["open_equity_usdt"].astype(float).tolist()
    ref_close_equity = reference["equity_usdt"].astype(float).tolist()
    ref_qty = reference["asset_qty"].astype(float).tolist()
    ref_asset = reference["asset"].astype(str).tolist()

    scale = 1.0
    cash = 0.0

    armed = False
    arm_month = None
    arm_date = None
    surge_running_peak = None
    cashout_pending = False
    cashout_signal = None
    used_surge_months: set[str] = set()

    cashouts = 0
    reentries = 0
    days_in_cash = 0
    search_activations = 0
    confirmed_pivots = 0
    stop_replacements = 0
    gap_open_fills = 0
    stop_price_fills = 0

    ref_peak_while_cash = None
    search_active = False
    search_activation_idx = None
    search_activation_date = None
    current_cash_asset = None
    asset_segment_start_idx = None
    stop_price = None
    stop_active_from_idx = None
    stop_pivot_date = None
    stop_confirmation_date = None
    open_cycle = None
    cycles = []

    values = []
    total_cashout_fees = 0.0
    total_reentry_fees = 0.0

    for i in range(n):
        ts = timestamps[i]
        month = months[i]
        asset = ref_asset[i]

        # Frozen U10 has already executed any scheduled rotation at this open.
        if cash > 0 and current_cash_asset is not None and asset != current_cash_asset:
            current_cash_asset = asset
            asset_segment_start_idx = i
            stop_price = None
            stop_active_from_idx = None
            stop_pivot_date = None
            stop_confirmation_date = None
            if open_cycle is not None:
                open_cycle["asset_resets"] += 1

        # Execute pending partial cash-out at current open.
        if cashout_pending:
            gross = scale * ref_open_equity[i] * CASH_FRACTION
            fee = gross * COST
            net = gross - fee
            scale *= 1.0 - CASH_FRACTION
            cash += net
            total_cashout_fees += fee
            cashouts += 1
            cashout_pending = False

            ref_peak_while_cash = float(cashout_signal["locked_peak_usdt"])
            search_active = False
            search_activation_idx = None
            search_activation_date = None
            current_cash_asset = asset
            asset_segment_start_idx = i
            stop_price = None
            stop_active_from_idx = None
            stop_pivot_date = None
            stop_confirmation_date = None

            open_cycle = {
                "arm_month": cashout_signal["arm_month"],
                "arm_date": cashout_signal["arm_date"],
                "cashout_signal_date": cashout_signal["signal_date"],
                "cashout_execution_date": ts.isoformat(),
                "initial_locked_peak_usdt": ref_peak_while_cash,
                "cashout_reference_equity_signal_usdt": cashout_signal[
                    "reference_equity_signal_usdt"
                ],
                "cashout_pullback_from_peak": cashout_signal["pullback_from_peak"],
                "cashout_asset": asset,
                "gross_cashout_usdt": gross,
                "cashout_fee_usdt": fee,
                "net_cash_parked_usdt": net,
                "asset_resets": 0,
                "confirmed_pivots": 0,
                "stop_replacements": 0,
            }

        # A previously confirmed stop is executable from today's session.
        if (
            cash > 0
            and search_active
            and stop_price is not None
            and stop_active_from_idx is not None
            and i >= stop_active_from_idx
        ):
            open_price = float(panel_eval.loc[i, asset + "_open"])
            high_price = float(panel_eval.loc[i, asset + "_high"])
            fill_price = None
            fill_type = None

            if open_price >= stop_price:
                fill_price = open_price
                fill_type = "GAP_OPEN"
            elif high_price >= stop_price:
                fill_price = float(stop_price)
                fill_type = "STOP_PRICE"

            if fill_price is not None:
                gross_cash = cash
                fee = gross_cash * COST
                net = gross_cash - fee
                bought_qty = net / fill_price
                if ref_qty[i] <= 0:
                    raise RuntimeError("non-positive reference qty")
                scale += bought_qty / ref_qty[i]
                cash = 0.0
                total_reentry_fees += fee
                reentries += 1

                if fill_type == "GAP_OPEN":
                    gap_open_fills += 1
                else:
                    stop_price_fills += 1

                latest_ref_peak = float(ref_peak_while_cash)
                reference_drawdown = (
                    ref_close_equity[i] / latest_ref_peak - 1.0
                    if latest_ref_peak > 0 else None
                )

                if open_cycle is not None:
                    open_cycle.update(
                        {
                            "search_activation_date": search_activation_date,
                            "final_reference_peak_usdt": latest_ref_peak,
                            "reentry_execution_date": ts.isoformat(),
                            "reentry_asset": asset,
                            "reentry_fill_type": fill_type,
                            "reentry_stop_price_usdt": float(stop_price),
                            "reentry_fill_price_usdt": float(fill_price),
                            "reentry_stop_pivot_date": stop_pivot_date,
                            "reentry_stop_confirmation_date": stop_confirmation_date,
                            "reentry_fee_usdt": fee,
                            "reference_drawdown_at_reentry_close": reference_drawdown,
                            "cash_cycle_days": int(
                                (ts - utc(open_cycle["cashout_execution_date"])).days
                            ),
                        }
                    )
                    cycles.append(open_cycle)

                # Cycle complete.
                open_cycle = None
                ref_peak_while_cash = None
                search_active = False
                search_activation_idx = None
                search_activation_date = None
                current_cash_asset = None
                asset_segment_start_idx = None
                stop_price = None
                stop_active_from_idx = None
                stop_pivot_date = None
                stop_confirmation_date = None
                armed = False
                arm_month = None
                arm_date = None
                surge_running_peak = None

        invested_close = scale * ref_close_equity[i]
        total_close = invested_close + cash
        values.append(total_close)
        if cash > 0:
            days_in_cash += 1

        if i == n - 1:
            continue

        if cash > 0:
            if ref_close_equity[i] > ref_peak_while_cash:
                ref_peak_while_cash = ref_close_equity[i]

            if not search_active:
                dd = ref_close_equity[i] / ref_peak_while_cash - 1.0
                if dd <= -SEARCH_DRAWDOWN:
                    search_active = True
                    search_activation_idx = i
                    search_activation_date = ts.isoformat()
                    search_activations += 1
                    if open_cycle is not None:
                        open_cycle["search_activation_date"] = search_activation_date
                        open_cycle["search_activation_reference_dd"] = dd

            if search_active:
                pivot = is_confirmed_pivot_high(
                    panel_eval,
                    reference,
                    i,
                    asset,
                    asset_segment_start_idx,
                    search_activation_idx,
                )
                if pivot is not None:
                    confirmed_pivots += 1
                    if open_cycle is not None:
                        open_cycle["confirmed_pivots"] += 1

                    candidate = float(pivot["pivot_high"])
                    if stop_price is None:
                        stop_price = candidate
                        stop_active_from_idx = i + 1
                        stop_pivot_date = pivot["pivot_center_date"]
                        stop_confirmation_date = pivot["confirmation_date"]
                    elif candidate < stop_price:
                        stop_price = candidate
                        stop_active_from_idx = i + 1
                        stop_pivot_date = pivot["pivot_center_date"]
                        stop_confirmation_date = pivot["confirmation_date"]
                        stop_replacements += 1
                        if open_cycle is not None:
                            open_cycle["stop_replacements"] += 1
            continue

        if armed:
            if ref_close_equity[i] > surge_running_peak:
                surge_running_peak = ref_close_equity[i]
            pullback = ref_close_equity[i] / surge_running_peak - 1.0
            if pullback <= -cashout_pullback:
                cashout_signal = {
                    "signal_date": ts.isoformat(),
                    "arm_month": arm_month,
                    "arm_date": arm_date,
                    "locked_peak_usdt": surge_running_peak,
                    "reference_equity_signal_usdt": ref_close_equity[i],
                    "pullback_from_peak": pullback,
                }
                cashout_pending = True
                used_surge_months.add(arm_month)
            continue

        if month in used_surge_months:
            continue
        anchor = anchor_by_month.get(month)
        if anchor is None or anchor <= 0:
            continue
        gain = ref_close_equity[i] / anchor - 1.0
        if gain >= surge_threshold:
            armed = True
            arm_month = month
            arm_date = ts.isoformat()
            surge_running_peak = ref_close_equity[i]

    if open_cycle is not None:
        open_cycle.update(
            {
                "final_reference_peak_usdt": ref_peak_while_cash,
                "reentry_execution_date": None,
                "reentry_asset": None,
                "reentry_fill_type": None,
                "reentry_stop_price_usdt": stop_price,
                "reentry_fill_price_usdt": None,
                "reentry_stop_pivot_date": stop_pivot_date,
                "reentry_stop_confirmation_date": stop_confirmation_date,
                "reentry_fee_usdt": None,
                "reference_drawdown_at_reentry_close": None,
                "cash_cycle_days": int(
                    (timestamps[-1] - utc(open_cycle["cashout_execution_date"])).days
                ),
            }
        )
        cycles.append(open_cycle)

    risk = analyze_values(timestamps, values, initial_usdt)
    result = {
        "surge_threshold": surge_threshold,
        "cashout_pullback": cashout_pullback,
        "cash_fraction": CASH_FRACTION,
        "search_drawdown": SEARCH_DRAWDOWN,
        "cashouts": cashouts,
        "reentries": reentries,
        "search_activations": search_activations,
        "confirmed_pivots": confirmed_pivots,
        "stop_replacements": stop_replacements,
        "gap_open_fills": gap_open_fills,
        "stop_price_fills": stop_price_fills,
        "unfinished_cycles": int(cash > 0),
        "days_in_cash": days_in_cash,
        "terminal_cash_usdt": cash,
        "cashout_fees_usdt": total_cashout_fees,
        "reentry_fees_usdt": total_reentry_fees,
        **risk,
    }
    return result, pd.DataFrame(cycles)


def summarize_alternatives(df: pd.DataFrame):
    alt = df[~df["is_canonical"]].copy()
    return {
        "count": int(len(alt)),
        "final_gt_baseline_rate": float(
            (alt["fractal_delta_vs_baseline_usdt"] > 0).mean()
        ),
        "dd_better_baseline_rate": float(
            (alt["fractal_dd_improvement_pp"] > 0).mean()
        ),
        "both_better_baseline_rate": float(
            (
                (alt["fractal_delta_vs_baseline_usdt"] > 0)
                & (alt["fractal_dd_improvement_pp"] > 0)
            ).mean()
        ),
        "fractal_gt_fixed25_rate": float(
            (alt["fractal_delta_vs_fixed25_usdt"] > 0).mean()
        ),
        "fractal_gt_trailing25_rate": float(
            (alt["fractal_delta_vs_trailing25_usdt"] > 0).mean()
        ),
        "unfinished_rate": float(
            (alt["fractal_unfinished_cycles"] > 0).mean()
        ),
        "median_delta_vs_baseline_pct": float(
            alt["fractal_delta_vs_baseline_pct"].median()
        ),
        "q25_delta_vs_baseline_pct": float(
            alt["fractal_delta_vs_baseline_pct"].quantile(0.25)
        ),
        "q75_delta_vs_baseline_pct": float(
            alt["fractal_delta_vs_baseline_pct"].quantile(0.75)
        ),
        "median_delta_vs_fixed25_pct": float(
            alt["fractal_delta_vs_fixed25_pct"].median()
        ),
        "median_delta_vs_trailing25_pct": float(
            alt["fractal_delta_vs_trailing25_pct"].median()
        ),
        "median_days_in_cash": float(alt["fractal_days_in_cash"].median()),
        "median_reentries": float(alt["fractal_reentries"].median()),
    }


def pct(x):
    return f"{100*x:+.2f}%"


def write_report(payload: dict, run_dir: Path):
    lines = [
        "# U10 Monthly Surge Fractal Buy-Stop Re-entry v1",
        "",
        "Mode: STRESS_TEST_ONLY",
        "Live/paper logic: UNCHANGED",
        "",
        "Rule: after +100% surge and 30% cash-out, start searching only after a 15% U10 reference drawdown. Re-enter through a next-session buy-stop above the latest confirmed 2-left/2-right swing high of the currently held U10 token. Lower confirmed swing highs ratchet the stop downward.",
        "",
        "## Canonical U10",
        "",
        "| Variant | Baseline | Fixed -25% | Trailing-peak -25% | Fractal buy-stop | Fractal vs baseline | Max DD fractal | Reentries | Cash days |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name in ("P1", "P2"):
        row = payload["canonical"][name]
        lines.append(
            f"| {name} | {row['baseline_final_equity_usdt']:,.2f} | "
            f"{row['fixed25_final_equity_usdt']:,.2f} | "
            f"{row['trailing25_final_equity_usdt']:,.2f} | "
            f"{row['fractal_final_equity_usdt']:,.2f} | "
            f"{pct(row['fractal_delta_vs_baseline_pct'])} | "
            f"{pct(row['fractal_max_drawdown'])} | "
            f"{row['fractal_reentries']} | {row['fractal_days_in_cash']} |"
        )

    lines += [
        "",
        "## 791 alternative U10s",
        "",
        "| Variant | Final > baseline | DD better | Both better | Fractal > fixed -25 | Fractal > trailing -25 | Unfinished | Median delta vs baseline |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name in ("P1", "P2"):
        row = payload["alternative_summary"][name]
        lines.append(
            f"| {name} | {100*row['final_gt_baseline_rate']:.2f}% | "
            f"{100*row['dd_better_baseline_rate']:.2f}% | "
            f"{100*row['both_better_baseline_rate']:.2f}% | "
            f"{100*row['fractal_gt_fixed25_rate']:.2f}% | "
            f"{100*row['fractal_gt_trailing25_rate']:.2f}% | "
            f"{100*row['unfinished_rate']:.2f}% | "
            f"{pct(row['median_delta_vs_baseline_pct'])} |"
        )

    lines += [
        "",
        "## Execution integrity",
        "",
        "- A pivot is known only after two right-side bars close.",
        "- The stop becomes executable only from the following session.",
        "- If session open gaps above the stop, fill uses the open, not the more favorable stop.",
        "- If U10 rotates assets, the old stop is cancelled before the session fill check.",
        "- No 2020-2022 result was used to select these parameters.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    ]
    (run_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff", default="2026-09-27T00:00:00Z")
    args = parser.parse_args()

    panel, metadata = download_ohlc_panel(utc(args.cutoff))
    events = build_events(panel)
    mature_start = utc(panel.iloc[LOOKBACK - 1]["timestamp"])

    panel_eval = panel[panel["timestamp"] >= mature_start].copy().reset_index(drop=True)

    canonical_key = universe_key(CANONICAL_U10)
    rows = []
    cycle_tables = []
    canonical = {}

    for idx, assets in enumerate(all_u10_universes(), start=1):
        key = universe_key(assets)
        is_canonical = key == canonical_key
        baseline, reference = run_reference(
            panel, events, assets, mature_start, INITIAL_USDT
        )
        if len(reference) != len(panel_eval):
            raise RuntimeError("reference/panel evaluation length mismatch")
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
            fractal, cycles = run_fractal_overlay(
                panel_eval,
                reference.reset_index(drop=True),
                anchors,
                surge_threshold=cfg["surge"],
                cashout_pullback=cfg["pullback"],
                initial_usdt=INITIAL_USDT,
            )

            row = {
                "universe_key": key,
                "variant": label,
                "is_canonical": is_canonical,
                "baseline_final_equity_usdt": baseline["final_equity_usdt"],
                "baseline_max_drawdown": baseline["max_drawdown"],
                "fixed25_final_equity_usdt": fixed25["final_equity_usdt"],
                "trailing25_final_equity_usdt": trailing25["final_equity_usdt"],
                "fractal_final_equity_usdt": fractal["final_equity_usdt"],
                "fractal_max_drawdown": fractal["max_drawdown"],
                "fractal_minimum_vs_initial": fractal["minimum_vs_initial"],
                "fractal_cashouts": fractal["cashouts"],
                "fractal_reentries": fractal["reentries"],
                "fractal_search_activations": fractal["search_activations"],
                "fractal_confirmed_pivots": fractal["confirmed_pivots"],
                "fractal_stop_replacements": fractal["stop_replacements"],
                "fractal_gap_open_fills": fractal["gap_open_fills"],
                "fractal_stop_price_fills": fractal["stop_price_fills"],
                "fractal_unfinished_cycles": fractal["unfinished_cycles"],
                "fractal_days_in_cash": fractal["days_in_cash"],
                "fractal_delta_vs_baseline_usdt": (
                    fractal["final_equity_usdt"] - baseline["final_equity_usdt"]
                ),
                "fractal_delta_vs_baseline_pct": (
                    fractal["final_equity_usdt"] / baseline["final_equity_usdt"] - 1
                ),
                "fractal_dd_improvement_pp": (
                    fractal["max_drawdown"] - baseline["max_drawdown"]
                ) * 100,
                "fractal_delta_vs_fixed25_usdt": (
                    fractal["final_equity_usdt"] - fixed25["final_equity_usdt"]
                ),
                "fractal_delta_vs_fixed25_pct": (
                    fractal["final_equity_usdt"] / fixed25["final_equity_usdt"] - 1
                ),
                "fractal_delta_vs_trailing25_usdt": (
                    fractal["final_equity_usdt"] - trailing25["final_equity_usdt"]
                ),
                "fractal_delta_vs_trailing25_pct": (
                    fractal["final_equity_usdt"] / trailing25["final_equity_usdt"] - 1
                ),
            }
            rows.append(row)
            if is_canonical:
                canonical[label] = dict(row)

            if not cycles.empty:
                cc = cycles.copy()
                cc.insert(0, "variant", label)
                cc.insert(0, "universe_key", key)
                cycle_tables.append(cc)

        if idx % 100 == 0:
            print(f"processed_universes={idx}")

    df = pd.DataFrame(rows)
    alternative_summary = {
        label: summarize_alternatives(df[df["variant"] == label].copy())
        for label in ("P1", "P2")
    }

    cycles_df = pd.concat(cycle_tables, ignore_index=True) if cycle_tables else pd.DataFrame()

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(run_dir / "all_792_fractal_comparison.csv", index=False)
    if not cycles_df.empty:
        cycles_df.to_csv(run_dir / "all_fractal_cycles.csv", index=False)

    canonical_cycles = (
        cycles_df[cycles_df["universe_key"] == canonical_key].copy()
        if not cycles_df.empty else pd.DataFrame()
    )
    if not canonical_cycles.empty:
        canonical_cycles.to_csv(run_dir / "canonical_fractal_cycles.csv", index=False)

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
            "search_drawdown": SEARCH_DRAWDOWN,
            "pivot_left": PIVOT_LEFT,
            "pivot_right": PIVOT_RIGHT,
            "cost": COST,
        },
        "canonical": canonical,
        "alternative_summary": alternative_summary,
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
        s = alternative_summary[label]
        print(
            "%s CAN base=%.8f fixed25=%.8f trailing25=%.8f fractal=%.8f "
            "delta=%.8f dd=%.8f cash=%d re=%d unfinished=%d"
            % (
                label,
                c["baseline_final_equity_usdt"],
                c["fixed25_final_equity_usdt"],
                c["trailing25_final_equity_usdt"],
                c["fractal_final_equity_usdt"],
                c["fractal_delta_vs_baseline_pct"],
                c["fractal_max_drawdown"],
                c["fractal_days_in_cash"],
                c["fractal_reentries"],
                c["fractal_unfinished_cycles"],
            )
        )
        print(
            "%s ALT finalbase=%.6f both=%.6f beatfixed=%.6f beattrail=%.6f "
            "unfinished=%.6f meddelta=%.6f"
            % (
                label,
                s["final_gt_baseline_rate"],
                s["both_better_baseline_rate"],
                s["fractal_gt_fixed25_rate"],
                s["fractal_gt_trailing25_rate"],
                s["unfinished_rate"],
                s["median_delta_vs_baseline_pct"],
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
