from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

import scripts.research_u10_monthly_surge_fractal_buystop_v1 as fractal
from scripts.research_u10_multimonth_streak95_v1 import (
    P1,
    P2,
    SEARCH_DRAWDOWN,
    STREAK_THRESHOLD,
    MIN_STREAK_MONTHS,
    build_streak_events,
    run_streak_overlay,
)
from scripts.research_u10_monthly_surge_pullback_v1 import (
    CANONICAL_U10,
    CASH_FRACTION,
    INITIAL_USDT,
    all_u10_universes,
    monthly_state,
    run_reference,
    source_sha,
    universe_key,
)
from scripts.research_u10_last_year_vs_u8_universes_v1 import COST, LOOKBACK, utc

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "u10_streak95_recovery15_v1"

FALLBACK_DAYS = 15
RECOVERY_RATIO = 0.98


def run_streak_recovery_overlay(
    panel_eval: pd.DataFrame,
    reference: pd.DataFrame,
    streak_events: list[dict],
    *,
    cashout_pullback: float,
) -> tuple[dict, pd.DataFrame]:
    n = len(reference)
    if n != len(panel_eval):
        raise RuntimeError("panel/reference length mismatch")

    timestamps = [utc(x) for x in reference["timestamp"]]
    ref_open_equity = reference["open_equity_usdt"].astype(float).tolist()
    ref_close_equity = reference["equity_usdt"].astype(float).tolist()
    ref_qty = reference["asset_qty"].astype(float).tolist()
    ref_asset = reference["asset"].astype(str).tolist()

    event_by_idx = {int(e["signal_idx"]): e for e in streak_events}

    scale = 1.0
    cash = 0.0

    armed = False
    active_event = None
    running_peak = None
    cashout_pending = False
    cashout_signal = None

    cashouts = 0
    reentries = 0
    skipped_events = 0
    days_in_cash = 0
    search_activations = 0
    confirmed_pivots = 0
    stop_replacements = 0
    fractal_reentries = 0
    recovery_reentries = 0
    recovery_immediate = 0
    recovery_cross = 0

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

    cashout_execution_ts = None
    cashout_reference_open = None
    recovery_level = None
    fallback_eligible = False
    fallback_below_mode = False
    fallback_prev_close = None
    fallback_pending = False
    fallback_pending_kind = None
    fallback_signal_date = None
    fallback_signal_ref_close = None
    fallback_eligibility_date = None

    open_cycle = None
    cycles = []
    values = []

    def clear_cash_state():
        nonlocal ref_peak_while_cash, search_active, search_activation_idx, search_activation_date
        nonlocal current_cash_asset, asset_segment_start_idx
        nonlocal stop_price, stop_active_from_idx, stop_pivot_date, stop_confirmation_date
        nonlocal cashout_execution_ts, cashout_reference_open, recovery_level
        nonlocal fallback_eligible, fallback_below_mode, fallback_prev_close
        nonlocal fallback_pending, fallback_pending_kind, fallback_signal_date
        nonlocal fallback_signal_ref_close, fallback_eligibility_date

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

        cashout_execution_ts = None
        cashout_reference_open = None
        recovery_level = None
        fallback_eligible = False
        fallback_below_mode = False
        fallback_prev_close = None
        fallback_pending = False
        fallback_pending_kind = None
        fallback_signal_date = None
        fallback_signal_ref_close = None
        fallback_eligibility_date = None

    def clear_arm():
        nonlocal armed, active_event, running_peak
        armed = False
        active_event = None
        running_peak = None

    for i in range(n):
        ts = timestamps[i]
        asset = ref_asset[i]

        if cash > 0 and current_cash_asset is not None and asset != current_cash_asset:
            current_cash_asset = asset
            asset_segment_start_idx = i
            stop_price = None
            stop_active_from_idx = None
            stop_pivot_date = None
            stop_confirmation_date = None
            if open_cycle is not None:
                open_cycle["asset_resets"] += 1

        if cashout_pending:
            gross = scale * ref_open_equity[i] * CASH_FRACTION
            fee = gross * COST
            net = gross - fee
            scale *= 1.0 - CASH_FRACTION
            cash += net
            cashouts += 1
            cashout_pending = False

            ref_peak_while_cash = float(cashout_signal["running_peak_usdt"])
            search_active = False
            search_activation_idx = None
            search_activation_date = None
            current_cash_asset = asset
            asset_segment_start_idx = i
            stop_price = None
            stop_active_from_idx = None
            stop_pivot_date = None
            stop_confirmation_date = None

            cashout_execution_ts = ts
            cashout_reference_open = float(ref_open_equity[i])
            recovery_level = cashout_reference_open * RECOVERY_RATIO

            fallback_eligible = False
            fallback_below_mode = False
            fallback_prev_close = None
            fallback_pending = False
            fallback_pending_kind = None
            fallback_signal_date = None
            fallback_signal_ref_close = None
            fallback_eligibility_date = None

            open_cycle = {
                **{k: v for k, v in cashout_signal["event"].items() if k != "signal_idx"},
                "cashout_signal_date": cashout_signal["signal_date"],
                "cashout_execution_date": ts.isoformat(),
                "cashout_asset": asset,
                "cashout_reference_peak_usdt": ref_peak_while_cash,
                "cashout_reference_open_usdt": cashout_reference_open,
                "recovery_level_usdt": recovery_level,
                "cashout_reference_equity_signal_usdt": cashout_signal["reference_equity_signal_usdt"],
                "cashout_pullback_from_peak": cashout_signal["pullback_from_peak"],
                "gross_cashout_usdt": gross,
                "cashout_fee_usdt": fee,
                "net_cash_parked_usdt": net,
                "asset_resets": 0,
                "confirmed_pivots": 0,
                "stop_replacements": 0,
            }

        # Prior-close recovery signal executes at today's open.
        if cash > 0 and fallback_pending:
            open_price = float(panel_eval.loc[i, asset + "_open"])
            gross_cash = cash
            fee = gross_cash * COST
            net = gross_cash - fee
            bought_qty = net / open_price
            scale += bought_qty / ref_qty[i]
            cash = 0.0
            reentries += 1
            recovery_reentries += 1
            if fallback_pending_kind == "RECOVERY_IMMEDIATE":
                recovery_immediate += 1
            else:
                recovery_cross += 1

            if open_cycle is not None:
                open_cycle.update({
                    "fallback_eligibility_date": fallback_eligibility_date,
                    "fallback_signal_date": fallback_signal_date,
                    "fallback_signal_reference_close_usdt": fallback_signal_ref_close,
                    "reentry_route": fallback_pending_kind,
                    "reentry_execution_date": ts.isoformat(),
                    "reentry_asset": asset,
                    "reentry_fill_price_usdt": open_price,
                    "reentry_fee_usdt": fee,
                    "reference_equity_open_at_reentry_usdt": ref_open_equity[i],
                    "reference_equity_close_at_reentry_usdt": ref_close_equity[i],
                    "reference_drawdown_at_reentry_close": (
                        ref_close_equity[i] / ref_peak_while_cash - 1.0
                    ),
                    "cash_cycle_days": int((ts - cashout_execution_ts).days),
                })
                cycles.append(open_cycle)

            open_cycle = None
            clear_cash_state()
            clear_arm()

        # Otherwise normal fractal can fill.
        if (
            cash > 0
            and not fallback_pending
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
                scale += bought_qty / ref_qty[i]
                cash = 0.0
                reentries += 1
                fractal_reentries += 1

                if open_cycle is not None:
                    open_cycle.update({
                        "fallback_eligibility_date": fallback_eligibility_date,
                        "fallback_signal_date": None,
                        "fallback_signal_reference_close_usdt": None,
                        "reentry_route": "FRACTAL",
                        "reentry_execution_date": ts.isoformat(),
                        "reentry_asset": asset,
                        "reentry_fill_type": fill_type,
                        "reentry_stop_price_usdt": float(stop_price),
                        "reentry_fill_price_usdt": float(fill_price),
                        "reentry_stop_pivot_date": stop_pivot_date,
                        "reentry_stop_confirmation_date": stop_confirmation_date,
                        "reentry_fee_usdt": fee,
                        "reference_equity_open_at_reentry_usdt": ref_open_equity[i],
                        "reference_equity_close_at_reentry_usdt": ref_close_equity[i],
                        "reference_drawdown_at_reentry_close": (
                            ref_close_equity[i] / ref_peak_while_cash - 1.0
                        ),
                        "cash_cycle_days": int((ts - cashout_execution_ts).days),
                    })
                    cycles.append(open_cycle)

                open_cycle = None
                clear_cash_state()
                clear_arm()

        invested_close = scale * ref_close_equity[i]
        total_close = invested_close + cash
        values.append(total_close)

        if cash > 0:
            days_in_cash += 1

        if i == n - 1:
            continue

        # Start a new multimonth event only if free.
        if i in event_by_idx:
            event = event_by_idx[i]
            if cash > 0 or armed or cashout_pending:
                skipped_events += 1
            else:
                armed = True
                active_event = event
                running_peak = max(
                    float(event["streak_peak_equity_usdt"]),
                    ref_close_equity[i],
                )

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
                pivot = fractal.is_confirmed_pivot_high(
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

            elapsed_days = int((ts - cashout_execution_ts).days)

            if not fallback_eligible and elapsed_days >= FALLBACK_DAYS:
                fallback_eligible = True
                fallback_eligibility_date = ts.isoformat()

                if open_cycle is not None:
                    open_cycle["fallback_eligibility_date"] = fallback_eligibility_date
                    open_cycle["fallback_reference_close_at_eligibility_usdt"] = ref_close_equity[i]
                    open_cycle["fallback_reference_to_recovery_ratio"] = (
                        ref_close_equity[i] / recovery_level
                    )

                if ref_close_equity[i] >= recovery_level:
                    fallback_pending = True
                    fallback_pending_kind = "RECOVERY_IMMEDIATE"
                    fallback_signal_date = ts.isoformat()
                    fallback_signal_ref_close = ref_close_equity[i]
                else:
                    fallback_below_mode = True
                    fallback_prev_close = ref_close_equity[i]

            elif fallback_eligible and fallback_below_mode and not fallback_pending:
                prev = fallback_prev_close
                curr = ref_close_equity[i]
                if prev is not None and prev < recovery_level <= curr:
                    fallback_pending = True
                    fallback_pending_kind = "RECOVERY_CROSS"
                    fallback_signal_date = ts.isoformat()
                    fallback_signal_ref_close = curr
                fallback_prev_close = curr

            continue

        if armed:
            if ref_close_equity[i] > running_peak:
                running_peak = ref_close_equity[i]

            pullback = ref_close_equity[i] / running_peak - 1.0
            if pullback <= -cashout_pullback:
                cashout_signal = {
                    "signal_date": ts.isoformat(),
                    "event": active_event,
                    "running_peak_usdt": running_peak,
                    "reference_equity_signal_usdt": ref_close_equity[i],
                    "pullback_from_peak": pullback,
                }
                cashout_pending = True

    if open_cycle is not None:
        open_cycle.update({
            "fallback_eligibility_date": fallback_eligibility_date,
            "fallback_signal_date": fallback_signal_date,
            "fallback_signal_reference_close_usdt": fallback_signal_ref_close,
            "reentry_route": None,
            "reentry_execution_date": None,
            "reentry_asset": None,
            "reentry_fill_price_usdt": None,
            "reentry_fee_usdt": None,
            "cash_cycle_days": int((timestamps[-1] - cashout_execution_ts).days),
        })
        cycles.append(open_cycle)

    risk = fractal.analyze_values(timestamps, values, INITIAL_USDT)
    result = {
        "streak_event_count": len(streak_events),
        "cashout_pullback": cashout_pullback,
        "cashouts": cashouts,
        "reentries": reentries,
        "skipped_events": skipped_events,
        "fractal_reentries": fractal_reentries,
        "recovery_reentries": recovery_reentries,
        "recovery_immediate": recovery_immediate,
        "recovery_cross": recovery_cross,
        "unfinished_cycles": int(cash > 0),
        "days_in_cash": days_in_cash,
        "search_activations": search_activations,
        "confirmed_pivots": confirmed_pivots,
        "stop_replacements": stop_replacements,
        "terminal_cash_usdt": cash,
        **risk,
    }
    return result, pd.DataFrame(cycles)


def summarize_alt(df: pd.DataFrame) -> dict:
    alt = df[~df["is_canonical"]].copy()
    return {
        "count": int(len(alt)),
        "candidate_gt_control_rate": float((alt["delta_vs_control_usdt"] > 0).mean()),
        "candidate_eq_control_rate": float((alt["delta_vs_control_usdt"].abs() < 1e-8).mean()),
        "candidate_lt_control_rate": float((alt["delta_vs_control_usdt"] < -1e-8).mean()),
        "candidate_gt_baseline_rate": float((alt["delta_vs_baseline_usdt"] > 0).mean()),
        "both_better_baseline_rate": float(
            ((alt["delta_vs_baseline_usdt"] > 0) & (alt["dd_improvement_pp"] > 0)).mean()
        ),
        "unfinished_rate": float((alt["candidate_unfinished_cycles"] > 0).mean()),
        "fallback_used_rate": float((alt["candidate_recovery_reentries"] > 0).mean()),
        "median_delta_vs_control_pct": float(alt["delta_vs_control_pct"].median()),
        "q25_delta_vs_control_pct": float(alt["delta_vs_control_pct"].quantile(0.25)),
        "q75_delta_vs_control_pct": float(alt["delta_vs_control_pct"].quantile(0.75)),
        "median_delta_vs_baseline_pct": float(alt["delta_vs_baseline_pct"].median()),
        "median_cash_day_change": float(
            (alt["candidate_days_in_cash"] - alt["control_days_in_cash"]).median()
        ),
    }


def pct(x: float) -> str:
    return f"{100*x:+.2f}%"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cutoff", default="2026-09-27T00:00:00Z")
    args = ap.parse_args()

    panel, metadata = fractal.download_ohlc_panel(utc(args.cutoff))
    event_map = fractal.build_events(panel)
    mature_start = utc(panel.iloc[LOOKBACK - 1]["timestamp"])
    panel_eval = panel[panel["timestamp"] >= mature_start].copy().reset_index(drop=True)

    canonical_key = universe_key(CANONICAL_U10)
    rows = []
    cycles = []
    canonical = {}

    for idx, assets in enumerate(all_u10_universes(), start=1):
        key = universe_key(assets)
        is_canonical = key == canonical_key

        baseline, reference = run_reference(
            panel, event_map, assets, mature_start, INITIAL_USDT
        )
        reference = reference.reset_index(drop=True)
        monthly, _ = monthly_state(reference, INITIAL_USDT)
        streak_events = build_streak_events(reference, monthly)

        for label, cfg in (("P1", P1), ("P2", P2)):
            control, _ = run_streak_overlay(
                panel_eval,
                reference,
                streak_events,
                cashout_pullback=cfg["pullback"],
            )
            candidate, cy = run_streak_recovery_overlay(
                panel_eval,
                reference,
                streak_events,
                cashout_pullback=cfg["pullback"],
            )

            row = {
                "universe_key": key,
                "variant": label,
                "is_canonical": is_canonical,
                "baseline_final_equity_usdt": baseline["final_equity_usdt"],
                "baseline_max_drawdown": baseline["max_drawdown"],
                "control_final_equity_usdt": control["final_equity_usdt"],
                "control_max_drawdown": control["max_drawdown"],
                "control_days_in_cash": control["days_in_cash"],
                "candidate_final_equity_usdt": candidate["final_equity_usdt"],
                "candidate_max_drawdown": candidate["max_drawdown"],
                "candidate_days_in_cash": candidate["days_in_cash"],
                "candidate_unfinished_cycles": candidate["unfinished_cycles"],
                "candidate_fractal_reentries": candidate["fractal_reentries"],
                "candidate_recovery_reentries": candidate["recovery_reentries"],
                "candidate_recovery_immediate": candidate["recovery_immediate"],
                "candidate_recovery_cross": candidate["recovery_cross"],
                "delta_vs_control_usdt": (
                    candidate["final_equity_usdt"] - control["final_equity_usdt"]
                ),
                "delta_vs_control_pct": (
                    candidate["final_equity_usdt"] / control["final_equity_usdt"] - 1.0
                ),
                "delta_vs_baseline_usdt": (
                    candidate["final_equity_usdt"] - baseline["final_equity_usdt"]
                ),
                "delta_vs_baseline_pct": (
                    candidate["final_equity_usdt"] / baseline["final_equity_usdt"] - 1.0
                ),
                "dd_improvement_pp": (
                    candidate["max_drawdown"] - baseline["max_drawdown"]
                ) * 100.0,
            }
            rows.append(row)

            if is_canonical:
                canonical[label] = dict(row)

            if not cy.empty:
                x = cy.copy()
                x.insert(0, "variant", label)
                x.insert(0, "is_canonical", is_canonical)
                x.insert(0, "universe_key", key)
                cycles.append(x)

        if idx % 100 == 0:
            print(f"processed_universes={idx}")

    df = pd.DataFrame(rows)
    cycles_df = pd.concat(cycles, ignore_index=True) if cycles else pd.DataFrame()

    alt_summary = {
        label: summarize_alt(df[df["variant"] == label].copy())
        for label in ("P1", "P2")
    }

    cycle_summary = {}
    if not cycles_df.empty:
        for label in ("P1", "P2"):
            z = cycles_df[
                (cycles_df["variant"] == label)
                & (~cycles_df["is_canonical"])
                & (cycles_df["reentry_execution_date"].notna())
            ].copy()
            if z.empty:
                continue
            fall = z[z["reentry_route"].isin(["RECOVERY_IMMEDIATE", "RECOVERY_CROSS"])].copy()
            cycle_summary[label] = {
                "completed_cycles": int(len(z)),
                "fractal_count": int((z["reentry_route"] == "FRACTAL").sum()),
                "recovery_immediate_count": int((z["reentry_route"] == "RECOVERY_IMMEDIATE").sum()),
                "recovery_cross_count": int((z["reentry_route"] == "RECOVERY_CROSS").sum()),
                "fallback_rate": float(z["reentry_route"].isin(["RECOVERY_IMMEDIATE", "RECOVERY_CROSS"]).mean()),
                "median_cash_days": float(z["cash_cycle_days"].median()),
                "fallback_ratio_min": (
                    float(fall["fallback_reference_to_recovery_ratio"].min())
                    if not fall.empty and "fallback_reference_to_recovery_ratio" in fall
                    else None
                ),
                "fallback_ratio_median": (
                    float(fall["fallback_reference_to_recovery_ratio"].median())
                    if not fall.empty and "fallback_reference_to_recovery_ratio" in fall
                    else None
                ),
                "fallback_ratio_max": (
                    float(fall["fallback_reference_to_recovery_ratio"].max())
                    if not fall.empty and "fallback_reference_to_recovery_ratio" in fall
                    else None
                ),
            }

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    df.to_csv(run_dir / "all_792_streak95_recovery15.csv", index=False)
    if not cycles_df.empty:
        cycles_df.to_csv(run_dir / "all_candidate_cycles.csv", index=False)
        cycles_df[cycles_df["is_canonical"] == True].to_csv(
            run_dir / "canonical_candidate_cycles.csv", index=False
        )

    payload = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit": source_sha(),
        "mature_start": mature_start.isoformat(),
        "parameters": {
            "streak_threshold": STREAK_THRESHOLD,
            "minimum_positive_months": MIN_STREAK_MONTHS,
            "fallback_days": FALLBACK_DAYS,
            "recovery_ratio": RECOVERY_RATIO,
            "cash_fraction": CASH_FRACTION,
            "search_drawdown": SEARCH_DRAWDOWN,
            "pivot_left": fractal.PIVOT_LEFT,
            "pivot_right": fractal.PIVOT_RIGHT,
        },
        "canonical": canonical,
        "alternative_summary": alt_summary,
        "cycle_summary": cycle_summary,
        "data_metadata": metadata,
    }

    (run_dir / "results.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False),
        encoding="utf-8",
    )

    lines = [
        "# U10 Multimonth STREAK95 + Recovery15 v1",
        "",
        "Mode: STRESS_TEST_ONLY",
        "Live/paper logic: UNCHANGED",
        "",
        "CONTROL: multimonth STREAK95 + 5-bar fractal.",
        "CANDIDATE: identical control + 15-day / 98%-reference fallback.",
        "",
        "## Canonical",
        "",
        "| Variant | Baseline | Control | Candidate | Candidate vs control | Candidate vs baseline | DD candidate | Fallback reentries | Cash days control | Cash days candidate |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for label in ("P1", "P2"):
        c = canonical[label]
        lines.append(
            f"| {label} | {c['baseline_final_equity_usdt']:,.2f} | "
            f"{c['control_final_equity_usdt']:,.2f} | {c['candidate_final_equity_usdt']:,.2f} | "
            f"{pct(c['delta_vs_control_pct'])} | {pct(c['delta_vs_baseline_pct'])} | "
            f"{pct(c['candidate_max_drawdown'])} | {c['candidate_recovery_reentries']} | "
            f"{c['control_days_in_cash']} | {c['candidate_days_in_cash']} |"
        )

    lines += ["", "## 791 alternative U10s", ""]
    for label in ("P1", "P2"):
        s = alt_summary[label]
        lines += [
            f"### {label}",
            f"- candidate > control: {100*s['candidate_gt_control_rate']:.2f}%",
            f"- equal: {100*s['candidate_eq_control_rate']:.2f}%",
            f"- candidate < control: {100*s['candidate_lt_control_rate']:.2f}%",
            f"- candidate > baseline: {100*s['candidate_gt_baseline_rate']:.2f}%",
            f"- both final and DD > baseline: {100*s['both_better_baseline_rate']:.2f}%",
            f"- fallback used: {100*s['fallback_used_rate']:.2f}%",
            f"- unfinished cash: {100*s['unfinished_rate']:.2f}%",
            f"- median delta vs control: {pct(s['median_delta_vs_control_pct'])}",
            f"- q25/q75 vs control: {pct(s['q25_delta_vs_control_pct'])} / {pct(s['q75_delta_vs_control_pct'])}",
            f"- median cash-day change: {s['median_cash_day_change']:+.1f}",
            "",
        ]

    lines += ["TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST"]
    (run_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    print("run_dir=" + str(run_dir))
    print("CANONICAL", canonical)
    print("ALT", alt_summary)
    print("CYCLES", cycle_summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
