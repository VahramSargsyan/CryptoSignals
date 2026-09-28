from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

import scripts.research_u10_monthly_surge_fractal_buystop_v1 as fractal
from scripts.research_u10_multimonth_streak95_v1 import (
    P1,
    P2,
    STREAK_THRESHOLD,
    MIN_STREAK_MONTHS,
    SEARCH_DRAWDOWN,
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
from scripts.research_u10_last_year_vs_u8_universes_v1 import (
    COST,
    LOOKBACK,
    utc,
)

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "u10_streak95_trendbreak_v1"


def attach_breaks(
    reference: pd.DataFrame,
    monthly: pd.DataFrame,
    events: list[dict],
) -> list[dict]:
    ref_months = pd.to_datetime(reference["timestamp"], utc=True).dt.strftime("%Y-%m")
    month_rows = monthly.reset_index(drop=True)

    out = []
    for event in events:
        e = dict(event)
        matches = month_rows.index[month_rows["month"] == event["streak_end_month"]].tolist()
        if not matches:
            raise RuntimeError(f"missing streak end month {event['streak_end_month']}")
        trigger_pos = int(matches[0])

        break_row = None
        for pos in range(trigger_pos + 1, len(month_rows)):
            row = month_rows.iloc[pos]
            if float(row["selected_change"]) <= 0:
                break_row = row
                break

        if break_row is None:
            e.update(
                {
                    "break_month": None,
                    "break_idx": None,
                    "break_date": None,
                    "break_selected_change": None,
                }
            )
        else:
            break_month = str(break_row["month"])
            indices = reference.index[ref_months == break_month].tolist()
            if not indices:
                raise RuntimeError(f"missing reference month {break_month}")
            break_idx = int(indices[-1])
            e.update(
                {
                    "break_month": break_month,
                    "break_idx": break_idx,
                    "break_date": utc(reference.loc[break_idx, "timestamp"]).isoformat(),
                    "break_selected_change": float(break_row["selected_change"]),
                }
            )
        out.append(e)
    return out


def run_trendbreak_overlay(
    panel_eval: pd.DataFrame,
    reference: pd.DataFrame,
    events: list[dict],
    *,
    cashout_pullback: float,
) -> tuple[dict, pd.DataFrame]:
    n = len(reference)
    if n != len(panel_eval):
        raise RuntimeError("panel/reference length mismatch")
    if not n:
        raise RuntimeError("empty reference")

    timestamps = [utc(x) for x in reference["timestamp"]]
    ref_open_equity = reference["open_equity_usdt"].astype(float).tolist()
    ref_close_equity = reference["equity_usdt"].astype(float).tolist()
    ref_qty = reference["asset_qty"].astype(float).tolist()
    ref_asset = reference["asset"].astype(str).tolist()

    event_by_idx = {int(e["signal_idx"]): e for e in events}

    scale = 1.0
    cash = 0.0

    active_event = None
    running_peak = None
    watch_start_idx = None
    watch_days = 0
    lower_high_confirmed = False
    lower_high_info = None

    current_watch_asset = None
    watch_segment_start_idx = None
    prior_pivot_high = None
    prior_pivot_date = None
    prior_pivot_confirmation_date = None

    cashout_pending = False
    cashout_signal = None

    cashouts = 0
    reentries = 0
    skipped_events = 0
    lower_high_confirmations = 0
    unfinished_watch = 0
    days_in_cash = 0

    ref_peak_while_cash = None
    search_active = False
    search_activation_idx = None
    search_activation_date = None
    current_cash_asset = None
    cash_segment_start_idx = None
    stop_price = None
    stop_active_from_idx = None
    stop_pivot_date = None
    stop_confirmation_date = None

    open_cycle = None
    cycles = []
    values = []

    def reset_watch():
        nonlocal active_event, running_peak, watch_start_idx
        nonlocal lower_high_confirmed, lower_high_info
        nonlocal current_watch_asset, watch_segment_start_idx
        nonlocal prior_pivot_high, prior_pivot_date, prior_pivot_confirmation_date

        active_event = None
        running_peak = None
        watch_start_idx = None
        lower_high_confirmed = False
        lower_high_info = None
        current_watch_asset = None
        watch_segment_start_idx = None
        prior_pivot_high = None
        prior_pivot_date = None
        prior_pivot_confirmation_date = None

    for i in range(n):
        ts = timestamps[i]
        asset = ref_asset[i]

        # Asset reset while WATCH.
        if (
            active_event is not None
            and cash <= 0
            and current_watch_asset is not None
            and asset != current_watch_asset
        ):
            current_watch_asset = asset
            watch_segment_start_idx = i
            prior_pivot_high = None
            prior_pivot_date = None
            prior_pivot_confirmation_date = None
            lower_high_confirmed = False
            lower_high_info = None

        # Frozen U10 rotation has already happened at this open; reset old cash stop first.
        if cash > 0 and current_cash_asset is not None and asset != current_cash_asset:
            current_cash_asset = asset
            cash_segment_start_idx = i
            stop_price = None
            stop_active_from_idx = None
            stop_pivot_date = None
            stop_confirmation_date = None
            if open_cycle is not None:
                open_cycle["reentry_asset_resets"] += 1

        # Execute sale scheduled from prior close.
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
            cash_segment_start_idx = i
            stop_price = None
            stop_active_from_idx = None
            stop_pivot_date = None
            stop_confirmation_date = None

            open_cycle = {
                **{k: v for k, v in cashout_signal["event"].items() if k != "signal_idx"},
                "lower_high_prior_pivot_usdt": cashout_signal["lower_high"]["prior_pivot_high"],
                "lower_high_prior_pivot_date": cashout_signal["lower_high"]["prior_pivot_date"],
                "lower_high_pivot_usdt": cashout_signal["lower_high"]["lower_pivot_high"],
                "lower_high_pivot_date": cashout_signal["lower_high"]["lower_pivot_date"],
                "lower_high_confirmation_date": cashout_signal["lower_high"]["confirmation_date"],
                "lower_high_asset": cashout_signal["lower_high"]["asset"],
                "reference_dd_at_lower_high": cashout_signal["lower_high"]["reference_dd"],
                "cashout_signal_date": cashout_signal["signal_date"],
                "cashout_execution_date": ts.isoformat(),
                "cashout_reference_peak_usdt": ref_peak_while_cash,
                "cashout_reference_equity_signal_usdt": cashout_signal["reference_equity_signal_usdt"],
                "cashout_pullback_from_peak": cashout_signal["pullback_from_peak"],
                "watch_days_to_sale_signal": cashout_signal["watch_days_to_signal"],
                "cashout_asset": asset,
                "gross_cashout_usdt": gross,
                "cashout_fee_usdt": fee,
                "net_cash_parked_usdt": net,
                "reentry_asset_resets": 0,
                "reentry_confirmed_pivots": 0,
                "reentry_stop_replacements": 0,
            }

            # WATCH is finished once sale executes.
            reset_watch()

        # Existing 5-bar buy-stop re-entry.
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
                scale += bought_qty / ref_qty[i]
                cash = 0.0
                reentries += 1

                ref_dd = ref_close_equity[i] / ref_peak_while_cash - 1.0
                if open_cycle is not None:
                    open_cycle.update(
                        {
                            "search_activation_date": search_activation_date,
                            "final_reference_peak_usdt": ref_peak_while_cash,
                            "reentry_execution_date": ts.isoformat(),
                            "reentry_asset": asset,
                            "reentry_fill_type": fill_type,
                            "reentry_stop_price_usdt": float(stop_price),
                            "reentry_fill_price_usdt": float(fill_price),
                            "reentry_stop_pivot_date": stop_pivot_date,
                            "reentry_stop_confirmation_date": stop_confirmation_date,
                            "reentry_fee_usdt": fee,
                            "reference_drawdown_at_reentry_close": ref_dd,
                            "cash_cycle_days": int(
                                (ts - utc(open_cycle["cashout_execution_date"])).days
                            ),
                        }
                    )
                    cycles.append(open_cycle)

                open_cycle = None
                ref_peak_while_cash = None
                search_active = False
                search_activation_idx = None
                search_activation_date = None
                current_cash_asset = None
                cash_segment_start_idx = None
                stop_price = None
                stop_active_from_idx = None
                stop_pivot_date = None
                stop_confirmation_date = None

        invested_close = scale * ref_close_equity[i]
        total_close = invested_close + cash
        values.append(total_close)

        if cash > 0:
            days_in_cash += 1
        elif active_event is not None:
            watch_days += 1

        if i == n - 1:
            continue

        # New STREAK95 event starts WATCH only if free.
        if i in event_by_idx:
            e = event_by_idx[i]
            if active_event is not None or cash > 0 or cashout_pending:
                skipped_events += 1
            else:
                active_event = e
                running_peak = max(
                    float(e["streak_peak_equity_usdt"]),
                    ref_close_equity[i],
                )
                watch_start_idx = i
                current_watch_asset = asset
                watch_segment_start_idx = i
                prior_pivot_high = None
                prior_pivot_date = None
                prior_pivot_confirmation_date = None
                lower_high_confirmed = False
                lower_high_info = None

        # Cash phase: unchanged 5-bar re-entry.
        if cash > 0:
            if ref_close_equity[i] > ref_peak_while_cash:
                ref_peak_while_cash = ref_close_equity[i]

            if not search_active:
                dd = ref_close_equity[i] / ref_peak_while_cash - 1.0
                if dd <= -SEARCH_DRAWDOWN:
                    search_active = True
                    search_activation_idx = i
                    search_activation_date = ts.isoformat()

            if search_active:
                pivot = fractal.is_confirmed_pivot_high(
                    panel_eval,
                    reference,
                    i,
                    asset,
                    cash_segment_start_idx,
                    search_activation_idx,
                )
                if pivot is not None:
                    if open_cycle is not None:
                        open_cycle["reentry_confirmed_pivots"] += 1
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
                        if open_cycle is not None:
                            open_cycle["reentry_stop_replacements"] += 1
            continue

        # WATCH phase.
        if active_event is not None:
            if ref_close_equity[i] > running_peak:
                running_peak = ref_close_equity[i]

            # Collect confirmed pivots causally from event trigger onward.
            pivot = fractal.is_confirmed_pivot_high(
                panel_eval,
                reference,
                i,
                asset,
                watch_segment_start_idx,
                int(active_event["signal_idx"]),
            )

            if pivot is not None:
                candidate = float(pivot["pivot_high"])
                break_idx = active_event.get("break_idx")
                after_break = break_idx is not None and i >= int(break_idx)

                if (
                    prior_pivot_high is not None
                    and candidate < prior_pivot_high
                    and after_break
                ):
                    lower_high_confirmed = True
                    lower_high_confirmations += 1
                    reference_dd = ref_close_equity[i] / running_peak - 1.0
                    lower_high_info = {
                        "asset": asset,
                        "prior_pivot_high": prior_pivot_high,
                        "prior_pivot_date": prior_pivot_date,
                        "prior_pivot_confirmation_date": prior_pivot_confirmation_date,
                        "lower_pivot_high": candidate,
                        "lower_pivot_date": pivot["pivot_center_date"],
                        "confirmation_date": pivot["confirmation_date"],
                        "reference_dd": reference_dd,
                    }

                prior_pivot_high = candidate
                prior_pivot_date = pivot["pivot_center_date"]
                prior_pivot_confirmation_date = pivot["confirmation_date"]

            if lower_high_confirmed:
                pullback = ref_close_equity[i] / running_peak - 1.0
                if pullback <= -cashout_pullback:
                    cashout_signal = {
                        "signal_date": ts.isoformat(),
                        "event": active_event,
                        "running_peak_usdt": running_peak,
                        "reference_equity_signal_usdt": ref_close_equity[i],
                        "pullback_from_peak": pullback,
                        "lower_high": lower_high_info,
                        "watch_days_to_signal": int(
                            (ts - utc(active_event["signal_date"])).days
                        ),
                    }
                    cashout_pending = True

    if active_event is not None and cash <= 0:
        unfinished_watch = 1

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

    risk = fractal.analyze_values(timestamps, values, INITIAL_USDT)
    result = {
        "streak_event_count": len(events),
        "cashout_pullback": cashout_pullback,
        "cashouts": cashouts,
        "reentries": reentries,
        "skipped_events": skipped_events,
        "lower_high_confirmations": lower_high_confirmations,
        "unfinished_watch": unfinished_watch,
        "unfinished_cash_cycles": int(cash > 0),
        "watch_days": watch_days,
        "days_in_cash": days_in_cash,
        "terminal_cash_usdt": cash,
        **risk,
    }
    return result, pd.DataFrame(cycles)


def summarize(df: pd.DataFrame) -> dict:
    alt = df[~df["is_canonical"]].copy()
    return {
        "count": int(len(alt)),
        "final_gt_baseline_rate": float((alt["tb_delta_vs_baseline_usdt"] > 0).mean()),
        "final_gt_original_rate": float((alt["tb_delta_vs_original_usdt"] > 0).mean()),
        "dd_better_rate": float((alt["tb_dd_improvement_pp"] > 0).mean()),
        "both_better_rate": float(
            (
                (alt["tb_delta_vs_baseline_usdt"] > 0)
                & (alt["tb_dd_improvement_pp"] > 0)
            ).mean()
        ),
        "unfinished_watch_rate": float((alt["tb_unfinished_watch"] > 0).mean()),
        "unfinished_cash_rate": float((alt["tb_unfinished_cash_cycles"] > 0).mean()),
        "median_delta_vs_baseline_pct": float(alt["tb_delta_vs_baseline_pct"].median()),
        "q25_delta_vs_baseline_pct": float(alt["tb_delta_vs_baseline_pct"].quantile(0.25)),
        "q75_delta_vs_baseline_pct": float(alt["tb_delta_vs_baseline_pct"].quantile(0.75)),
        "median_delta_vs_original_pct": float(alt["tb_delta_vs_original_pct"].median()),
        "median_watch_days": float(alt["tb_watch_days"].median()),
        "median_cash_days": float(alt["tb_days_in_cash"].median()),
    }


def pct(x: float) -> str:
    return f"{100*x:+.2f}%"


def write_report(payload: dict, run_dir: Path) -> None:
    lines = [
        "# U10 STREAK95 Trend-Break Exit v1",
        "",
        "Mode: STRESS_TEST_ONLY",
        "Live/paper logic: UNCHANGED",
        "",
        "Trend-break filter: STREAK95 -> wait for first non-positive selected month -> confirm 5-bar lower high on current U10 asset -> require same 5%/10% reference pullback -> sell 30% next open.",
        "",
        "Re-entry: unchanged 5-bar buy-stop after -15% reference drawdown.",
        "",
        "## Canonical U10",
        "",
        "| Variant | Baseline | Original STREAK95 | Trend-break | TB vs baseline | TB vs original | Max DD TB | Cash-outs | Reentries | Watch days | Cash days |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for label in ("P1", "P2"):
        c = payload["canonical"][label]
        lines.append(
            f"| {label} | {c['baseline_final_equity_usdt']:,.2f} | "
            f"{c['original_final_equity_usdt']:,.2f} | "
            f"{c['tb_final_equity_usdt']:,.2f} | "
            f"{pct(c['tb_delta_vs_baseline_pct'])} | "
            f"{pct(c['tb_delta_vs_original_pct'])} | "
            f"{pct(c['tb_max_drawdown'])} | "
            f"{c['tb_cashouts']} | {c['tb_reentries']} | "
            f"{c['tb_watch_days']} | {c['tb_days_in_cash']} |"
        )

    lines += [
        "",
        "## 791 alternative U10s",
        "",
        "| Variant | Final > baseline | Final > original STREAK95 | DD better | Both better | Unfinished watch | Unfinished cash | Median delta vs baseline | Median delta vs original |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for label in ("P1", "P2"):
        s = payload["alternative_summary"][label]
        lines.append(
            f"| {label} | {100*s['final_gt_baseline_rate']:.2f}% | "
            f"{100*s['final_gt_original_rate']:.2f}% | "
            f"{100*s['dd_better_rate']:.2f}% | "
            f"{100*s['both_better_rate']:.2f}% | "
            f"{100*s['unfinished_watch_rate']:.2f}% | "
            f"{100*s['unfinished_cash_rate']:.2f}% | "
            f"{pct(s['median_delta_vs_baseline_pct'])} | "
            f"{pct(s['median_delta_vs_original_pct'])} |"
        )

    lines += [
        "",
        "## Guardrail",
        "",
        "- No threshold sweep.",
        "- STREAK95 remains >=95% and >=2 consecutive positive months.",
        "- Lower high uses 5-bar only.",
        "- Re-entry mechanics are unchanged.",
        "- No 2020-2022 tuning.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    ]
    (run_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


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
    cycles_all = []
    canonical = {}

    for idx, assets in enumerate(all_u10_universes(), start=1):
        key = universe_key(assets)
        is_canonical = key == canonical_key

        baseline, reference = run_reference(
            panel, event_map, assets, mature_start, INITIAL_USDT
        )
        reference = reference.reset_index(drop=True)
        monthly, _ = monthly_state(reference, INITIAL_USDT)

        raw_events = build_streak_events(reference, monthly)
        events = attach_breaks(reference, monthly, raw_events)

        for label, cfg in (("P1", P1), ("P2", P2)):
            original, _ = run_streak_overlay(
                panel_eval,
                reference,
                raw_events,
                cashout_pullback=cfg["pullback"],
            )
            tb, cycles = run_trendbreak_overlay(
                panel_eval,
                reference,
                events,
                cashout_pullback=cfg["pullback"],
            )

            row = {
                "universe_key": key,
                "variant": label,
                "is_canonical": is_canonical,
                "baseline_final_equity_usdt": baseline["final_equity_usdt"],
                "original_final_equity_usdt": original["final_equity_usdt"],
                "tb_final_equity_usdt": tb["final_equity_usdt"],
                "tb_max_drawdown": tb["max_drawdown"],
                "tb_cashouts": tb["cashouts"],
                "tb_reentries": tb["reentries"],
                "tb_unfinished_watch": tb["unfinished_watch"],
                "tb_unfinished_cash_cycles": tb["unfinished_cash_cycles"],
                "tb_watch_days": tb["watch_days"],
                "tb_days_in_cash": tb["days_in_cash"],
                "tb_delta_vs_baseline_usdt": (
                    tb["final_equity_usdt"] - baseline["final_equity_usdt"]
                ),
                "tb_delta_vs_baseline_pct": (
                    tb["final_equity_usdt"] / baseline["final_equity_usdt"] - 1.0
                ),
                "tb_delta_vs_original_usdt": (
                    tb["final_equity_usdt"] - original["final_equity_usdt"]
                ),
                "tb_delta_vs_original_pct": (
                    tb["final_equity_usdt"] / original["final_equity_usdt"] - 1.0
                ),
                "tb_dd_improvement_pp": (
                    tb["max_drawdown"] - baseline["max_drawdown"]
                ) * 100.0,
            }
            rows.append(row)
            if is_canonical:
                canonical[label] = dict(row)

            if not cycles.empty:
                cc = cycles.copy()
                cc.insert(0, "is_canonical", is_canonical)
                cc.insert(0, "variant", label)
                cc.insert(0, "universe_key", key)
                cycles_all.append(cc)

        if idx % 100 == 0:
            print(f"processed_universes={idx}")

    df = pd.DataFrame(rows)
    cycles_df = pd.concat(cycles_all, ignore_index=True) if cycles_all else pd.DataFrame()

    alt_summary = {
        label: summarize(df[df["variant"] == label].copy())
        for label in ("P1", "P2")
    }

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(run_dir / "all_792_trendbreak_results.csv", index=False)
    if not cycles_df.empty:
        cycles_df.to_csv(run_dir / "all_trendbreak_cycles.csv", index=False)
        cycles_df[cycles_df["is_canonical"] == True].to_csv(
            run_dir / "canonical_trendbreak_cycles.csv", index=False
        )

    payload = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit": source_sha(),
        "mature_start": mature_start.isoformat(),
        "parameters": {
            "streak_threshold": STREAK_THRESHOLD,
            "minimum_positive_months": MIN_STREAK_MONTHS,
            "p1_pullback": P1["pullback"],
            "p2_pullback": P2["pullback"],
            "cash_fraction": CASH_FRACTION,
            "lower_high_pivot_left": 2,
            "lower_high_pivot_right": 2,
            "reentry_search_drawdown": SEARCH_DRAWDOWN,
            "reentry_pivot_left": 2,
            "reentry_pivot_right": 2,
        },
        "canonical": canonical,
        "alternative_summary": alt_summary,
        "data_metadata": metadata,
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
            "%s CAN base=%.8f orig=%.8f tb=%.8f db=%.8f do=%.8f dd=%.8f cashouts=%d re=%d watch=%d cash=%d"
            % (
                label,
                c["baseline_final_equity_usdt"],
                c["original_final_equity_usdt"],
                c["tb_final_equity_usdt"],
                c["tb_delta_vs_baseline_pct"],
                c["tb_delta_vs_original_pct"],
                c["tb_max_drawdown"],
                c["tb_cashouts"],
                c["tb_reentries"],
                c["tb_watch_days"],
                c["tb_days_in_cash"],
            )
        )
        print(
            "%s ALT base=%.6f orig=%.6f both=%.6f uw=%.6f uc=%.6f medb=%.6f medo=%.6f"
            % (
                label,
                s["final_gt_baseline_rate"],
                s["final_gt_original_rate"],
                s["both_better_rate"],
                s["unfinished_watch_rate"],
                s["unfinished_cash_rate"],
                s["median_delta_vs_baseline_pct"],
                s["median_delta_vs_original_pct"],
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
