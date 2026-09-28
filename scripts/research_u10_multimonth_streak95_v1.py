from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path

import pandas as pd

import scripts.research_u10_monthly_surge_fractal_buystop_v1 as fractal
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
OUT = ROOT / "research_artifacts" / "u10_multimonth_streak95_v1"

STREAK_THRESHOLD = 0.95
MIN_STREAK_MONTHS = 2
SEARCH_DRAWDOWN = 0.15
P1 = {"name": "P1_STREAK95", "pullback": 0.05}
P2 = {"name": "P2_STREAK95", "pullback": 0.10}


def month_mask(reference: pd.DataFrame, month: str) -> pd.Series:
    return pd.to_datetime(reference["timestamp"], utc=True).dt.strftime("%Y-%m") == month


def build_streak_events(reference: pd.DataFrame, monthly: pd.DataFrame) -> list[dict]:
    events: list[dict] = []
    streak_start_row = None
    pre_streak_equity = None
    streak_months = 0
    streak_triggered = False
    streak_has_single_month_100 = False

    ref_months = pd.to_datetime(reference["timestamp"], utc=True).dt.strftime("%Y-%m")

    for _, row in monthly.iterrows():
        change = float(row["selected_change"])
        month = str(row["month"])

        if change <= 0:
            streak_start_row = None
            pre_streak_equity = None
            streak_months = 0
            streak_triggered = False
            streak_has_single_month_100 = False
            continue

        if streak_start_row is None:
            streak_start_row = row
            pre_streak_equity = float(row["anchor_before_usdt"])
            streak_months = 0
            streak_triggered = False
            streak_has_single_month_100 = False

        streak_months += 1
        if change >= 1.0:
            streak_has_single_month_100 = True

        cumulative_gain = float(row["selected_equity_usdt"]) / pre_streak_equity - 1.0

        if (
            not streak_triggered
            and streak_months >= MIN_STREAK_MONTHS
            and cumulative_gain >= STREAK_THRESHOLD
        ):
            month_indices = reference.index[ref_months == month].tolist()
            if not month_indices:
                raise RuntimeError(f"no reference rows for month {month}")
            signal_idx = int(month_indices[-1])

            start_month = str(streak_start_row["month"])
            streak_mask = (ref_months >= start_month) & (ref_months <= month)
            streak_ref = reference.loc[streak_mask].copy()
            peak_idx = int(streak_ref["equity_usdt"].idxmax())
            peak_equity = float(reference.loc[peak_idx, "equity_usdt"])
            peak_date = utc(reference.loc[peak_idx, "timestamp"]).isoformat()

            events.append(
                {
                    "streak_start_month": start_month,
                    "streak_end_month": month,
                    "streak_month_count": int(streak_months),
                    "pre_streak_equity_usdt": float(pre_streak_equity),
                    "final_selected_equity_usdt": float(row["selected_equity_usdt"]),
                    "cumulative_streak_gain": float(cumulative_gain),
                    "signal_idx": signal_idx,
                    "signal_date": utc(reference.loc[signal_idx, "timestamp"]).isoformat(),
                    "streak_peak_equity_usdt": peak_equity,
                    "streak_peak_date": peak_date,
                    "overlaps_single_month_100": bool(streak_has_single_month_100),
                }
            )
            streak_triggered = True

    return events


def event_drawdown_stats(
    reference: pd.DataFrame,
    event: dict,
    horizon_days: int,
) -> dict:
    start = utc(event["signal_date"])
    end = start + pd.Timedelta(f"{horizon_days}D")
    window = reference[
        (reference["timestamp"] >= start)
        & (reference["timestamp"] <= end)
    ].copy()

    peak = float(event["streak_peak_equity_usdt"])
    worst = 0.0
    trough_date = None
    trough_equity = None

    for _, row in window.iterrows():
        value = float(row["equity_usdt"])
        if value > peak:
            peak = value
        dd = value / peak - 1.0
        if dd < worst:
            worst = dd
            trough_date = utc(row["timestamp"]).isoformat()
            trough_equity = value

    return {
        f"worst_running_dd_{horizon_days}d": float(worst),
        f"trough_date_{horizon_days}d": trough_date,
        f"trough_equity_{horizon_days}d": trough_equity,
        f"hit_minus15_{horizon_days}d": bool(worst <= -0.15),
        f"hit_minus25_{horizon_days}d": bool(worst <= -0.25),
        f"hit_minus35_{horizon_days}d": bool(worst <= -0.35),
    }


def build_event_study(reference: pd.DataFrame, events: list[dict]) -> pd.DataFrame:
    rows = []
    for event in events:
        row = dict(event)
        for horizon in (31, 62, 93):
            row.update(event_drawdown_stats(reference, event, horizon))
        rows.append(row)
    return pd.DataFrame(rows)


def pivot_high(
    panel: pd.DataFrame,
    reference: pd.DataFrame,
    confirm_idx: int,
    asset: str,
    segment_start_idx: int,
    search_start_idx: int,
):
    return fractal.is_confirmed_pivot_high(
        panel,
        reference,
        confirm_idx,
        asset,
        segment_start_idx,
        search_start_idx,
    )


def run_streak_overlay(
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

    for i in range(n):
        ts = timestamps[i]
        asset = ref_asset[i]

        # Frozen U10 rotation has already been reflected at the current open.
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

            open_cycle = {
                **{k: v for k, v in cashout_signal["event"].items() if k != "signal_idx"},
                "cashout_signal_date": cashout_signal["signal_date"],
                "cashout_execution_date": ts.isoformat(),
                "cashout_asset": asset,
                "cashout_reference_peak_usdt": ref_peak_while_cash,
                "cashout_reference_equity_signal_usdt": cashout_signal[
                    "reference_equity_signal_usdt"
                ],
                "cashout_pullback_from_peak": cashout_signal["pullback_from_peak"],
                "gross_cashout_usdt": gross,
                "cashout_fee_usdt": fee,
                "net_cash_parked_usdt": net,
                "asset_resets": 0,
                "confirmed_pivots": 0,
                "stop_replacements": 0,
            }

        # Stop can fill only from sessions after confirmation.
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

                if fill_type == "GAP_OPEN":
                    gap_open_fills += 1
                else:
                    stop_price_fills += 1

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
                asset_segment_start_idx = None
                stop_price = None
                stop_active_from_idx = None
                stop_pivot_date = None
                stop_confirmation_date = None
                armed = False
                active_event = None
                running_peak = None

        invested_close = scale * ref_close_equity[i]
        total_close = invested_close + cash
        values.append(total_close)

        if cash > 0:
            days_in_cash += 1

        if i == n - 1:
            continue

        # Handle a newly completed streak event at this close.
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
                pivot = pivot_high(
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
        "streak_event_count": len(streak_events),
        "cashout_pullback": cashout_pullback,
        "cashouts": cashouts,
        "reentries": reentries,
        "skipped_events": skipped_events,
        "search_activations": search_activations,
        "confirmed_pivots": confirmed_pivots,
        "stop_replacements": stop_replacements,
        "gap_open_fills": gap_open_fills,
        "stop_price_fills": stop_price_fills,
        "unfinished_cycles": int(cash > 0),
        "days_in_cash": days_in_cash,
        "terminal_cash_usdt": cash,
        **risk,
    }
    return result, pd.DataFrame(cycles)


def summarize_events(events_df: pd.DataFrame) -> dict:
    if events_df.empty:
        return {
            "event_count": 0,
            "hit25_31": None,
            "hit25_62": None,
            "hit25_93": None,
        }
    return {
        "event_count": int(len(events_df)),
        "hit25_31": float(events_df["hit_minus25_31d"].mean()),
        "hit25_62": float(events_df["hit_minus25_62d"].mean()),
        "hit25_93": float(events_df["hit_minus25_93d"].mean()),
        "median_dd_31": float(events_df["worst_running_dd_31d"].median()),
        "median_dd_62": float(events_df["worst_running_dd_62d"].median()),
        "median_dd_93": float(events_df["worst_running_dd_93d"].median()),
        "overlap_single_month_100_rate": float(
            events_df["overlaps_single_month_100"].mean()
        ),
    }


def summarize_alt(df: pd.DataFrame) -> dict:
    return {
        "count": int(len(df)),
        "universe_with_event_rate": float((df["streak_event_count"] > 0).mean()),
        "final_gt_baseline_rate": float((df["delta_vs_baseline_usdt"] > 0).mean()),
        "dd_better_rate": float((df["dd_improvement_pp"] > 0).mean()),
        "both_better_rate": float(
            ((df["delta_vs_baseline_usdt"] > 0) & (df["dd_improvement_pp"] > 0)).mean()
        ),
        "unfinished_rate": float((df["unfinished_cycles"] > 0).mean()),
        "median_delta_pct": float(df["delta_vs_baseline_pct"].median()),
        "q25_delta_pct": float(df["delta_vs_baseline_pct"].quantile(0.25)),
        "q75_delta_pct": float(df["delta_vs_baseline_pct"].quantile(0.75)),
        "median_days_in_cash": float(df["days_in_cash"].median()),
    }


def pct(x):
    if x is None:
        return "n/a"
    return f"{100*x:+.2f}%"


def write_report(payload: dict, run_dir: Path) -> None:
    lines = [
        "# U10 Consecutive Positive-Month Streak >=95% v1",
        "",
        "Mode: STRESS_TEST_ONLY",
        "Live/paper logic: UNCHANGED",
        "",
        "Trigger: at least 2 consecutive positive selected months, cumulative gain >=95%, any non-positive month resets the streak.",
        "",
        "## Canonical U10 events",
        "",
        f"Qualifying streaks: {payload['canonical_event_summary']['event_count']}",
        f"-25% subsequent pullback hit within 31d: {pct(payload['canonical_event_summary']['hit25_31'])}",
        f"within 62d: {pct(payload['canonical_event_summary']['hit25_62'])}",
        f"within 93d: {pct(payload['canonical_event_summary']['hit25_93'])}",
        "",
        "## Canonical portfolio",
        "",
        "| Scenario | Final equity | Delta vs baseline | Max DD | Events | Cash-outs | Reentries | Cash days |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
        f"| Baseline | {payload['canonical_baseline']['final_equity_usdt']:,.2f} | — | "
        f"{pct(payload['canonical_baseline']['max_drawdown'])} | — | — | — | — |",
    ]

    for label in ("P1", "P2"):
        row = payload["canonical"][label]
        lines.append(
            f"| {label}-STREAK95 | {row['final_equity_usdt']:,.2f} | "
            f"{pct(row['delta_vs_baseline_pct'])} | {pct(row['max_drawdown'])} | "
            f"{row['streak_event_count']} | {row['cashouts']} | {row['reentries']} | "
            f"{row['days_in_cash']} |"
        )

    lines += [
        "",
        "## 791 alternative U10s",
        "",
        "| Variant | Universes with event | Final > baseline | DD better | Both better | Unfinished | Median delta |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]

    for label in ("P1", "P2"):
        s = payload["alternative_summary"][label]
        lines.append(
            f"| {label} | {100*s['universe_with_event_rate']:.2f}% | "
            f"{100*s['final_gt_baseline_rate']:.2f}% | "
            f"{100*s['dd_better_rate']:.2f}% | "
            f"{100*s['both_better_rate']:.2f}% | "
            f"{100*s['unfinished_rate']:.2f}% | {pct(s['median_delta_pct'])} |"
        )

    lines += [
        "",
        "## Guardrail",
        "",
        "- 95% was the only cumulative threshold tested.",
        "- Minimum streak length was fixed at 2 months.",
        "- A negative/zero selected month resets the streak.",
        "- 5-bar fractal mechanics were frozen.",
        "- No combination with the single-month +100% trigger was tested.",
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
    event_frames = []
    cycle_frames = []
    canonical = {}
    canonical_baseline = None
    canonical_event_summary = None

    for idx, assets in enumerate(all_u10_universes(), start=1):
        key = universe_key(assets)
        is_canonical = key == canonical_key

        baseline, reference = run_reference(
            panel, event_map, assets, mature_start, INITIAL_USDT
        )
        reference = reference.reset_index(drop=True)
        monthly, _ = monthly_state(reference, INITIAL_USDT)

        streak_events = build_streak_events(reference, monthly)
        event_study = build_event_study(reference, streak_events)

        if not event_study.empty:
            ef = event_study.copy()
            ef.insert(0, "is_canonical", is_canonical)
            ef.insert(0, "universe_key", key)
            event_frames.append(ef)

        if is_canonical:
            canonical_baseline = baseline
            canonical_event_summary = summarize_events(event_study)

        for label, cfg in (("P1", P1), ("P2", P2)):
            result, cycles = run_streak_overlay(
                panel_eval,
                reference,
                streak_events,
                cashout_pullback=cfg["pullback"],
            )
            result["delta_vs_baseline_usdt"] = (
                result["final_equity_usdt"] - baseline["final_equity_usdt"]
            )
            result["delta_vs_baseline_pct"] = (
                result["final_equity_usdt"] / baseline["final_equity_usdt"] - 1.0
            )
            result["dd_improvement_pp"] = (
                result["max_drawdown"] - baseline["max_drawdown"]
            ) * 100.0

            row = {
                "universe_key": key,
                "variant": label,
                "is_canonical": is_canonical,
                "baseline_final_equity_usdt": baseline["final_equity_usdt"],
                **result,
            }
            rows.append(row)

            if is_canonical:
                canonical[label] = dict(result)

            if not cycles.empty:
                cc = cycles.copy()
                cc.insert(0, "is_canonical", is_canonical)
                cc.insert(0, "variant", label)
                cc.insert(0, "universe_key", key)
                cycle_frames.append(cc)

        if idx % 100 == 0:
            print(f"processed_universes={idx}")

    df = pd.DataFrame(rows)
    events_df = pd.concat(event_frames, ignore_index=True) if event_frames else pd.DataFrame()
    cycles_df = pd.concat(cycle_frames, ignore_index=True) if cycle_frames else pd.DataFrame()

    alternative_summary = {}
    for label in ("P1", "P2"):
        alt = df[(df["variant"] == label) & (~df["is_canonical"])].copy()
        alternative_summary[label] = summarize_alt(alt)

    alt_event_summary = summarize_events(
        events_df[events_df["is_canonical"] == False].copy()
        if not events_df.empty else pd.DataFrame()
    )

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    df.to_csv(run_dir / "all_792_streak95_results.csv", index=False)
    if not events_df.empty:
        events_df.to_csv(run_dir / "all_streak95_events.csv", index=False)
    if not cycles_df.empty:
        cycles_df.to_csv(run_dir / "all_streak95_cycles.csv", index=False)

    payload = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit": source_sha(),
        "mature_start": mature_start.isoformat(),
        "parameters": {
            "streak_threshold": STREAK_THRESHOLD,
            "minimum_positive_months": MIN_STREAK_MONTHS,
            "cash_fraction": CASH_FRACTION,
            "p1_pullback": 0.05,
            "p2_pullback": 0.10,
            "search_drawdown": SEARCH_DRAWDOWN,
            "pivot_left": 2,
            "pivot_right": 2,
        },
        "canonical_baseline": canonical_baseline,
        "canonical_event_summary": canonical_event_summary,
        "canonical": canonical,
        "alternative_event_summary": alt_event_summary,
        "alternative_summary": alternative_summary,
        "data_metadata": metadata,
    }

    (run_dir / "results.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False),
        encoding="utf-8",
    )
    write_report(payload, run_dir)

    print("run_dir=" + str(run_dir))
    print("CAN_EVENT", canonical_event_summary)
    for label in ("P1", "P2"):
        c = canonical[label]
        s = alternative_summary[label]
        print(
            "%s CAN final=%.8f delta=%.8f dd=%.8f events=%d cashouts=%d re=%d cashdays=%d unfinished=%d"
            % (
                label,
                c["final_equity_usdt"],
                c["delta_vs_baseline_pct"],
                c["max_drawdown"],
                c["streak_event_count"],
                c["cashouts"],
                c["reentries"],
                c["days_in_cash"],
                c["unfinished_cycles"],
            )
        )
        print(
            "%s ALT eventrate=%.6f final=%.6f both=%.6f unfinished=%.6f med=%.6f"
            % (
                label,
                s["universe_with_event_rate"],
                s["final_gt_baseline_rate"],
                s["both_better_rate"],
                s["unfinished_rate"],
                s["median_delta_pct"],
            )
        )
    print("ALT_EVENT", alt_event_summary)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
