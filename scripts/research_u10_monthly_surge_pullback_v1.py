from __future__ import annotations

import argparse
import itertools
import json
import math
import os
import subprocess
from pathlib import Path

import pandas as pd

from scripts.research_u10_last_year_vs_u8_universes_v1 import (
    ARM,
    COST,
    LOOKBACK,
    MANDATORY,
    POOL,
    REVERSAL,
    build_events,
    download_panel,
    utc,
)

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "u10_monthly_surge_pullback_v1"

INITIAL_USDT = 10000.0
START_ASSET = "ATOM"
CANONICAL_U10 = (
    "ATOM", "TWT", "PEPE", "BNB", "SOL",
    "TRX", "AAVE", "LINK", "FIL", "HBAR",
)

SURGE_THRESHOLDS = (0.75, 1.00, 1.25)
PULLBACK_THRESHOLDS = (0.05, 0.10, 0.15)
REENTRY_THRESHOLDS = (0.20, 0.25, 0.30, 0.35)
CASH_FRACTION = 0.30

PRIMARY_VARIANTS = (
    ("P1", 1.00, 0.05, 0.25),
    ("P2", 1.00, 0.10, 0.25),
)


def source_sha() -> str:
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def universe_key(assets: tuple[str, ...]) -> str:
    return "|".join(sorted(assets))


def all_u10_universes() -> list[tuple[str, ...]]:
    rest = [asset for asset in POOL if asset not in MANDATORY]
    return [
        tuple(MANDATORY + extra)
        for extra in itertools.combinations(rest, 7)
    ]


def choose_candidate(event_map, ts, current, aset):
    candidates = [
        event
        for event in event_map.get(ts, [])
        if event["from_asset"] == current and event["to_asset"] in aset
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


def run_reference(
    panel: pd.DataFrame,
    event_map,
    assets: tuple[str, ...],
    start: pd.Timestamp,
    initial_usdt: float,
) -> tuple[dict, pd.DataFrame]:
    aset = set(assets)
    window = panel[panel["timestamp"] >= start].copy().reset_index(drop=True)
    if window.empty:
        raise RuntimeError("empty reference window")

    current = START_ASSET
    qty = initial_usdt / float(window.iloc[0][current + "_open"])
    pending = None
    rows = []
    transitions = 0
    conflicts = 0
    route = [current]

    for pos, row in window.iterrows():
        ts = utc(row["timestamp"])
        transitioned = False

        if pending is not None:
            old_open = float(row[current + "_open"])
            gross = qty * old_open
            current = pending["to_asset"]
            qty = gross * (1.0 - COST) / float(row[current + "_open"])
            pending = None
            transitions += 1
            transitioned = True
            route.append(current)

        current_open = float(row[current + "_open"])
        open_equity = qty * current_open
        close_equity = qty * float(row[current + "_close"])

        rows.append(
            {
                "timestamp": ts,
                "asset": current,
                "asset_qty": qty,
                "open_equity_usdt": open_equity,
                "equity_usdt": close_equity,
                "transitioned_at_open": transitioned,
            }
        )

        if pos == len(window) - 1:
            continue

        candidate, count = choose_candidate(event_map, ts, current, aset)
        if count > 1:
            conflicts += 1
        if candidate is not None:
            pending = dict(candidate)

    equity = pd.DataFrame(rows)
    risk = analyze_values(
        equity["timestamp"],
        equity["equity_usdt"],
        initial_usdt,
    )
    result = {
        "assets": list(assets),
        "start": utc(equity.iloc[0]["timestamp"]).isoformat(),
        "end": utc(equity.iloc[-1]["timestamp"]).isoformat(),
        "initial_usdt": initial_usdt,
        "transitions": transitions,
        "conflicts": conflicts,
        "route": route,
        **risk,
    }
    return result, equity


def analyze_values(timestamps, values, initial_usdt: float) -> dict:
    ts = pd.to_datetime(pd.Series(timestamps), utc=True).reset_index(drop=True)
    vals = pd.Series(values, dtype=float).reset_index(drop=True)

    aug_values = pd.concat(
        [pd.Series([float(initial_usdt)]), vals],
        ignore_index=True,
    )
    synthetic_ts = ts.iloc[0] - pd.Timedelta("1D")
    aug_ts = pd.concat(
        [pd.Series([synthetic_ts]), ts],
        ignore_index=True,
    )

    running_peak = aug_values.cummax()
    drawdown = aug_values / running_peak - 1.0

    min_idx = int(aug_values.idxmin())
    dd_idx = int(drawdown.idxmin())
    peak_idx = int(aug_values.loc[:dd_idx].idxmax())

    def label(idx: int) -> str:
        if idx == 0:
            return "INITIAL"
        return utc(aug_ts.iloc[idx]).isoformat()

    return {
        "final_equity_usdt": float(aug_values.iloc[-1]),
        "total_return": float(aug_values.iloc[-1] / initial_usdt - 1.0),
        "minimum_equity_usdt": float(aug_values.iloc[min_idx]),
        "minimum_equity_date": label(min_idx),
        "minimum_vs_initial": float(
            aug_values.iloc[min_idx] / initial_usdt - 1.0
        ),
        "max_drawdown": float(drawdown.iloc[dd_idx]),
        "max_drawdown_peak_equity_usdt": float(aug_values.iloc[peak_idx]),
        "max_drawdown_peak_date": label(peak_idx),
        "max_drawdown_trough_equity_usdt": float(aug_values.iloc[dd_idx]),
        "max_drawdown_trough_date": label(dd_idx),
    }


def monthly_state(
    reference: pd.DataFrame,
    initial_usdt: float,
) -> tuple[pd.DataFrame, dict[str, float]]:
    frame = reference[["timestamp", "equity_usdt"]].copy()
    frame["month"] = pd.to_datetime(frame["timestamp"], utc=True).dt.strftime("%Y-%m")

    selected_rows = []
    anchor_by_month: dict[str, float] = {}
    previous_selected = float(initial_usdt)

    for month, group in frame.groupby("month", sort=False):
        anchor_by_month[month] = previous_selected

        max_idx = int(group["equity_usdt"].idxmax())
        min_idx = int(group["equity_usdt"].idxmin())
        max_value = float(frame.loc[max_idx, "equity_usdt"])
        min_value = float(frame.loc[min_idx, "equity_usdt"])

        max_change = max_value / previous_selected - 1.0
        min_change = min_value / previous_selected - 1.0

        if abs(max_change) >= abs(min_change):
            idx = max_idx
            typ = "MAX"
            value = max_value
            change = max_change
        else:
            idx = min_idx
            typ = "MIN"
            value = min_value
            change = min_change

        selected_rows.append(
            {
                "month": month,
                "anchor_before_usdt": previous_selected,
                "selected_type": typ,
                "selected_date": utc(frame.loc[idx, "timestamp"]).isoformat(),
                "selected_equity_usdt": value,
                "selected_change": change,
                "monthly_max_usdt": max_value,
                "monthly_min_usdt": min_value,
            }
        )
        previous_selected = value

    return pd.DataFrame(selected_rows), anchor_by_month


def worst_running_drawdown(
    reference: pd.DataFrame,
    event_date: pd.Timestamp,
    horizon_days: int,
    initial_peak: float,
) -> tuple[float, str | None, float]:
    start = utc(event_date)
    end = start + pd.Timedelta(f"{horizon_days}D")
    window = reference[
        (reference["timestamp"] >= start)
        & (reference["timestamp"] <= end)
    ].copy()
    if window.empty:
        return float("nan"), None, float("nan")

    peak = float(initial_peak)
    worst = 0.0
    trough_date = None
    terminal_peak = peak

    for _, row in window.iterrows():
        value = float(row["equity_usdt"])
        if value > peak:
            peak = value
        dd = value / peak - 1.0
        if dd < worst:
            worst = dd
            trough_date = utc(row["timestamp"]).isoformat()
        terminal_peak = peak

    return float(worst), trough_date, float(terminal_peak)


def event_study(
    reference: pd.DataFrame,
    monthly: pd.DataFrame,
    surge_threshold: float = 1.0,
) -> pd.DataFrame:
    rows = []
    qualifying = monthly[monthly["selected_change"] >= surge_threshold]

    for _, event in qualifying.iterrows():
        event_date = utc(event["selected_date"])
        event_value = float(event["selected_equity_usdt"])

        dd31, trough31, peak31 = worst_running_drawdown(
            reference, event_date, 31, event_value
        )
        dd62, trough62, peak62 = worst_running_drawdown(
            reference, event_date, 62, event_value
        )
        rows.append(
            {
                "surge_month": event["month"],
                "surge_date": event_date.isoformat(),
                "surge_selected_change": float(event["selected_change"]),
                "surge_equity_usdt": event_value,
                "worst_running_dd_31d": dd31,
                "hit_minus25_31d": bool(dd31 <= -0.25),
                "trough_date_31d": trough31,
                "peak_seen_31d": peak31,
                "worst_running_dd_62d": dd62,
                "hit_minus25_62d": bool(dd62 <= -0.25),
                "trough_date_62d": trough62,
                "peak_seen_62d": peak62,
            }
        )

    return pd.DataFrame(rows)


def run_overlay(
    reference: pd.DataFrame,
    anchor_by_month: dict[str, float],
    *,
    surge_threshold: float,
    pullback_threshold: float,
    reentry_threshold: float,
    cash_fraction: float,
    initial_usdt: float,
) -> tuple[dict, pd.DataFrame]:
    n = len(reference)
    if n == 0:
        raise RuntimeError("empty reference")

    timestamps = [utc(value) for value in reference["timestamp"]]
    months = [ts.strftime("%Y-%m") for ts in timestamps]
    ref_open = reference["open_equity_usdt"].astype(float).tolist()
    ref_close = reference["equity_usdt"].astype(float).tolist()

    scale = 1.0
    cash = 0.0
    armed = False
    arm_month = None
    arm_date = None
    running_peak = None
    locked_peak = None

    cashout_pending = False
    reentry_pending = False
    cashout_signal = None
    reentry_signal = None

    used_surge_months: set[str] = set()
    arms = 0
    cashouts = 0
    reentries = 0
    days_in_cash = 0
    total_cashout_fees = 0.0
    total_reentry_fees = 0.0
    cycle_rows = []

    peak_equity = float(initial_usdt)
    peak_date = "INITIAL"
    minimum_equity = float(initial_usdt)
    minimum_date = "INITIAL"
    max_drawdown = 0.0
    max_dd_peak_equity = float(initial_usdt)
    max_dd_peak_date = "INITIAL"
    max_dd_trough_equity = float(initial_usdt)
    max_dd_trough_date = "INITIAL"
    final_total = float(initial_usdt)

    for i in range(n):
        ts = timestamps[i]
        month = months[i]

        # Execute scheduled overlay action after frozen U10 has already reached
        # the current day's reference asset/open state.
        if cashout_pending:
            gross = scale * ref_open[i] * cash_fraction
            fee = gross * COST
            net = gross - fee
            scale *= 1.0 - cash_fraction
            cash += net
            total_cashout_fees += fee
            cashouts += 1
            cashout_pending = False

            cycle_rows.append(
                {
                    "event": "CASH_OUT",
                    "signal_date": cashout_signal["signal_date"],
                    "execution_date": ts.isoformat(),
                    "arm_month": cashout_signal["arm_month"],
                    "arm_date": cashout_signal["arm_date"],
                    "locked_peak_usdt": cashout_signal["locked_peak_usdt"],
                    "reference_equity_signal_usdt": cashout_signal[
                        "reference_equity_signal_usdt"
                    ],
                    "pullback_from_peak": cashout_signal["pullback_from_peak"],
                    "gross_usdt": gross,
                    "fee_usdt": fee,
                    "net_cash_usdt": net,
                }
            )

        if reentry_pending:
            gross_cash = cash
            fee = gross_cash * COST
            net = gross_cash - fee
            if ref_open[i] <= 0:
                raise RuntimeError("non-positive reference open equity")
            scale += net / ref_open[i]
            cash = 0.0
            total_reentry_fees += fee
            reentries += 1
            reentry_pending = False

            cycle_rows.append(
                {
                    "event": "RE_ENTRY",
                    "signal_date": reentry_signal["signal_date"],
                    "execution_date": ts.isoformat(),
                    "arm_month": reentry_signal["arm_month"],
                    "arm_date": reentry_signal["arm_date"],
                    "locked_peak_usdt": reentry_signal["locked_peak_usdt"],
                    "reference_equity_signal_usdt": reentry_signal[
                        "reference_equity_signal_usdt"
                    ],
                    "pullback_from_peak": reentry_signal["pullback_from_peak"],
                    "gross_usdt": gross_cash,
                    "fee_usdt": fee,
                    "net_cash_usdt": net,
                }
            )

            armed = False
            arm_month = None
            arm_date = None
            running_peak = None
            locked_peak = None

        invested_close = scale * ref_close[i]
        total_close = invested_close + cash
        final_total = total_close

        if cash > 0:
            days_in_cash += 1

        if total_close < minimum_equity:
            minimum_equity = total_close
            minimum_date = ts.isoformat()

        if total_close > peak_equity:
            peak_equity = total_close
            peak_date = ts.isoformat()

        drawdown = total_close / peak_equity - 1.0
        if drawdown < max_drawdown:
            max_drawdown = drawdown
            max_dd_peak_equity = peak_equity
            max_dd_peak_date = peak_date
            max_dd_trough_equity = total_close
            max_dd_trough_date = ts.isoformat()

        # End-of-day signal logic. No same-close execution.
        if i == n - 1:
            continue

        if cash > 0:
            if (
                not reentry_pending
                and locked_peak is not None
                and ts > utc(cycle_rows[-1]["execution_date"])
            ):
                dd = ref_close[i] / locked_peak - 1.0
                if dd <= -reentry_threshold:
                    reentry_signal = {
                        "signal_date": ts.isoformat(),
                        "arm_month": arm_month,
                        "arm_date": arm_date,
                        "locked_peak_usdt": locked_peak,
                        "reference_equity_signal_usdt": ref_close[i],
                        "pullback_from_peak": dd,
                    }
                    reentry_pending = True
            continue

        if armed:
            if ref_close[i] > running_peak:
                running_peak = ref_close[i]

            pullback = ref_close[i] / running_peak - 1.0
            if not cashout_pending and pullback <= -pullback_threshold:
                locked_peak = running_peak
                cashout_signal = {
                    "signal_date": ts.isoformat(),
                    "arm_month": arm_month,
                    "arm_date": arm_date,
                    "locked_peak_usdt": locked_peak,
                    "reference_equity_signal_usdt": ref_close[i],
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

        gain = ref_close[i] / anchor - 1.0
        if gain >= surge_threshold:
            armed = True
            arm_month = month
            arm_date = ts.isoformat()
            running_peak = ref_close[i]
            arms += 1

    result = {
        "surge_threshold": surge_threshold,
        "pullback_threshold": pullback_threshold,
        "reentry_threshold": reentry_threshold,
        "cash_fraction": cash_fraction,
        "arms": arms,
        "cashouts": cashouts,
        "reentries": reentries,
        "days_in_cash": days_in_cash,
        "terminal_cash_usdt": cash,
        "cashout_fees_usdt": total_cashout_fees,
        "reentry_fees_usdt": total_reentry_fees,
        "final_equity_usdt": float(final_total),
        "total_return": float(final_total / initial_usdt - 1.0),
        "minimum_equity_usdt": float(minimum_equity),
        "minimum_equity_date": minimum_date,
        "minimum_vs_initial": float(minimum_equity / initial_usdt - 1.0),
        "max_drawdown": float(max_drawdown),
        "max_drawdown_peak_equity_usdt": float(max_dd_peak_equity),
        "max_drawdown_peak_date": max_dd_peak_date,
        "max_drawdown_trough_equity_usdt": float(max_dd_trough_equity),
        "max_drawdown_trough_date": max_dd_trough_date,
    }
    return result, pd.DataFrame(cycle_rows)

def param_key(surge, pullback, reentry) -> str:
    return (
        f"S{int(round(surge*100)):03d}"
        f"_P{int(round(pullback*100)):02d}"
        f"_R{int(round(reentry*100)):02d}"
    )


def summarize_cross_universe(
    overlay_df: pd.DataFrame,
    canonical_key: str,
) -> pd.DataFrame:
    alt = overlay_df[overlay_df["universe_key"] != canonical_key].copy()
    rows = []

    for key, group in alt.groupby("param_key", sort=True):
        first = group.iloc[0]
        terminal_delta = group["terminal_delta_pct_vs_baseline"]
        dd_change = group["max_dd_improvement_pp"]

        rows.append(
            {
                "param_key": key,
                "surge_threshold": float(first["surge_threshold"]),
                "pullback_threshold": float(first["pullback_threshold"]),
                "reentry_threshold": float(first["reentry_threshold"]),
                "alternative_universe_count": len(group),
                "terminal_improve_rate": float(
                    (group["terminal_delta_usdt_vs_baseline"] > 0).mean()
                ),
                "dd_improve_rate": float(
                    (group["max_dd_improvement_pp"] > 0).mean()
                ),
                "both_improve_rate": float(
                    (
                        (group["terminal_delta_usdt_vs_baseline"] > 0)
                        & (group["max_dd_improvement_pp"] > 0)
                    ).mean()
                ),
                "median_terminal_delta_pct": float(terminal_delta.median()),
                "q25_terminal_delta_pct": float(terminal_delta.quantile(0.25)),
                "q75_terminal_delta_pct": float(terminal_delta.quantile(0.75)),
                "median_max_dd_improvement_pp": float(dd_change.median()),
                "median_cashouts": float(group["cashouts"].median()),
                "median_reentries": float(group["reentries"].median()),
                "median_days_in_cash": float(group["days_in_cash"].median()),
            }
        )

    return pd.DataFrame(rows)


def pct(value: float) -> str:
    return f"{100*value:+.2f}%"


def write_report(payload: dict, run_dir: Path) -> None:
    canonical = payload["canonical"]
    p1 = canonical["primary_variants"]["P1"]
    p2 = canonical["primary_variants"]["P2"]
    event = canonical["event_study"]

    cross = pd.DataFrame(payload["cross_universe_summary"])
    p1_cross = cross[cross["param_key"] == param_key(1.0, 0.05, 0.25)].iloc[0]
    p2_cross = cross[cross["param_key"] == param_key(1.0, 0.10, 0.25)].iloc[0]

    lines = [
        "# U10 Monthly Surge -> Pullback Overlay v1",
        "",
        "Mode: STRESS_TEST_ONLY",
        "Live/paper strategy logic: UNCHANGED",
        "",
        "## Canonical U10 direct event study",
        "",
        f"Canonical +100% selected-month surge events: {event['event_count']}",
        f"-25% running-pullback hit rate within 31 days: {100*event['hit_rate_31d']:.2f}%",
        f"-25% running-pullback hit rate within 62 days: {100*event['hit_rate_62d']:.2f}%",
        f"Median worst running DD within 31d: {pct(event['median_dd_31d'])}",
        f"Median worst running DD within 62d: {pct(event['median_dd_62d'])}",
        "",
        "## Canonical baseline",
        "",
        f"Final equity: {canonical['baseline']['final_equity_usdt']:,.2f} USDT",
        f"Return: {pct(canonical['baseline']['total_return'])}",
        f"Max DD: {pct(canonical['baseline']['max_drawdown'])}",
        "",
        "## Primary variants",
        "",
        "| Variant | Surge | Sell after pullback | Cash | Re-enter | Final equity | Delta vs baseline | Max DD | Cash-outs | Re-entries | Days in cash |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]

    for name, row in (("P1", p1), ("P2", p2)):
        lines.append(
            f"| {name} | {100*row['surge_threshold']:.0f}% | "
            f"{100*row['pullback_threshold']:.0f}% | 30% | "
            f"{100*row['reentry_threshold']:.0f}% | "
            f"{row['final_equity_usdt']:,.2f} | "
            f"{pct(row['terminal_delta_pct_vs_baseline'])} | "
            f"{pct(row['max_drawdown'])} | {row['cashouts']} | "
            f"{row['reentries']} | {row['days_in_cash']} |"
        )

    lines += [
        "",
        "## 791 alternative-U10 topology robustness",
        "",
        "| Variant | Terminal improve rate | DD improve rate | Both improve | Median terminal delta | Median DD change | Median cash-outs |",
        "|---|---:|---:|---:|---:|---:|---:|",
        f"| P1: +100 / -5 / re-enter -25 | "
        f"{100*p1_cross['terminal_improve_rate']:.2f}% | "
        f"{100*p1_cross['dd_improve_rate']:.2f}% | "
        f"{100*p1_cross['both_improve_rate']:.2f}% | "
        f"{pct(p1_cross['median_terminal_delta_pct'])} | "
        f"{p1_cross['median_max_dd_improvement_pp']:+.2f} pp | "
        f"{p1_cross['median_cashouts']:.1f} |",
        f"| P2: +100 / -10 / re-enter -25 | "
        f"{100*p2_cross['terminal_improve_rate']:.2f}% | "
        f"{100*p2_cross['dd_improve_rate']:.2f}% | "
        f"{100*p2_cross['both_improve_rate']:.2f}% | "
        f"{pct(p2_cross['median_terminal_delta_pct'])} | "
        f"{p2_cross['median_max_dd_improvement_pp']:+.2f} pp | "
        f"{p2_cross['median_cashouts']:.1f} |",
        "",
        "## Interpretation guardrail",
        "",
        "- P1/P2 are the preregistered user variants and remain primary even if another sensitivity cell looks stronger.",
        "- The 791 alternatives change topology but share the same market dates, so this is not future out-of-sample validation.",
        "- Sensitivity-grid leaders are exploratory regions, not an optimized strategy.",
        "- Cash is modeled as non-yielding USDT.",
        "- Overlay cash-out and re-entry each pay 0.1%.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    ]

    (run_dir / "report.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff", default="2026-09-27T00:00:00Z")
    args = parser.parse_args()

    cutoff = utc(args.cutoff)
    panel, metadata = download_panel(cutoff)
    event_map = build_events(panel)

    mature_start = utc(panel.iloc[LOOKBACK - 1]["timestamp"])
    universes = all_u10_universes()
    if len(universes) != 792:
        raise AssertionError(f"expected 792 U10 universes, got {len(universes)}")

    canonical_key = universe_key(CANONICAL_U10)
    if canonical_key not in {universe_key(u) for u in universes}:
        raise AssertionError("canonical U10 missing from exhaustive U10 set")

    params = [
        (surge, pullback, reentry)
        for surge in SURGE_THRESHOLDS
        for pullback in PULLBACK_THRESHOLDS
        for reentry in REENTRY_THRESHOLDS
    ]

    baseline_rows = []
    overlay_rows = []
    event_rows = []
    canonical_detail = None
    canonical_cycle_tables = []

    for universe_index, assets in enumerate(universes, start=1):
        key = universe_key(assets)
        baseline, reference = run_reference(
            panel, event_map, assets, mature_start, INITIAL_USDT
        )
        monthly, anchor_by_month = monthly_state(reference, INITIAL_USDT)
        events = event_study(reference, monthly, surge_threshold=1.0)

        baseline_rows.append(
            {
                "universe_key": key,
                "assets": "|".join(assets),
                **{k: v for k, v in baseline.items() if k not in {"route", "assets"}},
            }
        )

        if not events.empty:
            ev = events.copy()
            ev.insert(0, "universe_key", key)
            event_rows.append(ev)

        canonical_primary = {}

        for surge, pullback, reentry in params:
            result, cycles = run_overlay(
                reference,
                anchor_by_month,
                surge_threshold=surge,
                pullback_threshold=pullback,
                reentry_threshold=reentry,
                cash_fraction=CASH_FRACTION,
                initial_usdt=INITIAL_USDT,
            )
            result["param_key"] = param_key(surge, pullback, reentry)
            result["universe_key"] = key
            result["terminal_delta_usdt_vs_baseline"] = (
                result["final_equity_usdt"] - baseline["final_equity_usdt"]
            )
            result["terminal_delta_pct_vs_baseline"] = (
                result["final_equity_usdt"] / baseline["final_equity_usdt"] - 1.0
            )
            result["max_dd_improvement_pp"] = (
                result["max_drawdown"] - baseline["max_drawdown"]
            ) * 100.0
            overlay_rows.append(result)

            if key == canonical_key:
                for name, ps, pp, pr in PRIMARY_VARIANTS:
                    if (
                        math.isclose(surge, ps)
                        and math.isclose(pullback, pp)
                        and math.isclose(reentry, pr)
                    ):
                        canonical_primary[name] = dict(result)
                        if not cycles.empty:
                            cc = cycles.copy()
                            cc.insert(0, "variant", name)
                            canonical_cycle_tables.append(cc)

        if key == canonical_key:
            canonical_events = events
            canonical_event_summary = {
                "event_count": int(len(canonical_events)),
                "hit_rate_31d": float(
                    canonical_events["hit_minus25_31d"].mean()
                ) if len(canonical_events) else float("nan"),
                "hit_rate_62d": float(
                    canonical_events["hit_minus25_62d"].mean()
                ) if len(canonical_events) else float("nan"),
                "median_dd_31d": float(
                    canonical_events["worst_running_dd_31d"].median()
                ) if len(canonical_events) else float("nan"),
                "median_dd_62d": float(
                    canonical_events["worst_running_dd_62d"].median()
                ) if len(canonical_events) else float("nan"),
            }
            canonical_detail = {
                "baseline": baseline,
                "monthly_selected_extremes": monthly.to_dict(orient="records"),
                "event_study": canonical_event_summary,
                "event_rows": canonical_events.to_dict(orient="records"),
                "primary_variants": canonical_primary,
            }

        if universe_index % 100 == 0:
            print(f"processed_universes={universe_index}")

    if canonical_detail is None:
        raise AssertionError("canonical detail was not collected")

    baseline_df = pd.DataFrame(baseline_rows)
    overlay_df = pd.DataFrame(overlay_rows)
    events_df = (
        pd.concat(event_rows, ignore_index=True)
        if event_rows
        else pd.DataFrame()
    )

    cross = summarize_cross_universe(overlay_df, canonical_key)

    alt_events = events_df[events_df["universe_key"] != canonical_key].copy()
    alt_universe_event_counts = (
        alt_events.groupby("universe_key").size()
        if not alt_events.empty else pd.Series(dtype=int)
    )

    pooled_event_summary = {
        "alternative_universes": 791,
        "alternative_universes_with_100pct_surge_event": int(
            alt_events["universe_key"].nunique()
        ) if not alt_events.empty else 0,
        "total_100pct_surge_events": int(len(alt_events)),
        "pooled_hit_rate_31d": float(
            alt_events["hit_minus25_31d"].mean()
        ) if len(alt_events) else None,
        "pooled_hit_rate_62d": float(
            alt_events["hit_minus25_62d"].mean()
        ) if len(alt_events) else None,
        "pooled_median_dd_31d": float(
            alt_events["worst_running_dd_31d"].median()
        ) if len(alt_events) else None,
        "pooled_median_dd_62d": float(
            alt_events["worst_running_dd_62d"].median()
        ) if len(alt_events) else None,
        "median_events_per_universe_with_event": float(
            alt_universe_event_counts.median()
        ) if len(alt_universe_event_counts) else 0.0,
    }

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    baseline_df.to_csv(run_dir / "all_792_u10_baselines.csv", index=False)
    overlay_df.to_csv(run_dir / "all_792_u10_overlay_grid.csv", index=False)
    cross.to_csv(run_dir / "cross_universe_summary.csv", index=False)
    if not events_df.empty:
        events_df.to_csv(run_dir / "surge_event_study.csv", index=False)
    if canonical_cycle_tables:
        pd.concat(canonical_cycle_tables, ignore_index=True).to_csv(
            run_dir / "canonical_primary_cycles.csv", index=False
        )

    payload = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit": source_sha(),
        "data_metadata": metadata,
        "common_start": utc(panel.iloc[0]["timestamp"]).isoformat(),
        "common_end": utc(panel.iloc[-1]["timestamp"]).isoformat(),
        "mature_start": mature_start.isoformat(),
        "universe_count": len(universes),
        "canonical_u10": list(CANONICAL_U10),
        "candidate_pool": list(POOL),
        "parameters": {
            "surge_thresholds": list(SURGE_THRESHOLDS),
            "pullback_thresholds": list(PULLBACK_THRESHOLDS),
            "reentry_thresholds": list(REENTRY_THRESHOLDS),
            "cash_fraction": CASH_FRACTION,
            "transition_cost": COST,
            "overlay_cashout_cost": COST,
            "overlay_reentry_cost": COST,
        },
        "canonical": canonical_detail,
        "alternative_event_study": pooled_event_summary,
        "cross_universe_summary": cross.to_dict(orient="records"),
    }

    (run_dir / "results.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False),
        encoding="utf-8",
    )
    write_report(payload, run_dir)

    print("run_dir=" + str(run_dir))
    print("canonical_final=%.8f" % canonical_detail["baseline"]["final_equity_usdt"])
    print("canonical_dd=%.8f" % canonical_detail["baseline"]["max_drawdown"])
    print("canonical_event_count=%d" % canonical_detail["event_study"]["event_count"])
    print(
        "canonical_event_hit31=%.6f"
        % canonical_detail["event_study"]["hit_rate_31d"]
    )
    print(
        "canonical_event_hit62=%.6f"
        % canonical_detail["event_study"]["hit_rate_62d"]
    )
    for name in ("P1", "P2"):
        row = canonical_detail["primary_variants"][name]
        print(
            "%s final=%.8f delta=%.8f dd=%.8f cashouts=%d reentries=%d cashdays=%d"
            % (
                name,
                row["final_equity_usdt"],
                row["terminal_delta_pct_vs_baseline"],
                row["max_drawdown"],
                row["cashouts"],
                row["reentries"],
                row["days_in_cash"],
            )
        )

    for name, surge, pullback, reentry in PRIMARY_VARIANTS:
        key = param_key(surge, pullback, reentry)
        row = cross[cross["param_key"] == key].iloc[0]
        print(
            "%s_ALT improve=%.6f dd_improve=%.6f both=%.6f median_delta=%.6f median_ddpp=%.6f"
            % (
                name,
                row["terminal_improve_rate"],
                row["dd_improve_rate"],
                row["both_improve_rate"],
                row["median_terminal_delta_pct"],
                row["median_max_dd_improvement_pp"],
            )
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
