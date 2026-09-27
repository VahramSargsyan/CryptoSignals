from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from scripts.research_u10_monthly_surge_pullback_v1 import (
    CANONICAL_U10,
    CASH_FRACTION,
    INITIAL_USDT,
    PRIMARY_VARIANTS,
    all_u10_universes,
    monthly_state,
    run_overlay,
    run_reference,
    source_sha,
    universe_key,
)
from scripts.research_u10_monthly_surge_timeout65_v1 import (
    TIMEOUT_DAYS,
    run_overlay_timeout,
)
from scripts.research_u10_monthly_surge_peak_reclaim_v1 import (
    run_overlay_peak_reclaim,
)
from scripts.research_u10_last_year_vs_u8_universes_v1 import (
    COST,
    LOOKBACK,
    build_events,
    download_panel,
    utc,
)

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "u10_monthly_surge_trailing_reentry_peak_v1"


def primary_key(name: str) -> tuple[float, float, float]:
    for n, surge, pullback, reentry in PRIMARY_VARIANTS:
        if n == name:
            return surge, pullback, reentry
    raise KeyError(name)


def run_overlay_trailing_peak(
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

    cashout_pending = False
    reentry_pending = False
    cashout_signal = None
    reentry_signal = None

    cashout_execution_ts = None
    reentry_running_peak = None
    initial_locked_peak = None

    used_surge_months: set[str] = set()

    arms = 0
    cashouts = 0
    reentries = 0
    days_in_cash = 0
    peak_updates_while_cash = 0
    cycles_with_peak_update = 0
    total_cashout_fees = 0.0
    total_reentry_fees = 0.0

    cycle_rows: list[dict] = []
    open_cycle = None
    cycle_peak_updated = False

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

        if reentry_pending:
            gross_cash = cash
            fee = gross_cash * COST
            net = gross_cash - fee
            scale += net / ref_open[i]
            cash = 0.0
            total_reentry_fees += fee
            reentries += 1

            if open_cycle is not None:
                open_cycle.update(
                    {
                        "final_reentry_peak_usdt": reentry_running_peak,
                        "peak_changed_while_cash": cycle_peak_updated,
                        "reentry_signal_date": reentry_signal["signal_date"],
                        "reentry_execution_date": ts.isoformat(),
                        "reentry_reference_open_usdt": float(ref_open[i]),
                        "reentry_fee_usdt": fee,
                        "reentry_drawdown_from_latest_peak": reentry_signal[
                            "drawdown_from_latest_peak"
                        ],
                        "cash_cycle_days": int(
                            (ts - cashout_execution_ts).days
                        ),
                    }
                )
                cycle_rows.append(open_cycle)
                open_cycle = None

            reentry_pending = False
            reentry_signal = None
            cashout_execution_ts = None
            reentry_running_peak = None
            initial_locked_peak = None
            cycle_peak_updated = False

            armed = False
            arm_month = None
            arm_date = None
            running_peak = None

        if cashout_pending:
            gross = scale * ref_open[i] * cash_fraction
            fee = gross * COST
            net = gross - fee
            scale *= 1.0 - cash_fraction
            cash += net
            total_cashout_fees += fee
            cashouts += 1
            cashout_pending = False
            cashout_execution_ts = ts

            initial_locked_peak = float(cashout_signal["locked_peak_usdt"])
            reentry_running_peak = initial_locked_peak
            cycle_peak_updated = False

            open_cycle = {
                "arm_month": cashout_signal["arm_month"],
                "arm_date": cashout_signal["arm_date"],
                "cashout_signal_date": cashout_signal["signal_date"],
                "cashout_execution_date": ts.isoformat(),
                "initial_locked_peak_usdt": initial_locked_peak,
                "reference_equity_cashout_signal_usdt": cashout_signal[
                    "reference_equity_signal_usdt"
                ],
                "cashout_pullback_from_peak": cashout_signal[
                    "pullback_from_peak"
                ],
                "gross_cashout_usdt": gross,
                "cashout_fee_usdt": fee,
                "net_cash_parked_usdt": net,
            }

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

        if i == n - 1:
            continue

        if cash > 0:
            if reentry_running_peak is None:
                raise AssertionError("cash parked without re-entry peak")

            if ref_close[i] > reentry_running_peak:
                reentry_running_peak = ref_close[i]
                peak_updates_while_cash += 1
                if not cycle_peak_updated:
                    cycles_with_peak_update += 1
                    cycle_peak_updated = True

            dd = ref_close[i] / reentry_running_peak - 1.0
            if dd <= -reentry_threshold:
                reentry_signal = {
                    "signal_date": ts.isoformat(),
                    "reference_equity_signal_usdt": ref_close[i],
                    "drawdown_from_latest_peak": dd,
                }
                reentry_pending = True
            continue

        if armed:
            if ref_close[i] > running_peak:
                running_peak = ref_close[i]

            pullback = ref_close[i] / running_peak - 1.0
            if pullback <= -pullback_threshold:
                cashout_signal = {
                    "signal_date": ts.isoformat(),
                    "arm_month": arm_month,
                    "arm_date": arm_date,
                    "locked_peak_usdt": running_peak,
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

    if open_cycle is not None:
        open_cycle.update(
            {
                "final_reentry_peak_usdt": reentry_running_peak,
                "peak_changed_while_cash": cycle_peak_updated,
                "reentry_signal_date": None,
                "reentry_execution_date": None,
                "reentry_reference_open_usdt": None,
                "reentry_fee_usdt": None,
                "reentry_drawdown_from_latest_peak": None,
                "cash_cycle_days": int(
                    (timestamps[-1] - cashout_execution_ts).days
                ),
            }
        )
        cycle_rows.append(open_cycle)

    result = {
        "surge_threshold": surge_threshold,
        "pullback_threshold": pullback_threshold,
        "reentry_threshold": reentry_threshold,
        "cash_fraction": cash_fraction,
        "arms": arms,
        "cashouts": cashouts,
        "reentries": reentries,
        "unfinished_cycles": int(cash > 0),
        "days_in_cash": days_in_cash,
        "peak_updates_while_cash": peak_updates_while_cash,
        "cycles_with_peak_update": cycles_with_peak_update,
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


def summarize(df: pd.DataFrame) -> dict:
    alt = df[~df["is_canonical"]].copy()
    return {
        "alternative_count": int(len(alt)),
        "final_gt_baseline_rate": float(
            (alt["trailing_delta_vs_baseline_usdt"] > 0).mean()
        ),
        "dd_better_baseline_rate": float(
            (alt["trailing_dd_improvement_pp"] > 0).mean()
        ),
        "both_better_baseline_rate": float(
            (
                (alt["trailing_delta_vs_baseline_usdt"] > 0)
                & (alt["trailing_dd_improvement_pp"] > 0)
            ).mean()
        ),
        "trailing_better_no_fallback_rate": float(
            (alt["trailing_delta_vs_no_fallback_usdt"] > 0).mean()
        ),
        "trailing_better_timeout65_rate": float(
            (alt["trailing_delta_vs_timeout65_usdt"] > 0).mean()
        ),
        "trailing_better_reclaim_rate": float(
            (alt["trailing_delta_vs_reclaim_usdt"] > 0).mean()
        ),
        "median_delta_vs_no_fallback_pct": float(
            alt["trailing_delta_vs_no_fallback_pct"].median()
        ),
        "q25_delta_vs_no_fallback_pct": float(
            alt["trailing_delta_vs_no_fallback_pct"].quantile(0.25)
        ),
        "q75_delta_vs_no_fallback_pct": float(
            alt["trailing_delta_vs_no_fallback_pct"].quantile(0.75)
        ),
        "median_delta_vs_timeout65_pct": float(
            alt["trailing_delta_vs_timeout65_pct"].median()
        ),
        "median_delta_vs_reclaim_pct": float(
            alt["trailing_delta_vs_reclaim_pct"].median()
        ),
        "unfinished_rate": float((alt["trailing_unfinished_cycles"] > 0).mean()),
        "peak_changed_rate": float(
            (alt["trailing_cycles_with_peak_update"] > 0).mean()
        ),
        "median_days_in_cash": float(alt["trailing_days_in_cash"].median()),
    }


def pct(value: float) -> str:
    return f"{100*value:+.2f}%"


def write_report(payload: dict, run_dir: Path) -> None:
    c = payload["canonical"]
    s = payload["alternative_summary"]

    lines = [
        "# U10 Monthly Surge Pullback + Trailing Re-entry Peak v1",
        "",
        "Mode: STRESS_TEST_ONLY",
        "Live/paper logic: UNCHANGED",
        "",
        "Rule: while cash is parked, keep updating the U10 reference running peak. Re-enter only after a 25% close-to-peak drawdown from the latest peak.",
        "",
        "## Canonical U10",
        "",
        "| Variant | Baseline | No fallback | 65d | Peak reclaim | Trailing peak | Trailing vs no fallback | Max DD trailing | Cash days |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name in ("P1", "P2"):
        row = c[name]
        lines.append(
            f"| {name} | {row['baseline_final_equity_usdt']:,.2f} | "
            f"{row['no_fallback_final_equity_usdt']:,.2f} | "
            f"{row['timeout65_final_equity_usdt']:,.2f} | "
            f"{row['reclaim_final_equity_usdt']:,.2f} | "
            f"{row['trailing_final_equity_usdt']:,.2f} | "
            f"{pct(row['trailing_delta_vs_no_fallback_pct'])} | "
            f"{pct(row['trailing_max_drawdown'])} | "
            f"{row['trailing_days_in_cash']} |"
        )

    lines += [
        "",
        "## 791 alternative U10s",
        "",
        "| Variant | Final > baseline | DD better | Both better | Trailing > no fallback | Trailing > 65d | Trailing > reclaim | Peak updated while cash | Unfinished |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name in ("P1", "P2"):
        row = s[name]
        lines.append(
            f"| {name} | {100*row['final_gt_baseline_rate']:.2f}% | "
            f"{100*row['dd_better_baseline_rate']:.2f}% | "
            f"{100*row['both_better_baseline_rate']:.2f}% | "
            f"{100*row['trailing_better_no_fallback_rate']:.2f}% | "
            f"{100*row['trailing_better_timeout65_rate']:.2f}% | "
            f"{100*row['trailing_better_reclaim_rate']:.2f}% | "
            f"{100*row['peak_changed_rate']:.2f}% | "
            f"{100*row['unfinished_rate']:.2f}% |"
        )

    lines += [
        "",
        "## Guardrail",
        "",
        "- No new threshold was introduced.",
        "- Re-entry depth remains exactly -25%.",
        "- No timeout or reclaim trigger is used.",
        "- 2020-2022 remains unopened for rule selection.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    ]
    (run_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff", default="2026-09-27T00:00:00Z")
    args = parser.parse_args()

    panel, metadata = download_panel(utc(args.cutoff))
    event_map = build_events(panel)
    mature_start = utc(panel.iloc[LOOKBACK - 1]["timestamp"])

    canonical_key = universe_key(CANONICAL_U10)
    rows = []
    cycle_tables = []
    canonical = {}

    for idx, assets in enumerate(all_u10_universes(), start=1):
        key = universe_key(assets)
        is_canonical = key == canonical_key

        baseline, reference = run_reference(
            panel, event_map, assets, mature_start, INITIAL_USDT
        )
        _, anchors = monthly_state(reference, INITIAL_USDT)

        for name in ("P1", "P2"):
            surge, pullback, reentry = primary_key(name)

            no_fallback, no_cycles = run_overlay(
                reference, anchors,
                surge_threshold=surge,
                pullback_threshold=pullback,
                reentry_threshold=reentry,
                cash_fraction=CASH_FRACTION,
                initial_usdt=INITIAL_USDT,
            )
            timeout65, _ = run_overlay_timeout(
                reference, anchors,
                surge_threshold=surge,
                pullback_threshold=pullback,
                reentry_threshold=reentry,
                cash_fraction=CASH_FRACTION,
                timeout_days=TIMEOUT_DAYS,
                initial_usdt=INITIAL_USDT,
            )
            reclaim, _ = run_overlay_peak_reclaim(
                reference, anchors,
                surge_threshold=surge,
                pullback_threshold=pullback,
                reentry_threshold=reentry,
                cash_fraction=CASH_FRACTION,
                initial_usdt=INITIAL_USDT,
            )
            trailing, trailing_cycles = run_overlay_trailing_peak(
                reference, anchors,
                surge_threshold=surge,
                pullback_threshold=pullback,
                reentry_threshold=reentry,
                cash_fraction=CASH_FRACTION,
                initial_usdt=INITIAL_USDT,
            )

            row = {
                "universe_key": key,
                "variant": name,
                "is_canonical": is_canonical,
                "baseline_final_equity_usdt": baseline["final_equity_usdt"],
                "baseline_max_drawdown": baseline["max_drawdown"],
                "no_fallback_final_equity_usdt": no_fallback["final_equity_usdt"],
                "timeout65_final_equity_usdt": timeout65["final_equity_usdt"],
                "reclaim_final_equity_usdt": reclaim["final_equity_usdt"],
                "trailing_final_equity_usdt": trailing["final_equity_usdt"],
                "trailing_max_drawdown": trailing["max_drawdown"],
                "trailing_days_in_cash": trailing["days_in_cash"],
                "trailing_reentries": trailing["reentries"],
                "trailing_unfinished_cycles": trailing["unfinished_cycles"],
                "trailing_cycles_with_peak_update": trailing["cycles_with_peak_update"],
                "trailing_peak_updates_while_cash": trailing["peak_updates_while_cash"],
                "trailing_delta_vs_baseline_usdt": (
                    trailing["final_equity_usdt"] - baseline["final_equity_usdt"]
                ),
                "trailing_delta_vs_baseline_pct": (
                    trailing["final_equity_usdt"] / baseline["final_equity_usdt"] - 1
                ),
                "trailing_dd_improvement_pp": (
                    trailing["max_drawdown"] - baseline["max_drawdown"]
                ) * 100,
                "trailing_delta_vs_no_fallback_usdt": (
                    trailing["final_equity_usdt"] - no_fallback["final_equity_usdt"]
                ),
                "trailing_delta_vs_no_fallback_pct": (
                    trailing["final_equity_usdt"] / no_fallback["final_equity_usdt"] - 1
                ),
                "trailing_delta_vs_timeout65_usdt": (
                    trailing["final_equity_usdt"] - timeout65["final_equity_usdt"]
                ),
                "trailing_delta_vs_timeout65_pct": (
                    trailing["final_equity_usdt"] / timeout65["final_equity_usdt"] - 1
                ),
                "trailing_delta_vs_reclaim_usdt": (
                    trailing["final_equity_usdt"] - reclaim["final_equity_usdt"]
                ),
                "trailing_delta_vs_reclaim_pct": (
                    trailing["final_equity_usdt"] / reclaim["final_equity_usdt"] - 1
                ),
            }
            rows.append(row)

            if is_canonical:
                canonical[name] = dict(row)

            if not trailing_cycles.empty:
                tc = trailing_cycles.copy()
                tc.insert(0, "variant", name)
                tc.insert(0, "universe_key", key)

                natural_map = {}
                if not no_cycles.empty:
                    current_cashout = None
                    for _, nr in no_cycles.iterrows():
                        if nr["event"] == "CASH_OUT":
                            current_cashout = nr
                        elif nr["event"] == "RE_ENTRY" and current_cashout is not None:
                            k = (
                                str(current_cashout["arm_month"]),
                                str(current_cashout["execution_date"]),
                            )
                            natural_map[k] = {
                                "original_reentry_execution_date": str(nr["execution_date"]),
                                "original_wait_days": int(
                                    (utc(nr["execution_date"]) - utc(current_cashout["execution_date"])).days
                                ),
                            }
                            current_cashout = None

                original_dates = []
                original_days = []
                for _, tr in tc.iterrows():
                    k = (str(tr["arm_month"]), str(tr["cashout_execution_date"]))
                    info = natural_map.get(k, {})
                    original_dates.append(info.get("original_reentry_execution_date"))
                    original_days.append(info.get("original_wait_days"))
                tc["original_reentry_execution_date"] = original_dates
                tc["original_wait_days"] = original_days
                cycle_tables.append(tc)

        if idx % 100 == 0:
            print(f"processed_universes={idx}")

    df = pd.DataFrame(rows)
    alt_summary = {
        name: summarize(df[df["variant"] == name].copy())
        for name in ("P1", "P2")
    }

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    df.to_csv(run_dir / "all_792_trailing_reentry_comparison.csv", index=False)
    cycles = pd.concat(cycle_tables, ignore_index=True) if cycle_tables else pd.DataFrame()
    if not cycles.empty:
        cycles.to_csv(run_dir / "all_trailing_reentry_cycles.csv", index=False)

    payload = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit": source_sha(),
        "data_metadata": metadata,
        "mature_start": mature_start.isoformat(),
        "canonical": canonical,
        "alternative_summary": alt_summary,
    }
    (run_dir / "results.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False),
        encoding="utf-8",
    )
    write_report(payload, run_dir)

    print("run_dir=" + str(run_dir))
    for name in ("P1", "P2"):
        c = canonical[name]
        s = alt_summary[name]
        print(
            "%s CAN base=%.8f no=%.8f t65=%.8f reclaim=%.8f trailing=%.8f "
            "dd=%.8f cash=%d unfinished=%d"
            % (
                name,
                c["baseline_final_equity_usdt"],
                c["no_fallback_final_equity_usdt"],
                c["timeout65_final_equity_usdt"],
                c["reclaim_final_equity_usdt"],
                c["trailing_final_equity_usdt"],
                c["trailing_max_drawdown"],
                c["trailing_days_in_cash"],
                c["trailing_unfinished_cycles"],
            )
        )
        print(
            "%s ALT finalbase=%.6f both=%.6f beatno=%.6f beat65=%.6f "
            "beatreclaim=%.6f peakchanged=%.6f unfinished=%.6f"
            % (
                name,
                s["final_gt_baseline_rate"],
                s["both_better_baseline_rate"],
                s["trailing_better_no_fallback_rate"],
                s["trailing_better_timeout65_rate"],
                s["trailing_better_reclaim_rate"],
                s["peak_changed_rate"],
                s["unfinished_rate"],
            )
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
