from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
from pathlib import Path

import pandas as pd

from scripts.research_u10_monthly_surge_pullback_v1 import (
    CANONICAL_U10,
    CASH_FRACTION,
    INITIAL_USDT,
    PRIMARY_VARIANTS,
    all_u10_universes,
    analyze_values,
    monthly_state,
    param_key,
    run_overlay,
    run_reference,
    source_sha,
    universe_key,
)
from scripts.research_u10_last_year_vs_u8_universes_v1 import (
    COST,
    LOOKBACK,
    build_events,
    download_panel,
    utc,
)

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "u10_monthly_surge_timeout65_v1"
TIMEOUT_DAYS = 65


def run_overlay_timeout(
    reference: pd.DataFrame,
    anchor_by_month: dict[str, float],
    *,
    surge_threshold: float,
    pullback_threshold: float,
    reentry_threshold: float,
    cash_fraction: float,
    timeout_days: int,
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
    reentry_reason_pending = None
    cashout_signal = None
    reentry_signal = None

    cashout_execution_ts = None
    timeout_deadline = None

    used_surge_months: set[str] = set()

    arms = 0
    cashouts = 0
    natural_reentries = 0
    timeout_reentries = 0
    days_in_cash = 0
    total_cashout_fees = 0.0
    total_reentry_fees = 0.0
    cycle_rows: list[dict] = []
    open_cycle = None

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

        # Natural re-entry signal from the previous close has priority.
        if reentry_pending:
            gross_cash = cash
            fee = gross_cash * COST
            net = gross_cash - fee
            if ref_open[i] <= 0:
                raise RuntimeError("non-positive reference open equity")
            scale += net / ref_open[i]
            cash = 0.0
            total_reentry_fees += fee

            if reentry_reason_pending == "NATURAL_MINUS25":
                natural_reentries += 1
            elif reentry_reason_pending == "TIMEOUT_65D":
                timeout_reentries += 1
            else:
                raise AssertionError("missing reentry reason")

            if open_cycle is not None:
                open_cycle.update(
                    {
                        "reentry_reason": reentry_reason_pending,
                        "reentry_signal_date": (
                            reentry_signal["signal_date"]
                            if reentry_signal is not None
                            else None
                        ),
                        "reentry_execution_date": ts.isoformat(),
                        "reentry_reference_open_usdt": float(ref_open[i]),
                        "reentry_fee_usdt": fee,
                        "cash_cycle_days": int(
                            (ts - cashout_execution_ts).days
                        ),
                    }
                )
                cycle_rows.append(open_cycle)
                open_cycle = None

            reentry_pending = False
            reentry_reason_pending = None
            reentry_signal = None
            cashout_execution_ts = None
            timeout_deadline = None

            armed = False
            arm_month = None
            arm_date = None
            running_peak = None
            locked_peak = None

        # Execute pending cash-out after U10 reaches the current open state.
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
            timeout_deadline = ts + pd.Timedelta(f"{timeout_days}D")
            open_cycle = {
                "arm_month": cashout_signal["arm_month"],
                "arm_date": cashout_signal["arm_date"],
                "cashout_signal_date": cashout_signal["signal_date"],
                "cashout_execution_date": ts.isoformat(),
                "locked_peak_usdt": cashout_signal["locked_peak_usdt"],
                "reference_equity_cashout_signal_usdt": cashout_signal[
                    "reference_equity_signal_usdt"
                ],
                "cashout_pullback_from_peak": cashout_signal[
                    "pullback_from_peak"
                ],
                "gross_cashout_usdt": gross,
                "cashout_fee_usdt": fee,
                "net_cash_parked_usdt": net,
                "timeout_deadline": timeout_deadline.isoformat(),
            }

        # Timeout re-entry happens at the first open on/after the deadline,
        # unless a natural re-entry was already pending and executed above.
        if (
            cash > 0
            and not reentry_pending
            and timeout_deadline is not None
            and ts >= timeout_deadline
        ):
            gross_cash = cash
            fee = gross_cash * COST
            net = gross_cash - fee
            if ref_open[i] <= 0:
                raise RuntimeError("non-positive reference open equity")
            scale += net / ref_open[i]
            cash = 0.0
            total_reentry_fees += fee
            timeout_reentries += 1

            if open_cycle is not None:
                open_cycle.update(
                    {
                        "reentry_reason": "TIMEOUT_65D",
                        "reentry_signal_date": None,
                        "reentry_execution_date": ts.isoformat(),
                        "reentry_reference_open_usdt": float(ref_open[i]),
                        "reentry_fee_usdt": fee,
                        "cash_cycle_days": int(
                            (ts - cashout_execution_ts).days
                        ),
                    }
                )
                cycle_rows.append(open_cycle)
                open_cycle = None

            cashout_execution_ts = None
            timeout_deadline = None
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

        if i == n - 1:
            continue

        # If cash remains parked, only the natural -25% condition is checked
        # at the close. Timeout itself is evaluated at the next open.
        if cash > 0:
            if locked_peak is None:
                raise AssertionError("cash parked without locked peak")
            dd = ref_close[i] / locked_peak - 1.0
            if dd <= -reentry_threshold:
                reentry_signal = {
                    "signal_date": ts.isoformat(),
                    "reference_equity_signal_usdt": ref_close[i],
                    "pullback_from_peak": dd,
                }
                reentry_pending = True
                reentry_reason_pending = "NATURAL_MINUS25"
            continue

        if armed:
            if ref_close[i] > running_peak:
                running_peak = ref_close[i]
            pullback = ref_close[i] / running_peak - 1.0
            if pullback <= -pullback_threshold:
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

    if open_cycle is not None:
        open_cycle.update(
            {
                "reentry_reason": "UNFINISHED_AT_END",
                "reentry_signal_date": None,
                "reentry_execution_date": None,
                "reentry_reference_open_usdt": None,
                "reentry_fee_usdt": None,
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
        "timeout_days": timeout_days,
        "arms": arms,
        "cashouts": cashouts,
        "natural_reentries": natural_reentries,
        "timeout_reentries": timeout_reentries,
        "unfinished_cycles": int(cash > 0),
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


def pct(value: float) -> str:
    return f"{100*value:+.2f}%"


def primary_key(name: str) -> tuple[float, float, float]:
    for n, surge, pullback, reentry in PRIMARY_VARIANTS:
        if n == name:
            return surge, pullback, reentry
    raise KeyError(name)


def summarize_variant(df: pd.DataFrame, name: str) -> dict:
    alt = df[df["is_canonical"] == False].copy()
    return {
        "alternative_count": int(len(alt)),
        "terminal_improve_rate_vs_baseline": float(
            (alt["timeout_delta_vs_baseline_usdt"] > 0).mean()
        ),
        "dd_improve_rate_vs_baseline": float(
            (alt["timeout_max_dd_improvement_pp"] > 0).mean()
        ),
        "both_improve_rate_vs_baseline": float(
            (
                (alt["timeout_delta_vs_baseline_usdt"] > 0)
                & (alt["timeout_max_dd_improvement_pp"] > 0)
            ).mean()
        ),
        "timeout_used_rate": float(
            (alt["timeout_reentries"] > 0).mean()
        ),
        "timeout_better_than_no_timeout_rate": float(
            (alt["timeout_delta_vs_no_timeout_usdt"] > 0).mean()
        ),
        "timeout_worse_than_no_timeout_rate": float(
            (alt["timeout_delta_vs_no_timeout_usdt"] < 0).mean()
        ),
        "median_timeout_delta_vs_no_timeout_pct": float(
            alt["timeout_delta_vs_no_timeout_pct"].median()
        ),
        "q25_timeout_delta_vs_no_timeout_pct": float(
            alt["timeout_delta_vs_no_timeout_pct"].quantile(0.25)
        ),
        "q75_timeout_delta_vs_no_timeout_pct": float(
            alt["timeout_delta_vs_no_timeout_pct"].quantile(0.75)
        ),
        "median_days_in_cash_no_timeout": float(
            alt["no_timeout_days_in_cash"].median()
        ),
        "median_days_in_cash_timeout": float(
            alt["timeout_days_in_cash"].median()
        ),
        "median_days_in_cash_change": float(
            (
                alt["timeout_days_in_cash"]
                - alt["no_timeout_days_in_cash"]
            ).median()
        ),
    }


def write_report(payload: dict, run_dir: Path) -> None:
    c = payload["canonical"]
    p1 = c["P1"]
    p2 = c["P2"]
    a1 = payload["alternative_summary"]["P1"]
    a2 = payload["alternative_summary"]["P2"]

    lines = [
        "# U10 Monthly Surge Pullback + 65d Timeout v1",
        "",
        "Mode: STRESS_TEST_ONLY",
        "Live/paper logic: UNCHANGED",
        "",
        "Fallback: if natural -25% re-entry has not occurred, re-enter at the first daily open on/after 65 calendar days from cash-out execution.",
        "",
        "## Canonical U10",
        "",
        "| Variant | No-timeout final | 65d-timeout final | Timeout delta vs no-timeout | Baseline delta with timeout | Max DD timeout | Natural reentries | Timeout reentries | Cash days no-timeout -> timeout |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name, row in (("P1", p1), ("P2", p2)):
        lines.append(
            f"| {name} | {row['no_timeout_final_equity_usdt']:,.2f} | "
            f"{row['timeout_final_equity_usdt']:,.2f} | "
            f"{pct(row['timeout_delta_vs_no_timeout_pct'])} | "
            f"{pct(row['timeout_delta_vs_baseline_pct'])} | "
            f"{pct(row['timeout_max_drawdown'])} | "
            f"{row['natural_reentries']} | {row['timeout_reentries']} | "
            f"{row['no_timeout_days_in_cash']} -> {row['timeout_days_in_cash']} |"
        )

    lines += [
        "",
        "## 791 alternative U10s",
        "",
        "| Variant | Timeout used | Timeout better than no-timeout | Timeout worse | Median timeout effect | Final > baseline | DD better than baseline | Both better | Median cash days change |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name, row in (("P1", a1), ("P2", a2)):
        lines.append(
            f"| {name} | {100*row['timeout_used_rate']:.2f}% | "
            f"{100*row['timeout_better_than_no_timeout_rate']:.2f}% | "
            f"{100*row['timeout_worse_than_no_timeout_rate']:.2f}% | "
            f"{pct(row['median_timeout_delta_vs_no_timeout_pct'])} | "
            f"{100*row['terminal_improve_rate_vs_baseline']:.2f}% | "
            f"{100*row['dd_improve_rate_vs_baseline']:.2f}% | "
            f"{100*row['both_improve_rate_vs_baseline']:.2f}% | "
            f"{row['median_days_in_cash_change']:+.1f} days |"
        )

    lines += [
        "",
        "## Guardrail",
        "",
        "- 65 days was the only timeout tested.",
        "- No 45/55/75/90-day search was performed.",
        "- P1/P2 signal thresholds and -25% natural re-entry were unchanged.",
        "- Older 2020-2022 data were deliberately not used yet.",
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
    canonical_key = universe_key(CANONICAL_U10)

    rows = []
    cycle_tables = []
    canonical = {}

    for idx, assets in enumerate(universes, start=1):
        key = universe_key(assets)
        is_canonical = key == canonical_key

        baseline, reference = run_reference(
            panel, event_map, assets, mature_start, INITIAL_USDT
        )
        _, anchor_by_month = monthly_state(reference, INITIAL_USDT)

        for name in ("P1", "P2"):
            surge, pullback, reentry = primary_key(name)

            no_timeout, no_cycles = run_overlay(
                reference,
                anchor_by_month,
                surge_threshold=surge,
                pullback_threshold=pullback,
                reentry_threshold=reentry,
                cash_fraction=CASH_FRACTION,
                initial_usdt=INITIAL_USDT,
            )
            timeout, timeout_cycles = run_overlay_timeout(
                reference,
                anchor_by_month,
                surge_threshold=surge,
                pullback_threshold=pullback,
                reentry_threshold=reentry,
                cash_fraction=CASH_FRACTION,
                timeout_days=TIMEOUT_DAYS,
                initial_usdt=INITIAL_USDT,
            )

            row = {
                "universe_key": key,
                "variant": name,
                "is_canonical": is_canonical,
                "baseline_final_equity_usdt": baseline["final_equity_usdt"],
                "baseline_max_drawdown": baseline["max_drawdown"],
                "no_timeout_final_equity_usdt": no_timeout["final_equity_usdt"],
                "no_timeout_max_drawdown": no_timeout["max_drawdown"],
                "no_timeout_days_in_cash": no_timeout["days_in_cash"],
                "timeout_final_equity_usdt": timeout["final_equity_usdt"],
                "timeout_max_drawdown": timeout["max_drawdown"],
                "timeout_days_in_cash": timeout["days_in_cash"],
                "natural_reentries": timeout["natural_reentries"],
                "timeout_reentries": timeout["timeout_reentries"],
                "unfinished_cycles": timeout["unfinished_cycles"],
                "timeout_delta_vs_no_timeout_usdt": (
                    timeout["final_equity_usdt"]
                    - no_timeout["final_equity_usdt"]
                ),
                "timeout_delta_vs_no_timeout_pct": (
                    timeout["final_equity_usdt"]
                    / no_timeout["final_equity_usdt"]
                    - 1.0
                ),
                "timeout_delta_vs_baseline_usdt": (
                    timeout["final_equity_usdt"]
                    - baseline["final_equity_usdt"]
                ),
                "timeout_delta_vs_baseline_pct": (
                    timeout["final_equity_usdt"]
                    / baseline["final_equity_usdt"]
                    - 1.0
                ),
                "timeout_max_dd_improvement_pp": (
                    timeout["max_drawdown"]
                    - baseline["max_drawdown"]
                ) * 100.0,
            }
            rows.append(row)

            if is_canonical:
                canonical[name] = dict(row)

            if not timeout_cycles.empty:
                cc = timeout_cycles.copy()
                cc.insert(0, "variant", name)
                cc.insert(0, "universe_key", key)

                # Attach no-timeout natural re-entry timing for same cash-out cycle.
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
                                "natural_reentry_execution_date": str(
                                    nr["execution_date"]
                                ),
                                "natural_wait_days": int(
                                    (
                                        utc(nr["execution_date"])
                                        - utc(current_cashout["execution_date"])
                                    ).days
                                ),
                            }
                            current_cashout = None

                nat_dates = []
                nat_days = []
                for _, tr in cc.iterrows():
                    k = (
                        str(tr["arm_month"]),
                        str(tr["cashout_execution_date"]),
                    )
                    info = natural_map.get(k, {})
                    nat_dates.append(
                        info.get("natural_reentry_execution_date")
                    )
                    nat_days.append(info.get("natural_wait_days"))
                cc["no_timeout_natural_reentry_execution_date"] = nat_dates
                cc["no_timeout_natural_wait_days"] = nat_days
                cycle_tables.append(cc)

        if idx % 100 == 0:
            print(f"processed_universes={idx}")

    df = pd.DataFrame(rows)
    alt_summary = {
        name: summarize_variant(
            df[df["variant"] == name].copy(), name
        )
        for name in ("P1", "P2")
    }

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    df.to_csv(run_dir / "all_792_timeout_comparison.csv", index=False)
    cycles = (
        pd.concat(cycle_tables, ignore_index=True)
        if cycle_tables
        else pd.DataFrame()
    )
    if not cycles.empty:
        cycles.to_csv(run_dir / "all_timeout_cycles.csv", index=False)
        failure = cycles[
            (cycles["reentry_reason"] == "TIMEOUT_65D")
            | (cycles["reentry_reason"] == "UNFINISHED_AT_END")
        ].copy()
        failure.to_csv(
            run_dir / "timeout_used_or_unfinished_cycles.csv",
            index=False,
        )

    payload = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit": source_sha(),
        "data_metadata": metadata,
        "mature_start": mature_start.isoformat(),
        "timeout_days": TIMEOUT_DAYS,
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
        row = canonical[name]
        print(
            "%s canonical no_timeout=%.8f timeout=%.8f timeout_effect=%.8f "
            "baseline_effect=%.8f natural=%d timeout_re=%d cashdays=%d->%d"
            % (
                name,
                row["no_timeout_final_equity_usdt"],
                row["timeout_final_equity_usdt"],
                row["timeout_delta_vs_no_timeout_pct"],
                row["timeout_delta_vs_baseline_pct"],
                row["natural_reentries"],
                row["timeout_reentries"],
                row["no_timeout_days_in_cash"],
                row["timeout_days_in_cash"],
            )
        )
        a = alt_summary[name]
        print(
            "%s ALT used=%.6f better=%.6f worse=%.6f med=%.6f both=%.6f"
            % (
                name,
                a["timeout_used_rate"],
                a["timeout_better_than_no_timeout_rate"],
                a["timeout_worse_than_no_timeout_rate"],
                a["median_timeout_delta_vs_no_timeout_pct"],
                a["both_improve_rate_vs_baseline"],
            )
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
