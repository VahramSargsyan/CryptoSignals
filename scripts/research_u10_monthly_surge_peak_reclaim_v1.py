from __future__ import annotations

import argparse
import json
import math
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
from scripts.research_u10_last_year_vs_u8_universes_v1 import (
    COST,
    LOOKBACK,
    build_events,
    download_panel,
    utc,
)

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "u10_monthly_surge_peak_reclaim_v1"


def primary_key(name: str) -> tuple[float, float, float]:
    for n, surge, pullback, reentry in PRIMARY_VARIANTS:
        if n == name:
            return surge, pullback, reentry
    raise KeyError(name)


def run_overlay_peak_reclaim(
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
    reentry_reason_pending = None
    cashout_signal = None
    reentry_signal = None

    cashout_execution_ts = None
    used_surge_months: set[str] = set()

    arms = 0
    cashouts = 0
    natural_reentries = 0
    reclaim_reentries = 0
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
            elif reentry_reason_pending == "PEAK_RECLAIM":
                reclaim_reentries += 1
            else:
                raise AssertionError("missing re-entry reason")

            if open_cycle is not None:
                open_cycle.update(
                    {
                        "reentry_reason": reentry_reason_pending,
                        "reentry_signal_date": reentry_signal["signal_date"],
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

            armed = False
            arm_month = None
            arm_date = None
            running_peak = None
            locked_peak = None

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
            if locked_peak is None:
                raise AssertionError("cash parked without locked peak")

            dd = ref_close[i] / locked_peak - 1.0

            # Natural deep-pullback re-entry remains primary.
            if dd <= -reentry_threshold:
                reentry_signal = {
                    "signal_date": ts.isoformat(),
                    "reference_equity_signal_usdt": ref_close[i],
                    "pullback_from_peak": dd,
                }
                reentry_pending = True
                reentry_reason_pending = "NATURAL_MINUS25"
            elif ref_close[i] >= locked_peak:
                reentry_signal = {
                    "signal_date": ts.isoformat(),
                    "reference_equity_signal_usdt": ref_close[i],
                    "pullback_from_peak": dd,
                }
                reentry_pending = True
                reentry_reason_pending = "PEAK_RECLAIM"
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
        "arms": arms,
        "cashouts": cashouts,
        "natural_reentries": natural_reentries,
        "reclaim_reentries": reclaim_reentries,
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


def summary(df: pd.DataFrame) -> dict:
    alt = df[df["is_canonical"] == False].copy()
    return {
        "alternative_count": int(len(alt)),
        "reclaim_used_rate": float((alt["reclaim_reentries"] > 0).mean()),
        "final_gt_baseline_rate": float(
            (alt["reclaim_delta_vs_baseline_usdt"] > 0).mean()
        ),
        "dd_better_baseline_rate": float(
            (alt["reclaim_dd_improvement_pp"] > 0).mean()
        ),
        "both_better_baseline_rate": float(
            (
                (alt["reclaim_delta_vs_baseline_usdt"] > 0)
                & (alt["reclaim_dd_improvement_pp"] > 0)
            ).mean()
        ),
        "reclaim_better_no_fallback_rate": float(
            (alt["reclaim_delta_vs_no_fallback_usdt"] > 0).mean()
        ),
        "reclaim_worse_no_fallback_rate": float(
            (alt["reclaim_delta_vs_no_fallback_usdt"] < 0).mean()
        ),
        "reclaim_better_timeout65_rate": float(
            (alt["reclaim_delta_vs_timeout65_usdt"] > 0).mean()
        ),
        "reclaim_worse_timeout65_rate": float(
            (alt["reclaim_delta_vs_timeout65_usdt"] < 0).mean()
        ),
        "median_delta_vs_no_fallback_pct": float(
            alt["reclaim_delta_vs_no_fallback_pct"].median()
        ),
        "q25_delta_vs_no_fallback_pct": float(
            alt["reclaim_delta_vs_no_fallback_pct"].quantile(0.25)
        ),
        "q75_delta_vs_no_fallback_pct": float(
            alt["reclaim_delta_vs_no_fallback_pct"].quantile(0.75)
        ),
        "median_delta_vs_timeout65_pct": float(
            alt["reclaim_delta_vs_timeout65_pct"].median()
        ),
        "median_days_in_cash_reclaim": float(
            alt["reclaim_days_in_cash"].median()
        ),
        "median_days_in_cash_no_fallback": float(
            alt["no_fallback_days_in_cash"].median()
        ),
    }


def pct(x: float) -> str:
    return f"{100*x:+.2f}%"


def write_report(payload: dict, run_dir: Path) -> None:
    c = payload["canonical"]
    s = payload["alternative_summary"]

    lines = [
        "# U10 Monthly Surge Pullback + Peak-Reclaim Fallback v1",
        "",
        "Mode: STRESS_TEST_ONLY",
        "Live/paper logic: UNCHANGED",
        "",
        "Fallback: after cash-out, prefer natural -25% re-entry. If reference equity instead closes back at/above the locked peak first, re-enter next open.",
        "",
        "## Canonical U10",
        "",
        "| Variant | No fallback final | 65d final | Peak-reclaim final | Reclaim vs no fallback | Reclaim vs 65d | Max DD reclaim | Natural | Reclaim | Cash days |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]

    for name in ("P1", "P2"):
        row = c[name]
        lines.append(
            f"| {name} | {row['no_fallback_final_equity_usdt']:,.2f} | "
            f"{row['timeout65_final_equity_usdt']:,.2f} | "
            f"{row['reclaim_final_equity_usdt']:,.2f} | "
            f"{pct(row['reclaim_delta_vs_no_fallback_pct'])} | "
            f"{pct(row['reclaim_delta_vs_timeout65_pct'])} | "
            f"{pct(row['reclaim_max_drawdown'])} | "
            f"{row['natural_reentries']} | {row['reclaim_reentries']} | "
            f"{row['reclaim_days_in_cash']} |"
        )

    lines += [
        "",
        "## 791 alternative U10s",
        "",
        "| Variant | Reclaim used | Final > baseline | DD better | Both better | Reclaim > no fallback | Reclaim > 65d | Median delta vs no fallback |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]

    for name in ("P1", "P2"):
        row = s[name]
        lines.append(
            f"| {name} | {100*row['reclaim_used_rate']:.2f}% | "
            f"{100*row['final_gt_baseline_rate']:.2f}% | "
            f"{100*row['dd_better_baseline_rate']:.2f}% | "
            f"{100*row['both_better_baseline_rate']:.2f}% | "
            f"{100*row['reclaim_better_no_fallback_rate']:.2f}% | "
            f"{100*row['reclaim_better_timeout65_rate']:.2f}% | "
            f"{pct(row['median_delta_vs_no_fallback_pct'])} |"
        )

    lines += [
        "",
        "## Guardrail",
        "",
        "- No time threshold is used by peak reclaim.",
        "- No reclaim buffer or MA filter was tested.",
        "- P1/P2 and natural -25% re-entry were unchanged.",
        "- 2020-2022 data were still not opened during fallback selection.",
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
                reference,
                anchors,
                surge_threshold=surge,
                pullback_threshold=pullback,
                reentry_threshold=reentry,
                cash_fraction=CASH_FRACTION,
                initial_usdt=INITIAL_USDT,
            )

            timeout65, timeout_cycles = run_overlay_timeout(
                reference,
                anchors,
                surge_threshold=surge,
                pullback_threshold=pullback,
                reentry_threshold=reentry,
                cash_fraction=CASH_FRACTION,
                timeout_days=TIMEOUT_DAYS,
                initial_usdt=INITIAL_USDT,
            )

            reclaim, reclaim_cycles = run_overlay_peak_reclaim(
                reference,
                anchors,
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
                "no_fallback_max_drawdown": no_fallback["max_drawdown"],
                "no_fallback_days_in_cash": no_fallback["days_in_cash"],
                "timeout65_final_equity_usdt": timeout65["final_equity_usdt"],
                "timeout65_max_drawdown": timeout65["max_drawdown"],
                "timeout65_days_in_cash": timeout65["days_in_cash"],
                "reclaim_final_equity_usdt": reclaim["final_equity_usdt"],
                "reclaim_max_drawdown": reclaim["max_drawdown"],
                "reclaim_days_in_cash": reclaim["days_in_cash"],
                "natural_reentries": reclaim["natural_reentries"],
                "reclaim_reentries": reclaim["reclaim_reentries"],
                "unfinished_cycles": reclaim["unfinished_cycles"],
                "reclaim_delta_vs_baseline_usdt": (
                    reclaim["final_equity_usdt"]
                    - baseline["final_equity_usdt"]
                ),
                "reclaim_delta_vs_baseline_pct": (
                    reclaim["final_equity_usdt"]
                    / baseline["final_equity_usdt"]
                    - 1.0
                ),
                "reclaim_dd_improvement_pp": (
                    reclaim["max_drawdown"]
                    - baseline["max_drawdown"]
                ) * 100.0,
                "reclaim_delta_vs_no_fallback_usdt": (
                    reclaim["final_equity_usdt"]
                    - no_fallback["final_equity_usdt"]
                ),
                "reclaim_delta_vs_no_fallback_pct": (
                    reclaim["final_equity_usdt"]
                    / no_fallback["final_equity_usdt"]
                    - 1.0
                ),
                "reclaim_delta_vs_timeout65_usdt": (
                    reclaim["final_equity_usdt"]
                    - timeout65["final_equity_usdt"]
                ),
                "reclaim_delta_vs_timeout65_pct": (
                    reclaim["final_equity_usdt"]
                    / timeout65["final_equity_usdt"]
                    - 1.0
                ),
            }
            rows.append(row)

            if is_canonical:
                canonical[name] = dict(row)

            if not reclaim_cycles.empty:
                rc = reclaim_cycles.copy()
                rc.insert(0, "variant", name)
                rc.insert(0, "universe_key", key)

                # Attach eventual natural re-entry from no-fallback path.
                natural_map = {}
                if not no_cycles.empty:
                    active_cashout = None
                    for _, nr in no_cycles.iterrows():
                        if nr["event"] == "CASH_OUT":
                            active_cashout = nr
                        elif nr["event"] == "RE_ENTRY" and active_cashout is not None:
                            k = (
                                str(active_cashout["arm_month"]),
                                str(active_cashout["execution_date"]),
                            )
                            natural_map[k] = {
                                "natural_reentry_execution_date": str(
                                    nr["execution_date"]
                                ),
                                "natural_wait_days": int(
                                    (
                                        utc(nr["execution_date"])
                                        - utc(active_cashout["execution_date"])
                                    ).days
                                ),
                            }
                            active_cashout = None

                nat_dates = []
                nat_days = []
                for _, rr in rc.iterrows():
                    k = (
                        str(rr["arm_month"]),
                        str(rr["cashout_execution_date"]),
                    )
                    info = natural_map.get(k, {})
                    nat_dates.append(info.get("natural_reentry_execution_date"))
                    nat_days.append(info.get("natural_wait_days"))
                rc["no_fallback_natural_reentry_execution_date"] = nat_dates
                rc["no_fallback_natural_wait_days"] = nat_days
                cycle_tables.append(rc)

        if idx % 100 == 0:
            print(f"processed_universes={idx}")

    df = pd.DataFrame(rows)
    alt_summary = {
        name: summary(df[df["variant"] == name].copy())
        for name in ("P1", "P2")
    }

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    df.to_csv(run_dir / "all_792_reclaim_comparison.csv", index=False)
    cycles = (
        pd.concat(cycle_tables, ignore_index=True)
        if cycle_tables else pd.DataFrame()
    )
    if not cycles.empty:
        cycles.to_csv(run_dir / "all_reclaim_cycles.csv", index=False)
        cycles[cycles["reentry_reason"] == "PEAK_RECLAIM"].to_csv(
            run_dir / "peak_reclaim_cycles.csv", index=False
        )

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
            "%s CAN no=%.8f timeout=%.8f reclaim=%.8f "
            "reclaim_vs_no=%.8f reclaim_vs_timeout=%.8f natural=%d reclaim_n=%d cashdays=%d"
            % (
                name,
                c["no_fallback_final_equity_usdt"],
                c["timeout65_final_equity_usdt"],
                c["reclaim_final_equity_usdt"],
                c["reclaim_delta_vs_no_fallback_pct"],
                c["reclaim_delta_vs_timeout65_pct"],
                c["natural_reentries"],
                c["reclaim_reentries"],
                c["reclaim_days_in_cash"],
            )
        )
        print(
            "%s ALT used=%.6f finalbase=%.6f both=%.6f "
            "beat_no=%.6f beat65=%.6f median_no=%.6f"
            % (
                name,
                s["reclaim_used_rate"],
                s["final_gt_baseline_rate"],
                s["both_better_baseline_rate"],
                s["reclaim_better_no_fallback_rate"],
                s["reclaim_better_timeout65_rate"],
                s["median_delta_vs_no_fallback_pct"],
            )
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
