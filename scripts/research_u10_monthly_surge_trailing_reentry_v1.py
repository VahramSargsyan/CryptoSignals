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
OUT = ROOT / "research_artifacts" / "u10_monthly_surge_trailing_reentry_v1"


def primary_key(name: str):
    for n, surge, pullback, reentry in PRIMARY_VARIANTS:
        if n == name:
            return surge, pullback, reentry
    raise KeyError(name)


def run_overlay_trailing_reentry(
    reference: pd.DataFrame,
    anchors: dict[str, float],
    *,
    surge_threshold: float,
    pullback_threshold: float,
    reentry_threshold: float,
    cash_fraction: float,
    initial_usdt: float,
):
    timestamps = [utc(x) for x in reference["timestamp"]]
    months = [x.strftime("%Y-%m") for x in timestamps]
    opens = reference["open_equity_usdt"].astype(float).tolist()
    closes = reference["equity_usdt"].astype(float).tolist()

    scale = 1.0
    cash = 0.0
    armed = False
    arm_month = None
    arm_date = None
    pre_cash_peak = None
    reentry_peak = None
    initial_locked_peak = None
    cashout_pending = None
    reentry_pending = None
    cashout_exec_ts = None
    used_months = set()

    arms = cashouts = reentries = days_cash = peak_updates = 0
    cycles = []
    open_cycle = None

    peak_eq = min_eq = float(initial_usdt)
    peak_date = min_date = "INITIAL"
    max_dd = 0.0
    dd_peak_eq = dd_trough_eq = float(initial_usdt)
    dd_peak_date = dd_trough_date = "INITIAL"
    final = float(initial_usdt)

    for i, ts in enumerate(timestamps):
        month = months[i]

        if reentry_pending is not None:
            gross = cash
            fee = gross * COST
            net = gross - fee
            scale += net / opens[i]
            cash = 0.0
            reentries += 1

            if open_cycle is not None:
                open_cycle.update({
                    "reentry_signal_date": reentry_pending["signal_date"],
                    "reentry_execution_date": ts.isoformat(),
                    "reentry_reference_open_usdt": opens[i],
                    "reentry_fee_usdt": fee,
                    "cash_cycle_days": int((ts - cashout_exec_ts).days),
                    "final_reentry_peak_usdt": reentry_pending["reentry_peak_usdt"],
                    "reentry_pullback": reentry_pending["pullback"],
                    "peak_updates_while_cash": reentry_pending["peak_updates"],
                })
                cycles.append(open_cycle)
                open_cycle = None

            reentry_pending = None
            cashout_exec_ts = None
            armed = False
            arm_month = arm_date = None
            pre_cash_peak = reentry_peak = initial_locked_peak = None

        if cashout_pending is not None:
            gross = scale * opens[i] * cash_fraction
            fee = gross * COST
            net = gross - fee
            scale *= 1.0 - cash_fraction
            cash += net
            cashouts += 1
            cashout_exec_ts = ts
            initial_locked_peak = cashout_pending["locked_peak"]
            reentry_peak = initial_locked_peak
            peak_updates = 0

            open_cycle = {
                "arm_month": cashout_pending["arm_month"],
                "arm_date": cashout_pending["arm_date"],
                "cashout_signal_date": cashout_pending["signal_date"],
                "cashout_execution_date": ts.isoformat(),
                "initial_locked_peak_usdt": initial_locked_peak,
                "cashout_signal_equity_usdt": cashout_pending["signal_equity"],
                "cashout_pullback": cashout_pending["pullback"],
                "gross_cashout_usdt": gross,
                "cashout_fee_usdt": fee,
                "net_cash_usdt": net,
            }
            cashout_pending = None

        invested = scale * closes[i]
        total = invested + cash
        final = total

        if cash > 0:
            days_cash += 1

        if total < min_eq:
            min_eq, min_date = total, ts.isoformat()
        if total > peak_eq:
            peak_eq, peak_date = total, ts.isoformat()
        dd = total / peak_eq - 1.0
        if dd < max_dd:
            max_dd = dd
            dd_peak_eq = peak_eq
            dd_peak_date = peak_date
            dd_trough_eq = total
            dd_trough_date = ts.isoformat()

        if i == len(timestamps) - 1:
            continue

        if cash > 0:
            if closes[i] > reentry_peak:
                reentry_peak = closes[i]
                peak_updates += 1
            pullback = closes[i] / reentry_peak - 1.0
            if pullback <= -reentry_threshold:
                reentry_pending = {
                    "signal_date": ts.isoformat(),
                    "reentry_peak_usdt": reentry_peak,
                    "pullback": pullback,
                    "peak_updates": peak_updates,
                }
            continue

        if armed:
            if closes[i] > pre_cash_peak:
                pre_cash_peak = closes[i]
            pullback = closes[i] / pre_cash_peak - 1.0
            if pullback <= -pullback_threshold:
                cashout_pending = {
                    "signal_date": ts.isoformat(),
                    "arm_month": arm_month,
                    "arm_date": arm_date,
                    "locked_peak": pre_cash_peak,
                    "signal_equity": closes[i],
                    "pullback": pullback,
                }
                used_months.add(arm_month)
            continue

        if month in used_months:
            continue
        anchor = anchors.get(month)
        if not anchor:
            continue
        gain = closes[i] / anchor - 1.0
        if gain >= surge_threshold:
            armed = True
            arm_month = month
            arm_date = ts.isoformat()
            pre_cash_peak = closes[i]
            arms += 1

    if open_cycle is not None:
        open_cycle.update({
            "reentry_signal_date": None,
            "reentry_execution_date": None,
            "reentry_reference_open_usdt": None,
            "reentry_fee_usdt": None,
            "cash_cycle_days": int((timestamps[-1] - cashout_exec_ts).days),
            "final_reentry_peak_usdt": reentry_peak,
            "reentry_pullback": closes[-1] / reentry_peak - 1.0 if reentry_peak else None,
            "peak_updates_while_cash": peak_updates,
        })
        cycles.append(open_cycle)

    result = {
        "surge_threshold": surge_threshold,
        "pullback_threshold": pullback_threshold,
        "reentry_threshold": reentry_threshold,
        "cash_fraction": cash_fraction,
        "arms": arms,
        "cashouts": cashouts,
        "reentries": reentries,
        "unfinished_cycles": int(cash > 0),
        "days_in_cash": days_cash,
        "terminal_cash_usdt": cash,
        "final_equity_usdt": float(final),
        "total_return": float(final / initial_usdt - 1.0),
        "minimum_equity_usdt": float(min_eq),
        "minimum_equity_date": min_date,
        "minimum_vs_initial": float(min_eq / initial_usdt - 1.0),
        "max_drawdown": float(max_dd),
        "max_drawdown_peak_equity_usdt": float(dd_peak_eq),
        "max_drawdown_peak_date": dd_peak_date,
        "max_drawdown_trough_equity_usdt": float(dd_trough_eq),
        "max_drawdown_trough_date": dd_trough_date,
    }
    return result, pd.DataFrame(cycles)


def summarize(d: pd.DataFrame):
    alt = d[d["is_canonical"] == False]
    return {
        "count": int(len(alt)),
        "final_gt_baseline_rate": float((alt["trail_delta_vs_baseline_usdt"] > 0).mean()),
        "dd_better_rate": float((alt["trail_dd_improvement_pp"] > 0).mean()),
        "both_rate": float(((alt["trail_delta_vs_baseline_usdt"] > 0) & (alt["trail_dd_improvement_pp"] > 0)).mean()),
        "beat_no_rate": float((alt["trail_delta_vs_no_usdt"] > 0).mean()),
        "beat_65_rate": float((alt["trail_delta_vs_65_usdt"] > 0).mean()),
        "beat_reclaim_rate": float((alt["trail_delta_vs_reclaim_usdt"] > 0).mean()),
        "median_delta_vs_no_pct": float(alt["trail_delta_vs_no_pct"].median()),
        "q25_delta_vs_no_pct": float(alt["trail_delta_vs_no_pct"].quantile(.25)),
        "q75_delta_vs_no_pct": float(alt["trail_delta_vs_no_pct"].quantile(.75)),
        "median_cash_days": float(alt["trail_days_cash"].median()),
        "unfinished_rate": float((alt["trail_unfinished"] > 0).mean()),
    }


def pct(x): return f"{100*x:+.2f}%"


def write_report(payload, run_dir):
    lines = [
        "# U10 Monthly Surge Pullback + Trailing Re-entry Peak v1","",
        "Mode: STRESS_TEST_ONLY",
        "Live/paper logic: UNCHANGED","",
        "After cash-out, the -25% re-entry threshold trails upward with every new reference-equity high.","",
        "## Canonical U10","",
        "| Variant | No fallback | 65d | Old-peak reclaim | Trailing re-entry | Trail vs no fallback | Max DD | Cash days | Unfinished |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name in ("P1","P2"):
        r=payload["canonical"][name]
        lines.append(
            f"| {name} | {r['no_final']:,.2f} | {r['d65_final']:,.2f} | {r['reclaim_final']:,.2f} | "
            f"{r['trail_final']:,.2f} | {pct(r['trail_delta_vs_no_pct'])} | "
            f"{pct(r['trail_max_dd'])} | {r['trail_days_cash']} | {r['trail_unfinished']} |"
        )
    lines += ["","## 791 alternative U10s","",
        "| Variant | Final > baseline | DD better | Both better | Trail > no fallback | Trail > 65d | Trail > old reclaim | Median delta vs no fallback | Unfinished |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for name in ("P1","P2"):
        s=payload["alternative_summary"][name]
        lines.append(
            f"| {name} | {100*s['final_gt_baseline_rate']:.2f}% | {100*s['dd_better_rate']:.2f}% | "
            f"{100*s['both_rate']:.2f}% | {100*s['beat_no_rate']:.2f}% | {100*s['beat_65_rate']:.2f}% | "
            f"{100*s['beat_reclaim_rate']:.2f}% | {pct(s['median_delta_vs_no_pct'])} | "
            f"{100*s['unfinished_rate']:.2f}% |"
        )
    lines += ["","## Guardrail","",
        "- No new timeout or percentage was introduced.",
        "- Existing -25% re-entry depth was retained.",
        "- 2020-2022 remained unopened during rule selection.","",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST"]
    (run_dir/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cutoff",default="2026-09-27T00:00:00Z")
    args=ap.parse_args()

    panel,meta=download_panel(utc(args.cutoff))
    events=build_events(panel)
    mature=utc(panel.iloc[LOOKBACK-1]["timestamp"])
    can_key=universe_key(CANONICAL_U10)

    rows=[]; cycles=[]; canonical={}
    for idx,assets in enumerate(all_u10_universes(),1):
        key=universe_key(assets); is_can=key==can_key
        base,ref=run_reference(panel,events,assets,mature,INITIAL_USDT)
        _,anchors=monthly_state(ref,INITIAL_USDT)
        for name in ("P1","P2"):
            surge,pullback,reentry=primary_key(name)
            no,_=run_overlay(ref,anchors,surge_threshold=surge,pullback_threshold=pullback,reentry_threshold=reentry,cash_fraction=CASH_FRACTION,initial_usdt=INITIAL_USDT)
            d65,_=run_overlay_timeout(ref,anchors,surge_threshold=surge,pullback_threshold=pullback,reentry_threshold=reentry,cash_fraction=CASH_FRACTION,timeout_days=TIMEOUT_DAYS,initial_usdt=INITIAL_USDT)
            rec,_=run_overlay_peak_reclaim(ref,anchors,surge_threshold=surge,pullback_threshold=pullback,reentry_threshold=reentry,cash_fraction=CASH_FRACTION,initial_usdt=INITIAL_USDT)
            trail,tc=run_overlay_trailing_reentry(ref,anchors,surge_threshold=surge,pullback_threshold=pullback,reentry_threshold=reentry,cash_fraction=CASH_FRACTION,initial_usdt=INITIAL_USDT)

            row={
                "universe_key":key,"variant":name,"is_canonical":is_can,
                "baseline_final":base["final_equity_usdt"],"baseline_dd":base["max_drawdown"],
                "no_final":no["final_equity_usdt"],"d65_final":d65["final_equity_usdt"],"reclaim_final":rec["final_equity_usdt"],
                "trail_final":trail["final_equity_usdt"],"trail_max_dd":trail["max_drawdown"],
                "trail_days_cash":trail["days_in_cash"],"trail_unfinished":trail["unfinished_cycles"],
                "trail_delta_vs_baseline_usdt":trail["final_equity_usdt"]-base["final_equity_usdt"],
                "trail_delta_vs_baseline_pct":trail["final_equity_usdt"]/base["final_equity_usdt"]-1,
                "trail_dd_improvement_pp":(trail["max_drawdown"]-base["max_drawdown"])*100,
                "trail_delta_vs_no_usdt":trail["final_equity_usdt"]-no["final_equity_usdt"],
                "trail_delta_vs_no_pct":trail["final_equity_usdt"]/no["final_equity_usdt"]-1,
                "trail_delta_vs_65_usdt":trail["final_equity_usdt"]-d65["final_equity_usdt"],
                "trail_delta_vs_65_pct":trail["final_equity_usdt"]/d65["final_equity_usdt"]-1,
                "trail_delta_vs_reclaim_usdt":trail["final_equity_usdt"]-rec["final_equity_usdt"],
                "trail_delta_vs_reclaim_pct":trail["final_equity_usdt"]/rec["final_equity_usdt"]-1,
            }
            rows.append(row)
            if is_can: canonical[name]=dict(row)
            if not tc.empty:
                t=tc.copy(); t.insert(0,"variant",name); t.insert(0,"universe_key",key); cycles.append(t)
        if idx%100==0: print(f"processed_universes={idx}")

    df=pd.DataFrame(rows)
    alt_summary={name:summarize(df[df.variant==name]) for name in ("P1","P2")}
    run_dir=OUT/pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True,exist_ok=True)
    df.to_csv(run_dir/"all_792_trailing_reentry_comparison.csv",index=False)
    if cycles: pd.concat(cycles,ignore_index=True).to_csv(run_dir/"all_trailing_reentry_cycles.csv",index=False)
    payload={"generated_at":pd.Timestamp.now(tz="UTC").isoformat(),"source_commit":source_sha(),"data_metadata":meta,
             "mature_start":mature.isoformat(),"canonical":canonical,"alternative_summary":alt_summary}
    (run_dir/"results.json").write_text(json.dumps(payload,indent=2,sort_keys=True,allow_nan=False),encoding="utf-8")
    write_report(payload,run_dir)
    print("run_dir="+str(run_dir))
    for name in ("P1","P2"):
        c=canonical[name]; s=alt_summary[name]
        print(f"{name} CAN trail={c['trail_final']:.8f} vsno={c['trail_delta_vs_no_pct']:.8f} dd={c['trail_max_dd']:.8f} days={c['trail_days_cash']} unfinished={c['trail_unfinished']}")
        print(f"{name} ALT finalbase={s['final_gt_baseline_rate']:.6f} both={s['both_rate']:.6f} beatno={s['beat_no_rate']:.6f} beat65={s['beat_65_rate']:.6f} beatrec={s['beat_reclaim_rate']:.6f} med={s['median_delta_vs_no_pct']:.6f} unfinished={s['unfinished_rate']:.6f}")
    return 0

if __name__=="__main__":
    raise SystemExit(main())
