from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd

import scripts.research_rr_ddg_acceleration_fork_v1 as base
from strategies.crypto.relative_rotation.paper_live import TARGET_ASSETS

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "rr_extreme_continuation_classifier_v2"

AS_OF = base.AS_OF
RECENT_START = base.RECENT_START
ACCEL_THRESHOLD = 0.10
OBS_DAYS = (1, 2, 3)
HORIZONS = (3, 7, 14, 30)
PRIMARY_HORIZON = 14
VARIANTS = ("PERSIST1", "PERSIST2", "PERSIST2_REEXPAND")

V1_REFERENCE_14D = {
    "candidate_beats_corrected_destination_rate": 0.465,
    "median_relative_excess": -0.0166,
    "mean_relative_excess": 0.0437,
    "strong_continuation_ge20_rate": 0.150,
}


def source_sha() -> str:
    return os.environ.get("SOURCE_COMMIT_SHA") or subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def asset_return_since_entry(
    panel: pd.DataFrame,
    asset: str,
    entry_i: int,
    obs_i: int,
) -> float:
    return (
        float(panel.iloc[obs_i][f"{asset}_close"])
        / float(panel.iloc[entry_i][f"{asset}_open"])
        - 1.0
    )


def cross_section_snapshot(
    panel: pd.DataFrame,
    entry_i: int,
    obs_i: int,
    candidate: str,
) -> dict:
    returns = {
        asset: asset_return_since_entry(panel, asset, entry_i, obs_i)
        for asset in TARGET_ASSETS
    }
    ranked = sorted(returns.items(), key=lambda kv: (-kv[1], kv[0]))
    rank_map = {asset: rank + 1 for rank, (asset, _) in enumerate(ranked)}
    candidate_ret = returns[candidate]
    others = [ret for asset, ret in ranked if asset != candidate]
    second_best_other = max(others) if others else np.nan
    breadth = sum(candidate_ret > ret for asset, ret in returns.items() if asset != candidate)
    return {
        "candidate_abs_return": float(candidate_ret),
        "candidate_rank": int(rank_map[candidate]),
        "candidate_margin_vs_second": float(candidate_ret - second_best_other),
        "candidate_beats_target_count": int(breadth),
    }


def topology_snapshot(
    states_by_date: dict[pd.Timestamp, list[dict]],
    date: pd.Timestamp,
    candidate: str,
) -> dict:
    out_count = 0
    in_count = 0
    max_out_dislocation = 0.0
    max_in_dislocation = 0.0

    for row in states_by_date.get(pd.Timestamp(date), []):
        mode = str(row.get("mode") or "NONE").upper()
        if mode not in {"HIGH", "LOW"}:
            continue
        frm = str(row.get("from_asset") or "").upper()
        to = str(row.get("to_asset") or "").upper()
        if frm not in set(TARGET_ASSETS) or to not in set(TARGET_ASSETS):
            continue
        strength = float(row.get("max_dislocation") or 0.0)
        if frm == candidate and to != candidate:
            out_count += 1
            max_out_dislocation = max(max_out_dislocation, strength)
        elif to == candidate and frm != candidate:
            in_count += 1
            max_in_dislocation = max(max_in_dislocation, strength)

    return {
        "rr_out_count": int(out_count),
        "rr_in_count": int(in_count),
        "rr_net_out_minus_in": int(out_count - in_count),
        "rr_max_out_dislocation": float(max_out_dislocation),
        "rr_max_in_dislocation": float(max_in_dislocation),
    }


def candidate_impulse_from_scores(
    scores: list[tuple[str, float]],
    candidate: str,
) -> float:
    mapping = dict(scores)
    if candidate not in mapping:
        raise RuntimeError(f"Candidate {candidate} missing from acceleration scores")
    return float(mapping[candidate])


def build_classifier_events(
    panel: pd.DataFrame,
    routes: pd.DataFrame,
    states_by_date: dict[pd.Timestamp, list[dict]],
) -> pd.DataFrame:
    date_to_i = {
        pd.Timestamp(ts): i
        for i, ts in enumerate(pd.to_datetime(panel["timestamp"], utc=True))
    }
    rows = []

    for route_id, route in routes.iterrows():
        signal_date = pd.Timestamp(route["signal_date"])
        signal_i = date_to_i[signal_date]
        entry_i = signal_i + 1
        if entry_i >= len(panel):
            continue

        held = str(route["effective_to"])
        trigger = None
        for day in OBS_DAYS:
            obs_i = entry_i + day - 1
            if obs_i >= len(panel):
                break
            candidate, impulse, scores = base.strongest_accel(
                panel, held, entry_i, obs_i
            )
            if impulse >= ACCEL_THRESHOLD:
                trigger = (day, obs_i, candidate, impulse, scores)
                break

        if trigger is None:
            continue

        trigger_day, trigger_i, candidate, trigger_impulse, trigger_scores = trigger
        confirm1_i = trigger_i + 1
        confirm2_i = trigger_i + 2

        row = {
            "route_id": int(route_id),
            "signal_date": signal_date,
            "source": str(route["source"]),
            "held": held,
            "ddg_override": bool(route["override"]),
            "trigger_day": int(trigger_day),
            "trigger_date": pd.Timestamp(panel.iloc[trigger_i]["timestamp"]),
            "candidate": candidate,
            "trigger_impulse": float(trigger_impulse),
            "confirmable_1": confirm1_i < len(panel),
            "confirmable_2": confirm2_i < len(panel),
        }

        trigger_xs = cross_section_snapshot(panel, entry_i, trigger_i, candidate)
        trigger_topology = topology_snapshot(
            states_by_date,
            pd.Timestamp(panel.iloc[trigger_i]["timestamp"]),
            candidate,
        )
        for key, value in trigger_xs.items():
            row[f"trigger_{key}"] = value
        for key, value in trigger_topology.items():
            row[f"trigger_{key}"] = value

        persist1 = False
        persist2 = False
        reexpand = False
        confirm1_impulse = np.nan
        confirm2_impulse = np.nan
        confirm1_top = ""
        confirm2_top = ""

        if confirm1_i < len(panel):
            top1, _, scores1 = base.strongest_accel(
                panel, held, entry_i, confirm1_i
            )
            confirm1_top = top1
            confirm1_impulse = candidate_impulse_from_scores(scores1, candidate)
            persist1 = top1 == candidate

        if confirm2_i < len(panel):
            top2, _, scores2 = base.strongest_accel(
                panel, held, entry_i, confirm2_i
            )
            confirm2_top = top2
            confirm2_impulse = candidate_impulse_from_scores(scores2, candidate)
            persist2 = persist1 and top2 == candidate
            reexpand = bool(confirm2_impulse > trigger_impulse)

            confirm2_xs = cross_section_snapshot(
                panel, entry_i, confirm2_i, candidate
            )
            confirm2_topology = topology_snapshot(
                states_by_date,
                pd.Timestamp(panel.iloc[confirm2_i]["timestamp"]),
                candidate,
            )
            for key, value in confirm2_xs.items():
                row[f"confirm2_{key}"] = value
            for key, value in confirm2_topology.items():
                row[f"confirm2_{key}"] = value

        row.update(
            {
                "confirm1_date": (
                    pd.Timestamp(panel.iloc[confirm1_i]["timestamp"])
                    if confirm1_i < len(panel) else pd.NaT
                ),
                "confirm1_top": confirm1_top,
                "confirm1_candidate_impulse": confirm1_impulse,
                "confirm2_date": (
                    pd.Timestamp(panel.iloc[confirm2_i]["timestamp"])
                    if confirm2_i < len(panel) else pd.NaT
                ),
                "confirm2_top": confirm2_top,
                "confirm2_candidate_impulse": confirm2_impulse,
                "persist1_pass": bool(persist1),
                "persist2_pass": bool(persist2),
                "reexpand_pass": bool(reexpand),
                "primary_pass": bool(persist2 and reexpand),
            }
        )
        rows.append(row)

    return pd.DataFrame(rows).sort_values(
        ["signal_date", "source", "held"]
    ).reset_index(drop=True)


def build_variant_results(
    panel: pd.DataFrame,
    events: pd.DataFrame,
) -> pd.DataFrame:
    date_to_i = {
        pd.Timestamp(ts): i
        for i, ts in enumerate(pd.to_datetime(panel["timestamp"], utc=True))
    }
    rows = []

    for event_id, event in events.iterrows():
        for variant in VARIANTS:
            if variant == "PERSIST1":
                passed = bool(event["persist1_pass"])
                confirm_date = event["confirm1_date"]
            elif variant == "PERSIST2":
                passed = bool(event["persist2_pass"])
                confirm_date = event["confirm2_date"]
            else:
                passed = bool(event["primary_pass"])
                confirm_date = event["confirm2_date"]

            base_row = {
                "event_id": int(event_id),
                "variant": variant,
                "signal_date": event["signal_date"],
                "source": event["source"],
                "held": event["held"],
                "candidate": event["candidate"],
                "trigger_date": event["trigger_date"],
                "passed": passed,
            }
            if not passed or pd.isna(confirm_date):
                rows.append(base_row)
                continue

            confirm_date = pd.Timestamp(confirm_date)
            confirm_i = date_to_i[confirm_date]
            switch_i = confirm_i + 1
            row = {
                **base_row,
                "confirmation_date": confirm_date,
                "switch_date": (
                    pd.Timestamp(panel.iloc[switch_i]["timestamp"])
                    if switch_i < len(panel) else pd.NaT
                ),
            }

            for h in HORIZONS:
                c = base.fwd_open_ret(panel, str(event["candidate"]), switch_i, h)
                b = base.fwd_open_ret(panel, str(event["held"]), switch_i, h)
                row[f"candidate_fwd_{h}d"] = c
                row[f"held_fwd_{h}d"] = b
                row[f"relative_excess_{h}d"] = base.rel_excess(c, b)

            if switch_i < len(panel):
                c_latest = base.latest_ret(panel, str(event["candidate"]), switch_i)
                b_latest = base.latest_ret(panel, str(event["held"]), switch_i)
                row["relative_excess_to_latest_close"] = base.rel_excess(
                    c_latest, b_latest
                )
            rows.append(row)

    return pd.DataFrame(rows)


def summarize_variants(
    events: pd.DataFrame,
    results: pd.DataFrame,
) -> pd.DataFrame:
    rows = []
    base_trigger_count = int(len(events))

    for variant in VARIANTS:
        sub = results[results["variant"] == variant].copy()
        passed = sub[sub["passed"]].copy()

        for h in HORIZONS:
            col = f"relative_excess_{h}d"
            vals = (
                passed[col].dropna().astype(float)
                if col in passed.columns
                else pd.Series(dtype=float)
            )
            rows.append(
                {
                    "variant": variant,
                    "horizon_days": h,
                    "base_trigger_count": base_trigger_count,
                    "pass_count": int(len(passed)),
                    "pass_rate_vs_base_triggers": (
                        float(len(passed) / base_trigger_count)
                        if base_trigger_count else np.nan
                    ),
                    "forward_n": int(len(vals)),
                    "candidate_beats_held_rate": (
                        float((vals > 0).mean()) if len(vals) else np.nan
                    ),
                    "median_relative_excess": (
                        float(vals.median()) if len(vals) else np.nan
                    ),
                    "mean_relative_excess": (
                        float(vals.mean()) if len(vals) else np.nan
                    ),
                    "p25_relative_excess": (
                        float(vals.quantile(0.25)) if len(vals) else np.nan
                    ),
                    "p75_relative_excess": (
                        float(vals.quantile(0.75)) if len(vals) else np.nan
                    ),
                    "false_positive_rate": (
                        float((vals <= 0).mean()) if len(vals) else np.nan
                    ),
                    "strong_continuation_ge20_rate": (
                        float((vals >= 0.20).mean()) if len(vals) else np.nan
                    ),
                }
            )

    return pd.DataFrame(rows)


def build_primary_diagnostic_summary(
    events: pd.DataFrame,
    results: pd.DataFrame,
) -> pd.DataFrame:
    primary = results[
        (results["variant"] == "PERSIST2_REEXPAND")
        & (results["passed"])
    ].copy()
    if primary.empty or "relative_excess_14d" not in primary.columns:
        return pd.DataFrame()

    merged = primary.merge(
        events.reset_index(names="event_id"),
        on=[
            "event_id", "signal_date", "source", "held",
            "candidate", "trigger_date"
        ],
        how="left",
        validate="one_to_one",
    )
    merged = merged[merged["relative_excess_14d"].notna()].copy()
    if merged.empty:
        return pd.DataFrame()

    merged["outcome_group"] = np.where(
        merged["relative_excess_14d"] >= 0.20,
        "STRONG_GE20",
        "NON_STRONG",
    )

    features = [
        "trigger_impulse",
        "confirm2_candidate_impulse",
        "trigger_candidate_margin_vs_second",
        "confirm2_candidate_margin_vs_second",
        "trigger_candidate_beats_target_count",
        "confirm2_candidate_beats_target_count",
        "trigger_rr_out_count",
        "trigger_rr_in_count",
        "trigger_rr_net_out_minus_in",
        "confirm2_rr_out_count",
        "confirm2_rr_in_count",
        "confirm2_rr_net_out_minus_in",
    ]

    rows = []
    for group, sub in merged.groupby("outcome_group", sort=True):
        row = {
            "outcome_group": group,
            "n": int(len(sub)),
            "median_14d_relative_excess": float(
                sub["relative_excess_14d"].median()
            ),
        }
        for feature in features:
            row[f"median_{feature}"] = float(sub[feature].median())
        rows.append(row)
    return pd.DataFrame(rows)


def build_aave_case(
    panel: pd.DataFrame,
    routes: pd.DataFrame,
    events: pd.DataFrame,
    results: pd.DataFrame,
) -> dict:
    case_route = routes[
        (routes["signal_date"] == base.CASE_DATE)
        & (routes["source"] == "LINK")
    ]
    if case_route.empty:
        raise RuntimeError("Missing corrected LINK route")
    route = case_route.iloc[0]

    case_events = events[
        (events["signal_date"] == base.CASE_DATE)
        & (events["source"] == "LINK")
        & (events["held"] == "TRX")
        & (events["candidate"] == "AAVE")
    ]
    if case_events.empty:
        raise RuntimeError("AAVE base trigger missing for corrected LINK -> TRX")
    event = case_events.iloc[0]

    primary = results[
        (results["variant"] == "PERSIST2_REEXPAND")
        & (results["signal_date"] == base.CASE_DATE)
        & (results["source"] == "LINK")
        & (results["candidate"] == "AAVE")
    ]
    if primary.empty:
        raise RuntimeError("AAVE primary result missing")
    primary = primary.iloc[0]

    return {
        "signal_date": base.CASE_DATE.isoformat(),
        "old_baseline_to": str(route["baseline_to"]),
        "corrected_effective_to": str(route["effective_to"]),
        "base_trigger_date": pd.Timestamp(event["trigger_date"]).isoformat(),
        "trigger_impulse": float(event["trigger_impulse"]),
        "confirm1_date": pd.Timestamp(event["confirm1_date"]).isoformat(),
        "confirm1_candidate_impulse": float(event["confirm1_candidate_impulse"]),
        "confirm1_top": str(event["confirm1_top"]),
        "confirm2_date": pd.Timestamp(event["confirm2_date"]).isoformat(),
        "confirm2_candidate_impulse": float(event["confirm2_candidate_impulse"]),
        "confirm2_top": str(event["confirm2_top"]),
        "persist1_pass": bool(event["persist1_pass"]),
        "persist2_pass": bool(event["persist2_pass"]),
        "reexpand_pass": bool(event["reexpand_pass"]),
        "primary_pass": bool(event["primary_pass"]),
        "hypothetical_switch_date": (
            None if pd.isna(primary.get("switch_date"))
            else pd.Timestamp(primary["switch_date"]).isoformat()
        ),
        "relative_excess_to_latest_close": (
            None
            if pd.isna(primary.get("relative_excess_to_latest_close"))
            else float(primary["relative_excess_to_latest_close"])
        ),
        "trigger_rr_out_count": int(event["trigger_rr_out_count"]),
        "trigger_rr_in_count": int(event["trigger_rr_in_count"]),
        "confirm2_rr_out_count": int(event["confirm2_rr_out_count"]),
        "confirm2_rr_in_count": int(event["confirm2_rr_in_count"]),
    }


def classify(primary14: pd.Series) -> str:
    win = primary14.get("candidate_beats_held_rate")
    med = primary14.get("median_relative_excess")
    strong = primary14.get("strong_continuation_ge20_rate")
    n = primary14.get("forward_n")

    if pd.isna(win) or pd.isna(med) or not n:
        return "CLASSIFIER_NOT_SUPPORTED"
    if win > 0.50 and med > 0 and strong >= V1_REFERENCE_14D["strong_continuation_ge20_rate"]:
        return "CLASSIFIER_PROMISING_RESEARCH_SIGNAL"
    if (
        win > V1_REFERENCE_14D["candidate_beats_corrected_destination_rate"]
        or med > V1_REFERENCE_14D["median_relative_excess"]
    ):
        return "CLASSIFIER_IMPROVES_PRECISION_BUT_NOT_PRODUCTION_READY"
    return "CLASSIFIER_NOT_SUPPORTED"


def fmt_pct(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{100*float(v):+.2f}%"


def fmt_rate(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{100*float(v):.1f}%"


def main() -> None:
    panel, data_meta = base.download_panel()
    events_by_date, states_by_date = base.build_monitor_history(panel)
    routes = base.build_effective_routes(panel, events_by_date, states_by_date)

    classifier_events = build_classifier_events(panel, routes, states_by_date)
    variant_results = build_variant_results(panel, classifier_events)
    summary = summarize_variants(classifier_events, variant_results)
    diagnostics = build_primary_diagnostic_summary(
        classifier_events, variant_results
    )
    aave_case = build_aave_case(
        panel, routes, classifier_events, variant_results
    )

    primary14 = summary[
        (summary["variant"] == "PERSIST2_REEXPAND")
        & (summary["horizon_days"] == PRIMARY_HORIZON)
    ].iloc[0]
    classification = classify(primary14)

    recent_events = classifier_events[
        classifier_events["signal_date"] >= RECENT_START
    ].copy()
    recent_results = variant_results[
        variant_results["signal_date"] >= RECENT_START
    ].copy()
    recent_summary = summarize_variants(recent_events, recent_results)

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    classifier_events.to_csv(
        run_dir / "base_trigger_classifier_events.csv", index=False
    )
    variant_results.to_csv(
        run_dir / "variant_results.csv", index=False
    )
    summary.to_csv(run_dir / "variant_summary.csv", index=False)
    diagnostics.to_csv(
        run_dir / "primary_outcome_diagnostics.csv", index=False
    )
    recent_events.to_csv(
        run_dir / "recent_month_classifier_events.csv", index=False
    )
    recent_summary.to_csv(
        run_dir / "recent_month_variant_summary.csv", index=False
    )
    (run_dir / "aave_case.json").write_text(
        json.dumps(aave_case, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    summary_json = {
        "experiment": "RR_EXTREME_CONTINUATION_CLASSIFIER_V2",
        "workflow_mode": "STRESS_TEST_ONLY",
        "source_commit_sha": source_sha(),
        "as_of": AS_OF.isoformat(),
        "latest_closed_candle": pd.Timestamp(panel.iloc[-1]["timestamp"]).isoformat(),
        "base_trigger_count": int(len(classifier_events)),
        "classification": classification,
        "v1_reference_14d": V1_REFERENCE_14D,
        "primary_14d": {
            key: (
                None if pd.isna(value)
                else int(value) if key in {"base_trigger_count", "pass_count", "forward_n", "horizon_days"}
                else float(value)
            )
            for key, value in primary14.to_dict().items()
            if key != "variant"
        },
        "aave_case": aave_case,
        "production_changes": "NONE",
        "test_level": "GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_STRESS_TEST",
        "data_metadata": data_meta,
    }
    (run_dir / "summary.json").write_text(
        json.dumps(summary_json, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    lines = [
        "# RR EXTREME CONTINUATION CLASSIFIER V2 — RESULTS",
        "",
        "Mode: STRESS_TEST_ONLY",
        "",
        f"Classification: {classification}",
        f"As-of: {AS_OF.isoformat()}",
        f"Latest closed D1: {pd.Timestamp(panel.iloc[-1]['timestamp']).isoformat()}",
        f"Base +10% triggers: {len(classifier_events)}",
        "",
        "## Variant results",
        "",
        "|Variant|Horizon|Pass count|Pass rate|Forward N|Candidate beats held|Median excess|Mean excess|>=20% continuation|",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in summary.iterrows():
        lines.append(
            f"|{row['variant']}|{int(row['horizon_days'])}d|"
            f"{int(row['pass_count'])}|{fmt_rate(row['pass_rate_vs_base_triggers'])}|"
            f"{int(row['forward_n'])}|{fmt_rate(row['candidate_beats_held_rate'])}|"
            f"{fmt_pct(row['median_relative_excess'])}|"
            f"{fmt_pct(row['mean_relative_excess'])}|"
            f"{fmt_rate(row['strong_continuation_ge20_rate'])}|"
        )

    lines += [
        "",
        "## V1 reference at 14d",
        "",
        f"- naive +10% acceleration win rate: {100*V1_REFERENCE_14D['candidate_beats_corrected_destination_rate']:.1f}%",
        f"- naive +10% acceleration median excess: {fmt_pct(V1_REFERENCE_14D['median_relative_excess'])}",
        f"- naive +10% acceleration mean excess: {fmt_pct(V1_REFERENCE_14D['mean_relative_excess'])}",
        f"- naive +10% acceleration >=20% continuation: {100*V1_REFERENCE_14D['strong_continuation_ge20_rate']:.1f}%",
        "",
        "## Corrected LINK -> TRX / AAVE case",
        "",
        f"- base trigger: {aave_case['base_trigger_date']} at {fmt_pct(aave_case['trigger_impulse'])}",
        f"- next close: {aave_case['confirm1_date']}, top={aave_case['confirm1_top']}, AAVE impulse={fmt_pct(aave_case['confirm1_candidate_impulse'])}",
        f"- second close: {aave_case['confirm2_date']}, top={aave_case['confirm2_top']}, AAVE impulse={fmt_pct(aave_case['confirm2_candidate_impulse'])}",
        f"- PERSIST1: {aave_case['persist1_pass']}",
        f"- PERSIST2: {aave_case['persist2_pass']}",
        f"- REEXPAND: {aave_case['reexpand_pass']}",
        f"- PRIMARY PASS: {aave_case['primary_pass']}",
        f"- hypothetical switch: {aave_case['hypothetical_switch_date']}",
        f"- relative excess vs TRX to latest close: {fmt_pct(aave_case['relative_excess_to_latest_close'])}",
        "",
        "## Primary structural diagnostics",
        "",
    ]

    if diagnostics.empty:
        lines.append("- No primary-pass events with complete 14d outcomes.")
    else:
        cols = [
            "outcome_group", "n", "median_14d_relative_excess",
            "median_trigger_impulse", "median_confirm2_candidate_impulse",
            "median_trigger_candidate_margin_vs_second",
            "median_confirm2_candidate_margin_vs_second",
            "median_trigger_rr_net_out_minus_in",
            "median_confirm2_rr_net_out_minus_in",
        ]
        lines += [
            "|Outcome|N|14d median excess|Trigger impulse|Confirm2 impulse|Trigger margin vs #2|Confirm2 margin vs #2|Trigger RR net out-in|Confirm2 RR net out-in|",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
        for _, row in diagnostics[cols].iterrows():
            lines.append(
                f"|{row['outcome_group']}|{int(row['n'])}|"
                f"{fmt_pct(row['median_14d_relative_excess'])}|"
                f"{fmt_pct(row['median_trigger_impulse'])}|"
                f"{fmt_pct(row['median_confirm2_candidate_impulse'])}|"
                f"{fmt_pct(row['median_trigger_candidate_margin_vs_second'])}|"
                f"{fmt_pct(row['median_confirm2_candidate_margin_vs_second'])}|"
                f"{row['median_trigger_rr_net_out_minus_in']:.1f}|"
                f"{row['median_confirm2_rr_net_out_minus_in']:.1f}|"
            )

    lines += [
        "",
        "## Recent-month primary",
        "",
    ]
    recent_primary = recent_summary[
        (recent_summary["variant"] == "PERSIST2_REEXPAND")
        & (recent_summary["horizon_days"] == PRIMARY_HORIZON)
    ]
    if recent_primary.empty:
        lines.append("- No recent-month primary row.")
    else:
        row = recent_primary.iloc[0]
        lines += [
            f"- base triggers: {int(row['base_trigger_count'])}",
            f"- primary passes: {int(row['pass_count'])}",
            f"- pass rate: {fmt_rate(row['pass_rate_vs_base_triggers'])}",
            f"- complete 14d outcomes: {int(row['forward_n'])}",
            f"- win rate: {fmt_rate(row['candidate_beats_held_rate'])}",
            f"- median excess: {fmt_pct(row['median_relative_excess'])}",
        ]

    lines += [
        "",
        "## Boundary",
        "",
        "- The structural diagnostics were not used to retune the primary classifier.",
        "- No production/live/Telegram/exchange behavior changed.",
        "- Any promotion still requires path-dependent simulation, costs, and unseen forward evidence.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_STRESS_TEST",
        "",
    ]

    report = "\n".join(lines)
    (run_dir / "report.md").write_text(report, encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()
