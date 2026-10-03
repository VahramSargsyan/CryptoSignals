from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import (
    ARM_THRESHOLD,
    ASSETS,
    LOOKBACK,
    REVERSAL,
    TARGET_ASSETS,
    build_pair_monitor,
)

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "rr_post_signal_acceleration_v1"

AS_OF = pd.Timestamp("2026-10-03T16:30:00Z")
DOWNLOAD_START = pd.Timestamp("2023-05-05T00:00:00Z")
CASE_SIGNAL_DATE = pd.Timestamp("2026-09-28T00:00:00Z")
CASE_SOURCE = "LINK"
CASE_BASELINE = "ALGO"

THRESHOLDS = (0.05, 0.10, 0.15)
PRIMARY_THRESHOLD = 0.10
OBSERVATION_DAYS = (1, 2, 3)
FORWARD_HORIZONS = (3, 7, 14, 30)


def source_sha() -> str:
    return os.environ.get("SOURCE_COMMIT_SHA") or subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def download_panel() -> tuple[pd.DataFrame, dict]:
    client = BinanceSpotRestClient()
    pieces = []
    metadata: dict[str, dict] = {}

    for asset in ASSETS:
        result = download_historical_dataset(
            client,
            symbol=f"{asset}USDT",
            start=DOWNLOAD_START,
            end=AS_OF,
            timeframe="1D",
            as_of=AS_OF,
        )
        if result.dataset is None:
            raise RuntimeError(f"{asset}: no closed D1 dataset ({result.metadata.status})")
        if result.dataset.quality.has_critical_issues:
            raise RuntimeError(f"{asset}: critical D1 data quality: {result.dataset.quality}")

        frame = result.dataset.candles[["timestamp", "open", "close"]].copy()
        frame = frame.rename(
            columns={
                "open": f"{asset}_open",
                "close": f"{asset}_close",
            }
        )
        pieces.append(frame)
        metadata[asset] = {
            "symbol": f"{asset}USDT",
            "dataset_id": result.dataset.dataset_id,
            "rows": int(len(frame)),
            "actual_start": result.metadata.actual_start,
            "actual_end": result.metadata.actual_end,
            "status": result.metadata.status,
        }

    panel = pieces[0]
    for frame in pieces[1:]:
        panel = panel.merge(frame, on="timestamp", how="inner", validate="one_to_one")
    panel = panel.sort_values("timestamp", kind="stable").reset_index(drop=True)

    metadata["panel"] = {
        "rows": int(len(panel)),
        "start": pd.Timestamp(panel.iloc[0]["timestamp"]).isoformat(),
        "end": pd.Timestamp(panel.iloc[-1]["timestamp"]).isoformat(),
    }
    return panel, metadata


def primary_confirmed_routes(events: list[dict]) -> pd.DataFrame:
    rows = [
        dict(event)
        for event in events
        if str(event.get("event") or "").upper() == "CONFIRMED"
        and str(event.get("to_asset") or "").upper() in set(TARGET_ASSETS)
        and str(event.get("from_asset") or "").upper() in set(ASSETS)
    ]
    if not rows:
        raise RuntimeError("No CONFIRMED routes found")

    frame = pd.DataFrame(rows)
    frame["date"] = pd.to_datetime(frame["date"], utc=True)
    frame["from_asset"] = frame["from_asset"].astype(str).str.upper()
    frame["to_asset"] = frame["to_asset"].astype(str).str.upper()
    frame["max_dislocation"] = frame["max_dislocation"].astype(float)

    frame = frame.sort_values(
        ["date", "from_asset", "max_dislocation", "to_asset", "pair"],
        ascending=[True, True, False, True, True],
        kind="stable",
    )
    frame = frame.groupby(["date", "from_asset"], as_index=False, sort=True).first()
    return frame.sort_values(["date", "from_asset"]).reset_index(drop=True)


def relative_impulse(
    panel: pd.DataFrame,
    candidate: str,
    baseline: str,
    entry_i: int,
    detect_i: int,
) -> float:
    c_open = float(panel.iloc[entry_i][f"{candidate}_open"])
    b_open = float(panel.iloc[entry_i][f"{baseline}_open"])
    c_close = float(panel.iloc[detect_i][f"{candidate}_close"])
    b_close = float(panel.iloc[detect_i][f"{baseline}_close"])
    c_ret = c_close / c_open
    b_ret = b_close / b_open
    return c_ret / b_ret - 1.0


def forward_open_return(
    panel: pd.DataFrame,
    asset: str,
    start_i: int,
    horizon: int,
) -> float:
    end_i = start_i + horizon
    if start_i >= len(panel) or end_i >= len(panel):
        return np.nan
    start = float(panel.iloc[start_i][f"{asset}_open"])
    end = float(panel.iloc[end_i][f"{asset}_open"])
    return end / start - 1.0


def latest_close_return(
    panel: pd.DataFrame,
    asset: str,
    start_i: int,
) -> float:
    if start_i >= len(panel):
        return np.nan
    start = float(panel.iloc[start_i][f"{asset}_open"])
    end = float(panel.iloc[-1][f"{asset}_close"])
    return end / start - 1.0


def relative_excess(candidate_return: float, baseline_return: float) -> float:
    if pd.isna(candidate_return) or pd.isna(baseline_return):
        return np.nan
    return (1.0 + float(candidate_return)) / (1.0 + float(baseline_return)) - 1.0


def strongest_candidate(
    panel: pd.DataFrame,
    source: str,
    baseline: str,
    entry_i: int,
    detect_i: int,
) -> tuple[str, float, list[tuple[str, float]]]:
    candidates = [
        asset for asset in TARGET_ASSETS
        if asset not in {source, baseline}
    ]
    scores = [
        (asset, relative_impulse(panel, asset, baseline, entry_i, detect_i))
        for asset in candidates
    ]
    scores.sort(key=lambda x: (-float(x[1]), x[0]))
    if not scores:
        raise RuntimeError("No acceleration candidates")
    return scores[0][0], float(scores[0][1]), scores


def build_trigger_ledger(
    panel: pd.DataFrame,
    primary: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    date_to_i = {
        pd.Timestamp(ts): i
        for i, ts in enumerate(pd.to_datetime(panel["timestamp"], utc=True))
    }
    event_rows = []
    trigger_rows = []

    for event_id, (_, event) in enumerate(primary.iterrows(), start=1):
        signal_date = pd.Timestamp(event["date"])
        if signal_date not in date_to_i:
            continue
        signal_i = date_to_i[signal_date]
        entry_i = signal_i + 1
        last_detect_i = entry_i + max(OBSERVATION_DAYS) - 1
        if last_detect_i >= len(panel):
            continue

        source = str(event["from_asset"]).upper()
        baseline = str(event["to_asset"]).upper()

        snapshots = {}
        for day in OBSERVATION_DAYS:
            detect_i = entry_i + day - 1
            top_asset, top_impulse, scores = strongest_candidate(
                panel, source, baseline, entry_i, detect_i
            )
            snapshots[day] = {
                "detect_i": detect_i,
                "top_asset": top_asset,
                "top_impulse": top_impulse,
                "scores": scores,
            }

        event_rows.append(
            {
                "event_id": event_id,
                "signal_date": signal_date,
                "entry_date": pd.Timestamp(panel.iloc[entry_i]["timestamp"]),
                "source": source,
                "baseline": baseline,
                "pair": event["pair"],
                "baseline_max_dislocation": float(event["max_dislocation"]),
                "baseline_reversal": float(event["reversal_from_extreme"]),
                "day1_top_asset": snapshots[1]["top_asset"],
                "day1_top_impulse": snapshots[1]["top_impulse"],
                "day2_top_asset": snapshots[2]["top_asset"],
                "day2_top_impulse": snapshots[2]["top_impulse"],
                "day3_top_asset": snapshots[3]["top_asset"],
                "day3_top_impulse": snapshots[3]["top_impulse"],
            }
        )

        for threshold in THRESHOLDS:
            trigger = None
            for day in OBSERVATION_DAYS:
                snap = snapshots[day]
                if snap["top_impulse"] >= threshold:
                    trigger = (day, snap)
                    break

            if trigger is None:
                trigger_rows.append(
                    {
                        "event_id": event_id,
                        "threshold": threshold,
                        "triggered": False,
                        "signal_date": signal_date,
                        "entry_date": pd.Timestamp(panel.iloc[entry_i]["timestamp"]),
                        "source": source,
                        "baseline": baseline,
                        "candidate": "",
                        "detection_day": np.nan,
                        "detection_date": pd.NaT,
                        "switch_date": pd.NaT,
                        "trigger_impulse": np.nan,
                    }
                )
                continue

            day, snap = trigger
            detect_i = int(snap["detect_i"])
            switch_i = detect_i + 1
            candidate = str(snap["top_asset"])

            row = {
                "event_id": event_id,
                "threshold": threshold,
                "triggered": True,
                "signal_date": signal_date,
                "entry_date": pd.Timestamp(panel.iloc[entry_i]["timestamp"]),
                "source": source,
                "baseline": baseline,
                "candidate": candidate,
                "detection_day": int(day),
                "detection_date": pd.Timestamp(panel.iloc[detect_i]["timestamp"]),
                "switch_date": (
                    pd.Timestamp(panel.iloc[switch_i]["timestamp"])
                    if switch_i < len(panel) else pd.NaT
                ),
                "trigger_impulse": float(snap["top_impulse"]),
            }

            for h in FORWARD_HORIZONS:
                c = forward_open_return(panel, candidate, switch_i, h)
                b = forward_open_return(panel, baseline, switch_i, h)
                row[f"candidate_fwd_{h}d"] = c
                row[f"baseline_fwd_{h}d"] = b
                row[f"relative_excess_{h}d"] = relative_excess(c, b)

            trigger_rows.append(row)

    return pd.DataFrame(event_rows), pd.DataFrame(trigger_rows)


def summarize(
    event_ledger: pd.DataFrame,
    trigger_ledger: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    opportunity_count = int(len(event_ledger))
    rows = []
    day_rows = []

    for threshold in THRESHOLDS:
        sub = trigger_ledger[trigger_ledger["threshold"] == threshold].copy()
        trig = sub[sub["triggered"]].copy()
        trigger_count = int(len(trig))

        for day in OBSERVATION_DAYS:
            day_rows.append(
                {
                    "threshold": threshold,
                    "detection_day": day,
                    "trigger_count": int((trig["detection_day"] == day).sum()),
                    "share_of_triggers": (
                        float((trig["detection_day"] == day).mean())
                        if trigger_count else np.nan
                    ),
                }
            )

        for h in FORWARD_HORIZONS:
            vals = trig[f"relative_excess_{h}d"].dropna().astype(float)
            abs_vals = trig[f"candidate_fwd_{h}d"].dropna().astype(float)
            rows.append(
                {
                    "threshold": threshold,
                    "eligible_opportunities": opportunity_count,
                    "trigger_count": trigger_count,
                    "trigger_rate": trigger_count / opportunity_count if opportunity_count else np.nan,
                    "horizon_days": h,
                    "triggers_with_forward_data": int(len(vals)),
                    "candidate_beats_baseline_rate": (
                        float((vals > 0).mean()) if len(vals) else np.nan
                    ),
                    "median_relative_excess": float(vals.median()) if len(vals) else np.nan,
                    "mean_relative_excess": float(vals.mean()) if len(vals) else np.nan,
                    "p25_relative_excess": float(vals.quantile(0.25)) if len(vals) else np.nan,
                    "p75_relative_excess": float(vals.quantile(0.75)) if len(vals) else np.nan,
                    "false_positive_rate": (
                        float((vals <= 0).mean()) if len(vals) else np.nan
                    ),
                    "strong_continuation_rate_ge_20pct": (
                        float((vals >= 0.20).mean()) if len(vals) else np.nan
                    ),
                    "median_candidate_absolute_return": (
                        float(abs_vals.median()) if len(abs_vals) else np.nan
                    ),
                }
            )

    return pd.DataFrame(rows), pd.DataFrame(day_rows)


def build_case_audit(
    panel: pd.DataFrame,
    primary: pd.DataFrame,
) -> tuple[pd.DataFrame, dict]:
    dates = pd.to_datetime(panel["timestamp"], utc=True)
    matches = np.flatnonzero(dates.eq(CASE_SIGNAL_DATE).to_numpy())
    if not len(matches):
        raise RuntimeError("Case signal date missing")
    signal_i = int(matches[0])

    match = primary[
        (primary["date"] == CASE_SIGNAL_DATE)
        & (primary["from_asset"] == CASE_SOURCE)
    ].copy()
    if match.empty:
        raise RuntimeError("Canonical LINK primary route missing on case date")
    baseline_event = match.sort_values(
        ["max_dislocation", "to_asset"], ascending=[False, True]
    ).iloc[0]
    baseline = str(baseline_event["to_asset"]).upper()
    if baseline != CASE_BASELINE:
        raise RuntimeError(f"Expected LINK -> ALGO, got LINK -> {baseline}")

    entry_i = signal_i + 1
    rows = []
    aave_crossings = {str(int(t * 100)): None for t in THRESHOLDS}

    for day in OBSERVATION_DAYS:
        detect_i = entry_i + day - 1
        top_asset, top_impulse, scores = strongest_candidate(
            panel, CASE_SOURCE, baseline, entry_i, detect_i
        )
        score_map = dict(scores)
        aave_impulse = float(score_map["AAVE"])

        for threshold in THRESHOLDS:
            key = str(int(threshold * 100))
            if aave_crossings[key] is None and aave_impulse >= threshold:
                switch_i = detect_i + 1
                aave_crossings[key] = {
                    "detection_day": day,
                    "detection_date": pd.Timestamp(panel.iloc[detect_i]["timestamp"]).isoformat(),
                    "switch_date": (
                        pd.Timestamp(panel.iloc[switch_i]["timestamp"]).isoformat()
                        if switch_i < len(panel) else None
                    ),
                    "aave_impulse": aave_impulse,
                    "top_candidate": top_asset,
                    "top_impulse": top_impulse,
                    "aave_was_top_candidate": top_asset == "AAVE",
                    "aave_remaining_return_to_latest_close": (
                        latest_close_return(panel, "AAVE", switch_i)
                        if switch_i < len(panel) else np.nan
                    ),
                    "algo_remaining_return_to_latest_close": (
                        latest_close_return(panel, "ALGO", switch_i)
                        if switch_i < len(panel) else np.nan
                    ),
                }

        rows.append(
            {
                "observation_day": day,
                "observation_date": pd.Timestamp(panel.iloc[detect_i]["timestamp"]),
                "top_candidate": top_asset,
                "top_relative_impulse": top_impulse,
                "aave_relative_impulse_vs_algo": aave_impulse,
                "aave_rank_by_impulse": (
                    1 + [asset for asset, _ in scores].index("AAVE")
                ),
                "aave_crossed_5pct": aave_impulse >= 0.05,
                "aave_crossed_10pct": aave_impulse >= 0.10,
                "aave_crossed_15pct": aave_impulse >= 0.15,
            }
        )

    for value in aave_crossings.values():
        if value is not None:
            c = value["aave_remaining_return_to_latest_close"]
            b = value["algo_remaining_return_to_latest_close"]
            value["aave_relative_excess_to_latest_close"] = relative_excess(c, b)

    info = {
        "signal_date": CASE_SIGNAL_DATE.isoformat(),
        "source": CASE_SOURCE,
        "baseline": baseline,
        "baseline_max_dislocation": float(baseline_event["max_dislocation"]),
        "baseline_reversal": float(baseline_event["reversal_from_extreme"]),
        "entry_date": pd.Timestamp(panel.iloc[entry_i]["timestamp"]).isoformat(),
        "latest_closed_candle": pd.Timestamp(panel.iloc[-1]["timestamp"]).isoformat(),
        "aave_threshold_crossings": aave_crossings,
    }
    return pd.DataFrame(rows), info


def fmt_pct(value) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{100.0 * float(value):+.2f}%"


def main() -> None:
    panel, data_meta = download_panel()
    monitor_cols = ["timestamp"] + [f"{a}_close" for a in ASSETS]
    events, _ = build_pair_monitor(
        panel[monitor_cols],
        assets=ASSETS,
        lookback=LOOKBACK,
        arm_threshold=ARM_THRESHOLD,
        reversal=REVERSAL,
    )
    primary = primary_confirmed_routes(events)

    event_ledger, trigger_ledger = build_trigger_ledger(panel, primary)
    aggregate, day_distribution = summarize(event_ledger, trigger_ledger)
    case_table, case_info = build_case_audit(panel, primary)

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    event_ledger.to_csv(run_dir / "eligible_rr_opportunities.csv", index=False)
    trigger_ledger.to_csv(run_dir / "trigger_ledger.csv", index=False)
    aggregate.to_csv(run_dir / "aggregate_summary.csv", index=False)
    day_distribution.to_csv(run_dir / "detection_day_distribution.csv", index=False)
    case_table.to_csv(run_dir / "link_algo_aave_case.csv", index=False)
    (run_dir / "link_algo_aave_case.json").write_text(
        json.dumps(case_info, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )

    primary14 = aggregate[
        (aggregate["threshold"] == PRIMARY_THRESHOLD)
        & (aggregate["horizon_days"] == 14)
    ].iloc[0]

    summary = {
        "experiment": "RR_POST_SIGNAL_ACCELERATION_V1",
        "workflow_mode": "STRESS_TEST_ONLY",
        "source_commit_sha": source_sha(),
        "as_of": AS_OF.isoformat(),
        "latest_closed_candle": pd.Timestamp(panel.iloc[-1]["timestamp"]).isoformat(),
        "eligible_rr_opportunities": int(len(event_ledger)),
        "primary_threshold": PRIMARY_THRESHOLD,
        "primary_14d": {
            "trigger_count": int(primary14["trigger_count"]),
            "trigger_rate": float(primary14["trigger_rate"]),
            "triggers_with_forward_data": int(primary14["triggers_with_forward_data"]),
            "candidate_beats_baseline_rate": (
                None if pd.isna(primary14["candidate_beats_baseline_rate"])
                else float(primary14["candidate_beats_baseline_rate"])
            ),
            "median_relative_excess": (
                None if pd.isna(primary14["median_relative_excess"])
                else float(primary14["median_relative_excess"])
            ),
            "mean_relative_excess": (
                None if pd.isna(primary14["mean_relative_excess"])
                else float(primary14["mean_relative_excess"])
            ),
            "false_positive_rate": (
                None if pd.isna(primary14["false_positive_rate"])
                else float(primary14["false_positive_rate"])
            ),
            "strong_continuation_rate_ge_20pct": (
                None if pd.isna(primary14["strong_continuation_rate_ge_20pct"])
                else float(primary14["strong_continuation_rate_ge_20pct"])
            ),
        },
        "current_case": case_info,
        "production_changes": "NONE",
        "test_level": "GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_STRESS_TEST",
        "data_metadata": data_meta,
    }
    (run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )

    lines = [
        "# RR POST-SIGNAL ACCELERATION V1 — RESULTS",
        "",
        "Mode: STRESS_TEST_ONLY",
        "",
        f"As-of: {AS_OF.isoformat()}",
        f"Latest closed D1 candle: {pd.Timestamp(panel.iloc[-1]['timestamp']).isoformat()}",
        f"Eligible RR opportunities: {len(event_ledger)}",
        "",
        "## Trigger results",
        "",
        "|Trigger|Horizon|Triggers|Trigger rate|Forward N|Candidate beats baseline|Median relative excess|Mean relative excess|Strong continuation >=20%|",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in aggregate.iterrows():
        lines.append(
            f"|{int(round(100*row['threshold']))}%|{int(row['horizon_days'])}d|"
            f"{int(row['trigger_count'])}|{100*row['trigger_rate']:.1f}%|"
            f"{int(row['triggers_with_forward_data'])}|"
            f"{'—' if pd.isna(row['candidate_beats_baseline_rate']) else f'{100*row['candidate_beats_baseline_rate']:.1f}%'}|"
            f"{fmt_pct(row['median_relative_excess'])}|"
            f"{fmt_pct(row['mean_relative_excess'])}|"
            f"{'—' if pd.isna(row['strong_continuation_rate_ge_20pct']) else f'{100*row['strong_continuation_rate_ge_20pct']:.1f}%'}|"
        )

    lines += [
        "",
        "## Detection-day distribution",
        "",
        "|Trigger|Day|Count|Share of triggers|",
        "|---:|---:|---:|---:|",
    ]
    for _, row in day_distribution.iterrows():
        lines.append(
            f"|{int(round(100*row['threshold']))}%|{int(row['detection_day'])}|"
            f"{int(row['trigger_count'])}|"
            f"{'—' if pd.isna(row['share_of_triggers']) else f'{100*row['share_of_triggers']:.1f}%'}|"
        )

    lines += [
        "",
        "## LINK -> ALGO / AAVE case",
        "",
        f"- signal: {case_info['signal_date']}",
        f"- baseline: {case_info['source']} -> {case_info['baseline']}",
        f"- entry date: {case_info['entry_date']}",
        f"- latest closed candle: {case_info['latest_closed_candle']}",
        "",
        "|Observation day|Date|Top candidate|Top impulse|AAVE impulse vs ALGO|AAVE impulse rank|",
        "|---:|---|---|---:|---:|---:|",
    ]
    for _, row in case_table.iterrows():
        lines.append(
            f"|{int(row['observation_day'])}|{pd.Timestamp(row['observation_date']).date()}|"
            f"{row['top_candidate']}|{fmt_pct(row['top_relative_impulse'])}|"
            f"{fmt_pct(row['aave_relative_impulse_vs_algo'])}|"
            f"{int(row['aave_rank_by_impulse'])}|"
        )

    lines += [
        "",
        "### AAVE threshold crossings",
        "",
    ]
    for key in ("5", "10", "15"):
        crossing = case_info["aave_threshold_crossings"][key]
        if crossing is None:
            lines.append(f"- +{key}%: not reached within first 3 completed bars.")
        else:
            lines.append(
                f"- +{key}%: day {crossing['detection_day']} "
                f"({crossing['detection_date']}), hypothetical switch "
                f"{crossing['switch_date']}; top candidate was "
                f"{crossing['top_candidate']} "
                f"({'AAVE' if crossing['aave_was_top_candidate'] else 'not AAVE'}); "
                f"AAVE relative excess to latest close "
                f"{fmt_pct(crossing['aave_relative_excess_to_latest_close'])}."
            )

    lines += [
        "",
        "## Interpretation boundary",
        "",
        "- Candidate selection is fully causal and occurs only after new closed D1 bars.",
        "- The primary trigger is +10% relative impulse within the first 3 bars.",
        "- +5% and +15% are fixed robustness thresholds, not optimization.",
        "- No transaction-cost/full-path production conclusion is allowed from this diagnostic alone.",
        "- Production/live/Telegram/exchange behavior is unchanged.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_STRESS_TEST",
        "",
    ]

    report = "\n".join(lines)
    (run_dir / "report.md").write_text(report, encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()
