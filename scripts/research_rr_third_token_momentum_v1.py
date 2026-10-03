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
OUT = ROOT / "research_artifacts" / "rr_third_token_momentum_v1"

AS_OF = pd.Timestamp("2026-10-03T15:55:00Z")
DOWNLOAD_START = pd.Timestamp("2023-05-05T00:00:00Z")
CASE_SIGNAL_DATE = pd.Timestamp("2026-09-28T00:00:00Z")
CASE_SOURCE = "LINK"
CASE_BASELINE = "ALGO"
CASE_FOCUS = ("AAVE", "TRX", "ALGO")

MOMENTUM_LOOKBACKS = (7, 14)
FORWARD_HORIZONS = (3, 7, 14, 30)
DDG_MIN_RATIO = 1.50


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

    if len(panel) < LOOKBACK + max(MOMENTUM_LOOKBACKS) + 2:
        raise RuntimeError(f"Common panel too short: {len(panel)}")

    metadata["panel"] = {
        "rows": int(len(panel)),
        "start": pd.Timestamp(panel.iloc[0]["timestamp"]).isoformat(),
        "end": pd.Timestamp(panel.iloc[-1]["timestamp"]).isoformat(),
    }
    return panel, metadata


def pct_return(panel: pd.DataFrame, asset: str, i: int, lookback: int) -> float:
    if i - lookback < 0:
        return np.nan
    now = float(panel.iloc[i][f"{asset}_close"])
    past = float(panel.iloc[i - lookback][f"{asset}_close"])
    return now / past - 1.0


def forward_open_return(
    panel: pd.DataFrame,
    asset: str,
    execute_i: int,
    horizon: int,
) -> float:
    end_i = execute_i + horizon
    if execute_i >= len(panel) or end_i >= len(panel):
        return np.nan
    start = float(panel.iloc[execute_i][f"{asset}_open"])
    end = float(panel.iloc[end_i][f"{asset}_open"])
    return end / start - 1.0


def relative_excess(candidate_return: float, baseline_return: float) -> float:
    if pd.isna(candidate_return) or pd.isna(baseline_return):
        return np.nan
    return (1.0 + float(candidate_return)) / (1.0 + float(baseline_return)) - 1.0


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

    # This mirrors the baseline strongest-confirmed ordering per source/date.
    frame = frame.sort_values(
        ["date", "from_asset", "max_dislocation", "to_asset", "pair"],
        ascending=[True, True, False, True, True],
        kind="stable",
    )
    frame = frame.groupby(["date", "from_asset"], as_index=False, sort=True).first()
    return frame.sort_values(["date", "from_asset"]).reset_index(drop=True)


def build_route_opportunities(panel: pd.DataFrame, primary: pd.DataFrame) -> pd.DataFrame:
    index_by_date = {
        pd.Timestamp(ts): i for i, ts in enumerate(pd.to_datetime(panel["timestamp"], utc=True))
    }
    rows = []

    for _, event in primary.iterrows():
        signal_date = pd.Timestamp(event["date"])
        if signal_date not in index_by_date:
            continue
        i = index_by_date[signal_date]
        execute_i = i + 1
        if execute_i >= len(panel):
            continue

        source = str(event["from_asset"]).upper()
        baseline = str(event["to_asset"]).upper()
        allowed = [
            asset for asset in TARGET_ASSETS
            if asset not in {source, baseline}
        ]
        if not allowed:
            continue

        mom = {
            lb: {asset: pct_return(panel, asset, i, lb) for asset in TARGET_ASSETS}
            for lb in MOMENTUM_LOOKBACKS
        }
        c7 = sorted(allowed, key=lambda a: (-mom[7][a], a))[0]
        c14 = sorted(allowed, key=lambda a: (-mom[14][a], a))[0]

        row = {
            "signal_date": signal_date,
            "execute_date": pd.Timestamp(panel.iloc[execute_i]["timestamp"]),
            "source": source,
            "baseline": baseline,
            "pair": event["pair"],
            "baseline_max_dislocation": float(event["max_dislocation"]),
            "baseline_reversal": float(event["reversal_from_extreme"]),
            "baseline_m7": float(mom[7][baseline]),
            "baseline_m14": float(mom[14][baseline]),
            "candidate7": c7,
            "candidate7_m7": float(mom[7][c7]),
            "candidate7_m14": float(mom[14][c7]),
            "candidate14": c14,
            "candidate14_m7": float(mom[7][c14]),
            "candidate14_m14": float(mom[14][c14]),
            "candidate7_m7_minus_baseline": float(mom[7][c7] - mom[7][baseline]),
            "candidate14_m14_minus_baseline": float(mom[14][c14] - mom[14][baseline]),
        }

        for h in FORWARD_HORIZONS:
            b = forward_open_return(panel, baseline, execute_i, h)
            c7r = forward_open_return(panel, c7, execute_i, h)
            c14r = forward_open_return(panel, c14, execute_i, h)
            row[f"baseline_fwd_{h}d"] = b
            row[f"candidate7_fwd_{h}d"] = c7r
            row[f"candidate14_fwd_{h}d"] = c14r
            row[f"candidate7_excess_{h}d"] = relative_excess(c7r, b)
            row[f"candidate14_excess_{h}d"] = relative_excess(c14r, b)

        rows.append(row)

    return pd.DataFrame(rows).sort_values(["signal_date", "source"]).reset_index(drop=True)


def summarize_variant(opps: pd.DataFrame, variant: str) -> pd.DataFrame:
    if variant == "M14":
        diff_col = "candidate14_m14_minus_baseline"
        prefix = "candidate14"
    elif variant == "M7":
        diff_col = "candidate7_m7_minus_baseline"
        prefix = "candidate7"
    else:
        raise ValueError(variant)

    groups = [
        ("ALL_PRIMARY_CONFIRMED", pd.Series(True, index=opps.index)),
        ("CANDIDATE_MOMENTUM_GT_BASELINE", opps[diff_col] > 0),
        ("CANDIDATE_MOMENTUM_GE_BASELINE_PLUS_10PP", opps[diff_col] >= 0.10),
    ]
    rows = []
    for group_name, mask in groups:
        sub = opps[mask].copy()
        for h in FORWARD_HORIZONS:
            col = f"{prefix}_excess_{h}d"
            vals = sub[col].dropna().astype(float)
            rows.append(
                {
                    "variant": variant,
                    "group": group_name,
                    "horizon_days": h,
                    "events_total": int(len(sub)),
                    "events_with_forward_data": int(len(vals)),
                    "candidate_outperformance_rate": (
                        float((vals > 0).mean()) if len(vals) else np.nan
                    ),
                    "median_relative_excess": float(vals.median()) if len(vals) else np.nan,
                    "mean_relative_excess": float(vals.mean()) if len(vals) else np.nan,
                    "p25_relative_excess": float(vals.quantile(0.25)) if len(vals) else np.nan,
                    "p75_relative_excess": float(vals.quantile(0.75)) if len(vals) else np.nan,
                }
            )
    return pd.DataFrame(rows)


def pair_row_for(states: list[dict], asset_a: str, asset_b: str) -> dict | None:
    wanted = {asset_a.upper(), asset_b.upper()}
    for row in states:
        parts = str(row.get("pair") or "").upper().split("/")
        if len(parts) == 2 and set(parts) == wanted:
            return dict(row)
    return None


def directed_value(row: dict | None, source: str, target: str) -> float | None:
    if not row or row.get("deviation") is None:
        return None
    parts = str(row.get("pair") or "").upper().split("/")
    if len(parts) != 2:
        return None
    left, right = parts
    dev = float(row["deviation"])
    source = source.upper()
    target = target.upper()
    if source == right and target == left:
        return dev
    if source == left and target == right:
        return -dev
    return None


def relation_snapshot(
    events: list[dict],
    states: list[dict],
    date: pd.Timestamp,
    source: str,
    target: str,
) -> dict:
    date_iso = pd.Timestamp(date).isoformat()
    source = source.upper()
    target = target.upper()

    same_day = [
        dict(e)
        for e in events
        if str(e.get("date") or "") == date_iso
        and {str(e.get("from_asset") or "").upper(), str(e.get("to_asset") or "").upper()}
        == {source, target}
        and str(e.get("event") or "").upper() in {"ARMED", "CONFIRMED"}
    ]
    same_day.sort(
        key=lambda e: (
            0 if str(e.get("event") or "").upper() == "CONFIRMED" else 1,
            -float(e.get("max_dislocation") or 0.0),
        )
    )
    for e in same_day:
        f = str(e.get("from_asset") or "").upper()
        t = str(e.get("to_asset") or "").upper()
        return {
            "status": f"{str(e.get('event')).upper()}_{'FORWARD' if (f, t) == (source, target) else 'REVERSE'}",
            "forward": (f, t) == (source, target),
            "event": str(e.get("event")).upper(),
            "max_dislocation": float(e.get("max_dislocation") or 0.0),
            "deviation": float(e.get("deviation") or 0.0),
            "reversal_from_extreme": float(e.get("reversal_from_extreme") or 0.0),
            "pair": e.get("pair"),
        }

    row = pair_row_for(states, source, target)
    if row is None:
        return {
            "status": "MISSING",
            "forward": False,
            "event": "NONE",
            "max_dislocation": np.nan,
            "deviation": np.nan,
            "reversal_from_extreme": np.nan,
            "pair": None,
        }

    f = str(row.get("from_asset") or "").upper()
    t = str(row.get("to_asset") or "").upper()
    mode = str(row.get("mode") or "NONE").upper()
    if mode in {"HIGH", "LOW"} and f and t:
        is_forward = (f, t) == (source, target)
        return {
            "status": f"ARMED_{'FORWARD' if is_forward else 'REVERSE'}",
            "forward": is_forward,
            "event": "ARMED",
            "max_dislocation": float(row.get("max_dislocation") or 0.0),
            "deviation": float(row.get("deviation") or 0.0),
            "reversal_from_extreme": float(row.get("reversal_from_extreme") or 0.0),
            "pair": row.get("pair"),
        }

    dv = directed_value(row, source, target)
    return {
        "status": "NONE",
        "forward": False,
        "event": "NONE",
        "max_dislocation": float(abs(dv)) if dv is not None else np.nan,
        "deviation": float(dv) if dv is not None else np.nan,
        "reversal_from_extreme": np.nan,
        "pair": row.get("pair"),
    }


def build_current_case(
    panel: pd.DataFrame,
    full_primary: pd.DataFrame,
) -> tuple[pd.DataFrame, dict]:
    dates = pd.to_datetime(panel["timestamp"], utc=True)
    matches = np.flatnonzero(dates.eq(CASE_SIGNAL_DATE).to_numpy())
    if not len(matches):
        raise RuntimeError(f"Case signal date not found: {CASE_SIGNAL_DATE}")
    i = int(matches[0])

    subset = panel.iloc[: i + 1].copy()
    monitor_cols = ["timestamp"] + [f"{a}_close" for a in ASSETS]
    events, states = build_pair_monitor(
        subset[monitor_cols],
        assets=ASSETS,
        lookback=LOOKBACK,
        arm_threshold=ARM_THRESHOLD,
        reversal=REVERSAL,
    )

    baseline_match = full_primary[
        (full_primary["date"] == CASE_SIGNAL_DATE)
        & (full_primary["from_asset"] == CASE_SOURCE)
    ].copy()
    if baseline_match.empty:
        raise RuntimeError("No primary LINK route on 2026-09-28")
    baseline_event = baseline_match.sort_values(
        ["max_dislocation", "to_asset"], ascending=[False, True]
    ).iloc[0].to_dict()

    execute_i = i + 1
    latest_i = len(panel) - 1
    targets = [asset for asset in TARGET_ASSETS if asset != CASE_SOURCE]
    rows = []
    for target in targets:
        rel = relation_snapshot(events, states, CASE_SIGNAL_DATE, CASE_SOURCE, target)
        latest_ret = (
            float(panel.iloc[latest_i][f"{target}_close"])
            / float(panel.iloc[execute_i][f"{target}_open"])
            - 1.0
            if execute_i <= latest_i else np.nan
        )
        row = {
            "asset": target,
            "m7": pct_return(panel, target, i, 7),
            "m14": pct_return(panel, target, i, 14),
            "link_relation_status": rel["status"],
            "link_directional_deviation": rel["deviation"],
            "link_pair_max_dislocation": rel["max_dislocation"],
            "latest_available_return_from_next_open": latest_ret,
        }
        for h in FORWARD_HORIZONS:
            row[f"fwd_{h}d"] = forward_open_return(panel, target, execute_i, h)
        rows.append(row)

    frame = pd.DataFrame(rows)
    frame["m7_rank_1_best"] = frame["m7"].rank(method="min", ascending=False).astype(int)
    frame["m14_rank_1_best"] = frame["m14"].rank(method="min", ascending=False).astype(int)
    frame = frame.sort_values(["m14_rank_1_best", "asset"]).reset_index(drop=True)

    source_to_aave = relation_snapshot(events, states, CASE_SIGNAL_DATE, "LINK", "AAVE")
    algo_to_aave = relation_snapshot(events, states, CASE_SIGNAL_DATE, "ALGO", "AAVE")
    base_strength = float(baseline_event["max_dislocation"])
    candidate_strength = float(source_to_aave["max_dislocation"]) if pd.notna(source_to_aave["max_dislocation"]) else np.nan

    ddg_eligible = bool(
        source_to_aave["event"] in {"ARMED", "CONFIRMED"}
        and source_to_aave["forward"]
        and pd.notna(candidate_strength)
        and candidate_strength + 1e-12 >= DDG_MIN_RATIO * base_strength
        and algo_to_aave["event"] in {"ARMED", "CONFIRMED"}
        and algo_to_aave["forward"]
    )

    info = {
        "signal_date": CASE_SIGNAL_DATE.isoformat(),
        "source": CASE_SOURCE,
        "baseline_primary": {
            "to_asset": str(baseline_event["to_asset"]),
            "pair": str(baseline_event["pair"]),
            "max_dislocation": base_strength,
            "reversal_from_extreme": float(baseline_event["reversal_from_extreme"]),
        },
        "link_to_aave": source_to_aave,
        "algo_to_aave": algo_to_aave,
        "ddg_min_strength_ratio": DDG_MIN_RATIO,
        "required_link_to_aave_strength": DDG_MIN_RATIO * base_strength,
        "aave_ddg_eligible": ddg_eligible,
        "latest_closed_candle": pd.Timestamp(panel.iloc[-1]["timestamp"]).isoformat(),
    }
    return frame, info


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
    opportunities = build_route_opportunities(panel, primary)

    summary_m14 = summarize_variant(opportunities, "M14")
    summary_m7 = summarize_variant(opportunities, "M7")
    aggregate = pd.concat([summary_m14, summary_m7], ignore_index=True)

    case_frame, case_info = build_current_case(panel, primary)

    primary14 = aggregate[
        (aggregate["variant"] == "M14")
        & (aggregate["group"] == "CANDIDATE_MOMENTUM_GT_BASELINE")
        & (aggregate["horizon_days"] == 14)
    ].iloc[0]

    case_focus = case_frame[case_frame["asset"].isin(CASE_FOCUS)].copy()
    case_focus = case_focus.sort_values("asset")

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    opportunities.to_csv(run_dir / "route_opportunities.csv", index=False)
    aggregate.to_csv(run_dir / "aggregate_summary.csv", index=False)
    case_frame.to_csv(run_dir / "link_algo_case_all_targets.csv", index=False)
    (run_dir / "link_algo_case_ddg.json").write_text(
        json.dumps(case_info, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )

    summary = {
        "experiment": "RR_THIRD_TOKEN_MOMENTUM_V1",
        "workflow_mode": "STRESS_TEST_ONLY",
        "source_commit_sha": source_sha(),
        "as_of": AS_OF.isoformat(),
        "latest_closed_candle": pd.Timestamp(panel.iloc[-1]["timestamp"]).isoformat(),
        "primary_confirmed_route_opportunities": int(len(opportunities)),
        "primary_m14_group": {
            "events_total": int(primary14["events_total"]),
            "events_with_forward_data": int(primary14["events_with_forward_data"]),
            "candidate_outperformance_rate_14d": (
                None if pd.isna(primary14["candidate_outperformance_rate"])
                else float(primary14["candidate_outperformance_rate"])
            ),
            "median_relative_excess_14d": (
                None if pd.isna(primary14["median_relative_excess"])
                else float(primary14["median_relative_excess"])
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
        "# RR THIRD-TOKEN MOMENTUM V1 — RESULTS",
        "",
        "Mode: STRESS_TEST_ONLY",
        "",
        f"As-of: {AS_OF.isoformat()}",
        f"Latest closed D1 candle in common panel: {pd.Timestamp(panel.iloc[-1]['timestamp']).isoformat()}",
        f"Primary confirmed route-opportunities: {len(opportunities)}",
        "",
        "## Primary test — strongest 14D third token",
        "",
        "The primary subgroup contains events where the strongest third token by causal M14 already had higher 14D momentum than the baseline confirmed destination.",
        "",
        "|Horizon|Events with data|Third token beats baseline|Median relative excess|Mean relative excess|",
        "|---:|---:|---:|---:|---:|",
    ]
    table = aggregate[
        (aggregate["variant"] == "M14")
        & (aggregate["group"] == "CANDIDATE_MOMENTUM_GT_BASELINE")
    ].copy()
    for _, row in table.iterrows():
        lines.append(
            f"|{int(row['horizon_days'])}d|{int(row['events_with_forward_data'])}|"
            f"{100*row['candidate_outperformance_rate']:.1f}%|"
            f"{fmt_pct(row['median_relative_excess'])}|"
            f"{fmt_pct(row['mean_relative_excess'])}|"
        )

    lines += [
        "",
        "## Stronger threshold subgroup — M14 at least +10 pp above baseline",
        "",
        "|Horizon|Events with data|Third token beats baseline|Median relative excess|",
        "|---:|---:|---:|---:|",
    ]
    table10 = aggregate[
        (aggregate["variant"] == "M14")
        & (aggregate["group"] == "CANDIDATE_MOMENTUM_GE_BASELINE_PLUS_10PP")
    ].copy()
    for _, row in table10.iterrows():
        rate = row["candidate_outperformance_rate"]
        lines.append(
            f"|{int(row['horizon_days'])}d|{int(row['events_with_forward_data'])}|"
            f"{'—' if pd.isna(rate) else f'{100*rate:.1f}%'}|"
            f"{fmt_pct(row['median_relative_excess'])}|"
        )

    lines += [
        "",
        "## LINK -> ALGO live-case reconstruction at 2026-09-28 close",
        "",
        f"- baseline primary: LINK -> {case_info['baseline_primary']['to_asset']}",
        f"- baseline max dislocation: {fmt_pct(case_info['baseline_primary']['max_dislocation'])}",
        f"- baseline reversal: {fmt_pct(case_info['baseline_primary']['reversal_from_extreme'])}",
        f"- LINK -> AAVE state: {case_info['link_to_aave']['status']}",
        f"- LINK -> AAVE max dislocation/state magnitude: {fmt_pct(case_info['link_to_aave']['max_dislocation'])}",
        f"- ALGO -> AAVE state: {case_info['algo_to_aave']['status']}",
        f"- DDG required LINK -> AAVE strength for a 1.5x override: {fmt_pct(case_info['required_link_to_aave_strength'])}",
        f"- AAVE eligible for accepted DDG override at that close: {'YES' if case_info['aave_ddg_eligible'] else 'NO'}",
        "",
        "### AAVE / ALGO / TRX at the signal close",
        "",
        "|Asset|M7|M14|M7 rank|M14 rank|LINK pair state|Latest available return from Sep-29 open|",
        "|---|---:|---:|---:|---:|---|---:|",
    ]
    for _, row in case_focus.iterrows():
        lines.append(
            f"|{row['asset']}|{fmt_pct(row['m7'])}|{fmt_pct(row['m14'])}|"
            f"{int(row['m7_rank_1_best'])}|{int(row['m14_rank_1_best'])}|"
            f"{row['link_relation_status']}|"
            f"{fmt_pct(row['latest_available_return_from_next_open'])}|"
        )

    lines += [
        "",
        "## Interpretation boundary",
        "",
        "- Candidate selection uses only data available at the signal close.",
        "- Forward returns are measured from the next daily open.",
        "- This test does not rewrite historical routes or authorize momentum chasing.",
        "- The current AAVE case is separated into signal-time evidence and later realized strength.",
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
