from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd

from scripts.research_global_macro_risk_regime_v1 import (
    ANALYSIS_END,
    CRYPTO_DOWNLOAD_START,
    build_crypto_breadth,
    build_crypto_stress_episodes,
)
from scripts.research_oss_market_regime_comparator_v1 import (
    MARKET_DOWNLOAD_START,
    add_crypto_mode,
    align_market_to_crypto,
    build_market_regimes,
    download_market_panel,
    regime_distribution,
)
from scripts.research_relative_rotation_graph_intelligence import download_panel

REPO_ROOT = Path(__file__).resolve().parents[1]
LEAD_WINDOWS = (0, 7, 14, 30)


def _source_commit() -> str:
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, text=True
    ).strip()


def _json_safe(value):
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, np.generic):
        return _json_safe(value.item())
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_json_safe(payload), indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def _row_for_date(daily: pd.DataFrame, date_value) -> pd.Series | None:
    if pd.isna(date_value):
        return None
    date = pd.Timestamp(date_value)
    rows = daily[daily["date"] == date]
    if rows.empty:
        return None
    return rows.iloc[-1]


def _nearest_preceding(
    daily: pd.DataFrame,
    signal_date,
    target_regime: str,
    lookback_days: int = 30,
) -> tuple[pd.Timestamp | None, int | None]:
    if pd.isna(signal_date):
        return None, None
    signal = pd.Timestamp(signal_date)
    window = daily[
        (daily["date"] <= signal)
        & (daily["date"] >= signal - pd.Timedelta(days=lookback_days))
        & (daily["market_regime"] == target_regime)
    ]
    if window.empty:
        return None, None
    regime_date = pd.Timestamp(window.iloc[-1]["date"])
    return regime_date, int((signal - regime_date).days)


def build_episode_evidence(
    daily: pd.DataFrame,
    episodes: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    episode_rows: list[dict] = []
    entry_rows: list[dict] = []
    exit_rows: list[dict] = []
    distribution_rows: list[dict] = []

    for episode_id, (_, ep) in enumerate(episodes.iterrows(), start=1):
        entry_signal = pd.Timestamp(ep["entry_signal_date"])
        entry_exec = pd.Timestamp(ep["entry_execution_date"])
        exit_signal = pd.Timestamp(ep["exit_signal_date"]) if pd.notna(ep.get("exit_signal_date")) else pd.NaT
        exit_exec = pd.Timestamp(ep["exit_execution_date"]) if pd.notna(ep.get("exit_execution_date")) else pd.NaT

        entry_market = _row_for_date(daily, entry_signal)
        exit_market = _row_for_date(daily, exit_signal)

        risk_off_date, risk_off_lead = _nearest_preceding(
            daily, entry_signal, "BROAD_RISK_OFF", 30
        )
        risk_on_date, risk_on_lead = _nearest_preceding(
            daily, exit_signal, "BROAD_RISK_ON", 30
        )

        entry_row = {
            "episode_id": episode_id,
            "signal_date": entry_signal,
            "target_regime": "BROAD_RISK_OFF",
            "found": risk_off_date is not None,
            "regime_date": risk_off_date,
            "lead_days": risk_off_lead,
        }
        exit_row = {
            "episode_id": episode_id,
            "signal_date": exit_signal,
            "target_regime": "BROAD_RISK_ON",
            "found": risk_on_date is not None,
            "regime_date": risk_on_date,
            "lead_days": risk_on_lead,
        }
        for window in LEAD_WINDOWS:
            entry_row[f"within_{window}d"] = (
                risk_off_lead is not None and risk_off_lead <= window
            )
            exit_row[f"within_{window}d"] = (
                risk_on_lead is not None and risk_on_lead <= window
            )
        entry_rows.append(entry_row)
        exit_rows.append(exit_row)

        episode_rows.append(
            {
                "episode_id": episode_id,
                "entry_signal_date": entry_signal,
                "entry_execution_date": entry_exec,
                "entry_breadth": int(ep["entry_breadth"]),
                "entry_market_regime": (
                    entry_market["market_regime"] if entry_market is not None else None
                ),
                "entry_market_confidence": (
                    entry_market["market_regime_confidence"]
                    if entry_market is not None
                    else None
                ),
                "risk_off_lead_days": risk_off_lead,
                "exit_signal_date": exit_signal,
                "exit_execution_date": exit_exec,
                "exit_breadth": (
                    int(ep["exit_breadth"]) if pd.notna(ep.get("exit_breadth")) else None
                ),
                "exit_market_regime": (
                    exit_market["market_regime"] if exit_market is not None else None
                ),
                "exit_market_confidence": (
                    exit_market["market_regime_confidence"]
                    if exit_market is not None
                    else None
                ),
                "risk_on_lead_days": risk_on_lead,
            }
        )

        if pd.isna(exit_exec):
            episode_daily = daily[daily["date"] >= entry_exec]
        else:
            episode_daily = daily[
                (daily["date"] >= entry_exec) & (daily["date"] < exit_exec)
            ]
        counts = episode_daily["market_regime"].value_counts(dropna=True)
        total = int(counts.sum())
        for regime, count in counts.items():
            distribution_rows.append(
                {
                    "episode_id": episode_id,
                    "market_regime": regime,
                    "days": int(count),
                    "share": float(count / total) if total else np.nan,
                }
            )

    return (
        pd.DataFrame(episode_rows),
        pd.DataFrame(entry_rows),
        pd.DataFrame(exit_rows),
        pd.DataFrame(distribution_rows),
    )


def lead_window_summary(leads: pd.DataFrame) -> dict:
    usable = leads[leads["signal_date"].notna()].copy()
    result = {"signals": int(len(usable))}
    found = usable[usable["found"] == True]  # noqa: E712
    result["found_within_30d"] = int(len(found))
    result["median_lead_days_when_found"] = (
        float(found["lead_days"].median()) if len(found) else None
    )
    for window in LEAD_WINDOWS:
        col = f"within_{window}d"
        count = int(usable[col].fillna(False).sum()) if len(usable) else 0
        result[f"within_{window}d_count"] = count
        result[f"within_{window}d_rate"] = (
            float(count / len(usable)) if len(usable) else None
        )
    return result


def lead_summary_by_year(leads: pd.DataFrame) -> list[dict]:
    x = leads[leads["signal_date"].notna()].copy()
    if x.empty:
        return []
    x["year"] = pd.to_datetime(x["signal_date"]).dt.year
    rows: list[dict] = []
    for year, group in x.groupby("year"):
        row = {"year": int(year), "signals": int(len(group))}
        for window in LEAD_WINDOWS:
            col = f"within_{window}d"
            count = int(group[col].fillna(False).sum())
            row[f"within_{window}d_count"] = count
            row[f"within_{window}d_rate"] = float(count / len(group))
        rows.append(row)
    return rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Full-history replication of the frozen OSS market-regime comparator."
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=REPO_ROOT
        / "research_artifacts"
        / "oss_market_regime_full_history_v1",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    panel, crypto_meta = download_panel(CRYPTO_DOWNLOAD_START, ANALYSIS_END)
    crypto = build_crypto_breadth(panel)
    if crypto.empty:
        raise RuntimeError("No fully SMA200-ready crypto breadth rows.")

    history_start = pd.Timestamp(crypto.iloc[0]["date"])
    episodes = build_crypto_stress_episodes(crypto)

    close = download_market_panel(MARKET_DOWNLOAD_START, ANALYSIS_END.tz_localize(None))
    market = build_market_regimes(close)
    daily = align_market_to_crypto(crypto, market)
    daily = add_crypto_mode(daily, episodes)

    episode_table, entry_leads, exit_leads, episode_distribution = (
        build_episode_evidence(daily, episodes)
    )
    aggregate_distribution = regime_distribution(daily)

    run_id = f"OSS_MARKET_REGIME_FULL_HISTORY_V1_{_source_commit()[:12]}"
    run_dir = args.output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    crypto.to_csv(run_dir / "crypto_breadth_full_history.csv", index=False)
    episodes.to_csv(run_dir / "crypto_stress_episodes_full_history.csv", index=False)
    daily.to_csv(run_dir / "crypto_market_regime_daily_full_history.csv", index=False)
    episode_table.to_csv(run_dir / "episode_evidence.csv", index=False)
    entry_leads.to_csv(run_dir / "entry_risk_off_leads.csv", index=False)
    exit_leads.to_csv(run_dir / "exit_risk_on_leads.csv", index=False)
    episode_distribution.to_csv(
        run_dir / "market_regime_distribution_by_episode.csv", index=False
    )
    aggregate_distribution.to_csv(
        run_dir / "market_regime_distribution_by_crypto_mode.csv", index=False
    )

    closed = episodes["exit_execution_date"].notna() if not episodes.empty else pd.Series(dtype=bool)
    summary = {
        "run_id": run_id,
        "source_commit_sha": _source_commit(),
        "status": "RESEARCH_ONLY_OSS_MARKET_REGIME_FULL_HISTORY_EXECUTED",
        "analysis_end": ANALYSIS_END.date().isoformat(),
        "crypto_history_start_first_sma200_ready": history_start.date().isoformat(),
        "full_history_state_reset_at_first_eligible_date": True,
        "retrospective_not_untouched": True,
        "external_threshold_tuning": False,
        "crypto_frozen_rule_changed": False,
        "episode_count": int(len(episodes)),
        "closed_episode_count": int(closed.sum()) if len(episodes) else 0,
        "entry_risk_off": lead_window_summary(entry_leads),
        "exit_risk_on": lead_window_summary(exit_leads),
        "entry_risk_off_by_year": lead_summary_by_year(entry_leads),
        "exit_risk_on_by_year": lead_summary_by_year(exit_leads),
        "aggregate_regime_distribution": aggregate_distribution.to_dict(
            orient="records"
        ),
        "crypto_dataset": crypto_meta,
        "limitations": [
            "This is retrospective descriptive replication, not untouched validation.",
            "External market rules are unchanged from the successful untouched comparator.",
            "Yahoo adjusted history can be revised; point-in-time vendor vintages are not modeled.",
            "The crypto state machine is reset once at the first fully SMA200-ready date.",
            "No result authorizes swaps or changes production/paper-live behavior.",
        ],
    }
    _write_json(run_dir / "summary.json", summary)

    print(json.dumps(_json_safe(summary), indent=2, sort_keys=True, allow_nan=False))
    print(f"output={run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
