from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from scripts.research_cash_defense_destination_ablation_v1 import (
    CASH_LABEL,
    CASH_YIELD,
    complete_windows,
    evaluate_period as evaluate_frozen_destination,
    first_fully_eligible_date,
    summarize_ablation,
)
from scripts.research_defensive_low_vol_untouched import (
    ASSETS,
    CONFIRM_DAYS,
    ENTER_BREADTH_MAX,
    EXIT_BREADTH_MIN,
    HIST_START,
    REPRO_END,
    REPRO_START,
    TRANSITION_COST,
    UNTOUCHED_END,
    UNTOUCHED_START,
    add_defensive_features,
    reproduction_pass,
    summarize as summarize_low_vol,
)
from scripts.research_relative_rotation_graph_intelligence import (
    PairSignal,
    build_pair_context,
    choose_candidate,
    download_panel,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEV_START = HIST_START
DEV_END = UNTOUCHED_END

FAST_GRID = (5, 7, 10, 12, 15, 20, 25, 30)
SLOW_GRID = (20, 25, 30, 40, 50, 60, 75, 100)
SMA_PAIRS = tuple((f, s) for f in FAST_GRID for s in SLOW_GRID if f < s)
ROBUSTNESS_DAYS = (180, 120)


@dataclass(frozen=True)
class BtcReentryResult:
    start_asset: str
    total_return: float
    max_drawdown: float
    actual_transitions: int
    cash_entries: int
    cash_days: int
    btc_reentry_exits: int
    rearm_count: int
    disarmed_days: int
    unresolved_cash_end: bool
    median_cash_wait_days: float
    max_cash_wait_days: float
    period_days: int


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


def download_btc_close(start: pd.Timestamp, end: pd.Timestamp) -> tuple[pd.DataFrame, dict]:
    client = BinanceSpotRestClient()
    cutoff = end + pd.Timedelta(days=1)
    result = download_historical_dataset(
        client,
        symbol="BTCUSDT",
        start=start,
        end=cutoff,
        timeframe="1D",
        as_of=cutoff,
    )
    if result.dataset is None:
        raise RuntimeError(f"No BTCUSDT dataset: {result.metadata.status}")
    if result.dataset.quality.has_critical_issues:
        raise RuntimeError(f"Critical BTCUSDT data quality issue: {result.dataset.quality}")

    frame = result.dataset.candles[["timestamp", "close"]].copy()
    frame = frame.rename(columns={"close": "BTC_close"})
    frame = frame.sort_values("timestamp").reset_index(drop=True)

    metadata = {
        "dataset_id": result.dataset.dataset_id,
        "rows": len(frame),
        "actual_start": result.metadata.actual_start,
        "actual_end": result.metadata.actual_end,
        "listing_truncated": result.metadata.listing_truncated,
        "quality": result.dataset.quality.__dict__,
        "requested_start": start.isoformat(),
        "requested_end": end.isoformat(),
        "older_history_requested": False,
    }
    return frame, metadata


def add_btc_to_panel(panel: pd.DataFrame, btc: pd.DataFrame) -> pd.DataFrame:
    out = panel.merge(btc, on="timestamp", how="inner", validate="one_to_one")
    if out.empty:
        raise RuntimeError("BTC merge produced empty panel")
    return out.sort_values("timestamp").reset_index(drop=True)


def btc_cross_series(panel: pd.DataFrame, fast: int, slow: int) -> pd.Series:
    close = panel["BTC_close"].astype(float)
    fast_sma = close.rolling(fast, min_periods=fast).mean()
    slow_sma = close.rolling(slow, min_periods=slow).mean()
    return ((fast_sma > slow_sma) & (fast_sma.shift(1) <= slow_sma.shift(1))).fillna(False)


def run_btc_sma_reentry_backtest(
    panel: pd.DataFrame,
    signals_by_date: dict[pd.Timestamp, list[PairSignal]],
    cross_by_date: dict[pd.Timestamp, bool],
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    start_asset: str,
    episode_log: list[dict] | None = None,
) -> BtcReentryResult:
    window = panel[(panel["timestamp"] >= start) & (panel["timestamp"] <= end)].copy()
    if window.empty:
        raise ValueError("Empty evaluation window")

    first = window.iloc[0]
    shadow_asset = start_asset
    actual_asset: str | None = start_asset
    qty = 1.0 / float(first[f"{start_asset}_open"])
    cash_value: float | None = None

    pending_shadow: PairSignal | None = None
    pending_action: str | None = None
    pending_exit_target: str | None = None

    state = "ARMED"
    low_streak = 0
    recovery_streak = 0

    actual_transitions = 0
    cash_entries = 0
    cash_days = 0
    btc_reentry_exits = 0
    rearm_count = 0
    disarmed_days = 0
    current_cash_entry_date: pd.Timestamp | None = None
    cash_wait_days: list[int] = []

    equity: list[float] = []
    dates: list[pd.Timestamp] = []

    for pos, (_, row) in enumerate(window.iterrows()):
        ts = pd.Timestamp(row["timestamp"])

        previous_shadow = shadow_asset
        if pending_shadow is not None:
            shadow_asset = pending_shadow.to_asset
            pending_shadow = None

        if pending_action is not None:
            action = pending_action
            pending_action = None

            if action == "ENTER":
                if state != "ARMED" or actual_asset is None:
                    raise RuntimeError("ENTER requires ARMED crypto state")
                value = qty * float(row[f"{actual_asset}_open"])
                cash_value = value * (1.0 - TRANSITION_COST)
                qty = 0.0
                actual_asset = None
                state = "CASH"
                low_streak = 0
                recovery_streak = 0
                actual_transitions += 1
                cash_entries += 1
                current_cash_entry_date = ts
                if episode_log is not None:
                    episode_log.append({
                        "start_asset": start_asset,
                        "action": "ENTER_CASH",
                        "date": ts.isoformat(),
                        "shadow_asset": shadow_asset,
                        "breadth": int(row["breadth_sma200"]),
                    })

            elif action == "EXIT":
                if state != "CASH" or cash_value is None or pending_exit_target is None:
                    raise RuntimeError("EXIT requires CASH state and target")
                if shadow_asset != pending_exit_target:
                    raise RuntimeError("Shadow target changed unexpectedly at BTC exit")
                target = shadow_asset
                qty = cash_value * (1.0 - TRANSITION_COST) / float(row[f"{target}_open"])
                cash_value = None
                actual_asset = target
                state = "POST_CASH_DISARMED"
                low_streak = 0
                recovery_streak = 0
                actual_transitions += 1
                btc_reentry_exits += 1

                wait_days = int((ts - current_cash_entry_date).days) if current_cash_entry_date is not None else 0
                cash_wait_days.append(wait_days)
                current_cash_entry_date = None
                pending_exit_target = None

                if episode_log is not None:
                    episode_log.append({
                        "start_asset": start_asset,
                        "action": "EXIT_CASH_ON_BTC_SMA_CROSS",
                        "date": ts.isoformat(),
                        "actual_asset": actual_asset,
                        "shadow_asset": shadow_asset,
                        "breadth": int(row["breadth_sma200"]),
                        "cash_wait_days": wait_days,
                    })
            else:
                raise RuntimeError(f"Unknown pending action: {action}")

        elif state != "CASH" and shadow_asset != previous_shadow:
            if actual_asset is None:
                raise RuntimeError("Non-cash state missing actual asset")
            if actual_asset != shadow_asset:
                value = qty * float(row[f"{actual_asset}_open"])
                qty = value * (1.0 - TRANSITION_COST) / float(row[f"{shadow_asset}_open"])
                actual_asset = shadow_asset
                actual_transitions += 1

        if state == "CASH":
            if cash_value is None:
                raise RuntimeError("CASH state missing cash value")
            value_close = cash_value * (1.0 + CASH_YIELD)
            cash_days += 1
        else:
            if actual_asset is None:
                raise RuntimeError("Crypto state missing actual asset")
            value_close = qty * float(row[f"{actual_asset}_close"])
            if state == "POST_CASH_DISARMED":
                disarmed_days += 1

        equity.append(value_close)
        dates.append(ts)

        if pos == len(window) - 1:
            continue

        candidates = [
            signal
            for signal in signals_by_date.get(ts, [])
            if signal.from_asset == shadow_asset
        ]
        pending_shadow = choose_candidate(candidates, "BASELINE", None) if candidates else None
        prospective_shadow = pending_shadow.to_asset if pending_shadow is not None else shadow_asset

        breadth = int(row["breadth_sma200"])

        if state == "CASH":
            low_streak = 0
            recovery_streak = 0
            if bool(cross_by_date.get(ts, False)):
                pending_action = "EXIT"
                pending_exit_target = prospective_shadow
            continue

        if state == "POST_CASH_DISARMED":
            low_streak = 0
            if breadth >= EXIT_BREADTH_MIN:
                recovery_streak += 1
            else:
                recovery_streak = 0
            if recovery_streak >= CONFIRM_DAYS:
                state = "ARMED"
                recovery_streak = 0
                low_streak = 0
                rearm_count += 1
            continue

        recovery_streak = 0
        if breadth <= ENTER_BREADTH_MAX:
            low_streak += 1
        else:
            low_streak = 0
        if low_streak >= CONFIRM_DAYS:
            pending_action = "ENTER"
            low_streak = 0

    series = pd.Series(equity, index=dates, dtype=float)
    waits = cash_wait_days
    return BtcReentryResult(
        start_asset=start_asset,
        total_return=float(series.iloc[-1] / series.iloc[0] - 1.0),
        max_drawdown=float((series / series.cummax() - 1.0).min()),
        actual_transitions=actual_transitions,
        cash_entries=cash_entries,
        cash_days=cash_days,
        btc_reentry_exits=btc_reentry_exits,
        rearm_count=rearm_count,
        disarmed_days=disarmed_days,
        unresolved_cash_end=(state == "CASH"),
        median_cash_wait_days=float(np.median(waits)) if waits else float("nan"),
        max_cash_wait_days=float(max(waits)) if waits else float("nan"),
        period_days=len(window),
    )


def evaluate_pair(
    panel: pd.DataFrame,
    signals_by_date: dict[pd.Timestamp, list[PairSignal]],
    cross_by_date: dict[pd.Timestamp, bool],
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    rows=[]
    for asset in ASSETS:
        r=run_btc_sma_reentry_backtest(
            panel, signals_by_date, cross_by_date,
            start=start, end=end, start_asset=asset
        )
        rows.append(r.__dict__)
    return pd.DataFrame(rows)


def summarize(df: pd.DataFrame) -> dict:
    waits=pd.to_numeric(df["median_cash_wait_days"],errors="coerce").dropna()
    max_waits=pd.to_numeric(df["max_cash_wait_days"],errors="coerce").dropna()
    return {
        "median_return":float(df["total_return"].median()),
        "worst_return":float(df["total_return"].min()),
        "median_max_drawdown":float(df["max_drawdown"].median()),
        "worst_max_drawdown":float(df["max_drawdown"].min()),
        "positive_starts":int((df["total_return"]>0).sum()),
        "median_actual_transitions":float(df["actual_transitions"].median()),
        "median_cash_entries":float(df["cash_entries"].median()),
        "median_cash_exposure":float((df["cash_days"]/df["period_days"]).median()),
        "median_reentry_exits":float(df["btc_reentry_exits"].median()),
        "median_rearm_count":float(df["rearm_count"].median()),
        "median_disarmed_exposure":float((df["disarmed_days"]/df["period_days"]).median()),
        "unresolved_cash_starts":int(df["unresolved_cash_end"].sum()),
        "median_cash_wait_days":float(waits.median()) if len(waits) else None,
        "max_cash_wait_days":float(max_waits.max()) if len(max_waits) else None,
    }


def ranking_tuple(row: dict) -> tuple:
    return (
        -int(row["robust_both_wins"]),
        -int(row["robust_dd_wins"]),
        -int(row["robust_return_wins"]),
        -float(row["repro_median_max_drawdown"]),
        -float(row["repro_median_return"]),
        -float(row["full_median_return"]),
        int(row["fast_sma"]),
        int(row["slow_sma"]),
    )


def build_parser() -> argparse.ArgumentParser:
    p=argparse.ArgumentParser(description="Development-only BTC SMA cash re-entry grid search.")
    p.add_argument(
        "--output-root", type=Path,
        default=REPO_ROOT/"research_artifacts"/"btc_sma_reentry_development_v1"
    )
    return p


def main(argv:list[str]|None=None)->int:
    args=build_parser().parse_args(argv)

    panel,crypto_meta=download_panel(DEV_START,DEV_END)
    panel=add_defensive_features(panel)
    btc,btc_meta=download_btc_close(DEV_START,DEV_END)
    panel=add_btc_to_panel(panel,btc)
    signals_by_date,_=build_pair_context(panel)

    frozen_repro=evaluate_frozen_destination(
        panel,signals_by_date,start=REPRO_START,end=REPRO_END,with_daily=False
    )
    gate_pass,gate=reproduction_pass(
        summarize_low_vol(frozen_repro["baseline"],frozen_repro["low_vol"])
    )

    run_id=f"BTC_SMA_REENTRY_DEVELOPMENT_V1_{_source_commit()[:12]}"
    run_dir=args.output_root/run_id
    run_dir.mkdir(parents=True,exist_ok=True)

    if not gate_pass:
        _write_json(run_dir/"summary.json",{
            "run_id":run_id,
            "status":"REPRODUCTION_FAILED_DEVELOPMENT_NOT_INTERPRETED",
            "reproduction_gate":gate,
            "crypto_dataset":crypto_meta,
            "btc_dataset":btc_meta,
        })
        return 4

    full_start=first_fully_eligible_date(panel)
    frozen_opened=evaluate_frozen_destination(
        panel,signals_by_date,start=UNTOUCHED_START,end=UNTOUCHED_END,with_daily=False
    )
    frozen_full=evaluate_frozen_destination(
        panel,signals_by_date,start=full_start,end=UNTOUCHED_END,with_daily=False
    )

    window_specs=[]
    frozen_windows={}
    for days in ROBUSTNESS_DAYS:
        for idx,(start,end) in enumerate(complete_windows(full_start,UNTOUCHED_END,days),start=1):
            key=(days,idx)
            window_specs.append((days,idx,start,end))
            frozen_windows[key]=summarize_ablation(
                evaluate_frozen_destination(
                    panel,signals_by_date,start=start,end=end,with_daily=False
                )
            )

    candidate_rows=[]
    robustness_rows=[]

    for fast,slow in SMA_PAIRS:
        cross=btc_cross_series(panel,fast,slow)
        cross_by_date={
            pd.Timestamp(ts):bool(flag)
            for ts,flag in zip(panel["timestamp"],cross,strict=True)
        }

        repro=summarize(evaluate_pair(
            panel,signals_by_date,cross_by_date,start=REPRO_START,end=REPRO_END
        ))
        opened=summarize(evaluate_pair(
            panel,signals_by_date,cross_by_date,start=UNTOUCHED_START,end=UNTOUCHED_END
        ))
        full=summarize(evaluate_pair(
            panel,signals_by_date,cross_by_date,start=full_start,end=UNTOUCHED_END
        ))

        return_wins=0
        dd_wins=0
        both_wins=0

        for days,idx,start,end in window_specs:
            s=summarize(evaluate_pair(
                panel,signals_by_date,cross_by_date,start=start,end=end
            ))
            low=frozen_windows[(days,idx)]["LOW_VOL_CRYPTO"]
            beat_return=s["median_return"]>low["median_return"]
            beat_dd=s["median_max_drawdown"]>low["median_max_drawdown"]
            beat_both=beat_return and beat_dd
            return_wins+=int(beat_return)
            dd_wins+=int(beat_dd)
            both_wins+=int(beat_both)
            robustness_rows.append({
                "fast_sma":fast,
                "slow_sma":slow,
                "window_days":days,
                "window_index":idx,
                "start":start.isoformat(),
                "end":end.isoformat(),
                "median_return":s["median_return"],
                "median_max_drawdown":s["median_max_drawdown"],
                "cash_exposure":s["median_cash_exposure"],
                "median_cash_wait_days":s["median_cash_wait_days"],
                "unresolved_cash_starts":s["unresolved_cash_starts"],
                "low_vol_return":low["median_return"],
                "low_vol_drawdown":low["median_max_drawdown"],
                "beats_low_vol_return":beat_return,
                "beats_low_vol_drawdown":beat_dd,
                "beats_low_vol_both":beat_both,
            })

        candidate_rows.append({
            "fast_sma":fast,
            "slow_sma":slow,
            "robust_both_wins":both_wins,
            "robust_dd_wins":dd_wins,
            "robust_return_wins":return_wins,
            "repro_median_return":repro["median_return"],
            "repro_median_max_drawdown":repro["median_max_drawdown"],
            "repro_cash_wait_days":repro["median_cash_wait_days"],
            "opened_2026_median_return":opened["median_return"],
            "opened_2026_median_max_drawdown":opened["median_max_drawdown"],
            "opened_2026_cash_wait_days":opened["median_cash_wait_days"],
            "full_median_return":full["median_return"],
            "full_median_max_drawdown":full["median_max_drawdown"],
            "full_worst_return":full["worst_return"],
            "full_positive_starts":full["positive_starts"],
            "full_cash_exposure":full["median_cash_exposure"],
            "full_cash_wait_days":full["median_cash_wait_days"],
            "full_max_cash_wait_days":full["max_cash_wait_days"],
            "full_unresolved_cash_starts":full["unresolved_cash_starts"],
        })

    candidates=pd.DataFrame(candidate_rows)
    ranked=sorted(candidate_rows,key=ranking_tuple)
    top3=ranked[:3]
    robustness=pd.DataFrame(robustness_rows)

    candidates["development_rank"]=candidates.apply(
        lambda r: 1+next(
            i for i,x in enumerate(ranked)
            if int(x["fast_sma"])==int(r["fast_sma"]) and int(x["slow_sma"])==int(r["slow_sma"])
        ),
        axis=1,
    )
    candidates=candidates.sort_values("development_rank")

    candidates.to_csv(run_dir/"candidate_summary.csv",index=False)
    robustness.to_csv(run_dir/"robustness_by_candidate.csv",index=False)
    pd.DataFrame(top3).to_csv(run_dir/"top3_frozen_candidates.csv",index=False)

    report={
        "run_id":run_id,
        "source_commit_sha":_source_commit(),
        "status":"BTC_SMA_REENTRY_DEVELOPMENT_SEARCH_EXECUTED",
        "development_only":True,
        "validation_claim":False,
        "older_holdout_used":False,
        "development_range":{
            "start":DEV_START.date().isoformat(),
            "end":DEV_END.date().isoformat(),
            "full_evaluation_start":full_start.date().isoformat(),
        },
        "search_grid":{
            "fast":list(FAST_GRID),
            "slow":list(SLOW_GRID),
            "pairs_tested":len(SMA_PAIRS),
            "constraint":"fast < slow",
        },
        "ranking_rule":[
            "robust_both_wins desc",
            "robust_dd_wins desc",
            "robust_return_wins desc",
            "repro_median_max_drawdown less_negative",
            "repro_median_return desc",
            "full_median_return desc",
        ],
        "top3_frozen_candidates":top3,
        "frozen_comparators":{
            "reproduction":summarize_ablation(frozen_repro),
            "opened_2026":summarize_ablation(frozen_opened),
            "full_history":summarize_ablation(frozen_full),
        },
        "reproduction_gate":gate,
        "crypto_dataset":crypto_meta,
        "btc_dataset":btc_meta,
        "future_validation":{
            "use_older_unseen_history":True,
            "retune_on_holdout":False,
            "candidates_to_test":top3,
            "note":"Reverse-time historical holdout, not prospective OOS.",
        },
    }
    _write_json(run_dir/"summary.json",report)
    print(json.dumps(_json_safe(report),indent=2,sort_keys=True))
    print(f"output={run_dir}")
    return 0


if __name__=="__main__":
    raise SystemExit(main())
