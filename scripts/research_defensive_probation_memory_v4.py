from __future__ import annotations

import json
import os
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd

from scripts.research_defensive_low_vol_untouched import (
    CONFIRM_DAYS,
    ENTER_BREADTH_MAX,
    EXIT_BREADTH_MIN,
    HIST_START,
    add_defensive_features,
    run_defensive_backtest,
)
from scripts.research_defensive_shadow_reentry_v2 import (
    NON_WEAK_180D,
    WEAK_180D,
    WINDOWS_120D,
    WINDOWS_180D,
    YEAR_WINDOW,
    summarize_variant,
)
from scripts.research_relative_rotation_graph_intelligence import (
    ASSETS,
    TRANSITION_COST,
    PairSignal,
    build_pair_context,
    choose_candidate,
    download_panel,
    run_backtest,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEV_END = pd.Timestamp("2026-03-28", tz="UTC")
REPLAY_START = pd.Timestamp("2026-03-29", tz="UTC")
REPLAY_END = pd.Timestamp("2026-09-26", tz="UTC")
PROBATION_DAYS = 14


@dataclass(frozen=True)
class V4Result:
    start_asset: str
    total_return: float
    max_drawdown: float
    actual_transitions: int
    defensive_transitions: int
    forced_defensive_days: int
    actual_defensive_token_days: int
    probation_days: int
    defensive_entries: int
    probes_started: int
    probes_succeeded: int
    probes_failed: int
    period_days: int


def _utc(value: str) -> pd.Timestamp:
    return pd.Timestamp(value, tz="UTC")


def _source_commit() -> str:
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO_ROOT,
        text=True,
    ).strip()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def run_probation_memory_backtest(
    panel: pd.DataFrame,
    signals_by_date: dict[pd.Timestamp, list[PairSignal]],
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    start_asset: str,
    episode_log: list[dict] | None = None,
    daily_log: list[dict] | None = None,
) -> V4Result:
    window = panel[(panel["timestamp"] >= start) & (panel["timestamp"] <= end)].copy()
    if window.empty:
        raise ValueError("Empty evaluation window")

    first = window.iloc[0]
    shadow_asset = start_asset
    actual_asset = start_asset
    qty = 1.0 / float(first[f"{actual_asset}_open"])

    state = "NORMAL"
    defensive_asset: str | None = None

    pending_shadow: PairSignal | None = None
    pending_action: tuple[str, str | None] | None = None

    low_streak = 0
    high_streak = 0
    probation_elapsed = 0

    actual_transitions = 0
    defensive_transitions = 0
    forced_defensive_days = 0
    actual_defensive_token_days = 0
    probation_days = 0
    defensive_entries = 0
    probes_started = 0
    probes_succeeded = 0
    probes_failed = 0

    equity: list[float] = []
    dates: list[pd.Timestamp] = []

    def move_actual(row: pd.Series, target: str, *, defensive_move: bool) -> None:
        nonlocal qty, actual_asset, actual_transitions, defensive_transitions
        if actual_asset == target:
            return
        value = qty * float(row[f"{actual_asset}_open"])
        qty = value * (1.0 - TRANSITION_COST) / float(row[f"{target}_open"])
        actual_asset = target
        actual_transitions += 1
        if defensive_move:
            defensive_transitions += 1

    for pos, (_, row) in enumerate(window.iterrows()):
        ts = row["timestamp"]

        previous_shadow = shadow_asset
        shadow_transitioned = False
        if pending_shadow is not None:
            shadow_asset = pending_shadow.to_asset
            pending_shadow = None
            shadow_transitioned = shadow_asset != previous_shadow

        if pending_action is not None:
            action, target = pending_action
            pending_action = None

            if action == "ENTER_DEFENSE":
                if target is None:
                    raise RuntimeError("ENTER_DEFENSE requires target")
                defensive_asset = target
                move_actual(row, target, defensive_move=True)
                state = "DEFENSIVE"
                defensive_entries += 1
                probation_elapsed = 0
                high_streak = 0
                if episode_log is not None:
                    episode_log.append(
                        {
                            "start_asset": start_asset,
                            "action": "ENTER_DEFENSE",
                            "date": ts.isoformat(),
                            "shadow_asset": shadow_asset,
                            "actual_asset": actual_asset,
                            "defensive_asset": defensive_asset,
                            "breadth": int(row["breadth_sma200"]),
                        }
                    )

            elif action == "EXIT_BROAD":
                move_actual(row, shadow_asset, defensive_move=True)
                if episode_log is not None:
                    episode_log.append(
                        {
                            "start_asset": start_asset,
                            "action": "EXIT_BROAD_RECOVERY",
                            "date": ts.isoformat(),
                            "shadow_asset": shadow_asset,
                            "actual_asset": actual_asset,
                            "defensive_asset": defensive_asset,
                            "breadth": int(row["breadth_sma200"]),
                        }
                    )
                state = "NORMAL"
                defensive_asset = None
                low_streak = 0
                high_streak = 0
                probation_elapsed = 0

            elif action == "START_PROBE":
                if not shadow_transitioned:
                    raise RuntimeError("START_PROBE requires a newly executed shadow transition")
                move_actual(row, shadow_asset, defensive_move=True)
                state = "PROBATION"
                probation_elapsed = 0
                probes_started += 1
                if episode_log is not None:
                    episode_log.append(
                        {
                            "start_asset": start_asset,
                            "action": "PROBE_START",
                            "date": ts.isoformat(),
                            "shadow_asset": shadow_asset,
                            "actual_asset": actual_asset,
                            "defensive_asset": defensive_asset,
                            "breadth": int(row["breadth_sma200"]),
                        }
                    )

            elif action == "FALLBACK":
                if defensive_asset is None:
                    raise RuntimeError("FALLBACK requires retained defensive asset")
                move_actual(row, defensive_asset, defensive_move=True)
                state = "DEFENSIVE"
                probation_elapsed = 0
                probes_failed += 1
                high_streak = 0
                if episode_log is not None:
                    episode_log.append(
                        {
                            "start_asset": start_asset,
                            "action": "PROBE_FAIL_FALLBACK",
                            "date": ts.isoformat(),
                            "shadow_asset": shadow_asset,
                            "actual_asset": actual_asset,
                            "defensive_asset": defensive_asset,
                            "breadth": int(row["breadth_sma200"]),
                        }
                    )
            else:
                raise RuntimeError(f"Unknown action: {action}")

        else:
            if state == "NORMAL" and shadow_transitioned:
                move_actual(row, shadow_asset, defensive_move=False)
            elif state == "PROBATION" and shadow_transitioned:
                move_actual(row, shadow_asset, defensive_move=False)
            # In DEFENSIVE, logical shadow transitions do not move actual capital
            # unless START_PROBE was explicitly scheduled at the prior close.

        value_close = qty * float(row[f"{actual_asset}_close"])
        equity.append(value_close)
        dates.append(ts)

        if state == "DEFENSIVE":
            forced_defensive_days += 1
        elif state == "PROBATION":
            probation_days += 1

        if defensive_asset is not None and actual_asset == defensive_asset:
            actual_defensive_token_days += 1

        if daily_log is not None:
            daily_log.append(
                {
                    "start_asset": start_asset,
                    "date": ts.isoformat(),
                    "state": state,
                    "shadow_asset": shadow_asset,
                    "actual_asset": actual_asset,
                    "defensive_asset": defensive_asset,
                    "breadth": int(row["breadth_sma200"]),
                    "equity": value_close,
                    "probation_elapsed": probation_elapsed,
                }
            )

        if pos == len(window) - 1:
            continue

        candidates = [
            sig
            for sig in signals_by_date.get(ts, [])
            if sig.from_asset == shadow_asset
        ]
        next_shadow: PairSignal | None = None
        if candidates:
            next_shadow = choose_candidate(candidates, "BASELINE", None)
            pending_shadow = next_shadow

        breadth = int(row["breadth_sma200"])

        if state == "NORMAL":
            high_streak = 0
            if breadth <= ENTER_BREADTH_MAX:
                low_streak += 1
            else:
                low_streak = 0

            if low_streak >= CONFIRM_DAYS:
                target = row["lowest_vol_asset"]
                if pd.isna(target):
                    raise RuntimeError(f"Missing defensive target at {ts}")
                pending_action = ("ENTER_DEFENSE", str(target))
                low_streak = 0

        elif state == "DEFENSIVE":
            low_streak = 0
            if breadth >= EXIT_BREADTH_MIN:
                high_streak += 1
            else:
                high_streak = 0

            if high_streak >= CONFIRM_DAYS:
                pending_action = ("EXIT_BROAD", None)
                high_streak = 0
            elif next_shadow is not None:
                # This signal is new while already defensive, so it starts a probe.
                pending_action = ("START_PROBE", next_shadow.to_asset)

        elif state == "PROBATION":
            low_streak = 0
            probation_elapsed += 1

            if breadth >= EXIT_BREADTH_MIN:
                high_streak += 1
            else:
                high_streak = 0

            if high_streak >= CONFIRM_DAYS:
                state = "NORMAL"
                probes_succeeded += 1
                if episode_log is not None:
                    episode_log.append(
                        {
                            "start_asset": start_asset,
                            "action": "PROBE_SUCCESS_BROAD_RECOVERY",
                            "date": ts.isoformat(),
                            "shadow_asset": shadow_asset,
                            "actual_asset": actual_asset,
                            "defensive_asset": defensive_asset,
                            "breadth": breadth,
                            "probation_elapsed": probation_elapsed,
                        }
                    )
                defensive_asset = None
                probation_elapsed = 0
                high_streak = 0
                low_streak = 0
            elif probation_elapsed >= PROBATION_DAYS:
                pending_action = ("FALLBACK", None)

        else:
            raise RuntimeError(f"Unknown state: {state}")

    series = pd.Series(equity, index=dates, dtype=float)
    return V4Result(
        start_asset=start_asset,
        total_return=float(series.iloc[-1] / series.iloc[0] - 1.0),
        max_drawdown=float((series / series.cummax() - 1.0).min()),
        actual_transitions=actual_transitions,
        defensive_transitions=defensive_transitions,
        forced_defensive_days=forced_defensive_days,
        actual_defensive_token_days=actual_defensive_token_days,
        probation_days=probation_days,
        defensive_entries=defensive_entries,
        probes_started=probes_started,
        probes_succeeded=probes_succeeded,
        probes_failed=probes_failed,
        period_days=len(window),
    )


def summarize_v4(df: pd.DataFrame) -> dict:
    return {
        "median_return": float(df["total_return"].median()),
        "worst_return": float(df["total_return"].min()),
        "median_max_drawdown": float(df["max_drawdown"].median()),
        "positive_starts": int((df["total_return"] > 0).sum()),
        "median_actual_transitions": float(df["actual_transitions"].median()),
        "median_defensive_transitions": float(df["defensive_transitions"].median()),
        "median_forced_defensive_exposure": float(
            (df["forced_defensive_days"] / df["period_days"]).median()
        ),
        "median_actual_defensive_token_exposure": float(
            (df["actual_defensive_token_days"] / df["period_days"]).median()
        ),
        "median_probation_exposure": float(
            (df["probation_days"] / df["period_days"]).median()
        ),
        "median_probes_started": float(df["probes_started"].median()),
        "median_probes_succeeded": float(df["probes_succeeded"].median()),
        "median_probes_failed": float(df["probes_failed"].median()),
    }


def evaluate_window(
    panel: pd.DataFrame,
    signals_by_date: dict[pd.Timestamp, list[PairSignal]],
    *,
    start_text: str,
    end_text: str,
    window_type: str,
    v4_episode_rows: list[dict],
) -> list[dict]:
    start = _utc(start_text)
    end = _utc(end_text)

    base_rows = []
    original_rows = []
    v4_rows = []

    for asset in ASSETS:
        base_rows.append(
            asdict(
                run_backtest(
                    panel,
                    signals_by_date,
                    {},
                    start=start,
                    end=end,
                    start_asset=asset,
                    variant="BASELINE",
                )
            )
        )
        original_rows.append(
            asdict(
                run_defensive_backtest(
                    panel,
                    signals_by_date,
                    start=start,
                    end=end,
                    start_asset=asset,
                )
            )
        )
        local_episodes: list[dict] = []
        v4_rows.append(
            asdict(
                run_probation_memory_backtest(
                    panel,
                    signals_by_date,
                    start=start,
                    end=end,
                    start_asset=asset,
                    episode_log=local_episodes,
                )
            )
        )
        for row in local_episodes:
            row["window_type"] = window_type
            row["period_start"] = start_text
            row["period_end"] = end_text
            v4_episode_rows.append(row)

    original_df = pd.DataFrame(original_rows)
    original_summary = summarize_variant(original_df)
    original_summary["median_forced_defensive_exposure"] = original_summary.pop(
        "median_defensive_exposure"
    )
    original_summary["median_actual_defensive_token_exposure"] = original_summary[
        "median_forced_defensive_exposure"
    ]
    original_summary["median_probation_exposure"] = 0.0
    original_summary["median_probes_started"] = 0.0
    original_summary["median_probes_succeeded"] = 0.0
    original_summary["median_probes_failed"] = 0.0

    base_summary = summarize_variant(pd.DataFrame(base_rows), base=True)
    base_summary["median_forced_defensive_exposure"] = 0.0
    base_summary["median_actual_defensive_token_exposure"] = 0.0
    base_summary["median_probation_exposure"] = 0.0
    base_summary["median_probes_started"] = 0.0
    base_summary["median_probes_succeeded"] = 0.0
    base_summary["median_probes_failed"] = 0.0

    return [
        {
            "window_type": window_type,
            "period_start": start_text,
            "period_end": end_text,
            "variant": "BASELINE",
            **base_summary,
        },
        {
            "window_type": window_type,
            "period_start": start_text,
            "period_end": end_text,
            "variant": "ORIGINAL_BREADTH_3_5",
            **original_summary,
        },
        {
            "window_type": window_type,
            "period_start": start_text,
            "period_end": end_text,
            "variant": "V4_PROBATION14_MEMORY",
            **summarize_v4(pd.DataFrame(v4_rows)),
        },
    ]


def evaluate_gates(summary: pd.DataFrame) -> dict:
    d180 = summary[summary["window_type"] == "180D"].copy()
    original = d180[d180["variant"] == "ORIGINAL_BREADTH_3_5"].set_index(
        ["period_start", "period_end"]
    )
    v4 = d180[d180["variant"] == "V4_PROBATION14_MEMORY"].set_index(
        ["period_start", "period_end"]
    )

    protection = {}
    for key in sorted(WEAK_180D):
        original_dd = float(original.loc[key, "median_max_drawdown"])
        v4_dd = float(v4.loc[key, "median_max_drawdown"])
        deterioration = abs(v4_dd) - abs(original_dd)
        protection["|".join(key)] = {
            "original_dd": original_dd,
            "v4_dd": v4_dd,
            "drawdown_deterioration": deterioration,
            "pass": deterioration <= 0.10,
        }

    opportunity_wins = sum(
        float(v4.loc[key, "median_return"]) > float(original.loc[key, "median_return"])
        for key in NON_WEAK_180D
    )

    occupancy_wins = sum(
        float(v4.loc[key, "median_actual_defensive_token_exposure"])
        < float(original.loc[key, "median_actual_defensive_token_exposure"])
        for key in NON_WEAK_180D
    )

    churn_pass = bool((v4["median_defensive_transitions"] <= 6).all())

    catastrophic = {}
    catastrophic_pass = True
    for key in original.index:
        orig_ret = float(original.loc[key, "median_return"])
        v4_ret = float(v4.loc[key, "median_return"])
        fail = orig_ret > 0 and v4_ret < -0.10
        catastrophic_pass = catastrophic_pass and not fail
        catastrophic["|".join(key)] = {
            "original_return": orig_ret,
            "v4_return": v4_ret,
            "pass": not fail,
        }

    gates = {
        "protection_retention": {
            "details": protection,
            "pass": all(v["pass"] for v in protection.values()),
        },
        "opportunity_cost_improvement": {
            "wins_required": 2,
            "wins_observed": int(opportunity_wins),
            "pass": opportunity_wins >= 2,
        },
        "defensive_occupancy_improvement": {
            "wins_required": 2,
            "wins_observed": int(occupancy_wins),
            "pass": occupancy_wins >= 2,
        },
        "churn_control": {
            "max_median_defensive_transitions_per_180d": 6,
            "pass": churn_pass,
        },
        "no_catastrophic_regression": {
            "details": catastrophic,
            "pass": catastrophic_pass,
        },
    }
    gates["all_pass"] = all(
        value["pass"] for key, value in gates.items() if key != "all_pass"
    )
    return gates


def evaluate_replay(
    panel: pd.DataFrame,
    signals_by_date: dict[pd.Timestamp, list[PairSignal]],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rows = evaluate_window(
        panel,
        signals_by_date,
        start_text=REPLAY_START.date().isoformat(),
        end_text=REPLAY_END.date().isoformat(),
        window_type="POST_HOC_2026",
        v4_episode_rows=(episodes := []),
    )

    daily_rows: list[dict] = []
    v4_by_start: list[dict] = []
    for asset in ASSETS:
        local_daily: list[dict] = []
        result = run_probation_memory_backtest(
            panel,
            signals_by_date,
            start=REPLAY_START,
            end=REPLAY_END,
            start_asset=asset,
            daily_log=local_daily,
        )
        v4_by_start.append(asdict(result))
        daily_rows.extend(local_daily)

    return pd.DataFrame(rows), pd.DataFrame(episodes), pd.DataFrame(daily_rows)


def main() -> int:
    run_id = f"DEF_PROB_MEMORY_V4_{_source_commit()[:12]}"
    run_dir = (
        REPO_ROOT
        / "research_artifacts"
        / "defensive_probation_memory_v4"
        / run_id
    )
    run_dir.mkdir(parents=True, exist_ok=True)

    # Phase A: development evidence. Do not retrieve post-2026-03-28 data yet.
    dev_panel, dev_meta = download_panel(HIST_START, DEV_END)
    dev_panel = add_defensive_features(dev_panel)
    dev_signals, _ = build_pair_context(dev_panel)

    dev_rows: list[dict] = []
    dev_episodes: list[dict] = []

    dev_rows.extend(
        evaluate_window(
            dev_panel,
            dev_signals,
            start_text=YEAR_WINDOW[0],
            end_text=YEAR_WINDOW[1],
            window_type="1Y",
            v4_episode_rows=dev_episodes,
        )
    )
    for start_text, end_text in WINDOWS_180D:
        dev_rows.extend(
            evaluate_window(
                dev_panel,
                dev_signals,
                start_text=start_text,
                end_text=end_text,
                window_type="180D",
                v4_episode_rows=dev_episodes,
            )
        )
    for start_text, end_text in WINDOWS_120D:
        dev_rows.extend(
            evaluate_window(
                dev_panel,
                dev_signals,
                start_text=start_text,
                end_text=end_text,
                window_type="120D",
                v4_episode_rows=dev_episodes,
            )
        )

    dev_summary = pd.DataFrame(dev_rows)
    gates = evaluate_gates(dev_summary)
    dev_summary.to_csv(run_dir / "development_window_summary.csv", index=False)
    pd.DataFrame(dev_episodes).to_csv(run_dir / "development_v4_episodes.csv", index=False)

    # Phase B: explicitly post-hoc diagnostic replay on the already-opened 2026 period.
    replay_panel, replay_meta = download_panel(HIST_START, REPLAY_END)
    replay_panel = add_defensive_features(replay_panel)
    replay_signals, _ = build_pair_context(replay_panel)
    replay_summary, replay_episodes, replay_daily = evaluate_replay(
        replay_panel,
        replay_signals,
    )
    replay_summary.to_csv(run_dir / "posthoc_2026_summary.csv", index=False)
    replay_episodes.to_csv(run_dir / "posthoc_2026_v4_episodes.csv", index=False)
    replay_daily.to_csv(run_dir / "posthoc_2026_v4_daily.csv", index=False)

    safe_dev = dev_summary.astype(object).where(pd.notna(dev_summary), None)
    safe_replay = replay_summary.astype(object).where(pd.notna(replay_summary), None)

    report = {
        "run_id": run_id,
        "source_commit_sha": _source_commit(),
        "candidate": "DEFENSIVE_LOW_VOL_CRYPTO_PROBATION14_MEMORY_ROUTER_V4",
        "post_oos_redesign_attempt_number": 3,
        "probation_days": PROBATION_DAYS,
        "development": {
            "data_end": DEV_END.date().isoformat(),
            "post_2026_03_28_data_used": False,
            "dataset": dev_meta,
            "gates": gates,
            "status": (
                "DEVELOPMENT_PASS_FORWARD_CANDIDATE"
                if gates["all_pass"]
                else "DEVELOPMENT_FAIL_DO_NOT_PROMOTE"
            ),
            "summary": safe_dev.to_dict(orient="records"),
        },
        "posthoc_2026_replay": {
            "label": "POST_HOC_DIAGNOSTIC_REPLAY_NOT_OOS",
            "period_start": REPLAY_START.date().isoformat(),
            "period_end": REPLAY_END.date().isoformat(),
            "dataset": replay_meta,
            "summary": safe_replay.to_dict(orient="records"),
        },
    }
    _write_json(run_dir / "summary.json", report)

    print("development:")
    print(dev_summary.to_string(index=False))
    print("gates:")
    print(json.dumps(gates, indent=2))
    print("posthoc_2026_replay:")
    print(replay_summary.to_string(index=False))
    print("posthoc_v4_episodes:")
    if replay_episodes.empty:
        print("NONE")
    else:
        print(replay_episodes.to_string(index=False))
    print(f"development_status={report['development']['status']}")
    print("replay_label=POST_HOC_DIAGNOSTIC_REPLAY_NOT_OOS")
    print(f"output={run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
