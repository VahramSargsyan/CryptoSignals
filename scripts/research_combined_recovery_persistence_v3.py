from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd

import scripts.research_combined_recovery_state_v2 as v2

REPO_ROOT = Path(__file__).resolve().parents[1]
BTC_CANDIDATES = ((25, 100), (30, 100), (12, 100))
ROBUSTNESS_DAYS = (180, 120)
CONFIRM_DAYS = v2.v1.dev.CONFIRM_DAYS


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


def run_persistent_reentry_backtest(
    panel: pd.DataFrame,
    signals_by_date,
    recovery_state_by_date: dict[pd.Timestamp, bool],
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    start_asset: str,
) -> dict:
    dev = v2.v1.dev
    window = panel[(panel["timestamp"] >= start) & (panel["timestamp"] <= end)].copy()
    if window.empty:
        raise ValueError("Empty evaluation window")

    first = window.iloc[0]
    shadow_asset = start_asset
    actual_asset: str | None = start_asset
    qty = 1.0 / float(first[f"{start_asset}_open"])
    cash_value: float | None = None

    pending_shadow = None
    pending_action: str | None = None
    pending_exit_target: str | None = None

    state = "ARMED"
    low_streak = 0
    rearm_streak = 0
    cash_recovery_streak = 0

    actual_transitions = 0
    cash_entries = 0
    cash_days = 0
    reentry_exits = 0
    rearm_count = 0
    disarmed_days = 0
    current_cash_entry_date: pd.Timestamp | None = None
    cash_wait_days: list[int] = []
    streak_resets = 0
    streak_completions = 0

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
                cash_value = value * (1.0 - dev.TRANSITION_COST)
                qty = 0.0
                actual_asset = None
                state = "CASH"
                low_streak = 0
                rearm_streak = 0
                cash_recovery_streak = 0
                actual_transitions += 1
                cash_entries += 1
                current_cash_entry_date = ts

            elif action == "EXIT":
                if state != "CASH" or cash_value is None or pending_exit_target is None:
                    raise RuntimeError("EXIT requires CASH state and target")
                if shadow_asset != pending_exit_target:
                    raise RuntimeError("Shadow target changed unexpectedly at persistent exit")

                target = shadow_asset
                qty = cash_value * (1.0 - dev.TRANSITION_COST) / float(
                    row[f"{target}_open"]
                )
                cash_value = None
                actual_asset = target
                state = "POST_CASH_DISARMED"
                actual_transitions += 1
                reentry_exits += 1
                low_streak = 0
                rearm_streak = 0
                cash_recovery_streak = 0

                if current_cash_entry_date is not None:
                    cash_wait_days.append(int((ts - current_cash_entry_date).days))
                current_cash_entry_date = None
                pending_exit_target = None

            else:
                raise RuntimeError(f"Unknown pending action: {action}")

        elif state != "CASH" and shadow_asset != previous_shadow:
            if actual_asset is None:
                raise RuntimeError("Non-cash state missing actual asset")
            if actual_asset != shadow_asset:
                value = qty * float(row[f"{actual_asset}_open"])
                qty = value * (1.0 - dev.TRANSITION_COST) / float(
                    row[f"{shadow_asset}_open"]
                )
                actual_asset = shadow_asset
                actual_transitions += 1

        if state == "CASH":
            if cash_value is None:
                raise RuntimeError("CASH state missing cash value")
            value_close = cash_value
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
        pending_shadow = (
            dev.choose_candidate(candidates, "BASELINE", None)
            if candidates else None
        )
        prospective_shadow = (
            pending_shadow.to_asset if pending_shadow is not None else shadow_asset
        )

        breadth = int(row["breadth_sma200"])

        if state == "CASH":
            low_streak = 0
            rearm_streak = 0
            recovery_ok = bool(recovery_state_by_date.get(ts, False))

            if recovery_ok:
                cash_recovery_streak += 1
                if cash_recovery_streak >= CONFIRM_DAYS:
                    pending_action = "EXIT"
                    pending_exit_target = prospective_shadow
                    streak_completions += 1
                    cash_recovery_streak = 0
            else:
                if cash_recovery_streak > 0:
                    streak_resets += 1
                cash_recovery_streak = 0
            continue

        cash_recovery_streak = 0

        if state == "POST_CASH_DISARMED":
            low_streak = 0
            if breadth >= dev.EXIT_BREADTH_MIN:
                rearm_streak += 1
            else:
                rearm_streak = 0
            if rearm_streak >= dev.CONFIRM_DAYS:
                state = "ARMED"
                rearm_streak = 0
                low_streak = 0
                rearm_count += 1
            continue

        rearm_streak = 0
        if breadth <= dev.ENTER_BREADTH_MAX:
            low_streak += 1
        else:
            low_streak = 0
        if low_streak >= dev.CONFIRM_DAYS:
            pending_action = "ENTER"
            low_streak = 0

    series = pd.Series(equity, index=dates, dtype=float)
    waits = cash_wait_days

    return {
        "start_asset": start_asset,
        "total_return": float(series.iloc[-1] / series.iloc[0] - 1.0),
        "max_drawdown": float((series / series.cummax() - 1.0).min()),
        "actual_transitions": actual_transitions,
        "cash_entries": cash_entries,
        "cash_days": cash_days,
        "btc_reentry_exits": reentry_exits,
        "rearm_count": rearm_count,
        "disarmed_days": disarmed_days,
        "unresolved_cash_end": state == "CASH",
        "median_cash_wait_days": float(np.median(waits)) if waits else float("nan"),
        "max_cash_wait_days": float(max(waits)) if waits else float("nan"),
        "period_days": len(window),
        "cash_recovery_streak_resets": streak_resets,
        "cash_recovery_streak_completions": streak_completions,
    }


def evaluate_current(
    panel,
    signals,
    recovery_state_map,
    *,
    start,
    end,
) -> pd.DataFrame:
    rows = []
    for asset in v2.v1.dev.ASSETS:
        rows.append(
            run_persistent_reentry_backtest(
                panel,
                signals,
                recovery_state_map,
                start=start,
                end=end,
                start_asset=asset,
            )
        )
    return pd.DataFrame(rows)


def evaluate_legacy(
    panel,
    signals,
    recovery_state_map,
    *,
    start,
    end,
) -> pd.DataFrame:
    rows = []
    for asset in v2.v1.leg.LEGACY_ASSETS:
        rows.append(
            run_persistent_reentry_backtest(
                panel,
                signals,
                recovery_state_map,
                start=start,
                end=end,
                start_asset=asset,
            )
        )
    return pd.DataFrame(rows)


def summarize(df: pd.DataFrame) -> dict:
    base = v2.v1.dev.summarize(df)
    base["median_streak_resets"] = float(
        df["cash_recovery_streak_resets"].median()
    )
    base["median_streak_completions"] = float(
        df["cash_recovery_streak_completions"].median()
    )
    return base


def robustness_summary(df: pd.DataFrame) -> dict:
    out = {}
    for days, g in df.groupby("window_days"):
        out[f"{int(days)}d"] = {
            "windows": int(len(g)),
            "return_wins_vs_low": int(g["v3_beats_low_return"].sum()),
            "dd_wins_vs_low": int(g["v3_beats_low_dd"].sum()),
            "both_wins_vs_low": int(g["v3_beats_low_both"].sum()),
            "return_wins_vs_v2": int(g["v3_beats_v2_return"].sum()),
            "dd_wins_vs_v2": int(g["v3_beats_v2_dd"].sum()),
            "both_wins_vs_v2": int(g["v3_beats_v2_both"].sum()),
        }
    out["all"] = {
        "windows": int(len(df)),
        "return_wins_vs_low": int(df["v3_beats_low_return"].sum()),
        "dd_wins_vs_low": int(df["v3_beats_low_dd"].sum()),
        "both_wins_vs_low": int(df["v3_beats_low_both"].sum()),
        "return_wins_vs_v2": int(df["v3_beats_v2_return"].sum()),
        "dd_wins_vs_v2": int(df["v3_beats_v2_dd"].sum()),
        "both_wins_vs_v2": int(df["v3_beats_v2_both"].sum()),
    }
    return out


def current_windows(
    panel,
    signals,
    anchor,
    end,
    fast,
    slow,
    v2_map,
    v3_map,
):
    rows = []
    for days in ROBUSTNESS_DAYS:
        for idx, (start, stop) in enumerate(
            v2.v1.dev.complete_windows(anchor, end, days), start=1
        ):
            low = v2.v1.current_low(panel, signals, start, stop)
            v2s = v2.current_period_summary(panel, signals, v2_map, start, stop)
            v3s = summarize(
                evaluate_current(
                    panel, signals, v3_map, start=start, end=stop
                )
            )
            rows.append({
                "window_days": days,
                "window_index": idx,
                "start": start.isoformat(),
                "end": stop.isoformat(),
                "fast_sma": fast,
                "slow_sma": slow,
                "low_return": low["median_return"],
                "low_dd": low["median_max_drawdown"],
                "v2_return": v2s["median_return"],
                "v2_dd": v2s["median_max_drawdown"],
                "v3_return": v3s["median_return"],
                "v3_dd": v3s["median_max_drawdown"],
                "v3_beats_low_return": v3s["median_return"] > low["median_return"],
                "v3_beats_low_dd": v3s["median_max_drawdown"] > low["median_max_drawdown"],
                "v3_beats_low_both": (
                    v3s["median_return"] > low["median_return"]
                    and v3s["median_max_drawdown"] > low["median_max_drawdown"]
                ),
                "v3_beats_v2_return": v3s["median_return"] > v2s["median_return"],
                "v3_beats_v2_dd": v3s["median_max_drawdown"] > v2s["median_max_drawdown"],
                "v3_beats_v2_both": (
                    v3s["median_return"] > v2s["median_return"]
                    and v3s["median_max_drawdown"] > v2s["median_max_drawdown"]
                ),
            })
    return pd.DataFrame(rows)


def legacy_windows(
    panel,
    signals,
    anchor,
    end,
    fast,
    slow,
    v2_map,
    v3_map,
):
    rows = []
    for days in ROBUSTNESS_DAYS:
        for idx, (start, stop) in enumerate(
            v2.v1.leg.complete_windows(anchor, end, days), start=1
        ):
            low = v2.v1.legacy_low(panel, signals, start, stop)
            v2s = v2.legacy_period_summary(
                panel, signals, start, stop, v2_map, fast, slow
            )
            v3s = summarize(
                evaluate_legacy(
                    panel, signals, v3_map, start=start, end=stop
                )
            )
            rows.append({
                "window_days": days,
                "window_index": idx,
                "start": start.isoformat(),
                "end": stop.isoformat(),
                "fast_sma": fast,
                "slow_sma": slow,
                "low_return": low["median_return"],
                "low_dd": low["median_max_drawdown"],
                "v2_return": v2s["median_return"],
                "v2_dd": v2s["median_max_drawdown"],
                "v3_return": v3s["median_return"],
                "v3_dd": v3s["median_max_drawdown"],
                "v3_beats_low_return": v3s["median_return"] > low["median_return"],
                "v3_beats_low_dd": v3s["median_max_drawdown"] > low["median_max_drawdown"],
                "v3_beats_low_both": (
                    v3s["median_return"] > low["median_return"]
                    and v3s["median_max_drawdown"] > low["median_max_drawdown"]
                ),
                "v3_beats_v2_return": v3s["median_return"] > v2s["median_return"],
                "v3_beats_v2_dd": v3s["median_max_drawdown"] > v2s["median_max_drawdown"],
                "v3_beats_v2_both": (
                    v3s["median_return"] > v2s["median_return"]
                    and v3s["median_max_drawdown"] > v2s["median_max_drawdown"]
                ),
            })
    return pd.DataFrame(rows)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Combined recovery persistence V3: V2 states must persist 3 closes."
    )
    p.add_argument(
        "--output-root",
        type=Path,
        default=REPO_ROOT / "research_artifacts" / "combined_recovery_persistence_v3",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    # Current 8-asset history.
    panel, crypto_meta = v2.v1.dev.download_panel(
        v2.v1.dev.DEV_START, v2.v1.dev.DEV_END
    )
    panel = v2.v1.dev.add_defensive_features(panel)
    btc, btc_meta = v2.v1.dev.download_btc_close(
        v2.v1.dev.DEV_START, v2.v1.dev.DEV_END
    )
    panel = v2.v1.dev.add_btc_to_panel(panel, btc)
    panel = v2.v1.add_causal_m2(panel)
    signals, _ = v2.v1.dev.build_pair_context(panel)
    full_start = v2.v1.dev.first_fully_eligible_date(panel)

    current_results = {}
    current_robustness = []

    for fast, slow in BTC_CANDIDATES:
        btc_state = v2.btc_bull_state_map(panel, fast, slow)
        all_state_map, state_diag = v2.build_state_gate_map(panel, btc_state)

        periods = {
            "reproduction": (v2.v1.dev.REPRO_START, v2.v1.dev.REPRO_END),
            "opened_2026": (
                v2.v1.dev.UNTOUCHED_START,
                v2.v1.dev.UNTOUCHED_END,
            ),
            "full": (full_start, v2.v1.dev.UNTOUCHED_END),
        }

        pdata = {}
        for name, (start, end) in periods.items():
            low = v2.v1.current_low(panel, signals, start, end)
            v2s = v2.current_period_summary(
                panel, signals, all_state_map, start, end
            )
            v3s = summarize(
                evaluate_current(
                    panel,
                    signals,
                    all_state_map,
                    start=start,
                    end=end,
                )
            )
            pdata[name] = {
                "low_vol": low,
                "v2_state_gate": v2s,
                "v3_persistent_state_gate": v3s,
            }

        rw = current_windows(
            panel,
            signals,
            full_start,
            v2.v1.dev.UNTOUCHED_END,
            fast,
            slow,
            all_state_map,
            all_state_map,
        )
        current_robustness.append(rw)

        current_results[f"{fast}_{slow}"] = {
            "state_diagnostics": state_diag,
            "periods": pdata,
            "robustness": robustness_summary(rw),
        }

    current_robustness_df = pd.concat(current_robustness, ignore_index=True)

    # Retrospective LEGACY-7.
    lpanel, _, legacy_meta = v2.v1.leg.download_legacy_panel()
    lpanel = v2.v1.leg.add_features(lpanel)
    lpanel = v2.v1.add_causal_m2(lpanel)
    lsignals = v2.v1.leg.build_pair_signals(lpanel)
    lanchor = v2.v1.leg.eligible_anchor(lpanel)

    legacy_results = {}
    legacy_robustness = []

    for fast, slow in BTC_CANDIDATES:
        btc_state = v2.btc_bull_state_map(lpanel, fast, slow)
        all_state_map, state_diag = v2.build_state_gate_map(lpanel, btc_state)

        low_full = v2.v1.legacy_low(
            lpanel, lsignals, lanchor, v2.v1.leg.HOLDOUT_END
        )
        v2_full = v2.legacy_period_summary(
            lpanel, lsignals, lanchor, v2.v1.leg.HOLDOUT_END,
            all_state_map, fast, slow
        )
        v3_full = summarize(
            evaluate_legacy(
                lpanel, lsignals, all_state_map,
                start=lanchor, end=v2.v1.leg.HOLDOUT_END
            )
        )

        low_critical = v2.v1.legacy_low(
            lpanel,
            lsignals,
            v2.v1.CRITICAL_2022_START,
            v2.v1.CRITICAL_2022_END,
        )
        v2_critical = v2.legacy_period_summary(
            lpanel,
            lsignals,
            v2.v1.CRITICAL_2022_START,
            v2.v1.CRITICAL_2022_END,
            all_state_map,
            fast,
            slow,
        )
        v3_critical = summarize(
            evaluate_legacy(
                lpanel,
                lsignals,
                all_state_map,
                start=v2.v1.CRITICAL_2022_START,
                end=v2.v1.CRITICAL_2022_END,
            )
        )

        rw = legacy_windows(
            lpanel,
            lsignals,
            lanchor,
            v2.v1.leg.HOLDOUT_END,
            fast,
            slow,
            all_state_map,
            all_state_map,
        )
        legacy_robustness.append(rw)

        legacy_results[f"{fast}_{slow}"] = {
            "state_diagnostics": state_diag,
            "full_old_period": {
                "low_vol": low_full,
                "v2_state_gate": v2_full,
                "v3_persistent_state_gate": v3_full,
            },
            "critical_2022": {
                "start": v2.v1.CRITICAL_2022_START.date().isoformat(),
                "end": v2.v1.CRITICAL_2022_END.date().isoformat(),
                "low_vol": low_critical,
                "v2_state_gate": v2_critical,
                "v3_persistent_state_gate": v3_critical,
            },
            "robustness": robustness_summary(rw),
        }

    legacy_robustness_df = pd.concat(legacy_robustness, ignore_index=True)

    run_id = f"COMBINED_RECOVERY_PERSISTENCE_V3_{_source_commit()[:12]}"
    run_dir = args.output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    current_robustness_df.to_csv(
        run_dir / "current8_robustness_windows.csv", index=False
    )
    legacy_robustness_df.to_csv(
        run_dir / "legacy7_robustness_windows.csv", index=False
    )

    report = {
        "run_id": run_id,
        "source_commit_sha": _source_commit(),
        "status": "COMBINED_RECOVERY_PERSISTENCE_V3_EXECUTED",
        "single_change_from_v2": (
            "All V2 recovery states must be true for 3 consecutive closes while in CASH "
            "before next-open re-entry."
        ),
        "rule": {
            "entry": "breadth<=3 x3 -> next open CASH",
            "m2_state": "3m>0 AND 6m>0 AND 12m>0",
            "btc_state": "fast SMA > SMA100",
            "btc_candidates": [f"{f}/{s}" for f, s in BTC_CANDIDATES],
            "breadth_state": "breadth>=4",
            "recovery_persistence_closes": CONFIRM_DAYS,
            "exit": "third consecutive all-state close -> next open CASH to current shadow target",
            "rearm": "breadth>=5 x3",
        },
        "current8": {
            "full_start": full_start,
            "end": v2.v1.dev.UNTOUCHED_END,
            "results": current_results,
            "crypto_dataset": crypto_meta,
            "btc_dataset": btc_meta,
        },
        "legacy7_retrospective": {
            "label": "RETROSPECTIVE_LEGACY7_ROBUSTNESS_NOT_VALIDATION",
            "anchor": lanchor,
            "end": v2.v1.leg.HOLDOUT_END,
            "results": legacy_results,
            "data": legacy_meta,
        },
        "interpretation_boundary": [
            "Retrospective V3 informed by V2 results.",
            "3-close persistence reuses the already-frozen confirmation count.",
            "No BTC SMA length, M2 horizon, or breadth threshold was searched.",
            "LEGACY-7 is not untouched validation.",
            "No production or paper-live change is authorized.",
        ],
    }

    _write_json(run_dir / "summary.json", report)
    print(json.dumps(_json_safe(report), indent=2, sort_keys=True))
    print(f"output={run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
