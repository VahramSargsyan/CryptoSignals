from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd

import scripts.research_combined_recovery_gate_v1 as v1

REPO_ROOT = Path(__file__).resolve().parents[1]
BTC_CANDIDATES = ((25, 100), (30, 100), (12, 100))
ROBUSTNESS_DAYS = (180, 120)


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


def btc_bull_state_map(
    panel: pd.DataFrame,
    fast: int,
    slow: int,
) -> dict[pd.Timestamp, bool]:
    close = panel["BTC_close"].astype(float)
    fast_sma = close.rolling(fast, min_periods=fast).mean()
    slow_sma = close.rolling(slow, min_periods=slow).mean()
    state = (fast_sma > slow_sma).fillna(False)
    return {
        pd.Timestamp(ts): bool(flag)
        for ts, flag in zip(panel["timestamp"], state, strict=True)
    }


def build_state_gate_map(
    panel: pd.DataFrame,
    btc_state_by_date: dict[pd.Timestamp, bool],
) -> tuple[dict[pd.Timestamp, bool], dict]:
    gate = {}
    btc_state_days = 0
    blocked_m2 = 0
    blocked_breadth = 0
    passed = 0

    for _, row in panel.iterrows():
        ts = pd.Timestamp(row["timestamp"])
        btc_ok = bool(btc_state_by_date.get(ts, False))
        if not btc_ok:
            gate[ts] = False
            continue

        btc_state_days += 1
        m2_ok = (
            bool(row["m2_expansion_all"])
            if pd.notna(row["m2_expansion_all"])
            else False
        )
        breadth_ok = int(row["breadth_sma200"]) >= 4

        if not m2_ok:
            blocked_m2 += 1
            gate[ts] = False
        elif not breadth_ok:
            blocked_breadth += 1
            gate[ts] = False
        else:
            passed += 1
            gate[ts] = True

    return gate, {
        "btc_bull_state_days": btc_state_days,
        "blocked_by_m2": blocked_m2,
        "blocked_by_breadth_after_m2_pass": blocked_breadth,
        "all_recovery_state_days": passed,
    }


def current_period_summary(panel, signals, trigger_map, start, end):
    return v1.summarize_current_period(panel, signals, trigger_map, start, end)


def current_window_rows(
    panel,
    signals,
    anchor,
    end,
    fast,
    slow,
    v1_event_map,
    v2_state_map,
):
    rows = []
    for days in ROBUSTNESS_DAYS:
        for idx, (start, stop) in enumerate(
            v1.dev.complete_windows(anchor, end, days), start=1
        ):
            low = v1.current_low(panel, signals, start, stop)
            old = current_period_summary(panel, signals, v1_event_map, start, stop)
            new = current_period_summary(panel, signals, v2_state_map, start, stop)

            rows.append({
                "window_days": days,
                "window_index": idx,
                "start": start.isoformat(),
                "end": stop.isoformat(),
                "fast_sma": fast,
                "slow_sma": slow,
                "low_return": low["median_return"],
                "low_dd": low["median_max_drawdown"],
                "v1_return": old["median_return"],
                "v1_dd": old["median_max_drawdown"],
                "v2_return": new["median_return"],
                "v2_dd": new["median_max_drawdown"],
                "v2_beats_low_return": new["median_return"] > low["median_return"],
                "v2_beats_low_dd": new["median_max_drawdown"] > low["median_max_drawdown"],
                "v2_beats_low_both": (
                    new["median_return"] > low["median_return"]
                    and new["median_max_drawdown"] > low["median_max_drawdown"]
                ),
                "v2_beats_v1_return": new["median_return"] > old["median_return"],
                "v2_beats_v1_dd": new["median_max_drawdown"] > old["median_max_drawdown"],
                "v2_beats_v1_both": (
                    new["median_return"] > old["median_return"]
                    and new["median_max_drawdown"] > old["median_max_drawdown"]
                ),
            })
    return pd.DataFrame(rows)


def legacy_period_summary(
    panel,
    signals,
    start,
    end,
    trigger_map,
    fast,
    slow,
):
    return v1.legacy_summary(panel, signals, start, end, trigger_map, fast, slow)


def legacy_window_rows(
    panel,
    signals,
    anchor,
    end,
    fast,
    slow,
    v1_event_map,
    v2_state_map,
):
    rows = []
    for days in ROBUSTNESS_DAYS:
        for idx, (start, stop) in enumerate(
            v1.leg.complete_windows(anchor, end, days), start=1
        ):
            low = v1.legacy_low(panel, signals, start, stop)
            old = legacy_period_summary(
                panel, signals, start, stop, v1_event_map, fast, slow
            )
            new = legacy_period_summary(
                panel, signals, start, stop, v2_state_map, fast, slow
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
                "v1_return": old["median_return"],
                "v1_dd": old["median_max_drawdown"],
                "v2_return": new["median_return"],
                "v2_dd": new["median_max_drawdown"],
                "v2_beats_low_return": new["median_return"] > low["median_return"],
                "v2_beats_low_dd": new["median_max_drawdown"] > low["median_max_drawdown"],
                "v2_beats_low_both": (
                    new["median_return"] > low["median_return"]
                    and new["median_max_drawdown"] > low["median_max_drawdown"]
                ),
                "v2_beats_v1_return": new["median_return"] > old["median_return"],
                "v2_beats_v1_dd": new["median_max_drawdown"] > old["median_max_drawdown"],
            })
    return pd.DataFrame(rows)


def robustness_summary(df: pd.DataFrame) -> dict:
    out = {}
    for days, g in df.groupby("window_days"):
        out[f"{int(days)}d"] = {
            "windows": int(len(g)),
            "return_wins_vs_low": int(g["v2_beats_low_return"].sum()),
            "dd_wins_vs_low": int(g["v2_beats_low_dd"].sum()),
            "both_wins_vs_low": int(g["v2_beats_low_both"].sum()),
            "return_wins_vs_v1": int(g["v2_beats_v1_return"].sum()),
            "dd_wins_vs_v1": int(g["v2_beats_v1_dd"].sum()),
        }
    out["all"] = {
        "windows": int(len(df)),
        "return_wins_vs_low": int(df["v2_beats_low_return"].sum()),
        "dd_wins_vs_low": int(df["v2_beats_low_dd"].sum()),
        "both_wins_vs_low": int(df["v2_beats_low_both"].sum()),
        "return_wins_vs_v1": int(df["v2_beats_v1_return"].sum()),
        "dd_wins_vs_v1": int(df["v2_beats_v1_dd"].sum()),
    }
    return out


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Combined recovery STATE V2: M2 expansion + BTC bullish state + breadth>=4."
    )
    p.add_argument(
        "--output-root",
        type=Path,
        default=REPO_ROOT / "research_artifacts" / "combined_recovery_state_v2",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    # Current 8-asset history.
    panel, crypto_meta = v1.dev.download_panel(v1.dev.DEV_START, v1.dev.DEV_END)
    panel = v1.dev.add_defensive_features(panel)
    btc, btc_meta = v1.dev.download_btc_close(v1.dev.DEV_START, v1.dev.DEV_END)
    panel = v1.dev.add_btc_to_panel(panel, btc)
    panel = v1.add_causal_m2(panel)
    signals, _ = v1.dev.build_pair_context(panel)
    full_start = v1.dev.first_fully_eligible_date(panel)

    current_results = {}
    current_windows = []

    for fast, slow in BTC_CANDIDATES:
        cross_map = v1.current_cross_map(panel, fast, slow)
        v1_event_map, v1_diag = v1.build_gate_map(panel, cross_map)

        btc_state = btc_bull_state_map(panel, fast, slow)
        v2_state_map, v2_diag = build_state_gate_map(panel, btc_state)

        periods = {
            "reproduction": (v1.dev.REPRO_START, v1.dev.REPRO_END),
            "opened_2026": (v1.dev.UNTOUCHED_START, v1.dev.UNTOUCHED_END),
            "full": (full_start, v1.dev.UNTOUCHED_END),
        }

        pdata = {}
        for name, (start, end) in periods.items():
            pdata[name] = {
                "low_vol": v1.current_low(panel, signals, start, end),
                "v1_event_gate": current_period_summary(
                    panel, signals, v1_event_map, start, end
                ),
                "v2_state_gate": current_period_summary(
                    panel, signals, v2_state_map, start, end
                ),
            }

        rw = current_window_rows(
            panel,
            signals,
            full_start,
            v1.dev.UNTOUCHED_END,
            fast,
            slow,
            v1_event_map,
            v2_state_map,
        )
        current_windows.append(rw)

        current_results[f"{fast}_{slow}"] = {
            "v1_event_diagnostics": v1_diag,
            "v2_state_diagnostics": v2_diag,
            "periods": pdata,
            "robustness": robustness_summary(rw),
        }

    current_windows_df = pd.concat(current_windows, ignore_index=True)

    # Retrospective LEGACY-7 stress test.
    lpanel, _, legacy_meta = v1.leg.download_legacy_panel()
    lpanel = v1.leg.add_features(lpanel)
    lpanel = v1.add_causal_m2(lpanel)
    lsignals = v1.leg.build_pair_signals(lpanel)
    lanchor = v1.leg.eligible_anchor(lpanel)

    legacy_results = {}
    legacy_windows = []

    for fast, slow in BTC_CANDIDATES:
        cross_map = v1.leg.btc_cross_map(lpanel, fast, slow)
        v1_event_map, v1_diag = v1.build_gate_map(lpanel, cross_map)

        btc_state = btc_bull_state_map(lpanel, fast, slow)
        v2_state_map, v2_diag = build_state_gate_map(lpanel, btc_state)

        full_old = {
            "low_vol": v1.legacy_low(
                lpanel, lsignals, lanchor, v1.leg.HOLDOUT_END
            ),
            "v1_event_gate": legacy_period_summary(
                lpanel, lsignals, lanchor, v1.leg.HOLDOUT_END,
                v1_event_map, fast, slow
            ),
            "v2_state_gate": legacy_period_summary(
                lpanel, lsignals, lanchor, v1.leg.HOLDOUT_END,
                v2_state_map, fast, slow
            ),
        }

        critical = {
            "start": v1.CRITICAL_2022_START.date().isoformat(),
            "end": v1.CRITICAL_2022_END.date().isoformat(),
            "low_vol": v1.legacy_low(
                lpanel, lsignals, v1.CRITICAL_2022_START, v1.CRITICAL_2022_END
            ),
            "v1_event_gate": legacy_period_summary(
                lpanel, lsignals,
                v1.CRITICAL_2022_START, v1.CRITICAL_2022_END,
                v1_event_map, fast, slow
            ),
            "v2_state_gate": legacy_period_summary(
                lpanel, lsignals,
                v1.CRITICAL_2022_START, v1.CRITICAL_2022_END,
                v2_state_map, fast, slow
            ),
        }

        rw = legacy_window_rows(
            lpanel,
            lsignals,
            lanchor,
            v1.leg.HOLDOUT_END,
            fast,
            slow,
            v1_event_map,
            v2_state_map,
        )
        legacy_windows.append(rw)

        legacy_results[f"{fast}_{slow}"] = {
            "v1_event_diagnostics": v1_diag,
            "v2_state_diagnostics": v2_diag,
            "full_old_period": full_old,
            "critical_2022": critical,
            "robustness": robustness_summary(rw),
        }

    legacy_windows_df = pd.concat(legacy_windows, ignore_index=True)

    run_id = f"COMBINED_RECOVERY_STATE_V2_{_source_commit()[:12]}"
    run_dir = args.output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    current_windows_df.to_csv(
        run_dir / "current8_robustness_windows.csv", index=False
    )
    legacy_windows_df.to_csv(
        run_dir / "legacy7_robustness_windows.csv", index=False
    )

    report = {
        "run_id": run_id,
        "source_commit_sha": _source_commit(),
        "status": "COMBINED_RECOVERY_STATE_V2_EXECUTED",
        "rule": {
            "entry": "breadth<=3 x3 -> next open CASH",
            "m2_state": "3m>0 AND 6m>0 AND 12m>0",
            "btc_state": "fast SMA > SMA100",
            "btc_candidates": [f"{f}/{s}" for f, s in BTC_CANDIDATES],
            "breadth_state": "breadth>=4",
            "exit": "first close with all states true -> next open CASH to current shadow target",
            "rearm": "breadth>=5 x3",
        },
        "change_from_v1": (
            "Fresh BTC crossover event is replaced by persistent BTC bullish state. "
            "No threshold or SMA length changed."
        ),
        "current8": {
            "full_start": full_start,
            "end": v1.dev.UNTOUCHED_END,
            "results": current_results,
            "crypto_dataset": crypto_meta,
            "btc_dataset": btc_meta,
        },
        "legacy7_retrospective": {
            "label": "RETROSPECTIVE_LEGACY7_ROBUSTNESS_NOT_VALIDATION",
            "anchor": lanchor,
            "end": v1.leg.HOLDOUT_END,
            "results": legacy_results,
            "data": legacy_meta,
        },
        "m2_snapshot": {
            "path": str(v1.M2_PATH.relative_to(REPO_ROOT)),
            "sha256": v1.M2_SHA256,
            "official_source": "Federal Reserve H.6 M2.M",
        },
        "interpretation_boundary": [
            "Retrospective V2 informed by V1 failure mode.",
            "No new BTC SMA lengths were searched.",
            "No M2 horizon was changed.",
            "Breadth threshold remains >=4 without optimization.",
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
