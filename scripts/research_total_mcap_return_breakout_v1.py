from __future__ import annotations

import argparse
import hashlib
import math
from pathlib import Path

import numpy as np
import pandas as pd

EXPECTED_SHA256 = "d0c9b3bac915e484069c6302ddce1a0b050abbb8de155e4106e39cf6e519d3ee"
SMAS = (25, 50, 100)
THRESHOLDS = (0.10, 0.15, 0.20, 0.25, 0.30, 0.40)
HORIZONS = (7, 14, 30, 60, 90)


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _days(a: pd.Timestamp, b: pd.Timestamp) -> int:
    return int((b - a).days)


def _horizon_row(frame: pd.DataFrame, trigger_i: int, days: int) -> pd.Series | None:
    target = frame.iloc[trigger_i]["timestamp"] + pd.Timedelta(days=days)
    timestamps = frame["timestamp"].to_numpy(dtype="datetime64[ns]")
    j = int(np.searchsorted(timestamps, target.to_datetime64(), side="right") - 1)
    if j < trigger_i or j >= len(frame):
        return None
    if frame.iloc[j]["timestamp"] > frame.iloc[-1]["timestamp"]:
        return None
    if frame.iloc[-1]["timestamp"] < target:
        return None
    return frame.iloc[j]


def build_episodes(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []

    for window in SMAS:
        sma_col = f"sma{window}"
        slope_col = f"sma{window}_slope5"
        dist_col = f"dist{window}"

        valid_idx = frame.index[frame[sma_col].notna()].tolist()
        if len(valid_idx) < 2:
            continue

        for threshold in THRESHOLDS:
            for direction_name, direction in (("UP", 1.0), ("DOWN", -1.0)):
                active = False
                prev_i = valid_idx[0]

                for i in valid_idx[1:]:
                    gap = direction * float(frame.at[i, dist_col])
                    prev_gap = direction * float(frame.at[prev_i, dist_col])

                    if active and gap <= threshold * 0.50:
                        active = False

                    crossed = (not active) and prev_gap < threshold <= gap
                    if crossed:
                        active = True
                        trigger = frame.iloc[i]
                        trigger_ts = trigger["timestamp"]
                        trigger_close = float(trigger["close"])
                        trigger_sma = float(trigger[sma_col])
                        trigger_gap = gap
                        end_ts = trigger_ts + pd.Timedelta(days=90)

                        future = frame[(frame["timestamp"] > trigger_ts) & (frame["timestamp"] <= end_ts)].copy()
                        if future.empty:
                            outcome = "CENSORED"
                            return_ts = None
                            breakout_ts = None
                            touch_ts = None
                            peak_ts = trigger_ts
                            max_gap = trigger_gap
                        else:
                            future_gap = direction * future[dist_col].astype(float)
                            breakout_mask = future_gap >= trigger_gap * 1.25
                            return_mask = future_gap <= trigger_gap * 0.50
                            touch_mask = future_gap <= 0.0

                            breakout_ts = future.loc[breakout_mask, "timestamp"].iloc[0] if breakout_mask.any() else None
                            return_ts = future.loc[return_mask, "timestamp"].iloc[0] if return_mask.any() else None
                            touch_ts = future.loc[touch_mask, "timestamp"].iloc[0] if touch_mask.any() else None

                            combined_gap = pd.concat([
                                pd.Series([trigger_gap], index=[i]),
                                future_gap,
                            ])
                            peak_idx = combined_gap.idxmax()
                            peak_ts = frame.loc[peak_idx, "timestamp"]
                            max_gap = float(combined_gap.max())

                            if return_ts is not None and breakout_ts is not None:
                                if return_ts < breakout_ts:
                                    outcome = "RETURN_FIRST"
                                elif breakout_ts < return_ts:
                                    outcome = "BREAKOUT_FIRST"
                                else:
                                    outcome = "SAME_DAY"
                            elif return_ts is not None:
                                outcome = "RETURN_FIRST"
                            elif breakout_ts is not None:
                                outcome = "BREAKOUT_FIRST"
                            else:
                                outcome = (
                                    "CENSORED"
                                    if frame.iloc[-1]["timestamp"] < end_ts
                                    else "UNRESOLVED_90D"
                                )

                        price_contrib = np.nan
                        sma_contrib = np.nan
                        driver = None
                        if return_ts is not None:
                            ret_row = frame.loc[frame["timestamp"] == return_ts].iloc[0]
                            dlog_p = math.log(float(ret_row["close"]) / trigger_close)
                            dlog_s = math.log(float(ret_row[sma_col]) / trigger_sma)
                            price_contrib = -direction * dlog_p
                            sma_contrib = direction * dlog_s
                            driver = (
                                "PRICE_RETURN_DOMINANT"
                                if price_contrib > sma_contrib
                                else "SMA_CATCHUP_DOMINANT"
                            )

                        s25 = float(trigger["sma25"])
                        s50 = float(trigger["sma50"])
                        s100 = float(trigger["sma100"])
                        fan_spread = (max(s25, s50, s100) - min(s25, s50, s100)) / s100
                        if s25 > s50 > s100:
                            fan_order = "BULL_STACK"
                        elif s25 < s50 < s100:
                            fan_order = "BEAR_STACK"
                        else:
                            fan_order = "MIXED"

                        row = {
                            "sma": window,
                            "threshold_pct": threshold * 100,
                            "direction": direction_name,
                            "trigger_date": trigger_ts.isoformat(),
                            "trigger_close": trigger_close,
                            "trigger_sma": trigger_sma,
                            "trigger_distance_pct": direction * float(trigger[dist_col]) * 100,
                            "sma_slope_5d_pct": float(trigger[slope_col]) * 100,
                            "slope_alignment": (
                                "ALIGNED"
                                if direction * float(trigger[slope_col]) > 0
                                else "OPPOSED_OR_FLAT"
                            ),
                            "fan_spread_pct": fan_spread * 100,
                            "fan_order": fan_order,
                            "first_outcome": outcome,
                            "max_directional_distance_pct": max_gap * 100,
                            "additional_extension_pp": (max_gap - trigger_gap) * 100,
                            "days_to_max_extension": _days(trigger_ts, peak_ts),
                            "days_to_return50": (
                                _days(trigger_ts, return_ts) if return_ts is not None else np.nan
                            ),
                            "days_to_sma_touch": (
                                _days(trigger_ts, touch_ts) if touch_ts is not None else np.nan
                            ),
                            "days_to_breakout25": (
                                _days(trigger_ts, breakout_ts) if breakout_ts is not None else np.nan
                            ),
                            "price_convergence_log": price_contrib,
                            "sma_catchup_log": sma_contrib,
                            "normalization_driver": driver,
                        }

                        for h in HORIZONS:
                            hr = _horizon_row(frame, i, h)
                            if hr is None:
                                row[f"close_return_{h}d_pct"] = np.nan
                                row[f"gap_{h}d_pct"] = np.nan
                            else:
                                row[f"close_return_{h}d_pct"] = (
                                    float(hr["close"]) / trigger_close - 1.0
                                ) * 100
                                row[f"gap_{h}d_pct"] = direction * float(hr[dist_col]) * 100

                        rows.append(row)

                    prev_i = i

    return pd.DataFrame(rows)


def _pct_share(series: pd.Series, label: str) -> float:
    if len(series) == 0:
        return float("nan")
    return float((series == label).mean() * 100)


def summarize(episodes: pd.DataFrame) -> pd.DataFrame:
    out: list[dict] = []
    keys = ["sma", "threshold_pct", "direction"]
    for key, g in episodes.groupby(keys, sort=True):
        resolved_driver = g[g["normalization_driver"].notna()]
        out.append({
            "sma": key[0],
            "threshold_pct": key[1],
            "direction": key[2],
            "episodes": len(g),
            "return_first_pct": _pct_share(g["first_outcome"], "RETURN_FIRST"),
            "breakout_first_pct": _pct_share(g["first_outcome"], "BREAKOUT_FIRST"),
            "same_day_pct": _pct_share(g["first_outcome"], "SAME_DAY"),
            "unresolved_or_censored_pct": float(
                g["first_outcome"].isin(["UNRESOLVED_90D", "CENSORED"]).mean() * 100
            ),
            "median_trigger_distance_pct": g["trigger_distance_pct"].median(),
            "median_additional_extension_pp": g["additional_extension_pp"].median(),
            "median_days_to_max_extension": g["days_to_max_extension"].median(),
            "median_days_to_return50": g["days_to_return50"].median(),
            "median_days_to_sma_touch": g["days_to_sma_touch"].median(),
            "median_sma_slope_5d_pct": g["sma_slope_5d_pct"].median(),
            "price_return_dominant_pct_of_return50": (
                float((resolved_driver["normalization_driver"] == "PRICE_RETURN_DOMINANT").mean() * 100)
                if len(resolved_driver) else np.nan
            ),
            "sma_catchup_dominant_pct_of_return50": (
                float((resolved_driver["normalization_driver"] == "SMA_CATCHUP_DOMINANT").mean() * 100)
                if len(resolved_driver) else np.nan
            ),
        })
    return pd.DataFrame(out)


def slope_summary(episodes: pd.DataFrame) -> pd.DataFrame:
    out: list[dict] = []
    keys = ["sma", "threshold_pct", "direction", "slope_alignment"]
    for key, g in episodes.groupby(keys, sort=True):
        out.append({
            "sma": key[0],
            "threshold_pct": key[1],
            "direction": key[2],
            "slope_alignment": key[3],
            "episodes": len(g),
            "return_first_pct": _pct_share(g["first_outcome"], "RETURN_FIRST"),
            "breakout_first_pct": _pct_share(g["first_outcome"], "BREAKOUT_FIRST"),
            "median_additional_extension_pp": g["additional_extension_pp"].median(),
            "median_days_to_return50": g["days_to_return50"].median(),
        })
    return pd.DataFrame(out)


def fmt(v: object, digits: int = 1) -> str:
    if pd.isna(v):
        return "-"
    return f"{float(v):.{digits}f}"


def build_report(frame: pd.DataFrame, episodes: pd.DataFrame, summary: pd.DataFrame) -> str:
    lines = [
        "# TOTAL Market Cap Return vs Breakout V1 — Results",
        "",
        f"Rows: {len(frame):,}",
        f"Range: {frame.iloc[0]['timestamp'].date()} -> {frame.iloc[-1]['timestamp'].date()}",
        f"Episodes: {len(episodes):,}",
        "",
        "Competing barriers:",
        "- RETURN_FIRST = gap halves before it expands another 25% relative to trigger gap",
        "- BREAKOUT_FIRST = gap expands 25% before halving",
        "",
        "## Summary",
        "",
        "| SMA | Trigger | Direction | N | Return first | Breakout first | Unresolved | Median extra extension | Median days to half-gap | Median days to SMA touch |",
        "|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, r in summary.iterrows():
        lines.append(
            f"| {int(r['sma'])} | {r['threshold_pct']:.0f}% | {r['direction']} | {int(r['episodes'])} | "
            f"{fmt(r['return_first_pct'])}% | {fmt(r['breakout_first_pct'])}% | "
            f"{fmt(r['unresolved_or_censored_pct'])}% | {fmt(r['median_additional_extension_pp'])} pp | "
            f"{fmt(r['median_days_to_return50'])} | {fmt(r['median_days_to_sma_touch'])} |"
        )

    lines.extend([
        "",
        "## Boundary",
        "",
        "Descriptive historical evidence only. No threshold in this report is a production trading signal.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_REPOSITORY_CACHE_STRESS_TEST",
    ])
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=Path("data/market/cryptocap_total_d1.csv"))
    parser.add_argument("--output-dir", type=Path, default=Path("research_artifacts/total_mcap_return_breakout_v1"))
    args = parser.parse_args()

    actual_hash = _hash(args.input)
    if actual_hash != EXPECTED_SHA256:
        raise RuntimeError(f"dataset SHA256 mismatch: {actual_hash}")

    frame = pd.read_csv(args.input)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame = frame.sort_values("timestamp", kind="stable").reset_index(drop=True)

    if frame["timestamp"].duplicated().any():
        raise RuntimeError("duplicate timestamps")
    if frame["close"].isna().any() or (frame["close"] <= 0).any():
        raise RuntimeError("invalid close data")

    for window in SMAS:
        frame[f"sma{window}"] = frame["close"].rolling(window, min_periods=window).mean()
        frame[f"sma{window}_slope5"] = frame[f"sma{window}"] / frame[f"sma{window}"].shift(5) - 1.0
        frame[f"dist{window}"] = frame["close"] / frame[f"sma{window}"] - 1.0

    episodes = build_episodes(frame)
    summary = summarize(episodes)
    slope = slope_summary(episodes)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    episodes.to_csv(args.output_dir / "episodes.csv", index=False)
    summary.to_csv(args.output_dir / "summary.csv", index=False)
    slope.to_csv(args.output_dir / "slope_split.csv", index=False)
    report = build_report(frame, episodes, summary)
    (args.output_dir / "report.md").write_text(report, encoding="utf-8")

    print(f"dataset_sha256={actual_hash}")
    print(f"rows={len(frame)}")
    print(f"range={frame.iloc[0]['timestamp'].isoformat()}..{frame.iloc[-1]['timestamp'].isoformat()}")
    print(f"episodes={len(episodes)}")
    print("SUMMARY_CSV_BEGIN")
    print(summary.to_csv(index=False))
    print("SUMMARY_CSV_END")
    print("SLOPE_SPLIT_CSV_BEGIN")
    print(slope.to_csv(index=False))
    print("SLOPE_SPLIT_CSV_END")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
