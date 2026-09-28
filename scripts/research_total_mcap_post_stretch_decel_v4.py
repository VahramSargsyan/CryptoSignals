from __future__ import annotations

import hashlib
from pathlib import Path

import numpy as np
import pandas as pd

SRC = Path("data/market/cryptocap_total_d1.csv")
OUT = Path("research_artifacts/total_mcap_post_stretch_decel_v4")
EXPECTED_SHA256 = "d0c9b3bac915e484069c6302ddce1a0b050abbb8de155e4106e39cf6e519d3ee"
ANCHORS = (0.15, 0.20)


def med(series: pd.Series) -> float:
    s = series.dropna()
    return float(s.median()) if len(s) else np.nan


def first_outcome(
    frame: pd.DataFrame,
    start_i: int,
    gap_col: str,
    base_gap: float,
    horizon_days: int = 90,
) -> tuple[str, pd.Timestamp | None, pd.Timestamp | None, pd.Timestamp | None, float, pd.Timestamp]:
    t0 = frame.at[start_i, "timestamp"]
    end = t0 + pd.Timedelta(days=horizon_days)
    ret_ts = None
    br_ts = None
    touch_ts = None
    max_gap = float(base_gap)
    max_ts = t0

    for j in range(start_i + 1, len(frame)):
        ts = frame.at[j, "timestamp"]
        if ts > end:
            break
        gap = float(frame.at[j, gap_col])
        if gap > max_gap:
            max_gap = gap
            max_ts = ts
        if ret_ts is None and gap <= base_gap * 0.50:
            ret_ts = ts
        if br_ts is None and gap >= base_gap * 1.25:
            br_ts = ts
        if touch_ts is None and gap <= 0:
            touch_ts = ts

    if ret_ts is not None and br_ts is not None:
        if ret_ts < br_ts:
            outcome = "RETURN_FIRST"
        elif br_ts < ret_ts:
            outcome = "BREAKOUT_FIRST"
        else:
            outcome = "SAME_DAY"
    elif ret_ts is not None:
        outcome = "RETURN_FIRST"
    elif br_ts is not None:
        outcome = "BREAKOUT_FIRST"
    else:
        outcome = (
            "CENSORED"
            if frame.iloc[-1]["timestamp"] < end
            else "UNRESOLVED_90D"
        )
    return outcome, ret_ts, br_ts, touch_ts, max_gap, max_ts


def main() -> int:
    actual = hashlib.sha256(SRC.read_bytes()).hexdigest()
    if actual != EXPECTED_SHA256:
        raise RuntimeError(f"dataset hash mismatch: {actual}")

    f = pd.read_csv(SRC)
    f["timestamp"] = pd.to_datetime(f["timestamp"], utc=True)
    f = f.sort_values("timestamp", kind="stable").reset_index(drop=True)

    for w in (25, 100):
        f[f"sma{w}"] = f["close"].rolling(w, min_periods=w).mean()

    f["dist100"] = f["close"] / f["sma100"] - 1.0

    f["mom5"] = f["close"] / f["close"].shift(5) - 1.0
    f["prev_mom5"] = f["close"].shift(5) / f["close"].shift(10) - 1.0
    f["price_accel5"] = f["mom5"] - f["prev_mom5"]

    f["sma25_slope5"] = f["sma25"] / f["sma25"].shift(5) - 1.0
    f["prev_sma25_slope5"] = f["sma25"].shift(5) / f["sma25"].shift(10) - 1.0
    f["sma25_accel5"] = f["sma25_slope5"] - f["prev_sma25_slope5"]

    f["dual_deceleration"] = (f["price_accel5"] < 0) & (f["sma25_accel5"] < 0)

    episodes: list[dict] = []
    first = int(f["sma100"].first_valid_index())

    for th in ANCHORS:
        active = False
        prev_i = first

        for i in range(first + 1, len(f)):
            gap = float(f.at[i, "dist100"])
            prev_gap = float(f.at[prev_i, "dist100"])

            if active and gap <= th * 0.50:
                active = False

            if (not active) and prev_gap < th <= gap:
                active = True
                trigger_i = i
                trigger_ts = f.at[i, "timestamp"]
                trigger_gap = gap

                anchor_outcome, anchor_ret, anchor_br, anchor_touch, anchor_max_gap, anchor_max_ts = first_outcome(
                    f, trigger_i, "dist100", trigger_gap
                )

                search_end = trigger_ts + pd.Timedelta(days=90)
                decel_i = None
                running_peak_before_decel = trigger_gap

                for j in range(trigger_i + 1, len(f)):
                    ts = f.at[j, "timestamp"]
                    if ts > search_end:
                        break
                    g = float(f.at[j, "dist100"])
                    if g > running_peak_before_decel:
                        running_peak_before_decel = g

                    if g <= th * 0.50:
                        break

                    if bool(f.at[j, "dual_deceleration"]):
                        decel_i = j
                        break

                row = {
                    "anchor_pct": th * 100,
                    "trigger_date": trigger_ts.date().isoformat(),
                    "trigger_gap_pct": trigger_gap * 100,
                    "anchor_outcome": anchor_outcome,
                    "anchor_extra_extension_pp": (anchor_max_gap - trigger_gap) * 100,
                    "anchor_days_to_return50": (
                        (anchor_ret - trigger_ts).days if anchor_ret is not None else np.nan
                    ),
                    "decel_found": decel_i is not None,
                }

                if decel_i is not None:
                    decel_ts = f.at[decel_i, "timestamp"]
                    decel_gap = float(f.at[decel_i, "dist100"])
                    decel_outcome, decel_ret, decel_br, decel_touch, decel_max_gap, decel_max_ts = first_outcome(
                        f, decel_i, "dist100", decel_gap
                    )

                    new_peak_before_return = False
                    end2 = decel_ts + pd.Timedelta(days=90)
                    stop_ts = decel_ret if decel_ret is not None else end2
                    for j in range(decel_i + 1, len(f)):
                        ts = f.at[j, "timestamp"]
                        if ts > end2 or ts > stop_ts:
                            break
                        if float(f.at[j, "dist100"]) > running_peak_before_decel:
                            new_peak_before_return = True
                            break

                    row.update({
                        "decel_date": decel_ts.date().isoformat(),
                        "days_trigger_to_decel": int((decel_ts - trigger_ts).days),
                        "decel_gap_pct": decel_gap * 100,
                        "decel_mom5_pct": float(f.at[decel_i, "mom5"] * 100),
                        "decel_sma25_slope5_pct": float(f.at[decel_i, "sma25_slope5"] * 100),
                        "both_momentum_levels_positive": bool(
                            f.at[decel_i, "mom5"] > 0 and f.at[decel_i, "sma25_slope5"] > 0
                        ),
                        "price_accel5_pp": float(f.at[decel_i, "price_accel5"] * 100),
                        "sma25_accel5_pp": float(f.at[decel_i, "sma25_accel5"] * 100),
                        "running_peak_before_decel_pct": running_peak_before_decel * 100,
                        "decel_outcome": decel_outcome,
                        "new_peak_before_decel_return50": new_peak_before_return,
                        "decel_extra_extension_pp": (decel_max_gap - decel_gap) * 100,
                        "decel_days_to_return50": (
                            (decel_ret - decel_ts).days if decel_ret is not None else np.nan
                        ),
                        "decel_days_to_touch": (
                            (decel_touch - decel_ts).days if decel_touch is not None else np.nan
                        ),
                    })
                else:
                    row.update({
                        "decel_date": None,
                        "days_trigger_to_decel": np.nan,
                        "decel_gap_pct": np.nan,
                        "decel_mom5_pct": np.nan,
                        "decel_sma25_slope5_pct": np.nan,
                        "both_momentum_levels_positive": np.nan,
                        "price_accel5_pp": np.nan,
                        "sma25_accel5_pp": np.nan,
                        "running_peak_before_decel_pct": np.nan,
                        "decel_outcome": None,
                        "new_peak_before_decel_return50": np.nan,
                        "decel_extra_extension_pp": np.nan,
                        "decel_days_to_return50": np.nan,
                        "decel_days_to_touch": np.nan,
                    })

                episodes.append(row)

            prev_i = i

    e = pd.DataFrame(episodes)

    summary_rows = []
    for th, g in e.groupby("anchor_pct", sort=True):
        d = g[g["decel_found"] == True]
        summary_rows.append({
            "anchor_pct": th,
            "episodes": len(g),
            "anchor_return_first_pct": float((g["anchor_outcome"] == "RETURN_FIRST").mean() * 100),
            "anchor_breakout_first_pct": float((g["anchor_outcome"] == "BREAKOUT_FIRST").mean() * 100),
            "decel_found_pct": float(g["decel_found"].mean() * 100),
            "decel_n": len(d),
            "median_days_trigger_to_decel": med(d["days_trigger_to_decel"]),
            "decel_return_first_pct": float((d["decel_outcome"] == "RETURN_FIRST").mean() * 100) if len(d) else np.nan,
            "decel_breakout_first_pct": float((d["decel_outcome"] == "BREAKOUT_FIRST").mean() * 100) if len(d) else np.nan,
            "decel_unresolved_pct": float(d["decel_outcome"].isin(["UNRESOLVED_90D", "CENSORED"]).mean() * 100) if len(d) else np.nan,
            "positive_levels_at_decel_pct": float(d["both_momentum_levels_positive"].mean() * 100) if len(d) else np.nan,
            "new_peak_before_return50_pct": float(d["new_peak_before_decel_return50"].mean() * 100) if len(d) else np.nan,
            "median_decel_gap_pct": med(d["decel_gap_pct"]),
            "median_decel_extra_extension_pp": med(d["decel_extra_extension_pp"]),
            "median_decel_days_to_return50": med(d["decel_days_to_return50"]),
            "median_decel_days_to_touch": med(d["decel_days_to_touch"]),
        })

    summary = pd.DataFrame(summary_rows)

    latest_ts = f.iloc[-1]["timestamp"]
    current_rows = []
    for th in ANCHORS:
        candidates = e[e["anchor_pct"] == th]
        if len(candidates):
            last = candidates.iloc[-1]
            current_rows.append({
                "anchor_pct": th * 100,
                "latest_cached_date": latest_ts.date().isoformat(),
                "latest_trigger_date": last["trigger_date"],
                "decel_found": bool(last["decel_found"]),
                "decel_date": last["decel_date"],
                "decel_outcome_so_far": last["decel_outcome"],
                "latest_gap100_pct": float(f.iloc[-1]["dist100"] * 100),
                "latest_mom5_pct": float(f.iloc[-1]["mom5"] * 100),
                "latest_sma25_slope5_pct": float(f.iloc[-1]["sma25_slope5"] * 100),
                "latest_price_accel5_pp": float(f.iloc[-1]["price_accel5"] * 100),
                "latest_sma25_accel5_pp": float(f.iloc[-1]["sma25_accel5"] * 100),
            })

    current = pd.DataFrame(current_rows)

    OUT.mkdir(parents=True, exist_ok=True)
    e.to_csv(OUT / "episodes.csv", index=False)
    summary.to_csv(OUT / "summary.csv", index=False)
    current.to_csv(OUT / "current.csv", index=False)

    print(f"dataset_sha256={actual}")
    print(f"episodes={len(e)}")
    print("SUMMARY_BEGIN")
    print(summary.to_csv(index=False))
    print("SUMMARY_END")
    print("CURRENT_BEGIN")
    print(current.to_csv(index=False))
    print("CURRENT_END")
    print("EPISODES_BEGIN")
    print(e.to_csv(index=False))
    print("EPISODES_END")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
