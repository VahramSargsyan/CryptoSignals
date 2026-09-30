from __future__ import annotations

import hashlib
from pathlib import Path

import numpy as np
import pandas as pd

SRC = Path("data/market/cryptocap_total_d1.csv")
OUT = Path("research_artifacts/total_mcap_momentum_loss_v3")
EXPECTED_SHA256 = "d0c9b3bac915e484069c6302ddce1a0b050abbb8de155e4106e39cf6e519d3ee"
THRESHOLDS = (0.10, 0.15, 0.20, 0.25, 0.30, 0.40)


def med(s: pd.Series) -> float:
    s = s.dropna()
    return float(s.median()) if len(s) else np.nan


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

    def label(row: pd.Series) -> str:
        pa = row["price_accel5"]
        sa = row["sma25_accel5"]
        if pd.isna(pa) or pd.isna(sa):
            return "INSUFFICIENT"
        if pa > 0 and sa > 0:
            return "DUAL_ACCELERATION"
        if pa < 0 and sa < 0:
            return "DUAL_DECELERATION"
        return "MIXED_MOMENTUM"

    episodes: list[dict] = []
    first = int(f["sma100"].first_valid_index())

    for th in THRESHOLDS:
        active = False
        prev_i = first
        for i in range(first + 1, len(f)):
            gap = float(f.at[i, "dist100"])
            prev_gap = float(f.at[prev_i, "dist100"])

            if active and gap <= th * 0.50:
                active = False

            if (not active) and prev_gap < th <= gap:
                active = True
                trigger = f.iloc[i]
                t0 = trigger["timestamp"]
                end = t0 + pd.Timedelta(days=90)
                trigger_gap = gap

                ret_ts = None
                br_ts = None
                touch_ts = None
                max_gap = trigger_gap
                max_ts = t0

                future = f[(f["timestamp"] > t0) & (f["timestamp"] <= end)]
                for _, r in future.iterrows():
                    g = float(r["dist100"])
                    if g > max_gap:
                        max_gap = g
                        max_ts = r["timestamp"]
                    if ret_ts is None and g <= trigger_gap * 0.50:
                        ret_ts = r["timestamp"]
                    if br_ts is None and g >= trigger_gap * 1.25:
                        br_ts = r["timestamp"]
                    if touch_ts is None and g <= 0.0:
                        touch_ts = r["timestamp"]

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
                        if f.iloc[-1]["timestamp"] < end
                        else "UNRESOLVED_90D"
                    )

                episodes.append({
                    "threshold_pct": th * 100,
                    "trigger_date": t0.date().isoformat(),
                    "trigger_distance_pct": trigger_gap * 100,
                    "momentum_label": label(trigger),
                    "mom5_pct": float(trigger["mom5"] * 100),
                    "prev_mom5_pct": float(trigger["prev_mom5"] * 100),
                    "price_accel5_pp": float(trigger["price_accel5"] * 100),
                    "sma25_slope5_pct": float(trigger["sma25_slope5"] * 100),
                    "prev_sma25_slope5_pct": float(trigger["prev_sma25_slope5"] * 100),
                    "sma25_accel5_pp": float(trigger["sma25_accel5"] * 100),
                    "outcome": outcome,
                    "extra_extension_pp": (max_gap - trigger_gap) * 100,
                    "days_to_max": int((max_ts - t0).days),
                    "days_to_return50": int((ret_ts - t0).days) if ret_ts is not None else np.nan,
                    "days_to_touch": int((touch_ts - t0).days) if touch_ts is not None else np.nan,
                })

            prev_i = i

    e = pd.DataFrame(episodes)

    summary_rows = []
    for (th, lab), g in e.groupby(["threshold_pct", "momentum_label"], sort=True):
        summary_rows.append({
            "threshold_pct": th,
            "momentum_label": lab,
            "n": len(g),
            "return_first_pct": float((g["outcome"] == "RETURN_FIRST").mean() * 100),
            "breakout_first_pct": float((g["outcome"] == "BREAKOUT_FIRST").mean() * 100),
            "unresolved_pct": float(g["outcome"].isin(["UNRESOLVED_90D", "CENSORED"]).mean() * 100),
            "median_trigger_distance_pct": med(g["trigger_distance_pct"]),
            "median_mom5_pct": med(g["mom5_pct"]),
            "median_price_accel5_pp": med(g["price_accel5_pp"]),
            "median_sma25_accel5_pp": med(g["sma25_accel5_pp"]),
            "median_extra_extension_pp": med(g["extra_extension_pp"]),
            "median_days_to_max": med(g["days_to_max"]),
            "median_days_to_return50": med(g["days_to_return50"]),
            "median_days_to_touch": med(g["days_to_touch"]),
        })
    summary = pd.DataFrame(summary_rows)

    agg = e[(e["threshold_pct"] >= 10) & (e["threshold_pct"] <= 30)]
    aggregate_rows = []
    for lab, g in agg.groupby("momentum_label", sort=True):
        aggregate_rows.append({
            "momentum_label": lab,
            "n": len(g),
            "return_first_pct": float((g["outcome"] == "RETURN_FIRST").mean() * 100),
            "breakout_first_pct": float((g["outcome"] == "BREAKOUT_FIRST").mean() * 100),
            "unresolved_pct": float(g["outcome"].isin(["UNRESOLVED_90D", "CENSORED"]).mean() * 100),
            "median_mom5_pct": med(g["mom5_pct"]),
            "median_price_accel5_pp": med(g["price_accel5_pp"]),
            "median_sma25_accel5_pp": med(g["sma25_accel5_pp"]),
            "median_extra_extension_pp": med(g["extra_extension_pp"]),
            "median_days_to_return50": med(g["days_to_return50"]),
            "median_days_to_touch": med(g["days_to_touch"]),
        })
    aggregate = pd.DataFrame(aggregate_rows)

    latest = f.iloc[-1]
    current = pd.DataFrame([{
        "date": latest["timestamp"].date().isoformat(),
        "distance100_pct": latest["dist100"] * 100,
        "mom5_pct": latest["mom5"] * 100,
        "prev_mom5_pct": latest["prev_mom5"] * 100,
        "price_accel5_pp": latest["price_accel5"] * 100,
        "sma25_slope5_pct": latest["sma25_slope5"] * 100,
        "prev_sma25_slope5_pct": latest["prev_sma25_slope5"] * 100,
        "sma25_accel5_pp": latest["sma25_accel5"] * 100,
        "momentum_label": label(latest),
    }])

    OUT.mkdir(parents=True, exist_ok=True)
    e.to_csv(OUT / "episodes.csv", index=False)
    summary.to_csv(OUT / "summary.csv", index=False)
    aggregate.to_csv(OUT / "aggregate.csv", index=False)
    current.to_csv(OUT / "current.csv", index=False)

    print(f"dataset_sha256={actual}")
    print(f"episodes={len(e)}")
    print("SUMMARY_BEGIN")
    print(summary.to_csv(index=False))
    print("SUMMARY_END")
    print("AGGREGATE_BEGIN")
    print(aggregate.to_csv(index=False))
    print("AGGREGATE_END")
    print("CURRENT_BEGIN")
    print(current.to_csv(index=False))
    print("CURRENT_END")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
