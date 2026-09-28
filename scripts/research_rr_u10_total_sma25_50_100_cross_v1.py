from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd

import scripts.research_robust_exhaustive_u9_u10_atom_replacement_v1 as core


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "rr_u10_total_sma25_50_100_cross_v1"
CACHE = ROOT / "data" / "market" / "cryptocap_total_d1.csv"
CACHE_SHA256 = "d0c9b3bac915e484069c6302ddce1a0b050abbb8de155e4106e39cf6e519d3ee"

U10 = (
    "TWT", "PEPE", "BNB", "TRX", "AAVE",
    "AVAX", "FIL", "ALGO", "XRP", "HBAR",
)
MATURE_START = pd.Timestamp("2023-10-31", tz="UTC")
CUTOFF = pd.Timestamp("2026-09-26", tz="UTC")
HORIZONS = (7, 14, 30, 60, 90)
SEQUENCE_MAX_DAYS = 90


def source_sha() -> str:
    return os.environ.get("SOURCE_COMMIT_SHA") or subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def load_total() -> pd.DataFrame:
    digest = hashlib.sha256(CACHE.read_bytes()).hexdigest()
    if digest != CACHE_SHA256:
        raise RuntimeError(
            f"TOTAL cache SHA mismatch: {digest} != {CACHE_SHA256}"
        )
    frame = pd.read_csv(CACHE)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame = frame.sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)
    frame = frame[frame["timestamp"] <= CUTOFF].reset_index(drop=True)

    for n in (25, 50, 100):
        frame[f"sma{n}_calc"] = frame["close"].astype(float).rolling(n, min_periods=n).mean()

    # Verify cached SMA50/100 semantics match recomputation closely.
    for n in (50, 100):
        valid = frame[[f"sma{n}", f"sma{n}_calc"]].dropna()
        if valid.empty:
            raise RuntimeError(f"No valid SMA{n} rows")
        diff = np.max(np.abs(valid[f"sma{n}"].to_numpy(float) - valid[f"sma{n}_calc"].to_numpy(float)))
        scale = np.max(np.abs(valid[f"sma{n}"].to_numpy(float)))
        if diff > max(1.0, scale * 1e-10):
            raise RuntimeError(f"Cached SMA{n} mismatch: max diff {diff}")

    return frame


def cross_events(total: pd.DataFrame) -> pd.DataFrame:
    rows = []
    specs = [
        ("SMA25_CROSS_ABOVE_50", "BULL", 25, 50, "above"),
        ("SMA25_CROSS_BELOW_50", "BEAR", 25, 50, "below"),
        ("SMA25_CROSS_ABOVE_100", "BULL", 25, 100, "above"),
        ("SMA25_CROSS_BELOW_100", "BEAR", 25, 100, "below"),
        ("SMA50_CROSS_ABOVE_100", "BULL", 50, 100, "above"),
        ("SMA50_CROSS_BELOW_100", "BEAR", 50, 100, "below"),
    ]

    for i in range(1, len(total)):
        cur = total.iloc[i]
        prev = total.iloc[i - 1]
        if pd.Timestamp(cur["timestamp"]) < MATURE_START:
            continue
        for event, direction, fast, slow, kind in specs:
            p_fast = prev[f"sma{fast}_calc"]
            p_slow = prev[f"sma{slow}_calc"]
            c_fast = cur[f"sma{fast}_calc"]
            c_slow = cur[f"sma{slow}_calc"]
            if any(pd.isna(x) for x in (p_fast, p_slow, c_fast, c_slow)):
                continue
            hit = (
                (p_fast <= p_slow and c_fast > c_slow)
                if kind == "above"
                else (p_fast >= p_slow and c_fast < c_slow)
            )
            if not hit:
                continue
            rows.append({
                "date": pd.Timestamp(cur["timestamp"]),
                "event": event,
                "direction": direction,
                "total_close_usd": float(cur["close"]),
                "sma25": float(cur["sma25_calc"]),
                "sma50": float(cur["sma50_calc"]),
                "sma100": float(cur["sma100_calc"]),
                "ma_state": str(cur.get("ma_state", "")),
            })

    return pd.DataFrame(rows).sort_values(["date", "event"]).reset_index(drop=True)


def first_event_between(
    events: pd.DataFrame,
    event_name: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.Series | None:
    match = events[
        (events["event"] == event_name)
        & (events["date"] > start)
        & (events["date"] <= end)
    ]
    if match.empty:
        return None
    return match.iloc[0]


def sequence_events(events: pd.DataFrame) -> pd.DataFrame:
    rows = []
    definitions = [
        {
            "sequence": "SEQ_A_25_ABOVE_50_THEN_25_ABOVE_100",
            "direction": "BULL",
            "start_event": "SMA25_CROSS_ABOVE_50",
            "confirm_event": "SMA25_CROSS_ABOVE_100",
            "invalidate_event": "SMA25_CROSS_BELOW_50",
        },
        {
            "sequence": "SEQ_A_25_BELOW_50_THEN_25_BELOW_100",
            "direction": "BEAR",
            "start_event": "SMA25_CROSS_BELOW_50",
            "confirm_event": "SMA25_CROSS_BELOW_100",
            "invalidate_event": "SMA25_CROSS_ABOVE_50",
        },
        {
            "sequence": "SEQ_B_25_ABOVE_50_THEN_50_ABOVE_100",
            "direction": "BULL",
            "start_event": "SMA25_CROSS_ABOVE_50",
            "confirm_event": "SMA50_CROSS_ABOVE_100",
            "invalidate_event": "SMA25_CROSS_BELOW_50",
        },
        {
            "sequence": "SEQ_B_25_BELOW_50_THEN_50_BELOW_100",
            "direction": "BEAR",
            "start_event": "SMA25_CROSS_BELOW_50",
            "confirm_event": "SMA50_CROSS_BELOW_100",
            "invalidate_event": "SMA25_CROSS_ABOVE_50",
        },
    ]

    for d in definitions:
        starts = events[events["event"] == d["start_event"]]
        for _, start_row in starts.iterrows():
            start = pd.Timestamp(start_row["date"])
            end = min(start + pd.Timedelta(days=SEQUENCE_MAX_DAYS), CUTOFF)
            confirm = first_event_between(events, d["confirm_event"], start, end)
            invalid = first_event_between(events, d["invalidate_event"], start, end)
            if confirm is None:
                continue
            confirm_date = pd.Timestamp(confirm["date"])
            if invalid is not None and pd.Timestamp(invalid["date"]) < confirm_date:
                continue
            rows.append({
                "date": confirm_date,
                "start_date": start,
                "sequence": d["sequence"],
                "direction": d["direction"],
                "confirmation_event": d["confirm_event"],
                "days_to_confirmation": int((confirm_date - start).days),
                "total_close_usd": float(confirm["total_close_usd"]),
                "sma25": float(confirm["sma25"]),
                "sma50": float(confirm["sma50"]),
                "sma100": float(confirm["sma100"]),
                "ma_state": str(confirm["ma_state"]),
            })

    if not rows:
        return pd.DataFrame(columns=[
            "date", "start_date", "sequence", "direction",
            "confirmation_event", "days_to_confirmation",
            "total_close_usd", "sma25", "sma50", "sma100", "ma_state"
        ])
    return pd.DataFrame(rows).sort_values(["date", "sequence"]).reset_index(drop=True)


def build_u10_equities() -> tuple[pd.DatetimeIndex, dict[str, np.ndarray], dict]:
    core.POOL = U10
    panel, data_meta = core.download_panel(CUTOFF + pd.Timedelta(days=1))
    timestamps, opens, closes = core.prepare_arrays(panel)
    sig_pos, sig_candidates = core.build_signal_index(panel)
    start_i, end_i = core.bounds(timestamps, MATURE_START, CUTOFF)
    eval_ts = timestamps[start_i:end_i + 1]
    equities: dict[str, np.ndarray] = {}
    for asset in U10:
        result = core.simulate_one(
            opens, closes, sig_pos, sig_candidates,
            U10, start_i, end_i, asset, collect_equity=True
        )
        eq = np.asarray(result["equity"], dtype=float)
        if len(eq) != len(eval_ts):
            raise RuntimeError(f"Equity mismatch {asset}")
        equities[asset] = eq
    return eval_ts, equities, data_meta


def idx_on_or_before(ts: pd.DatetimeIndex, when: pd.Timestamp) -> int:
    return int(ts.searchsorted(when, side="right") - 1)


def forward_return(
    ts: pd.DatetimeIndex,
    values: np.ndarray,
    event_date: pd.Timestamp,
    days: int,
) -> float:
    i = idx_on_or_before(ts, event_date)
    target = event_date + pd.Timedelta(days=days)
    j = idx_on_or_before(ts, target)
    if i < 0 or j <= i or j >= len(values):
        return np.nan
    if ts[j] < target - pd.Timedelta(days=2):
        return np.nan
    return float(values[j] / values[i] - 1.0)


def total_forward_return(
    total: pd.DataFrame,
    event_date: pd.Timestamp,
    days: int,
) -> float:
    ts = pd.DatetimeIndex(total["timestamp"])
    values = total["close"].to_numpy(float)
    return forward_return(ts, values, event_date, days)


def event_horizon_detail(
    total: pd.DataFrame,
    crosses: pd.DataFrame,
    sequences: pd.DataFrame,
    u10_ts: pd.DatetimeIndex,
    equities: dict[str, np.ndarray],
) -> pd.DataFrame:
    event_rows = []
    for _, row in crosses.iterrows():
        event_rows.append({
            "date": pd.Timestamp(row["date"]),
            "signal": str(row["event"]),
            "signal_class": "CROSS",
            "direction": str(row["direction"]),
            "days_to_confirmation": 0,
            "ma_state": str(row["ma_state"]),
        })
    for _, row in sequences.iterrows():
        event_rows.append({
            "date": pd.Timestamp(row["date"]),
            "signal": str(row["sequence"]),
            "signal_class": "SEQUENCE",
            "direction": str(row["direction"]),
            "days_to_confirmation": int(row["days_to_confirmation"]),
            "ma_state": str(row["ma_state"]),
        })

    rows = []
    for e in event_rows:
        for horizon in HORIZONS:
            vals = []
            for asset, equity in equities.items():
                ret = forward_return(u10_ts, equity, e["date"], horizon)
                if np.isfinite(ret):
                    vals.append((asset, ret))
            total_ret = total_forward_return(total, e["date"], horizon)
            if not vals or not np.isfinite(total_ret):
                continue
            arr = np.asarray([x[1] for x in vals], dtype=float)
            med = float(np.median(arr))
            rows.append({
                **e,
                "horizon_days": horizon,
                "covered_start_assets": len(vals),
                "u10_median_return": med,
                "u10_min_start_return": float(np.min(arr)),
                "u10_max_start_return": float(np.max(arr)),
                "u10_100usdt_median_capital": float(100.0 * (1.0 + med)),
                "total_forward_return": float(total_ret),
                "total_100usdt_equivalent": float(100.0 * (1.0 + total_ret)),
            })
    return pd.DataFrame(rows)


def event_summary(detail: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (signal, direction, horizon), g in detail.groupby(
        ["signal", "direction", "horizon_days"], sort=False
    ):
        arr = g["u10_median_return"].to_numpy(float)
        rows.append({
            "signal": signal,
            "direction": direction,
            "horizon_days": int(horizon),
            "event_count": int(len(g)),
            "median_u10_return": float(np.median(arr)),
            "worst_event_u10_return": float(np.min(arr)),
            "best_event_u10_return": float(np.max(arr)),
            "positive_event_rate": float(np.mean(arr > 0)),
            "median_100usdt_capital": float(100.0 * (1.0 + np.median(arr))),
            "median_total_return": float(np.median(g["total_forward_return"].to_numpy(float))),
            "median_days_to_confirmation": float(np.median(g["days_to_confirmation"].to_numpy(float))),
        })
    return pd.DataFrame(rows)


def pct(x: float) -> str:
    return f"{100.0 * float(x):+.1f}%"


def report_summary_table(summary: pd.DataFrame) -> str:
    lines = [
        "|Signal|Dir|Horizon|Events|U10 median|100 USDT ->|Worst|Best|Positive events|TOTAL median|Confirm lag|",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, r in summary.iterrows():
        lines.append(
            f"|{r['signal']}|{r['direction']}|{int(r['horizon_days'])}d|"
            f"{int(r['event_count'])}|{pct(r['median_u10_return'])}|"
            f"{r['median_100usdt_capital']:.2f}|"
            f"{pct(r['worst_event_u10_return'])}|"
            f"{pct(r['best_event_u10_return'])}|"
            f"{100*r['positive_event_rate']:.1f}%|"
            f"{pct(r['median_total_return'])}|"
            f"{r['median_days_to_confirmation']:.0f}d|"
        )
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff", default="2026-09-26T00:00:00Z")
    args = parser.parse_args()
    cutoff = core.utc(args.cutoff)
    if cutoff.normalize() != CUTOFF:
        raise RuntimeError(f"Expected cutoff {CUTOFF}, got {cutoff}")

    total = load_total()
    crosses = cross_events(total)
    sequences = sequence_events(crosses)
    u10_ts, equities, data_meta = build_u10_equities()
    detail = event_horizon_detail(total, crosses, sequences, u10_ts, equities)
    summary = event_summary(detail)

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)
    crosses.to_csv(run_dir / "cross_events.csv", index=False)
    sequences.to_csv(run_dir / "sequence_events.csv", index=False)
    detail.to_csv(run_dir / "event_horizon_detail.csv", index=False)
    summary.to_csv(run_dir / "event_summary.csv", index=False)

    meta = {
        "experiment": "RR_U10_TOTAL_SMA25_50_100_CROSS_V1",
        "workflow_mode": "STRESS_TEST_ONLY",
        "source_commit_sha": source_sha(),
        "cutoff": CUTOFF.isoformat(),
        "total_cache": {
            "path": str(CACHE.relative_to(ROOT)),
            "sha256": CACHE_SHA256,
            "rows": int(len(total)),
            "start": total.iloc[0]["timestamp"].isoformat(),
            "end": total.iloc[-1]["timestamp"].isoformat(),
        },
        "frozen_universe_id": "RR_TARGET_U10_CANDIDATE_HBAR_V1",
        "assets": list(U10),
        "horizons_days": list(HORIZONS),
        "sequence_max_days": SEQUENCE_MAX_DAYS,
        "cross_event_count": int(len(crosses)),
        "sequence_event_count": int(len(sequences)),
        "data_metadata": data_meta,
        "production_changes": "NONE",
        "test_level": "GITHUB_ACTIONS_REPOSITORY_CACHE_STRESS_TEST",
    }
    (run_dir / "summary.json").write_text(
        json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8"
    )

    report = [
        "# RR U10 TOTAL SMA25/50/100 CROSS EVENT STUDY V1",
        "",
        "Mode: STRESS_TEST_ONLY",
        "",
        f"Cross events: {len(crosses)}",
        f"Completed sequences: {len(sequences)}",
        "",
        "## Event summary",
        "",
        report_summary_table(summary),
        "",
        "## Sequence events",
        "",
        "|Date|Sequence|Direction|Start date|Days to confirmation|TOTAL|SMA25|SMA50|SMA100|State|",
        "|---|---|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for _, r in sequences.iterrows():
        report.append(
            f"|{pd.Timestamp(r['date']).date()}|{r['sequence']}|{r['direction']}|"
            f"{pd.Timestamp(r['start_date']).date()}|{int(r['days_to_confirmation'])}|"
            f"{r['total_close_usd']/1e12:.2f}T|{r['sma25']/1e12:.2f}T|"
            f"{r['sma50']/1e12:.2f}T|{r['sma100']/1e12:.2f}T|{r['ma_state']}|"
        )

    report += [
        "",
        "## Interpretation boundary",
        "",
        "- 100 USDT is a normalization of U10 equity at each event, not a claim about future money.",
        "- Small event counts must be treated as exploratory evidence.",
        "- Crosses are end-of-day market-cap signals; no production execution rule is created.",
        "- Market-cap data came only from the repository cache; no TradingView refresh occurred.",
        "- No live/paper/Telegram/exchange behavior changed.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_REPOSITORY_CACHE_STRESS_TEST",
        "",
    ]
    text = "\n".join(report)
    (run_dir / "report.md").write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
