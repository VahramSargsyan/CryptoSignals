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
from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "rr_u10_combined_total_btceth_regime_v1"

TOTAL_CACHE = ROOT / "data" / "market" / "cryptocap_total_d1.csv"
TOTAL_CACHE_SHA256 = "d0c9b3bac915e484069c6302ddce1a0b050abbb8de155e4106e39cf6e519d3ee"

U10 = (
    "TWT", "PEPE", "BNB", "TRX", "AAVE",
    "AVAX", "FIL", "ALGO", "XRP", "HBAR",
)
START = pd.Timestamp("2023-10-31", tz="UTC")
CUTOFF = pd.Timestamp("2026-09-26", tz="UTC")
RATIO_WARMUP = pd.Timestamp("2023-01-01", tz="UTC")
HORIZONS = (7, 14, 30, 60, 90)


def source_sha() -> str:
    return os.environ.get("SOURCE_COMMIT_SHA") or subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        text=True,
    ).strip()


def classify_total_state(row: pd.Series) -> str:
    needed = [row["sma50_calc"], row["sma100_calc"], row["sma200_calc"], row["sma300_calc"]]
    if any(pd.isna(x) for x in needed):
        return "INSUFFICIENT_HISTORY"

    close = float(row["close"])
    s50 = float(row["sma50_calc"])
    s100 = float(row["sma100_calc"])
    s200 = float(row["sma200_calc"])
    s300 = float(row["sma300_calc"])

    if close > s50 > s100 > s200 > s300:
        return "FULL_BULL_ALIGNMENT"
    if close < s50 < s100 < s200 < s300:
        return "FULL_BEAR_ALIGNMENT"
    if close > s50 > s100:
        return "BULL_BUILDING"
    if close < s50 < s100:
        return "BEAR_BUILDING"
    return "MIXED"


def load_total_cache() -> pd.DataFrame:
    digest = hashlib.sha256(TOTAL_CACHE.read_bytes()).hexdigest()
    if digest != TOTAL_CACHE_SHA256:
        raise RuntimeError(
            f"TOTAL cache SHA mismatch: {digest} != {TOTAL_CACHE_SHA256}"
        )

    frame = pd.read_csv(TOTAL_CACHE)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame = (
        frame.dropna(subset=["timestamp", "close"])
        .drop_duplicates("timestamp", keep="last")
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    frame = frame[frame["timestamp"] <= CUTOFF].reset_index(drop=True)
    frame["date"] = frame["timestamp"].dt.normalize()

    for n in (25, 50, 100, 200, 300):
        frame[f"sma{n}_calc"] = frame["close"].astype(float).rolling(
            n, min_periods=n
        ).mean()

    # Verify cached SMA50/100/200/300 against recomputation.
    for n in (50, 100, 200, 300):
        if f"sma{n}" not in frame.columns:
            raise RuntimeError(f"Cached SMA{n} missing")
        valid = frame[[f"sma{n}", f"sma{n}_calc"]].dropna()
        diff = np.max(
            np.abs(
                valid[f"sma{n}"].to_numpy(float)
                - valid[f"sma{n}_calc"].to_numpy(float)
            )
        )
        scale = np.max(np.abs(valid[f"sma{n}"].to_numpy(float)))
        if diff > max(1.0, scale * 1e-10):
            raise RuntimeError(f"Cached SMA{n} mismatch: max diff={diff}")

    frame["total_state_calc"] = [
        classify_total_state(row) for _, row in frame.iterrows()
    ]

    if "ma_state" in frame.columns:
        check = frame[
            (frame["timestamp"] >= START)
            & (frame["ma_state"].notna())
        ][["ma_state", "total_state_calc"]]
        mismatch = check[check["ma_state"] != check["total_state_calc"]]
        if not mismatch.empty:
            raise RuntimeError(
                f"TOTAL cached ma_state mismatch count={len(mismatch)}"
            )

    frame["total_bull"] = frame["total_state_calc"].isin(
        ["BULL_BUILDING", "FULL_BULL_ALIGNMENT"]
    )
    frame["total_bear"] = frame["total_state_calc"].isin(
        ["BEAR_BUILDING", "FULL_BEAR_ALIGNMENT"]
    )
    return frame


def download_symbol(symbol: str, cutoff: pd.Timestamp) -> pd.DataFrame:
    result = download_historical_dataset(
        BinanceSpotRestClient(),
        symbol=symbol,
        start=RATIO_WARMUP,
        end=cutoff + pd.Timedelta(days=1),
        timeframe="1D",
        as_of=cutoff + pd.Timedelta(days=1),
    )
    if result.dataset is None:
        raise RuntimeError(f"{symbol}: {result.metadata.status}")
    if result.dataset.quality.has_critical_issues:
        raise RuntimeError(f"{symbol}: critical data quality issues")

    frame = result.dataset.candles[["timestamp", "close"]].copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame = (
        frame.dropna(subset=["timestamp", "close"])
        .drop_duplicates("timestamp", keep="last")
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    frame["date"] = frame["timestamp"].dt.normalize()
    return frame


def build_ratio(btc: pd.DataFrame, eth: pd.DataFrame) -> pd.DataFrame:
    b = btc[["date", "timestamp", "close"]].rename(
        columns={"timestamp": "btc_timestamp", "close": "btc_close"}
    )
    e = eth[["date", "timestamp", "close"]].rename(
        columns={"timestamp": "eth_timestamp", "close": "eth_close"}
    )
    frame = b.merge(e, on="date", how="inner")
    frame["btc_eth_ratio"] = frame["btc_close"] / frame["eth_close"]
    for n in (25, 50, 200):
        frame[f"ratio_sma{n}"] = frame["btc_eth_ratio"].rolling(
            n, min_periods=n
        ).mean()

    below25 = frame["btc_eth_ratio"] < frame["ratio_sma25"]
    below50 = frame["btc_eth_ratio"] < frame["ratio_sma50"]
    above200 = frame["btc_eth_ratio"] > frame["ratio_sma200"]

    frame["eth_fast_25"] = (
        below25
        & below25.shift(1, fill_value=False)
        & below25.shift(2, fill_value=False)
    )
    frame["eth_medium_50"] = (
        below50
        & below50.shift(1, fill_value=False)
        & below50.shift(2, fill_value=False)
    )
    frame["btc_long_200"] = (
        above200
        & above200.shift(1, fill_value=False)
        & above200.shift(2, fill_value=False)
    )
    return frame


def build_joint_state(total: pd.DataFrame, ratio: pd.DataFrame) -> pd.DataFrame:
    tcols = [
        "date",
        "timestamp",
        "close",
        "sma25_calc",
        "sma50_calc",
        "sma100_calc",
        "sma200_calc",
        "sma300_calc",
        "total_state_calc",
        "total_bull",
        "total_bear",
    ]
    rcols = [
        "date",
        "btc_close",
        "eth_close",
        "btc_eth_ratio",
        "ratio_sma25",
        "ratio_sma50",
        "ratio_sma200",
        "eth_fast_25",
        "eth_medium_50",
        "btc_long_200",
    ]
    frame = total[tcols].merge(ratio[rcols], on="date", how="inner")
    frame = frame[
        (frame["date"] >= START.normalize())
        & (frame["date"] <= CUTOFF.normalize())
    ].reset_index(drop=True)

    frame["combo_bull_25"] = frame["total_bull"] & frame["eth_fast_25"]
    frame["combo_bull_50"] = frame["total_bull"] & frame["eth_medium_50"]
    frame["combo_risk_200"] = frame["total_bear"] & frame["btc_long_200"]

    return frame


SIGNALS = {
    "TOTAL_BULL_ONLY": "total_bull",
    "TOTAL_BEAR_ONLY": "total_bear",
    "ETH_FAST_25_ONLY": "eth_fast_25",
    "ETH_MEDIUM_50_ONLY": "eth_medium_50",
    "BTC_LONG_200_ONLY": "btc_long_200",
    "COMBO_BULL_25": "combo_bull_25",
    "COMBO_BULL_50": "combo_bull_50",
    "COMBO_RISK_200": "combo_risk_200",
}


def build_signal_events(joint: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for signal, col in SIGNALS.items():
        values = joint[col].fillna(False).astype(bool)
        entries = values & ~values.shift(1, fill_value=False)
        for idx in np.flatnonzero(entries.to_numpy()):
            row = joint.iloc[int(idx)]
            rows.append(
                {
                    "date": pd.Timestamp(row["date"]),
                    "signal": signal,
                    "total_state": row["total_state_calc"],
                    "total_close_usd": float(row["close"]),
                    "btc_close": float(row["btc_close"]),
                    "eth_close": float(row["eth_close"]),
                    "btc_eth_ratio": float(row["btc_eth_ratio"]),
                    "ratio_sma25": float(row["ratio_sma25"])
                    if pd.notna(row["ratio_sma25"]) else np.nan,
                    "ratio_sma50": float(row["ratio_sma50"])
                    if pd.notna(row["ratio_sma50"]) else np.nan,
                    "ratio_sma200": float(row["ratio_sma200"])
                    if pd.notna(row["ratio_sma200"]) else np.nan,
                }
            )
    return pd.DataFrame(rows).sort_values(
        ["date", "signal"]
    ).reset_index(drop=True)


def build_u10_equities():
    core.POOL = U10
    panel, data_meta = core.download_panel(CUTOFF + pd.Timedelta(days=1))
    timestamps, opens, closes = core.prepare_arrays(panel)
    sig_pos, sig_candidates = core.build_signal_index(panel)
    start_i, end_i = core.bounds(timestamps, START, CUTOFF)
    eval_ts = timestamps[start_i:end_i + 1]
    equities = {}
    for asset in U10:
        result = core.simulate_one(
            opens,
            closes,
            sig_pos,
            sig_candidates,
            U10,
            start_i,
            end_i,
            asset,
            collect_equity=True,
        )
        equity = np.asarray(result["equity"], dtype=float)
        if len(equity) != len(eval_ts):
            raise RuntimeError(f"Equity mismatch for {asset}")
        equities[asset] = equity
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


def build_market_arrays(joint: pd.DataFrame):
    ts = pd.DatetimeIndex(pd.to_datetime(joint["date"], utc=True))
    arrays = {
        "total": joint["close"].to_numpy(float),
        "btc": joint["btc_close"].to_numpy(float),
        "eth": joint["eth_close"].to_numpy(float),
        "ratio": joint["btc_eth_ratio"].to_numpy(float),
    }
    return ts, arrays


def build_detail(
    joint: pd.DataFrame,
    events: pd.DataFrame,
    u10_ts: pd.DatetimeIndex,
    equities: dict[str, np.ndarray],
) -> pd.DataFrame:
    market_ts, arrays = build_market_arrays(joint)
    rows = []
    for _, event in events.iterrows():
        date = pd.Timestamp(event["date"])
        for horizon in HORIZONS:
            u10_values = []
            for asset, equity in equities.items():
                ret = forward_return(u10_ts, equity, date, horizon)
                if np.isfinite(ret):
                    u10_values.append(ret)

            market_returns = {
                key: forward_return(market_ts, values, date, horizon)
                for key, values in arrays.items()
            }
            if (
                not u10_values
                or any(not np.isfinite(x) for x in market_returns.values())
            ):
                continue

            arr = np.asarray(u10_values, dtype=float)
            med = float(np.median(arr))
            rows.append(
                {
                    "date": date,
                    "signal": event["signal"],
                    "total_state": event["total_state"],
                    "horizon_days": horizon,
                    "covered_start_assets": len(arr),
                    "u10_median_return": med,
                    "u10_min_start_return": float(np.min(arr)),
                    "u10_max_start_return": float(np.max(arr)),
                    "u10_100usdt_median_capital": float(
                        100.0 * (1.0 + med)
                    ),
                    "total_return": float(market_returns["total"]),
                    "btc_return": float(market_returns["btc"]),
                    "eth_return": float(market_returns["eth"]),
                    "btc_eth_ratio_return": float(market_returns["ratio"]),
                }
            )
    return pd.DataFrame(rows)


def summarize(detail: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (signal, horizon), group in detail.groupby(
        ["signal", "horizon_days"], sort=False
    ):
        values = group["u10_median_return"].to_numpy(float)
        rows.append(
            {
                "signal": signal,
                "horizon_days": int(horizon),
                "event_count": int(len(group)),
                "median_u10_return": float(np.median(values)),
                "worst_event_u10_return": float(np.min(values)),
                "best_event_u10_return": float(np.max(values)),
                "positive_event_rate": float(np.mean(values > 0)),
                "median_100usdt_capital": float(
                    100.0 * (1.0 + np.median(values))
                ),
                "median_total_return": float(
                    np.median(group["total_return"].to_numpy(float))
                ),
                "median_btc_return": float(
                    np.median(group["btc_return"].to_numpy(float))
                ),
                "median_eth_return": float(
                    np.median(group["eth_return"].to_numpy(float))
                ),
                "median_ratio_return": float(
                    np.median(group["btc_eth_ratio_return"].to_numpy(float))
                ),
            }
        )
    return pd.DataFrame(rows)


def build_lift(summary: pd.DataFrame) -> pd.DataFrame:
    comparisons = [
        ("COMBO_BULL_25", "TOTAL_BULL_ONLY"),
        ("COMBO_BULL_25", "ETH_FAST_25_ONLY"),
        ("COMBO_BULL_50", "TOTAL_BULL_ONLY"),
        ("COMBO_BULL_50", "ETH_MEDIUM_50_ONLY"),
        ("COMBO_RISK_200", "TOTAL_BEAR_ONLY"),
        ("COMBO_RISK_200", "BTC_LONG_200_ONLY"),
    ]
    rows = []
    keyed = {
        (row["signal"], int(row["horizon_days"])): row
        for _, row in summary.iterrows()
    }
    for combo, component in comparisons:
        for horizon in HORIZONS:
            a = keyed.get((combo, horizon))
            b = keyed.get((component, horizon))
            if a is None or b is None:
                continue
            rows.append(
                {
                    "combo_signal": combo,
                    "component_signal": component,
                    "horizon_days": horizon,
                    "combo_event_count": int(a["event_count"]),
                    "component_event_count": int(b["event_count"]),
                    "event_count_reduction": int(
                        b["event_count"] - a["event_count"]
                    ),
                    "median_u10_return_lift": float(
                        a["median_u10_return"] - b["median_u10_return"]
                    ),
                    "positive_event_rate_lift": float(
                        a["positive_event_rate"] - b["positive_event_rate"]
                    ),
                }
            )
    return pd.DataFrame(rows)


def current_state(joint: pd.DataFrame) -> dict:
    row = joint.iloc[-1]
    return {
        "date": pd.Timestamp(row["date"]).isoformat(),
        "total_state": str(row["total_state_calc"]),
        "total_close_usd": float(row["close"]),
        "btc_eth_ratio": float(row["btc_eth_ratio"]),
        "ratio_sma25": float(row["ratio_sma25"]),
        "ratio_sma50": float(row["ratio_sma50"]),
        "ratio_sma200": float(row["ratio_sma200"]),
        "eth_fast_25": bool(row["eth_fast_25"]),
        "eth_medium_50": bool(row["eth_medium_50"]),
        "btc_long_200": bool(row["btc_long_200"]),
        "combo_bull_25": bool(row["combo_bull_25"]),
        "combo_bull_50": bool(row["combo_bull_50"]),
        "combo_risk_200": bool(row["combo_risk_200"]),
    }


def pct(x: float) -> str:
    return f"{100.0 * float(x):+.1f}%"


def summary_table(summary: pd.DataFrame) -> str:
    order = list(SIGNALS.keys())
    rank = {name: i for i, name in enumerate(order)}
    frame = summary.copy()
    frame["signal_order"] = frame["signal"].map(rank)
    frame = frame.sort_values(["signal_order", "horizon_days"])
    lines = [
        "|Signal|Horizon|Events|U10 median|100 USDT ->|Positive|TOTAL|BTC|ETH|BTC/ETH|",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in frame.iterrows():
        lines.append(
            f"|{row['signal']}|{int(row['horizon_days'])}d|"
            f"{int(row['event_count'])}|{pct(row['median_u10_return'])}|"
            f"{row['median_100usdt_capital']:.2f}|"
            f"{100*row['positive_event_rate']:.1f}%|"
            f"{pct(row['median_total_return'])}|"
            f"{pct(row['median_btc_return'])}|"
            f"{pct(row['median_eth_return'])}|"
            f"{pct(row['median_ratio_return'])}|"
        )
    return "\n".join(lines)


def lift_table(lift: pd.DataFrame) -> str:
    lines = [
        "|Combo|Compared with|Horizon|Combo events|Component events|Return lift|Positive-rate lift|",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for _, row in lift.iterrows():
        lines.append(
            f"|{row['combo_signal']}|{row['component_signal']}|"
            f"{int(row['horizon_days'])}d|{int(row['combo_event_count'])}|"
            f"{int(row['component_event_count'])}|"
            f"{pct(row['median_u10_return_lift'])}|"
            f"{100*row['positive_event_rate_lift']:+.1f} pp|"
        )
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff", default="2026-09-26T00:00:00Z")
    args = parser.parse_args()
    cutoff = core.utc(args.cutoff)
    if cutoff.normalize() != CUTOFF:
        raise RuntimeError(
            f"Expected cutoff {CUTOFF.date()}, got {cutoff.date()}"
        )

    total = load_total_cache()
    btc = download_symbol("BTCUSDT", cutoff)
    eth = download_symbol("ETHUSDT", cutoff)
    ratio = build_ratio(btc, eth)
    joint = build_joint_state(total, ratio)
    events = build_signal_events(joint)
    u10_ts, equities, data_meta = build_u10_equities()
    detail = build_detail(joint, events, u10_ts, equities)
    summary = summarize(detail)
    lift = build_lift(summary)
    now = current_state(joint)

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    joint.to_csv(run_dir / "daily_joint_state.csv", index=False)
    events.to_csv(run_dir / "signal_events.csv", index=False)
    detail.to_csv(run_dir / "event_horizon_detail.csv", index=False)
    summary.to_csv(run_dir / "event_summary.csv", index=False)
    lift.to_csv(run_dir / "combination_lift.csv", index=False)

    meta = {
        "experiment": "RR_U10_COMBINED_TOTAL_BTCETH_REGIME_V1",
        "workflow_mode": "STRESS_TEST_ONLY",
        "source_commit_sha": source_sha(),
        "cutoff": CUTOFF.isoformat(),
        "total_cache": {
            "path": str(TOTAL_CACHE.relative_to(ROOT)),
            "sha256": TOTAL_CACHE_SHA256,
            "rows": int(len(total)),
            "start": total.iloc[0]["timestamp"].isoformat(),
            "end": total.iloc[-1]["timestamp"].isoformat(),
        },
        "frozen_universe_id": "RR_TARGET_U10_CANDIDATE_HBAR_V1",
        "assets": list(U10),
        "signal_event_count": int(len(events)),
        "current_state": now,
        "data_metadata": data_meta,
        "production_changes": "NONE",
        "test_level": (
            "GITHUB_ACTIONS_REPOSITORY_CACHE_PLUS_LIVE_PUBLIC_DATA_STRESS_TEST"
        ),
    }
    (run_dir / "summary.json").write_text(
        json.dumps(meta, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    report = [
        "# RR U10 COMBINED TOTAL + BTC/ETH REGIME V1",
        "",
        "Mode: STRESS_TEST_ONLY",
        "",
        "## Event summary",
        "",
        summary_table(summary),
        "",
        "## Combination lift",
        "",
        lift_table(lift),
        "",
        "## Current state at cutoff",
        "",
        f"- date: {now['date']}",
        f"- TOTAL state: {now['total_state']}",
        f"- BTC/ETH: {now['btc_eth_ratio']:.6f}",
        f"- BTC/ETH SMA25: {now['ratio_sma25']:.6f}",
        f"- BTC/ETH SMA50: {now['ratio_sma50']:.6f}",
        f"- BTC/ETH SMA200: {now['ratio_sma200']:.6f}",
        f"- ETH_FAST_25: {now['eth_fast_25']}",
        f"- ETH_MEDIUM_50: {now['eth_medium_50']}",
        f"- BTC_LONG_200: {now['btc_long_200']}",
        f"- COMBO_BULL_25: {now['combo_bull_25']}",
        f"- COMBO_BULL_50: {now['combo_bull_50']}",
        f"- COMBO_RISK_200: {now['combo_risk_200']}",
        "",
        "## Interpretation boundary",
        "",
        "- Combined conditions were preregistered from previously frozen component findings.",
        "- Event counts may shrink substantially after intersection; small samples must be stated explicitly.",
        "- 100 USDT is normalized historical U10 equity, not a future-return forecast.",
        "- TOTAL history came only from the repository cache; BTC/ETH uses Binance public D1 history.",
        "- No live/paper/Telegram/exchange behavior changed.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_REPOSITORY_CACHE_PLUS_LIVE_PUBLIC_DATA_STRESS_TEST",
        "",
    ]
    text = "\n".join(report)
    (run_dir / "report.md").write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
