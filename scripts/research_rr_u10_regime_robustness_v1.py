from __future__ import annotations

import argparse
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
OUT = ROOT / "research_artifacts" / "rr_u10_regime_robustness_v1"

U10 = (
    "TWT", "PEPE", "BNB", "TRX", "AAVE",
    "AVAX", "FIL", "ALGO", "XRP", "HBAR",
)
MATURE_START = pd.Timestamp("2023-10-31", tz="UTC")
EVAL_END = pd.Timestamp("2026-09-26", tz="UTC")
BTC_WARMUP_START = pd.Timestamp("2022-01-01", tz="UTC")


def source_sha() -> str:
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def download_btc(cutoff):
    client = BinanceSpotRestClient()
    result = download_historical_dataset(
        client,
        symbol="BTCUSDT",
        start=BTC_WARMUP_START,
        end=cutoff,
        timeframe="1D",
        as_of=cutoff,
    )
    if result.dataset is None:
        raise RuntimeError(f"BTCUSDT: {result.metadata.status}")
    if result.dataset.quality.has_critical_issues:
        raise RuntimeError("BTCUSDT: critical data quality issues")
    frame = result.dataset.candles[["timestamp", "close"]].copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame["btc_close"] = frame["close"].astype(float)
    frame = frame.drop(columns=["close"]).sort_values("timestamp").reset_index(drop=True)
    return frame, {
        "rows": int(len(frame)),
        "start": pd.Timestamp(frame.iloc[0]["timestamp"]).isoformat(),
        "end": pd.Timestamp(frame.iloc[-1]["timestamp"]).isoformat(),
    }


def build_regimes(panel, btc):
    strategy_ts = pd.DatetimeIndex(pd.to_datetime(panel["timestamp"], utc=True))

    b = btc.set_index("timestamp").copy()
    b["btc_sma50"] = b["btc_close"].rolling(50).mean()
    b["btc_sma200"] = b["btc_close"].rolling(200).mean()
    b["btc_trend"] = "UNKNOWN"
    ready = b[["btc_close", "btc_sma50", "btc_sma200"]].notna().all(axis=1)
    bull = ready & (b["btc_close"] > b["btc_sma200"]) & (b["btc_sma50"] > b["btc_sma200"])
    bear = ready & (b["btc_close"] < b["btc_sma200"]) & (b["btc_sma50"] < b["btc_sma200"])
    b.loc[bull, "btc_trend"] = "BTC_BULL"
    b.loc[bear, "btc_trend"] = "BTC_BEAR"
    b.loc[ready & ~(bull | bear), "btc_trend"] = "BTC_TRANSITION"
    b["btc_daily_return"] = b["btc_close"].pct_change(fill_method=None)
    b = b.reindex(strategy_ts)

    close_frame = pd.DataFrame(
        {asset: panel[f"{asset}_close"].astype(float).to_numpy() for asset in U10},
        index=strategy_ts,
    )
    sma50 = close_frame.rolling(50).mean()
    eligible = close_frame.notna() & sma50.notna()
    eligible_count = eligible.sum(axis=1)
    above_count = ((close_frame > sma50) & eligible).sum(axis=1)
    breadth = (above_count / eligible_count).where(eligible_count >= 5)

    broad = pd.Series("UNKNOWN", index=strategy_ts, dtype="object")
    broad.loc[breadth >= 0.60] = "BROAD_BULL"
    broad.loc[breadth <= 0.40] = "BROAD_BEAR"
    broad.loc[breadth.notna() & (breadth > 0.40) & (breadth < 0.60)] = "BROAD_SIDEWAYS"

    token_returns = close_frame.pct_change(fill_method=None)
    u10_market_daily = token_returns.median(axis=1, skipna=True)

    out = pd.DataFrame({
        "timestamp": strategy_ts,
        "btc_close": b["btc_close"].to_numpy(),
        "btc_sma50": b["btc_sma50"].to_numpy(),
        "btc_sma200": b["btc_sma200"].to_numpy(),
        "btc_trend": b["btc_trend"].fillna("UNKNOWN").to_numpy(),
        "btc_daily_return": b["btc_daily_return"].to_numpy(),
        "u10_breadth": breadth.to_numpy(),
        "broad_crypto_trend": broad.to_numpy(),
        "u10_market_daily_return": u10_market_daily.to_numpy(),
    })
    return out


def conditioned_metrics(daily_returns, mask):
    mask = np.asarray(mask, dtype=bool)
    values = np.asarray(daily_returns, dtype=float)
    selected = values[mask]
    selected = selected[np.isfinite(selected)]
    if len(selected) == 0:
        return {
            "day_count": 0,
            "conditioned_return": np.nan,
            "conditioned_dd": np.nan,
            "avg_daily_return": np.nan,
            "median_daily_return": np.nan,
            "positive_day_rate": np.nan,
        }
    conditioned = np.where(mask, values, 0.0)
    conditioned = np.where(np.isfinite(conditioned), conditioned, 0.0)
    curve = np.cumprod(1.0 + conditioned)
    peak = np.maximum.accumulate(curve)
    dd = float(np.min(curve / peak - 1.0))
    return {
        "day_count": int(len(selected)),
        "conditioned_return": float(np.prod(1.0 + selected) - 1.0),
        "conditioned_dd": dd,
        "avg_daily_return": float(np.mean(selected)),
        "median_daily_return": float(np.median(selected)),
        "positive_day_rate": float(np.mean(selected > 0)),
    }


def episode_metrics(daily_returns, labels, target):
    returns = np.asarray(daily_returns, dtype=float)
    labels = np.asarray(labels, dtype=object)
    episodes = []
    lengths = []
    i = 0
    while i < len(labels):
        if labels[i] != target:
            i += 1
            continue
        j = i
        while j + 1 < len(labels) and labels[j + 1] == target:
            j += 1
        vals = returns[i:j + 1]
        vals = vals[np.isfinite(vals)]
        if len(vals):
            episodes.append(float(np.prod(1.0 + vals) - 1.0))
            lengths.append(int(j - i + 1))
        i = j + 1

    if not episodes:
        return {
            "episode_count": 0,
            "median_episode_return": np.nan,
            "worst_episode_return": np.nan,
            "best_episode_return": np.nan,
            "positive_episode_rate": np.nan,
            "longest_episode_days": 0,
        }
    arr = np.asarray(episodes, dtype=float)
    return {
        "episode_count": int(len(arr)),
        "median_episode_return": float(np.median(arr)),
        "worst_episode_return": float(np.min(arr)),
        "best_episode_return": float(np.max(arr)),
        "positive_episode_rate": float(np.mean(arr > 0)),
        "longest_episode_days": int(max(lengths)),
    }


def benchmark_conditioned(daily_returns, mask):
    vals = np.asarray(daily_returns, dtype=float)[np.asarray(mask, dtype=bool)]
    vals = vals[np.isfinite(vals)]
    if not len(vals):
        return np.nan
    return float(np.prod(1.0 + vals) - 1.0)


def aggregate_start_rows(frame):
    rows = []
    group_cols = ["dimension", "regime"]
    metric_cols = [
        "conditioned_return",
        "conditioned_dd",
        "avg_daily_return",
        "median_daily_return",
        "positive_day_rate",
        "median_episode_return",
        "worst_episode_return",
        "best_episode_return",
        "positive_episode_rate",
    ]
    for keys, group in frame.groupby(group_cols, sort=False):
        row = {
            "dimension": keys[0],
            "regime": keys[1],
            "start_count": int(len(group)),
            "day_count": int(group["day_count"].iloc[0]),
            "episode_count": int(group["episode_count"].iloc[0]),
            "longest_episode_days": int(group["longest_episode_days"].max()),
            "btc_conditioned_return": float(group["btc_conditioned_return"].iloc[0]),
            "u10_market_conditioned_return": float(group["u10_market_conditioned_return"].iloc[0]),
        }
        for col in metric_cols:
            row[f"median_start_{col}"] = float(group[col].median())
            row[f"worst_start_{col}"] = float(group[col].min())
            row[f"best_start_{col}"] = float(group[col].max())
        row["positive_start_rate"] = float((group["conditioned_return"] > 0).mean())
        rows.append(row)
    return pd.DataFrame(rows)


def yearly_summary(daily_by_start):
    rows = []
    for (dimension, regime, year), group in daily_by_start.groupby(
        ["dimension", "regime", "year"], sort=False
    ):
        start_returns = []
        day_count = None
        for _, sg in group.groupby("start_asset"):
            vals = sg.loc[sg["is_regime"], "strategy_daily_return"].dropna().astype(float)
            day_count = int(len(vals))
            if len(vals):
                start_returns.append(float(np.prod(1.0 + vals.to_numpy()) - 1.0))
        if start_returns:
            arr = np.asarray(start_returns, dtype=float)
            rows.append({
                "dimension": dimension,
                "regime": regime,
                "year": int(year),
                "day_count": int(day_count or 0),
                "median_start_return": float(np.median(arr)),
                "worst_start_return": float(np.min(arr)),
                "best_start_return": float(np.max(arr)),
                "positive_start_rate": float(np.mean(arr > 0)),
            })
    return pd.DataFrame(rows)


def pct(x):
    return "n/a" if pd.isna(x) else f"{100.0 * float(x):+.1f}%"


def report_table(aggregate, dimension):
    sub = aggregate[aggregate["dimension"] == dimension]
    lines = [
        "|regime|days|episodes|strategy median|strategy worst start|positive starts|conditioned DD|median episode|positive episodes|BTC same days|U10 market same days|",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in sub.iterrows():
        lines.append(
            f"|{row['regime']}|{int(row['day_count'])}|{int(row['episode_count'])}|"
            f"{pct(row['median_start_conditioned_return'])}|"
            f"{pct(row['worst_start_conditioned_return'])}|"
            f"{100*row['positive_start_rate']:.1f}%|"
            f"{pct(row['median_start_conditioned_dd'])}|"
            f"{pct(row['median_start_median_episode_return'])}|"
            f"{100*row['median_start_positive_episode_rate']:.1f}%|"
            f"{pct(row['btc_conditioned_return'])}|"
            f"{pct(row['u10_market_conditioned_return'])}|"
        )
    return "\n".join(lines)


def classify(aggregate):
    lookup = {
        (row["dimension"], row["regime"]): row
        for _, row in aggregate.iterrows()
    }
    bear_rows = [
        lookup.get(("btc_trend", "BTC_BEAR")),
        lookup.get(("broad_crypto_trend", "BROAD_BEAR")),
    ]
    bear_rows = [x for x in bear_rows if x is not None]
    if len(bear_rows) < 2:
        return "MIXED_REGIME"
    robust = all(
        float(r["median_start_conditioned_return"]) > 0
        and float(r["positive_start_rate"]) >= 0.8
        for r in bear_rows
    )
    dependent = all(
        float(r["median_start_conditioned_return"]) < 0
        for r in bear_rows
    )
    if robust:
        return "REGIME_ROBUST_HISTORICALLY"
    if dependent:
        return "BULL_DEPENDENT"
    return "MIXED_REGIME"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff", default="2026-09-27T00:00:00Z")
    args = parser.parse_args()

    core.POOL = U10
    panel, data_meta = core.download_panel(core.utc(args.cutoff))
    timestamps, opens, closes = core.prepare_arrays(panel)
    signal_positions, signal_candidates = core.build_signal_index(panel)
    btc, btc_meta = download_btc(core.utc(args.cutoff))
    regimes = build_regimes(panel, btc)

    start_i, end_i = core.bounds(timestamps, MATURE_START, EVAL_END)
    local_regimes = regimes.iloc[start_i:end_i + 1].reset_index(drop=True)
    local_ts = pd.DatetimeIndex(pd.to_datetime(local_regimes["timestamp"], utc=True))

    start_rows = []
    daily_rows = []

    dimensions = {
        "btc_trend": ("BTC_BULL", "BTC_BEAR", "BTC_TRANSITION"),
        "broad_crypto_trend": ("BROAD_BULL", "BROAD_BEAR", "BROAD_SIDEWAYS"),
    }

    for start_asset in U10:
        result = core.simulate_one(
            opens,
            closes,
            signal_positions,
            signal_candidates,
            U10,
            start_i,
            end_i,
            start_asset,
            collect_equity=True,
        )
        equity = np.asarray(result["equity"], dtype=float)
        daily = np.zeros(len(equity), dtype=float)
        if len(equity) > 1:
            daily[1:] = equity[1:] / equity[:-1] - 1.0

        for dimension, labels in dimensions.items():
            label_values = local_regimes[dimension].astype(str).to_numpy()
            for regime in labels:
                mask = label_values == regime
                cm = conditioned_metrics(daily, mask)
                em = episode_metrics(daily, label_values, regime)
                row = {
                    "start_asset": start_asset,
                    "dimension": dimension,
                    "regime": regime,
                    **cm,
                    **em,
                    "btc_conditioned_return": benchmark_conditioned(
                        local_regimes["btc_daily_return"].to_numpy(), mask
                    ),
                    "u10_market_conditioned_return": benchmark_conditioned(
                        local_regimes["u10_market_daily_return"].to_numpy(), mask
                    ),
                }
                start_rows.append(row)

                for idx in range(len(daily)):
                    daily_rows.append({
                        "timestamp": local_ts[idx].isoformat(),
                        "year": int(local_ts[idx].year),
                        "start_asset": start_asset,
                        "dimension": dimension,
                        "regime": regime,
                        "is_regime": bool(mask[idx]),
                        "strategy_daily_return": float(daily[idx]),
                    })

    start_df = pd.DataFrame(start_rows)
    aggregate = aggregate_start_rows(start_df)
    daily_df = pd.DataFrame(daily_rows)
    yearly = yearly_summary(daily_df)

    coverage_rows = []
    for dimension in dimensions:
        counts = local_regimes[dimension].value_counts(dropna=False)
        for regime, count in counts.items():
            coverage_rows.append({
                "dimension": dimension,
                "regime": str(regime),
                "day_count": int(count),
                "share": float(count / len(local_regimes)),
            })
    coverage = pd.DataFrame(coverage_rows)

    classification = classify(aggregate)

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)
    start_df.to_csv(run_dir / "regime_by_start.csv", index=False)
    aggregate.to_csv(run_dir / "regime_aggregate.csv", index=False)
    yearly.to_csv(run_dir / "regime_by_year.csv", index=False)
    coverage.to_csv(run_dir / "regime_coverage.csv", index=False)

    summary = {
        "experiment": "RR_U10_REGIME_ROBUSTNESS_V1",
        "workflow_mode": "STRESS_TEST_ONLY",
        "source_commit_sha": source_sha(),
        "cutoff": core.utc(args.cutoff).isoformat(),
        "frozen_universe_id": "RR_TARGET_U10_CANDIDATE_HBAR_V1",
        "assets": list(U10),
        "mature_window": {
            "start": MATURE_START.isoformat(),
            "end": EVAL_END.isoformat(),
            "rows": int(len(local_regimes)),
        },
        "regime_definition": {
            "btc_trend": "H8 SMA50/SMA200",
            "broad_crypto_trend": "H8-style U10 breadth vs 50D SMA; bull>=0.60 bear<=0.40",
        },
        "classification": classification,
        "data_metadata": data_meta,
        "btc_metadata": btc_meta,
        "aggregate": json.loads(aggregate.to_json(orient="records")),
        "yearly": json.loads(yearly.to_json(orient="records")),
        "production_changes": "NONE",
        "test_level": "GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    }
    (run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    report = [
        "# RR U10 REGIME ROBUSTNESS V1",
        "",
        "Mode: STRESS_TEST_ONLY",
        "",
        f"Classification: {classification}",
        "",
        "Frozen universe: RR_TARGET_U10_CANDIDATE_HBAR_V1",
        "",
        "## BTC trend (H8 SMA50/SMA200)",
        "",
        report_table(aggregate, "btc_trend"),
        "",
        "## Broad U10 trend (H8 breadth vs 50D SMA)",
        "",
        report_table(aggregate, "broad_crypto_trend"),
        "",
        "## Regime coverage",
        "",
        "|dimension|regime|days|share|",
        "|---|---|---:|---:|",
    ]
    for _, row in coverage.iterrows():
        report.append(
            f"|{row['dimension']}|{row['regime']}|{int(row['day_count'])}|"
            f"{100*row['share']:.1f}%|"
        )

    report += [
        "",
        "## Safety",
        "",
        "- This is historical regime attribution, not a future-return forecast.",
        "- The single concentrated U10 strategy is tested; multibook overlays are excluded.",
        "- Regime definitions were frozen from existing H8 rules before runtime inspection.",
        "- No production/paper-live/Telegram/exchange behavior changed.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
        "",
    ]
    text_report = "\n".join(report)
    (run_dir / "report.md").write_text(text_report, encoding="utf-8")
    print(text_report)


if __name__ == "__main__":
    main()
