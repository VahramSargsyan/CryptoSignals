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
OUT = ROOT / "research_artifacts" / "rr_u10_btceth_ratio_sma_cross_v1"

U10 = (
    "TWT", "PEPE", "BNB", "TRX", "AAVE",
    "AVAX", "FIL", "ALGO", "XRP", "HBAR",
)
START = pd.Timestamp("2023-10-31", tz="UTC")
CUTOFF = pd.Timestamp("2026-09-26", tz="UTC")
WARMUP = pd.Timestamp("2023-01-01", tz="UTC")
SMAS = (25, 50, 100, 200)
HORIZONS = (7, 14, 30, 60, 90)


def source_sha() -> str:
    return os.environ.get("SOURCE_COMMIT_SHA") or subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def download_symbol(symbol: str, cutoff: pd.Timestamp) -> pd.DataFrame:
    result = download_historical_dataset(
        BinanceSpotRestClient(),
        symbol=symbol,
        start=WARMUP,
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
    return frame


def build_ratio(btc: pd.DataFrame, eth: pd.DataFrame) -> pd.DataFrame:
    b = btc.rename(columns={"close": "btc_close"})
    e = eth.rename(columns={"close": "eth_close"})
    frame = b.merge(e, on="timestamp", how="inner")
    frame["btc_eth_ratio"] = frame["btc_close"] / frame["eth_close"]
    for n in SMAS:
        frame[f"sma{n}"] = frame["btc_eth_ratio"].rolling(
            n, min_periods=n
        ).mean()
    return frame


def build_crosses(frame: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for i in range(1, len(frame)):
        cur = frame.iloc[i]
        prev = frame.iloc[i - 1]
        date = pd.Timestamp(cur["timestamp"])
        if date < START or date > CUTOFF:
            continue

        for n in SMAS:
            p_ratio = float(prev["btc_eth_ratio"])
            c_ratio = float(cur["btc_eth_ratio"])
            p_sma = prev[f"sma{n}"]
            c_sma = cur[f"sma{n}"]
            if pd.isna(p_sma) or pd.isna(c_sma):
                continue

            event = None
            direction = None
            crossed_side = None
            if p_ratio >= float(p_sma) and c_ratio < float(c_sma):
                event = f"BTC_ETH_CROSS_BELOW_SMA{n}"
                direction = "ETH_STRENGTH"
                crossed_side = "below"
            elif p_ratio <= float(p_sma) and c_ratio > float(c_sma):
                event = f"BTC_ETH_CROSS_ABOVE_SMA{n}"
                direction = "BTC_STRENGTH"
                crossed_side = "above"

            if event is None:
                continue

            persistent = False
            if i + 3 < len(frame):
                future = frame.iloc[i + 1 : i + 4]
                if crossed_side == "below":
                    persistent = bool(
                        (
                            future["btc_eth_ratio"]
                            < future[f"sma{n}"]
                        ).all()
                    )
                else:
                    persistent = bool(
                        (
                            future["btc_eth_ratio"]
                            > future[f"sma{n}"]
                        ).all()
                    )

            rows.append(
                {
                    "date": date,
                    "event": event,
                    "direction": direction,
                    "sma_period": n,
                    "persistent_3d": persistent,
                    "btc_eth_ratio": c_ratio,
                    "sma": float(c_sma),
                    "btc_close": float(cur["btc_close"]),
                    "eth_close": float(cur["eth_close"]),
                }
            )

    return pd.DataFrame(rows).sort_values(
        ["date", "sma_period", "event"]
    ).reset_index(drop=True)


def build_u10_equities():
    core.POOL = U10
    panel, data_meta = core.download_panel(
        CUTOFF + pd.Timedelta(days=1)
    )
    timestamps, opens, closes = core.prepare_arrays(panel)
    sig_pos, sig_candidates = core.build_signal_index(panel)
    start_i, end_i = core.bounds(timestamps, START, CUTOFF)
    eval_ts = timestamps[start_i : end_i + 1]
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
            raise RuntimeError(f"U10 equity mismatch for {asset}")
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


def build_detail(
    ratio: pd.DataFrame,
    crosses: pd.DataFrame,
    u10_ts: pd.DatetimeIndex,
    equities: dict[str, np.ndarray],
) -> pd.DataFrame:
    ratio_ts = pd.DatetimeIndex(ratio["timestamp"])
    btc_values = ratio["btc_close"].to_numpy(float)
    eth_values = ratio["eth_close"].to_numpy(float)
    ratio_values = ratio["btc_eth_ratio"].to_numpy(float)

    rows = []
    for _, event in crosses.iterrows():
        date = pd.Timestamp(event["date"])
        for horizon in HORIZONS:
            u10_returns = []
            for asset, equity in equities.items():
                value = forward_return(
                    u10_ts, equity, date, horizon
                )
                if np.isfinite(value):
                    u10_returns.append(value)

            btc_ret = forward_return(
                ratio_ts, btc_values, date, horizon
            )
            eth_ret = forward_return(
                ratio_ts, eth_values, date, horizon
            )
            ratio_ret = forward_return(
                ratio_ts, ratio_values, date, horizon
            )

            if (
                not u10_returns
                or not np.isfinite(btc_ret)
                or not np.isfinite(eth_ret)
                or not np.isfinite(ratio_ret)
            ):
                continue

            arr = np.asarray(u10_returns, dtype=float)
            med = float(np.median(arr))
            rows.append(
                {
                    "date": date,
                    "event": event["event"],
                    "direction": event["direction"],
                    "sma_period": int(event["sma_period"]),
                    "persistent_3d": bool(event["persistent_3d"]),
                    "horizon_days": horizon,
                    "covered_start_assets": len(arr),
                    "u10_median_return": med,
                    "u10_min_start_return": float(np.min(arr)),
                    "u10_max_start_return": float(np.max(arr)),
                    "u10_100usdt_median_capital": float(
                        100.0 * (1.0 + med)
                    ),
                    "btc_return": float(btc_ret),
                    "eth_return": float(eth_ret),
                    "btc_eth_ratio_return": float(ratio_ret),
                }
            )
    return pd.DataFrame(rows)


def summarize(detail: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for subset_name, subset in [
        ("RAW", detail),
        ("PERSISTENT_3D", detail[detail["persistent_3d"]]),
    ]:
        for (
            direction,
            sma_period,
            horizon,
        ), group in subset.groupby(
            ["direction", "sma_period", "horizon_days"],
            sort=False,
        ):
            values = group["u10_median_return"].to_numpy(float)
            rows.append(
                {
                    "subset": subset_name,
                    "direction": direction,
                    "sma_period": int(sma_period),
                    "horizon_days": int(horizon),
                    "event_count": int(len(group)),
                    "median_u10_return": float(np.median(values)),
                    "worst_event_u10_return": float(np.min(values)),
                    "best_event_u10_return": float(np.max(values)),
                    "positive_event_rate": float(
                        np.mean(values > 0)
                    ),
                    "median_100usdt_capital": float(
                        100.0 * (1.0 + np.median(values))
                    ),
                    "median_btc_return": float(
                        np.median(group["btc_return"].to_numpy(float))
                    ),
                    "median_eth_return": float(
                        np.median(group["eth_return"].to_numpy(float))
                    ),
                    "median_ratio_return": float(
                        np.median(
                            group["btc_eth_ratio_return"].to_numpy(float)
                        )
                    ),
                }
            )
    return pd.DataFrame(rows)


def pct(x: float) -> str:
    return f"{100.0 * float(x):+.1f}%"


def summary_table(summary: pd.DataFrame, subset: str) -> str:
    frame = summary[summary["subset"] == subset].copy()
    lines = [
        "|Dir|SMA|Horizon|Events|U10 median|100 USDT ->|Positive|BTC|ETH|BTC/ETH|",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in frame.iterrows():
        lines.append(
            f"|{row['direction']}|{int(row['sma_period'])}|"
            f"{int(row['horizon_days'])}d|{int(row['event_count'])}|"
            f"{pct(row['median_u10_return'])}|"
            f"{row['median_100usdt_capital']:.2f}|"
            f"{100*row['positive_event_rate']:.1f}%|"
            f"{pct(row['median_btc_return'])}|"
            f"{pct(row['median_eth_return'])}|"
            f"{pct(row['median_ratio_return'])}|"
        )
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cutoff",
        default="2026-09-26T00:00:00Z",
    )
    args = parser.parse_args()
    cutoff = core.utc(args.cutoff)
    if cutoff.normalize() != CUTOFF:
        raise RuntimeError(
            f"Expected cutoff {CUTOFF.date()}, got {cutoff.date()}"
        )

    btc = download_symbol("BTCUSDT", cutoff)
    eth = download_symbol("ETHUSDT", cutoff)
    ratio = build_ratio(btc, eth)
    crosses = build_crosses(ratio)
    u10_ts, equities, data_meta = build_u10_equities()
    detail = build_detail(ratio, crosses, u10_ts, equities)
    summary = summarize(detail)

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime(
        "%Y%m%dT%H%M%SZ"
    )
    run_dir.mkdir(parents=True, exist_ok=True)

    ratio.to_csv(run_dir / "btc_eth_daily.csv", index=False)
    crosses.to_csv(run_dir / "cross_events.csv", index=False)
    detail.to_csv(run_dir / "event_horizon_detail.csv", index=False)
    summary.to_csv(run_dir / "event_summary.csv", index=False)

    meta = {
        "experiment": "RR_U10_BTC_ETH_RATIO_SMA_CROSS_V1",
        "workflow_mode": "STRESS_TEST_ONLY",
        "source_commit_sha": source_sha(),
        "cutoff": CUTOFF.isoformat(),
        "ratio_definition": "BTCUSDT close / ETHUSDT close",
        "ratio_interpretation": (
            "ratio down = ETH outperforming BTC; "
            "ratio up = BTC outperforming ETH"
        ),
        "smas": list(SMAS),
        "horizons": list(HORIZONS),
        "cross_event_count": int(len(crosses)),
        "persistent_3d_count": int(
            crosses["persistent_3d"].sum()
        ),
        "frozen_universe_id": (
            "RR_TARGET_U10_CANDIDATE_HBAR_V1"
        ),
        "assets": list(U10),
        "data_metadata": data_meta,
        "production_changes": "NONE",
        "test_level": (
            "GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST"
        ),
    }
    (run_dir / "summary.json").write_text(
        json.dumps(meta, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    report = [
        "# RR U10 BTC/ETH RATIO SMA CROSS V1",
        "",
        "Mode: STRESS_TEST_ONLY",
        "",
        "Ratio: BTCUSDT / ETHUSDT",
        "",
        f"Cross events: {len(crosses)}",
        f"3-day persistent crosses: "
        f"{int(crosses['persistent_3d'].sum())}",
        "",
        "## Raw crosses",
        "",
        summary_table(summary, "RAW"),
        "",
        "## 3-day persistent crosses",
        "",
        summary_table(summary, "PERSISTENT_3D"),
        "",
        "## Interpretation boundary",
        "",
        "- BTC/ETH falling means ETH is outperforming BTC; it is not itself the formal definition of Altcoin Season.",
        "- 100 USDT values are normalized historical U10 equity, not future-return forecasts.",
        "- Small event counts must be treated as exploratory.",
        "- No live/paper/Telegram/exchange behavior changed.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
        "",
    ]
    text = "\n".join(report)
    (run_dir / "report.md").write_text(
        text,
        encoding="utf-8",
    )
    print(text)


if __name__ == "__main__":
    main()
