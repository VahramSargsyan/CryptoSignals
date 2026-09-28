from __future__ import annotations

import argparse
import calendar
import json
import os
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd

import scripts.research_robust_exhaustive_u9_u10_atom_replacement_v1 as core
from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from tvDatafeed import Interval, TvDatafeed


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "rr_u10_monthly_total_marketcap_ma_v1"

U10 = (
    "TWT", "PEPE", "BNB", "TRX", "AAVE",
    "AVAX", "FIL", "ALGO", "XRP", "HBAR",
)
MATURE_START = pd.Timestamp("2023-10-31", tz="UTC")
CUTOFF = pd.Timestamp("2026-09-26", tz="UTC")
TV_MIN_ROWS = 1300
TV_BARS = 1400
TOTAL_CACHE_PATH = ROOT / "data" / "market" / "cryptocap_total_d1.csv"
TOTAL_CACHE_META_PATH = ROOT / "data" / "market" / "cryptocap_total_d1.meta.json"


def source_sha() -> str:
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def month_ends() -> list[pd.Timestamp]:
    rows: list[pd.Timestamp] = []
    cursor = pd.Timestamp("2023-10-01", tz="UTC")
    while cursor <= CUTOFF:
        if cursor.year == CUTOFF.year and cursor.month == CUTOFF.month:
            rows.append(CUTOFF)
            break
        day = calendar.monthrange(cursor.year, cursor.month)[1]
        rows.append(
            pd.Timestamp(
                year=cursor.year,
                month=cursor.month,
                day=day,
                tz="UTC",
            )
        )
        cursor = cursor + pd.offsets.MonthBegin(1)
    return rows


def load_total_cache_if_fresh(cutoff: pd.Timestamp) -> pd.DataFrame | None:
    if not TOTAL_CACHE_PATH.exists():
        return None

    frame = pd.read_csv(TOTAL_CACHE_PATH)
    if "timestamp" not in frame.columns or "close" not in frame.columns:
        raise RuntimeError(
            f"Invalid TOTAL cache schema: {TOTAL_CACHE_PATH}"
        )

    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame = (
        frame.dropna(subset=["timestamp", "close"])
        .drop_duplicates("timestamp", keep="last")
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    last_date = pd.Timestamp(frame.iloc[-1]["timestamp"])
    if last_date < cutoff.normalize():
        return None

    frame = frame[frame["timestamp"] <= cutoff].reset_index(drop=True)
    if len(frame) < TV_MIN_ROWS - 5:
        raise RuntimeError(
            f"TOTAL cache too short after cutoff: {len(frame)} rows"
        )
    if frame.iloc[0]["timestamp"] > pd.Timestamp("2022-12-01", tz="UTC"):
        raise RuntimeError(
            "TOTAL cache does not provide enough SMA300 warmup"
        )

    print(
        f"[TOTAL cache] using {TOTAL_CACHE_PATH} "
        f"through {last_date.date()}",
        flush=True,
    )
    return frame


def fetch_tradingview_total() -> pd.DataFrame:
    tv = TvDatafeed()
    raw = tv.get_hist(
        symbol="TOTAL",
        exchange="CRYPTOCAP",
        interval=Interval.in_daily,
        n_bars=TV_BARS,
    )
    if raw is None or len(raw) < TV_MIN_ROWS:
        raise RuntimeError(
            f"TradingView TOTAL history insufficient: "
            f"{0 if raw is None else len(raw)} rows"
        )
    frame = raw.copy().reset_index()
    first_col = frame.columns[0]
    frame = frame.rename(columns={first_col: "timestamp"})
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame = frame[
        ["timestamp", "open", "high", "low", "close", "volume"]
    ].copy()
    for col in ("open", "high", "low", "close", "volume"):
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame = (
        frame.dropna(subset=["timestamp", "close"])
        .drop_duplicates("timestamp", keep="last")
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    frame = frame[frame["timestamp"] <= CUTOFF].reset_index(drop=True)

    if len(frame) < TV_MIN_ROWS - 5:
        raise RuntimeError(
            f"TOTAL rows after cutoff too short: {len(frame)}"
        )
    if frame.iloc[0]["timestamp"] > pd.Timestamp("2022-12-01", tz="UTC"):
        raise RuntimeError(
            "TOTAL history does not provide enough SMA300 warmup"
        )

    for n in (50, 100, 200, 300):
        frame[f"sma{n}"] = frame["close"].rolling(
            n, min_periods=n
        ).mean()
        frame[f"sma{n}_slope_5d"] = (
            frame[f"sma{n}"] / frame[f"sma{n}"].shift(5) - 1.0
        )

    frame["ma_state"] = [
        classify_ma_state(row) for _, row in frame.iterrows()
    ]
    return frame


def load_or_fetch_tradingview_total(
    cutoff: pd.Timestamp,
) -> tuple[pd.DataFrame, str]:
    cached = load_total_cache_if_fresh(cutoff)
    if cached is not None:
        return cached, "REPOSITORY_CACHE"

    print(
        "[TOTAL cache] cache missing/stale; falling back to TradingView",
        flush=True,
    )
    frame = fetch_tradingview_total()
    frame = frame[frame["timestamp"] <= cutoff].reset_index(drop=True)
    return frame, "TRADINGVIEW_REFRESH_REQUIRED"


def classify_ma_state(row: pd.Series) -> str:
    required = [
        row.get("sma50"),
        row.get("sma100"),
        row.get("sma200"),
        row.get("sma300"),
    ]
    if any(pd.isna(x) for x in required):
        return "INSUFFICIENT_HISTORY"

    close = float(row["close"])
    s50 = float(row["sma50"])
    s100 = float(row["sma100"])
    s200 = float(row["sma200"])
    s300 = float(row["sma300"])

    if close > s50 > s100 > s200 > s300:
        return "FULL_BULL_ALIGNMENT"
    if close < s50 < s100 < s200 < s300:
        return "FULL_BEAR_ALIGNMENT"
    if close > s50 > s100:
        return "BULL_BUILDING"
    if close < s50 < s100:
        return "BEAR_BUILDING"
    return "MIXED"


def download_btc(cutoff: pd.Timestamp) -> pd.DataFrame:
    result = download_historical_dataset(
        BinanceSpotRestClient(),
        symbol="BTCUSDT",
        start=pd.Timestamp("2023-10-01", tz="UTC"),
        end=cutoff + pd.Timedelta(days=1),
        timeframe="1D",
        as_of=cutoff + pd.Timedelta(days=1),
    )
    if result.dataset is None:
        raise RuntimeError(f"BTCUSDT: {result.metadata.status}")
    if result.dataset.quality.has_critical_issues:
        raise RuntimeError("BTCUSDT: critical data quality issues")
    frame = result.dataset.candles[["timestamp", "close"]].copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame["btc_close"] = frame["close"].astype(float)
    return (
        frame.drop(columns=["close"])
        .sort_values("timestamp")
        .drop_duplicates("timestamp", keep="last")
        .reset_index(drop=True)
    )


def last_row_on_or_before(
    frame: pd.DataFrame,
    when: pd.Timestamp,
) -> pd.Series:
    sub = frame[frame["timestamp"] <= when]
    if sub.empty:
        raise RuntimeError(f"No row on/before {when}")
    return sub.iloc[-1]


def build_u10_equities(
    panel: pd.DataFrame,
) -> tuple[pd.DatetimeIndex, dict[str, np.ndarray], dict]:
    timestamps, opens, closes = core.prepare_arrays(panel)
    signal_positions, signal_candidates = core.build_signal_index(panel)
    start_i, end_i = core.bounds(timestamps, MATURE_START, CUTOFF)
    eval_ts = timestamps[start_i : end_i + 1]

    equity_by_start: dict[str, np.ndarray] = {}
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
        if len(equity) != len(eval_ts):
            raise RuntimeError(
                f"U10 equity length mismatch {start_asset}: "
                f"{len(equity)} != {len(eval_ts)}"
            )
        equity_by_start[start_asset] = equity

    meta = {
        "start_assets": list(U10),
        "monthly_aggregation": (
            "median monthly return across 10 frozen start states"
        ),
        "daily_forward_aggregation": (
            "median forward return across 10 frozen start states"
        ),
        "evaluation_start": eval_ts[0].isoformat(),
        "evaluation_end": eval_ts[-1].isoformat(),
        "evaluation_rows": int(len(eval_ts)),
    }
    return eval_ts, equity_by_start, meta


def sample_array(
    timestamps: pd.DatetimeIndex,
    values: np.ndarray,
    when: pd.Timestamp,
) -> float:
    idx = int(timestamps.searchsorted(when, side="right") - 1)
    if idx < 0:
        return np.nan
    return float(values[idx])


def forward_return_array(
    timestamps: pd.DatetimeIndex,
    values: np.ndarray,
    event_date: pd.Timestamp,
    days: int,
) -> float:
    start_idx = int(timestamps.searchsorted(event_date, side="right") - 1)
    target_date = event_date + pd.Timedelta(days=days)
    end_idx = int(timestamps.searchsorted(target_date, side="right") - 1)
    if start_idx < 0 or end_idx <= start_idx:
        return np.nan
    if end_idx >= len(values):
        return np.nan
    if timestamps[end_idx] < target_date - pd.Timedelta(days=2):
        return np.nan
    return float(values[end_idx] / values[start_idx] - 1.0)


def build_monthly_table(
    total: pd.DataFrame,
    btc: pd.DataFrame,
    u10_ts: pd.DatetimeIndex,
    equity_by_start: dict[str, np.ndarray],
) -> pd.DataFrame:
    rows = []
    dates = month_ends()

    for when in dates:
        total_row = last_row_on_or_before(total, when)
        btc_row = last_row_on_or_before(btc, when)
        sampled_eq = {
            asset: sample_array(u10_ts, eq, when)
            for asset, eq in equity_by_start.items()
        }
        rows.append(
            {
                "requested_date": when,
                "total_date": total_row["timestamp"],
                "total_close_usd": float(total_row["close"]),
                "sma50_usd": float(total_row["sma50"]),
                "sma100_usd": float(total_row["sma100"]),
                "sma200_usd": float(total_row["sma200"]),
                "sma300_usd": float(total_row["sma300"]),
                "sma50_slope_5d": float(total_row["sma50_slope_5d"]),
                "sma100_slope_5d": float(total_row["sma100_slope_5d"]),
                "sma200_slope_5d": float(total_row["sma200_slope_5d"]),
                "sma300_slope_5d": float(total_row["sma300_slope_5d"]),
                "ma_state": str(total_row["ma_state"]),
                "btc_date": btc_row["timestamp"],
                "btc_close_usd": float(btc_row["btc_close"]),
                **{
                    f"eq_{asset}": value
                    for asset, value in sampled_eq.items()
                },
            }
        )

    frame = pd.DataFrame(rows)
    frame["month"] = frame["requested_date"].dt.strftime("%Y-%m")
    frame["total_mom"] = frame["total_close_usd"].pct_change(
        fill_method=None
    )
    frame["btc_mom"] = frame["btc_close_usd"].pct_change(
        fill_method=None
    )

    medians = [np.nan]
    minimums = [np.nan]
    maximums = [np.nan]
    for i in range(1, len(frame)):
        vals = []
        for asset in U10:
            prev = float(frame.iloc[i - 1][f"eq_{asset}"])
            cur = float(frame.iloc[i][f"eq_{asset}"])
            vals.append(cur / prev - 1.0)
        arr = np.asarray(vals, dtype=float)
        medians.append(float(np.median(arr)))
        minimums.append(float(np.min(arr)))
        maximums.append(float(np.max(arr)))
    frame["u10_mom_median"] = medians
    frame["u10_mom_min"] = minimums
    frame["u10_mom_max"] = maximums

    for n in (50, 100, 200, 300):
        frame[f"total_vs_sma{n}"] = (
            frame["total_close_usd"] / frame[f"sma{n}_usd"] - 1.0
        )

    return frame


def state_persists(
    states: pd.Series,
    idx: int,
    state: str,
    bars: int = 3,
) -> bool:
    if idx + bars > len(states):
        return False
    return bool((states.iloc[idx : idx + bars] == state).all())


def not_state_persists(
    states: pd.Series,
    idx: int,
    state: str,
    bars: int = 3,
) -> bool:
    if idx + bars > len(states):
        return False
    return bool((states.iloc[idx : idx + bars] != state).all())


def future_btc_return(
    btc: pd.DataFrame,
    event_date: pd.Timestamp,
    days: int,
) -> float:
    start = last_row_on_or_before(btc, event_date)
    target = event_date + pd.Timedelta(days=days)
    if target > CUTOFF:
        return np.nan
    end = last_row_on_or_before(btc, target)
    if end["timestamp"] < target - pd.Timedelta(days=2):
        return np.nan
    return float(end["btc_close"] / start["btc_close"] - 1.0)


def future_u10_return(
    u10_ts: pd.DatetimeIndex,
    equity_by_start: dict[str, np.ndarray],
    event_date: pd.Timestamp,
    days: int,
) -> float:
    vals = []
    for equity in equity_by_start.values():
        ret = forward_return_array(
            u10_ts, equity, event_date, days
        )
        if np.isfinite(ret):
            vals.append(ret)
    if not vals:
        return np.nan
    return float(np.median(np.asarray(vals, dtype=float)))


def build_transition_events(
    total: pd.DataFrame,
    btc: pd.DataFrame,
    u10_ts: pd.DatetimeIndex,
    equity_by_start: dict[str, np.ndarray],
) -> pd.DataFrame:
    work = total[
        (total["timestamp"] >= MATURE_START)
        & (total["timestamp"] <= CUTOFF)
    ].copy().reset_index(drop=True)
    states = work["ma_state"].astype(str)

    rows = []
    for i in range(1, len(work)):
        prev = work.iloc[i - 1]
        cur = work.iloc[i]
        date = pd.Timestamp(cur["timestamp"])

        for n in (50, 100, 200, 300):
            prev_diff = float(prev["close"] - prev[f"sma{n}"])
            cur_diff = float(cur["close"] - cur[f"sma{n}"])
            event = None
            if prev_diff <= 0 < cur_diff:
                event = f"TOTAL_CROSS_ABOVE_SMA{n}"
            elif prev_diff >= 0 > cur_diff:
                event = f"TOTAL_CROSS_BELOW_SMA{n}"
            if event:
                rows.append(
                    event_row(
                        event,
                        date,
                        cur,
                        btc,
                        u10_ts,
                        equity_by_start,
                        persisted_3d=np.nan,
                    )
                )

        prev_state = str(prev["ma_state"])
        cur_state = str(cur["ma_state"])

        if (
            cur_state == "FULL_BULL_ALIGNMENT"
            and prev_state != "FULL_BULL_ALIGNMENT"
        ):
            rows.append(
                event_row(
                    "ENTER_FULL_BULL_ALIGNMENT",
                    date,
                    cur,
                    btc,
                    u10_ts,
                    equity_by_start,
                    persisted_3d=state_persists(
                        states, i, "FULL_BULL_ALIGNMENT", 3
                    ),
                )
            )
        if (
            prev_state == "FULL_BULL_ALIGNMENT"
            and cur_state != "FULL_BULL_ALIGNMENT"
        ):
            rows.append(
                event_row(
                    "EXIT_FULL_BULL_ALIGNMENT",
                    date,
                    cur,
                    btc,
                    u10_ts,
                    equity_by_start,
                    persisted_3d=not_state_persists(
                        states, i, "FULL_BULL_ALIGNMENT", 3
                    ),
                )
            )
        if (
            cur_state == "FULL_BEAR_ALIGNMENT"
            and prev_state != "FULL_BEAR_ALIGNMENT"
        ):
            rows.append(
                event_row(
                    "ENTER_FULL_BEAR_ALIGNMENT",
                    date,
                    cur,
                    btc,
                    u10_ts,
                    equity_by_start,
                    persisted_3d=state_persists(
                        states, i, "FULL_BEAR_ALIGNMENT", 3
                    ),
                )
            )
        if (
            prev_state == "FULL_BEAR_ALIGNMENT"
            and cur_state != "FULL_BEAR_ALIGNMENT"
        ):
            rows.append(
                event_row(
                    "EXIT_FULL_BEAR_ALIGNMENT",
                    date,
                    cur,
                    btc,
                    u10_ts,
                    equity_by_start,
                    persisted_3d=not_state_persists(
                        states, i, "FULL_BEAR_ALIGNMENT", 3
                    ),
                )
            )

    return pd.DataFrame(rows).sort_values(
        ["date", "event"]
    ).reset_index(drop=True)


def event_row(
    event: str,
    date: pd.Timestamp,
    total_row: pd.Series,
    btc: pd.DataFrame,
    u10_ts: pd.DatetimeIndex,
    equity_by_start: dict[str, np.ndarray],
    persisted_3d,
) -> dict:
    btc_row = last_row_on_or_before(btc, date)
    return {
        "date": date,
        "event": event,
        "ma_state": str(total_row["ma_state"]),
        "persisted_3d": persisted_3d,
        "total_close_usd": float(total_row["close"]),
        "sma50_usd": float(total_row["sma50"]),
        "sma100_usd": float(total_row["sma100"]),
        "sma200_usd": float(total_row["sma200"]),
        "sma300_usd": float(total_row["sma300"]),
        "btc_close_usd": float(btc_row["btc_close"]),
        "btc_next_30d": future_btc_return(btc, date, 30),
        "btc_next_90d": future_btc_return(btc, date, 90),
        "u10_next_30d": future_u10_return(
            u10_ts, equity_by_start, date, 30
        ),
        "u10_next_90d": future_u10_return(
            u10_ts, equity_by_start, date, 90
        ),
    }


def corr(a: pd.Series, b: pd.Series) -> float:
    valid = pd.concat([a, b], axis=1).dropna()
    if len(valid) < 3:
        return np.nan
    return float(valid.iloc[:, 0].corr(valid.iloc[:, 1]))


def directional_agreement(a: pd.Series, b: pd.Series) -> float:
    valid = pd.concat([a, b], axis=1).dropna()
    if valid.empty:
        return np.nan
    return float(
        np.mean(
            np.sign(valid.iloc[:, 0].to_numpy(float))
            == np.sign(valid.iloc[:, 1].to_numpy(float))
        )
    )


def build_state_summary(monthly: pd.DataFrame) -> pd.DataFrame:
    valid = monthly.dropna(
        subset=["total_mom", "btc_mom", "u10_mom_median"]
    )
    rows = []
    for state, g in valid.groupby("ma_state", sort=False):
        if state == "INSUFFICIENT_HISTORY":
            continue
        rows.append(
            {
                "ma_state": state,
                "month_count": int(len(g)),
                "median_total_mom": float(g["total_mom"].median()),
                "median_btc_mom": float(g["btc_mom"].median()),
                "median_u10_mom": float(g["u10_mom_median"].median()),
                "btc_positive_month_rate": float(
                    (g["btc_mom"] > 0).mean()
                ),
                "u10_positive_month_rate": float(
                    (g["u10_mom_median"] > 0).mean()
                ),
            }
        )
    return pd.DataFrame(rows)


def fmt_pct(value) -> str:
    return "—" if pd.isna(value) else f"{100*float(value):+.1f}%"


def fmt_trillion(value) -> str:
    return "—" if pd.isna(value) else "$" + f"{float(value)/1e12:.2f}T"


def monthly_report_table(monthly: pd.DataFrame) -> str:
    lines = [
        "|Month|TOTAL|TOTAL MoM|BTC MoM|U10 MoM|SMA50|SMA100|SMA200|SMA300|State|",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for _, r in monthly.iterrows():
        lines.append(
            f"|{r['month']}|{fmt_trillion(r['total_close_usd'])}|"
            f"{fmt_pct(r['total_mom'])}|{fmt_pct(r['btc_mom'])}|"
            f"{fmt_pct(r['u10_mom_median'])}|"
            f"{fmt_trillion(r['sma50_usd'])}|"
            f"{fmt_trillion(r['sma100_usd'])}|"
            f"{fmt_trillion(r['sma200_usd'])}|"
            f"{fmt_trillion(r['sma300_usd'])}|{r['ma_state']}|"
        )
    return "\n".join(lines)


def major_event_report(events: pd.DataFrame) -> str:
    major = events[
        events["event"].isin(
            [
                "ENTER_FULL_BULL_ALIGNMENT",
                "EXIT_FULL_BULL_ALIGNMENT",
                "ENTER_FULL_BEAR_ALIGNMENT",
                "EXIT_FULL_BEAR_ALIGNMENT",
                "TOTAL_CROSS_ABOVE_SMA200",
                "TOTAL_CROSS_BELOW_SMA200",
                "TOTAL_CROSS_ABOVE_SMA300",
                "TOTAL_CROSS_BELOW_SMA300",
            ]
        )
    ].copy()
    lines = [
        "|Date|Event|TOTAL|State|3d confirmed|BTC next 30d|BTC next 90d|U10 next 30d|U10 next 90d|",
        "|---|---|---:|---|---:|---:|---:|---:|---:|",
    ]
    for _, r in major.iterrows():
        confirmation = (
            "—"
            if pd.isna(r["persisted_3d"])
            else ("yes" if bool(r["persisted_3d"]) else "no")
        )
        lines.append(
            f"|{pd.Timestamp(r['date']).date()}|{r['event']}|"
            f"{fmt_trillion(r['total_close_usd'])}|{r['ma_state']}|"
            f"{confirmation}|{fmt_pct(r['btc_next_30d'])}|"
            f"{fmt_pct(r['btc_next_90d'])}|"
            f"{fmt_pct(r['u10_next_30d'])}|"
            f"{fmt_pct(r['u10_next_90d'])}|"
        )
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff", default="2026-09-26T00:00:00Z")
    args = parser.parse_args()

    cutoff = core.utc(args.cutoff)
    if cutoff.normalize() != CUTOFF:
        raise RuntimeError(
            f"Expected preregistered cutoff {CUTOFF.date()}, "
            f"got {cutoff.date()}"
        )

    total, total_data_mode = load_or_fetch_tradingview_total(cutoff)
    btc = download_btc(cutoff)

    core.POOL = U10
    panel, data_meta = core.download_panel(
        cutoff + pd.Timedelta(days=1)
    )
    u10_ts, equities, u10_meta = build_u10_equities(panel)

    monthly = build_monthly_table(
        total, btc, u10_ts, equities
    )
    events = build_transition_events(
        total, btc, u10_ts, equities
    )
    states = build_state_summary(monthly)

    total_next_btc = monthly["btc_mom"].shift(-1)
    total_next_u10 = monthly["u10_mom_median"].shift(-1)

    relationships = {
        "same_month_total_vs_btc": corr(
            monthly["total_mom"], monthly["btc_mom"]
        ),
        "same_month_total_vs_u10": corr(
            monthly["total_mom"], monthly["u10_mom_median"]
        ),
        "direction_agreement_total_vs_btc": directional_agreement(
            monthly["total_mom"], monthly["btc_mom"]
        ),
        "direction_agreement_total_vs_u10": directional_agreement(
            monthly["total_mom"], monthly["u10_mom_median"]
        ),
        "lag1_total_vs_next_btc": corr(
            monthly["total_mom"], total_next_btc
        ),
        "lag1_total_vs_next_u10": corr(
            monthly["total_mom"], total_next_u10
        ),
    }

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime(
        "%Y%m%dT%H%M%SZ"
    )
    run_dir.mkdir(parents=True, exist_ok=True)

    total.to_csv(run_dir / "total_daily.csv", index=False)
    monthly.to_csv(run_dir / "monthly_table.csv", index=False)
    events.to_csv(
        run_dir / "daily_transition_events.csv", index=False
    )
    states.to_csv(run_dir / "ma_state_summary.csv", index=False)

    summary = {
        "experiment": "RR_U10_MONTHLY_TRADINGVIEW_TOTAL_SMA_V1",
        "workflow_mode": "STRESS_TEST_ONLY",
        "source_commit_sha": source_sha(),
        "cutoff": cutoff.isoformat(),
        "frozen_universe_id": "RR_TARGET_U10_CANDIDATE_HBAR_V1",
        "assets": list(U10),
        "market_cap_source": {
            "symbol": "CRYPTOCAP:TOTAL",
            "provider": "TradingView",
            "definition": (
                "Accumulated market capitalization of top-125 "
                "cryptocurrencies from TradingView Crypto Coins Screener"
            ),
            "transport": (
                "tvdatafeed anonymous historical client, "
                "pinned OSS commit"
            ),
            "tvdatafeed_commit": (
                "e6f6aaa7de439ac6e454d9b26d2760ded8dc4923"
            ),
            "daily_rows": int(len(total)),
            "daily_start": total.iloc[0]["timestamp"].isoformat(),
            "daily_end": total.iloc[-1]["timestamp"].isoformat(),
            "data_mode": total_data_mode,
            "repository_cache_path": str(
                TOTAL_CACHE_PATH.relative_to(ROOT)
            ),
            "repository_cache_meta_path": str(
                TOTAL_CACHE_META_PATH.relative_to(ROOT)
            ),
        },
        "invalid_exploration": {
            "run_id": 36435470008,
            "classification": "INVALID_SOURCE_SEMANTICS",
            "reason": (
                "CoinMarketCap historical page globalMetrics.marketCap "
                "was current-site metadata, not historical snapshot value"
            ),
        },
        "u10_sampling": u10_meta,
        "data_metadata": data_meta,
        "relationships": relationships,
        "state_summary": json.loads(
            states.to_json(orient="records")
        ),
        "major_events": json.loads(
            events[
                events["event"].str.contains(
                    "FULL_|SMA200|SMA300", regex=True
                )
            ].to_json(orient="records", date_format="iso")
        ),
        "production_changes": "NONE",
        "test_level": "GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    }
    (run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    report = [
        "# RR U10 MONTHLY TRADINGVIEW TOTAL + SMA V1",
        "",
        "Mode: STRESS_TEST_ONLY",
        "",
        "Source: TradingView CRYPTOCAP:TOTAL (top-125 crypto market cap)",
        "",
        "## Monthly table",
        "",
        monthly_report_table(monthly),
        "",
        "## Relationship",
        "",
        f"- Same-month TOTAL vs BTC Pearson: "
        f"{relationships['same_month_total_vs_btc']:.3f}",
        f"- Same-month TOTAL vs U10 Pearson: "
        f"{relationships['same_month_total_vs_u10']:.3f}",
        f"- Direction agreement TOTAL vs BTC: "
        f"{100*relationships['direction_agreement_total_vs_btc']:.1f}%",
        f"- Direction agreement TOTAL vs U10: "
        f"{100*relationships['direction_agreement_total_vs_u10']:.1f}%",
        f"- TOTAL MoM vs next-month BTC Pearson: "
        f"{relationships['lag1_total_vs_next_btc']:.3f}",
        f"- TOTAL MoM vs next-month U10 Pearson: "
        f"{relationships['lag1_total_vs_next_u10']:.3f}",
        "",
        "## MA state summary",
        "",
        "|State|Months|Median TOTAL MoM|Median BTC|Median U10|BTC positive|U10 positive|",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]

    for _, r in states.iterrows():
        report.append(
            f"|{r['ma_state']}|{int(r['month_count'])}|"
            f"{fmt_pct(r['median_total_mom'])}|"
            f"{fmt_pct(r['median_btc_mom'])}|"
            f"{fmt_pct(r['median_u10_mom'])}|"
            f"{100*r['btc_positive_month_rate']:.1f}%|"
            f"{100*r['u10_positive_month_rate']:.1f}%|"
        )

    report += [
        "",
        "## Major daily transition events",
        "",
        major_event_report(events),
        "",
        "## Validation",
        "",
        "- Exploratory CMC globalMetrics series was rejected before evidence persistence.",
        "- TOTAL daily series is used for exact SMA50/SMA100/SMA200/SMA300.",
        f"- TOTAL data mode: {total_data_mode}.",
        "- Market cap and prices are mechanically related; correlation is not causality.",
        "- MA rules were frozen before inspecting this corrected runtime.",
        "- No production/paper-live/Telegram/exchange behavior changed.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
        "",
    ]

    text = "\n".join(report)
    (run_dir / "report.md").write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
