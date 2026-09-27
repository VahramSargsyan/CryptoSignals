from __future__ import annotations

import argparse
import itertools
import json
import math
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from scripts.research_relative_rotation_graph_intelligence import (
    ARM_THRESHOLD,
    LOOKBACK,
    REVERSAL,
    TRANSITION_COST,
    PairSignal,
    choose_candidate,
)

REPO_ROOT = Path(__file__).resolve().parents[1]

LEGACY_ASSETS = ("ATOM", "TWT", "BNB", "SOL", "TRX", "AAVE", "LINK")
SYMBOLS = {asset: f"{asset}USDT" for asset in LEGACY_ASSETS}
BTC_SYMBOL = "BTCUSDT"

REQUEST_START = pd.Timestamp("2020-01-01", tz="UTC")
HOLDOUT_END = pd.Timestamp("2023-05-04", tz="UTC")

SMA_DAYS = 200
VOL_DAYS = 30
ENTER_BREADTH_MAX = 3
EXIT_BREADTH_MIN = 5
CONFIRM_DAYS = 3

FROZEN_BTC_CANDIDATES = ((25, 100), (30, 100), (12, 100))
ROBUSTNESS_DAYS = (180, 120)


@dataclass
class PairState:
    mode: str = "NONE"
    extreme: float | None = None
    max_dislocation: float = 0.0


@dataclass(frozen=True)
class Result:
    variant: str
    start_asset: str
    total_return: float
    max_drawdown: float
    transitions: int
    defensive_entries: int
    defensive_days: int
    reentry_exits: int
    unresolved_cash_end: bool
    median_cash_wait_days: float
    max_cash_wait_days: float
    period_days: int


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


def download_symbol(
    client: BinanceSpotRestClient,
    symbol: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> tuple[pd.DataFrame, dict]:
    cutoff = end + pd.Timedelta(days=1)
    result = download_historical_dataset(
        client,
        symbol=symbol,
        start=start,
        end=cutoff,
        timeframe="1D",
        as_of=cutoff,
    )
    if result.dataset is None:
        raise RuntimeError(f"No dataset for {symbol}: {result.metadata.status}")
    if result.dataset.quality.has_critical_issues:
        raise RuntimeError(f"Critical data quality for {symbol}: {result.dataset.quality}")

    frame = result.dataset.candles[["timestamp", "open", "close"]].copy()
    meta = {
        "dataset_id": result.dataset.dataset_id,
        "rows": len(frame),
        "actual_start": result.metadata.actual_start,
        "actual_end": result.metadata.actual_end,
        "listing_truncated": result.metadata.listing_truncated,
        "quality": result.dataset.quality.__dict__,
    }
    return frame, meta


def download_legacy_panel() -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    client = BinanceSpotRestClient()
    pieces = []
    metadata = {}

    for asset in LEGACY_ASSETS:
        frame, meta = download_symbol(client, SYMBOLS[asset], REQUEST_START, HOLDOUT_END)
        frame = frame.rename(columns={"open": f"{asset}_open", "close": f"{asset}_close"})
        pieces.append(frame)
        metadata[asset] = meta

    panel = pieces[0]
    for frame in pieces[1:]:
        panel = panel.merge(frame, on="timestamp", how="inner", validate="one_to_one")
    panel = panel.sort_values("timestamp").reset_index(drop=True)

    btc, btc_meta = download_symbol(client, BTC_SYMBOL, REQUEST_START, HOLDOUT_END)
    btc = btc[["timestamp", "close"]].rename(columns={"close": "BTC_close"})
    btc = btc.sort_values("timestamp").reset_index(drop=True)

    panel = panel.merge(btc, on="timestamp", how="inner", validate="one_to_one")
    panel = panel.sort_values("timestamp").reset_index(drop=True)

    if panel.empty:
        raise RuntimeError("Legacy-7 common panel is empty")

    metadata["BTC"] = btc_meta
    metadata["panel"] = {
        "rows": len(panel),
        "actual_common_start": panel["timestamp"].min().isoformat(),
        "actual_common_end": panel["timestamp"].max().isoformat(),
        "requested_start": REQUEST_START.isoformat(),
        "requested_end": HOLDOUT_END.isoformat(),
        "assets": list(LEGACY_ASSETS),
        "pepe_included": False,
    }
    return panel, btc, metadata


def add_features(panel: pd.DataFrame) -> pd.DataFrame:
    out = panel.copy()
    breadth_parts = []
    vol_cols = []

    for asset in LEGACY_ASSETS:
        close = out[f"{asset}_close"].astype(float)
        sma_col = f"{asset}_sma{SMA_DAYS}"
        vol_col = f"{asset}_vol{VOL_DAYS}"
        out[sma_col] = close.rolling(SMA_DAYS, min_periods=SMA_DAYS).mean()
        out[vol_col] = close.pct_change().rolling(VOL_DAYS, min_periods=VOL_DAYS).std()
        breadth_parts.append((close > out[sma_col]).astype(int))
        vol_cols.append(vol_col)

    out["breadth_sma200"] = sum(breadth_parts)
    out["lowest_vol_asset"] = out[vol_cols].idxmin(axis=1).str.replace(
        f"_vol{VOL_DAYS}", "", regex=False
    )
    out.loc[out[vol_cols].isna().any(axis=1), "lowest_vol_asset"] = pd.NA
    return out


def build_pair_signals(panel: pd.DataFrame) -> dict[pd.Timestamp, list[PairSignal]]:
    signals = {ts: [] for ts in panel["timestamp"]}
    ratios = {}
    medians = {}
    deviations = {}

    for left, right in itertools.combinations(LEGACY_ASSETS, 2):
        ratio = panel[f"{right}_close"] / panel[f"{left}_close"]
        median = ratio.rolling(LOOKBACK, min_periods=LOOKBACK).median()
        ratios[(left, right)] = ratio
        medians[(left, right)] = median
        deviations[(left, right)] = ratio / median - 1.0

    states = {pair: PairState() for pair in ratios}

    for idx, ts in enumerate(panel["timestamp"]):
        for pair, ratio_series in ratios.items():
            left, right = pair
            ratio = float(ratio_series.iloc[idx])
            median = medians[pair].iloc[idx]
            dev = deviations[pair].iloc[idx]
            if pd.isna(median) or pd.isna(dev):
                continue
            dev = float(dev)
            state = states[pair]

            if state.mode == "NONE":
                if dev >= ARM_THRESHOLD:
                    state.mode = "HIGH"
                    state.extreme = ratio
                    state.max_dislocation = abs(dev)
                elif dev <= -ARM_THRESHOLD:
                    state.mode = "LOW"
                    state.extreme = ratio
                    state.max_dislocation = abs(dev)
                continue

            if state.mode == "HIGH":
                assert state.extreme is not None
                if ratio > state.extreme:
                    state.extreme = ratio
                    state.max_dislocation = max(state.max_dislocation, abs(dev))
                if ratio <= state.extreme * (1.0 - REVERSAL):
                    signals[ts].append(
                        PairSignal(
                            date=ts,
                            from_asset=right,
                            to_asset=left,
                            strength=state.max_dislocation,
                            pair=f"{left}/{right}",
                        )
                    )
                    states[pair] = PairState()
                continue

            if state.mode == "LOW":
                assert state.extreme is not None
                if ratio < state.extreme:
                    state.extreme = ratio
                    state.max_dislocation = max(state.max_dislocation, abs(dev))
                if ratio >= state.extreme * (1.0 + REVERSAL):
                    signals[ts].append(
                        PairSignal(
                            date=ts,
                            from_asset=left,
                            to_asset=right,
                            strength=state.max_dislocation,
                            pair=f"{left}/{right}",
                        )
                    )
                    states[pair] = PairState()

    return signals


def btc_cross_map(panel: pd.DataFrame, fast: int, slow: int) -> dict[pd.Timestamp, bool]:
    close = panel["BTC_close"].astype(float)
    f = close.rolling(fast, min_periods=fast).mean()
    s = close.rolling(slow, min_periods=slow).mean()
    cross = ((f > s) & (f.shift(1) <= s.shift(1))).fillna(False)
    return {
        pd.Timestamp(ts): bool(flag)
        for ts, flag in zip(panel["timestamp"], cross, strict=True)
    }


def eligible_anchor(panel: pd.DataFrame) -> pd.Timestamp:
    required = [f"{a}_sma{SMA_DAYS}" for a in LEGACY_ASSETS]
    required += [f"{a}_vol{VOL_DAYS}" for a in LEGACY_ASSETS]
    mask = panel[required].notna().all(axis=1)
    eligible = panel.loc[mask, "timestamp"]
    if eligible.empty:
        raise RuntimeError("No fully eligible legacy-7 date")
    anchor = pd.Timestamp(eligible.iloc[0])
    # Pair signals use LOOKBACK=180, BTC slow SMA uses max=100; SMA200 is the longest warmup.
    return anchor


def _apply_router_transition(
    row: pd.Series,
    actual_asset: str,
    target: str,
    qty: float,
) -> tuple[str, float, int]:
    if actual_asset == target:
        return actual_asset, qty, 0
    value = qty * float(row[f"{actual_asset}_open"])
    qty = value * (1.0 - TRANSITION_COST) / float(row[f"{target}_open"])
    return target, qty, 1


def run_baseline(
    panel: pd.DataFrame,
    signals_by_date: dict[pd.Timestamp, list[PairSignal]],
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    start_asset: str,
) -> Result:
    window = panel[(panel["timestamp"] >= start) & (panel["timestamp"] <= end)].copy()
    first = window.iloc[0]
    current = start_asset
    qty = 1.0 / float(first[f"{current}_open"])
    pending = None
    transitions = 0
    equity = []

    for pos, (_, row) in enumerate(window.iterrows()):
        ts = row["timestamp"]
        if pending is not None:
            current, qty, inc = _apply_router_transition(row, current, pending.to_asset, qty)
            transitions += inc
            pending = None

        equity.append(qty * float(row[f"{current}_close"]))

        if pos == len(window) - 1:
            continue
        candidates = [x for x in signals_by_date.get(ts, []) if x.from_asset == current]
        if candidates:
            pending = choose_candidate(candidates, "BASELINE", None)

    series = pd.Series(equity, dtype=float)
    return Result(
        variant="BASELINE",
        start_asset=start_asset,
        total_return=float(series.iloc[-1] / series.iloc[0] - 1.0),
        max_drawdown=float((series / series.cummax() - 1.0).min()),
        transitions=transitions,
        defensive_entries=0,
        defensive_days=0,
        reentry_exits=0,
        unresolved_cash_end=False,
        median_cash_wait_days=float("nan"),
        max_cash_wait_days=float("nan"),
        period_days=len(window),
    )


def run_low_vol(
    panel: pd.DataFrame,
    signals_by_date: dict[pd.Timestamp, list[PairSignal]],
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    start_asset: str,
) -> Result:
    window = panel[(panel["timestamp"] >= start) & (panel["timestamp"] <= end)].copy()
    first = window.iloc[0]
    shadow = start_asset
    actual = start_asset
    qty = 1.0 / float(first[f"{actual}_open"])
    pending_shadow = None
    pending_action: tuple[str, str | None] | None = None
    defensive = False
    low_streak = high_streak = 0
    transitions = entries = defensive_days = 0
    equity = []

    for pos, (_, row) in enumerate(window.iterrows()):
        ts = row["timestamp"]
        old_shadow = shadow
        if pending_shadow is not None:
            shadow = pending_shadow.to_asset
            pending_shadow = None

        if pending_action is not None:
            action, target = pending_action
            pending_action = None
            if action == "ENTER":
                assert target is not None
                actual, qty, inc = _apply_router_transition(row, actual, target, qty)
                transitions += inc
                defensive = True
                entries += 1
            elif action == "EXIT":
                actual, qty, inc = _apply_router_transition(row, actual, shadow, qty)
                transitions += inc
                defensive = False
            else:
                raise RuntimeError(action)
        elif not defensive and shadow != old_shadow:
            actual, qty, inc = _apply_router_transition(row, actual, shadow, qty)
            transitions += inc

        equity.append(qty * float(row[f"{actual}_close"]))
        if defensive:
            defensive_days += 1

        if pos == len(window) - 1:
            continue

        candidates = [x for x in signals_by_date.get(ts, []) if x.from_asset == shadow]
        if candidates:
            pending_shadow = choose_candidate(candidates, "BASELINE", None)

        breadth = int(row["breadth_sma200"])
        low_streak = low_streak + 1 if breadth <= ENTER_BREADTH_MAX else 0
        high_streak = high_streak + 1 if breadth >= EXIT_BREADTH_MIN else 0

        if not defensive and low_streak >= CONFIRM_DAYS:
            target = row["lowest_vol_asset"]
            if pd.isna(target):
                raise RuntimeError(f"Missing low-vol asset at {ts}")
            pending_action = ("ENTER", str(target))
            low_streak = high_streak = 0
        elif defensive and high_streak >= CONFIRM_DAYS:
            pending_action = ("EXIT", None)
            low_streak = high_streak = 0

    series = pd.Series(equity, dtype=float)
    return Result(
        variant="LOW_VOL",
        start_asset=start_asset,
        total_return=float(series.iloc[-1] / series.iloc[0] - 1.0),
        max_drawdown=float((series / series.cummax() - 1.0).min()),
        transitions=transitions,
        defensive_entries=entries,
        defensive_days=defensive_days,
        reentry_exits=0,
        unresolved_cash_end=False,
        median_cash_wait_days=float("nan"),
        max_cash_wait_days=float("nan"),
        period_days=len(window),
    )


def run_frozen_cash(
    panel: pd.DataFrame,
    signals_by_date: dict[pd.Timestamp, list[PairSignal]],
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    start_asset: str,
) -> Result:
    window = panel[(panel["timestamp"] >= start) & (panel["timestamp"] <= end)].copy()
    first = window.iloc[0]
    shadow = start_asset
    actual: str | None = start_asset
    qty = 1.0 / float(first[f"{start_asset}_open"])
    cash_value: float | None = None
    pending_shadow = None
    pending_action: str | None = None
    defensive = False
    low_streak = high_streak = 0
    transitions = entries = cash_days = exits = 0
    equity = []

    for pos, (_, row) in enumerate(window.iterrows()):
        ts = row["timestamp"]
        old_shadow = shadow
        if pending_shadow is not None:
            shadow = pending_shadow.to_asset
            pending_shadow = None

        if pending_action == "ENTER":
            pending_action = None
            assert actual is not None
            value = qty * float(row[f"{actual}_open"])
            cash_value = value * (1.0 - TRANSITION_COST)
            qty = 0.0
            actual = None
            defensive = True
            transitions += 1
            entries += 1
        elif pending_action == "EXIT":
            pending_action = None
            assert cash_value is not None
            qty = cash_value * (1.0 - TRANSITION_COST) / float(row[f"{shadow}_open"])
            cash_value = None
            actual = shadow
            defensive = False
            transitions += 1
            exits += 1
        elif not defensive and shadow != old_shadow:
            assert actual is not None
            actual, qty, inc = _apply_router_transition(row, actual, shadow, qty)
            transitions += inc

        if defensive:
            assert cash_value is not None
            equity.append(cash_value)
            cash_days += 1
        else:
            assert actual is not None
            equity.append(qty * float(row[f"{actual}_close"]))

        if pos == len(window) - 1:
            continue

        candidates = [x for x in signals_by_date.get(ts, []) if x.from_asset == shadow]
        if candidates:
            pending_shadow = choose_candidate(candidates, "BASELINE", None)

        breadth = int(row["breadth_sma200"])
        low_streak = low_streak + 1 if breadth <= ENTER_BREADTH_MAX else 0
        high_streak = high_streak + 1 if breadth >= EXIT_BREADTH_MIN else 0

        if not defensive and low_streak >= CONFIRM_DAYS:
            pending_action = "ENTER"
            low_streak = high_streak = 0
        elif defensive and high_streak >= CONFIRM_DAYS:
            pending_action = "EXIT"
            low_streak = high_streak = 0

    series = pd.Series(equity, dtype=float)
    return Result(
        variant="FROZEN_CASH",
        start_asset=start_asset,
        total_return=float(series.iloc[-1] / series.iloc[0] - 1.0),
        max_drawdown=float((series / series.cummax() - 1.0).min()),
        transitions=transitions,
        defensive_entries=entries,
        defensive_days=cash_days,
        reentry_exits=exits,
        unresolved_cash_end=defensive,
        median_cash_wait_days=float("nan"),
        max_cash_wait_days=float("nan"),
        period_days=len(window),
    )


def run_btc_candidate(
    panel: pd.DataFrame,
    signals_by_date: dict[pd.Timestamp, list[PairSignal]],
    cross_by_date: dict[pd.Timestamp, bool],
    *,
    fast: int,
    slow: int,
    start: pd.Timestamp,
    end: pd.Timestamp,
    start_asset: str,
) -> Result:
    window = panel[(panel["timestamp"] >= start) & (panel["timestamp"] <= end)].copy()
    first = window.iloc[0]
    shadow = start_asset
    actual: str | None = start_asset
    qty = 1.0 / float(first[f"{start_asset}_open"])
    cash_value: float | None = None
    pending_shadow = None
    pending_action: str | None = None
    pending_exit_target: str | None = None
    state = "ARMED"
    low_streak = recovery_streak = 0
    transitions = entries = cash_days = exits = 0
    entry_date: pd.Timestamp | None = None
    waits = []
    equity = []

    for pos, (_, row) in enumerate(window.iterrows()):
        ts = pd.Timestamp(row["timestamp"])
        old_shadow = shadow
        if pending_shadow is not None:
            shadow = pending_shadow.to_asset
            pending_shadow = None

        if pending_action == "ENTER":
            pending_action = None
            assert actual is not None
            value = qty * float(row[f"{actual}_open"])
            cash_value = value * (1.0 - TRANSITION_COST)
            qty = 0.0
            actual = None
            state = "CASH"
            transitions += 1
            entries += 1
            entry_date = ts
            low_streak = recovery_streak = 0

        elif pending_action == "EXIT":
            pending_action = None
            assert cash_value is not None and pending_exit_target is not None
            if shadow != pending_exit_target:
                raise RuntimeError("Shadow changed unexpectedly at BTC exit")
            qty = cash_value * (1.0 - TRANSITION_COST) / float(row[f"{shadow}_open"])
            cash_value = None
            actual = shadow
            state = "POST_CASH_DISARMED"
            transitions += 1
            exits += 1
            if entry_date is not None:
                waits.append(int((ts - entry_date).days))
            entry_date = None
            pending_exit_target = None
            low_streak = recovery_streak = 0

        elif state != "CASH" and shadow != old_shadow:
            assert actual is not None
            actual, qty, inc = _apply_router_transition(row, actual, shadow, qty)
            transitions += inc

        if state == "CASH":
            assert cash_value is not None
            equity.append(cash_value)
            cash_days += 1
        else:
            assert actual is not None
            equity.append(qty * float(row[f"{actual}_close"]))

        if pos == len(window) - 1:
            continue

        candidates = [x for x in signals_by_date.get(ts, []) if x.from_asset == shadow]
        pending_shadow = choose_candidate(candidates, "BASELINE", None) if candidates else None
        prospective_shadow = pending_shadow.to_asset if pending_shadow is not None else shadow

        breadth = int(row["breadth_sma200"])

        if state == "CASH":
            low_streak = recovery_streak = 0
            if bool(cross_by_date.get(ts, False)):
                pending_action = "EXIT"
                pending_exit_target = prospective_shadow
            continue

        if state == "POST_CASH_DISARMED":
            low_streak = 0
            recovery_streak = recovery_streak + 1 if breadth >= EXIT_BREADTH_MIN else 0
            if recovery_streak >= CONFIRM_DAYS:
                state = "ARMED"
                recovery_streak = 0
            continue

        recovery_streak = 0
        low_streak = low_streak + 1 if breadth <= ENTER_BREADTH_MAX else 0
        if low_streak >= CONFIRM_DAYS:
            pending_action = "ENTER"
            low_streak = 0

    series = pd.Series(equity, dtype=float)
    return Result(
        variant=f"BTC_{fast}_{slow}",
        start_asset=start_asset,
        total_return=float(series.iloc[-1] / series.iloc[0] - 1.0),
        max_drawdown=float((series / series.cummax() - 1.0).min()),
        transitions=transitions,
        defensive_entries=entries,
        defensive_days=cash_days,
        reentry_exits=exits,
        unresolved_cash_end=(state == "CASH"),
        median_cash_wait_days=float(np.median(waits)) if waits else float("nan"),
        max_cash_wait_days=float(max(waits)) if waits else float("nan"),
        period_days=len(window),
    )


def complete_windows(anchor: pd.Timestamp, end: pd.Timestamp, days: int):
    out = []
    start = anchor
    while True:
        stop = start + pd.Timedelta(days=days - 1)
        if stop > end:
            break
        out.append((start, stop))
        start = stop + pd.Timedelta(days=1)
    return out


def summarize(df: pd.DataFrame) -> dict:
    waits = pd.to_numeric(df["median_cash_wait_days"], errors="coerce").dropna()
    max_waits = pd.to_numeric(df["max_cash_wait_days"], errors="coerce").dropna()
    return {
        "median_return": float(df["total_return"].median()),
        "worst_return": float(df["total_return"].min()),
        "median_max_drawdown": float(df["max_drawdown"].median()),
        "worst_max_drawdown": float(df["max_drawdown"].min()),
        "positive_starts": int((df["total_return"] > 0).sum()),
        "median_transitions": float(df["transitions"].median()),
        "median_defensive_entries": float(df["defensive_entries"].median()),
        "median_defensive_exposure": float(
            (df["defensive_days"] / df["period_days"]).median()
        ),
        "median_reentry_exits": float(df["reentry_exits"].median()),
        "unresolved_cash_starts": int(df["unresolved_cash_end"].sum()),
        "median_cash_wait_days": float(waits.median()) if len(waits) else None,
        "max_cash_wait_days": float(max_waits.max()) if len(max_waits) else None,
    }


def evaluate_variant(
    variant: str,
    panel: pd.DataFrame,
    signals: dict[pd.Timestamp, list[PairSignal]],
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    cross_map: dict[pd.Timestamp, bool] | None = None,
    fast: int | None = None,
    slow: int | None = None,
) -> pd.DataFrame:
    rows = []
    for asset in LEGACY_ASSETS:
        if variant == "BASELINE":
            r = run_baseline(panel, signals, start=start, end=end, start_asset=asset)
        elif variant == "LOW_VOL":
            r = run_low_vol(panel, signals, start=start, end=end, start_asset=asset)
        elif variant == "FROZEN_CASH":
            r = run_frozen_cash(panel, signals, start=start, end=end, start_asset=asset)
        elif variant == "BTC":
            assert cross_map is not None and fast is not None and slow is not None
            r = run_btc_candidate(
                panel, signals, cross_map,
                fast=fast, slow=slow,
                start=start, end=end, start_asset=asset
            )
        else:
            raise ValueError(variant)
        rows.append(r.__dict__)
    return pd.DataFrame(rows)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Legacy-7 reverse-time holdout for frozen BTC SMA candidates.")
    p.add_argument(
        "--output-root", type=Path,
        default=REPO_ROOT / "research_artifacts" / "btc_sma_legacy7_holdout_v1"
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    panel, _, data_meta = download_legacy_panel()
    panel = add_features(panel)
    signals = build_pair_signals(panel)
    anchor = eligible_anchor(panel)

    if anchor > HOLDOUT_END:
        raise RuntimeError("No eligible holdout after warmup")

    crosses = {
        (fast, slow): btc_cross_map(panel, fast, slow)
        for fast, slow in FROZEN_BTC_CANDIDATES
    }

    variants = {
        "BASELINE": evaluate_variant("BASELINE", panel, signals, start=anchor, end=HOLDOUT_END),
        "LOW_VOL": evaluate_variant("LOW_VOL", panel, signals, start=anchor, end=HOLDOUT_END),
        "FROZEN_CASH": evaluate_variant("FROZEN_CASH", panel, signals, start=anchor, end=HOLDOUT_END),
    }
    for fast, slow in FROZEN_BTC_CANDIDATES:
        variants[f"BTC_{fast}_{slow}"] = evaluate_variant(
            "BTC", panel, signals,
            start=anchor, end=HOLDOUT_END,
            cross_map=crosses[(fast, slow)], fast=fast, slow=slow
        )

    summaries = {name: summarize(df) for name, df in variants.items()}

    robustness_rows = []
    for window_days in ROBUSTNESS_DAYS:
        for idx, (start, end) in enumerate(complete_windows(anchor, HOLDOUT_END, window_days), start=1):
            low_df = evaluate_variant("LOW_VOL", panel, signals, start=start, end=end)
            low = summarize(low_df)

            for fast, slow in FROZEN_BTC_CANDIDATES:
                df = evaluate_variant(
                    "BTC", panel, signals,
                    start=start, end=end,
                    cross_map=crosses[(fast, slow)], fast=fast, slow=slow
                )
                s = summarize(df)
                robustness_rows.append({
                    "fast_sma": fast,
                    "slow_sma": slow,
                    "window_days": window_days,
                    "window_index": idx,
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "median_return": s["median_return"],
                    "median_max_drawdown": s["median_max_drawdown"],
                    "low_vol_return": low["median_return"],
                    "low_vol_drawdown": low["median_max_drawdown"],
                    "beats_low_vol_return": s["median_return"] > low["median_return"],
                    "beats_low_vol_drawdown": s["median_max_drawdown"] > low["median_max_drawdown"],
                    "beats_low_vol_both": (
                        s["median_return"] > low["median_return"]
                        and s["median_max_drawdown"] > low["median_max_drawdown"]
                    ),
                    "cash_exposure": s["median_defensive_exposure"],
                    "cash_wait_days": s["median_cash_wait_days"],
                    "unresolved_cash_starts": s["unresolved_cash_starts"],
                })

    robustness = pd.DataFrame(robustness_rows)
    robust_summary = []
    for fast, slow in FROZEN_BTC_CANDIDATES:
        g = robustness[(robustness["fast_sma"] == fast) & (robustness["slow_sma"] == slow)]
        row = {
            "fast_sma": fast,
            "slow_sma": slow,
            "windows": int(len(g)),
            "return_wins": int(g["beats_low_vol_return"].sum()) if len(g) else 0,
            "dd_wins": int(g["beats_low_vol_drawdown"].sum()) if len(g) else 0,
            "both_wins": int(g["beats_low_vol_both"].sum()) if len(g) else 0,
        }
        for days in ROBUSTNESS_DAYS:
            x = g[g["window_days"] == days]
            row[f"{days}d_windows"] = int(len(x))
            row[f"{days}d_return_wins"] = int(x["beats_low_vol_return"].sum()) if len(x) else 0
            row[f"{days}d_dd_wins"] = int(x["beats_low_vol_drawdown"].sum()) if len(x) else 0
            row[f"{days}d_both_wins"] = int(x["beats_low_vol_both"].sum()) if len(x) else 0
        robust_summary.append(row)

    run_id = f"BTC_SMA_LEGACY7_HOLDOUT_V1_{_source_commit()[:12]}"
    run_dir = args.output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    for name, df in variants.items():
        df.to_csv(run_dir / f"{name.lower()}_by_start.csv", index=False)
    robustness.to_csv(run_dir / "robustness_windows.csv", index=False)
    pd.DataFrame(robust_summary).to_csv(run_dir / "robustness_summary.csv", index=False)

    report = {
        "run_id": run_id,
        "source_commit_sha": _source_commit(),
        "status": "BTC_SMA_LEGACY7_HOLDOUT_V1_EXECUTED",
        "exact_8_asset_validation": False,
        "holdout_type": "REVERSE_TIME_LEGACY7_DIFFERENT_UNIVERSE",
        "legacy_assets": list(LEGACY_ASSETS),
        "excluded_asset": "PEPE",
        "reason_exact_8_asset_holdout_impossible": "PEPE historical availability plus SMA200 warmup leaves no older unseen exact-universe interval.",
        "requested_range": {
            "start": REQUEST_START.date().isoformat(),
            "end": HOLDOUT_END.date().isoformat(),
        },
        "actual_common_panel": data_meta["panel"],
        "eligible_anchor": anchor.date().isoformat(),
        "frozen_rules": {
            "lookback": LOOKBACK,
            "arm_threshold": ARM_THRESHOLD,
            "reversal": REVERSAL,
            "sma_days": SMA_DAYS,
            "vol_days": VOL_DAYS,
            "enter_breadth_max": ENTER_BREADTH_MAX,
            "exit_breadth_min": EXIT_BREADTH_MIN,
            "confirm_days": CONFIRM_DAYS,
            "transition_cost": TRANSITION_COST,
        },
        "frozen_btc_candidates_in_original_development_order": [
            {"fast": f, "slow": s} for f, s in FROZEN_BTC_CANDIDATES
        ],
        "full_holdout_summary": summaries,
        "robustness_summary": robust_summary,
        "data": data_meta,
        "interpretation_boundary": [
            "No candidate was added or retuned on holdout.",
            "Development ranking order was preserved.",
            "Universe differs from current 8-asset strategy because PEPE is historically unavailable.",
            "This tests regime portability, not exact current-strategy validation.",
            "No production or paper-live behavior is changed.",
        ],
    }
    _write_json(run_dir / "summary.json", report)

    print(json.dumps(_json_safe(report), indent=2, sort_keys=True))
    print(f"output={run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
