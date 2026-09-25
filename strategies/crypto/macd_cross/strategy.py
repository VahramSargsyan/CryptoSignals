from __future__ import annotations

import math

import pandas as pd

from core.contracts.strategy import StrategyOutput, make_strategy_output
from core.indicators.technical import macd

STRATEGY_ID = "MACD_CROSS_V1"
STRATEGY_NAME = "MACD_CROSS"
STRATEGY_VERSION = "1.0.0"

DEFAULT_FAST_WINDOW = 12
DEFAULT_SLOW_WINDOW = 26
DEFAULT_SIGNAL_WINDOW = 9

REQUIRED_COLUMNS = {"timestamp", "open", "high", "low", "close", "volume"}


def _cross_signal(
    previous_macd: float,
    previous_signal: float,
    current_macd: float,
    current_signal: float,
) -> str | None:
    values = (previous_macd, previous_signal, current_macd, current_signal)
    if any(pd.isna(value) or not math.isfinite(float(value)) for value in values):
        return None
    if previous_macd < previous_signal and current_macd > current_signal:
        return "BUY"
    if previous_macd > previous_signal and current_macd < current_signal:
        return "SELL"
    return None


def generate_macd_cross_outputs(
    candles: pd.DataFrame,
    *,
    symbol: str,
    timeframe: str,
    source_commit_sha: str,
    run_id: str | None = None,
    include_hold: bool = False,
    fast_window: int = DEFAULT_FAST_WINDOW,
    slow_window: int = DEFAULT_SLOW_WINDOW,
    signal_window: int = DEFAULT_SIGNAL_WINDOW,
) -> list[StrategyOutput]:
    missing = sorted(REQUIRED_COLUMNS.difference(candles.columns))
    if missing:
        raise ValueError(f"Missing canonical candle columns: {missing}")

    values = macd(
        candles["close"],
        fast_window=fast_window,
        slow_window=slow_window,
        signal_window=signal_window,
    )
    working = candles.copy().join(values[["macd", "macd_signal"]])
    outputs: list[StrategyOutput] = []

    for position in range(len(working)):
        row = working.iloc[position]
        signal = None
        if position > 0:
            previous = working.iloc[position - 1]
            signal = _cross_signal(
                previous["macd"],
                previous["macd_signal"],
                row["macd"],
                row["macd_signal"],
            )

        if signal is None:
            if not include_hold:
                continue
            outputs.append(
                make_strategy_output(
                    strategy_id=STRATEGY_ID,
                    strategy_version=STRATEGY_VERSION,
                    symbol=symbol,
                    timeframe=timeframe,
                    timestamp=row["timestamp"],
                    signal="HOLD",
                    strength=0.0,
                    reasons=("macd_signal_cross_not_present",),
                    source_commit_sha=source_commit_sha,
                    run_id=run_id,
                )
            )
            continue

        outputs.append(
            make_strategy_output(
                strategy_id=STRATEGY_ID,
                strategy_version=STRATEGY_VERSION,
                symbol=symbol,
                timeframe=timeframe,
                timestamp=row["timestamp"],
                signal=signal,
                strength=100.0,
                reasons=(
                    "macd_crossed_signal_line",
                    "binary_source_signal_no_native_strength",
                    f"fast_window_{fast_window}",
                    f"slow_window_{slow_window}",
                    f"signal_window_{signal_window}",
                ),
                source_commit_sha=source_commit_sha,
                run_id=run_id,
            )
        )

    return outputs
