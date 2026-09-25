from __future__ import annotations

import math

import pandas as pd

from core.contracts.strategy import StrategyOutput, make_strategy_output
from core.indicators.technical import true_stoch_rsi

STRATEGY_ID = "STOCHRSI_CROSS_V1"
STRATEGY_NAME = "STOCHRSI_CROSS"
STRATEGY_VERSION = "1.0.0"

DEFAULT_RSI_WINDOW = 14
DEFAULT_STOCH_WINDOW = 14
DEFAULT_SMOOTH_K = 10
DEFAULT_SMOOTH_D = 3

REQUIRED_COLUMNS = {"timestamp", "open", "high", "low", "close", "volume"}


def _cross_signal(
    previous_k: float,
    previous_d: float,
    current_k: float,
    current_d: float,
) -> str | None:
    values = (previous_k, previous_d, current_k, current_d)
    if any(pd.isna(value) or not math.isfinite(float(value)) for value in values):
        return None
    if previous_k < previous_d and current_k > current_d:
        return "BUY"
    if previous_k > previous_d and current_k < current_d:
        return "SELL"
    return None


def generate_stochrsi_cross_outputs(
    candles: pd.DataFrame,
    *,
    symbol: str,
    timeframe: str,
    source_commit_sha: str,
    run_id: str | None = None,
    include_hold: bool = False,
    rsi_window: int = DEFAULT_RSI_WINDOW,
    stoch_window: int = DEFAULT_STOCH_WINDOW,
    smooth_k: int = DEFAULT_SMOOTH_K,
    smooth_d: int = DEFAULT_SMOOTH_D,
) -> list[StrategyOutput]:
    """Emit historical StochRSI K/D crossover events without look-ahead.

    The signal predicate is adapted from w1ld3r/crypto-signal's StochRSICross
    defaults. Indicator calculation is project-owned and uses the canonical
    local Wilder RSI / StochRSI implementation.
    """
    missing = sorted(REQUIRED_COLUMNS.difference(candles.columns))
    if missing:
        raise ValueError(f"Missing canonical candle columns: {missing}")

    stoch = true_stoch_rsi(
        candles["close"],
        rsi_window=rsi_window,
        stoch_window=stoch_window,
        smooth_k=smooth_k,
        smooth_d=smooth_d,
    )
    working = candles.copy().join(stoch[["stoch_rsi_k", "stoch_rsi_d"]])
    outputs: list[StrategyOutput] = []

    for position in range(len(working)):
        row = working.iloc[position]
        signal = None
        if position > 0:
            previous = working.iloc[position - 1]
            signal = _cross_signal(
                previous["stoch_rsi_k"],
                previous["stoch_rsi_d"],
                row["stoch_rsi_k"],
                row["stoch_rsi_d"],
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
                    reasons=("stochrsi_k_d_cross_not_present",),
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
                    "stochrsi_k_crossed_d",
                    "binary_source_signal_no_native_strength",
                    f"rsi_window_{rsi_window}",
                    f"stoch_window_{stoch_window}",
                    f"smooth_k_{smooth_k}",
                    f"smooth_d_{smooth_d}",
                ),
                source_commit_sha=source_commit_sha,
                run_id=run_id,
            )
        )

    return outputs
