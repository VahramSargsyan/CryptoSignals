from __future__ import annotations

import math

import pandas as pd

from core.contracts.strategy import StrategyOutput, make_strategy_output
from core.indicators.technical import build_standard_features

STRATEGY_ID = "WEIGHTED_MULTI_SIGNAL_V2"
STRATEGY_NAME = "WEIGHTED_MULTI_SIGNAL_CORE_CONFIRMATION"
STRATEGY_VERSION = "2.0.0"

CORE_BASE_STRENGTH = 80.0
MA_ALIGNED_STRENGTH = 100.0
MIN_CANDLE_BODY_STRENGTH = 0.5

REQUIRED_COLUMNS = {"timestamp", "open", "high", "low", "close", "volume"}


def _finite(*values: float) -> bool:
    return all(pd.notna(value) and math.isfinite(float(value)) for value in values)


def _core_direction(row: pd.Series) -> str | None:
    if not _finite(
        row.get("macd"),
        row.get("macd_signal"),
        row.get("stoch_rsi_k"),
        row.get("stoch_rsi_d"),
        row.get("volume"),
        row.get("volume_ma"),
        row.get("open"),
        row.get("close"),
        row.get("candle_body_strength"),
    ):
        return None

    active_volume = float(row["volume"]) > float(row["volume_ma"])
    strong_body = float(row["candle_body_strength"]) >= MIN_CANDLE_BODY_STRENGTH
    if not (active_volume and strong_body):
        return None

    bullish = (
        float(row["macd"]) > float(row["macd_signal"])
        and float(row["stoch_rsi_k"]) > float(row["stoch_rsi_d"])
        and float(row["close"]) > float(row["open"])
    )
    bearish = (
        float(row["macd"]) < float(row["macd_signal"])
        and float(row["stoch_rsi_k"]) < float(row["stoch_rsi_d"])
        and float(row["close"]) < float(row["open"])
    )
    if bullish:
        return "BUY"
    if bearish:
        return "SELL"
    return None


def _ma_context(row: pd.Series, signal: str) -> str:
    if not _finite(row.get("close"), row.get("sma_200")):
        return "MA_CONTEXT_UNAVAILABLE"

    if signal == "BUY":
        aligned = (
            bool(row.get("ma_bull_stack", False))
            and float(row["close"]) > float(row["sma_200"])
        )
        return "MA_CONTEXT_ALIGNED" if aligned else "MA_CONTEXT_NOT_ALIGNED"

    aligned = (
        bool(row.get("ma_bear_stack", False))
        and float(row["close"]) < float(row["sma_200"])
    )
    return "MA_CONTEXT_ALIGNED" if aligned else "MA_CONTEXT_NOT_ALIGNED"


def generate_weighted_multi_signal_v2_outputs(
    candles: pd.DataFrame,
    *,
    symbol: str,
    timeframe: str,
    source_commit_sha: str,
    run_id: str | None = None,
    include_hold: bool = False,
) -> list[StrategyOutput]:
    missing = sorted(REQUIRED_COLUMNS.difference(candles.columns))
    if missing:
        raise ValueError(f"Missing canonical candle columns: {missing}")

    features = build_standard_features(candles)
    working = candles.copy().join(features)
    outputs: list[StrategyOutput] = []

    for _, row in working.iterrows():
        signal = _core_direction(row)
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
                    reasons=("core_confirmation_not_present",),
                    source_commit_sha=source_commit_sha,
                    run_id=run_id,
                )
            )
            continue

        ma_context = _ma_context(row, signal)
        strength = (
            MA_ALIGNED_STRENGTH
            if ma_context == "MA_CONTEXT_ALIGNED"
            else CORE_BASE_STRENGTH
        )
        outputs.append(
            make_strategy_output(
                strategy_id=STRATEGY_ID,
                strategy_version=STRATEGY_VERSION,
                symbol=symbol,
                timeframe=timeframe,
                timestamp=row["timestamp"],
                signal=signal,
                strength=strength,
                reasons=(
                    "core_macd_confirmed",
                    "core_stochrsi_confirmed",
                    "core_volume_candle_confirmed",
                    f"candle_body_min_{MIN_CANDLE_BODY_STRENGTH:.2f}",
                    ma_context.lower(),
                ),
                source_commit_sha=source_commit_sha,
                run_id=run_id,
            )
        )

    return outputs
