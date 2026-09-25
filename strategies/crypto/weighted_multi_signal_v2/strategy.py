from __future__ import annotations

import math
from dataclasses import dataclass

import pandas as pd

from core.contracts.strategy import StrategyOutput, make_strategy_output
from core.indicators.technical import build_standard_features

STRATEGY_ID = "WEIGHTED_MULTI_SIGNAL_V2"
STRATEGY_NAME = "WEIGHTED_MULTI_SIGNAL_CORE_GATED"
STRATEGY_VERSION = "2.0.0"

CORE_BASE_STRENGTH = 75.0
MA_CONFIRM_BONUS = 25.0
MIN_BODY_STRENGTH = 0.50

REQUIRED_COLUMNS = {"timestamp", "open", "high", "low", "close", "volume"}


@dataclass(frozen=True)
class CoreConsensus:
    bullish_core: bool
    bearish_core: bool
    bullish_ma_confirmed: bool
    bearish_ma_confirmed: bool


def _finite(*values: float) -> bool:
    return all(pd.notna(value) and math.isfinite(float(value)) for value in values)


def evaluate_core_consensus(row: pd.Series) -> CoreConsensus:
    macd_bull = False
    macd_bear = False
    if _finite(row.get("macd"), row.get("macd_signal")):
        macd_bull = float(row["macd"]) > float(row["macd_signal"])
        macd_bear = float(row["macd"]) < float(row["macd_signal"])

    stoch_bull = False
    stoch_bear = False
    if _finite(row.get("stoch_rsi_k"), row.get("stoch_rsi_d")):
        stoch_bull = float(row["stoch_rsi_k"]) > float(row["stoch_rsi_d"])
        stoch_bear = float(row["stoch_rsi_k"]) < float(row["stoch_rsi_d"])

    volume_bull = False
    volume_bear = False
    if _finite(
        row.get("volume"),
        row.get("volume_ma"),
        row.get("open"),
        row.get("close"),
        row.get("candle_body_strength"),
    ):
        active_volume = float(row["volume"]) > float(row["volume_ma"])
        strong_body = float(row["candle_body_strength"]) >= MIN_BODY_STRENGTH
        if active_volume and strong_body:
            volume_bull = float(row["close"]) > float(row["open"])
            volume_bear = float(row["close"]) < float(row["open"])

    bullish_core = macd_bull and stoch_bull and volume_bull
    bearish_core = macd_bear and stoch_bear and volume_bear

    bullish_ma = False
    bearish_ma = False
    if _finite(row.get("close"), row.get("sma_200")):
        bullish_ma = (
            bool(row.get("ma_bull_stack", False))
            and float(row["close"]) > float(row["sma_200"])
        )
        bearish_ma = (
            bool(row.get("ma_bear_stack", False))
            and float(row["close"]) < float(row["sma_200"])
        )

    return CoreConsensus(
        bullish_core=bullish_core,
        bearish_core=bearish_core,
        bullish_ma_confirmed=bullish_core and bullish_ma,
        bearish_ma_confirmed=bearish_core and bearish_ma,
    )


def _signal_and_strength(consensus: CoreConsensus) -> tuple[str | None, float]:
    if consensus.bullish_core and not consensus.bearish_core:
        strength = CORE_BASE_STRENGTH + (
            MA_CONFIRM_BONUS if consensus.bullish_ma_confirmed else 0.0
        )
        return "BUY", strength

    if consensus.bearish_core and not consensus.bullish_core:
        strength = CORE_BASE_STRENGTH + (
            MA_CONFIRM_BONUS if consensus.bearish_ma_confirmed else 0.0
        )
        return "SELL", strength

    return None, 0.0


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
        consensus = evaluate_core_consensus(row)
        signal, strength = _signal_and_strength(consensus)

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
                    reasons=("core_consensus_not_met",),
                    source_commit_sha=source_commit_sha,
                    run_id=run_id,
                )
            )
            continue

        ma_confirmed = (
            consensus.bullish_ma_confirmed
            if signal == "BUY"
            else consensus.bearish_ma_confirmed
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
                    "core_macd",
                    "core_stochrsi",
                    "core_volume_candle",
                    "ma_context_confirmed" if ma_confirmed else "ma_context_not_confirmed",
                    f"body_strength_min_{MIN_BODY_STRENGTH:.2f}",
                ),
                source_commit_sha=source_commit_sha,
                run_id=run_id,
            )
        )

    return outputs
