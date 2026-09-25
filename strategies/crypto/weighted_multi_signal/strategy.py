from __future__ import annotations

import math
from dataclasses import dataclass

import pandas as pd

from core.contracts.strategy import StrategyOutput, make_strategy_output
from core.indicators.technical import build_standard_features

STRATEGY_ID = "WEIGHTED_MULTI_SIGNAL_V1"
STRATEGY_NAME = "WEIGHTED_MULTI_SIGNAL"
STRATEGY_VERSION = "1.0.0"

BLOCK_WEIGHT = 20.0
SIGNAL_THRESHOLD = 60.0
MIN_TRIGGERED_BLOCKS = 3

REQUIRED_COLUMNS = {"timestamp", "open", "high", "low", "close", "volume"}


@dataclass(frozen=True)
class WeightedScore:
    bullish_score: float
    bearish_score: float
    bullish_blocks: tuple[str, ...]
    bearish_blocks: tuple[str, ...]


def _finite(*values: float) -> bool:
    return all(pd.notna(value) and math.isfinite(float(value)) for value in values)


def score_weighted_row(row: pd.Series) -> WeightedScore:
    bull: list[str] = []
    bear: list[str] = []

    # 1) Long-term trend structure: MA 50 / 100 / 200 + price vs slow trend.
    if (
        bool(row.get("ma_bull_stack", False))
        and _finite(row.get("close"), row.get("sma_200"))
        and float(row["close"]) > float(row["sma_200"])
    ):
        bull.append("ma_trend")
    if (
        bool(row.get("ma_bear_stack", False))
        and _finite(row.get("close"), row.get("sma_200"))
        and float(row["close"]) < float(row["sma_200"])
    ):
        bear.append("ma_trend")

    # 2) MACD momentum state.
    if _finite(row.get("macd"), row.get("macd_signal")):
        if float(row["macd"]) > float(row["macd_signal"]):
            bull.append("macd")
        elif float(row["macd"]) < float(row["macd_signal"]):
            bear.append("macd")

    # 3) Canonical StochRSI K/D state.
    if _finite(row.get("stoch_rsi_k"), row.get("stoch_rsi_d")):
        if float(row["stoch_rsi_k"]) > float(row["stoch_rsi_d"]):
            bull.append("stochrsi")
        elif float(row["stoch_rsi_k"]) < float(row["stoch_rsi_d"]):
            bear.append("stochrsi")

    # 4) Bollinger location is intentionally mean-reversion oriented.
    if _finite(row.get("close"), row.get("bb_lower"), row.get("bb_upper")):
        close = float(row["close"])
        if close <= float(row["bb_lower"]):
            bull.append("bollinger_reversion")
        elif close >= float(row["bb_upper"]):
            bear.append("bollinger_reversion")

    # 5) Participation confirmation: above-average volume + directional strong body.
    if _finite(
        row.get("volume"),
        row.get("volume_ma"),
        row.get("open"),
        row.get("close"),
        row.get("candle_body_strength"),
    ):
        active_volume = float(row["volume"]) > float(row["volume_ma"])
        strong_body = float(row["candle_body_strength"]) >= 0.5
        if active_volume and strong_body:
            if float(row["close"]) > float(row["open"]):
                bull.append("volume_candle")
            elif float(row["close"]) < float(row["open"]):
                bear.append("volume_candle")

    return WeightedScore(
        bullish_score=len(bull) * BLOCK_WEIGHT,
        bearish_score=len(bear) * BLOCK_WEIGHT,
        bullish_blocks=tuple(bull),
        bearish_blocks=tuple(bear),
    )


def _signal_from_score(score: WeightedScore) -> str | None:
    bull_ready = (
        score.bullish_score >= SIGNAL_THRESHOLD
        and len(score.bullish_blocks) >= MIN_TRIGGERED_BLOCKS
        and score.bullish_score > score.bearish_score
    )
    bear_ready = (
        score.bearish_score >= SIGNAL_THRESHOLD
        and len(score.bearish_blocks) >= MIN_TRIGGERED_BLOCKS
        and score.bearish_score > score.bullish_score
    )
    if bull_ready:
        return "BUY"
    if bear_ready:
        return "SELL"
    return None


def generate_weighted_multi_signal_outputs(
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
        score = score_weighted_row(row)
        signal = _signal_from_score(score)

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
                    strength=max(score.bullish_score, score.bearish_score),
                    reasons=(
                        f"bull_score_{score.bullish_score:.0f}",
                        f"bear_score_{score.bearish_score:.0f}",
                        "weighted_threshold_not_met",
                    ),
                    source_commit_sha=source_commit_sha,
                    run_id=run_id,
                )
            )
            continue

        selected_blocks = score.bullish_blocks if signal == "BUY" else score.bearish_blocks
        selected_score = score.bullish_score if signal == "BUY" else score.bearish_score
        outputs.append(
            make_strategy_output(
                strategy_id=STRATEGY_ID,
                strategy_version=STRATEGY_VERSION,
                symbol=symbol,
                timeframe=timeframe,
                timestamp=row["timestamp"],
                signal=signal,
                strength=selected_score,
                reasons=(
                    f"weighted_score_{selected_score:.0f}",
                    f"triggered_blocks_{len(selected_blocks)}",
                    *tuple(f"block_{name}" for name in selected_blocks),
                ),
                source_commit_sha=source_commit_sha,
                run_id=run_id,
            )
        )

    return outputs
