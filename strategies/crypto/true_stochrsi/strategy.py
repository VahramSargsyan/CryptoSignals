from __future__ import annotations

import pandas as pd

from core.contracts.strategy import StrategyOutput, make_strategy_output
from core.indicators.technical import build_standard_features

STRATEGY_ID = "VAHRAM_TRUE_STOCHRSI_V2"
STRATEGY_NAME = "VAHRAM_TRUE_STOCHRSI"
STRATEGY_VERSION = "2.0.0"
REQUIRED_COLUMNS = {"timestamp", "open", "high", "low", "close", "volume"}


def _calculate_strength(row: pd.Series) -> tuple[str | None, float]:
    body_strength = float(row["candle_body_strength"])
    if body_strength < 0.2:
        return None, 0.0

    stoch = row["stoch_rsi_k"]
    if pd.isna(stoch):
        return None, 0.0

    strength = 0.0
    if (
        row["close"] <= row["bb_lower"]
        and stoch < 20
        and row["volume"] > row["volume_ma"]
    ):
        strength += (20 - stoch) * 2
        bb_diff = (row["bb_lower"] - row["close"]) / row["bb_lower"]
        strength += min(bb_diff * 100, 30)
        vol_boost = (row["volume"] - row["volume_ma"]) / row["volume_ma"]
        strength += min(vol_boost * 100, 30)
        strength += body_strength * 10
        return "BUY", round(min(strength, 100), 1)

    if (
        row["close"] >= row["bb_upper"]
        and stoch > 80
        and row["volume"] > row["volume_ma"]
    ):
        strength += (stoch - 80) * 2
        bb_diff = (row["close"] - row["bb_upper"]) / row["bb_upper"]
        strength += min(bb_diff * 100, 30)
        vol_boost = (row["volume"] - row["volume_ma"]) / row["volume_ma"]
        strength += min(vol_boost * 100, 30)
        strength += body_strength * 10
        return "SELL", round(min(strength, 100), 1)

    return None, 0.0


def generate_true_stochrsi_outputs(
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
        signal, strength = _calculate_strength(row)
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
                    reasons=("true_stochrsi_conditions_not_met",),
                    source_commit_sha=source_commit_sha,
                    run_id=run_id,
                )
            )
            continue

        reasons = (
            "close_at_or_beyond_bollinger_band",
            "true_stoch_rsi_k_threshold",
            "volume_above_20_period_mean",
            "candle_body_strength_at_least_0_2",
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
                reasons=reasons,
                source_commit_sha=source_commit_sha,
                run_id=run_id,
            )
        )

    return outputs
