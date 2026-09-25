from __future__ import annotations

from typing import Iterable

import pandas as pd

from core.contracts.strategy import StrategyOutput, make_strategy_output
from strategies.crypto.vahram_original.strategy import (
    STRATEGY_ID,
    STRATEGY_VERSION,
    apply_legacy_indicators,
    calculate_signal_strength,
)

REQUIRED_COLUMNS = {"timestamp", "open", "high", "low", "close", "volume"}


def _to_legacy_frame(candles: pd.DataFrame) -> pd.DataFrame:
    missing = sorted(REQUIRED_COLUMNS.difference(candles.columns))
    if missing:
        raise ValueError(f"Missing canonical candle columns: {missing}")
    return candles.rename(
        columns={
            "timestamp": "Date",
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
        }
    ).copy()


def generate_vahram_original_outputs(
    candles: pd.DataFrame,
    *,
    symbol: str,
    timeframe: str,
    source_commit_sha: str,
    run_id: str | None = None,
    include_hold: bool = False,
) -> list[StrategyOutput]:
    legacy = apply_legacy_indicators(_to_legacy_frame(candles))
    outputs: list[StrategyOutput] = []

    for _, row in legacy.iterrows():
        signal, strength = calculate_signal_strength(row)
        if signal is None:
            if not include_hold:
                continue
            outputs.append(
                make_strategy_output(
                    strategy_id=STRATEGY_ID,
                    strategy_version=STRATEGY_VERSION,
                    symbol=symbol,
                    timeframe=timeframe,
                    timestamp=row["Date"],
                    signal="HOLD",
                    strength=0.0,
                    reasons=("legacy_conditions_not_met",),
                    source_commit_sha=source_commit_sha,
                    run_id=run_id,
                )
            )
            continue

        reasons = (
            "close_at_or_beyond_bollinger_band",
            "vahram_close_range_threshold",
            "volume_above_20_period_mean",
            "candle_body_strength_at_least_0_2",
        )
        outputs.append(
            make_strategy_output(
                strategy_id=STRATEGY_ID,
                strategy_version=STRATEGY_VERSION,
                symbol=symbol,
                timeframe=timeframe,
                timestamp=row["Date"],
                signal=signal,
                strength=strength,
                reasons=reasons,
                source_commit_sha=source_commit_sha,
                run_id=run_id,
            )
        )

    return outputs
