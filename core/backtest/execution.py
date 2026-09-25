from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .contracts import ExecutionPolicy


@dataclass(frozen=True)
class EntryFill:
    side: str
    signal_timestamp: pd.Timestamp
    entry_timestamp: pd.Timestamp
    raw_open_price: float
    execution_price: float
    slippage_bps: float


def next_candle_open_fill(
    candles: pd.DataFrame,
    *,
    signal_position: int,
    side: str,
    policy: ExecutionPolicy,
) -> EntryFill:
    """Execute a close-generated signal only at the following candle open."""
    if signal_position < 0 or signal_position >= len(candles):
        raise IndexError("signal_position is outside candle data")
    if signal_position + 1 >= len(candles):
        raise IndexError("No next candle is available for execution")
    if side not in {"BUY", "SELL"}:
        raise ValueError("side must be BUY or SELL")

    signal_row = candles.iloc[signal_position]
    entry_row = candles.iloc[signal_position + 1]
    raw_open = float(entry_row["open"])
    slip = policy.slippage_bps / 10_000.0
    execution_price = raw_open * (1.0 + slip if side == "BUY" else 1.0 - slip)
    return EntryFill(
        side=side,
        signal_timestamp=pd.Timestamp(signal_row["timestamp"]),
        entry_timestamp=pd.Timestamp(entry_row["timestamp"]),
        raw_open_price=raw_open,
        execution_price=execution_price,
        slippage_bps=policy.slippage_bps,
    )
