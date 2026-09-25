from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable, Sequence

import pandas as pd

from core.contracts.strategy import StrategyOutput


@dataclass(frozen=True)
class EventStudyObservation:
    output_id: str
    strategy_id: str
    strategy_version: str
    symbol: str
    timeframe: str
    signal_timestamp: pd.Timestamp
    signal: str
    strength: float
    horizon_candles: int
    entry_timestamp: pd.Timestamp
    entry_open: float
    target_timestamp: pd.Timestamp
    target_close: float
    raw_return: float
    directional_return: float

    def to_record(self) -> dict:
        return asdict(self)


def run_event_study(
    candles: pd.DataFrame,
    outputs: Iterable[StrategyOutput],
    *,
    horizons: Sequence[int] = (1, 3, 7, 14),
) -> pd.DataFrame:
    required = {"timestamp", "open", "close"}
    missing = sorted(required.difference(candles.columns))
    if missing:
        raise ValueError(f"Missing candle columns for event study: {missing}")

    normalized_horizons = tuple(sorted(set(int(value) for value in horizons)))
    if not normalized_horizons or any(value <= 0 for value in normalized_horizons):
        raise ValueError("horizons must contain positive integers")

    ordered = candles.sort_values("timestamp", kind="stable").reset_index(drop=True).copy()
    ordered["timestamp"] = pd.to_datetime(ordered["timestamp"], utc=True)
    if ordered["timestamp"].duplicated().any():
        raise ValueError("Event study requires unique candle timestamps")

    positions = {timestamp: position for position, timestamp in enumerate(ordered["timestamp"])}
    observations: list[EventStudyObservation] = []

    for output in outputs:
        if output.signal == "HOLD":
            continue
        signal_timestamp = pd.Timestamp(output.timestamp)
        if signal_timestamp.tzinfo is None:
            signal_timestamp = signal_timestamp.tz_localize("UTC")
        else:
            signal_timestamp = signal_timestamp.tz_convert("UTC")
        if signal_timestamp not in positions:
            raise ValueError(f"Signal timestamp not found in candle dataset: {signal_timestamp}")

        signal_position = positions[signal_timestamp]
        entry_position = signal_position + 1
        if entry_position >= len(ordered):
            continue

        entry_row = ordered.iloc[entry_position]
        entry_open = float(entry_row["open"])
        if entry_open <= 0:
            raise ValueError("entry open must be positive")

        for horizon in normalized_horizons:
            target_position = signal_position + horizon
            if target_position >= len(ordered):
                continue
            target_row = ordered.iloc[target_position]
            target_close = float(target_row["close"])
            raw_return = (target_close / entry_open) - 1.0
            directional_return = raw_return if output.signal == "BUY" else -raw_return
            observations.append(
                EventStudyObservation(
                    output_id=output.output_id,
                    strategy_id=output.strategy_id,
                    strategy_version=output.strategy_version,
                    symbol=output.symbol,
                    timeframe=output.timeframe,
                    signal_timestamp=signal_timestamp,
                    signal=output.signal,
                    strength=output.strength,
                    horizon_candles=horizon,
                    entry_timestamp=pd.Timestamp(entry_row["timestamp"]),
                    entry_open=entry_open,
                    target_timestamp=pd.Timestamp(target_row["timestamp"]),
                    target_close=target_close,
                    raw_return=raw_return,
                    directional_return=directional_return,
                )
            )

    return pd.DataFrame([observation.to_record() for observation in observations])


def summarize_event_study(observations: pd.DataFrame) -> pd.DataFrame:
    if observations.empty:
        return pd.DataFrame(
            columns=[
                "strategy_id",
                "strategy_version",
                "symbol",
                "timeframe",
                "horizon_candles",
                "observation_count",
                "win_rate",
                "average_directional_return",
                "median_directional_return",
            ]
        )

    required = {
        "strategy_id",
        "strategy_version",
        "symbol",
        "timeframe",
        "horizon_candles",
        "directional_return",
    }
    missing = sorted(required.difference(observations.columns))
    if missing:
        raise ValueError(f"Missing event-study columns: {missing}")

    grouped = observations.groupby(
        ["strategy_id", "strategy_version", "symbol", "timeframe", "horizon_candles"],
        dropna=False,
    )
    rows = []
    for keys, group in grouped:
        strategy_id, strategy_version, symbol, timeframe, horizon = keys
        directional = group["directional_return"]
        rows.append(
            {
                "strategy_id": strategy_id,
                "strategy_version": strategy_version,
                "symbol": symbol,
                "timeframe": timeframe,
                "horizon_candles": int(horizon),
                "observation_count": int(len(group)),
                "win_rate": float((directional > 0).mean()),
                "average_directional_return": float(directional.mean()),
                "median_directional_return": float(directional.median()),
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["strategy_id", "symbol", "timeframe", "horizon_candles"],
        kind="stable",
    ).reset_index(drop=True)
