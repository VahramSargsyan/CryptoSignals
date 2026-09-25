from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
from typing import Optional, Sequence

import pandas as pd

VALID_SIGNALS = {"BUY", "SELL", "HOLD"}


@dataclass(frozen=True)
class StrategyOutput:
    output_id: str
    strategy_id: str
    strategy_version: str
    symbol: str
    timeframe: str
    timestamp: pd.Timestamp
    signal: str
    strength: float
    reasons: tuple[str, ...]
    run_id: Optional[str]
    source_commit_sha: str

    def to_record(self) -> dict:
        record = asdict(self)
        record["timestamp"] = self.timestamp.isoformat()
        record["reasons"] = list(self.reasons)
        return record


def _utc_timestamp(value) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def build_output_id(
    *,
    strategy_id: str,
    strategy_version: str,
    symbol: str,
    timeframe: str,
    timestamp,
    signal: str,
) -> str:
    normalized_timestamp = _utc_timestamp(timestamp)
    payload = "|".join(
        (
            strategy_id,
            strategy_version,
            symbol.upper(),
            timeframe.upper(),
            normalized_timestamp.isoformat(),
            signal.upper(),
        )
    )
    return "OUT-" + sha256(payload.encode("utf-8")).hexdigest()[:24]


def make_strategy_output(
    *,
    strategy_id: str,
    strategy_version: str,
    symbol: str,
    timeframe: str,
    timestamp,
    signal: str,
    strength: float,
    reasons: Sequence[str],
    source_commit_sha: str,
    run_id: Optional[str] = None,
) -> StrategyOutput:
    signal = signal.upper()
    if signal not in VALID_SIGNALS:
        raise ValueError(f"Unsupported signal: {signal}")
    if not 0 <= float(strength) <= 100:
        raise ValueError("strength must be between 0 and 100")
    normalized_timestamp = _utc_timestamp(timestamp)
    return StrategyOutput(
        output_id=build_output_id(
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            symbol=symbol,
            timeframe=timeframe,
            timestamp=normalized_timestamp,
            signal=signal,
        ),
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        symbol=symbol.upper(),
        timeframe=timeframe.upper(),
        timestamp=normalized_timestamp,
        signal=signal,
        strength=float(strength),
        reasons=tuple(reasons),
        run_id=run_id,
        source_commit_sha=source_commit_sha,
    )
