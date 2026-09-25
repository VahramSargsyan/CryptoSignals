from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, field
from hashlib import sha256
from typing import Any, Mapping

ENTRY_RULE_NEXT_CANDLE_OPEN = "NEXT_CANDLE_OPEN"


@dataclass(frozen=True)
class ExecutionPolicy:
    signal_calculated_at: str = "CANDLE_CLOSE"
    entry_rule: str = ENTRY_RULE_NEXT_CANDLE_OPEN
    fee_bps: float = 10.0
    slippage_bps: float = 5.0

    def __post_init__(self) -> None:
        if self.entry_rule != ENTRY_RULE_NEXT_CANDLE_OPEN:
            raise ValueError("Foundation currently supports NEXT_CANDLE_OPEN only")
        if not math.isfinite(self.fee_bps) or not math.isfinite(self.slippage_bps):
            raise ValueError("Fees and slippage must be finite")
        if self.fee_bps < 0 or self.slippage_bps < 0:
            raise ValueError("Fees and slippage must be non-negative")


@dataclass(frozen=True)
class BacktestRunManifest:
    strategy_id: str
    strategy_version: str
    source_commit_sha: str
    dataset_id: str
    symbol: str
    timeframe: str
    period_start: str
    period_end: str
    execution: ExecutionPolicy = field(default_factory=ExecutionPolicy)
    parameters: Mapping[str, Any] = field(default_factory=dict)
    validation_type: str = "RESEARCH"

    def canonical_payload(self) -> str:
        payload = asdict(self)
        payload["parameters"] = dict(sorted(self.parameters.items()))
        return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)

    @property
    def run_id(self) -> str:
        digest = sha256(self.canonical_payload().encode("utf-8")).hexdigest()[:20]
        return f"RUN-{digest}"
