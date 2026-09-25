from __future__ import annotations

import json
import math
import numbers
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from core.backtest.contracts import BacktestRunManifest
from core.backtest.trading import LongOnlyTradingPolicy, TradingBacktestResult

EVIDENCE_SCHEMA_VERSION = "1.0.0"
OPTIONAL_TRADE_FIELDS = ("exit_signal_id", "exit_signal_timestamp", "exit_strength")


def _json_safe(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if is_dataclass(value):
        return _json_safe(asdict(value))
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, numbers.Real) and not isinstance(value, bool):
        numeric = float(value)
        if not math.isfinite(numeric):
            raise ValueError("Evidence cannot contain NaN or Infinity")
        if hasattr(value, "item"):
            return value.item()
        return value
    if hasattr(value, "item"):
        return _json_safe(value.item())
    return value


def _normalize_trade_record(record: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    for field in OPTIONAL_TRADE_FIELDS:
        value = normalized.get(field)
        if value is None or value is pd.NA or value is pd.NaT:
            normalized[field] = None
            continue
        if isinstance(value, numbers.Real) and not isinstance(value, bool):
            numeric = float(value)
            if math.isnan(numeric):
                normalized[field] = None
    return normalized


def build_backtest_evidence(
    *,
    manifest: BacktestRunManifest,
    policy: LongOnlyTradingPolicy,
    result: TradingBacktestResult,
    dataset_quality: Any = None,
    created_at: Optional[Any] = None,
) -> dict:
    if result.run_id != manifest.run_id:
        raise ValueError("Backtest result run_id does not match manifest")

    timestamp = pd.Timestamp.now(tz="UTC") if created_at is None else pd.Timestamp(created_at)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")

    trades = []
    if not result.trades.empty:
        trades = [
            _json_safe(_normalize_trade_record(record))
            for record in result.trades.to_dict("records")
        ]

    return {
        "evidence_schema_version": EVIDENCE_SCHEMA_VERSION,
        "created_at": timestamp.isoformat(),
        "run_id": manifest.run_id,
        "manifest": json.loads(manifest.canonical_payload()),
        "trading_policy": policy.to_config(),
        "dataset_quality": _json_safe(dataset_quality),
        "metrics": _json_safe(result.metrics),
        "trade_count": int(result.metrics.trade_count),
        "trades": trades,
    }


def write_backtest_evidence(path: str | Path, evidence: dict) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(
        _json_safe(evidence),
        sort_keys=True,
        indent=2,
        allow_nan=False,
    )
    destination.write_text(payload + "\n", encoding="utf-8")
    return destination
