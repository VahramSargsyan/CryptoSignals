from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Protocol, Sequence

import pandas as pd

from core.data.candles import (
    HistoricalDataset,
    build_dataset_id,
    canonical_sort,
    keep_closed_candles,
    validate_candles,
)

BINANCE_INTERVALS = {
    "1D": "1d",
    "4H": "4h",
    "1H": "1h",
}


class HistoricalKlineClient(Protocol):
    def get_historical_klines(
        self,
        symbol: str,
        interval: str,
        start_str: Any,
        end_str: Any = None,
    ) -> Sequence[Sequence[Any]]:
        ...


@dataclass(frozen=True)
class BinanceDownloadMetadata:
    symbol: str
    timeframe: str
    requested_start: str
    requested_end: Optional[str]
    actual_start: Optional[str]
    actual_end: Optional[str]
    listing_truncated: bool
    raw_rows: int
    closed_rows: int
    status: str


@dataclass(frozen=True)
class BinanceDownloadResult:
    metadata: BinanceDownloadMetadata
    dataset: Optional[HistoricalDataset]


def _utc_timestamp(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def _epoch_ms(value: pd.Timestamp) -> int:
    return int(value.timestamp() * 1000)


def parse_binance_klines(rows: Sequence[Sequence[Any]]) -> pd.DataFrame:
    records = []
    for row in rows:
        if len(row) < 6:
            raise ValueError("Binance kline row must contain at least 6 fields")
        records.append(
            {
                "timestamp": pd.to_datetime(int(row[0]), unit="ms", utc=True),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
            }
        )
    return pd.DataFrame(
        records,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )


def download_historical_dataset(
    client: HistoricalKlineClient,
    *,
    symbol: str,
    start: Any,
    end: Any = None,
    timeframe: str = "1D",
    as_of: Any = None,
) -> BinanceDownloadResult:
    """Download listing-aware Binance history into the shared canonical candle contract."""
    timeframe = timeframe.upper()
    if timeframe not in BINANCE_INTERVALS:
        raise ValueError(f"Unsupported Binance timeframe: {timeframe}")

    requested_start = _utc_timestamp(start)
    requested_end = _utc_timestamp(end) if end is not None else None
    if requested_end is not None and requested_end < requested_start:
        raise ValueError("end must be greater than or equal to start")

    interval = BINANCE_INTERVALS[timeframe]
    if requested_end is None:
        rows = client.get_historical_klines(
            symbol.upper(),
            interval,
            _epoch_ms(requested_start),
        )
    else:
        rows = client.get_historical_klines(
            symbol.upper(),
            interval,
            _epoch_ms(requested_start),
            _epoch_ms(requested_end),
        )

    raw_rows = len(rows)
    parsed = parse_binance_klines(rows)
    if parsed.empty:
        metadata = BinanceDownloadMetadata(
            symbol=symbol.upper(),
            timeframe=timeframe,
            requested_start=requested_start.isoformat(),
            requested_end=requested_end.isoformat() if requested_end is not None else None,
            actual_start=None,
            actual_end=None,
            listing_truncated=False,
            raw_rows=0,
            closed_rows=0,
            status="NO_DATA",
        )
        return BinanceDownloadResult(metadata=metadata, dataset=None)

    closed = keep_closed_candles(parsed, timeframe=timeframe, as_of=as_of)
    if closed.empty:
        metadata = BinanceDownloadMetadata(
            symbol=symbol.upper(),
            timeframe=timeframe,
            requested_start=requested_start.isoformat(),
            requested_end=requested_end.isoformat() if requested_end is not None else None,
            actual_start=None,
            actual_end=None,
            listing_truncated=False,
            raw_rows=raw_rows,
            closed_rows=0,
            status="NO_CLOSED_DATA",
        )
        return BinanceDownloadResult(metadata=metadata, dataset=None)

    canonical = canonical_sort(closed)
    quality = validate_candles(canonical, symbol=symbol.upper(), timeframe=timeframe)
    dataset_id = build_dataset_id(
        canonical,
        source="BINANCE",
        symbol=symbol.upper(),
        timeframe=timeframe,
    )
    actual_start = pd.Timestamp(canonical.iloc[0]["timestamp"])
    actual_end = pd.Timestamp(canonical.iloc[-1]["timestamp"])
    metadata = BinanceDownloadMetadata(
        symbol=symbol.upper(),
        timeframe=timeframe,
        requested_start=requested_start.isoformat(),
        requested_end=requested_end.isoformat() if requested_end is not None else None,
        actual_start=actual_start.isoformat(),
        actual_end=actual_end.isoformat(),
        listing_truncated=actual_start > requested_start,
        raw_rows=raw_rows,
        closed_rows=len(canonical),
        status="OK",
    )
    dataset = HistoricalDataset(
        symbol=symbol.upper(),
        timeframe=timeframe,
        source="BINANCE",
        dataset_id=dataset_id,
        candles=canonical,
        quality=quality,
    )
    return BinanceDownloadResult(metadata=metadata, dataset=dataset)
