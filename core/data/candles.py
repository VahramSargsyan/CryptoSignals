from __future__ import annotations

import math
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Optional

import pandas as pd

CANONICAL_COLUMNS = ("timestamp", "open", "high", "low", "close", "volume")
LEGACY_COLUMN_MAP = {
    "Date": "timestamp",
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Volume": "volume",
}
TIMEFRAME_DELTAS = {
    "1D": pd.Timedelta(days=1),
    "4H": pd.Timedelta(hours=4),
    "1H": pd.Timedelta(hours=1),
}


@dataclass(frozen=True)
class CandleQualityReport:
    symbol: str
    timeframe: str
    total_rows: int
    start_timestamp: Optional[str]
    end_timestamp: Optional[str]
    duplicate_timestamps: int
    out_of_order_rows: int
    missing_candles: int
    off_grid_timestamps: int
    invalid_ohlc_rows: int
    null_cells: int
    negative_volume_rows: int

    @property
    def has_critical_issues(self) -> bool:
        return any(
            (
                self.duplicate_timestamps,
                self.out_of_order_rows,
                self.missing_candles,
                self.off_grid_timestamps,
                self.invalid_ohlc_rows,
                self.null_cells,
                self.negative_volume_rows,
            )
        )


@dataclass(frozen=True)
class HistoricalDataset:
    symbol: str
    timeframe: str
    source: str
    dataset_id: str
    candles: pd.DataFrame
    quality: CandleQualityReport


def _require_columns(frame: pd.DataFrame) -> None:
    missing = [column for column in CANONICAL_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"Missing candle columns: {missing}")


def normalize_candles(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize legacy/current OHLCV columns without silently repairing row order."""
    normalized = frame.rename(columns=LEGACY_COLUMN_MAP).copy()
    _require_columns(normalized)
    normalized = normalized.loc[:, CANONICAL_COLUMNS]
    normalized["timestamp"] = pd.to_datetime(normalized["timestamp"], utc=True, errors="raise")
    for column in ("open", "high", "low", "close", "volume"):
        normalized[column] = pd.to_numeric(normalized[column], errors="raise")
    return normalized


def validate_candles(frame: pd.DataFrame, symbol: str, timeframe: str) -> CandleQualityReport:
    _require_columns(frame)
    if timeframe not in TIMEFRAME_DELTAS:
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    timestamps = pd.to_datetime(frame["timestamp"], utc=True, errors="raise")
    duplicate_timestamps = int(timestamps.duplicated().sum())
    diffs_in_input_order = timestamps.diff()
    out_of_order_rows = int((diffs_in_input_order < pd.Timedelta(0)).sum())

    expected = TIMEFRAME_DELTAS[timeframe]
    valid_timestamps = timestamps.dropna()
    off_grid_timestamps = int(
        ((valid_timestamps.astype("int64") % expected.value) != 0).sum()
    )

    unique_sorted = pd.Series(valid_timestamps.drop_duplicates().sort_values().to_numpy())
    missing_candles = 0
    if len(unique_sorted) > 1:
        for delta in unique_sorted.diff().dropna():
            if delta > expected:
                missing_candles += max(math.ceil(delta / expected) - 1, 0)

    null_cells = int(frame.loc[:, CANONICAL_COLUMNS].isna().sum().sum())
    numeric = frame.loc[:, ["open", "high", "low", "close", "volume"]]
    invalid_ohlc = (
        (numeric["high"] < numeric[["open", "close", "low"]].max(axis=1))
        | (numeric["low"] > numeric[["open", "close", "high"]].min(axis=1))
    )
    invalid_ohlc_rows = int(invalid_ohlc.sum())
    negative_volume_rows = int((numeric["volume"] < 0).sum())

    start = timestamps.min().isoformat() if len(valid_timestamps) else None
    end = timestamps.max().isoformat() if len(valid_timestamps) else None
    return CandleQualityReport(
        symbol=symbol,
        timeframe=timeframe,
        total_rows=len(frame),
        start_timestamp=start,
        end_timestamp=end,
        duplicate_timestamps=duplicate_timestamps,
        out_of_order_rows=out_of_order_rows,
        missing_candles=missing_candles,
        off_grid_timestamps=off_grid_timestamps,
        invalid_ohlc_rows=invalid_ohlc_rows,
        null_cells=null_cells,
        negative_volume_rows=negative_volume_rows,
    )


def canonical_sort(frame: pd.DataFrame) -> pd.DataFrame:
    _require_columns(frame)
    return frame.sort_values("timestamp", kind="stable").reset_index(drop=True)


def build_dataset_id(frame: pd.DataFrame, source: str, symbol: str, timeframe: str) -> str:
    canonical = canonical_sort(frame)
    payload = canonical.to_csv(index=False, date_format="%Y-%m-%dT%H:%M:%S%z").encode("utf-8")
    digest = sha256(payload).hexdigest()[:16]
    return f"{source.upper()}:{symbol.upper()}:{timeframe.upper()}:{digest}"


def load_csv_dataset(
    path: str | Path,
    *,
    symbol: str,
    timeframe: str = "1D",
    source: str = "BINANCE",
) -> HistoricalDataset:
    raw = pd.read_csv(path)
    normalized = normalize_candles(raw)
    quality = validate_candles(normalized, symbol=symbol, timeframe=timeframe)
    canonical = canonical_sort(normalized)
    dataset_id = build_dataset_id(canonical, source=source, symbol=symbol, timeframe=timeframe)
    return HistoricalDataset(
        symbol=symbol.upper(),
        timeframe=timeframe.upper(),
        source=source.upper(),
        dataset_id=dataset_id,
        candles=canonical,
        quality=quality,
    )


def keep_closed_candles(
    frame: pd.DataFrame,
    *,
    timeframe: str,
    as_of: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    """Keep candles whose full interval ended at or before as_of."""
    _require_columns(frame)
    if timeframe not in TIMEFRAME_DELTAS:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    now = pd.Timestamp.now(tz="UTC") if as_of is None else pd.Timestamp(as_of)
    if now.tzinfo is None:
        now = now.tz_localize("UTC")
    else:
        now = now.tz_convert("UTC")
    closes_at = pd.to_datetime(frame["timestamp"], utc=True) + TIMEFRAME_DELTAS[timeframe]
    return frame.loc[closes_at <= now].reset_index(drop=True)
