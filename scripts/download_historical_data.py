from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pandas as pd

from core.data.candles import TIMEFRAME_DELTAS
from integrations.binance.historical import BinanceDownloadResult, download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient


def _utc_timestamp(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def default_closed_candle_cutoff(timeframe: str, *, now: Any = None) -> pd.Timestamp:
    """Return the UTC opening boundary of the currently open candle."""
    timeframe = timeframe.upper()
    if timeframe not in TIMEFRAME_DELTAS:
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    current = pd.Timestamp.now(tz="UTC") if now is None else _utc_timestamp(now)
    step = TIMEFRAME_DELTAS[timeframe]
    floored_ns = (current.value // step.value) * step.value
    return pd.Timestamp(floored_ns, tz="UTC")


def resolve_download_window(
    *,
    timeframe: str,
    years: int,
    start: Any = None,
    end: Any = None,
    now: Any = None,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Resolve a closed-candle cutoff and either explicit or N-year start."""
    if years <= 0:
        raise ValueError("years must be positive")

    cutoff = _utc_timestamp(end) if end is not None else default_closed_candle_cutoff(
        timeframe,
        now=now,
    )
    start_timestamp = (
        _utc_timestamp(start)
        if start is not None
        else cutoff - pd.DateOffset(years=years)
    )
    if start_timestamp >= cutoff:
        raise ValueError("start must be earlier than end/cutoff")
    return start_timestamp, cutoff


def _date_label(value: str) -> str:
    return _utc_timestamp(value).strftime("%Y%m%d")


def _write_text_checked(path: Path, content: str, *, overwrite: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not overwrite:
        existing = path.read_text(encoding="utf-8")
        if existing == content:
            return
        raise FileExistsError(
            f"{path} already exists with different content; use --overwrite explicitly"
        )
    path.write_text(content, encoding="utf-8")


def write_dataset_bundle(
    result: BinanceDownloadResult,
    *,
    output_root: str | Path = "data/historical",
    manifest_root: str | Path = "evidence/datasets",
    overwrite: bool = False,
    retrieved_at: Any = None,
) -> tuple[Path, Path]:
    """Persist canonical candles plus a reproducibility/data-quality manifest."""
    if result.dataset is None:
        raise RuntimeError(f"Binance download produced no dataset: {result.metadata.status}")

    dataset = result.dataset
    actual_start = result.metadata.actual_start
    actual_end = result.metadata.actual_end
    if actual_start is None or actual_end is None:
        raise RuntimeError("Dataset exists but actual_start/actual_end are missing")

    symbol = dataset.symbol.upper()
    timeframe = dataset.timeframe.upper()
    stem = (
        f"{symbol}_{timeframe.lower()}_"
        f"{_date_label(actual_start)}_{_date_label(actual_end)}"
    )

    csv_path = Path(output_root) / symbol / timeframe.lower() / f"{stem}.csv"
    manifest_path = Path(manifest_root) / f"{stem}.json"

    csv_text = dataset.candles.to_csv(
        index=False,
        date_format="%Y-%m-%dT%H:%M:%SZ",
    )
    _write_text_checked(csv_path, csv_text, overwrite=overwrite)

    retrieved = (
        pd.Timestamp.now(tz="UTC")
        if retrieved_at is None
        else _utc_timestamp(retrieved_at)
    )
    quality = asdict(dataset.quality)
    quality["has_critical_issues"] = dataset.quality.has_critical_issues

    manifest = {
        "dataset_id": dataset.dataset_id,
        "source": dataset.source,
        "symbol": dataset.symbol,
        "timeframe": dataset.timeframe,
        "retrieved_at_utc": retrieved.isoformat(),
        "download": asdict(result.metadata),
        "quality": quality,
        "files": {
            "candles_csv": csv_path.as_posix(),
        },
    }
    _write_text_checked(
        manifest_path,
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        overwrite=overwrite,
    )
    return csv_path, manifest_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Download canonical closed Binance Spot OHLCV candles and write "
            "a CSV + evidence manifest for Strategy Lab tests."
        )
    )
    parser.add_argument("--symbol", default="LINKUSDT")
    parser.add_argument(
        "--timeframe",
        default="1D",
        choices=sorted(TIMEFRAME_DELTAS),
    )
    parser.add_argument(
        "--years",
        type=int,
        default=3,
        help="Lookback years when --start is omitted (default: 3).",
    )
    parser.add_argument(
        "--start",
        help="Explicit UTC start timestamp; overrides --years.",
    )
    parser.add_argument(
        "--end",
        help=(
            "UTC cutoff timestamp. Only candles fully closed by this timestamp "
            "are kept. Default: current candle boundary."
        ),
    )
    parser.add_argument("--output-root", default="data/historical")
    parser.add_argument("--manifest-root", default="evidence/datasets")
    parser.add_argument(
        "--allow-quality-issues",
        action="store_true",
        help="Return success even when candle validation reports critical issues.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow replacing an existing bundle with different content.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    start, cutoff = resolve_download_window(
        timeframe=args.timeframe,
        years=args.years,
        start=args.start,
        end=args.end,
    )

    client = BinanceSpotRestClient()
    result = download_historical_dataset(
        client,
        symbol=args.symbol,
        start=start,
        end=cutoff,
        timeframe=args.timeframe,
        as_of=cutoff,
    )

    if result.dataset is None:
        print(
            f"Download status={result.metadata.status} "
            f"symbol={result.metadata.symbol} timeframe={result.metadata.timeframe}"
        )
        return 2

    csv_path, manifest_path = write_dataset_bundle(
        result,
        output_root=args.output_root,
        manifest_root=args.manifest_root,
        overwrite=args.overwrite,
    )

    quality = result.dataset.quality
    print(f"dataset_id={result.dataset.dataset_id}")
    print(f"candles={len(result.dataset.candles)}")
    print(f"csv={csv_path}")
    print(f"manifest={manifest_path}")
    print(
        "quality="
        f"missing:{quality.missing_candles},"
        f"duplicates:{quality.duplicate_timestamps},"
        f"off_grid:{quality.off_grid_timestamps},"
        f"invalid_ohlc:{quality.invalid_ohlc_rows},"
        f"null_cells:{quality.null_cells},"
        f"negative_volume:{quality.negative_volume_rows}"
    )

    if quality.has_critical_issues and not args.allow_quality_issues:
        print(
            "Critical candle-quality issues detected; bundle was written for "
            "inspection but the command fails closed."
        )
        return 3

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
