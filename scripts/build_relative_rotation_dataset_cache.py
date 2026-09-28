from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from scripts.download_historical_data import write_dataset_bundle
from strategies.crypto.relative_rotation.paper_live import ASSETS

DEFAULT_START = "2023-05-05T00:00:00Z"
TIMEFRAMES = ("1H", "1D")


def _utc(value: Any) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _default_as_of() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC").floor("h")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def dataset_plan(
    *,
    assets: tuple[str, ...] = ASSETS,
    timeframes: tuple[str, ...] = TIMEFRAMES,
) -> list[tuple[str, str]]:
    return [(asset, timeframe) for asset in assets for timeframe in timeframes]


def build_cache(
    *,
    start: str,
    as_of: str | None,
    output_root: Path,
) -> dict:
    requested_start = _utc(start)
    cutoff = _utc(as_of) if as_of else _default_as_of()
    if requested_start >= cutoff:
        raise ValueError("start must be earlier than as_of")

    data_root = output_root / "data" / "historical"
    manifest_root = output_root / "evidence" / "datasets"
    client = BinanceSpotRestClient()
    datasets: list[dict] = []
    critical: list[dict] = []

    for asset, timeframe in dataset_plan():
        symbol = f"{asset}USDT"
        result = download_historical_dataset(
            client,
            symbol=symbol,
            start=requested_start,
            end=cutoff,
            timeframe=timeframe,
            as_of=cutoff,
        )
        if result.dataset is None:
            raise RuntimeError(
                f"{symbol}/{timeframe}: no dataset ({result.metadata.status})"
            )

        csv_path, manifest_path = write_dataset_bundle(
            result,
            output_root=data_root,
            manifest_root=manifest_root,
            overwrite=True,
            retrieved_at=cutoff,
        )
        quality = result.dataset.quality
        record = {
            "dataset_id": result.dataset.dataset_id,
            "symbol": symbol,
            "asset": asset,
            "timeframe": timeframe,
            "requested_start": result.metadata.requested_start,
            "requested_end": result.metadata.requested_end,
            "actual_start": result.metadata.actual_start,
            "actual_end": result.metadata.actual_end,
            "rows": len(result.dataset.candles),
            "listing_truncated": result.metadata.listing_truncated,
            "status": result.metadata.status,
            "csv_path": csv_path.relative_to(output_root).as_posix(),
            "manifest_path": manifest_path.relative_to(output_root).as_posix(),
            "csv_sha256": _sha256(csv_path),
            "quality": {
                "missing_candles": quality.missing_candles,
                "duplicates": quality.duplicate_timestamps,
                "off_grid": quality.off_grid_timestamps,
                "invalid_ohlc": quality.invalid_ohlc_rows,
                "null_cells": quality.null_cells,
                "negative_volume": quality.negative_volume_rows,
                "has_critical_issues": quality.has_critical_issues,
            },
        }
        datasets.append(record)
        if quality.has_critical_issues:
            critical.append(record)

    catalog = {
        "schema_version": 1,
        "capability_id": "RELATIVE_ROTATION_HISTORICAL_DATASET_CACHE_V1",
        "source": "BINANCE_SPOT",
        "retrieved_at_utc": cutoff.isoformat(),
        "requested_start": requested_start.isoformat(),
        "assets": list(ASSETS),
        "timeframes": list(TIMEFRAMES),
        "dataset_count": len(datasets),
        "critical_dataset_count": len(critical),
        "datasets": datasets,
    }
    output_root.mkdir(parents=True, exist_ok=True)
    catalog_path = output_root / "catalog.json"
    catalog_path.write_text(
        json.dumps(catalog, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    if critical:
        bad = ", ".join(f"{row['symbol']}/{row['timeframe']}" for row in critical)
        raise RuntimeError(
            "Critical candle-quality issues detected; release publication blocked: "
            + bad
        )
    return catalog


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build an immutable Relative Rotation historical dataset cache "
            "from Binance Spot closed candles."
        )
    )
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--as-of")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("build/relative_rotation_dataset_cache_v1"),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    catalog = build_cache(
        start=args.start,
        as_of=args.as_of,
        output_root=args.output_root,
    )
    print(f"dataset_count={catalog['dataset_count']}")
    print(f"critical_dataset_count={catalog['critical_dataset_count']}")
    print(f"catalog={args.output_root / 'catalog.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
