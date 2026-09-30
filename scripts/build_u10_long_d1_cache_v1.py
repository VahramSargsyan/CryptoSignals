from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from scripts.download_historical_data import write_dataset_bundle
from strategies.crypto.relative_rotation.paper_live import TARGET_ASSETS

DEFAULT_START = "2018-01-01T00:00:00Z"
DEFAULT_AS_OF = "2026-09-30T00:00:00Z"
TIMEFRAME = "1D"
GRID_PREHISTORY_DAYS = 1095
FULL_TEST_DAYS = 1095
MIN_FULL_REPLAY_ROWS = GRID_PREHISTORY_DAYS + FULL_TEST_DAYS


def _utc(value: str) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_cache(*, start: str, as_of: str, output_root: Path) -> dict:
    requested_start = _utc(start)
    cutoff = _utc(as_of)
    if requested_start >= cutoff:
        raise ValueError("start must be earlier than as_of")

    data_root = output_root / "data" / "historical"
    manifest_root = output_root / "evidence" / "datasets"
    client = BinanceSpotRestClient()

    datasets: list[dict] = []
    critical: list[dict] = []

    for asset in TARGET_ASSETS:
        symbol = f"{asset}USDT"
        result = download_historical_dataset(
            client,
            symbol=symbol,
            start=requested_start,
            end=cutoff,
            timeframe=TIMEFRAME,
            as_of=cutoff,
        )
        if result.dataset is None:
            raise RuntimeError(f"{symbol}/{TIMEFRAME}: no dataset ({result.metadata.status})")

        csv_path, manifest_path = write_dataset_bundle(
            result,
            output_root=data_root,
            manifest_root=manifest_root,
            overwrite=True,
            retrieved_at=cutoff,
        )
        quality = result.dataset.quality
        candles = result.dataset.candles
        rows = int(len(candles))
        actual_start = pd.Timestamp(result.metadata.actual_start)
        actual_end = pd.Timestamp(result.metadata.actual_end)
        span_days = int((actual_end - actual_start).days)
        full_replay_eligible = rows >= MIN_FULL_REPLAY_ROWS

        record = {
            "dataset_id": result.dataset.dataset_id,
            "asset": asset,
            "symbol": symbol,
            "timeframe": TIMEFRAME,
            "requested_start": result.metadata.requested_start,
            "requested_end": result.metadata.requested_end,
            "actual_start": result.metadata.actual_start,
            "actual_end": result.metadata.actual_end,
            "rows": rows,
            "span_days": span_days,
            "span_years_approx": span_days / 365.2425,
            "listing_truncated": bool(result.metadata.listing_truncated),
            "status": result.metadata.status,
            "full_3y_grid_plus_3y_test_eligible": full_replay_eligible,
            "minimum_rows_required": MIN_FULL_REPLAY_ROWS,
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

    eligible = [row["asset"] for row in datasets if row["full_3y_grid_plus_3y_test_eligible"]]
    insufficient = [row["asset"] for row in datasets if not row["full_3y_grid_plus_3y_test_eligible"]]

    catalog = {
        "schema_version": 1,
        "capability_id": "RR_U10_LONG_D1_CACHE_V1",
        "workflow_mode": "STRESS_TEST_ONLY",
        "source": "BINANCE_SPOT",
        "requested_start": requested_start.isoformat(),
        "as_of": cutoff.isoformat(),
        "timeframe": TIMEFRAME,
        "assets": list(TARGET_ASSETS),
        "dataset_count": len(datasets),
        "critical_dataset_count": len(critical),
        "grid_prehistory_days": GRID_PREHISTORY_DAYS,
        "full_test_days": FULL_TEST_DAYS,
        "minimum_full_replay_rows": MIN_FULL_REPLAY_ROWS,
        "full_3y_grid_plus_3y_test_eligible_assets": eligible,
        "insufficient_history_assets": insufficient,
        "datasets": datasets,
        "production_changes": "NONE",
        "test_level": "GITHUB_ACTIONS_BINANCE_D1_IMMUTABLE_CACHE",
    }

    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / "catalog.json").write_text(
        json.dumps(catalog, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    if critical:
        bad = ", ".join(f"{row['symbol']}/{TIMEFRAME}" for row in critical)
        raise RuntimeError("Critical candle-quality issues detected: " + bad)

    return catalog


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build long-history D1 cache for the frozen RR U10 universe."
    )
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--as-of", default=DEFAULT_AS_OF)
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("build/u10_long_d1_cache_v1"),
    )
    args = parser.parse_args(argv)

    catalog = build_cache(start=args.start, as_of=args.as_of, output_root=args.output_root)
    print(f"dataset_count={catalog['dataset_count']}")
    print("eligible=" + ",".join(catalog["full_3y_grid_plus_3y_test_eligible_assets"]))
    print("insufficient=" + ",".join(catalog["insufficient_history_assets"]))
    print(f"catalog={args.output_root / 'catalog.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
