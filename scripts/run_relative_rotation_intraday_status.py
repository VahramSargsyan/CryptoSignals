from __future__ import annotations

import argparse
import itertools
import json
from pathlib import Path

import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import ARM_THRESHOLD, ASSETS

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "config" / "relative_rotation_paper_live_v1.json"
DEFAULT_OUTPUT_ROOT = ROOT / "menu_artifacts" / "relative_rotation"
LOOKBACK_DAYS = 180
HOURS_PER_DAY = 24
LOOKBACK_HOURS = LOOKBACK_DAYS * HOURS_PER_DAY
DOWNLOAD_BUFFER_DAYS = 1
SYMBOLS = {asset: f"{asset}USDT" for asset in ASSETS}


def _utc(value: str | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _cutoff(now: str | pd.Timestamp | None = None) -> pd.Timestamp:
    current = pd.Timestamp.now(tz="UTC") if now is None else _utc(now)
    return current.floor("h")


def _read_config(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    books = payload.get("position_books") or []
    if not books:
        raise ValueError("position_books must not be empty")
    for book in books:
        asset = str(book.get("held_asset") or "").upper()
        if asset not in ASSETS:
            raise ValueError(f"Unsupported held asset in position_books: {asset!r}")
    return payload


def download_hourly_panel(
    *,
    start: pd.Timestamp,
    cutoff: pd.Timestamp,
) -> tuple[pd.DataFrame, dict]:
    client = BinanceSpotRestClient()
    pieces: list[pd.DataFrame] = []
    metadata: dict[str, dict] = {}

    for asset in ASSETS:
        result = download_historical_dataset(
            client,
            symbol=SYMBOLS[asset],
            start=start,
            end=cutoff,
            timeframe="1H",
            as_of=cutoff,
        )
        if result.dataset is None:
            raise RuntimeError(
                f"{asset}: no closed hourly dataset ({result.metadata.status})"
            )
        if result.dataset.quality.has_critical_issues:
            raise RuntimeError(
                f"{asset}: critical hourly data quality: {result.dataset.quality}"
            )

        frame = result.dataset.candles[["timestamp", "close"]].copy()
        frame = frame.rename(columns={"close": f"{asset}_close"})
        pieces.append(frame)
        metadata[asset] = {
            "symbol": SYMBOLS[asset],
            "timeframe": "1H",
            "dataset_id": result.dataset.dataset_id,
            "rows": len(frame),
            "actual_start": result.metadata.actual_start,
            "actual_end": result.metadata.actual_end,
            "status": result.metadata.status,
        }

    panel = pieces[0]
    for frame in pieces[1:]:
        panel = panel.merge(frame, on="timestamp", how="inner", validate="one_to_one")
    panel = panel.sort_values("timestamp", kind="stable").reset_index(drop=True)
    if len(panel) < LOOKBACK_HOURS:
        raise RuntimeError(
            f"Common hourly panel has only {len(panel)} rows; "
            f"{LOOKBACK_HOURS} are required for {LOOKBACK_DAYS} days"
        )

    metadata["panel"] = {
        "rows": len(panel),
        "start": pd.Timestamp(panel.iloc[0]["timestamp"]).isoformat(),
        "end": pd.Timestamp(panel.iloc[-1]["timestamp"]).isoformat(),
    }
    return panel, metadata


def build_intraday_pair_states(
    panel: pd.DataFrame,
    *,
    assets=ASSETS,
    lookback_hours: int = LOOKBACK_HOURS,
    arm_threshold: float = ARM_THRESHOLD,
) -> list[dict]:
    required = {"timestamp", *(f"{asset}_close" for asset in assets)}
    missing = required.difference(panel.columns)
    if missing:
        raise ValueError(f"Panel is missing columns: {sorted(missing)}")
    if len(panel) < lookback_hours:
        raise ValueError(
            f"Panel has {len(panel)} rows; lookback requires {lookback_hours}"
        )

    frame = panel.sort_values("timestamp", kind="stable").tail(lookback_hours)
    states: list[dict] = []

    for left, right in itertools.combinations(assets, 2):
        ratio_series = frame[f"{right}_close"] / frame[f"{left}_close"]
        median = float(ratio_series.median())
        latest_ratio = float(ratio_series.iloc[-1])
        deviation = latest_ratio / median - 1.0

        mode = "NONE"
        from_asset = None
        to_asset = None
        if deviation >= arm_threshold:
            mode = "HIGH"
            from_asset, to_asset = right, left
        elif deviation <= -arm_threshold:
            mode = "LOW"
            from_asset, to_asset = left, right

        states.append(
            {
                "pair": f"{left}/{right}",
                "mode": mode,
                "from_asset": from_asset,
                "to_asset": to_asset,
                "armed_at": None,
                "ratio": latest_ratio,
                "median": median,
                "deviation": float(deviation),
                "extreme": None,
                "max_dislocation": abs(float(deviation)),
                "reversal_from_extreme": None,
            }
        )

    return states


def build_payload(
    *,
    config: dict,
    panel: pd.DataFrame,
    data_metadata: dict,
    generated_at: pd.Timestamp | None = None,
) -> dict:
    latest_open = pd.Timestamp(panel.iloc[-1]["timestamp"])
    if latest_open.tzinfo is None:
        latest_open = latest_open.tz_localize("UTC")
    else:
        latest_open = latest_open.tz_convert("UTC")
    latest_end = latest_open + pd.Timedelta(hours=1)
    generated_at = generated_at or pd.Timestamp.now(tz="UTC")

    pair_states = build_intraday_pair_states(panel)
    book_events = {}
    for book in config["position_books"]:
        book_id = str(book["book_id"])
        book_events[book_id] = {
            "book": dict(book),
            "events": {
                "armed": [],
                "confirmed": [],
                "primary_confirmed": None,
            },
            "route_conflicts": [],
            "route_override": None,
        }

    latest_row = panel.iloc[-1]
    return {
        "schema_version": 1,
        "report_kind": "H1_INTRADAY_STATUS",
        "timeframe": "1H",
        "lookback_days": LOOKBACK_DAYS,
        "lookback_observations": LOOKBACK_HOURS,
        "generated_at": generated_at.isoformat(),
        "latest_closed_candle": latest_open.isoformat(),
        "latest_closed_candle_end": latest_end.isoformat(),
        "latest_closed_candle_end_yerevan": latest_end.tz_convert(
            "Asia/Yerevan"
        ).isoformat(),
        "target_assets": list(config.get("target_assets") or []),
        "position_books": [dict(book) for book in config["position_books"]],
        "book_events": book_events,
        "route_conflicts": {},
        "latest_pair_states": pair_states,
        "latest_close_prices_usdt": {
            asset: float(latest_row[f"{asset}_close"]) for asset in ASSETS
        },
        "data": data_metadata,
        "execution_allowed": False,
        "signal_semantics": "INFORMATIONAL_H1_SNAPSHOT_ONLY",
    }


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, default=str, allow_nan=False)
        + "\n",
        encoding="utf-8",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build an informational H1 Relative Rotation status snapshot."
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--cutoff")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    config = _read_config(args.config)
    cutoff = _utc(args.cutoff) if args.cutoff else _cutoff()
    start = cutoff - pd.Timedelta(days=LOOKBACK_DAYS + DOWNLOAD_BUFFER_DAYS)

    panel, data_metadata = download_hourly_panel(start=start, cutoff=cutoff)
    payload = build_payload(
        config=config,
        panel=panel,
        data_metadata=data_metadata,
    )

    run_id = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir = args.output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    _write_json(run_dir / "report.json", payload)

    print(f"run_dir={run_dir}")
    print(f"timeframe={payload['timeframe']}")
    print(f"latest_closed_candle_end={payload['latest_closed_candle_end']}")
    print(f"lookback_hours={LOOKBACK_HOURS}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
