from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.evidence.backtest_record import write_backtest_evidence
from core.research.comparison import compare_strategies_on_dataset
from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import (
    BinancePublicApiError,
    BinanceRestConfig,
    BinanceSpotRestClient,
)


def _load_symbols(tokens_file: Path) -> list[str]:
    with tokens_file.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if "symbol" not in (reader.fieldnames or []):
            raise ValueError("tokens file must contain a symbol column")
        symbols = [row["symbol"].strip().upper() for row in reader if row["symbol"].strip()]
    if not symbols:
        raise ValueError("No symbols found")
    return list(dict.fromkeys(symbols))


def _resolve_source_commit(explicit: str | None) -> str:
    if explicit:
        return explicit
    env_sha = os.environ.get("SOURCE_COMMIT_SHA")
    if env_sha:
        return env_sha
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=REPO_ROOT,
            text=True,
        ).strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise RuntimeError("source commit SHA is required") from exc


def _load_request(path: Path | None) -> dict:
    if path is None:
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _json_dump(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, sort_keys=True, indent=2, allow_nan=False, default=str) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run V1 vs V2 CryptoSignals research comparison")
    parser.add_argument("--request-file", type=Path)
    parser.add_argument("--tokens-file", type=Path, default=REPO_ROOT / "tokens.csv")
    parser.add_argument("--symbols", help="Comma-separated explicit symbols")
    parser.add_argument("--start", default="2021-01-01T00:00:00Z")
    parser.add_argument(
        "--end",
        default="2026-01-01T00:00:00Z",
        help="Exclusive closed-candle boundary via as_of; default protects 2026 OOS",
    )
    parser.add_argument("--timeframe", default="1D")
    parser.add_argument("--fee-bps", type=float, default=10.0)
    parser.add_argument("--slippage-bps", type=float, default=5.0)
    parser.add_argument("--validation-type", default="PRE_OOS_RESEARCH_2021_2025")
    parser.add_argument("--source-commit")
    parser.add_argument("--output-dir", type=Path, default=REPO_ROOT / "research_artifacts")
    args = parser.parse_args()

    request = _load_request(args.request_file)
    start = request.get("start", args.start)
    end = request.get("end", args.end)
    timeframe = request.get("timeframe", args.timeframe)
    fee_bps = float(request.get("fee_bps", args.fee_bps))
    slippage_bps = float(request.get("slippage_bps", args.slippage_bps))
    validation_type = request.get("validation_type", args.validation_type)
    horizons = tuple(int(value) for value in request.get("horizons", [1, 3, 7, 14]))

    explicit_symbols = request.get("symbols")
    if explicit_symbols:
        symbols = [str(symbol).strip().upper() for symbol in explicit_symbols]
    elif args.symbols:
        symbols = [symbol.strip().upper() for symbol in args.symbols.split(",") if symbol.strip()]
    else:
        symbols = _load_symbols(args.tokens_file)

    source_commit_sha = _resolve_source_commit(args.source_commit)
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    retrieved_at = datetime.now(timezone.utc).isoformat()
    client = BinanceSpotRestClient(BinanceRestConfig())
    summary_rows = []
    successful_symbols = 0

    run_header = {
        "request_file": str(args.request_file) if args.request_file else None,
        "request": request,
        "source_commit_sha": source_commit_sha,
        "retrieved_at": retrieved_at,
        "source": "BINANCE_SPOT_REST",
        "endpoint": "https://api.binance.com/api/v3/klines",
        "symbols": symbols,
        "start": start,
        "end": end,
        "timeframe": timeframe,
        "fee_bps": fee_bps,
        "slippage_bps": slippage_bps,
        "validation_type": validation_type,
        "horizons": list(horizons),
    }
    _json_dump(output_dir / "run_manifest.json", run_header)

    for symbol in symbols:
        symbol_dir = output_dir / "symbols" / symbol
        symbol_dir.mkdir(parents=True, exist_ok=True)
        try:
            download = download_historical_dataset(
                client,
                symbol=symbol,
                start=start,
                end=end,
                timeframe=timeframe,
                as_of=end,
            )
            _json_dump(
                symbol_dir / "download_metadata.json",
                {
                    "retrieved_at": retrieved_at,
                    "download": asdict(download.metadata),
                    "quality": asdict(download.dataset.quality) if download.dataset else None,
                },
            )

            if download.dataset is None:
                summary_rows.append(
                    {
                        "symbol": symbol,
                        "status": download.metadata.status,
                        "dataset_id": None,
                        "strategy_id": None,
                        "run_id": None,
                    }
                )
                continue

            dataset = download.dataset
            dataset.candles.to_csv(symbol_dir / "canonical_candles.csv", index=False)

            if dataset.quality.has_critical_issues:
                summary_rows.append(
                    {
                        "symbol": symbol,
                        "status": "DATA_QUALITY_BLOCKED",
                        "dataset_id": dataset.dataset_id,
                        "strategy_id": None,
                        "run_id": None,
                    }
                )
                continue

            comparison = compare_strategies_on_dataset(
                dataset,
                source_commit_sha=source_commit_sha,
                validation_type=validation_type,
                fee_bps=fee_bps,
                slippage_bps=slippage_bps,
                horizons=horizons,
            )
            successful_symbols += 1
            comparison["summary"].to_csv(symbol_dir / "strategy_summary.csv", index=False)

            for strategy_id, result in comparison["strategies"].items():
                run_id = result["manifest"].run_id
                write_backtest_evidence(
                    symbol_dir / "evidence" / f"{strategy_id}_{run_id}.json",
                    result["evidence"],
                )
                result["event_observations"].to_csv(
                    symbol_dir / "event_study" / f"{strategy_id}_{run_id}_observations.csv",
                    index=False,
                )
                result["event_summary"].to_csv(
                    symbol_dir / "event_study" / f"{strategy_id}_{run_id}_summary.csv",
                    index=False,
                )

            for row in comparison["summary"].to_dict("records"):
                summary_rows.append({"status": "OK", **row})

        except BinancePublicApiError as exc:
            _json_dump(
                symbol_dir / "error.json",
                {
                    "type": "BINANCE_API_ERROR",
                    "message": str(exc),
                    "status": exc.status,
                    "code": exc.code,
                },
            )
            summary_rows.append(
                {
                    "symbol": symbol,
                    "status": "BINANCE_API_ERROR",
                    "dataset_id": None,
                    "strategy_id": None,
                    "run_id": None,
                }
            )
        except Exception as exc:
            _json_dump(
                symbol_dir / "error.json",
                {
                    "type": type(exc).__name__,
                    "message": str(exc),
                },
            )
            summary_rows.append(
                {
                    "symbol": symbol,
                    "status": "ERROR",
                    "dataset_id": None,
                    "strategy_id": None,
                    "run_id": None,
                }
            )

    import pandas as pd

    summary = pd.DataFrame(summary_rows)
    summary.to_csv(output_dir / "comparison_summary.csv", index=False)
    _json_dump(
        output_dir / "run_result.json",
        {
            **run_header,
            "successful_symbols": successful_symbols,
            "requested_symbol_count": len(symbols),
            "status": "PASS_WITH_PARTIALS" if successful_symbols else "FAIL_NO_SUCCESSFUL_SYMBOLS",
        },
    )

    print(summary.to_string(index=False))
    print(f"Successful symbols: {successful_symbols}/{len(symbols)}")
    return 0 if successful_symbols else 2


if __name__ == "__main__":
    raise SystemExit(main())
