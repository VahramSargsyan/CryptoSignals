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

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.evidence.backtest_record import write_backtest_evidence
from core.regime.crypto import CryptoRegimeConfig, build_crypto_regime
from core.research.comparison import compare_strategies_on_dataset
from core.research.regime_analysis import (
    attach_regime_to_events,
    attach_regime_to_trades,
    summarize_events_by_regime,
    summarize_trades_by_regime,
)
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


def _regime_config(request: dict) -> CryptoRegimeConfig:
    raw = request.get("regime_config") or {}
    return CryptoRegimeConfig(**raw)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run pre-OOS CryptoSignals market-regime analysis")
    parser.add_argument("--request-file", type=Path)
    parser.add_argument("--tokens-file", type=Path, default=REPO_ROOT / "tokens.csv")
    parser.add_argument("--symbols", help="Comma-separated explicit symbols")
    parser.add_argument("--start", default="2021-01-01T00:00:00Z")
    parser.add_argument("--end", default="2026-01-01T00:00:00Z")
    parser.add_argument("--timeframe", default="1D")
    parser.add_argument("--fee-bps", type=float, default=10.0)
    parser.add_argument("--slippage-bps", type=float, default=5.0)
    parser.add_argument("--validation-type", default="PRE_OOS_REGIME_RESEARCH_2021_2025")
    parser.add_argument("--source-commit")
    parser.add_argument("--output-dir", type=Path, default=REPO_ROOT / "regime_artifacts")
    args = parser.parse_args()

    request = _load_request(args.request_file)
    start = request.get("start", args.start)
    end = request.get("end", args.end)
    timeframe = request.get("timeframe", args.timeframe).upper()
    fee_bps = float(request.get("fee_bps", args.fee_bps))
    slippage_bps = float(request.get("slippage_bps", args.slippage_bps))
    validation_type = request.get("validation_type", args.validation_type)
    horizons = tuple(int(value) for value in request.get("horizons", [1, 3, 7, 14]))
    config = _regime_config(request)

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

    run_header = {
        "request_file": str(args.request_file) if args.request_file else None,
        "request": request,
        "source_commit_sha": source_commit_sha,
        "retrieved_at": retrieved_at,
        "source": "BINANCE_SPOT_REST",
        "endpoint": "https://data-api.binance.vision/api/v3/klines",
        "symbols": symbols,
        "start": start,
        "end": end,
        "timeframe": timeframe,
        "fee_bps": fee_bps,
        "slippage_bps": slippage_bps,
        "validation_type": validation_type,
        "horizons": list(horizons),
        "regime_config": asdict(config),
        "protected_period": request.get("protected_period", "2026"),
    }
    _json_dump(output_dir / "run_manifest.json", run_header)

    client = BinanceSpotRestClient(BinanceRestConfig())
    datasets = {}
    download_rows = []

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
                download_rows.append(
                    {
                        "symbol": symbol,
                        "status": download.metadata.status,
                        "dataset_id": None,
                    }
                )
                continue
            if download.dataset.quality.has_critical_issues:
                download_rows.append(
                    {
                        "symbol": symbol,
                        "status": "DATA_QUALITY_BLOCKED",
                        "dataset_id": download.dataset.dataset_id,
                    }
                )
                continue

            datasets[symbol] = download.dataset
            download.dataset.candles.to_csv(symbol_dir / "canonical_candles.csv", index=False)
            download_rows.append(
                {
                    "symbol": symbol,
                    "status": "OK",
                    "dataset_id": download.dataset.dataset_id,
                }
            )
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
            download_rows.append({"symbol": symbol, "status": "BINANCE_API_ERROR", "dataset_id": None})
        except Exception as exc:
            _json_dump(
                symbol_dir / "error.json",
                {"type": type(exc).__name__, "message": str(exc)},
            )
            download_rows.append({"symbol": symbol, "status": "ERROR", "dataset_id": None})

    pd.DataFrame(download_rows).to_csv(output_dir / "download_summary.csv", index=False)

    benchmark = config.benchmark_symbol.upper()
    if benchmark not in datasets:
        _json_dump(
            output_dir / "run_result.json",
            {
                **run_header,
                "status": "FAIL_NO_BENCHMARK",
                "successful_downloads": len(datasets),
                "requested_symbol_count": len(symbols),
            },
        )
        return 2

    regime = build_crypto_regime(datasets, config=config)
    regime.to_csv(output_dir / "market_regime.csv", index=False)
    _json_dump(output_dir / "regime_config.json", asdict(config))

    all_events = []
    all_trades = []
    comparison_rows = []
    successful_comparisons = 0

    for symbol in symbols:
        if symbol not in datasets:
            continue
        dataset = datasets[symbol]
        symbol_dir = output_dir / "symbols" / symbol
        try:
            comparison = compare_strategies_on_dataset(
                dataset,
                source_commit_sha=source_commit_sha,
                validation_type=validation_type,
                fee_bps=fee_bps,
                slippage_bps=slippage_bps,
                horizons=horizons,
            )
            comparison["summary"].to_csv(symbol_dir / "strategy_summary.csv", index=False)
            comparison_rows.extend(comparison["summary"].to_dict("records"))

            for strategy_id, result in comparison["strategies"].items():
                run_id = result["manifest"].run_id
                evidence_dir = symbol_dir / "evidence"
                event_dir = symbol_dir / "event_study"
                regime_dir = symbol_dir / "regime"
                event_dir.mkdir(parents=True, exist_ok=True)
                regime_dir.mkdir(parents=True, exist_ok=True)

                write_backtest_evidence(
                    evidence_dir / f"{strategy_id}_{run_id}.json",
                    result["evidence"],
                )
                result["event_observations"].to_csv(
                    event_dir / f"{strategy_id}_{run_id}_observations.csv",
                    index=False,
                )
                result["event_summary"].to_csv(
                    event_dir / f"{strategy_id}_{run_id}_summary.csv",
                    index=False,
                )

                event_regime = attach_regime_to_events(
                    result["event_observations"],
                    regime,
                )
                trade_regime = attach_regime_to_trades(
                    result["trading"].trades,
                    regime,
                )
                event_regime.to_csv(
                    regime_dir / f"{strategy_id}_{run_id}_events.csv",
                    index=False,
                )
                trade_regime.to_csv(
                    regime_dir / f"{strategy_id}_{run_id}_trades.csv",
                    index=False,
                )
                if not event_regime.empty:
                    all_events.append(event_regime)
                if not trade_regime.empty:
                    all_trades.append(trade_regime)

            successful_comparisons += 1
        except Exception as exc:
            _json_dump(
                symbol_dir / "regime_comparison_error.json",
                {"type": type(exc).__name__, "message": str(exc)},
            )

    pd.DataFrame(comparison_rows).to_csv(output_dir / "comparison_summary.csv", index=False)

    events = pd.concat(all_events, ignore_index=True) if all_events else pd.DataFrame()
    trades = pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()

    if not events.empty:
        summarize_events_by_regime(events, group_by_symbol=True).to_csv(
            output_dir / "event_regime_by_symbol.csv",
            index=False,
        )
        summarize_events_by_regime(events, group_by_symbol=False).to_csv(
            output_dir / "event_regime_pooled.csv",
            index=False,
        )
        summarize_events_by_regime(
            events,
            group_by_symbol=False,
            group_by_year=True,
        ).to_csv(
            output_dir / "event_regime_by_year.csv",
            index=False,
        )

    if not trades.empty:
        summarize_trades_by_regime(trades, group_by_symbol=True).to_csv(
            output_dir / "trade_regime_by_symbol.csv",
            index=False,
        )
        summarize_trades_by_regime(trades, group_by_symbol=False).to_csv(
            output_dir / "trade_regime_pooled.csv",
            index=False,
        )
        summarize_trades_by_regime(
            trades,
            group_by_symbol=False,
            group_by_year=True,
        ).to_csv(
            output_dir / "trade_regime_by_year.csv",
            index=False,
        )

    status = (
        "PASS"
        if successful_comparisons == len(symbols)
        else "PASS_WITH_PARTIALS"
        if successful_comparisons
        else "FAIL_NO_SUCCESSFUL_COMPARISONS"
    )
    _json_dump(
        output_dir / "run_result.json",
        {
            **run_header,
            "status": status,
            "successful_downloads": len(datasets),
            "successful_comparisons": successful_comparisons,
            "requested_symbol_count": len(symbols),
            "regime_rows": len(regime),
            "event_rows": len(events),
            "trade_rows": len(trades),
        },
    )

    print(pd.DataFrame(download_rows).to_string(index=False))
    print(f"Successful comparisons: {successful_comparisons}/{len(symbols)}")
    print(f"Regime rows: {len(regime)}")
    return 0 if successful_comparisons else 2


if __name__ == "__main__":
    raise SystemExit(main())
