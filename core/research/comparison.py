from __future__ import annotations

from dataclasses import asdict
from typing import Callable

import pandas as pd

from core.backtest.contracts import BacktestRunManifest, ExecutionPolicy
from core.backtest.event_study import run_event_study, summarize_event_study
from core.backtest.trading import ENGINE_NAME, LongOnlyTradingPolicy, run_long_only_backtest
from core.data.candles import HistoricalDataset
from core.evidence.backtest_record import build_backtest_evidence
from strategies.crypto.macd_cross.strategy import (
    DEFAULT_FAST_WINDOW as MACD_FAST_WINDOW,
    DEFAULT_SIGNAL_WINDOW as MACD_SIGNAL_WINDOW,
    DEFAULT_SLOW_WINDOW as MACD_SLOW_WINDOW,
    STRATEGY_ID as MACD_CROSS_ID,
    STRATEGY_VERSION as MACD_CROSS_VERSION,
    generate_macd_cross_outputs,
)
from strategies.crypto.stochrsi_cross.strategy import (
    DEFAULT_RSI_WINDOW as CROSS_RSI_WINDOW,
    DEFAULT_SMOOTH_D as CROSS_SMOOTH_D,
    DEFAULT_SMOOTH_K as CROSS_SMOOTH_K,
    DEFAULT_STOCH_WINDOW as CROSS_STOCH_WINDOW,
    STRATEGY_ID as STOCHRSI_CROSS_ID,
    STRATEGY_VERSION as STOCHRSI_CROSS_VERSION,
    generate_stochrsi_cross_outputs,
)
from strategies.crypto.true_stochrsi.strategy import (
    STRATEGY_ID as TRUE_STOCHRSI_ID,
    STRATEGY_VERSION as TRUE_STOCHRSI_VERSION,
    generate_true_stochrsi_outputs,
)
from strategies.crypto.vahram_original.contract_adapter import generate_vahram_original_outputs
from strategies.crypto.vahram_original.strategy import (
    STRATEGY_ID as ORIGINAL_ID,
    STRATEGY_VERSION as ORIGINAL_VERSION,
)

DEFAULT_COMPARISON_STRATEGY_IDS = (ORIGINAL_ID, TRUE_STOCHRSI_ID)


def _strategy_specs() -> dict[str, tuple[str, Callable, dict, dict]]:
    return {
        ORIGINAL_ID: (
            ORIGINAL_VERSION,
            generate_vahram_original_outputs,
            {},
            {},
        ),
        TRUE_STOCHRSI_ID: (
            TRUE_STOCHRSI_VERSION,
            generate_true_stochrsi_outputs,
            {},
            {},
        ),
        STOCHRSI_CROSS_ID: (
            STOCHRSI_CROSS_VERSION,
            generate_stochrsi_cross_outputs,
            {
                "rsi_window": CROSS_RSI_WINDOW,
                "stoch_window": CROSS_STOCH_WINDOW,
                "smooth_k": CROSS_SMOOTH_K,
                "smooth_d": CROSS_SMOOTH_D,
                "signal_logic": "STRICT_K_D_CROSS",
            },
            {
                "rsi_window": CROSS_RSI_WINDOW,
                "stoch_window": CROSS_STOCH_WINDOW,
                "smooth_k": CROSS_SMOOTH_K,
                "smooth_d": CROSS_SMOOTH_D,
            },
        ),
        MACD_CROSS_ID: (
            MACD_CROSS_VERSION,
            generate_macd_cross_outputs,
            {
                "fast_window": MACD_FAST_WINDOW,
                "slow_window": MACD_SLOW_WINDOW,
                "signal_window": MACD_SIGNAL_WINDOW,
                "signal_logic": "STRICT_MACD_SIGNAL_CROSS",
            },
            {
                "fast_window": MACD_FAST_WINDOW,
                "slow_window": MACD_SLOW_WINDOW,
                "signal_window": MACD_SIGNAL_WINDOW,
            },
        ),
    }


def compare_strategies_on_dataset(
    dataset: HistoricalDataset,
    *,
    source_commit_sha: str,
    validation_type: str = "RESEARCH_FULL_HISTORY",
    fee_bps: float = 10.0,
    slippage_bps: float = 5.0,
    horizons: tuple[int, ...] = (1, 3, 7, 14),
    trading_policy: LongOnlyTradingPolicy | None = None,
    strategy_ids: tuple[str, ...] | None = None,
) -> dict:
    if dataset.candles.empty:
        raise ValueError("Cannot compare strategies on an empty dataset")
    if dataset.quality.has_critical_issues:
        raise ValueError("Cannot run formal comparison with critical dataset quality issues")

    specs = _strategy_specs()
    selected_ids = strategy_ids or DEFAULT_COMPARISON_STRATEGY_IDS
    if not selected_ids:
        raise ValueError("At least one strategy_id is required")
    if len(set(selected_ids)) != len(selected_ids):
        raise ValueError("strategy_ids must not contain duplicates")
    unknown = [strategy_id for strategy_id in selected_ids if strategy_id not in specs]
    if unknown:
        raise ValueError(f"Unknown strategy_ids: {unknown}")

    policy = trading_policy or LongOnlyTradingPolicy()
    execution = ExecutionPolicy(fee_bps=fee_bps, slippage_bps=slippage_bps)
    start = pd.Timestamp(dataset.candles.iloc[0]["timestamp"]).isoformat()
    end = pd.Timestamp(dataset.candles.iloc[-1]["timestamp"]).isoformat()

    summary_rows = []
    results = {}

    for strategy_id in selected_ids:
        strategy_version, generator, parameters, generator_kwargs = specs[strategy_id]
        manifest = BacktestRunManifest(
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            source_commit_sha=source_commit_sha,
            dataset_id=dataset.dataset_id,
            symbol=dataset.symbol,
            timeframe=dataset.timeframe,
            period_start=start,
            period_end=end,
            execution=execution,
            parameters=parameters,
            validation_type=validation_type,
            engine_name=ENGINE_NAME,
            engine_config=policy.to_config(),
        )
        outputs = generator(
            dataset.candles,
            symbol=dataset.symbol,
            timeframe=dataset.timeframe,
            source_commit_sha=source_commit_sha,
            run_id=manifest.run_id,
            **generator_kwargs,
        )
        events = run_event_study(dataset.candles, outputs, horizons=horizons)
        event_summary = summarize_event_study(events)
        trading = run_long_only_backtest(
            dataset.candles,
            outputs,
            manifest=manifest,
            policy=policy,
        )
        evidence = build_backtest_evidence(
            manifest=manifest,
            policy=policy,
            result=trading,
            dataset_quality=dataset.quality,
        )

        metrics = asdict(trading.metrics)
        summary_rows.append(
            {
                "strategy_id": strategy_id,
                "strategy_version": strategy_version,
                "run_id": manifest.run_id,
                "dataset_id": dataset.dataset_id,
                "symbol": dataset.symbol,
                "timeframe": dataset.timeframe,
                "signal_count": len(outputs),
                "event_observation_count": len(events),
                **metrics,
            }
        )
        results[strategy_id] = {
            "manifest": manifest,
            "outputs": outputs,
            "event_observations": events,
            "event_summary": event_summary,
            "trading": trading,
            "evidence": evidence,
        }

    return {
        "dataset_id": dataset.dataset_id,
        "symbol": dataset.symbol,
        "timeframe": dataset.timeframe,
        "summary": pd.DataFrame(summary_rows),
        "strategies": results,
    }
