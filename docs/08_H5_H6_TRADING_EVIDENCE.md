# H5/H6 — Trading Backtest and Evidence Record

WORKFLOW_MODE: BUILD_NEW_APP  
RISK_CLASS: L3 — core backtest/evidence semantics  
MIGRATION_REQUIRED: NO

## Definition of Ready

CAPABILITY_ID: CRYPTO_STRATEGY_LAB_H5_H6  
USER_RESULT: strategy outputs can be converted into reproducible closed trades, portfolio metrics, and a durable evidence record.  
CANONICAL_SOURCE_OF_TRUTH: canonical candle dataset + versioned StrategyOutput + BacktestRunManifest.  
SCOPE: first explicit long-only engine, costs, equity curve, required core metrics, evidence JSON.  
OUT_OF_SCOPE: short selling, leverage, partial 25/50/100 sizing, stops, portfolio multi-asset allocation, optimization, regimes, Sheets, Telegram/live trading.  
DO_NOT_TOUCH: legacy scripts and frozen VAHRAM_ORIGINAL_V1 behavior.  
SCHEMA / ID / RELATION IMPACT: no persisted production schema.  
MIGRATION_PLAN: not required.  
ROLLBACK: revert bounded PR.  
PROMOTION_PATH: tests -> PR -> main; runtime historical experiments later produce evidence files.  
UNRESOLVED_DECISIONS: NONE for this engine because its semantics are explicit and do not claim to be the final legacy Action model.

## Engine identity

LONG_ONLY_SIGNAL_FLIP_V1

This is deliberately a named research engine, not an invisible default.

Semantics:

- flat + BUY meeting entry strength threshold -> enter full-equity long at next candle open;
- long + SELL meeting exit strength threshold -> exit at next candle open;
- SELL while flat is ignored;
- BUY while already long is ignored;
- last-candle signal cannot execute without a future candle;
- an open position at dataset end is explicitly liquidated at final candle close;
- no short selling;
- no leverage;
- no partial position changes.

The legacy 25/50/100 Action percentages are not silently converted into portfolio sizing in H5.

## Costs

Execution uses the shared ExecutionPolicy:

- configurable fee_bps;
- configurable slippage_bps;
- BUY slippage worsens entry price;
- SELL slippage worsens exit price;
- fees are applied on both entry and exit.

## Metrics

H5 produces:

- trade count;
- win rate;
- average return;
- median return;
- profit factor;
- expectancy;
- max drawdown;
- total return;
- cost-adjusted buy-and-hold benchmark return;
- average holding period in candles;
- exposure.

When no losing trade exists, profit_factor is stored as null instead of non-standard JSON Infinity.

## Equity / drawdown

Portfolio starts at normalized equity 1.0.

During a position, the equity curve marks the position to candle close using a conservative liquidation-fee assumption.

This allows max drawdown to include intra-trade movement instead of measuring only closed-trade checkpoints.

## H6 Evidence Record

Backtest evidence contains:

- schema version;
- created_at;
- run_id;
- full canonical manifest;
- engine/trading policy;
- dataset quality report when provided;
- required metrics;
- complete closed-trade records.

JSON writing uses allow_nan=false so NaN/Infinity cannot silently enter reproducible evidence.

## Boundary

H5/H6 provides infrastructure only.

A strategy is not BACKTESTED or ACCEPTED merely because the engine exists.

BACKTEST_EXECUTED requires an actual historical dataset run. OUT_OF_SAMPLE_TESTED requires a held-out run. Those evidence levels are separate.
