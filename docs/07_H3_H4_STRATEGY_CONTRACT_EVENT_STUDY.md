# H3/H4 — Strategy Contract and Event Study

WORKFLOW_MODE: BUILD_NEW_APP  
RISK_CLASS: L3 — shared strategy/backtest semantics  
MIGRATION_REQUIRED: NO

## Definition of Ready

CAPABILITY_ID: CRYPTO_STRATEGY_LAB_H3_H4  
USER_RESULT: VAHRAM_ORIGINAL_V1 and VAHRAM_TRUE_STOCHRSI_V2 can publish the same output contract and be compared by one event-study engine.  
CANONICAL_SOURCE_OF_TRUTH: strategy source + version in GitHub; canonical candle dataset from the shared data layer.  
SCOPE: strategy output contract, original adapter, TRUE StochRSI V2, event study +1/+3/+7/+14-ready engine.  
OUT_OF_SCOPE: position lifecycle, stops, portfolio sizing, trading P&L, optimization, regimes, Google Sheets, Telegram, live trading.  
DO_NOT_TOUCH: frozen VAHRAM_ORIGINAL_V1 formulas and legacy scripts.  
SCHEMA / ID / RELATION IMPACT: none persisted.  
MIGRATION_PLAN: not required.  
ROLLBACK: revert this bounded PR; H0/H1/H2 remain usable.  
PROMOTION_PATH: tests -> PR -> main; no live promotion.  
UNRESOLVED_DECISIONS: NONE.

## Shared strategy output

Every strategy can now publish:

- output_id;
- strategy_id;
- strategy_version;
- symbol;
- timeframe;
- timestamp;
- BUY / SELL / HOLD;
- strength;
- reasons;
- run_id;
- source_commit_sha.

output_id is deterministic for strategy/version/symbol/timeframe/timestamp/signal and is suitable as a future duplicate-protection primitive.

## VAHRAM_ORIGINAL_V1 adapter

The original strategy is not rewritten.

The adapter converts canonical lowercase candles into the frozen legacy input shape, calls the frozen formula/scoring implementation, and publishes standardized outputs.

Regression tests must continue to reproduce the known SAGA fixture signals.

## VAHRAM_TRUE_STOCHRSI_V2

This is a separate strategy identity.

It intentionally keeps the surrounding V+ logic comparable:

- Bollinger threshold;
- Volume > 20-period Volume MA;
- candle body strength >= 0.2;
- same strength score structure;
- BUY threshold below 20;
- SELL threshold above 80.

The changed research variable is the oscillator:

VAHRAM_ORIGINAL_V1:
Close-range normalization.

VAHRAM_TRUE_STOCHRSI_V2:
Wilder RSI -> rolling RSI normalization -> smoothed StochRSI K.

Historical evidence must never be mixed between the two strategy IDs.

## H4 Event Study semantics

Event study is signal-quality research, not a trading-strategy backtest.

For a signal calculated on candle T close:

    reference entry = candle T+1 open

For horizon H:

    target = candle T+H close

Examples:

- +1 = T+1 open -> T+1 close;
- +3 = T+1 open -> T+3 close;
- +7 = T+1 open -> T+7 close;
- +14 = T+1 open -> T+14 close.

BUY directional return equals the underlying return.

SELL directional return is the sign-inverted underlying return.

If a future horizon is unavailable, no result is invented.

## Costs

Event Study does not apply fees/slippage.

That is deliberate: this phase measures signal direction after a conservative availability boundary.

Fees, slippage, exits, position state, holding period and P&L belong to H5 Trading Backtest and must not be confused with H4 event-study evidence.
