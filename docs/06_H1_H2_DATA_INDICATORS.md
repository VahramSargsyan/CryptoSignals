# H1/H2 — Market Data Adapter and Shared Indicators

WORKFLOW_MODE: BUILD_NEW_APP  
RISK_CLASS: L3 — shared research data/indicator infrastructure  
MIGRATION_REQUIRED: NO

## Definition of Ready

CAPABILITY_ID: CRYPTO_STRATEGY_LAB_H1_H2  
USER_RESULT: historical candles and indicators can be reused by every future strategy without changing legacy behavior.  
CANONICAL_SOURCE_OF_TRUTH: Binance/source-provider OHLCV for market data; repository code for indicator formulas.  
SCOPE: Binance historical adapter, listing-aware metadata, closed-candle filtering, shared indicators.  
OUT_OF_SCOPE: strategy contract, event study, position engine, optimization, regimes, Google Sheets, Telegram, live trading.  
DO_NOT_TOUCH: legacy scripts, VAHRAM_ORIGINAL_V1 formulas, existing repository CSV files.  
DEPENDENCIES: pandas; an injected Binance-compatible client for runtime download.  
SHARED_HOTSPOTS: core/data/candles.py, future strategy feature pipeline.  
SCHEMA / ID / RELATION IMPACT: none persisted.  
MIGRATION_PLAN: not required.  
ROLLBACK: revert this bounded feature PR; legacy runtime remains intact.  
PROMOTION_PATH: tests -> PR -> main; no production/live promotion in this phase.  
REVALIDATE_IF: Binance kline contract changes, timeframe semantics change, indicator formula changes, or canonical candle contract changes.  
UNRESOLVED_DECISIONS: NONE.

## H1 — Binance historical adapter

The adapter accepts a Binance-compatible client rather than constructing credentials or storing secrets.

It records:

- requested start/end;
- actual first/last closed candle;
- whether history begins after the requested start (listing_truncated);
- raw row count;
- closed row count;
- explicit NO_DATA / NO_CLOSED_DATA state.

Important listing rule:

A symbol that first appears in 2024 is not treated as having three years of missing candles merely because research requested data from 2021. Gap detection starts inside the history actually returned by the provider; the listing/start mismatch is preserved separately as metadata.

Current implementation starts with 1D and leaves 4H/1H available in the shared timeframe map.

## H2 — shared indicators

Shared feature formulas are pure reusable functions.

### Frozen historical oscillator

vahram_close_range_oscillator

This exactly preserves the old behavior formerly named StochRSI:

    Close position inside rolling Close min/max
    -> rolling smoothing
    -> 0..100

It is intentionally not renamed back to canonical StochRSI.

### RSI

rsi_wilder

Definition:

- price changes;
- gains/losses;
- initial SMA seed;
- Wilder recursive smoothing;
- RSI 0..100.

Flat gain/loss state returns 50.

### True Stochastic RSI

true_stoch_rsi

Definition:

    RSI
    -> rolling RSI min/max normalization
    -> raw StochRSI 0..100
    -> smoothed K
    -> smoothed D

If the rolling RSI range is zero, the value remains undefined rather than inventing a signal.

### Other shared features

- Bollinger Bands;
- Volume moving average;
- candle body strength.

## Protection rule

VAHRAM_ORIGINAL_V1 remains connected to its frozen implementation for now.

The new shared indicator layer does not silently alter historical signals.

A future H3 strategy adapter can prove equivalence before routing the baseline through shared components, and VAHRAM_TRUE_STOCHRSI_V2 will be a separate strategy identity.
