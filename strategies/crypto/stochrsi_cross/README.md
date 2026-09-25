# STOCHRSI_CROSS_V1

Status: **EXPERIMENTAL**  
WORKFLOW_MODE: **BUILD_NEW_APP**  
RISK_CLASS: **L2 — research-only strategy candidate**  
MIGRATION_REQUIRED: **NO**  
LIVE/PAPER-LIVE IMPACT: **NONE**

## Research objective

Create one independently testable StochRSI K/D crossover candidate from a previously shortlisted external OSS idea and run it through the existing CryptoSignals evidence pipeline.

## Canonical local identity

- strategy_id: `STOCHRSI_CROSS_V1`
- strategy_version: `1.0.0`
- timeframe for first research run: `1D`
- execution engine: existing `LONG_ONLY_SIGNAL_FLIP_V1`
- initial status: `EXPERIMENTAL`

## OSS provenance

External repository:

- repository: `w1ld3r/crypto-signal`
- source commit: `8ae473999f83d3025e6e9429c3734429fb0e180d`
- license: MIT
- source files reviewed:
  - `app/analyzers/indicators/stochrsi_cross.py`
  - `app/analyzers/indicators/stoch_rsi.py`
- license file reviewed at the same source commit.

The external default StochRSI Cross parameters are:

- RSI period = 14
- stochastic RSI period = 14
- smooth K = 10
- smooth D = 3

The source marks a bullish event when previous K < previous D and current K > current D, and a bearish event for the inverse strict cross.

## Local adaptation boundary

CryptoSignals does **not** import the external TA-Lib implementation.

Instead:

1. use project-owned `core.indicators.technical.true_stoch_rsi`;
2. preserve the external default 14 / 14 / 10 / 3 parameterization;
3. historicalize the same two-candle strict crossover predicate for every eligible candle;
4. map bullish cross -> `BUY`;
5. map bearish cross -> `SELL`;
6. assign strength 100 because the external crossover is binary and defines no native strength score.

This is an adaptation of the strategy concept, not a claim of byte-for-byte equivalence with TA-Lib output.

## Source-of-truth and affected files

Canonical strategy implementation:

`strategies/crypto/stochrsi_cross/strategy.py`

The shared V1/V2 default comparison must remain unchanged unless a research request explicitly selects this candidate.

## Data/schema impact

None.

No Google Sheets schema, IDs, columns, relations, production datasets, secrets, or live notification behavior are changed.

## External dependencies

No new runtime dependency is introduced.

The implementation deliberately reuses the repository's existing pandas-based canonical indicator engine.

## First research acceptance evidence

Before this strategy can move from EXPERIMENTAL to BACKTESTED:

1. repository unit/regression tests must pass;
2. exact source SHA and parameters must be recorded;
3. Binance 1D data quality must pass;
4. research period must be recorded;
5. fees = 10 bps and slippage = 5 bps must be included;
6. next-candle-open execution must be used;
7. trade/event evidence must be produced for all requested symbols;
8. no acceptance claim may be made from 2026 as a new blind period because 2026 has already been opened in this laboratory.

## Initial research window

First historical run:

- 2021-01-01 inclusive
- 2026-01-01 exclusive
- current 13-symbol universe
- 1D candles
- validation type: `HISTORICAL_RESEARCH_2021_2025`

This is historical research only, not blind OOS acceptance.

## Rollback

Remove the new strategy files and explicit-candidate selection support. Existing V1/V2 default comparison behavior must continue to produce exactly those two candidates.

## Promotion path

`EXPERIMENTAL -> BACKTESTED -> future held-out/walk-forward evidence -> acceptance decision -> PAPER_LIVE`

No direct promotion to live signals is allowed.
