# TOTAL MARKET CAP TREND SHAPE V2 — PREREG

Date: 2026-09-28
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Purpose

Test whether the *shape* of the SMA25/50/100 structure helps distinguish continuation from mean reversion when TradingView CRYPTOCAP:TOTAL is already stretched above SMA100.

This is a follow-up to V1. No V1 thresholds or outcome barriers are changed.

## Frozen source

Repository cache only:

`data/market/cryptocap_total_d1.csv`

- symbol: CRYPTOCAP:TOTAL
- provider: TradingView
- timeframe: 1D
- rows: 1,398
- range: 2022-11-29 through 2026-09-26
- SHA256: `d0c9b3bac915e484069c6302ddce1a0b050abbb8de155e4106e39cf6e519d3ee`

No external market data may be used.

## Reused V1 event semantics

Only SMA100 upside episodes are used.

Thresholds:
- +10%
- +15%
- +20%
- +25%
- +30%
- +40%

Episode de-duplication, RETURN50, BREAKOUT25 and 90-day horizon are exactly the same as V1.

## Causal features at trigger

Recompute from daily closes:
- SMA25
- SMA50
- SMA100
- 5-day slope of each SMA
- fan spread = `(SMA25 - SMA100) / SMA100` when bull-stacked
- fan spread 5-day change = current fan spread - fan spread 5 days earlier

## Frozen regime labels

### POWERED_TREND

All conditions are true at the trigger close:

1. `SMA25 > SMA50 > SMA100`
2. SMA25 5-day slope > 0
3. SMA50 5-day slope > 0
4. SMA100 5-day slope > 0
5. fan spread 5-day change > 0

Interpretation: all tested averages are rising and the bullish fan is still opening.

### CATCHUP_PHASE

All conditions are true:

1. `SMA25 > SMA50 > SMA100`
2. all three 5-day slopes > 0
3. fan spread 5-day change <= 0

Interpretation: the broad bullish stack remains intact, but the fan is no longer opening; slower averages are catching the faster structure.

### TRANSITION_MIXED

Everything else.

No regime thresholds are tuned after seeing the outcome data.

## Outputs

For each threshold and regime report:
- N
- RETURN_FIRST %
- BREAKOUT_FIRST %
- unresolved/censored %
- median trigger distance
- median additional extension
- median days to maximum extension
- median days to RETURN50
- median days to SMA100 touch

Also report an aggregate view across +10% through +30% threshold observations. The aggregate is descriptive only because multiple thresholds can belong to the same broad market move.

## Current-state classification

Classify the latest cached day, 2026-09-26, using the same frozen regime definition.

## Boundary

This test does not create or modify any trading rule, Telegram alert, allocation decision, or execution logic.

Target test level:

`TEST_LEVEL: REPOSITORY_CACHE_STRESS_TEST`
