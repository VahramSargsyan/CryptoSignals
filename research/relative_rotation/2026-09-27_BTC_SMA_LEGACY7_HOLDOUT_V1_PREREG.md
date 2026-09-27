# BTC SMA LEGACY-7 HOLDOUT V1 — Preregistration

Date: 2026-09-27
Branch: `research/btc-sma-legacy7-holdout-v1`
Mode: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING
Status: PREREGISTERED / HOLDOUT_UNOPENED_BEFORE_EXECUTION / NO PRODUCTION CHANGE

## Purpose

Test whether the frozen BTC-SMA development shortlist generalizes to an older historical regime that was not used in the 2023-2026 tuning search.

Because PEPE did not exist early enough to support the exact frozen 8-asset universe plus causal SMA200 warmup, an exact older 8-asset holdout is impossible.

Therefore this experiment is explicitly a different-universe reverse-time holdout:

`LEGACY_7 = ATOM, TWT, BNB, SOL, TRX, AAVE, LINK`

PEPE is excluded only because it is historically unavailable.

This experiment must NOT be called an exact validation of the 8-asset production candidate.

## Holdout data boundary

Older data request is frozen before execution:

- requested start: 2020-01-01 UTC
- requested end: 2023-05-04 UTC
- source: Binance 1D historical downloader
- BTC source: Binance BTCUSDT 1D

The actual common LEGACY_7 start will be determined only after download from the latest listing date among the seven assets.

Evaluation starts only after all required causal warmups are valid:
- 180-day relative median
- own SMA200
- VOL30
- BTC SMA100

No data from 2023-05-05 or later may enter this holdout.

## Frozen router and stress rules

Unchanged numeric rules:

Relative router:
- 180-day rolling relative median
- ARM threshold 15%
- reversal confirmation 3%
- next-open execution
- transition cost 0.1%

Stress layer:
- breadth = number of LEGACY_7 assets above own SMA200
- enter stress after 3 consecutive closes breadth <=3
- next-open defensive execution
- re-arm future crisis after 3 consecutive closes breadth >=5

Important:
The same numeric 3/5 thresholds are intentionally preserved even though the universe is 7 instead of 8.
This is a portability stress test, not a retuned 7-asset optimization.

## Frozen BTC candidates

Test exactly:

1. BTC SMA25 / SMA100
2. BTC SMA30 / SMA100
3. BTC SMA12 / SMA100

No other BTC SMA pair may be added.

BTC bullish crossover on close D:
- fast(D) > slow(D)
- fast(D-1) <= slow(D-1)

Cash re-entry:
- only a fresh crossover after cash entry counts;
- execute at D+1 open directly into current shadow target;
- ordinary relative rotations resume;
- another cash defense in the same crisis is forbidden;
- future crisis defense re-arms only after breadth>=5 x3.

BTC is not added to the relative-router universe.

## Comparators

LEGACY_7 comparators:

- BASELINE relative router
- LOW_VOL defense using lowest trailing-30d realized-volatility asset among LEGACY_7
- frozen-timing CASH defense
- BTC 25/100
- BTC 30/100
- BTC 12/100

All comparators use the same LEGACY_7 data and same state-reset boundary.

## Holdout evaluation

Primary:
- one full eligible holdout from first date where all causal warmups are valid through 2023-05-04.

Robustness:
- consecutive non-overlapping complete 180-day windows from the eligible anchor;
- consecutive non-overlapping complete 120-day windows from the same anchor;
- discard only incomplete terminal remainder;
- reset state at each window start.

## Required metrics

For every comparator:
- median return across all starting assets
- worst starting-asset return
- positive starts
- median and worst max drawdown
- actual transition count
- defensive/cash exposure
- defensive/cash entries

For BTC candidates:
- re-entry count
- median and maximum cash wait
- unresolved cash episodes

Robustness:
- BTC candidate wins vs LEGACY_7 LOW_VOL on return
- DD wins
- simultaneous return+DD wins
- 180d and 120d separately

## Interpretation rule

The development ranking is frozen and MUST NOT be recomputed on the holdout.

Report the three candidates in their original order:
1. 25/100
2. 30/100
3. 12/100

Possible conclusions:

- GENERALIZES_ON_LEGACY7
- PARTIAL_GENERALIZATION
- FAILS_LEGACY7
- INCONCLUSIVE_DATA_LIMITATION

Even a strong result is only evidence of regime portability.
It is not exact validation of the 8-asset strategy because the universe differs.

A weak result must be preserved and must not trigger holdout retuning.

## Runtime impact

Production changed: NONE
Paper-live changed: NONE
Migration required: NO
Automatic execution authorized: NO
Manual confirmation remains required.

TEST_LEVEL: PREREGISTRATION_ONLY
