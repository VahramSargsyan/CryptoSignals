# RR EXTREME CONTINUATION CLASSIFIER V2 — PREREGISTRATION

Date: 2026-10-03
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Research objective

The V1 acceleration tests established two facts:

1. AAVE-like episodes can be detected causally after the corrected RR+DDG entry.
2. A naive "+10% relative acceleration => switch" rule fails on typical cases.

V2 asks a narrower question:

> Can a fixed two-stage persistence/re-expansion pattern isolate a higher-quality subset of +10% acceleration events without retuning the +10% trigger itself?

## Frozen base trigger inherited from V1

For every corrected RR+DDG route SOURCE -> B:

- hypothetical entry into B at the next daily open;
- inspect the first 1-3 completed D1 bars;
- candidate C = strongest TARGET token relative to B since the common entry open;
- base trigger occurs at the first close where C's relative impulse versus B is >= +10%.

No candidate switch occurs at this base trigger in V2.

## Primary V2 classifier — PERSIST2_REEXPAND

After the first +10% trigger for candidate C:

1. observe the next completed D1 close;
2. observe one additional completed D1 close;
3. candidate C must remain the #1 acceleration candidate at BOTH closes;
4. at the second confirmation close, C's relative impulse versus B must be
   strictly greater than C's impulse at the original trigger close.

If all conditions pass:

- classifier = PASS;
- hypothetical switch B -> C occurs at the next daily open after the second
  confirmation close.

This pattern is fixed before seeing V2 results.

## Fixed secondary variants

These are diagnostics, not parameter search:

### PERSIST1
- C remains #1 at the next close;
- switch at the following open.

### PERSIST2
- C remains #1 at both next closes;
- switch at the following open after the second persistence close.

### PERSIST2_REEXPAND
- PERSIST2 plus final impulse > original trigger impulse;
- this is the PRIMARY V2 classifier.

No alternative threshold is selected after seeing results.

## Outcome convention

From each hypothetical switch open compare candidate C with staying in corrected
RR+DDG destination B.

Fixed horizons:
- 3 D1 bars
- 7 D1 bars
- 14 D1 bars
- 30 D1 bars

Primary horizon:
- 14 bars

Primary metrics:
- number of base +10% triggers;
- classifier pass count/rate;
- candidate beats B rate;
- median and mean relative excess;
- 25th/75th percentile;
- strong continuation rate: relative excess >= +20%;
- false-positive rate: relative excess <= 0.

The classifier is only interesting if it materially improves quality versus V1
while retaining a non-trivial sample.

## Current AAVE case

For corrected LINK -> TRX on 2026-09-28:

- base entry: TRX at 2026-09-29 open;
- verify AAVE's original +10% trigger;
- verify candidate identity on the next two closes;
- verify whether final impulse exceeds trigger impulse;
- report whether AAVE passes PERSIST1, PERSIST2 and PRIMARY;
- if PRIMARY passes, hypothetical switch date = next open;
- compare AAVE vs TRX through latest available closed D1 candle.

## Predeclared structural diagnostics

Diagnostics are recorded but are NOT used to change the primary rule in V2:

At trigger and at the second confirmation close record:

- candidate relative impulse;
- candidate cross-sectional return rank among TARGET assets;
- candidate margin versus the second-best TARGET return;
- number of TARGET assets candidate is outperforming since route entry;
- candidate active RR topology count:
  - OUT_COUNT = active pair states pointing from candidate into another TARGET;
  - IN_COUNT = active pair states pointing from another TARGET into candidate;
  - NET_OUT_MINUS_IN = OUT_COUNT - IN_COUNT.

For outcome interpretation, compare these diagnostics between:
- strong continuations (14d relative excess >= +20%);
- ordinary/non-strong outcomes.

No threshold derived from those diagnostics may be promoted inside V2.

## Recent-month audit

Also report V2 pass/fail for routes from:
- 2026-09-03 onward

This recent-month view is descriptive and may be right-censored.

## Fixed as-of

2026-10-03T16:30:00Z

No unclosed D1 candle may enter the test.

## Decision gate

Possible classifications:

- CLASSIFIER_NOT_SUPPORTED
- CLASSIFIER_IMPROVES_PRECISION_BUT_NOT_PRODUCTION_READY
- CLASSIFIER_PROMISING_RESEARCH_SIGNAL

No production promotion is authorized by this test alone.

Even a promising result requires:
- path-dependent full portfolio simulation;
- realistic execution costs;
- unseen forward observation.

## No-retune rule

Do not change after seeing V2:
- +10% base acceleration trigger;
- first-3-bar trigger window;
- two confirmation closes;
- "#1 on both closes" condition;
- final impulse > original trigger impulse condition;
- 14-bar primary horizon.

Any altered feature family or threshold requires V3.

TEST_LEVEL planned:
GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_STRESS_TEST
