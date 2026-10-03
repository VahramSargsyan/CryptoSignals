# RR MEDIAN CASCADE 25/50/90/180 V1 — PREREGISTRATION

Date: 2026-10-03
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Research question

The prior lookback-family test showed that replacing the canonical 180-day RR
anchor with shorter 25/45/50/90 references performs poorly.

This V1 tests a materially different mechanism:

> Keep the canonical MEDIAN_180 Relative Rotation engine unchanged, but read
> MEDIAN_25 / MEDIAN_50 / MEDIAN_90 / MEDIAN_180 together as a directional
> structure on the pair ratio C / B.

The hypothesis is that a sequential median cascade may distinguish a persistent
relative trend from a one-day acceleration spike.

## Canonical RR core remains unchanged

- Binance Spot D1 closed candles;
- pair ratio = right / left;
- canonical RR anchor = rolling median 180;
- ARM = 15%;
- reversal confirmation = 3%;
- Destination Dominance override = accepted 1.50x;
- execution convention for RR = next daily open;
- TARGET universe unchanged;
- no production/live behavior change.

## Median-cascade construction

For each ordered pair:

- candidate C;
- baseline / currently held asset B;
- ratio R = C_close / B_close.

Compute causal rolling medians:

- M25 = median(R, 25)
- M50 = median(R, 50)
- M90 = median(R, 90)
- M180 = median(R, 180)

All values use data available through the current closed D1 candle only.

## Upward cascade stages

The upward direction means candidate C is establishing a stronger relative
trend versus B.

### S1_FAST_CROSS

Event occurs on date T when:

- M25(T) > M50(T)
- M25(T-1) <= M50(T-1)

This is the first fast/medium crossover.

### S2_MID_STACK

Event occurs on the first date after an S1 event when:

- M25 > M50 > M90

No additional threshold is required.

### S3_FULL_STACK

Event occurs on the first date after S2 when:

- M25 > M50 > M90 > M180

No distance threshold is required.

The event sequence is stateful per ordered pair. After S3, a new upward
sequence cannot begin until the stack has broken enough that a future fresh S1
cross is possible.

## Downward mirror

For diagnostic symmetry, also record the mirrored downward sequence:

- D1: M25 crosses below M50;
- D2: M25 < M50 < M90;
- D3: M25 < M50 < M90 < M180.

Downward events are not used to select a production action in V1.

## Pair-event forward outcomes

At every upward S1/S2/S3 event compare candidate C with baseline B from the
next daily open.

Fixed horizons:
- 3 bars
- 7 bars
- 14 bars
- 30 bars

Metric:

RELATIVE_EXCESS(C,B,h) =
(1 + C_return_from_next_open_h) /
(1 + B_return_from_next_open_h) - 1

Report:
- event count;
- candidate beats baseline rate;
- median / mean relative excess;
- 25th / 75th percentile;
- >= +10% continuation rate;
- >= +20% continuation rate;
- <= 0 false-positive/non-outperformance rate.

Pair-event evidence is correlated and is not interpreted as independent trades.

## Cascade timing

For completed S1 -> S2 sequences report:
- median / mean / p25 / p75 days S1 -> S2.

For completed S1 -> S2 -> S3 sequences report:
- S2 -> S3 lag;
- S1 -> S3 total lag.

Also report:
- share of S1 events reaching S2 within 30 days;
- share of S1 events reaching S3 within 60 days.

These windows are descriptive timing summaries, not optimized trading rules.

## RR-overlay diagnostic

For each canonical corrected RR+DDG transition SOURCE -> B executed at next open:

1. after entry into B, inspect all other TARGET candidates C;
2. during the first 30 completed D1 bars, record the first upward cascade event
   for each stage S1 / S2 / S3;
3. on each stage date choose one practical candidate:
   - highest achieved stage first;
   - within the same stage, largest current ratio momentum over the prior
     10 closed bars:
       MOM10 = R_close / R_close_10bars_ago - 1;
   - ties broken alphabetically;
4. compare that candidate with simply staying in B from the next open.

Also persist a pre-entry state audit at each canonical RR signal close:
- for every alternative TARGET candidate C versus destination B;
- record current ordering state NONE / S1_ONLY / S2_STACK / S3_FULL_STACK;
- record MOM10;
- identify the highest-stage candidate already active before RR execution.

This pre-entry state audit is descriptive only and is not used to retune the
stage definitions.

This is diagnostic only. It does not mutate the later RR path in V1.

Primary overlay stage:
- S2_MID_STACK

Reason:
- S1 may be too early/noisy;
- S3 may be too late;
- S2 is fixed in advance as the middle structural confirmation.

S1 and S3 remain fixed comparison diagnostics.

## AAVE / TRX current episode

For pair:
- candidate = AAVE
- baseline = TRX

Persist daily values from 2026-08-01 through latest closed D1:

- ratio AAVE/TRX;
- M25;
- M50;
- M90;
- M180;
- stage flags;
- crossover events.

Report exact dates of:
- latest S1;
- subsequent S2, if any;
- subsequent S3, if any;
- current ordering as of latest closed candle.

Also compare these dates against:
- corrected LINK -> TRX signal on 2026-09-28 close;
- corrected TRX entry on 2026-09-29 open;
- AAVE acceleration observations on Sep-29 / Sep-30 / Oct-01.

This directly tests whether the median cascade would have been early enough to
help with the live case.

## Recent-month audit

Persist all upward cascade events from:
- 2026-09-03 onward

and identify which TARGET pairs were in S1/S2/S3 states on:
- 2026-09-28
- 2026-09-29
- 2026-09-30
- 2026-10-01
- latest closed candle.

## Interpretation gate

Frozen research classifications:

### CASCADE_NOT_SUPPORTED
if primary S2:
- 14d candidate-beats-baseline rate <= 50%;
- AND median 14d relative excess <= 0.

### CASCADE_PROMISING_DIAGNOSTIC
if primary S2:
- 14d candidate-beats-baseline rate > 50%;
- AND median 14d relative excess > 0;
- AND sample N >= 100.

### CASCADE_STRONG_RESEARCH_SIGNAL
requires all CASCADE_PROMISING_DIAGNOSTIC conditions plus:
- S2 >=20% continuation share > S1 >=20% continuation share;
- among calendar years with at least 20 complete S2 14d outcomes, at least
  two years independently have both:
  - candidate-beats-baseline rate > 50%;
  - median 14d relative excess > 0.

No production promotion follows automatically.

## No-retune rule

After V1 results do not:
- add 20/30/45/60/100/120 periods;
- add distance thresholds between medians;
- change S2 to another ordering;
- optimize MOM10;
- change forward horizons;
- redefine current AAVE example.

Any such change requires V2.

## Fixed as-of

2026-10-03T16:30:00Z

No unclosed D1 candle may enter the test.

## Evidence level planned

TEST_LEVEL:
GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_STRESS_TEST
