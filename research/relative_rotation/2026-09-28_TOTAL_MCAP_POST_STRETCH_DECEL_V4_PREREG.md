# TOTAL MARKET CAP POST-STRETCH DECELERATION V4 — PREREG

Date: 2026-09-28  
Workflow mode: STRESS_TEST_ONLY  
Production/live changes: NONE

## Question

After CRYPTOCAP:TOTAL is already materially stretched above SMA100, does the **first subsequent joint deceleration** of TOTAL momentum and SMA25 speed identify a more useful turning-point zone than the original distance threshold?

This follows V3, which found zero DUAL_DECELERATION observations on the exact threshold-crossing day.

## Frozen source

Repository cache only:

`data/market/cryptocap_total_d1.csv`

- symbol: CRYPTOCAP:TOTAL
- provider: TradingView
- timeframe: 1D
- rows: 1,398
- range: 2022-11-29 through 2026-09-26
- SHA256: `d0c9b3bac915e484069c6302ddce1a0b050abbb8de155e4106e39cf6e519d3ee`

No external market data.

## Anchor stretch episodes

Evaluate two independently reported anchor thresholds:

- +15% above SMA100
- +20% above SMA100

Episode start / re-arm semantics are unchanged from V1:
- start on first close crossing threshold from inside;
- re-arm only after the gap compresses to <= half the threshold.

The two anchor analyses are not independent because the same broad market move can cross both.

## Momentum definitions

TOTAL 5d momentum:
`mom5 = close_t / close_(t-5) - 1`

Price acceleration:
`price_accel5 = mom5_t - mom5_(t-5)`

SMA25 5d slope:
`sma25_slope5 = SMA25_t / SMA25_(t-5) - 1`

SMA25 acceleration:
`sma25_accel5 = sma25_slope5_t - sma25_slope5_(t-5)`

A **DUAL_DECELERATION** day requires:
- `price_accel5 < 0`
- `sma25_accel5 < 0`

## First post-stretch deceleration

For each anchor episode, search forward from the day after threshold crossing until the earliest of:
- 90 calendar days,
- dataset end,
- episode normalization to <= half the anchor threshold.

Record the first DUAL_DECELERATION day, if any.

No requirement is imposed that price momentum itself is still positive; that status is recorded separately.

## Outcome from the deceleration day

Let `Gd` be the positive SMA100 gap on the first DUAL_DECELERATION close.

Starting after that close and for up to 90 calendar days:

### DECEL_RETURN50
Gap <= `0.50 * Gd`

### DECEL_BREAKOUT25
Gap >= `1.25 * Gd`

First barrier wins:
- RETURN_FIRST
- BREAKOUT_FIRST
- SAME_DAY
- UNRESOLVED_90D
- CENSORED

Also record:
- days from stretch trigger to first dual deceleration;
- gap at deceleration;
- TOTAL 5d momentum at deceleration;
- SMA25 5d slope at deceleration;
- whether both momentum levels are still positive;
- prior running maximum gap since stretch trigger;
- whether a new running maximum gap occurs after deceleration before DECEL_RETURN50;
- maximum additional extension after deceleration;
- days to half-gap and SMA100 touch.

## Comparator

For each anchor threshold also report the original V1 trigger outcome distribution using the same RETURN50 / BREAKOUT25 semantics from the anchor trigger itself.

This allows direct descriptive comparison:

`anchor trigger` vs `first post-stretch dual deceleration`.

## Usefulness criterion

The feature is descriptively useful only if:
1. first post-stretch DUAL_DECELERATION occurs in a non-trivial share of anchor episodes;
2. its RETURN_FIRST rate is materially higher than the original anchor trigger's RETURN_FIRST rate;
3. the result is not based only on one or two deceleration events.

No production action is authorized.

## Current episode

For the currently cached 2026 stretch, report:
- most recent +15 and +20 anchor dates;
- whether a post-anchor DUAL_DECELERATION has already occurred;
- its date and state if present.

## Boundary

Research only. Do not modify paper/live logic, Telegram, allocations, execution, or universe definitions.

Target test level:

`TEST_LEVEL: GITHUB_ACTIONS_REPOSITORY_CACHE_STRESS_TEST`
