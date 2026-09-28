# U10 Single-Month Surge 95% v1 — preregistration

Date: 2026-09-28
Mode: STRESS_TEST_ONLY
Scope: one-parameter sensitivity check of the established single-month surge trigger; no live/paper changes

## Objective

Test exactly one change:

- existing single-month surge threshold: +100%
- candidate threshold: +95%

Everything else remains frozen.

This study is NOT the multimonth STREAK95 study.

A qualifying event here is one selected calendar month whose selected monthly change is at least +95%.

## Monthly basis

Reuse the existing selected-month extreme series:

- for each calendar month, calculate the daily-close equity minimum and maximum;
- compare both with the previously selected monthly extreme;
- select whichever has the larger absolute percentage displacement;
- selected monthly change = selected extreme / previous selected extreme - 1.

No month-end-close substitution.

## Event comparison

Compare:

1. all single-month events >= +100%;
2. all single-month events >= +95%;
3. incremental events in the band:
   +95% <= selected monthly change < +100%.

For each event measure worst running-peak drawdown within:
- 31 days;
- 62 days;
- 93 days.

Report hit rates for:
- -15%;
- -25%;
- -35%.

The incremental 95-100% band is the critical test:
if those newly admitted events behave materially worse than >=100% events, lowering the threshold is not supported.

## Frozen trading mechanics

Use the previously preregistered 5-bar fractal buy-stop implementation unchanged.

P1-SURGE95:
- arm after one selected calendar month >= +95%;
- track post-arm U10 reference running peak;
- cash-out after >=5% daily-close pullback;
- sell 30% next open;
- 0.1% modeled cash-out cost;
- while cash is parked, update latest U10 reference running peak;
- activate re-entry search after reference equity is >=15% below latest peak;
- use confirmed 5-bar swing high on currently held U10 asset:
  2 bars left / pivot / 2 bars right;
- pivot known only after both right bars close;
- buy-stop active from following session;
- lower confirmed pivot highs ratchet stop downward;
- U10 asset rotation cancels/resets the stop;
- gap above stop fills at open;
- otherwise session high touch fills at stop;
- reinvest all parked cash;
- 0.1% modeled re-entry cost.

P2-SURGE95:
- identical except cash-out after >=10% pullback.

## Controls

For canonical U10 and all alternative topologies compare:

- baseline U10;
- P1/P2 with +100% trigger and 5-bar fractal re-entry;
- P1/P2 with +95% trigger and the identical 5-bar fractal re-entry.

No other implementation changes are allowed.

## Required metrics

Event study:
- event count >=95%;
- event count >=100%;
- incremental 95-100% event count;
- -25% hit rate within 31/62/93d for each group;
- median worst running drawdown for each horizon.

Portfolio:
- final equity;
- delta vs baseline;
- delta vs same P1/P2 using +100%;
- max drawdown;
- max-DD change vs baseline;
- cash-outs;
- re-entries;
- unfinished cycles;
- cash days.

Cross-topology:
- 95% final > baseline rate;
- 95% final > corresponding 100% variant rate;
- DD improved vs baseline rate;
- BOTH final and DD improved rate;
- median/q25/q75 terminal delta vs baseline;
- median/q25/q75 delta vs 100% trigger;
- incremental-event prevalence.

## Topology stress

Use the same exhaustive 792 frozen U10 topologies:
- mandatory ATOM/TWT/PEPE;
- choose 7 of the same fixed remaining 12-asset pool.

These topologies share market dates and are not independent temporal samples.

## Guardrails

- test 95% only;
- do not test 90/92/97/99%;
- do not change P1/P2;
- do not change 30%;
- do not change 15% search activation;
- do not change 5-bar pivot mechanics;
- do not use 3-bar here;
- do not combine with multimonth STREAK95;
- no live/paper promotion.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
