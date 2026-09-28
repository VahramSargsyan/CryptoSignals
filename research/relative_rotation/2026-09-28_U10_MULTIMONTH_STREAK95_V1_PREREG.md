# U10 Consecutive Positive-Month Streak >=95% v1 — preregistration

Date: 2026-09-28
Mode: STRESS_TEST_ONLY
Scope: research-only trigger test; no live/paper strategy changes

## Objective

Test a distinct overheating hypothesis:

> A large cumulative rise built through several consecutive positive calendar months may deserve the same profit-lock treatment as a single +100% surge month.

The trigger is deliberately separated from the existing one-month +100% rule.

Only the trigger definition changes in this study.

## Monthly state basis

Reuse the already-defined monthly selected-extreme path.

For every completed calendar month:
- identify the monthly maximum and minimum U10 reference daily-close equity;
- compare both against the previously selected monthly extreme;
- retain whichever monthly extreme has the larger absolute percentage displacement;
- its change versus the previous selected extreme is the month's selected change.

This is the same diagnostic series already used in the prior monthly-surge research.

No month-end close substitution is introduced.

## Consecutive-positive streak

A streak consists only of completed calendar months whose selected change is strictly positive.

Rules:

- streak starts with the first positive selected month after a non-positive month;
- every following selected month must also be positive;
- if any selected month is zero or negative, the streak resets immediately;
- cumulative streak gain is measured from the selected extreme immediately before the streak started to the latest selected extreme in the streak.

Formula:

current selected extreme / pre-streak selected extreme - 1

## Primary threshold

Arm when cumulative consecutive-positive streak gain reaches or exceeds:

+95%

The 95% threshold is fixed before viewing this test result.

Do not also test 90%, 99%, 100%, 105%, etc. in this study.

## Minimum streak length

The streak must contain at least 2 consecutive positive calendar months.

A single month +95% does NOT qualify for this multimonth rule.

That remains the domain of the existing single-month surge research.

## Causal timing

A calendar month's selected extreme is known only after that calendar month has completed.

Therefore:

- determine streak status after the last daily close of month M;
- if the completed streak first reaches >=95%, arm the multimonth trigger after month M closes;
- use the highest frozen-U10 reference daily-close equity observed within the completed streak as the initial post-streak running peak;
- profit-lock signals can execute no earlier than the next daily session.

No intra-month hindsight trigger is allowed.

## Frozen sale/re-entry mechanics

To isolate the trigger, keep the current 5-bar fractal research mechanics unchanged.

P1-STREAK95:
- streak >=95% over at least 2 consecutive positive selected months
- after arm, track/reference the streak running peak
- sell 30% after >=5% reference daily-close pullback from the running peak
- cash-out next open
- 0.1% modeled cash-out cost
- after cash-out, wait until U10 reference equity is >=15% below latest reference running peak
- search current U10-held asset for confirmed 5-bar swing high:
  - 2 bars left
  - pivot
  - 2 bars right
- pivot known only after both right-side bars close
- buy-stop active from following session
- lower confirmed pivot highs ratchet stop downward
- U10 asset rotation cancels/resets the old stop
- gap above stop fills at open; otherwise daily-high touch fills at stop
- 0.1% re-entry cost

P2-STREAK95:
- identical except sell 30% after >=10% pullback.

## Separation from single-month +100% rule

This study does NOT combine the rules.

It independently measures the multimonth >=95% trigger.

If a historical episode would qualify for both the older single-month rule and this multimonth rule, record the overlap but do not double-sell capital.

Primary comparison is the standalone multimonth trigger versus baseline U10.

## Required event metrics

For every qualifying streak:
- streak start month
- streak end month
- number of positive months
- pre-streak equity
- final selected equity
- cumulative streak gain
- highest reference equity within streak
- subsequent worst running drawdown within 31, 62, and 93 days
- whether subsequent drawdown reached -15%, -25%, -35%

## Required portfolio metrics

For baseline and P1/P2-STREAK95:
- final equity
- total return
- max drawdown
- minimum equity vs initial
- cash-outs
- re-entries
- unfinished cycles
- days in cash
- actual re-entry depths
- cycle durations

## Topology robustness

Use all 792 frozen U10 topologies:
- mandatory ATOM/TWT/PEPE
- choose 7 of the same fixed remaining 12-asset pool

Report across 791 non-canonical U10s:
- number of qualifying streaks
- percentage of universes with at least one qualifying streak
- subsequent -25% hit rate within 31 / 62 / 93 days
- P1/P2 terminal > baseline rate
- DD improvement rate
- BOTH improvement rate
- median terminal delta
- q25/q75 terminal delta
- unfinished-cycle rate

## Guardrails

- no threshold sweep
- no changing 95%
- no changing minimum 2 months
- no combining with single-month trigger yet
- no 3-bar pivot in this study
- no 2020-2022 tuning
- no live/paper promotion

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
