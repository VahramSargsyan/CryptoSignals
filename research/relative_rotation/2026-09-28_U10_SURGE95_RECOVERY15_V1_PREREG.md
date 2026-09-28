# U10 SURGE95 15-Day Recovery-Stop Fallback v1 — preregistration

Date: 2026-09-28
Mode: STRESS_TEST_ONLY
Scope: research-only fallback re-entry test; no live/paper changes

## Objective

Test one fallback added to the current P1 SURGE95 + 5-bar fractal re-entry strategy.

Primary path remains unchanged:
- single selected-month surge >= +95%;
- P1 cash-out after >=5% pullback from post-surge running peak;
- sell 30% next open;
- 0.1% modeled cash-out cost;
- normal re-entry remains the existing 5-bar fractal buy-stop logic.

The fallback exists only while the 30% cash sleeve is still parked.

## Fixed fallback parameters

- fallback delay: 15 calendar days after actual cash-out execution;
- recovery level: 98% of frozen-U10 reference equity at the actual cash-out execution open;
- recovery reference is the frozen U10 portfolio reference, not the token price sold;
- fallback re-entry buys the asset currently held by frozen U10;
- 0.1% modeled re-entry cost.

No 10/20/30-day sweep.
No 97/99% sweep.

## Why reference equity, not sold-token price

Frozen U10 may rotate assets while cash is parked.

A fallback tied to the sold token could become irrelevant after rotation.

The recovery level therefore stays in U10-reference-equity space and the actual re-entry is into the current frozen-U10 asset.

## Fallback activation semantics

Let:
- cashout_ts = actual cash-out execution timestamp;
- cashout_reference_open = frozen-U10 open equity at that execution;
- recovery_level = 0.98 * cashout_reference_open.

The fallback is eligible on the first daily CLOSE whose calendar timestamp is at least 15 calendar days after cashout_ts, provided normal fractal re-entry has not already completed.

At first eligibility:

1. If frozen-U10 reference close >= recovery_level:
   - schedule full cash-sleeve re-entry at the next daily open.

2. If frozen-U10 reference close < recovery_level:
   - arm recovery-cross mode;
   - thereafter, when reference close first crosses from below recovery_level to >= recovery_level,
     schedule full cash-sleeve re-entry at the next daily open.

This is intentionally conservative:
- the synthetic U10-reference recovery trigger uses closed daily reference equity;
- no intraday synthetic reference high is invented;
- no same-close fill is allowed.

## Competition with normal fractal re-entry

Both paths may exist after day 15.

At every session:
- if a previously active fractal buy-stop fills at the current open/high, fractal wins and fallback is cancelled;
- otherwise any fallback signal generated at the prior close executes at the current open and cancels the fractal path.

The earlier executable re-entry wins.

A fallback signal is generated only at a daily close and can execute only on the following open.

## Control

Compare exactly:

CONTROL:
- SURGE95 + P1 + 5-bar fractal re-entry;
- no timeout/fallback.

CANDIDATE:
- identical CONTROL plus 15-day / 98%-of-cashout-reference recovery fallback.

No P2 in the primary test unless included only as secondary context.
Primary decision is P1 because prior research found it more robust.

## Required cycle metrics

For every cash-out cycle:
- cash-out date;
- cashout reference open equity;
- recovery level;
- 15-day eligibility date;
- reference close at eligibility;
- fallback armed immediately above level vs below-level crossing mode;
- normal fractal re-entry date if it would have occurred;
- actual candidate re-entry route:
  - FRACTAL
  - RECOVERY_IMMEDIATE
  - RECOVERY_CROSS;
- fallback signal date;
- candidate execution date;
- current U10 asset at re-entry;
- reference equity at fallback signal;
- cycle cash days;
- difference in cash days vs control.

## Required portfolio metrics

Canonical U10 and all 791 alternative U10 topologies:

- final equity control;
- final equity candidate;
- delta candidate vs control;
- delta candidate vs baseline U10;
- max drawdown control/candidate;
- max-DD difference;
- fallback-used count;
- percentage of cycles using fallback;
- fractal-used count;
- unfinished cash cycles;
- total cash days.

Cross-topology:
- candidate > control rate;
- candidate = control rate;
- candidate < control rate;
- candidate > ordinary baseline rate;
- BOTH final and DD better vs ordinary baseline rate;
- median/q25/q75 delta candidate vs control;
- median cash-day change;
- fallback-use distribution.

## Guardrails

- no parameter sweep;
- 15 days fixed;
- 98% fixed;
- +95% surge fixed;
- P1 5% cash-out fixed;
- 30% cash sleeve fixed;
- 5-bar fractal re-entry fixed;
- no 3-bar pivot;
- no 2020-2022 tuning;
- no live/paper promotion.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
