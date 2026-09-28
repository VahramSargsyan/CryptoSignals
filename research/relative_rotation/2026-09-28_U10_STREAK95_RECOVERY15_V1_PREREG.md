# U10 Multimonth STREAK95 + Recovery15 v1 — preregistration

Date: 2026-09-28
Mode: STRESS_TEST_ONLY
Scope: research-only fallback re-entry test for the multimonth STREAK95 regime; no live/paper changes

## Objective

Apply the exact same 15-day / 98%-reference recovery fallback previously tested on single-month SURGE95 to the multimonth STREAK95 strategy.

No other rule changes.

## Frozen STREAK95 trigger

A qualifying event requires:

- at least 2 consecutive positive selected calendar months;
- cumulative selected-equity gain >=95%;
- any zero/negative selected month resets the streak;
- a single +95% month does not qualify.

## Frozen cash-out logic

P1-STREAK95:
- after STREAK95, track latest running U10 reference peak;
- cash-out 30% after >=5% daily-close pullback;
- execute next open;
- 0.1% modeled cash-out cost.

P2-STREAK95:
- identical except cash-out after >=10%.

## Frozen normal re-entry

Normal path remains the existing 5-bar fractal buy-stop:

- while cash is parked, update latest U10 reference peak;
- activate fractal search after reference equity is >=15% below latest peak;
- use current held U10 asset HIGH prices;
- 2 bars left / pivot / 2 bars right;
- pivot known only after both right-side bars close;
- buy-stop active from next session;
- lower confirmed pivot highs ratchet the stop downward;
- U10 asset rotation resets the order;
- gap above stop fills at open;
- otherwise high touch fills at stop;
- re-invest all parked cash;
- 0.1% modeled re-entry cost.

## Recovery15 fallback

Exactly as in the prior SURGE95 fallback experiment:

- wait 15 calendar days after actual cash-out execution;
- recovery level = 98% of frozen-U10 reference OPEN equity at cash-out execution;
- reference is U10 portfolio equity, not sold-token price.

At first eligible daily close:

1. If reference close >= recovery level:
   - schedule full cash-sleeve re-entry at next open;
   - route = RECOVERY_IMMEDIATE.

2. If reference close < recovery level:
   - enter recovery-cross mode;
   - when reference close first crosses from below to >= recovery level,
     schedule full re-entry at next open;
   - route = RECOVERY_CROSS.

Fallback re-entry buys the asset currently held by frozen U10.

## Causal priority

If fallback signal was generated on PRIOR close:
- execute fallback at CURRENT open;
- cancel fractal stop before observing current-session high.

Otherwise:
- existing fractal stop may fill at current open/high.

No same-close fallback fill.

## Controls

For both P1 and P2 compare:

CONTROL:
- original multimonth STREAK95 + 5-bar fractal re-entry;
- no timeout/fallback.

CANDIDATE:
- same CONTROL plus Recovery15.

## Required metrics

Canonical U10:
- baseline final / DD;
- control P1/P2 final / DD;
- candidate P1/P2 final / DD;
- candidate vs control delta;
- cash days;
- fractal vs fallback re-entry counts;
- unfinished cycles.

Across 791 alternative U10s:
- candidate > control rate;
- candidate = control rate;
- candidate < control rate;
- candidate > ordinary baseline rate;
- BOTH final and DD better vs baseline rate;
- fallback-used universe rate;
- unfinished-cash rate;
- median/q25/q75 delta vs control;
- median cash-day change.

Cycle level:
- total completed cycles;
- FRACTAL count;
- RECOVERY_IMMEDIATE count;
- RECOVERY_CROSS count;
- fallback share;
- cash-cycle duration;
- reference/recovery ratio at fallback eligibility.

## Guardrails

- no parameter sweep;
- 15 days fixed;
- 98% fixed;
- STREAK95 fixed;
- P1/P2 fixed;
- 30% cash fixed;
- 5-bar fractal fixed;
- no 3-bar;
- no old-history tuning;
- no live/paper promotion.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
