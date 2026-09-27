# CASH CRISIS QUARANTINE REARM V2 — Preregistration

Date: 2026-09-27
Branch: `research/cash-crisis-quarantine-rearm-v2`
Mode: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING
Status: PREREGISTERED / FROZEN_BEFORE_EXECUTION / NO PRODUCTION CHANGE

## Purpose

Correct the semantic weakness found in CASH_CRISIS_QUARANTINE_V1.

V1 immediately re-armed crisis detection after fixed cash exit. In a persistent low-breadth regime this produced repeated cash entries inside the same underlying crisis.

V2 defines one cash reaction per crisis episode.

## State machine

### ARMED

Normal relative router is active.

Crisis entry:
- breadth <=3 for 3 consecutive closes;
- execute CASH_PROXY at next daily open.

### CASH_QUARANTINE

- hold CASH_PROXY for a fixed preregistered duration;
- shadow router continues;
- do not accumulate low-stress or high-recovery streaks;
- do not shorten or extend the quarantine.

At expiry:
- re-enter the current shadow target at next daily open;
- transition to POST_CASH_DISARMED.

### POST_CASH_DISARMED

- actual capital follows the normal relative router;
- another cash entry is forbidden;
- low-stress streak is not accumulated.

Re-arm only after the already-frozen recovery condition:
- breadth >=5 for 3 consecutive closes.

After the third recovery close:
- state becomes ARMED;
- recovery streak resets;
- the next crisis requires a completely fresh breadth<=3 x3 sequence.

Thus breadth>=5 x3 is used ONLY to declare the previous crisis episode finished and re-arm the future crisis detector.
It does NOT determine how long capital remains in cash.

## Frozen entry and execution

Unchanged:
- universe: ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK;
- own causal SMA200;
- entry after 3 closes breadth<=3;
- next-open execution;
- shadow router continues;
- 0.1% transition cost per actual transition.

## Preregistered cash durations

All must be reported:
- 14d
- 21d
- 30d
- 45d
- 60d

No duration may be selected post hoc and called validated.

Cash day convention:
- entry open = day 1;
- hold through close of day N;
- return to current shadow target at open of day N+1.

## Evaluation

Same boundaries as V1:
1. reproduction period 2025-03-29 -> 2026-03-28;
2. already-open 2026-03-29 -> 2026-09-26;
3. full eligible history from first SMA200/VOL30-ready date;
4. non-overlapping complete 180d windows;
5. non-overlapping complete 120d windows.

State resets at each evaluation-window start.

## Comparators

- baseline router;
- frozen LOW_VOL defense;
- frozen-timing CASH;
- V2 14/21/30/45/60d re-arm quarantine.

V1 evidence remains a separate comparator for interpreting re-trigger churn; its thresholds/durations are not changed.

## Required diagnostics

For each V2 duration:
- return;
- max drawdown;
- worst start;
- positive starts;
- cash entries;
- cash days/exposure;
- actual transitions;
- re-arm count;
- days spent POST_CASH_DISARMED;
- number of crisis episodes acted on.

Robustness:
- windows beating LOW_VOL on return;
- windows beating LOW_VOL on drawdown;
- windows beating LOW_VOL on both;
- windows beating frozen-timing CASH;
- report 180d and 120d separately.

## Interpretation

Possible descriptive verdicts:
- ONE_QUARANTINE_PER_CRISIS_PROMISING
- LOW_VOL_STILL_DOMINANT
- DURATION_SENSITIVE
- MIXED

No single duration can be promoted from this same history.

## Runtime impact

Production changed: NONE
Paper-live changed: NONE
Automatic execution authorized: NO
Migration required: NO

TEST_LEVEL: PREREGISTRATION_ONLY
