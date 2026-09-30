# U10 Monthly Surge Pullback + Peak-Reclaim Fallback v1 — preregistration

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Scope: research-only completion of P1/P2 re-entry logic; no live/paper changes

## Objective

Replace the failed fixed-time fallback idea with one market-state invalidation condition.

The existing natural re-entry remains primary:

- after cash-out, re-enter if frozen-U10 reference daily-close equity reaches -25% from the locked peak.

New fallback:

- if the reference equity instead closes back at or above the locked peak before the -25% natural re-entry occurs, treat the expected deep-pullback thesis as invalidated;
- re-enter all parked cash on the next daily open;
- apply 0.1% modeled re-entry cost.

This fallback has no elapsed-day parameter.

## Frozen P1 / P2

P1:
- arm after +100% monthly surge versus prior completed selected-month extreme;
- track post-arm running peak;
- cash-out after >=5% daily-close pullback from running peak;
- sell 30% next open;
- natural re-entry at -25% from locked peak.

P2:
- identical except cash-out requires >=10% pullback.

Cash-out and re-entry costs remain 0.1%.

No other parameter changes.

## Peak-reclaim semantics

After cash-out execution:

1. If daily close <= 75% of locked peak:
   - natural -25% re-entry signal;
   - re-enter next open;
   - reason = NATURAL_MINUS25.

2. Else if daily close >= locked peak:
   - deep-pullback thesis invalidated;
   - re-enter next open;
   - reason = PEAK_RECLAIM.

3. Otherwise remain in cash.

A completed cycle cannot re-arm from the same surge month.

## Comparison

For canonical U10 and all 791 alternative U10 topologies compare:

- baseline U10;
- P1/P2 without fallback;
- P1/P2 + 65d timeout control from the immediately prior preregistered study;
- P1/P2 + peak-reclaim fallback.

## Required metrics

Per topology/variant:
- final equity
- delta vs baseline
- delta vs no-fallback P1/P2
- delta vs 65d timeout control
- max drawdown
- natural re-entry count
- peak-reclaim re-entry count
- unfinished cycles
- days in cash
- terminal cash

Cross-topology:
- fraction final > baseline
- fraction DD improves vs baseline
- fraction both improve
- fraction peak-reclaim beats no-fallback
- fraction peak-reclaim beats 65d
- median/q25/q75 terminal delta vs no-fallback
- median/q25/q75 terminal delta vs 65d
- peak-reclaim usage rate

## Specific failure-mode analysis

For every peak-reclaim cycle report:
- arm month
- cash-out execution date
- locked peak
- reclaim signal/execution date
- cash duration
- whether a later -25% natural re-entry would have occurred without fallback
- if so, how many days later

This directly tests whether reclaim separates:
- false deep-pullback expectations, where market resumes the uptrend;
from
- slow but real corrections, where waiting longer would have been better.

## Anti-overfit guardrail

Do not add:
- a reclaim buffer above peak;
- a moving average;
- a second timeout;
- a percentage below peak;
- a fixed day count.

Only exact locked-peak reclaim is tested.

## Historical holdout guardrail

Do not use 2020-2022 data while selecting this fallback.

If this study produces a coherent completed cycle, freeze P1/P2 + peak-reclaim before opening the older-history validation.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
