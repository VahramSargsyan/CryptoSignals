# U10 Monthly Surge Pullback + 65d Timeout v1 — preregistration

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Scope: research-only completion of the cash re-entry cycle; no live/paper strategy changes

## Objective

Complete the already-preregistered P1/P2 surge-pullback overlay with one explicit fallback for cases where the planned -25% re-entry never arrives.

This study intentionally does **not** tune the timeout length.

Primary fallback:

65 calendar days after actual cash-out execution.

If the -25% locked-peak re-entry condition has not already triggered, reinvest all parked cash into the asset currently held by frozen U10 at the first daily open on or after the 65-day deadline.

## Frozen P1 / P2

P1:
- surge arm: +100% versus the prior completed selected-month extreme
- post-arm running-peak tracking
- cash-out signal: -5% from running peak
- cash fraction: 30%
- cash-out execution: next open
- re-entry signal: -25% from locked peak
- re-entry execution: next open
- cash-out and re-entry cost: 0.1% each

P2:
- identical except cash-out signal is -10% from running peak

No other parameter changes.

## 65-day fallback semantics

Timeout clock begins at the actual cash-out execution timestamp.

Natural re-entry has priority if its -25% signal was already generated before the timeout open.

Otherwise:

- timeout deadline = cash-out execution timestamp + 65 calendar days
- at the first daily open on or after that deadline, re-enter the full parked cash sleeve
- buy the asset currently held by frozen U10
- apply the same 0.1% modeled re-entry cost
- mark re-entry reason = TIMEOUT_65D

The timeout does not require price recovery, moving averages, or a new U10 signal.

This is deliberately a simple fallback completion rule.

## Comparison

For canonical U10 and all 791 alternative U10 topologies compare:

1. BASELINE U10
2. P1 without timeout
3. P1 + 65d timeout
4. P2 without timeout
5. P2 + 65d timeout

## Required metrics

For each timeout variant:
- final equity
- delta versus baseline
- delta versus same P1/P2 without timeout
- max drawdown
- max-DD improvement versus baseline
- cash-out count
- natural -25% re-entry count
- timeout re-entry count
- unfinished cash cycles at end
- total daily closes with parked cash
- median / max duration of completed cash cycles
- terminal cash if any

Cross-topology:
- terminal-improve rate vs baseline
- DD-improve rate vs baseline
- BOTH improve rate
- fraction where timeout improves terminal equity relative to the no-timeout version
- fraction where timeout worsens terminal equity relative to the no-timeout version
- median terminal effect of adding timeout
- median change in days-in-cash
- number of topologies where timeout was actually used

## Specific failure-regime diagnostic

Report every cash-out cycle where the natural -25% re-entry would take more than 65 calendar days or would not occur before the dataset ends.

For each such cycle:
- topology
- arm month
- cash-out date
- locked peak
- natural re-entry date if eventually available
- natural wait length
- 65d timeout date
- reference equity at timeout
- subsequent best/worst reference move over the next 31 and 62 days

This identifies what the timeout is actually doing rather than judging it only by terminal equity.

## Anti-overfit guardrail

Do not test 45d / 55d / 75d / 90d in this study.

Do not alter -25% re-entry depth.

Do not change 30% cash fraction.

If 65d looks poor, record that failure and design the next return rule in a separate preregistration.

## Older-history guardrail

Do NOT yet extend to 2020-2022 in this study.

The return cycle is frozen and evaluated first on the current 2023-2026 dataset.

Only after this fallback study is complete should a separate historical-validation study define an older investable universe and test 2020-2022.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
