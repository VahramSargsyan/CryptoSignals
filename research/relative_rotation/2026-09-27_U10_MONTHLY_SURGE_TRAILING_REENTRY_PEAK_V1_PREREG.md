# U10 Monthly Surge Pullback + Trailing Re-entry Peak v1 — preregistration

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Scope: research-only completion of P1/P2 re-entry logic; no live/paper changes

## Objective

Test one parameter-free refinement of the frozen P1/P2 re-entry logic.

Problem observed in prior studies:

1. Fixed 65-day fallback often re-entered too early.
2. Exact locked-peak reclaim also re-entered too early.
3. The original -25% re-entry can remain anchored to an obsolete old peak when U10 resumes making new highs.

Proposed rule:

> While cash is parked, keep updating the reference running peak. Re-entry remains exactly -25%, but the -25% threshold is always measured from the latest running peak, not the peak frozen at cash-out.

No timeout and no reclaim trigger are used.

## Frozen P1 / P2

P1:
- arm after +100% monthly surge versus prior completed selected-month extreme;
- track post-arm running peak;
- cash-out after >=5% daily-close pullback;
- sell 30% next open;
- 0.1% cash-out cost.

P2:
- identical except cash-out requires >=10% pullback.

## Trailing re-entry peak semantics

At cash-out execution:
- initialize re-entry running peak = the locked cash-out peak.

While cash is parked, after each daily close:

1. If current reference equity > re-entry running peak:
   - update re-entry running peak to current reference equity.

2. Compute:
   current reference equity / re-entry running peak - 1.

3. If this drawdown <= -25%:
   - generate re-entry signal;
   - re-enter full parked cash at the next daily open;
   - buy the asset currently held by frozen U10;
   - apply 0.1% modeled re-entry cost.

No time limit.
No peak-reclaim entry.
No moving average.
No additional percentage parameter.

A completed cycle cannot re-arm from the same surge month.

## Comparison

For canonical U10 and all 791 alternative U10 topologies compare:

1. baseline U10;
2. original P1/P2 with frozen -25% re-entry peak;
3. P1/P2 + 65d timeout control;
4. P1/P2 + exact peak-reclaim control;
5. P1/P2 + trailing re-entry peak.

## Required metrics

Per topology / variant:
- final equity;
- delta vs baseline;
- delta vs original no-fallback P1/P2;
- delta vs 65d;
- delta vs peak-reclaim;
- max drawdown;
- natural trailing re-entry count;
- unfinished cycles;
- days in cash;
- terminal cash;
- cash-cycle durations.

Cross-topology:
- final > baseline rate;
- DD improve rate vs baseline;
- BOTH improve rate;
- trailing better than original no-fallback rate;
- trailing better than 65d rate;
- trailing better than peak-reclaim rate;
- median/q25/q75 terminal deltas versus each control;
- unfinished-cycle rate.

## Specific cycle diagnostics

For every canonical cash cycle:
- arm date;
- cash-out date;
- initial locked peak;
- any higher peak formed while in cash;
- final peak used for -25% re-entry;
- re-entry signal/execution;
- cash duration.

For alternative U10s:
- summarize by arm month;
- count cases where trailing peak changed versus original locked peak;
- count cases where original rule never re-entered but trailing did;
- count cases where trailing delayed or accelerated re-entry.

## Anti-overfit guardrail

Do not add any new thresholds.

The only re-entry depth remains -25%.

Do not inspect 2020-2022 data before this study is complete.

If this rule completes the cycle coherently, freeze the rule before historical holdout validation.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
