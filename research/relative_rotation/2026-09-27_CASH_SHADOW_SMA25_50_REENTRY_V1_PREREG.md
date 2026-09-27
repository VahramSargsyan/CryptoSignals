# CASH SHADOW SMA25/50 REENTRY V1 — Preregistration

Date: 2026-09-27
Branch: `research/cash-shadow-sma25-50-reentry-v1`
Mode: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING
Status: PREREGISTERED / FROZEN_BEFORE_EXECUTION / NO PRODUCTION CHANGE

## Purpose

Test whether a causal bullish SMA25/SMA50 crossover can solve the cash re-entry timing problem after a frozen crypto-stress entry.

The user's hypothesis is interpreted as:

- enter CASH_PROXY when the already-frozen crypto crisis entry is confirmed;
- remain in cash while the shadow relative router continues virtually;
- return to crypto only after the relevant shadow target shows a bullish SMA25 crossover above SMA50;
- then execute directly from CASH_PROXY into the currently valid shadow target;
- resume ordinary relative rotations;
- do not permit another cash defense inside the same crisis;
- re-arm future crisis defense only after the old crisis has recovered by the existing breadth>=5 x3 condition.

## Frozen crisis entry

Unchanged:
- universe: ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK;
- own causal SMA200;
- breadth = number above own SMA200;
- crisis entry after 3 consecutive closes with breadth <=3;
- cash entry at next daily open;
- 0.1% transition cost;
- shadow router continues while actual capital is in cash.

## New re-entry condition

For every asset calculate:
- SMA25 of daily close;
- SMA50 of daily close.

Bullish crossover on date D:
- SMA25(D) > SMA50(D); and
- SMA25(D-1) <= SMA50(D-1).

No confirmation days are added.

### Which asset is tested

At close D:
1. evaluate the shadow router and determine the shadow target that would be active at the next open D+1;
2. test the bullish SMA25/SMA50 crossover on that prospective next-open shadow target using closes through D only;
3. if the crossover occurred on D, arm CASH EXIT;
4. at D+1 open:
   - first apply the pending shadow transition;
   - then execute actual CASH_PROXY -> current shadow target directly.

This avoids an intermediate cash -> old shadow -> new shadow trade and keeps the exit causal.

A crossover that occurred before cash entry does not count. A fresh post-entry crossover is required.

If no qualifying crossover occurs, cash remains active.

## One cash reaction per crisis

After SMA re-entry:
- state = POST_CASH_DISARMED;
- ordinary relative-router transitions resume;
- another cash entry is forbidden;
- breadth is watched only for crisis completion.

Re-arm:
- breadth >=5 for 3 consecutive closes.

After re-arm:
- state = ARMED;
- future cash defense requires a fresh breadth<=3 x3 sequence.

The breadth>=5 x3 rule does NOT control cash exit. It only allows the next future crisis episode to arm.

## Cash model

- CASH_PROXY yield = 0%;
- no crypto price exposure while in cash;
- 0.1% actual transition cost on cash entry and re-entry;
- no depeg/counterparty/custody/tax risk modeled.

## Comparators

Report:
- BASELINE relative router;
- frozen LOW_VOL defense;
- frozen-timing CASH destination;
- CASH_SHADOW_SMA25_50_REENTRY_V1.

Do not retune SMA lengths after seeing results.

## Evaluation boundaries

Use the same real Binance 1D panel through 2026-09-26.

Required:
1. reproduction period: 2025-03-29 -> 2026-03-28;
2. already-open period: 2026-03-29 -> 2026-09-26;
3. full eligible history from first existing SMA200/VOL30-ready date;
4. consecutive non-overlapping complete 180-day windows from that anchor;
5. consecutive non-overlapping complete 120-day windows from the same anchor.

State resets at every evaluation-window start.

This is retrospective research, not untouched validation.

## Required metrics

- median return;
- worst starting-asset return;
- positive starts;
- median/worst max drawdown;
- actual transition count;
- cash entries;
- cash days/exposure;
- SMA re-entry exits;
- days from cash entry to SMA re-entry;
- re-arm count;
- POST_CASH_DISARMED exposure;
- unresolved cash episodes at period end.

Robustness:
- windows beating LOW_VOL on return;
- windows beating LOW_VOL on drawdown;
- windows beating LOW_VOL on both;
- separate 180d and 120d breakdown.

## Interpretation discipline

Possible descriptive outcomes:
- SMA_REENTRY_PROMISING
- RETURN_RECOVERY_BUT_RISK_WEAK
- CASH_TOO_STICKY
- LOW_VOL_STILL_DOMINANT
- MIXED
- FAIL

No production promotion from this sample.
No neighboring SMA lengths may be searched after seeing this result and called validation.

## Parallel macro/liquidity work

This test deliberately does NOT use macro or Fed-liquidity signals.
Those remain an independent recovery hypothesis and can be compared later without contaminating this test.

## Runtime impact

Production changed: NONE
Paper-live changed: NONE
Migration required: NO
Automatic execution authorized: NO
Manual confirmation remains required.

TEST_LEVEL: PREREGISTRATION_ONLY
