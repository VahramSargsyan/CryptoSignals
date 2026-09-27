# COMBINED RECOVERY GATE V1 — Preregistration

Date: 2026-09-27
Branch: `research/combined-recovery-gate-v1`
Mode: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING
Status: PREREGISTERED / RETROSPECTIVE_COMBINATION_TEST / NO PRODUCTION CHANGE

## Question

Can three already-studied layers work better together for CASH re-entry than any one of them alone?

Required simultaneously on close D:

1. M2 expansion:
   - 3m M2 change > 0
   - 6m M2 change > 0
   - 12m M2 change > 0
2. BTC bullish crossover using one of the already frozen development candidates:
   - 25 / 100
   - 30 / 100
   - 12 / 100
3. crypto breadth is no longer inside the frozen stress-entry zone:
   - breadth >= 4
   - this is simply > ENTER_BREADTH_MAX=3; no new threshold search.

If all three are true on D:
- at D+1 open execute actual CASH_PROXY -> current shadow-router target directly;
- ordinary relative rotations resume;
- another cash entry is forbidden inside the same crisis;
- re-arm future crisis defense only after breadth>=5 for 3 closes.

## Frozen cash entry

Unchanged:
- enter after 3 consecutive closes breadth<=3;
- next-open execution;
- shadow router continues;
- 0.1% transition cost;
- CASH_PROXY yield 0%.

## BTC candidates

Do NOT search new SMA lengths.
Test exactly, in prior development order:
1. 25/100
2. 30/100
3. 12/100

No post-hoc winner may be called validated.

## M2

Official Federal Reserve H.6 M2.M, seasonally adjusted.

Use the verified official source-probe snapshot.
For legacy robustness extend the same official source-probe snapshot back to 2020 for causal 12m warmup.

M2 condition is fixed:
- 3m >0 AND 6m >0 AND 12m >0.

Do NOT search alternative M2 horizons or OR-combinations.

Availability:
- use actual/frozen monthly H.6 release schedule;
- a new value is usable only from the next UTC calendar day after release.

## Current 8-asset test

Universe unchanged:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK.

Evaluate:
- reproduction 2025-03-29 -> 2026-03-28;
- opened 2026-03-29 -> 2026-09-26;
- full eligible history;
- complete 180d and 120d windows.

Compare:
- frozen LOW_VOL;
- standalone frozen BTC candidate;
- COMBINED candidate.

## Legacy-7 retrospective robustness

Universe:
ATOM, TWT, BNB, SOL, TRX, AAVE, LINK.

Use the same old Binance period previously opened by the BTC holdout study.
This period is no longer untouched for the new combined rule.

Therefore label it:
`RETROSPECTIVE_LEGACY7_ROBUSTNESS`

Do NOT call it validation.

Keep:
- breadth entry <=3 x3;
- re-arm >=5 x3;
- router parameters unchanged.

Primary old-period comparison:
- LOW_VOL;
- standalone BTC;
- COMBINED.

Also explicitly report the previously critical 2022 window:
2022-02-10 -> 2022-08-08.

## Required outputs

For each BTC pair and each evaluation:
- median return;
- median/worst max DD;
- worst return;
- positive starts;
- cash exposure;
- cash entries;
- re-entry exits;
- median/max cash wait;
- unresolved cash episodes.

For COMBINED:
- count of BTC crossover days blocked by M2;
- count blocked by breadth;
- count passing all gates.

Robustness:
- wins vs LOW_VOL on return;
- wins vs LOW_VOL on DD;
- wins on both.

## Interpretation

This is a retrospective combination test informed by prior component results.

Possible verdicts:
- COMBINATION_IMPROVES_FAILURE_MODE
- PARTIAL_IMPROVEMENT
- NO_MATERIAL_IMPROVEMENT
- WORSE
- INCONCLUSIVE

No production promotion from this test.

If one pair looks best, preserve it only as a development candidate.
Do not retune the old LEGACY-7 period.

## Runtime impact

Production changed: NONE
Paper-live changed: NONE
Migration required: NO
Automatic execution authorized: NO
Manual confirmation remains required.

TEST_LEVEL: PREREGISTRATION_ONLY
