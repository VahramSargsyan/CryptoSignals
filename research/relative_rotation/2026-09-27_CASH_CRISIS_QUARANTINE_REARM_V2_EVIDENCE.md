# CASH CRISIS QUARANTINE REARM V2 — Evidence

Date: 2026-09-27  
Branch: `research/cash-crisis-quarantine-rearm-v2`  
Mode: STRESS_TEST_ONLY  
Status: EXECUTED / RESEARCH_ONLY / NO PRODUCTION CHANGE

## Run identity

- GitHub Actions run: `36301213660`
- Source commit: `72c71ee641f258f7d7c97735eec9d043683dd046`
- Artifact: `cash-crisis-quarantine-rearm-v2`
- Artifact ID: `10926076102`
- Workflow conclusion: SUCCESS
- reproduction gate: PASS

TEST_LEVEL: GITHUB_ACTIONS_EXECUTED + UNIT_TESTS + FROZEN_REPRODUCTION_GATE + REAL_BINANCE_1D + FULL_HISTORY + NON_OVERLAPPING_180D_120D_ROBUSTNESS

## State machine actually tested

1. ARMED:
   - frozen crisis entry remains breadth<=3 for 3 closes;
   - cash entry at next open.

2. CASH:
   - fixed preregistered 14/21/30/45/60 days;
   - no crisis/recovery streaks counted;
   - shadow router continues.

3. POST_CASH_DISARMED:
   - re-enter current shadow target after fixed cash duration;
   - normal relative rotations resume;
   - another cash entry is forbidden.

4. REARM:
   - only after breadth>=5 for 3 consecutive closes;
   - this recovery condition merely declares the old crisis episode finished;
   - then a completely fresh breadth<=3 x3 sequence is required for another cash entry.

Thus V2 implements one cash reaction per crisis episode.

## Re-trigger problem fixed

Full-history median cash entries:

V1 immediate re-arm:
- 14d: 33
- 21d: 25
- 30d: 19
- 45d: 14
- 60d: 11

V2 recovery re-arm:
- 14d: 8
- 21d: 6
- 30d: 6
- 45d: 6
- 60d: 5

The semantic correction removes most repeated cash churn inside persistent stress.

## Reproduction period 2025-03-29 -> 2026-03-28

Frozen LOW_VOL:
- +49.05% median return
- -47.13% median max DD

V2:
- 14d: **+54.46% / -59.30%**
- 21d: **+66.22% / -53.81%**
- 30d: **+63.83% / -53.30%**
- 45d: **+4.29% / -53.30%**
- 60d: **+23.48% / -53.30%**

Interpretation:
- 14/21/30 recover more upside than LOW_VOL;
- 21/30 are especially strong on return in this period;
- all tested durations have worse drawdown than LOW_VOL.

## Already-open 2026 period 2026-03-29 -> 2026-09-26

Reference:
- baseline router: +103.36% / -36.68%
- frozen LOW_VOL: +32.92% / -18.34%

V2:
- 14d: **+97.37% / -36.68%**
- 21d: **+91.34% / -36.68%**
- 30d: **+86.76% / -36.68%**
- 45d: **+59.41% / -34.77%**
- 60d: **+77.25% / -26.54%**

All starts were positive for every duration.

Interpretation:
- short quarantine restores most of the active-router recovery upside;
- 14d nearly reproduces baseline return because capital exits cash quickly;
- 60d offers more drawdown protection but still much less than frozen LOW_VOL;
- no duration dominates both return and risk.

## Full eligible history 2023-11-20 -> 2026-09-26

Frozen comparators:
- BASELINE: **+646.96% / -71.23%**
- LOW_VOL: **+887.02% / -47.13%**
- frozen-timing CASH: **+258.06% / -67.56%**

V2:
- 14d: **+567.39% / -69.53%**
- 21d: **+1,142.45% / -65.42%**
- 30d: **+1,684.58% / -65.04%**
- 45d: **+965.68% / -65.04%**
- 60d: **+470.70% / -68.31%**

Cash exposure:
- 14d: ~10.75%
- 21d: ~12.09%
- 30d: ~17.27%
- 45d: ~25.91%
- 60d: ~28.79%

POST_CASH_DISARMED exposure:
- 14d: ~45.01%
- 21d: ~44.34%
- 30d: ~39.44%
- 45d: ~34.74%
- 60d: ~33.97%

Important:
30d is the strongest full-history return row, but it is NOT selected or validated. All five durations were preregistered together and the ranking is materially sample-dependent.

## Long-crisis behavior

Example 30d state path:
- 2025-02-27: cash entry;
- 2025-03-29: return to current shadow crypto target;
- no second cash entry during the same continuing crisis;
- 2025-07-17: recovery re-arm after breadth>=5 x3.

Next long crisis:
- 2025-11-02: cash entry;
- 2025-12-02: return to current shadow crypto target;
- no second cash entry while the same crisis persisted;
- 2026-08-22: detector re-armed after recovery.

This is the intended “sit out the first shock, then resume rotations” behavior.

It also explains the risk:
after the fixed quarantine expires, capital can spend many months inside crypto while the broader crisis has not yet recovered.

## Robustness

13 preregistered non-overlapping windows:
- 5 x 180d
- 8 x 120d

Windows beating frozen LOW_VOL on RETURN:
- 14d: 4/13
- 21d: 7/13
- 30d: 8/13
- 45d: 6/13
- 60d: 4/13

Windows beating frozen LOW_VOL on DRAWDOWN:
- 14d: 2/13
- 21d: 2/13
- 30d: 2/13
- 45d: 2/13
- 60d: 2/13

Windows beating LOW_VOL on BOTH return and drawdown:
- 14d: 2/13
- 21d: 2/13
- 30d: 2/13
- 45d: 2/13
- 60d: 1/13

180d return wins versus LOW_VOL:
- 14d: 2/5
- 21d: 2/5
- 30d: 3/5
- 45d: 2/5
- 60d: 2/5

120d return wins:
- 14d: 2/8
- 21d: 5/8
- 30d: 5/8
- 45d: 4/8
- 60d: 2/8

No duration consistently improves LOW_VOL risk control.

## Verdict

`CASH_CRISIS_QUARANTINE_REARM_V2_VERDICT = PROMISING_RETURN_RECOVERY / RISK_CONTROL_WEAK / DURATION_SENSITIVE / DO_NOT_PROMOTE`

What survived:
- one cash quarantine per crisis is materially cleaner than V1 immediate re-trigger;
- finite cash quarantine can solve much of the LOW_VOL over-defense opportunity cost;
- 21d/30d show repeated return strength across several windows, but this is descriptive only;
- returning to the live shadow router after the first shock is a viable research architecture.

What failed:
- fixed time alone does not tell us when the bear market is actually safe to re-enter;
- drawdown generally remains much worse than frozen LOW_VOL;
- no duration dominates on both return and risk;
- choosing 30d because it has the best full-history return would be post-hoc overfitting.

## Research implication

The cash-entry side is no longer the main problem.

The unresolved question is the CASH EXIT / RE-ENTRY condition.

A future candidate should preserve:
- one cash reaction per crisis;
- shadow router evolution;
- no repeated cash churn inside the same crisis.

But instead of “always return after N days,” it should test a preregistered recovery condition, potentially with:
- a minimum cash quarantine;
- independent cross-asset recovery context;
- official Fed net-liquidity recovery;
- or another causal recovery signal.

That must be a separate preregistered experiment.

## Runtime impact

Production changed: NONE  
Paper-live changed: NONE  
Automatic execution authorized: NO  
Migration required: NO
