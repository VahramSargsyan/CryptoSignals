# COMBINED RECOVERY STATE V2 — Preregistration

Date: 2026-09-27
Branch: `research/combined-recovery-state-v2`
Mode: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING
Status: PREREGISTERED / RETROSPECTIVE_COMBINATION_TEST / NO PRODUCTION CHANGE

## Purpose

Test whether V1's overconstraint came from event synchronization rather than from the underlying recovery information.

V1 required:
- a fresh BTC bullish crossover on date D;
- M2 expansion on D;
- breadth>=4 on D.

If BTC crossed earlier while breadth was still <=3, that recovery event was discarded permanently and the strategy waited for a new BTC crossover.

V2 changes ONLY this event semantics.

## Frozen recovery states

While actual capital is in CASH, exit is allowed on the first close D where ALL are true:

1. M2 expansion state:
   - 3m M2 change > 0
   - 6m M2 change > 0
   - 12m M2 change > 0

2. BTC bullish trend state for one already-frozen candidate:
   - fast SMA > SMA100
   - candidates remain exactly:
     - 25/100
     - 30/100
     - 12/100

3. Crypto breadth recovery state:
   - breadth >= 4

Execution:
- next daily open D+1;
- CASH_PROXY -> current shadow-router target directly;
- ordinary relative rotations resume;
- no second cash entry inside the same crisis;
- future crisis defense re-arms only after breadth>=5 for 3 closes.

## Frozen cash entry

Unchanged:
- same 8-asset universe for current-history test;
- breadth<=3 for 3 closes;
- next-open CASH;
- shadow router continues;
- transition cost 0.1%;
- cash yield 0%.

## What is NOT changed

No search or retuning of:
- BTC SMA lengths;
- M2 horizons;
- breadth threshold;
- breadth confirmation days;
- router parameters;
- transaction cost;
- stress-entry rule.

Breadth>=4 is simply outside the frozen <=3 stress zone.

## Current 8-asset evaluation

Universe:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK.

Evaluate:
- reproduction: 2025-03-29 -> 2026-03-28;
- opened 2026: 2026-03-29 -> 2026-09-26;
- full eligible history;
- non-overlapping complete 180d windows;
- non-overlapping complete 120d windows.

Compare:
- frozen LOW_VOL;
- V1 same-day-event combined gate;
- V2 state-based combined gate.

For context also retain standalone BTC crossover result where already available from the same runner.

## Retrospective LEGACY-7 stress test

Universe:
ATOM, TWT, BNB, SOL, TRX, AAVE, LINK.

Period:
- same previously opened old Binance history;
- eligible anchor through 2023-05-04.

This period is NOT untouched validation.
Label:
`RETROSPECTIVE_LEGACY7_ROBUSTNESS`.

Compare:
- LOW_VOL;
- V1 event gate;
- V2 state gate.

Explicitly report the known critical window:
- 2022-02-10 -> 2022-08-08.

## Required diagnostics

For each BTC candidate:
- BTC bullish-state days;
- days blocked by M2;
- days blocked by breadth after M2 pass;
- days where all V2 recovery states are true.

Backtest metrics:
- median return;
- worst return;
- positive starts;
- median/worst max DD;
- cash exposure;
- cash entries;
- re-entry exits;
- median/max cash wait;
- unresolved cash starts.

Robustness:
- wins vs LOW_VOL on return;
- wins vs LOW_VOL on DD;
- wins on both;
- V2 vs V1 return/DD comparison.

## Interpretation

This is a retrospective development test informed by V1.

Possible verdicts:
- EVENT_SEMANTICS_FIX_WORKS
- PARTIAL_IMPROVEMENT
- STILL_OVERCONSTRAINED
- TOO_EARLY_REENTRY
- NO_MATERIAL_IMPROVEMENT
- WORSE

No production promotion from V2.

If V2 is materially better, preserve it only as a research candidate for future untouched/forward evidence.

## Runtime impact

Production changed: NONE
Paper-live changed: NONE
Migration required: NO
Automatic execution authorized: NO
Manual confirmation remains required.

TEST_LEVEL: PREREGISTRATION_ONLY
