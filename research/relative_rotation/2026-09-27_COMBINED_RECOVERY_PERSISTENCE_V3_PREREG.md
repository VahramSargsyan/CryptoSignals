# COMBINED RECOVERY PERSISTENCE V3 — Preregistration

Date: 2026-09-27
Branch: `research/combined-recovery-persistence-v3`
Mode: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING
Status: PREREGISTERED / RETROSPECTIVE / NO PRODUCTION CHANGE

## Purpose

V2 fixed V1's event-synchronization problem by turning BTC recovery from a one-day crossover event into a persistent bullish state. This normalized median waits from roughly 205-296 days to about 34.5-41 days, but full-history max drawdown returned to about -64%.

V3 tests one and only one semantic change:

- do not exit CASH on the first day all V2 recovery states become true;
- require all V2 recovery states to remain true for the already-existing frozen `CONFIRM_DAYS=3` consecutive closes;
- then exit at the next daily open.

No new numeric parameter is introduced.

## Frozen recovery states

All must be true simultaneously:

1. M2 expansion:
   - 3m > 0
   - 6m > 0
   - 12m > 0

2. BTC bullish state:
   - fast SMA > SMA100

Frozen BTC candidates:
- 25/100
- 30/100
- 12/100

3. Crypto breadth recovery:
- breadth >= 4

Persistence:
- all three conditions true for 3 consecutive closes.

If any one condition becomes false before the 3-close confirmation completes, the recovery streak resets to zero.

Execution:
- after the third qualifying close, exit CASH at next daily open;
- execute directly CASH -> current shadow-router target;
- resume ordinary relative rotations;
- no second CASH entry inside the same crisis;
- re-arm future crisis defense only after breadth>=5 x3.

## Frozen cash entry

Unchanged:
- current 8-asset universe;
- stress after breadth<=3 for 3 consecutive closes;
- next-open CASH;
- shadow router continues;
- 0.1% transition cost;
- cash yield 0%.

## What is not changed

Do not change or search:
- BTC SMA lengths;
- M2 horizons;
- breadth>=4 recovery threshold;
- stress-entry threshold;
- router parameters;
- transaction cost;
- re-arm threshold;
- confirmation length.

The 3-close recovery persistence reuses the existing system-wide confirmation count.

## Current 8-asset evaluation

Compare:
- frozen LOW_VOL;
- V2 state gate;
- V3 3-close persistent state gate.

Required periods:
- reproduction 2025-03-29 -> 2026-03-28;
- opened 2026 2026-03-29 -> 2026-09-26;
- full eligible history;
- complete non-overlapping 180d windows;
- complete non-overlapping 120d windows.

## Retrospective LEGACY-7 check

Use the already-known old period and label it:
`RETROSPECTIVE_LEGACY7_ROBUSTNESS_NOT_VALIDATION`.

Compare LOW_VOL, V2, V3.

Explicitly report:
- 2022-02-10 -> 2022-08-08.

The legacy period must not be treated as untouched validation.

## Required diagnostics

For each BTC candidate:
- median return;
- worst return;
- positive starts;
- median/worst max DD;
- cash exposure;
- cash entries;
- re-entry exits;
- median/max cash wait;
- unresolved cash starts;
- recovery-state streak completions;
- streak resets before confirmation.

Robustness:
- V3 wins vs LOW_VOL on return;
- V3 wins vs LOW_VOL on DD;
- V3 wins on both;
- V3 vs V2 return/DD comparison.

## Interpretation

Possible descriptive outcomes:
- PERSISTENCE_FILTER_IMPROVES_RISK
- WAIT_SLIGHTLY_HIGHER_BUT_ACCEPTABLE
- STILL_TOO_EARLY
- STILL_TOO_LATE
- NO_MATERIAL_IMPROVEMENT
- WORSE

No production promotion from this retrospective test.

## Runtime impact

Production changed: NONE
Paper-live changed: NONE
Migration required: NO
Automatic execution authorized: NO
Manual confirmation remains required.

TEST_LEVEL: PREREGISTRATION_ONLY
