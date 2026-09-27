# COMBINED RECOVERY GATE V1 — Evidence

Date: 2026-09-27
Branch: `research/combined-recovery-gate-v1`
Mode: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING
Status: EXECUTED / RETROSPECTIVE / DO_NOT_PROMOTE

## Run identity

- GitHub Actions run: `36307956119`
- source commit: `8053944a83c595b2fecc495a93ec8503b76f4f96`
- run id: `COMBINED_RECOVERY_GATE_V1_8053944a83c5`
- compile: PASS
- unit tests: PASS
- combined backtest: PASS
- artifact finalization: FAILED after successful calculation due GitHub Actions `ECONNRESET`

The artifact transport failure occurred after the runner had written and printed the complete summary.
Research result itself completed successfully.

## Frozen combined rule

CASH entry:
- breadth<=3 for 3 closes;
- next-open CASH;
- shadow router continues.

CASH exit on close D requires simultaneously:
1. official H.6 M2 expansion:
   - 3m > 0
   - 6m > 0
   - 12m > 0
2. fresh bullish BTC SMA crossover using frozen candidate:
   - 25/100, or
   - 30/100, or
   - 12/100
3. crypto breadth >=4.

Execution:
- D+1 open CASH -> current shadow target;
- no intermediate old-shadow trade;
- no second cash entry inside same crisis;
- re-arm only after breadth>=5 x3.

No new BTC lengths or M2 horizons were searched.

## Current 8-asset history

Frozen LOW_VOL reference:
- reproduction: +49.05% / DD -47.13%
- opened 2026: +32.92% / DD -18.34%
- full eligible history: +887.02% / DD -47.13%

### BTC 25/100 + M2 + breadth

Reproduction:
- standalone BTC: +104.66% / -43.20%
- COMBINED: **-30.06% / -31.97%**
- cash exposure: ~90.96%
- median cash wait: 185d
- unresolved cash starts: 8/8

Opened 2026:
- standalone BTC: +77.96% / -36.68%
- COMBINED: **+25.44% / -18.34%**
- LOW_VOL: +32.92% / -18.34%
- cash exposure: ~80.22%
- wait: 146d

Full:
- standalone BTC: +865.37% / -56.93%
- COMBINED: **+163.62% / -60.73%**
- LOW_VOL: +887.02% / -47.13%
- cash exposure: ~77.54%
- median cash wait: 296d
- max wait: 476d

Robustness vs LOW_VOL:
- 180d: both return+DD wins 1/5
- 120d: both wins 2/8
- all: **3/13**

Gate diagnostics:
- BTC crossover days: 7
- blocked by M2: 1
- blocked by breadth after M2 pass: 3
- passed all gates: 3

### BTC 30/100 + M2 + breadth

Reproduction:
- COMBINED: **-27.05% / -31.04%**
- cash exposure: ~91.51%
- wait: 187d

Opened 2026:
- COMBINED: **+25.44% / -18.34%**
- wait: 146d

Full:
- COMBINED: **+136.56% / -64.76%**
- standalone BTC: +593.10% / -55.67%
- LOW_VOL: +887.02% / -47.13%
- cash exposure: ~75.53%
- median wait: 205.5d
- max wait: 334d

Robustness:
- both wins vs LOW_VOL: **2/13**

Gate diagnostics:
- BTC cross days: 7
- M2 blocks: 1
- breadth blocks after M2: 2
- passes: 4

### BTC 12/100 + M2 + breadth

Reproduction:
- standalone BTC: +83.20% / -43.20%
- COMBINED: **+2.18% / -43.20%**
- cash exposure: ~85.75%
- wait: 166d

Opened 2026:
- standalone BTC: +91.34% / -36.68%
- COMBINED: **+20.99% / -18.34%**
- LOW_VOL: +32.92% / -18.34%

Full:
- standalone BTC: +846.30% / -53.69%
- COMBINED: **+245.01% / -48.61%**
- LOW_VOL: +887.02% / -47.13%
- cash exposure: ~76.10%
- median wait: 294d
- max wait: 403d

Robustness:
- both wins vs LOW_VOL: **2/13**

Gate diagnostics:
- BTC cross days: 8
- M2 blocks: 1
- breadth blocks after M2: 3
- passes: 4

## Current-8 conclusion

The combined same-day gate is too restrictive.

It frequently improves near-term DD by remaining in cash, but the opportunity cost is extreme:
- waits commonly 146-296d;
- full-history returns collapse relative to LOW_VOL;
- no candidate robustly dominates LOW_VOL.

Best full-history combined risk profile among the three is 12/100:
- +245.01% / -48.61%
versus LOW_VOL:
- +887.02% / -47.13%.

Thus the protection is approximately comparable while most upside is lost.

## Retrospective LEGACY-7 stress check

This old period is already known from the previous BTC holdout and is NOT untouched validation.

Period:
- eligible anchor 2021-08-14
- end 2023-05-04.

LOW_VOL:
- median return: **-40.08%**
- DD: **-76.24%**

Standalone BTC previously failed:
- 25/100: -70.74% / -93.76%
- 30/100: -72.88% / -94.21%
- 12/100: -64.69% / -92.46%.

COMBINED, all three variants:
- median return: **+30.17%**
- DD: **-37.06%**
- positive starts: 6/7.

However this is NOT a successful re-entry result:
- defensive/cash exposure: ~92.53%
- median re-entry exits: **0**
- unresolved cash at end: **7/7**.

Therefore the apparent old-period win is mostly cash quarantine / endpoint effect.

## Critical 2022 failure window

2022-02-10 -> 2022-08-08.

LOW_VOL:
- +2.34% / DD -36.79%.

Standalone BTC:
- 25/100: -58.08% / -79.09%
- 30/100: -60.87% / -79.86%
- 12/100: -49.40% / -79.09%.

COMBINED, all three:
- **-7.36% / DD -7.36%**
- cash exposure: ~98.33%
- re-entry exits: 0
- unresolved cash: 7/7.

This confirms the combined gate successfully blocks the catastrophic false BTC recovery that hurt the standalone candidates.

But it does so by never re-entering during the window.

## Mechanism

The main problem is NOT M2.

Current-8 gate diagnostics show M2 blocks only one BTC crossover for each candidate.

The stronger filter is breadth>=4:
- several BTC bullish crossovers occur while breadth remains in the <=3 stress zone;
- those crossovers are discarded permanently;
- once breadth later improves, the rule requires a NEW BTC crossover;
- the next crossover can be months later.

This creates 146-476 day waits.

The design therefore has an event-synchronization problem:
`BTC CROSSOVER AND BREADTH>=4 ON THE SAME DATE`
is much stricter than the intended conceptual condition:
`BTC TREND RECOVERED AND BREADTH HAS STARTED RECOVERING`.

## Verdict

`COMBINED_RECOVERY_GATE_V1 = FALSE_RECOVERY_FILTERED / OVERCONSTRAINED / CASH_QUARANTINE / NO_MATERIAL_IMPROVEMENT / DO_NOT_PROMOTE`

Supported:
- systemic/breadth confirmation can prevent the severe 2022 false BTC re-entry;
- M2 can remain a slow regime context layer;
- combining independent evidence changes the failure mode in the intended direction.

Not supported:
- requiring a fresh BTC crossover to occur on the same day that M2 is expanding and breadth>=4;
- production promotion;
- interpreting old LEGACY-7 endpoint performance as successful recovery timing.

## Next technical hypothesis

Do NOT tune more SMA lengths.

A cleaner separately preregistered V2 would change event semantics, not thresholds:

- M2 remains a slow regime state: all 3 horizons positive;
- BTC recovery is a STATE: fast SMA > slow SMA, not necessarily a fresh crossover today;
- breadth>=4 is a STATE;
- when all three states become true, next-open re-entry.

This would allow BTC to recover first and breadth to recover later without waiting for another full BTC crossover.

That V2 is not tested here.

## Runtime impact

Production changed: NONE
Paper-live changed: NONE
Migration required: NO
Automatic execution authorized: NO
Manual confirmation remains required.

TEST_LEVEL: GITHUB_ACTIONS_EXECUTED + UNIT_TESTS + REAL_BINANCE_1D + OFFICIAL_H6_M2_SNAPSHOT + CURRENT8_FULL_HISTORY + RETROSPECTIVE_LEGACY7 + 180D_120D_ROBUSTNESS
ARTIFACT_UPLOAD: FAILED_AFTER_SUCCESSFUL_BACKTEST / GITHUB_ACTIONS_ECONNRESET
