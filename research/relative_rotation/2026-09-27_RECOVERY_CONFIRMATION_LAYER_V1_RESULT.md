# Recovery Confirmation Layer V1 — Confirm3 Diagnostic Result

Date: 2026-09-27
Workflow mode: DIAGNOSTIC_ONLY
Status: RESEARCH_ONLY / CONFIRM3_REJECTED / NO TRADING

## Objective

Test whether the already-canonical 3-close confirmation rule can suppress false-early Breadth+Gap short-horizon recovery alerts without introducing a new tunable persistence parameter.

No strategy code was changed.
No trading rule was implemented.

## Frozen diagnostic rule

Input signal:

`BREADTH + MEDIAN_SMA200_GAP`

Short horizons only:

- 7 days
- 14 days

Diagnostic probability boundary:

`P(recovery <= horizon) >= 0.50`

Raw signal:

- first close satisfying the probability boundary.

Confirm3 signal:

- third consecutive close satisfying the same boundary.

No search over confirm1 / confirm2 / confirm4 / confirm5 was performed.

Confirmed recovery close itself remained excluded from the diagnostic sample.

## Data

Canonical Binance D1 data reused from:

- Strategy Lab data run: `36299600335`
- artifact: `10924993695`

Universe:

`ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK`

Held-out development episodes:

- STRESS_004 — 30d
- STRESS_005 — 11d
- STRESS_006 — 4d
- STRESS_007 — 141d

The Breadth+Gap probabilities were reused from the prior reproduced diagnostic outputs.

## Development episode-level results

### 7-day recovery horizon

| Episode | Raw first signal | Raw actual remaining | Raw timely? | Confirm3 first signal | Confirm3 actual remaining | Confirm3 timely? |
|---|---|---:|---|---|---:|---|
| STRESS_004 | none | — | — | none | — | — |
| STRESS_005 | none | — | — | none | — | — |
| STRESS_006 | 2024-11-06 | 2d | YES | none before recovery | — | NO SIGNAL |
| STRESS_007 | 2025-03-02 | 137d | NO | 2025-05-20 | 58d | NO |

Episode-level summary:

Raw:
- episodes with signal: 2/4
- timely: 1/2
- false-early: 1/2

Confirm3:
- episodes with signal: 1/4
- timely: 0/1
- false-early: 1/1

Critical result:

Confirm3 removed the good 7-day signal in the 4-day STRESS_006 episode because only two qualifying pre-recovery closes existed.

At the same time, it did NOT remove the long-crisis false alert in STRESS_007.

It delayed that false alert:

`137 days remaining -> 58 days remaining`

but 58 days remaining is still far outside the intended 7-day horizon.

### 14-day recovery horizon

| Episode | Raw first signal | Raw actual remaining | Raw timely? | Confirm3 first signal | Confirm3 actual remaining | Confirm3 timely? |
|---|---|---:|---|---|---:|---|
| STRESS_004 | 2024-08-29 | 30d | NO | 2024-08-31 | 28d | NO |
| STRESS_005 | 2024-10-02 | 11d | YES | 2024-10-04 | 9d | YES |
| STRESS_006 | 2024-11-04 | 4d | YES | 2024-11-06 | 2d | YES |
| STRESS_007 | 2025-02-26 | 141d | NO | 2025-02-28 | 139d | NO |

Episode-level summary:

Raw:
- signal: 4/4
- timely: 2/4
- false-early: 2/4

Confirm3:
- signal: 4/4
- timely: 2/4
- false-early: 2/4

Confirm3 did not improve the episode-level false-alert rate at all.

It merely delayed both good and bad signals by roughly two closes in these episodes.

## Daily alert audit

### 7d

STRESS_007:

Raw:
- true-positive alert days: 7
- false-positive alert days: 17

Confirm3:
- true-positive alert days: 5
- false-positive alert days: 10

Persistence reduces noisy alert-days but does not fix the episode-level false-start problem.

### 14d

STRESS_007:

Raw:
- true-positive alert days: 14
- false-positive alert days: 65

Confirm3:
- true-positive alert days: 14
- false-positive alert days: 50

Again, alert-day noise falls, but the model still enters a persistent false recovery state far too early.

## Post-hoc 2026 replay

Label:

`POST_HOC_2026_DIAGNOSTIC_NOT_VALIDATION`

### 7d

Raw:
- first signal: 2026-08-19
- actual remaining: 3d

Confirm3:
- first signal: 2026-08-21
- actual remaining: 1d

Confirm3 preserves correctness but sacrifices two days of useful lead time.

### 14d

Raw:
- first signal: 2026-08-13
- actual remaining: 9d

Confirm3:
- first signal: 2026-08-19
- actual remaining: 3d

Confirm3 sacrifices six days of lead time.

The 2026 replay therefore does not supply a compensating benefit for the development failure.

## Main conclusion

`CONFIRM3_RECOVERY_LAYER_V1 = REJECTED`

Three-close persistence is not the right discriminator between:

- a temporary/persistent false recovery-looking state inside a long crisis;
- a real short-horizon recovery.

It can reduce the number of false alert-days, but it does not remove the critical false episode-level starts.

Worse, at the 7d horizon it can suppress a valid short-crisis signal because the full three-close persistence window may not exist before confirmed recovery.

## What this rules out

Do NOT proceed directly to:

- confirm2 optimization;
- confirm4 / confirm5 optimization;
- threshold grids around 0.50;
- joint threshold × persistence search.

That would turn the same small four-episode sample into a parameter optimizer.

## Research implication

The next confirmation mechanism must be qualitatively different from plain persistence.

The evidence suggests the discriminator must ask not merely:

> has recovery probability stayed high for N days?

but something like:

> is the recovery evidence improving in a way that is consistent with an actual transition rather than remaining high during a structurally long stress regime?

Potential future diagnostic families, not tested here:

- monotonic / accelerating recovery evidence;
- survival-aware veto or contradiction signal;
- distance-to-recovery geometry rather than probability persistence;
- partial capital probe only after evidence quality improves.

No such mechanism is selected in this result.

## Runtime impact

Production code changed: NONE
Paper-live code changed: NONE
Strategy parameters changed: NONE
Schema changed: NONE
Migration required: NO

## Test level

`TEST_LEVEL: LOCAL_READ_ONLY_DIAGNOSTIC + CANONICAL_BINANCE_DATA_REUSE + BREADTH_GAP_PROBABILITY_REUSE + HELD_OUT_EPISODE_CONFIRMATION_AUDIT + POST_HOC_2026_REPLAY`

## Residual risks

- only four held-out development crises;
- raw 0.50 remains a diagnostic boundary, not a trading threshold;
- daily observations are correlated within episodes;
- recovery definition is breadth-derived;
- 2026 is post-hoc;
- no future clean crisis validates any confirmation mechanism.
