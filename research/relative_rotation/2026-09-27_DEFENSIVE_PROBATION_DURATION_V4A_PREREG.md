# Defensive Probation Duration Robustness V4A — Preregistered Sensitivity Study

Date: 2026-09-27
Workflow mode: STRESS_TEST_ONLY
Status: RESEARCH_ONLY / PARAMETER ROBUSTNESS / NOT PRODUCTION APPROVED

## Question

Was the user-proposed 14-day probation in V4 simply a poor duration, or does the full-capital probation mechanism remain weak across a broad duration range?

This study does NOT optimize for the single highest historical return.

## Frozen mechanism

Everything from V4 remains unchanged except the probation duration.

- defensive entry: SMA200 breadth <= 3 for 3 closes;
- defensive asset: lowest trailing VOL30 token;
- shadow router preserves logical state;
- a NEW shadow transition while defensive starts a real-capital probe;
- router controls actual capital during probation;
- further router transitions do not reset the probation clock;
- broad recovery remains breadth >= 5 for 3 closes;
- if broad recovery confirms during probation, remain in router;
- if probation expires first, actual capital returns to the retained defensive token;
- shadow state is preserved;
- 0.1% per actual transition;
- no USDT;
- no new indicator or filter.

## Predeclared probation grid

Test exactly:

`3, 5, 7, 10, 14, 21, 30, 45, 60` days.

No neighboring values are added after results are visible.

## Development boundary

Parameter assessment uses data only through:

`2026-03-28`

Required development windows:

- 2025-03-29 -> 2026-03-28;
- five non-overlapping 180d windows;
- seven non-overlapping 120d windows.

## Per-duration gates

Each duration is evaluated with the same V4 gates:

1. Protection retention:
   in each of the two known weak 180d windows, max drawdown may be no more than 10 percentage points worse than ORIGINAL defense.

2. Opportunity-cost improvement:
   return must exceed ORIGINAL defense in at least 2 of 3 non-weak 180d windows.

3. Defensive-occupancy improvement:
   physical defensive-token exposure must be lower than ORIGINAL defense in at least 2 of 3 non-weak 180d windows.

4. Churn control:
   median defensive transitions must remain <= 6 per 180d window.

5. No catastrophic regression:
   no ORIGINAL-defense positive 180d window may become a probation return below -10%.

## Robustness decision

A single best duration is NOT enough.

A duration region is called a `ROBUST PLATEAU` only if at least two adjacent predeclared durations both pass all five gates.

If only isolated durations pass, conclusion is:

`NO_ROBUST_DURATION_FOUND`

If no durations pass:

`DURATION_ALONE_DOES_NOT_FIX_V4`

If a robust plateau exists, no single duration is promoted from this study. The plateau is merely a forward-research candidate family.

## 2026 replay

After the development sweep is complete, replay the exact same grid on:

`2026-03-29 -> 2026-09-26`

Label:

`POST_HOC_DIAGNOSTIC_REPLAY_NOT_SELECTION_DATA`

The 2026 replay MUST NOT influence which duration is considered robust.

It is reported only to show sensitivity of:

- return;
- max drawdown;
- physical defensive-token exposure;
- probation exposure;
- actual transition count.

## Multiple-hypothesis record

Post-OOS redesign attempts before this study:

1. V2 shadow-target SMA200 confirm3 — FAILED.
2. V3 router reactivation — FAILED.
3. V4 probation14 memory router — FAILED.

V4A is a robustness study of V4, not a new independent production candidate.
