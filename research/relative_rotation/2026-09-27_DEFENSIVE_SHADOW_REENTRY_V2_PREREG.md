# Defensive Shadow-Target Re-entry V2 — Preregistered Research Plan

Date: 2026-09-27
Workflow mode: PATCH_FIX
Status: RESEARCH_ONLY / POST-OOS REDESIGN / NOT PRODUCTION APPROVED

## Problem

The frozen DEFENSIVE_LOW_VOL_CRYPTO rule passed its 2025-2026 reproduction gate and then produced a mixed untouched result on 2026-03-29 through 2026-09-26:

- drawdown improved materially;
- return opportunity cost was too large;
- the dominant observed problem was prolonged defensive occupancy while the shadow relative router continued to recover and change targets.

The 2026-03-29 through 2026-09-26 period is now opened data and MUST NOT be used as a clean OOS period for any redesigned exit rule.

## Candidate

Reference label:

`DEFENSIVE_LOW_VOL_CRYPTO_SHADOW_SMA200_CONFIRM3_V2`

Entry remains unchanged:

1. Universe = ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK.
2. Base relative router remains 180d median / 15% ARM / 3% reversal / next-open / 0.1%.
3. Breadth = count of 8 assets above own causal SMA200.
4. Enter defensive mode after 3 consecutive closes with breadth <= 3.
5. Defensive asset = lowest trailing 30d realized-volatility token at entry.
6. Hold that defensive token; do not reselect it while the episode is active.
7. Relative router continues in shadow.

Only the exit changes:

8. Track the CURRENT shadow-router target.
9. If the shadow target changes, reset recovery confirmation.
10. Count consecutive daily closes where that same current shadow target closes above its own causal SMA200.
11. After 3 consecutive qualifying closes, exit defense at the next daily open into that current shadow target.
12. Apply 0.1% cost to every actual asset transition.
13. No USDT.

No additional threshold, momentum filter, breadth recovery threshold, volatility threshold, or parameter sweep is allowed in V2.

## Causal rationale

The relative router already answers WHERE capital prefers to return.
The target's own SMA200 is used only as an absolute recovery confirmation for that specific destination.

This avoids requiring broad recovery across 5 of 8 tokens before allowing re-entry.

## Development boundary

This first V2 run is restricted to data through:

`2026-03-28`

The already-opened 2026-03-29 through 2026-09-26 period is EXCLUDED from this run.

All results from this first V2 run are DEVELOPMENT / ROBUSTNESS evidence only.

## Comparison

Compare on identical data and execution semantics:

- BASE 8-node router;
- ORIGINAL DEFENSIVE_LOW_VOL_CRYPTO 3/5 breadth exit;
- V2 shadow-target SMA200 confirm3 exit.

Required windows:

- highlighted 2025-03-29 -> 2026-03-28 historical year;
- five existing non-overlapping 180d windows;
- seven existing non-overlapping 120d windows.

## Predeclared gates

V2 is only worth future forward observation if ALL are true:

1. Protection retention:
   Across the two known weak 180d windows
   - 2024-04-28 -> 2024-10-24
   - 2025-10-20 -> 2026-03-28
   V2 median max drawdown must be no more than 10 percentage points worse than ORIGINAL defense in either window.

2. Opportunity-cost improvement:
   Across the three non-weak 180d windows, V2 return must exceed ORIGINAL defense in at least 2 of 3 windows.

3. Exposure improvement:
   V2 defensive exposure must be lower than ORIGINAL defense in at least 2 of those same 3 non-weak 180d windows.

4. Churn control:
   V2 median defensive transitions must remain <= 6 per 180d window.

5. No catastrophic regression:
   V2 must not turn any ORIGINAL-defense positive 180d window into a return below -10%.

If any gate fails, V2 is not promoted to forward observation.

## Interpretation boundary

Even if all gates pass, V2 status is at most:

`DEVELOPMENT_PASS / FORWARD_CANDIDATE`

It cannot be called OOS-tested because its design followed the opened 2026 failure diagnosis.

Only future paper/forward observations after 2026-09-27 can provide independent evidence.
