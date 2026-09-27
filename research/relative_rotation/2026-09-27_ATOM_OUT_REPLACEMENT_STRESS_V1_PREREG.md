# ATOM OUT Replacement Stress v1 — Preregistration

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Branch: research/atom-out-replacement-v1
Production changes: NONE

## Research question

Given the ATOM-free base network:

TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

should the tenth slot be filled by AVAX, ETH, ALGO, ADA, XRP, or should the strategy remain U9?

ATOM is included only as a reference arm, not as an eligible replacement winner.

## Frozen mechanics

- Binance Spot 1D closed candles
- common data start: 2023-05-05
- rolling median lookback: 180 days
- ARM threshold: 15%
- reversal confirmation: 3%
- strongest confirmed max-dislocation router
- next-open execution
- modeled transition cost: 0.1%
- no live/paper-live config changes

## Fair-start rule

All primary comparisons use the same nine common start assets:

TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

The added candidate is not allowed to improve its own score merely by adding a tenth start asset.

## Required comparisons

For U9 and every candidate-U10:

1. latest fully closed 1-year window;
2. latest fully closed 2-year window;
3. long mature window;
4. rolling 12-month windows;
5. rolling 24-month windows;
6. max drawdown;
7. candidate occupancy;
8. candidate entries/exits and completed candidate holding-spell outcomes;
9. endpoint sensitivity across 0/30/60/90/120/180-day shifted one-year endpoints;
10. broad-bull neutralization stress;
11. single-neighbor dependency stress.

## Broad-bull neutralization definition

A broad bull day is a day where the cross-sectional median 30-day return of the 15-token research pool is at least +25%.

For the neutralized stress:
- the route and transitions remain unchanged;
- positive route-equity daily returns on broad bull days are replaced with 0%;
- negative returns and modeled costs remain.

This is intentionally harsh. It asks whether a candidate still adds value when broad-bull upside is stripped from the realized route.

## Neighbor-dependency definition

For each base-U9 neighbor, remove that neighbor from both:
- the reduced base universe;
- the reduced base + candidate universe.

Then compare the candidate's incremental effect over the same 1Y and 2Y windows.

Also report transition-counterparty concentration for the candidate.

## Decision discipline

Do not select a winner by terminal return alone.

Primary robustness checks:
- 1Y median return improves vs U9;
- 2Y median return improves vs U9;
- 1Y median max DD is not materially worse (>3 percentage points worse fails);
- rolling 12M improvement rate > 50%;
- rolling 24M improvement rate > 50%;
- endpoint improvement rate >= 50%;
- bull-neutralized long return improves vs U9;
- neighbor-dependency positive rate >= 50%.

The candidate with the strongest robustness profile may be proposed for further forward validation.

If no candidate demonstrates a robust improvement, U9 / NOTHING is the valid result.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
