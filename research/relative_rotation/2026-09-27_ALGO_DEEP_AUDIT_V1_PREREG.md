# ALGO Deep Audit v1 — preregistration

Date: 2026-09-27
Mode: STRESS_TEST_ONLY

## Motivation

In the unconstrained latest-year exhaustive search:
- ALGO appeared in 10/10 top U8;
- ALGO appeared in 10/10 top U10;
- ALGO appeared in 50/50 top U8 and 50/50 top U10.

A prior bull-run audit classified ALGO as PRIMARY explosive:
- strongest observed 90d low-to-high drawup: about +365%;
- interval: 2024-11-04 -> 2024-12-07.

The latest-year ranking window begins 2025-09-27, well after that interval.

This audit asks whether ALGO is:
1. merely a historical bull-run contaminant,
2. a useful post-bull routing node,
3. or a hindsight artifact specific to certain companion assets.

No live universe changes are allowed.

## Candidate pool

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, AVAX, FIL, ETH, ALGO, ADA, XRP, HBAR

## Frozen strategy

- Binance Spot 1D closed candles
- rolling median 180d
- ARM 15%
- reversal confirmation 3%
- strongest confirmed max-dislocation router
- next-open execution
- transition cost 0.1%

No strategy parameter tuning.

## Fixed windows

- Mature: 2023-10-31 -> 2026-09-26
- Post-ALGO-bull: 2024-12-08 -> 2026-09-26
- Latest 2Y: 2024-09-27 -> 2026-09-26
- Latest 1Y: 2025-09-27 -> 2026-09-26

The post-bull window begins immediately after ALGO's already documented strongest 90d bull interval.

## Test A — ALGO price-path attribution

Report:
- ALGO HODL return over mature, post-bull, latest-2Y and latest-1Y windows;
- strongest 90d and 180d drawups;
- total common-history return.

This separates token appreciation from graph value.

## Test B — ALGO own-bull neutralization

For ALGO-containing universes, keep the signal/router path unchanged but neutralize positive daily strategy equity gains only while:
- held asset = ALGO
- date lies inside 2024-11-04 -> 2024-12-07.

Report raw vs ALGO-bull-neutralized terminal returns.

This measures direct dependence on ALGO's explosive interval.

## Test C — matched marginal value across unconstrained latest-year universes

Using the already exhaustive latest-year U8/U10 space:

For every ALGO-containing U8:
- remove ALGO to get its 7-asset base;
- compare that ALGO extension against every alternative 8th token available from the same 15-token pool.

For every ALGO-containing U10:
- analogous 9-asset base and alternative 10th tokens.

Report:
- ALGO win rate versus alternative tokens;
- median return advantage versus the median alternative extension;
- distribution of ALGO marginal deltas;
- number/fraction of contexts where ALGO is best, top-3, median-or-better, or worst.

This is descriptive latest-year attribution, not a selector.

## Test D — top-set topology

For the already identified latest-year:
- top-10 U8
- top-10 U10

Report ALGO:
- occupancy share across all starts;
- total entries/exits;
- number of routes ending in ALGO;
- incoming/outgoing transition edges;
- average/median holding-streak length;
- maximum holding-streak length.

Goal: distinguish bridge behavior from stale-hold behavior.

## Test E — current practical U9_CLEANER integration

Current cleaner research baseline:
ATOM, TWT, PEPE, BNB, TRX, AAVE, LINK, FIL, HBAR

Test fixed variants:
- U9_CLEANER
- U10_CLEANER_PLUS_ALGO
- replace BNB with ALGO
- replace TRX with ALGO
- replace AAVE with ALGO
- replace LINK with ALGO
- replace FIL with ALGO
- replace HBAR with ALGO

ATOM/TWT/PEPE remain fixed because they are user-held operational constraints.

Evaluate all variants on:
- mature
- post-ALGO-bull
- latest 2Y
- latest 1Y

Cross-variant medians use the seven starters common to all variants:
ATOM, TWT, PEPE, TRX/AAVE/LINK/etc cannot all be common after replacements, so for fairness use only mandatory common starters ATOM, TWT, PEPE as the primary comparable start set.
Also report ATOM-start separately.

A secondary all-member median is allowed only as descriptive context.

## Test F — post-bull rolling robustness

For U9_CLEANER and U10_CLEANER_PLUS_ALGO:
- rolling 12m windows starting after 2024-12-08;
- available rolling 18m windows if any.

Report median, worst window and positive-window rate.

## Interpretation guardrails

- ALGO's top-frequency result was discovered on already-opened historical data.
- A strong result does not authorize live promotion.
- Do not tune the bull interval, strategy parameters or replacement set after outcomes are visible.
- If ALGO helps only in combinations containing other historically explosive assets, report that dependency rather than calling ALGO universally useful.
- If ALGO's own bull interval contributes little or zero P&L but post-bull routing remains strong, classify its value as structural/topological rather than bull-run harvesting.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
