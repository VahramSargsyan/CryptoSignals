# ALGO Feeder Interaction v1 — preregistration

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Status: POST-AUDIT HYPOTHESIS GENERATION

## Derivation

The completed ALGO Deep Audit v1 found that ALGO is broadly useful in unconstrained latest-year contexts, but adding ALGO alone to U9_CLEANER does not change post-bull / 1Y / 2Y results for the mandatory ATOM/TWT/PEPE starts.

Top-10 route aggregation showed ALGO incoming transitions primarily from:
- AVAX
- ETH
- SOL
- XRP

and ALGO outgoing transitions primarily to:
- FIL
- TRX

U9_CLEANER already contains FIL and TRX, but it contains none of AVAX/ETH/SOL/XRP.

This test asks whether ALGO requires one of those feeder nodes to become active/useful.

## Frozen baseline

U9_CLEANER:
ATOM, TWT, PEPE, BNB, TRX, AAVE, LINK, FIL, HBAR

## Fixed feeder set

AVAX, ETH, SOL, XRP

No other feeders may be added after outcomes are visible.

## Fixed variants

For each feeder F:
- BASE + F
- BASE + F + ALGO

Controls:
- BASE
- BASE + ALGO

## Frozen strategy

Same canonical 180d / 15% ARM / 3% reversal / strongest max-dislocation / next-open / 0.1% transition-cost engine.

## Fair starters

Primary comparison uses only:
ATOM, TWT, PEPE

These are present in every variant.

## Windows

- Post-ALGO-bull: 2024-12-08 -> 2026-09-26
- Latest 2Y: 2024-09-27 -> 2026-09-26
- Latest 1Y: 2025-09-27 -> 2026-09-26
- Mature: 2023-10-31 -> 2026-09-26

## Interaction metric

For each feeder F:

ALGO_INCREMENT_WITH_F =
return(BASE + F + ALGO) - return(BASE + F)

Compare that to:

ALGO_INCREMENT_ALONE =
return(BASE + ALGO) - return(BASE)

Report both for each window.

Also report:
- ALGO occupancy share on ATOM/TWT/PEPE routes;
- ALGO entries/exits;
- incoming/outgoing edges;
- routes ending in ALGO.

## Guardrail

This is explicitly derived after observing ALGO topology and is not independent validation.
No live promotion is authorized.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
