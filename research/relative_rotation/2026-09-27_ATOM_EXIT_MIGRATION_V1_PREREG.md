# ATOM Exit Migration v1 — preregistration

Date: 2026-09-27
Mode: STRESS_TEST_ONLY

## Goal

Design a low-disruption path for removing ATOM from the research universe without forcing an immediate portfolio swap or breaking the existing relative-rotation graph.

No live universe change is authorized by this test.

## Current research references

BASE_U10:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

U9_CLEANER:
ATOM, TWT, PEPE, BNB, TRX, AAVE, LINK, FIL, HBAR

The user currently holds ATOM and also has other Cosmos-ecosystem assets that may later be consolidated into ATOM operationally. Those other Cosmos assets are not modeled here because their exact symbols/amounts are outside this research harness.

## Core migration idea

Do not force-sell ATOM merely because the target universe no longer contains it.

Test an EXIT_ONLY_ATOM migration mode:

- If current state is ATOM, ATOM remains temporarily eligible only as the current held asset.
- No future transition INTO ATOM is allowed.
- The first valid outbound ATOM signal is allowed to move capital to a target member of the no-ATOM target universe.
- After that first outbound transition, ATOM is permanently removed for the remainder of the route.
- For routes that do not start in ATOM, ATOM is absent from day one.

This models a gradual one-way retirement of ATOM.

## Frozen strategy

- Binance Spot 1D closed candles
- rolling median 180d
- ARM 15%
- reversal confirmation 3%
- strongest confirmed max-dislocation router
- next-open execution
- transition cost 0.1%

## Universes

### BASE_U10
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

### DROP_ATOM_U9
TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

### BASE_U9_CLEANER
ATOM, TWT, PEPE, BNB, TRX, AAVE, LINK, FIL, HBAR

### DROP_ATOM_U8_CLEANER
TWT, PEPE, BNB, TRX, AAVE, LINK, FIL, HBAR

## Mechanical one-token replacements

For BASE_U10, replace ATOM one-for-one with every candidate from the fixed 15-token pool not already in BASE_U10:
- AVAX
- ETH
- ALGO
- ADA
- XRP

For U9_CLEANER, replace ATOM one-for-one with every candidate not already in U9_CLEANER:
- SOL
- AVAX
- ETH
- ALGO
- ADA
- XRP

No candidate is chosen by future return.

## Evaluation windows

- Mature: 2023-10-31 -> 2026-09-26
- Post-PEPE-bull: 2024-06-11 -> 2026-09-26
- Latest 2Y: 2024-09-27 -> 2026-09-26
- Latest 1Y: 2025-09-27 -> 2026-09-26

## Fair start sets

For ordinary no-ATOM/replacement comparisons:
- TWT and PEPE are the common operational starters.
- All-member medians are reported only as secondary context.

For migration-mode evaluation:
- ATOM-start is the primary metric.
- Measure time-to-first-exit from ATOM.
- Record first destination.
- Record whether ATOM ever becomes stuck with no outbound transition through the window.
- After exit, no re-entry to ATOM is allowed.

## Robustness metrics

For each target universe / migration mode:
- terminal return;
- max drawdown;
- transitions;
- worst rolling 12m where available;
- latest 1Y / 2Y;
- ATOM-start migration result;
- days spent in ATOM before first exit;
- first exit destination;
- re-entry count (must be zero in EXIT_ONLY_ATOM by construction).

## Decision criteria

A no-ATOM research baseline is considered migration-ready only if:
1. dropping ATOM does not materially degrade latest 1Y/2Y and mature performance;
2. EXIT_ONLY_ATOM produces a viable outbound route without requiring an immediate forced sale;
3. no replacement token is promoted solely on one known endpoint;
4. replacement should beat or clearly improve on simple DROP_ATOM, otherwise prefer the smaller graph;
5. the target graph remains strong without ATOM on non-ATOM starts.

## Operational note

Consolidating other Cosmos-ecosystem holdings into ATOM is treated as a separate execution decision. This audit only establishes whether ATOM can serve as a temporary one-way staging asset before being retired from the graph.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
