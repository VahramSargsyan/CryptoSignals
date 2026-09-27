# ATOM Node Replacement v1 — preregistration

Date: 2026-09-27
Mode: STRESS_TEST_ONLY

## Goal

Find whether ATOM can be replaced as a graph node in the promoted live U10 without simultaneously changing any other member.

Current live/research U10:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

Fixed core after removing ATOM:
TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

## Fixed replacement candidates

Only assets already present in the established 15-token research pool and not already in U10:

- AVAX
- ETH
- ALGO
- ADA
- XRP

No candidate may be added after viewing outcomes.

Controls:
- BASE_U10
- NO_ATOM_U9
- U10_ATOM_TO_AVAX
- U10_ATOM_TO_ETH
- U10_ATOM_TO_ALGO
- U10_ATOM_TO_ADA
- U10_ATOM_TO_XRP

## Frozen strategy

- Binance Spot 1D fully closed candles
- rolling median 180d
- ARM 15%
- reversal confirmation 3%
- strongest confirmed max-dislocation router
- next-open execution
- transition cost 0.1%

No parameter tuning.

## Fair starters

Primary cross-universe comparison uses the exact nine starters present in every variant:

TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

This prevents a candidate from winning because its own starting asset happened to perform well.

For each replacement universe also report:
- the replacement token as a start, descriptive only;
- legacy ATOM EXIT_ONLY start: capital may begin in ATOM, outbound ATOM signals are allowed into the replacement universe, but ATOM can never be re-entered.

## Fixed windows

- Mature: 2023-10-31 -> 2026-09-26
- Post-PEPE-bull: 2024-05-24 -> 2026-09-26
- Latest 2Y: 2024-09-27 -> 2026-09-26
- Latest 1Y: 2025-09-27 -> 2026-09-26

## Rolling robustness

Monthly-start rolling windows:
- 12 months
- 18 months
- 24 months where available

Report:
- median window return
- worst window return
- positive-window rate
- ATOM EXIT_ONLY worst window

## Replacement diagnostics

For each X:
1. compare BASE_U10 vs NO_ATOM_U9 vs U10_ATOM_TO_X;
2. report delta versus BASE_U10 and versus NO_ATOM_U9 for every fixed window;
3. report median/max drawdown;
4. report transitions/conflicts;
5. report occupancy share of X on TWT/PEPE/BNB/SOL/TRX/AAVE/LINK/FIL/HBAR starts;
6. report incoming/outgoing X edges;
7. report legacy ATOM EXIT_ONLY result and first destination.

## Pre-registered interpretation

A token is a stronger replacement candidate if it:
- improves or preserves the long-horizon Mature and Latest-2Y results versus BASE_U10;
- remains positive on Latest-1Y;
- does not materially worsen the worst rolling 12m result versus NO_ATOM_U9;
- is actually used by the router (non-zero occupancy/edges), rather than being an inert tenth token.

No single historical winner is automatically production-approved.

## Guardrails

- No live membership change in this stress test.
- ATOM persistent Telegram watch remains separate from graph membership.
- No automatic execution.
- This is a one-node-at-a-time replacement test only.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
