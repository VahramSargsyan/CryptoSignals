# ATOM Sunset Migration v1 — preregistration

Date: 2026-09-27
Mode: STRESS_TEST_ONLY

## Goal

Design a low-disruption path for removing ATOM from the current research U10 without forcing an immediate all-in swap.

Current U10 under test:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

This v1 changes only ATOM membership. No other node replacement is allowed.

## Frozen strategy

- Binance Spot 1D closed candles
- rolling median 180d
- ARM 15%
- reversal confirmation 3%
- strongest confirmed max-dislocation router
- next-open execution
- transition cost 0.1%

## Fixed variants

1. BASE_U10
   Normal current U10, including entries into and exits from ATOM.

2. NO_ATOM_U9
   ATOM removed entirely:
   TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

3. ATOM_EXIT_ONLY
   ATOM may be the current/legacy held asset and outgoing ATOM signals remain active.
   All incoming transitions to ATOM are forbidden.
   Once a route leaves ATOM, it can never re-enter ATOM.

4. ATOM_4X25_SIGNAL_MIGRATION
   Model four equal 25% tranches initially held in ATOM.
   Tranche 1 exits on the first distinct eligible confirmed outbound ATOM signal after migration start.
   Tranche 2 exits on the second distinct eligible outbound ATOM signal.
   Tranche 3 exits on the third.
   Tranche 4 exits on the fourth.
   After each tranche exits, it follows the NO_ATOM_U9 router and can never re-enter ATOM.

If fewer than four outbound ATOM signals occur before the evaluation end, report incomplete migration.

## Historical migration start dates

To avoid one lucky start date, run migration starts monthly from:
2024-01-01 through 2026-03-01

For each start:
- measure days to 25%, 50%, 75%, 100% migrated;
- report destination assets for each tranche;
- report portfolio terminal return through 2026-09-26;
- compare with:
  a) staying in BASE_U10 from ATOM;
  b) ATOM_EXIT_ONLY full-capital exit on first eligible signal;
  c) immediate next-open switch from ATOM into the best available NO_ATOM_U9 target only if a valid signal exists; otherwise hold until the first valid signal.

## Fixed summary windows

Also compare BASE_U10, NO_ATOM_U9 and ATOM_EXIT_ONLY on:
- mature: 2023-10-31 -> 2026-09-26
- latest 2Y: 2024-09-27 -> 2026-09-26
- latest 1Y: 2025-09-27 -> 2026-09-26

For cross-universe medians, use common non-ATOM starters:
TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR.

ATOM-start is reported separately for BASE_U10 and ATOM_EXIT_ONLY.

## Operational interpretation

If EXIT_ONLY / staged migration performs close to or better than BASE_U10 while eliminating re-entry to ATOM, this supports a gradual sunset rather than a one-time forced swap.

Other Cosmos-ecosystem assets are outside this market backtest. They may be operationally consolidated into ATOM first only if the user chooses and if route/liquidity/unbonding/tax considerations are acceptable. Such consolidation should treat ATOM as a temporary transit asset, not a strategic destination.

## Guardrails

- No live code or paper-live membership changes.
- No forced target token is selected from future returns.
- No ATOM sell instruction is authorized by this test.
- No other U10 nodes are changed in v1.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
