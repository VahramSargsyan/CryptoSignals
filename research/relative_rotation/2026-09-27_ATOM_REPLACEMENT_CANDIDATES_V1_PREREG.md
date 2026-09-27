# ATOM Replacement Candidates v1 — preregistration

Date: 2026-09-27
Mode: STRESS_TEST_ONLY

## Goal

After the ATOM Sunset Migration audit, determine whether the active graph should be:

- U9 with ATOM removed and no replacement, or
- U10 with exactly one replacement for ATOM.

No live/paper-live change is authorized by this test. PR #54 remains unmerged while replacement research is in progress.

## Fixed base after removing ATOM

U9_NO_ATOM:

TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

## Fixed replacement candidates

The only remaining assets from the documented 15-token research pool are:

- AVAX
- ETH
- ALGO
- ADA
- XRP

No candidate may be added or removed after outcomes are viewed.

## Fixed variants

- U9_NO_ATOM
- U10_AVAX = U9_NO_ATOM + AVAX
- U10_ETH = U9_NO_ATOM + ETH
- U10_ALGO = U9_NO_ATOM + ALGO
- U10_ADA = U9_NO_ATOM + ADA
- U10_XRP = U9_NO_ATOM + XRP

## Frozen strategy

- Binance Spot 1D fully closed candles
- rolling median 180d
- ARM 15%
- reversal confirmation 3%
- strongest confirmed max-dislocation router
- next-open execution
- transition cost 0.1%

No signal or router parameter tuning.

## Fair starters

Primary cross-variant comparison uses the exact same nine starters present in every variant:

TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

The candidate itself is NOT added as an extra starter in the primary comparison.

A candidate-start result may be reported only as secondary context.

## Fixed windows

- MATURE: 2023-10-31 -> 2026-09-26
- LATEST_2Y: 2024-09-27 -> 2026-09-26
- LATEST_1Y: 2025-09-27 -> 2026-09-26

Also report rolling monthly-start:
- 12-month windows
- 24-month windows

## Candidate topology diagnostics

For each U10 candidate, aggregate across the nine common starters:

- candidate occupancy share
- entries
- exits
- routes ending in candidate
- incoming edge counts
- outgoing edge counts
- median / maximum candidate holding streak

A useful replacement should be an active bridge or destination, not merely an unused label.

## Bull-run confound control

For each candidate:

1. measure strongest 90d and 180d low-to-high drawup inside the strategy-common history beginning 2023-05-05;
2. retain the frozen PRIMARY-explosive rule:
   - 90d drawup >= +200%, or
   - 180d drawup >= +400%;
3. keep the route path unchanged;
4. neutralize positive daily strategy-equity gains only while the strategy actually holds that candidate during its own strongest 90d interval;
5. report raw versus candidate-bull-neutralized mature performance.

This tests P&L dependence on the candidate's own historical price explosion.

## Endpoint sensitivity

To reduce end-of-sample contamination, compare each candidate's incremental value versus U9_NO_ATOM on trailing 365-day windows ending:

- 2026-05-31
- 2026-06-30
- 2026-07-31
- 2026-08-31
- 2026-09-26

For each endpoint report:

candidate_increment =
median_return(U10_candidate) - median_return(U9_NO_ATOM)

A replacement that helps only at one endpoint is not considered stable.

## Decision logic

Do not choose a replacement merely because it has the highest single backtest return.

Evidence for promotion requires a candidate to show most of the following:

- improvement versus U9 on mature history;
- improvement or limited damage on latest 1Y and 2Y;
- rolling 12m / 24m robustness;
- useful topology occupancy and exits;
- no dominant dependence on its own bull-run interval;
- positive or at least stable incremental value across shifted endpoints.

If no candidate meets that bar, keep U9_NO_ATOM rather than force a tenth token.

## Guardrails

- ATOM is not reintroduced.
- No live code is modified by this research branch.
- No automatic trading is added.
- Results are historical/hypothesis-generating, not future-return guarantees.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
