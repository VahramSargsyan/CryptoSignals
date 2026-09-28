# ROBUST EXHAUSTIVE U9/U10 ATOM REPLACEMENT V1 — PREREG

Date: 2026-09-28  
Workflow mode: STRESS_TEST_ONLY  
Production/live changes: NONE

## Research question

After removing the requirement to keep ATOM, determine whether the relative-rotation network is more robust as U9 or U10, which token compositions remain strong across multiple historical windows, and whether ATOM / ALGO / AVAX / FIL / PEPE / BNB / TRX / HBAR / ETH / XRP add positive marginal network value.

This experiment must not promote a production universe. It produces evidence only.

## Fixed token pool

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, AVAX, FIL, ETH, ALGO, ADA, XRP, HBAR

No mandatory tokens.

## Exhaustive spaces

- U9: C(15,9) = 5,005 universes
- U10: C(15,10) = 3,003 universes
- Total: 8,008 universes

Each universe is evaluated from every possible starting asset inside that universe. Aggregation uses the median across starting assets.

## Fixed engine semantics

- Data: Binance spot daily OHLCV
- Pair monitor: existing repository relative-rotation engine
- Lookback: 180 days
- Arm threshold: 15%
- Reversal: 3%
- Transition cost: 0.1%
- Signal decision: confirmed signal at day T
- Execution: next available day open
- If multiple eligible destinations exist: highest max_dislocation, then deterministic token/pair tie-break
- No production behavior is changed

## Primary exhaustive windows

1. LAST_1Y: 2025-09-27 through 2026-09-26
2. LAST_2Y: 2024-09-27 through 2026-09-26
3. MATURE: 2023-10-31 through 2026-09-26

For every universe/window record:

- median return
- worst starting-token return
- best starting-token return
- positive-start count
- median max drawdown
- worst max drawdown
- median transitions
- median conflicts

## Robust ranking

Ranking is not based on one maximum-return year.

For each size independently:

1. Convert median return in each of the three primary windows to cross-sectional percentile.
2. PRIMARY_ROBUST_SCORE = minimum of the three return percentiles.
3. SECONDARY_ROBUST_SCORE = median of the three return percentiles.
4. WORST_START_SCORE = minimum percentile of worst-start return across the three windows.
5. Sort by:
   - PRIMARY_ROBUST_SCORE descending
   - SECONDARY_ROBUST_SCORE descending
   - WORST_START_SCORE descending
   - median drawdown on MATURE descending (less negative first)
   - canonical sorted key

This explicitly rewards universes that do not collapse in one of the required windows.

## Token-frequency evidence

For U9 and U10 robust rankings record token frequency in:

- Top 10
- Top 50
- Top 100

Also record intersection cores where applicable.

## Matched marginal contribution

For every U10 universe and each token X in that universe:

- compare U10 against the exactly matched U9 = U10 minus X
- delta = U10 metric minus matched-U9 metric

Aggregate by token across all matched contexts:

- context count
- median return delta for 1Y / 2Y / MATURE
- positive-delta rate for 1Y / 2Y / MATURE
- q25 / q75 return delta
- worst / best return delta
- median drawdown delta

This is the primary test for whether a token improves the network rather than merely appearing in hindsight winners.

## Rolling robustness — finalists only

To avoid an unnecessarily huge repeated brute-force pass, rolling tests are applied only after the exhaustive stage to the union of the Top 30 robust U9 and Top 30 robust U10 universes.

Rolling windows:

- monthly-start 12-month windows
- monthly-start 24-month windows

Record:

- window count
- median window return
- worst window return
- positive-window rate
- median max drawdown
- worst max drawdown

The rolling pass is a stability diagnostic for finalists, not a new optimization space.

## Endpoint sensitivity — finalists only

For Top 30 robust U9/U10, repeat 12-month evaluation with end-date offsets:

0, 30, 60, 90, 120, 180 days from 2026-09-26.

Record median return and drawdown distributions across endpoints.

## Bull-run neutralization — finalists only

Diagnostic only.

Define a broad bull day as a day when the median 30-day return across the 15-token pool is >= +25%.

For finalist MATURE equity series, positive daily strategy returns occurring on broad bull days are set to 0; negative daily returns remain unchanged.

This is not an alternate production strategy. It is only a confound diagnostic.

## Controls / anti-overfit rules

- Do not select a winner from LAST_1Y alone.
- Do not require ATOM, ALGO, FIL, PEPE, AVAX, or any other token.
- U9 is a valid control; "nobody replaces ATOM" remains possible.
- Preserve all results, including weak tokens and failed universes.
- No strategy promotion, notification change, merge to live config, or Telegram change is authorized by this stress test.
- Historical evidence is not future performance evidence.

## Outputs

- all_u9_primary_windows.csv
- all_u10_primary_windows.csv
- top_u9_robust.csv
- top_u10_robust.csv
- token_marginal_contribution.csv
- finalists_rolling_endpoint.csv
- summary.json
- report.md

## Test level target

TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

If runtime fails or public data cannot be retrieved, report the actual lower level reached.