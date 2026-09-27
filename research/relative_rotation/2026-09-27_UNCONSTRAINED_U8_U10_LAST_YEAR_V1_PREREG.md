# Unconstrained U8/U10 Last-Year Exhaustive Search v1 — preregistration

Date: 2026-09-27
Mode: STRESS_TEST_ONLY

## Question

What are the strongest U8 and U10 combinations over the latest fully closed one-year period if no asset is mandatory?

This is a descriptive exhaustive ranking of already-open historical data. It is NOT a selector for live deployment.

## Candidate pool

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, AVAX, FIL, ETH, ALGO, ADA, XRP, HBAR

## Exhaustive spaces

- U8: choose any 8 of 15 = 6,435 universes
- U10: choose any 10 of 15 = 3,003 universes

No mandatory assets.

## Evaluation window

2025-09-27 -> 2026-09-26

## Frozen strategy

- Binance Spot 1D closed candles
- rolling median 180d
- ARM 15%
- reversal confirmation 3%
- strongest confirmed max-dislocation router
- next-open execution
- transition cost 0.1%

Each universe is evaluated from every asset in that universe as a starting asset.
Primary ranking metric:
- median terminal return across all possible starting assets in that universe.

Secondary metrics:
- worst start return
- best start return
- positive-start count
- median max drawdown
- median transitions/conflicts
- ATOM-start return when ATOM is a member
- PEPE-start return when PEPE is a member
- TWT-start return when TWT is a member

## Outputs

1. Top 10 U8 by median return.
2. Top 10 U10 by median return.
3. Distribution quantiles for all U8 and all U10.
4. Asset frequency in top 10 / top 50 / top 100 for each universe size.
5. Repeated-core analysis.
6. Compare canonical U8, U9_CLEANER and research U10 against the unconstrained distributions when size-compatible.
7. Hindsight warning: top combinations are not candidates for live promotion from this test alone.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
