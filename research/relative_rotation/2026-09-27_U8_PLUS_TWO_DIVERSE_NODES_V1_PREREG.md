# U8 + Two Diverse Nodes v1 — preregistration

Date: 2026-09-27
Mode: STRESS_TEST_ONLY

## Research question

Does expanding the canonical U8 with two economically non-redundant nodes improve long-horizon relative-rotation performance without the topology pollution observed in broad U20 expansion?

Live U8 remains unchanged.

## Canonical U8

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK

Primary niches already represented:
- interoperability
- wallet
- meme
- exchange/platform
- smart-contract L1
- payments
- lending
- oracle

## Added nodes

Within the original documented 15-token discovery pool, the only two assets that add previously absent primary niches are:

- FIL — decentralized storage
- HBAR — enterprise DLT

They are selected for niche expansion, not for observed future return.

## Fixed universes

- U8 = canonical U8
- U9_FIL = U8 + FIL
- U9_HBAR = U8 + HBAR
- U10_FIL_HBAR = U8 + FIL + HBAR

No other token is admitted in this v1.

## Frozen strategy

- Binance Spot 1D closed candles
- rolling median 180d
- ARM 15%
- reversal confirmation 3%
- strongest confirmed max-dislocation router
- next-open execution
- transition cost 0.1%

No parameter tuning.

## Tests fixed before outcomes

1. Full mature common-history test after 180-row warm-up through 2026-09-26.
2. Rolling 12-month windows stepped monthly.
3. Rolling 24-month windows stepped monthly.
4. Same eight original starting assets are used for cross-universe comparison.
5. ATOM-start is reported separately.
6. Median max drawdown, worst return, transitions and conflicts are reported.
7. Compare each expanded universe directly against canonical U8.
8. Route occupancy and added-node usage are reported to distinguish useful bridges from inert or stale-hold nodes.
9. Cost sensitivity at 0.10%, 0.50%, 1.00% for U10.
10. Router sensitivity for U10: strongest, skip-conflict, weakest, deterministic-first.

## Interpretation guardrail

FIL/HBAR names are already known from opened historical diagnostics, so this test is HYPOTHESIS_GENERATION, not untouched validation.

No live change is authorized by this run.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
