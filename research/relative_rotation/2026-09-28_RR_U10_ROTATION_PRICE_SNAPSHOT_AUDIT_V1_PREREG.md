# RR U10 ROTATION PRICE SNAPSHOT AUDIT V1 — PREREG

Date: 2026-09-28
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Research question

Does the frozen U10 relative-rotation strategy increase USDT-equivalent capital
because of the rotation mechanics themselves across bull, mixed and bear market
regimes, rather than only because the broad market rises?

The test must expose every actual holding leg between rotations and the prices
of all U10 assets at the relevant execution snapshots.

## Frozen U10

Universe:

`RR_TARGET_U10_CANDIDATE_HBAR_V1`

Assets:

`TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR`

Core semantics remain unchanged:

- Binance Spot D1
- rolling median lookback = 180d
- ARM = 15%
- reversal = 3%
- strongest confirmed max-dislocation router
- signal on close T
- execution on next available daily open
- modeled transition cost = 0.1%
- all 10 possible starting assets
- MATURE window = 2023-10-31 through 2026-09-26

No strategy parameter is changed.

## Canonical equivalence requirement

The audit trace must reproduce the canonical `simulate_one()` path exactly.

For every starting asset verify:

- same transition count;
- same final asset;
- normalized final capital from the trace equals canonical final-equity return
  within floating-point tolerance;
- no route divergence.

If equivalence fails, the runtime fails.

## Capital normalization

For each start state, normalize canonical initial equity to:

`100.00 USDT`

This is a reporting normalization only.

The strategy still uses the original canonical quantity/execution formulas.

## What counts as one rotation leg

A completed rotation leg begins when the strategy rotates **into** a token and
ends at the next rotation execution out of that token.

Example:

`ATOM-like current asset -> TWT -> BNB`

The TWT leg is:

- entry: execution open when TWT is bought;
- exit: execution open when TWT is sold for BNB.

Important:

Immediate capital after an exchange is approximately the same capital minus the
0.1% modeled transition cost. That immediate exchange is **not** treated as the
profit of the rotation.

Profit of a completed leg is the capital change while holding the selected
token until the next rotation.

## Per-leg fields

For each start-state path and each completed rotated leg persist:

- start_asset
- leg_index
- signal date that caused entry
- entry execution date
- previous asset / entry asset
- entry asset open price
- entry capital after entry cost
- exit signal date
- exit execution date
- next asset
- exit asset open price
- exit capital before next transition cost
- next-entry capital after next 0.1% cost
- holding days
- gross holding return
- net cycle return through next-entry cost
- selected pair / max_dislocation used for the entry transition where available

Also preserve the final open leg through the cutoff with
`completed=false` and mark-to-market capital at cutoff.

## Full U10 price snapshots

At every rotation execution date persist the **open price of all 10 U10 tokens**.

For every completed leg calculate from the entry snapshot to exit snapshot:

- return of each of the 10 U10 tokens;
- median U10 token return;
- equal-weight U10 arithmetic mean return;
- held-token return;
- held-token excess versus median U10 token return;
- rank of the held token's return among the 10 tokens for that exact holding
  interval (1 = best).

This directly tests whether the selected token outperformed the contemporaneous
U10 cross-section during the holding leg.

## Market regime

Use the canonical repository cache:

`data/market/cryptocap_total_d1.csv`

Frozen SHA256:

`d0c9b3bac915e484069c6302ddce1a0b050abbb8de155e4106e39cf6e519d3ee`

No TradingView refresh.

Map canonical TOTAL states to:

- BULL:
  - BULL_BUILDING
  - FULL_BULL_ALIGNMENT
- BEAR:
  - BEAR_BUILDING
  - FULL_BEAR_ALIGNMENT
- MIXED:
  - MIXED

For every completed leg report:

1. entry regime;
2. exit regime;
3. majority regime across daily holding dates.

## Regime-level tests

For BULL / MIXED / BEAR separately, using all 10 start paths:

- completed leg count;
- median holding return;
- median net cycle return;
- positive-leg rate;
- median held-token rank among U10;
- median held-token excess versus U10 median;
- conditioned compounded net leg return per start path;
- median conditioned compounded return across the 10 starts.

Important: raw duplicated post-convergence legs must not be treated as
independent evidence.

Therefore also persist a de-duplicated leg table keyed by:

`entry execution date + held asset + exit execution date + next asset`

and report:
- unique leg count;
- number of start paths sharing each leg.

Primary regime conclusions should use **per-start summaries and unique-leg
statistics**, not a naive pooled count after convergence.

## "Always increases" falsification test

Explicitly test the strong hypothesis:

`EVERY_COMPLETED_ROTATION_LEG_INCREASES_CAPITAL`

Report:

- number of completed unique legs;
- positive / zero / negative unique legs;
- worst unique leg;
- best unique leg;
- whether every start path has monotonically increasing capital at every
  transition.

Possible classifications:

- `STRICT_ALWAYS_INCREASES`
- `NOT_STRICTLY_MONOTONIC_BUT_POSITIVE_ACROSS_REGIMES`
- `REGIME_DEPENDENT_ROTATION_EDGE`
- `NO_ROTATION_EDGE`

Do not soften a failed "always" hypothesis.

## Outputs

Persist:

- `rotation_price_snapshots.csv`
- `rotation_legs_all_starts.csv`
- `rotation_legs_unique.csv`
- `start_path_summary.csv`
- `regime_summary.csv`
- `canonical_equivalence.csv`
- `summary.json`
- `report.md`

## No-repeat / production boundary

Research only.

Do not alter:

- paper/live U9
- frozen U10 candidate
- defensive overlay
- Telegram
- exchange execution
- portfolio allocation

Target test level:

`TEST_LEVEL: GITHUB_ACTIONS_REPOSITORY_CACHE_PLUS_LIVE_PUBLIC_DATA_STRESS_TEST`
