# RR U10 MULTIBOOK DIVERSIFICATION V1 — PREREG

Date: 2026-09-28  
Workflow mode: STRESS_TEST_ONLY  
Production/live changes: NONE

## Frozen universe

Use the already frozen candidate only:

`RR_TARGET_U10_CANDIDATE_HBAR_V1`

Assets:

`TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR`

This experiment does not search for another universe and does not reopen the completed U9/U10 exhaustive selection.

## Research question

Can three independent relative-rotation capital books reduce single-asset concentration without destroying the historical return/risk profile?

Only two architectures are tested.

### V2 — THREE_INDEPENDENT_BOOKS_FREE

- Total capital is split equally into three independent books at the start.
- Each book runs the same frozen U10 relative-rotation mechanics.
- Each book has its own current asset and its own transition history.
- Books may converge into the same asset.
- No capital is transferred or rebalanced between books.

### V3 — THREE_INDEPENDENT_BOOKS_COLLISION_GUARD

Same as V2 except:

- after each simultaneous next-open execution, all three books must end in three distinct assets;
- when multiple books want conflicting destinations, select the feasible combination with the greatest total confirmed `max_dislocation`;
- ties prefer more executed transitions, then deterministic option order;
- if a book cannot take a feasible confirmed destination, it remains in its current asset;
- swaps are allowed when the final three assets are distinct.

This is the clean portfolio-level diversification rule for indivisible independent books.

A literal 40%/50% hard value cap is intentionally **not** added here because enforcing it after passive price drift would require cross-book rebalancing or partial-book transfers, creating a different strategy. Instead this test records actual daily largest-asset capital share so that a later rebalancing hypothesis can be justified from evidence if needed.

## Starting-state robustness

Three books must start in distinct assets.

Evaluate all unordered distinct start triplets from the frozen U10:

`C(10,3) = 120`

Each triplet starts with equal capital allocation:

`1/3 + 1/3 + 1/3`

No initial transaction cost is charged.

## Frozen relative-rotation semantics

Reuse repository mechanics:

- Binance Spot D1 OHLCV
- lookback: 180 days
- ARM threshold: 15%
- reversal confirmation: 3%
- confirmed signal at day T
- execution at next available day open
- transition cost: 0.1%
- candidate order: highest max_dislocation, then deterministic token/pair tie-break
- no look-ahead
- no production changes

## Primary windows

- LAST_1Y: 2025-09-27 -> 2026-09-26
- LAST_2Y: 2024-09-27 -> 2026-09-26
- MATURE: 2023-10-31 -> 2026-09-26

## Metrics per start triplet

For both V2 and V3 record:

- total portfolio return
- portfolio max drawdown
- transitions
- modeled transition costs
- daily largest-asset capital share:
  - maximum
  - median
  - share of days >50%
  - share of days >=2/3
  - share of days >=95%
- share of days with fewer than 3 unique held assets
- share of days with all 3 books in one asset
- ending held assets

For V3 additionally record:

- guard intervention count
- conflicted book-decisions
- alternative-destination executions
- forced stays

## Aggregate robustness

For each window and architecture report across all 120 start triplets:

- median return
- worst return
- best return
- median max drawdown
- worst max drawdown
- median / worst maximum concentration
- median collision-day share
- median transition count
- V3 intervention statistics

## Rolling temporal checks

For each architecture:

- monthly-start rolling 12-month windows
- monthly-start rolling 24-month windows

For every rolling window evaluate all 120 distinct start triplets and record:

- median portfolio return
- worst triplet return
- median max drawdown
- median maximum concentration
- median collision-day share

Then summarize stability across windows.

## Decision discipline

The experiment does not choose a production architecture automatically.

The useful comparison is the trade-off:

`V3 concentration reduction vs V2 return / drawdown / turnover cost`

V3 is historically interesting only if it materially reduces concentration/collision without an unacceptable degradation in return or drawdown.

No production/paper-live/Telegram change is authorized by this stress test.

## Persistent evidence / no-repeat

After a successful run, save:

- preregistration
- exact source commit
- GitHub Actions run ID
- artifact ID and hash
- full V2/V3 aggregate results
- rolling results
- decision log / no-repeat rule

Do not rerun the identical test merely to rediscover the same result unless new unseen data, engine semantics, costs, or portfolio rules materially change.

## Target test level

`TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`
