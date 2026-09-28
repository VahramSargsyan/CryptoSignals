# RR U10 MULTIBOOK MAX2 SHADOW FALLBACK V1 — PREREG

Date: 2026-09-28  
Workflow mode: STRESS_TEST_ONLY  
Production/live changes: NONE

## Frozen universe

`RR_TARGET_U10_CANDIDATE_HBAR_V1`

Assets:

`TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR`

No universe search, token optimization, or parameter tuning is allowed in this experiment.

## Research question

Can we preserve more of the unconstrained U10 consensus/upside while still preventing deliberate 100% physical concentration by allowing at most **2 of 3 books** to occupy the same token?

This is the distinct hypothesis previously recorded as:

`MAX_2_OF_3_BOOKS_PER_ASSET`

## Book state

Each of 3 equal books keeps two states:

1. `shadow_core_asset`
   - follows the ordinary frozen U10 relative-rotation graph;
   - continues to rotate even while the physical book is defensively parked.

2. `actual_asset`
   - physically carries that book's backtest capital;
   - is subject to a portfolio occupancy constraint.

## Portfolio occupancy rule

At every physical execution step:

`max books per actual asset = 2`

Therefore:

- 1+1+1 holdings are allowed;
- 2+1 holdings are allowed;
- 3-in-1 is forbidden.

If all three shadow books converge to the same target:
- at most two physical books may synchronize to that target;
- the remaining physical book uses defensive parking;
- its shadow state still follows the converged U10 core.

If only two shadow books share a target and the third has a different target:
- all three may synchronize normally;
- no defensive parking is required.

## Winner selection when a third book is blocked

For a shadow target desired by 3 books, the two physical slots are allocated in this order:

1. books already physically resident in the shadow target;
2. books with the strongest newly confirmed `max_dislocation`;
3. deterministic book index tie-break.

No future information is used.

## Defensive fallback for the blocked third book

The blocked book:

1. keeps its current defensive token if it is already parked there and that token remains below the occupancy cap of 2;
2. otherwise selects the lowest 30-day realized close-to-close volatility U10 token with fewer than 2 actual books assigned;
3. remains parked until it can synchronize back to its current shadow core without violating the max-2 occupancy rule.

TRX is **not hardcoded**.

The test records which tokens actually become defensive parking assets.

## Causal volatility

For a physical execution at open T, parking-token ranking uses only information available through close T-1.

## Frozen core mechanics

- Binance Spot D1
- rolling median lookback: 180 days
- ARM threshold: 15%
- reversal confirmation: 3%
- strongest confirmed max-dislocation router
- confirmed at close T -> physical/shadow execution at next available daily open
- deterministic tie-break
- 0.1% modeled transaction cost on actual physical transitions only
- no cost on shadow-only state transitions

## Start-state robustness

Evaluate every unordered distinct 3-of-10 starting triplet:

`C(10,3) = 120`

At initial open:
- actual == shadow for each book;
- each book receives 1/3 of capital;
- no initial fee.

## Comparator

Same-run control:

`V2_FREE`

- 3 independent U10 books;
- no occupancy limit;
- no defensive parking;
- books may fully converge.

Historical context, not rerun as a separate candidate:
- `V3_SHADOW_DEFENSIVE_FALLBACK` from run `36421236295`, where all three physical holdings had to remain distinct.

This new test changes the portfolio rule materially, so it does not violate the previous no-repeat rule.

## Primary windows

- LAST_1Y: 2025-09-27 -> 2026-09-26
- LAST_2Y: 2024-09-27 -> 2026-09-26
- MATURE: 2023-10-31 -> 2026-09-26

## Metrics

Per start triplet / window:

- portfolio return
- portfolio max drawdown
- actual physical transition count
- shadow transition count
- modeled transaction costs
- max / median largest-asset value share
- days with largest-asset value share >50%
- days with largest-asset value share >=2/3
- days with all 3 physical books in one asset (must be 0)
- days with a 2+1 physical structure
- days with 1+1+1 physical structure
- shadow collision / full-shadow-convergence rates
- actual/shadow mismatch rate
- defensive parking entries
- defensive parking book-days
- resynchronizations
- defensive displacements
- defensive-token distribution
- TRX share of defensive parking

## Rolling checks

Monthly-start:
- 12-month windows
- 24-month windows

Evaluate all 120 starting triplets in every rolling window.

Report:
- median return
- worst start-triplet return
- positive-window rate
- median and worst max DD
- peak concentration
- >50% concentration-day share
- actual/shadow mismatch
- defensive parking frequency

## Primary interpretation

The candidate is interesting only if it improves the trade-off relative to both known extremes:

- V2_FREE: very high historical upside but effectively 100% convergence;
- V3_SHADOW_DEFENSIVE_FALLBACK: strong concentration/DD reduction but large upside sacrifice.

The desired result is not a predefined return target. We will report the observed return/concentration/drawdown trade-off without retuning.

## No-repeat / production boundary

After a successful run:
- persist exact evidence and runtime identity;
- persist a decision log and no-repeat rule;
- do not change paper/live U9, frozen U10 candidate, Telegram, or exchange behavior.

Any production implementation would require a separate mode, persistent dual-state design, MIGRATION_PLAN, and rollback.

Target test level:

`TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`
