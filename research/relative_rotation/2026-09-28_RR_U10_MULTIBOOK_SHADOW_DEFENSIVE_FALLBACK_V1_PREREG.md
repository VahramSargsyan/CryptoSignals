# RR U10 MULTIBOOK SHADOW DEFENSIVE FALLBACK V1 — PREREG

Date: 2026-09-28  
Workflow mode: STRESS_TEST_ONLY  
Production/live changes: NONE

## Why this test exists

The completed `RR_U10_MULTIBOOK_DIVERSIFICATION_V1` strict-collision test used:

`blocked core transition -> remain in current physical asset`

That is not the intended architecture remembered by the user.

The repository already contains a defensive research concept with:

- no USDT requirement;
- a low-vol crypto token selected at defensive entry;
- defensive token frozen until exit;
- explicit `return-to-shadow routing remains manual` wording.

This experiment tests the missing dual-state architecture rather than rerunning the rejected strict-stay V3.

## Frozen universe

`RR_TARGET_U10_CANDIDATE_HBAR_V1`

Assets:

`TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR`

No universe search is performed.

## Book state

Each of 3 equal books has two separate states:

1. `shadow_core_asset`
   - follows the ordinary frozen U10 relative-rotation graph;
   - is hypothetical strategy state;
   - continues to rotate even while physical capital is parked defensively.

2. `actual_asset`
   - is the token physically carrying that book's capital in the backtest;
   - must remain distinct across the 3 books in this test;
   - may temporarily differ from `shadow_core_asset`.

A book with `actual_asset != shadow_core_asset` is in defensive parking.

## Core shadow mechanics

Frozen relative-rotation semantics:

- Binance Spot D1
- rolling median lookback: 180 days
- ARM threshold: 15%
- reversal confirmation: 3%
- strongest confirmed max-dislocation router
- confirmed at close T -> execution at next available daily open
- deterministic tie-break
- 0.1% cost is charged only on actual physical transitions

The shadow path itself does not carry capital and therefore does not pay a separate transaction cost.

## Defensive fallback

At a next-open execution:

1. Every book first updates its `shadow_core_asset` according to the ordinary unconstrained U10 core.
2. The physical layer then tries to synchronize every book's `actual_asset` to its `shadow_core_asset`.
3. Physical holdings must end the execution step in 3 distinct tokens.
4. If multiple books cannot all synchronize because their shadow targets collide:
   - maximize the number of books synchronized to shadow;
   - prefer a book already physically resident in its shadow asset;
   - then prefer the stronger just-confirmed max-dislocation;
   - deterministic tie-break after that.
5. A book that cannot synchronize enters or remains in defensive parking.
6. New defensive parking chooses the lowest 30-day realized close-to-close volatility token that is feasible after the distinct-holdings constraint.
7. An already parked defensive token is frozen until that book can synchronize back to its current shadow asset, matching the repository's existing defensive concept.
8. If a parked token must be displaced to keep the final state feasible, that is recorded explicitly as a defensive displacement; the optimizer should avoid such displacement before all lower-priority alternatives.

This is not a fixed-TRX rule.

TRX is part of the U10 and may naturally be selected when it is the lowest-vol feasible defensive token. The experiment records parking-token frequency so we can see how often TRX actually serves as the USDT substitute.

## Causal volatility

For physical execution at open T, the 30-day realized-volatility ranking uses only information available through close T-1.

No current-day high/low/close is used to choose the parking token.

## Starting-state robustness

Evaluate every unordered 3-of-10 distinct starting triplet:

`C(10,3) = 120`

At the initial open:

- `actual_asset == shadow_core_asset` for all 3 books;
- equal initial capital: 1/3 each;
- no initial transaction cost.

## Comparators

Primary candidate:

`V3_SHADOW_DEFENSIVE_FALLBACK`

Control:

`V2_FREE`

The V2 control is rerun only because this is a materially different portfolio-rule experiment and provides same-run fair comparison.

The previously tested strict collision/stay V3 is not rerun. Its persisted evidence is context only.

## Primary windows

- LAST_1Y: 2025-09-27 -> 2026-09-26
- LAST_2Y: 2024-09-27 -> 2026-09-26
- MATURE: 2023-10-31 -> 2026-09-26

## Metrics

Per start triplet / window:

- portfolio return
- portfolio max drawdown
- physical transitions
- modeled transaction costs
- peak largest-asset value share
- median largest-asset value share
- days >50% largest-asset share
- physical collision-day share
- shadow collision-day share
- actual/shadow mismatch-day share
- number of parking entries
- number of shadow resynchronizations
- parking days
- parking-token frequency
- TRX parking entries/days
- defensive displacements
- ending actual assets
- ending shadow assets

Aggregate across 120 start triplets:

- median / worst / best return
- median / worst max DD
- median / worst concentration
- mismatch and parking statistics
- TRX parking share
- transition/cost statistics

## Rolling checks

Monthly-start:

- 12-month windows
- 24-month windows

Evaluate all 120 starting triplets on every window.

Report:

- median portfolio return
- worst triplet return
- positive-window rate
- median and worst DD
- concentration
- mismatch / parking frequency

## Interpretation gate

This test answers:

Does shadow-state + low-vol defensive parking preserve materially more of the core's historical return mechanism than the rejected strict-stay V3 while still preventing deliberate 100% physical convergence?

It does not automatically approve production use.

## No-repeat discipline

After successful runtime:

- persist exact evidence;
- persist source SHA, run ID, artifact ID/hash;
- add no-repeat memory;
- do not rerun unchanged on the same historical data.

## Production boundary

- paper/live target unchanged;
- frozen U10 candidate unchanged;
- Telegram unchanged;
- no exchange orders;
- no production promotion.

Target test level:

`TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`
