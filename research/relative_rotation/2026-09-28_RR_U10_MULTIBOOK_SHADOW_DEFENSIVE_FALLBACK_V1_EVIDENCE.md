# RR U10 MULTIBOOK SHADOW DEFENSIVE FALLBACK V1 — EVIDENCE

Date: 2026-09-28  
Workflow mode: STRESS_TEST_ONLY  
Production/live changes: NONE

## Runtime identity

- Frozen universe: `RR_TARGET_U10_CANDIDATE_HBAR_V1`
- Assets: TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR
- GitHub Actions run: `36421236295`
- Source commit: `c9e9e91e4e2ce0cbe3ff99ceb76b310f0eb320a0`
- Artifact ID: `10969172637`
- Artifact: `rr-u10-multibook-shadow-defensive-fallback-v1-2`
- Artifact SHA256: `39dd7e3d588599c624100d0f70055c6bc88c664d1fd6c45bf5ff834b3dcb67b5`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Why this test supersedes the interpretation of strict V3

The earlier multibook test used a strict physical collision guard. If a core transition could not be executed without creating duplicate holdings, the affected book usually remained in its old physical token.

That was a valid test of that exact rule, but it was not the user's intended architecture.

The repository already contains a defensive research concept with:
- no USDT requirement;
- lowest 30-day realized-volatility crypto token selected for defensive holding;
- defensive token frozen until exit;
- explicit `return-to-shadow routing remains manual` wording.

This experiment therefore tested a dual-state architecture:

- `shadow_core_asset`: continues to follow ordinary frozen U10;
- `actual_asset`: carries real backtest capital and may be temporarily parked in a low-volatility token.

The old strict-V3 evidence remains valid for `STRICT_STAY_COLLISION_GUARD` only. It must not be interpreted as evidence against the shadow/defensive architecture.

## Frozen mechanics

Core shadow strategy:
- Binance Spot D1
- rolling median: 180 days
- ARM: 15%
- reversal: 3%
- strongest confirmed max-dislocation router
- signal close T -> next available daily open
- deterministic tie-break

Physical layer:
- 3 equal books
- all 120 distinct 3-of-10 start triplets
- actual holdings must remain distinct
- shadow state is unconstrained and continues even while physically parked
- a colliding shadow target has one synchronized physical winner
- a parked book keeps its defensive token while feasible
- otherwise it selects the lowest causal 30-day realized-volatility free U10 token
- physical transitions pay 0.1%
- shadow-only state changes carry no capital and no separate fee
- volatility used at open T uses information only through close T-1

TRX was **not hardcoded**.

## Primary results

| Window | Variant | Median return | Worst start | Median DD | Worst DD | Median peak concentration | Median daily largest share | Days >50% | Actual collision days | Shadow collision days | Any actual/shadow mismatch days |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1Y | V2_FREE | +135.3% | +24.5% | -62.4% | -73.6% | 100.0% | 100.0% | 98.4% | 98.4% | 98.4% | 0.0% |
| 1Y | V3_SHADOW_DEFENSIVE_FALLBACK | +8.0% | -5.4% | -40.9% | -51.6% | 52.4% | 43.8% | 4.4% | 0.0% | 98.4% | 98.4% |
| 2Y | V2_FREE | +1864.2% | +1696.2% | -62.4% | -62.4% | 100.0% | 100.0% | 99%+ | 98.9% | 98.9% | 0.0% |
| 2Y | V3_SHADOW_DEFENSIVE_FALLBACK | +358.5% | +263.9% | -46.2% | -52.9% | 66.9% | 52.1% | 60.6% | 0.0% | 98.9% | 98.9% |
| MATURE | V2_FREE | +3191.5% | +2708.3% | -62.4% | -62.4% | 100.0% | 100.0% | 99%+ | 98.7% | 98.7% | 0.0% |
| MATURE | V3_SHADOW_DEFENSIVE_FALLBACK | +673.9% | +375.5% | -45.3% | -52.7% | 76.5% | 43.6% | 25.9% | 0.0% | 98.7% | 98.7% |

## Risk / return trade-off versus unconstrained V2

V3 shadow minus V2 median-return delta:
- 1Y: -127.3 percentage points
- 2Y: -1505.7 percentage points
- MATURE: -2517.6 percentage points

Median max-drawdown improvement:
- 1Y: +21.6 pp
- 2Y: +16.3 pp
- MATURE: +17.1 pp

Median peak-concentration reduction:
- 1Y: -47.6 pp
- 2Y: -33.1 pp
- MATURE: -23.5 pp

So shadow/defensive parking gives up substantial upside, but unlike the old strict V3 it also produces a large and persistent drawdown improvement.

## Comparison with the previously tested strict V3

Previous strict V3:
- 1Y: +14.7%, DD -52.0%, median peak concentration 65.3%
- 2Y: +116.6%, DD -66.1%, median peak concentration 51.8%
- MATURE: +264.4%, DD -68.8%, median peak concentration 75.6%

Corrected shadow/defensive V3:
- 1Y: +8.0%, DD -40.9%, median peak concentration 52.4%
- 2Y: +358.5%, DD -46.2%, median peak concentration 66.9%
- MATURE: +673.9%, DD -45.3%, median peak concentration 76.5%

Interpretation:
- latest 1Y return is slightly lower than strict V3, but drawdown and concentration improve;
- 2Y and MATURE return are much higher than strict V3;
- 2Y and MATURE drawdown are dramatically better than strict V3;
- this confirms that allowing shadow-core routing to continue is materially different from simply forcing a conflicted book to stay in its old asset.

## Rolling checks

| Window | Variant | Windows | Median window return | Worst median-return window | Positive windows | Median window DD | Median peak concentration |
|---|---|---:|---:|---:|---:|---:|---:|
| 12m | V2_FREE | 23 | +148.2% | +9.5% | 100% | -47.8% | 100.0% |
| 12m | V3_SHADOW_DEFENSIVE_FALLBACK | 23 | +58.3% | -9.7% | 95.7% | -39.9% | 56.2% |
| 24m | V2_FREE | 11 | +558.2% | +262.4% | 100% | -58.5% | 100.0% |
| 24m | V3_SHADOW_DEFENSIVE_FALLBACK | 11 | +240.0% | +138.6% | 100% | -44.5% | 68.1% |

The 24m rolling windows are especially important:
- every shadow-fallback 24m window remained positive;
- median DD was materially lower than V2;
- concentration was substantially lower than V2.

## TRX as the practical USDT substitute

TRX was not preselected.

Across all 120 MATURE start triplets, defensive parking was:

| Token | Parking entries | Entry share | Parking book-days | Parking-day share |
|---|---:|---:|---:|---:|
| TRX | 280 | 23.5% | 123,778 | **49.5%** |
| BNB | 379 | 31.8% | 46,334 | 18.5% |
| XRP | 173 | 14.5% | 43,042 | 17.2% |
| TWT | 152 | 12.8% | 23,506 | 9.4% |
| HBAR | 208 | 17.4% | 13,241 | 5.3% |
| PEPE | 0 | 0% | 0 | 0% |
| AAVE | 0 | 0% | 0 | 0% |
| AVAX | 0 | 0% | 0 | 0% |
| FIL | 0 | 0% | 0 | 0% |
| ALGO | 0 | 0% | 0 | 0% |

TRX therefore naturally acted as the dominant defensive parking asset by duration, accounting for about half of all MATURE defensive parking book-days.

This supports the user's memory that TRX often served as the practical crypto alternative to USDT, while preserving the actual rule: **lowest-volatility feasible crypto token, not fixed TRX**.

## Structural finding

The shadow paths still converge almost completely:
- 1Y shadow collision: 98.4%
- 2Y: 98.9%
- MATURE: 98.7%

The physical layer prevents that convergence:
- actual collision: 0%.

Therefore, after shadow convergence, the architecture usually behaves like:
- one book physically follows the U10 consensus leader;
- two books remain defensively parked in other low-volatility U10 assets;
- their shadow states continue to follow the same core leader.

This is exactly why actual/shadow mismatch occurs on almost every day after convergence.

## Concentration caveat

Three distinct physical tokens do **not** create a hard 50% value cap.

Because books are not rebalanced across one another, one successful book can become much larger by market appreciation.

MATURE:
- median daily largest-asset share: 43.6%
- days above 50%: 25.9%
- median peak largest-asset share: 76.5%
- worst start-triplet peak: 86.6%

So this architecture materially reduces typical concentration, but it is not a hard capital-weight limit.

A true hard 50% value cap would require partial transfers/rebalancing and would be a separate strategy.

## Research classification

`V3_SHADOW_DEFENSIVE_FALLBACK`

Status:

`VIABLE_RESEARCH_ARCHITECTURE / NOT_PRODUCTION_APPROVED`

Reason:
- achieves 0% deliberate physical collision;
- materially lowers historical drawdown versus unconstrained V2;
- materially lowers typical concentration;
- preserves substantially more long-window return than strict-stay V3;
- 24m rolling median returns remain positive in all tested windows;
- TRX naturally emerges as the dominant defensive parking asset by duration.

It is **not** promoted because:
- it is selected/tested on known history;
- upside sacrifice versus unconstrained V2 remains very large;
- peak concentration can still exceed 50% substantially;
- the exact actual/shadow state machine has not been forward-validated.

## MAX_2_OF_3 note

The previously recorded idea:

`MAX_2_OF_3_BOOKS_PER_ASSET`

remains `IDEA_NOT_TESTED`.

This shadow/defensive version is not a failure. Therefore the max-2-of-3 idea is no longer an automatic next step; it is a separate higher-concentration / potentially higher-upside hypothesis to test only if desired.

## No-repeat rule

Do not rerun this exact shadow/defensive experiment on the same frozen U10 and same historical data merely to reproduce the result.

A rerun requires:
1. materially new unseen data;
2. documented engine/semantic correction;
3. changed cost assumptions;
4. changed universe;
5. changed portfolio rule;
6. separately preregistered confirmatory/forward validation.

## Production boundary

- current U9 paper/live target unchanged;
- frozen U10 candidate unchanged;
- defensive overlay production state unchanged;
- Telegram unchanged;
- no exchange/order behavior changed.

## Residual risks

- historical evidence only;
- no true future OOS evidence for this portfolio architecture;
- fixed 0.1% transition cost;
- no time-varying spread/liquidity model;
- no cross-book rebalance;
- low-volatility token selection is causal but still historically evaluated;
- common U10 panel begins in 2023.
