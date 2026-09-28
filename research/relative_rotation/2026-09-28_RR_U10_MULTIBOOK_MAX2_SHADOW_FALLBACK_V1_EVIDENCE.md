# RR U10 MULTIBOOK MAX2 SHADOW FALLBACK V1 — EVIDENCE

Date: 2026-09-28  
Workflow mode: STRESS_TEST_ONLY  
Production/live changes: NONE

## Runtime identity

- Frozen universe: `RR_TARGET_U10_CANDIDATE_HBAR_V1`
- GitHub Actions run: `36423150747`
- Source commit: `ca8f5b046794010e65d6dac31ed0b7f9af9121c4`
- Artifact ID: `10970109127`
- Artifact: `rr-u10-multibook-max2-shadow-fallback-v1-1`
- Artifact SHA256: `3f88ec08424b1093f8f1879b280e11ea06c89a99d4aa4bcfdafad05b860a7717`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Frozen universe and mechanics

U10:

`TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR`

Data:
- Binance Spot D1
- common panel rows: 1,241
- common panel start: 2023-05-05
- common panel end: 2026-09-26

Core:
- rolling median lookback: 180 days
- ARM: 15%
- reversal: 3%
- confirmed close T -> next available daily open
- strongest confirmed max-dislocation routing
- physical transaction cost: 0.1%

Portfolio:
- 3 books
- equal 1/3 initial allocation
- all 120 unordered distinct 3-of-10 start triplets
- shadow core continues while physically parked
- physical occupancy cap: max 2 books per actual asset
- third book uses lowest causal 30-day realized-volatility feasible U10 token
- TRX is not hardcoded

## What MAX2 means

The constraint is deliberately minimal:

- 1+1+1 physical structure: allowed
- 2+1 physical structure: allowed
- 3-in-1 physical structure: forbidden

If all three shadow books want the same token:
- two physical books may follow the core consensus;
- the third remains logically on the same shadow route but is physically parked defensively.

If only two shadow books want the same token, no constraint fires.

## Primary results

| Window | Variant | Median return | Worst triplet | Median max DD | Worst max DD | Median daily largest asset | Median peak concentration | Full 3-in-1 days |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1Y | V2_FREE | +135.3% | +24.5% | -62.4% | -73.6% | 100.0% | 100.0% | 88.2% |
| 1Y | V4_MAX2_SHADOW_FALLBACK | +92.5% | +18.8% | -39.6% | -51.7% | 56.4% | 82.4% | 0.0% |
| 2Y | V2_FREE | +1864.2% | +1696.2% | -62.4% | -62.4% | 100.0% | 100.0% | 94.5% |
| 2Y | V4_MAX2_SHADOW_FALLBACK | +1191.4% | +773.4% | -58.3% | -59.0% | 85.9% | 95.7% | 0.0% |
| MATURE | V2_FREE | +3191.5% | +2708.3% | -62.4% | -62.4% | 100.0% | 100.0% | 96.5% |
| MATURE | V4_MAX2_SHADOW_FALLBACK | +2223.8% | +1867.4% | -57.1% | -58.5% | 80.5% | 95.1% | 0.0% |

## Concentration behavior

MAX2 successfully removes deliberate 3-in-1 convergence:

- 1Y full 3-in-1 days: 0%
- 2Y: 0%
- MATURE: 0%

But it remains highly concentrated because two books usually share the same consensus leader.

MATURE:
- 2+1 physical structure: 98.7% of days
- all-distinct physical structure: 1.3%
- largest asset >50% of capital: 98.7% of days
- largest asset >=2/3 of capital: 77.2% of days
- median daily largest-asset share: 80.5%
- median peak concentration: 95.1%
- worst start-triplet peak concentration: 96.3%

Therefore MAX2 solves:

`all three books deliberately in one token`

but does **not** solve:

`one token can still dominate most of portfolio value`.

## Return / DD trade-off versus V2_FREE

### 1Y
- return: +135.3% -> +92.5%
- median DD: -62.4% -> -39.6%
- DD improvement: +22.8 percentage points
- median peak concentration: 100.0% -> 82.4%

### 2Y
- return: +1864.2% -> +1191.4%
- median DD: -62.4% -> -58.3%
- DD improvement: +4.1 pp
- median peak concentration: 100.0% -> 95.7%

### MATURE
- return: +3191.5% -> +2223.8%
- median DD: -62.4% -> -57.1%
- DD improvement: +5.3 pp
- median peak concentration: 100.0% -> 95.1%

On the long window MAX2 preserves much more upside than MAX1 shadow, but its concentration and DD protection are substantially weaker.

## Historical frontier with prior MAX1 shadow architecture

The correct MAX1 shadow architecture was previously tested in run `36421236295`.

MATURE comparison:

| Architecture | Median return | Median DD | Median daily largest asset | Median peak concentration | Full physical convergence |
|---|---:|---:|---:|---:|---:|
| V2_FREE | +3191.5% | -62.4% | 100.0% | 100.0% | 96.5% of days |
| V4_MAX2_SHADOW_FALLBACK | +2223.8% | -57.1% | 80.5% | 95.1% | 0% |
| V3_MAX1_SHADOW / distinct physical books | +673.9% | -45.3% | 43.6% | 76.5% | 0% |

This creates a clear historical frontier:

- V2_FREE preserves the most upside but converges almost completely;
- MAX2 preserves substantially more upside than MAX1 and prevents literal 3-in-1 occupancy;
- MAX1 provides materially stronger concentration and drawdown reduction, but sacrifices much more upside.

This is historical evidence, not a future-return forecast.

## Rolling checks

| Rolling | Variant | Windows | Median window return | Worst median-return window | Positive windows | Median window DD | Median peak concentration |
|---|---|---:|---:|---:|---:|---:|---:|
| 12m | V2_FREE | 23 | +148.2% | +9.5% | 100% | -47.8% | 100.0% |
| 12m | MAX2 | 23 | +101.9% | +4.6% | 100% | -44.5% | 80.7% |
| 24m | V2_FREE | 11 | +558.2% | +262.4% | 100% | -58.5% | 100.0% |
| 24m | MAX2 | 11 | +409.0% | +217.7% | 100% | -51.6% | 91.5% |

All tested rolling median-return windows remained positive for MAX2.

Worst individual start-triplet return in rolling windows can still be negative:
- worst 12m triplet: about -27.6%
- worst 24m triplet: +173.7%

## Defensive parking behavior

MATURE aggregate parking duration:

| Token | Parking entries | Entry share | Parking book-days | Parking-day share |
|---|---:|---:|---:|---:|
| TRX | 248 | 48.7% | 119,631 | 96.6% |
| XRP | 141 | 27.7% | 3,852 | 3.1% |
| BNB | 120 | 23.6% | 360 | 0.3% |
| others | 0 | 0% | 0 | 0% |

TRX again emerged naturally, and much more strongly than in the MAX1 architecture:
- median per-triplet TRX share of MATURE parking: 95.9%
- aggregate MATURE parking-day share: 96.6%

The mechanism remains `lowest-volatility feasible U10 token`, not fixed TRX.

## Transition behavior

MATURE median:
- V2 physical transitions: 77
- MAX2 physical transitions: 52
- MAX2 shadow transitions: 77
- MAX2 parking entries: 4
- MAX2 resynchronizations: 3
- MAX2 median parking book-days: 1,025
- MAX2 defensive displacements: 0

The third book therefore often stays parked for long stretches while the two physical books continue to follow consensus.

## Research classification

`V4_MAX2_SHADOW_FALLBACK`

Status:

`VIABLE_RESEARCH_ARCHITECTURE / NOT_PRODUCTION_APPROVED`

Why it remains viable:
- prevents deliberate 100% 3-book convergence;
- preserves substantially more historical upside than MAX1 shadow;
- improves DD versus V2 in every primary horizon tested;
- all rolling median-return windows are positive;
- shadow/actual state remains causal and reproducible.

Why it is not production-approved:
- concentration remains very high in value terms;
- historical selection/validation only;
- no true future OOS evidence;
- no production dual-state schema;
- fixed 0.1% cost model;
- no time-varying spread/liquidity model.

## No-repeat rule

Do not rerun this exact MAX2 experiment on the same frozen U10 and same historical data merely to reproduce the result.

A repeat requires:
1. materially new unseen data;
2. documented engine/semantic correction;
3. changed execution cost assumptions;
4. changed universe;
5. changed portfolio rule;
6. separate confirmatory/forward validation.

## Production boundary

- current paper/live U9 unchanged;
- frozen U10 candidate unchanged;
- Telegram unchanged;
- no exchange execution;
- no production state/schema changes.

Any production implementation would require a separate implementation mode and MIGRATION_PLAN because `actual_asset` and `shadow_core_asset` are persistent distinct states.
