# Relative Rotation Multibook MAX2 Decision Log V1

Date: 2026-09-28  
Status: ACTIVE RESEARCH MEMORY  
Workflow mode: STRESS_TEST_ONLY  
Production change: NONE

## Frozen universe

`RR_TARGET_U10_CANDIDATE_HBAR_V1`

## Tested architecture

`V4_MAX2_SHADOW_FALLBACK`

Three equal books keep dual state:
- `shadow_core_asset` follows ordinary unconstrained U10;
- `actual_asset` carries physical backtest capital.

Portfolio occupancy rule:

`MAX 2 OF 3 BOOKS PER ACTUAL ASSET`

A 2+1 structure is allowed. A 3-in-1 physical structure is forbidden.

When all three shadow books want the same token:
- two books may physically follow consensus;
- the third uses the lowest-volatility feasible defensive U10 token;
- the third shadow route continues normally.

TRX is not hardcoded.

Canonical evidence:

`research/relative_rotation/2026-09-28_RR_U10_MULTIBOOK_MAX2_SHADOW_FALLBACK_V1_EVIDENCE.md`

Runtime:
- GitHub Actions run: `36423150747`
- Source commit: `ca8f5b046794010e65d6dac31ed0b7f9af9121c4`
- Artifact ID: `10970109127`
- Artifact SHA256: `3f88ec08424b1093f8f1879b280e11ea06c89a99d4aa4bcfdafad05b860a7717`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Frozen primary results

### LAST_1Y
- return: +92.5%
- median DD: -39.6%
- median daily largest-asset share: 56.4%
- median peak concentration: 82.4%
- physical 3-in-1: 0%

### LAST_2Y
- return: +1191.4%
- median DD: -58.3%
- median daily largest-asset share: 85.9%
- median peak concentration: 95.7%
- physical 3-in-1: 0%

### MATURE
- return: +2223.8%
- worst start-triplet return: +1867.4%
- median DD: -57.1%
- worst DD: -58.5%
- median daily largest-asset share: 80.5%
- median peak concentration: 95.1%
- physical 3-in-1: 0%
- physical 2+1: 98.7%
- days above 50% largest-asset value share: 98.7%
- days at/above 2/3 largest-asset value share: 77.2%

## Rolling evidence

MAX2:

### 12m
- 23 windows
- median window return: +101.9%
- worst median-return window: +4.6%
- positive windows: 100%
- median DD: -44.5%
- median peak concentration: 80.7%

### 24m
- 11 windows
- median window return: +409.0%
- worst median-return window: +217.7%
- positive windows: 100%
- median DD: -51.6%
- median peak concentration: 91.5%

## Comparison spectrum

### V2_FREE
MATURE:
- return +3191.5%
- DD -62.4%
- typical / peak concentration effectively 100%
- full 3-in-1 convergence dominates.

### V4_MAX2_SHADOW_FALLBACK
MATURE:
- return +2223.8%
- DD -57.1%
- median daily largest share 80.5%
- peak concentration 95.1%
- deliberate 3-in-1 eliminated.

### V3_SHADOW_DEFENSIVE_FALLBACK / MAX1 physical
MATURE:
- return +673.9%
- DD -45.3%
- median daily largest share 43.6%
- peak concentration 76.5%
- physical collisions eliminated.

No version dominates return, drawdown, and concentration simultaneously.

## Decision

Status:

`VIABLE_RESEARCH_ARCHITECTURE / NOT_PRODUCTION_APPROVED`

MAX2 is the documented middle point between unconstrained consensus and fully distinct physical books.

It does what the literal occupancy rule requires:
- never puts all three physical books into one token;
- preserves much more historical upside than MAX1;
- improves DD versus V2 in every primary horizon tested.

But it is **not** a strong economic-diversification solution:
- MATURE median daily largest-asset share is still 80.5%;
- >50% concentration exists almost every day;
- peaks approach 95%+ because two consensus books can outperform the defensive third book.

Therefore:

`NO_3_IN_1 != HARD_DIVERSIFICATION`

## TRX finding

MATURE defensive parking by duration:
- TRX: 96.6%
- XRP: 3.1%
- BNB: 0.3%

TRX again emerges naturally as the dominant low-volatility parking token without being hardcoded.

## No-repeat rule

Do not rerun this exact MAX2 experiment on the same frozen U10 and same historical period merely to reproduce it.

A new run requires:
- new unseen data;
- documented semantic correction;
- changed costs;
- changed universe;
- changed portfolio constraint;
- or a separately preregistered forward/confirmatory protocol.

## Production boundary

No live/paper/Telegram/exchange behavior changes are authorized by this result.

A future production implementation would introduce persistent dual state and therefore requires:
- separate implementation mode;
- MIGRATION_PLAN;
- rollback;
- explicit actual/shadow state contract;
- forward/paper validation before real-money promotion.
