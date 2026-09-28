# Relative Rotation Multibook MAX2 Shadow Decision Log V1

Date: 2026-09-28  
Status: ACTIVE RESEARCH MEMORY  
Workflow mode: STRESS_TEST_ONLY  
Production change: NONE

## Frozen universe

`RR_TARGET_U10_CANDIDATE_HBAR_V1`

## Tested candidate

`V4_MAX2_SHADOW_FALLBACK`

Rule:

`MAX_2_OF_3_BOOKS_PER_ASSET`

Meaning:
- up to two physical books may follow the same U10 consensus token;
- the third physical book may not join them;
- the third book preserves the same `shadow_core_asset` but parks in the lowest-volatility feasible U10 token;
- when full 3-in-1 pressure disappears, it resynchronizes to shadow.

TRX is not hardcoded.

Canonical evidence:

`research/relative_rotation/2026-09-28_RR_U10_MULTIBOOK_MAX2_SHADOW_FALLBACK_V1_EVIDENCE.md`

Runtime:
- GitHub Actions run: `36423150747`
- Source commit: `ca8f5b046794010e65d6dac31ed0b7f9af9121c4`
- Artifact ID: `10970109127`
- Artifact SHA256: `3f88ec08424b1093f8f1879b280e11ea06c89a99d4aa4bcfdafad05b860a7717`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Classification

`VIABLE_RESEARCH_ARCHITECTURE / NOT_PRODUCTION_APPROVED`

## Frozen MATURE result

V2_FREE:
- median return: +3191.5%
- median max DD: -62.4%
- median daily largest asset: 100%
- median peak concentration: 100%
- full 3-in-1 physical convergence: 96.5% of days

MAX2:
- median return: +2223.8%
- worst start-triplet return: +1867.4%
- median max DD: -57.1%
- worst max DD: -58.5%
- median daily largest asset: 80.5%
- median peak concentration: 95.1%
- worst peak concentration: 96.3%
- full 3-in-1 physical convergence: 0%
- 2+1 physical structure: 98.7% of days
- largest asset >50% of value: 98.7% of days
- largest asset >=2/3 of value: 77.2% of days

Prior correct MAX1 shadow:
- median return: +673.9%
- median max DD: -45.3%
- median daily largest asset: 43.6%
- median peak concentration: 76.5%
- full physical convergence: 0%

## Main interpretation

MAX2 does exactly what it was designed to do structurally:

`prevent literal 3-of-3 occupancy while preserving two-book consensus`

It preserves much more historical upside than MAX1 shadow.

However, it does **not** create strong value diversification:
- two books occupy the consensus leader on almost every day;
- the leader holds about 80.5% of portfolio value on a median MATURE day;
- historical peak concentration still reaches about 95%.

Therefore MAX2 is a return-preserving concentration compromise, not a hard diversification solution.

## Rolling evidence

MAX2:
- rolling 12m median return: +101.9%
- worst 12m median-return window: +4.6%
- positive 12m median windows: 100%
- rolling 12m median DD: -44.5%

- rolling 24m median return: +409.0%
- worst 24m median-return window: +217.7%
- positive 24m median windows: 100%
- rolling 24m median DD: -51.6%

## Defensive parking

TRX naturally dominates defensive duration.

MATURE:
- TRX: 96.6% of aggregate parking book-days
- XRP: 3.1%
- BNB: 0.3%

Again, the canonical rule is lowest-volatility feasible token, not fixed TRX.

## Architecture frontier

The repo now has three materially different portfolio behaviors:

### V2_FREE
- strongest historical upside
- almost complete 3-in-1 convergence
- no diversification objective satisfied

### V4_MAX2_SHADOW_FALLBACK
- preserves substantial upside
- prevents literal 3-in-1 occupancy
- still highly concentrated by value

### V3_MAX1_SHADOW
- strongest tested concentration/DD reduction
- much larger upside sacrifice

No architecture is promoted automatically.

## No-repeat rule

Do not rerun the exact MAX2 experiment on the same frozen U10/history.

A repeat requires materially new:
- unseen data;
- semantic correction;
- cost model;
- universe;
- portfolio rule;
- or confirmatory/forward protocol.

## Production boundary

No live/paper/Telegram/exchange behavior changes.

A future production implementation requires a separate implementation mode and MIGRATION_PLAN because persistent `actual_asset` and `shadow_core_asset` state would be added.
