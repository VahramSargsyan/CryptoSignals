# Relative Rotation Multibook Shadow Defensive Decision Log V1

Date: 2026-09-28  
Status: ACTIVE RESEARCH MEMORY  
Workflow mode: STRESS_TEST_ONLY  
Production change: NONE

## Frozen universe

`RR_TARGET_U10_CANDIDATE_HBAR_V1`

## Tested architecture

`V3_SHADOW_DEFENSIVE_FALLBACK`

Three equal capital books maintain two states:

- `shadow_core_asset`: continues the ordinary unconstrained U10 route;
- `actual_asset`: physically carries backtest capital.

When shadow routes collide:
- one physical book may occupy the shared shadow target;
- other books park in low-volatility feasible U10 tokens;
- already parked defensive tokens remain frozen while feasible;
- books resynchronize when their shadow target becomes physically available.

TRX is not hardcoded.

Canonical evidence:

`research/relative_rotation/2026-09-28_RR_U10_MULTIBOOK_SHADOW_DEFENSIVE_FALLBACK_V1_EVIDENCE.md`

Runtime:
- GitHub Actions run: `36421236295`
- Source commit: `c9e9e91e4e2ce0cbe3ff99ceb76b310f0eb320a0`
- Artifact ID: `10969172637`
- Artifact SHA256: `39dd7e3d588599c624100d0f70055c6bc88c664d1fd6c45bf5ff834b3dcb67b5`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Decision

Status:

`VIABLE_RESEARCH_ARCHITECTURE / NOT_PRODUCTION_APPROVED`

The corrected shadow architecture is materially different from the previously rejected strict-stay collision guard.

It should not inherit the old strict-V3 rejection.

## Key evidence

### MATURE

Unconstrained V2:
- median return: +3191.5%
- median max DD: -62.4%
- median peak concentration: 100%

Shadow/defensive:
- median return: +673.9%
- worst start-triplet return: +375.5%
- median max DD: -45.3%
- worst max DD: -52.7%
- median peak concentration: 76.5%
- median daily largest-asset share: 43.6%
- days above 50%: 25.9%
- actual collision days: 0%
- shadow collision days: 98.7%

### Rolling 24m

Shadow/defensive:
- median window return: +240.0%
- worst median-return window: +138.6%
- positive windows: 100%
- median window DD: -44.5%
- median peak concentration: 68.1%

## TRX finding

TRX emerged naturally as the dominant defensive parking token by duration.

MATURE parking duration:
- TRX: 49.5%
- BNB: 18.5%
- XRP: 17.2%
- TWT: 9.4%
- HBAR: 5.3%

This supports the user's remembered idea of TRX as a practical crypto substitute for USDT, but the canonical mechanism is:

`lowest-volatility feasible defensive token`

not:

`always TRX`.

## Important interpretation

The U10 core strongly converges regardless of starting state.

With shadow routing:
- one book usually follows the converged U10 leader physically;
- the other books preserve their shadow/core state but carry capital in defensive low-vol tokens.

This keeps the strategy's logical route alive without forcing all physical capital into the same token.

## Why this is not yet production-approved

The architecture remains historical research because:
- no true forward period exists yet;
- it was evaluated on known data;
- return remains materially below unconstrained V2;
- physical distinctness does not guarantee a hard <=50% value concentration;
- no production state schema exists yet for explicit `actual_asset` and `shadow_core_asset`.

Any production implementation would therefore require a separate PATCH_FIX or BUILD step and, because it adds persistent dual state, a MIGRATION_PLAN + rollback.

## Prior strict V3 scope correction

The earlier decision log:

`docs/evidence/2026-09-28_RELATIVE_ROTATION_MULTIBOOK_DIVERSIFICATION_DECISION_LOG_V1.md`

remains correct only for:

`V3_COLLISION_GUARD / STRICT_STAY`

Its rejection must not be generalized to this shadow/defensive architecture.

## MAX_2_OF_3

`MAX_2_OF_3_BOOKS_PER_ASSET`

remains:

`IDEA_NOT_TESTED`

It is a separate hypothesis that may preserve more upside while allowing up to two books to converge. It is not required merely because shadow-fallback failed; shadow-fallback did not fail mechanically.

## No-repeat rule

Do not repeat this exact test on the same U10/history.

A new run requires:
- unseen data;
- documented semantic correction;
- changed costs;
- changed universe;
- changed portfolio rule;
- or a separate confirmatory/forward-validation protocol.

## Production boundary

No paper/live, Telegram, exchange, or real-money execution behavior is changed by this research decision.
