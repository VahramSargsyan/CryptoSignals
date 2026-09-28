# Relative Rotation Multibook Diversification Decision Log V1

> **Scope correction — 2026-09-28:** The V3 rejection in this file applies only to the exact `V3_COLLISION_GUARD / STRICT_STAY` implementation tested here. It must not be generalized to a dual-state shadow/defensive architecture. A later materially different test, `V3_SHADOW_DEFENSIVE_FALLBACK`, keeps the U10 core route alive in `shadow_core_asset` while physical capital parks in low-volatility crypto. That later architecture is classified `VIABLE_RESEARCH_ARCHITECTURE / NOT_PRODUCTION_APPROVED`. Read `docs/evidence/2026-09-28_RELATIVE_ROTATION_MULTIBOOK_SHADOW_DEFENSIVE_DECISION_LOG_V1.md` before making any multibook conclusion.

Date: 2026-09-28  
Status: ACTIVE RESEARCH MEMORY  
Production change: NONE

## Frozen subject

Universe:

`RR_TARGET_U10_CANDIDATE_HBAR_V1`

Tested portfolio architecture:

- three equal independent books;
- all 120 distinct 3-of-10 starting triplets;
- V2_FREE: collisions allowed;
- V3_COLLISION_GUARD: final holdings must remain distinct.

Canonical evidence:

`research/relative_rotation/2026-09-28_RR_U10_MULTIBOOK_DIVERSIFICATION_V1_EVIDENCE.md`

Runtime:

- GitHub Actions run: `36418004180`
- Source commit: `2d5af0de54085260dd36843fa83f699568a73f91`
- Artifact ID: `10967857929`
- Artifact SHA256: `c8ce913dd2d54caf0c8e57f9bba69d9c08ac112d075817bc93af534a3669ea06`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Frozen conclusions

### V2_FREE

`REJECTED_AS_DIVERSIFICATION_MECHANISM`

Why:

- 1Y median full convergence: 88.2% of days
- 2Y median full convergence: 94.5%
- MATURE median full convergence: 96.5%
- median peak concentration: 100%

Three nominally independent books rapidly converge to the same relative-rotation leader, so V2 does not solve the user's concentration objective.

### V3_COLLISION_GUARD

`REJECTED_IN_CURRENT_STRICT_FORM`

Why:

It eliminates deliberate collision, but the historical performance sacrifice is too large and long-window drawdown does not consistently improve.

Primary median returns:

- 1Y: V2 +135.3% vs V3 +14.7%
- 2Y: V2 +1864.2% vs V3 +116.6%
- MATURE: V2 +3191.5% vs V3 +264.4%

Median max drawdown:

- 1Y: V2 -62.4% vs V3 -52.0%
- 2Y: V2 -62.4% vs V3 -66.1%
- MATURE: V2 -62.4% vs V3 -68.8%

Rolling positive-window rate:

- 12m: V2 100% vs V3 56.5%
- 24m: V2 100% vs V3 54.5%

## Mechanism finding

The strategy's historical paths show very strong convergence from different starting states toward the same asset.

That convergence appears to be part of the historical return mechanism.

Therefore:

- allowing unlimited convergence does not diversify;
- forbidding all convergence damages the mechanism too aggressively.

This is historical mechanism evidence, not a forecast.

## Future research candidate

Do not rerun V2/V3.

If diversification research continues, the next distinct hypothesis may be:

`MAX_2_OF_3_BOOKS_PER_ASSET`

Meaning:

- two books may follow consensus into the same asset;
- the third book may not join them;
- deliberate 100% convergence is prevented;
- partial consensus is preserved.

Status:

`IDEA_NOT_TESTED`

It must receive its own preregistration before execution.

## Hard percentage-cap note

A true hard 40%/50% capital-value cap cannot be implemented cleanly with three indivisible independent books without adding cross-book rebalancing or partial transfers.

The completed V3 test therefore used an occupancy collision guard and separately measured actual value concentration.

Passive relative performance still allowed one book to exceed 50% of portfolio value.

Any future hard-value-cap test is a materially different portfolio strategy and must be preregistered separately.

## No-repeat rule

Do not rerun the exact V2_FREE vs V3_COLLISION_GUARD experiment on the same frozen U10 and same historical data.

A repeat requires a documented material difference:

- unseen data;
- engine correction;
- changed costs;
- changed universe;
- changed portfolio constraint;
- confirmatory/forward protocol.

## Production boundary

No live/paper/Telegram change is authorized or implied by this decision log.
