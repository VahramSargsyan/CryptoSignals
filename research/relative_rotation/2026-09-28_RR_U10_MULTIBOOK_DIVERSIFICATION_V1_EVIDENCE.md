# RR U10 MULTIBOOK DIVERSIFICATION V1 — EVIDENCE

Date: 2026-09-28  
Workflow mode: STRESS_TEST_ONLY  
Production/live changes: NONE

## Runtime identity

- Frozen universe: `RR_TARGET_U10_CANDIDATE_HBAR_V1`
- Assets: TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR
- GitHub Actions run: `36418004180`
- Source commit: `2d5af0de54085260dd36843fa83f699568a73f91`
- Artifact ID: `10967857929`
- Artifact: `rr-u10-multibook-diversification-v1-1`
- Artifact SHA256: `c8ce913dd2d54caf0c8e57f9bba69d9c08ac112d075817bc93af534a3669ea06`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Data / mechanics

- Binance Spot D1
- Common panel: 1,241 rows
- Start: 2023-05-05
- End: 2026-09-26
- Lookback: 180 days
- ARM: 15%
- Reversal: 3%
- Transition cost: 0.1%
- Confirmed signal at T -> next-day open
- Three independent books
- Equal initial allocation: 1/3 each
- All 120 distinct 3-of-10 starting triplets evaluated

## Tested architectures

### V2_FREE

Three independent books. Each book follows the strongest eligible relative-rotation signal. Books are allowed to converge into the same asset.

### V3_COLLISION_GUARD

Same three independent books, but final holdings after a simultaneous transition must be three distinct assets.

When conflicts exist, the feasible assignment with the greatest sum of confirmed max_dislocation is selected. Ties prefer more executed transitions, then deterministic ordering. A book may take an alternate confirmed destination or stay in place.

No cross-book daily capital rebalancing was introduced.

## Primary results

| Window | Architecture | Median return | Worst start triplet | Median DD | Worst DD | Median peak concentration | Median collision days | Median full convergence days |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1Y | V2_FREE | +135.3% | +24.5% | -62.4% | -73.6% | 100.0% | 98.4% | 88.2% |
| 1Y | V3_COLLISION_GUARD | +14.7% | +5.1% | -52.0% | -56.1% | 65.3% | 0.0% | 0.0% |
| 2Y | V2_FREE | +1864.2% | +1696.2% | -62.4% | -62.4% | 100.0% | 98.9% | 94.5% |
| 2Y | V3_COLLISION_GUARD | +116.6% | +97.8% | -66.1% | -66.8% | 51.8% | 0.0% | 0.0% |
| MATURE | V2_FREE | +3191.5% | +2708.3% | -62.4% | -62.4% | 100.0% | 98.7% | 96.5% |
| MATURE | V3_COLLISION_GUARD | +264.4% | +208.0% | -68.8% | -69.8% | 75.6% | 0.0% | 0.0% |

## What V2 actually does

V2 was intended to create independent paths, but the paths almost immediately collapse onto the same relative-rotation leader.

Median behavior:

- 1Y collision days: 98.4%
- 1Y full three-book convergence: 88.2%
- 2Y collision days: 98.9%
- 2Y full convergence: 94.5%
- MATURE collision days: 98.7%
- MATURE full convergence: 96.5%

Median daily largest-asset share is 100% in all three primary windows.

Therefore V2 is **not a practical diversification architecture** for this strategy. The shared signal graph causes initially independent books to converge.

## What V3 fixes

V3 completely removes deliberate book collisions:

- collision-day share: 0% across every primary window;
- full-convergence share: 0%;
- no two books intentionally finish a transition in the same asset.

Concentration is materially lower, although passive relative performance can still make one book dominate total value because no cross-book capital rebalance exists.

Median peak largest-asset share:

- 1Y: 65.3%
- 2Y: 51.8%
- MATURE: 75.6%

Worst observed peak across start triplets:

- 1Y: 68.5%
- 2Y: 61.3%
- MATURE: 83.4%

This proves an occupancy/collision guard does not itself guarantee a hard value-weight cap after price drift.

## Cost of V3

The diversification guard materially changes the strategy path.

Median guard effects:

### 1Y
- guard intervention days: 51
- conflicted book decisions: 64
- alternative-destination executions: 4.5
- forced stays: 59
- transitions: 19 vs 35 in V2

### 2Y
- guard intervention days: 106
- conflicted book decisions: 120
- alternative moves: 14
- forced stays: 107
- transitions: 36 vs 61 in V2

### MATURE
- guard intervention days: 133
- conflicted book decisions: 152
- alternative moves: 16
- forced stays: 136
- transitions: 46 vs 77 in V2

Most conflicts therefore become **forced stays**, not successful alternate routes. This is a major mechanism behind the performance loss.

## Return / risk trade-off

V3 minus V2 median-return delta:

- 1Y: -120.7 percentage points
- 2Y: -1747.6 percentage points
- MATURE: -2927.1 percentage points

Median DD:

- 1Y improves by about 10.4 pp (-62.4% -> -52.0%)
- 2Y worsens by about 3.7 pp (-62.4% -> -66.1%)
- MATURE worsens by about 6.4 pp (-62.4% -> -68.8%)

So the strict diversification rule does **not** produce a stable return-for-risk improvement. It lowers concentration, but on longer windows it also lowers return dramatically and worsens drawdown.

## Rolling checks

| Window | Architecture | Count | Median window return | Worst median-return window | Positive windows | Median concentration peak | Median collision days |
|---|---|---:|---:|---:|---:|---:|---:|
| 12m | V2_FREE | 23 | +148.2% | +9.5% | 100% | 100.0% | 99.2% |
| 12m | V3_COLLISION_GUARD | 23 | +23.9% | -36.8% | 56.5% | 50.8% | 0.0% |
| 24m | V2_FREE | 11 | +558.2% | +262.4% | 100% | 100.0% | 99.6% |
| 24m | V3_COLLISION_GUARD | 11 | +32.7% | -26.0% | 54.5% | 52.6% | 0.0% |

Temporal evidence reinforces the primary result: V3 solves collision but is not robust enough in its current strict form.

## Research classification

### V2_FREE

Status:

`REJECTED_AS_DIVERSIFICATION_MECHANISM`

Reason:

Independent books rapidly converge onto the same asset. It behaves much closer to one concentrated rotation path than to three diversified strategies.

### V3_COLLISION_GUARD

Status:

`REJECTED_IN_CURRENT_STRICT_FORM`

Reason:

It achieves the diversification objective mechanically, but historical return degradation is very large and long-window drawdown does not consistently improve.

This does **not** reject portfolio diversification as a concept. It rejects this exact rule: "all three books must always hold three distinct assets."

## Important structural finding

The relative-rotation strategy appears to derive a large part of its historical performance from **consensus/convergence**: different starting states are pulled toward the same strongest node.

Therefore forcing complete path separation removes part of the mechanism that generated the historical result.

This is a mechanism finding, not proof about future returns.

## Future hypothesis — not tested here

A less restrictive architecture could allow:

`maximum 2 of 3 books in the same asset`

This would prevent 100% deliberate convergence while preserving some consensus behavior.

It is only a future hypothesis. It was **not** tested in this pass because the requested scope was V2 and V3 only.

A separate preregistration is required before testing it.

## No-repeat rule

Do not rerun the identical V2_FREE vs V3_COLLISION_GUARD experiment on the same frozen U10 and same data merely to reproduce this result.

A rerun is justified only by:

1. materially new unseen market data;
2. a documented engine/semantic correction;
3. materially different cost assumptions;
4. a changed frozen universe;
5. a changed portfolio rule;
6. a separately preregistered confirmatory/forward-validation protocol.

## Production boundary

- Current U9 paper/live target: unchanged.
- Frozen U10 candidate: unchanged.
- Telegram: unchanged.
- No automated trading change.
- No production promotion is implied.

## Residual risks

- This is historical evidence.
- V3 uses an occupancy collision guard, not cross-book value rebalancing.
- Passive book outperformance can still produce >50% value concentration.
- Costs use a fixed 0.1% transition assumption.
- Common U10 history begins in 2023.
