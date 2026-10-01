# RR Fractional Recapture Grid + 20% Robustness — Decision Log V1

Date: 2026-10-01
Status: ACTIVE RESEARCH MEMORY
Workflow mode: STRESS_TEST_ONLY
Production change: NONE

Canonical evidence:
`research/relative_rotation/2026-10-01_RR_FRACTIONAL_RECAPTURE_GRID_20PCT_V1_EVIDENCE.md`

Runtime:

Grid:
- run `36865207170`
- commit `e6aaf65253cbfd91b5a911a743d8625b2ac528bb`
- artifact `11163527006`
- SHA256 `6f5c8419923748a7b37e0ba38e5faa5f0bd8aba6375a46af3765cc15c39cfcc1`

Robustness:
- run `36866188244`
- commit `2cbe09bacfd6a41bb4b4c627fd3fd0c100c7450b`
- artifact `11163822298`
- SHA256 `8b600aa72a086d0e3c14a791c04b5f64e1f455e0c43749c44f50676ba8e25003`

TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Frozen findings

### 60% user example

60% recapture does not improve the full strategy under 3/6/12/24h fallbacks.

Classification:
`60PCT = REJECTED`

### 20% family

20% / 3h, 6h and 12h all beat canonical on the three endpoint windows.

20% / 6h:
- Validation 1Y: +198.9% vs +188.7%
- Last 2Y: +2246.1% vs +2102.3%
- MATURE: +3848.9% vs +3588.4%
- MATURE DD: -61.5% vs -62.4%
- final capital vs canonical: 1.0706x
- final capital vs two-fee next-open: 1.0961x

### Rolling robustness

20% / 6h:
- 12m windows: 23; median factor 1.048x; 73.9% wins; worst 0.963x
- 24m windows: 11; median factor 1.077x; 100% wins; worst 1.051x

Across 110 24m start-asset/window observations:
- 100% beat canonical
- worst factor 1.026x

### Execution

20% / 6h:
- MATURE direct episodes: 28
- full-limit completion: 82.1%
- median completion: 2.5h
- median realized exchange improvement: +0.50%
- bootstrap 95% interval for median improvement: +0.07% to +1.21%

## Interpretation

The historical evidence supports a **short-timeout small-recoup execution family**, not aggressive recapture.

The useful region is:
- small fraction;
- short timeout;
- preserve next daily RR state.

The result does not authorize live execution because 20% was selected from the historical grid.

## Classification

`20PCT_SHORT_TIMEOUT = VIABLE_RESEARCH_CANDIDATE`

`20PCT_6H = FORWARD_SHADOW_CANDIDATE`

`PRODUCTION_APPROVAL = NO`

## No-repeat / anti-overfit

Do not optimize 15/20/25% or 4/6/8h on the same history before freezing a forward benchmark.

## Production boundary

No live/paper/Telegram/exchange/order changes authorized.
