# Recovery Evidence Feature Ablation V1 — Diagnostic Result

Date: 2026-09-27
Workflow mode: DIAGNOSTIC_ONLY
Status: RESEARCH_ONLY / NO TRADING / NO STRATEGY CODE CHANGED

## Question

Does the state-conditioned recovery model learn information beyond the frozen market-breadth signal itself?

Frozen feature families:

1. BREADTH_ONLY
   - breadth_sma200

2. SMA_GAP_ONLY
   - median_sma200_gap

3. BREADTH_PLUS_GAP
   - breadth_sma200
   - median_sma200_gap

4. FULL_STATE
   - breadth_sma200
   - episode_age_days
   - breadth_delta_7d
   - median_sma200_gap
   - dispersion_sma200_gap
   - median_vol30

No feature search was performed after results were visible.

## Predeclared decision rule

FULL_STATE may be called incrementally informative beyond BREADTH_ONLY only if BOTH are true:

1. FULL_STATE has lower episode-balanced mean Brier than BREADTH_ONLY.
2. FULL_STATE beats BREADTH_ONLY in at least 2 of 4 held-out development stress episodes.

## Data and semantics

Canonical D1 Binance data reused from the successful existing Strategy Lab data run:

- run: `36299600335`
- artifact: `10924993695`
- data request commit: `6c4ac54f6b19a6984e1c8ee72d028e05c4466b5e`

Universe:

`ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK`

Stress reconstruction exactly matched the prior V1/V2 episode dates.

Confirmed recovery close itself was excluded from both training and scoring.

This forces the models to estimate approaching recovery before the frozen `breadth >= 5 for 3 closes` rule has already declared the episode finished.

## Model formulation

Read-only local diagnostic.

For each recovery horizon:

`7 / 14 / 30 / 45 / 60 days`

a fixed L2-regularized logistic classifier was trained only on prior completed stress episodes.

No hyperparameter tuning was performed.

Training rows were episode-balanced:

each completed training episode contributed total weight 1 regardless of duration.

All features were standardized using the training sample only.

The current held-out episode was never used for training.

## Development walk-forward

Development boundary:

`data <= 2026-03-28`

Held-out episodes:

- STRESS_004: 30d
- STRESS_005: 11d
- STRESS_006: 4d
- STRESS_007: 141d

Scored pre-recovery daily states:

`186`

## Aggregate scores

### State-weighted mean Brier

| Feature set | Mean Brier |
|---|---:|
| BREADTH_PLUS_GAP | **0.3790** |
| SMA_GAP_ONLY | 0.3831 |
| BREADTH_ONLY | 0.3958 |
| FULL_STATE | **0.4637** |

### Episode-balanced mean Brier

Each crisis receives equal weight.

| Feature set | Episode-balanced Brier |
|---|---:|
| BREADTH_PLUS_GAP | **0.1844** |
| SMA_GAP_ONLY | 0.1895 |
| BREADTH_ONLY | 0.1933 |
| FULL_STATE | **0.2217** |

Relative to BREADTH_ONLY:

- BREADTH_PLUS_GAP improves episode-balanced Brier by ~4.6%.
- FULL_STATE worsens episode-balanced Brier by ~14.7%.

## Per-episode mean Brier

| Episode | BREADTH_ONLY | SMA_GAP_ONLY | BREADTH_PLUS_GAP | FULL_STATE |
|---|---:|---:|---:|---:|
| STRESS_004 | 0.0825 | 0.0801 | 0.0789 | **0.0555** |
| STRESS_005 | 0.1112 | 0.1051 | **0.1024** | 0.1446 |
| STRESS_006 | 0.0862 | 0.0954 | **0.0834** | 0.1010 |
| STRESS_007 | 0.4934 | 0.4775 | **0.4728** | 0.5858 |

Wins versus BREADTH_ONLY:

- FULL_STATE: **1 / 4**
- SMA_GAP_ONLY: **3 / 4**
- BREADTH_PLUS_GAP: **4 / 4**

Therefore the preregistered FULL_STATE gate FAILS.

## By horizon — episode-balanced Brier

| Horizon | BREADTH_ONLY | SMA_GAP_ONLY | BREADTH_PLUS_GAP | FULL_STATE |
|---:|---:|---:|---:|---:|
| 7d | 0.2270 | 0.2320 | **0.2104** | 0.2715 |
| 14d | 0.2348 | 0.2105 | **0.2066** | 0.3297 |
| 30d | 0.1911 | 0.1913 | **0.1910** | 0.1935 |
| 45d | 0.1702 | 0.1702 | 0.1702 | 0.1702 |
| 60d | 0.1436 | 0.1436 | 0.1436 | 0.1436 |

The incremental value of SMA gap is concentrated mainly in the 7d and 14d recovery horizons.

Long 45d/60d horizons contain too little discriminating historical structure in this tiny episode sample for the feature sets to separate meaningfully.

## Interpretation

FULL_STATE does NOT show robust incremental information.

Adding:

- episode age,
- breadth slope,
- cross-sectional gap dispersion,
- median VOL30

hurts out-of-episode generalization in this sample.

The simplest supported recovery-evidence representation is:

`BREADTH + MEDIAN_SMA200_GAP`

This is materially more parsimonious than FULL_STATE and beats BREADTH_ONLY in all four held-out development episodes.

## Why median SMA200 gap can add information beyond breadth

Breadth is discrete:

`0 .. 8 assets above SMA200`

It does not encode how far the remaining assets are from their SMA200.

Median gap adds continuous distance information.

Example:

- breadth = 4/8 with median gap -12% is materially different from
- breadth = 4/8 with median gap +0.5%.

The second state is much closer to broad recovery even though the raw breadth count is identical.

This is the clearest supported incremental mechanism from the ablation.

## Post-hoc 2026 replay

Label:

`POST_HOC_2026_DIAGNOSTIC_NOT_SELECTION_DATA`

Period:

`2026-03-29 -> 2026-08-21`

Mean Brier:

| Feature set | 2026 mean Brier |
|---|---:|
| FULL_STATE | **0.2022** |
| BREADTH_PLUS_GAP | 0.2457 |
| SMA_GAP_ONLY | 0.2912 |
| BREADTH_ONLY | 0.3206 |

This post-hoc replay superficially favors FULL_STATE.

It MUST NOT override the development selection result because 2026 was already opened before this ablation.

The discrepancy is itself evidence that FULL_STATE may be specialized to the extreme long-regime structure rather than robust across independent episodes.

## 2026 timing illustration

First >=50% recovery probability:

### BREADTH_ONLY

- <=7d: 2026-08-20, actual remaining 2d
- <=14d: 2026-08-19, actual remaining 3d

### BREADTH_PLUS_GAP

- <=7d: 2026-08-19, actual remaining 3d
- <=14d: 2026-08-13, actual remaining 9d

### FULL_STATE

- <=7d: 2026-08-19, actual remaining 3d
- <=14d: 2026-08-19, actual remaining 3d

The BREADTH_PLUS_GAP model produced the earliest useful 14-day warning among the development-supported feature sets.

However this is post-hoc and not promotion evidence.

## Coefficient sanity check

On all completed development episodes, standardized BREADTH_PLUS_GAP coefficients were consistently positive:

- breadth: approximately +0.27 to +0.49 depending on horizon
- median SMA200 gap: approximately +0.40 to +0.46

This is directionally coherent:

more assets above SMA200 and a stronger median distance above SMA200 both increase estimated recovery probability.

No coefficient tuning or sign constraints were applied.

## Decision

`FULL_STATE_INCREMENTAL_SIGNAL = REJECTED_V1`

`BREADTH_PLUS_MEDIAN_SMA200_GAP = PROMISING_RECOVERY_EVIDENCE_CANDIDATE`

This is NOT a trading approval.

Do not connect the candidate to capital allocation yet.

The next justified diagnostic is to test whether the BREADTH_PLUS_GAP signal remains useful when combined with the existing V2 OOS / censor-aware survival guard, rather than replacing V2.

## Runtime impact

Production code changed: NONE
Paper-live code changed: NONE
Strategy parameters changed: NONE
Schema changed: NONE
Migration required: NO

## Test level

`TEST_LEVEL: LOCAL_READ_ONLY_DIAGNOSTIC + CANONICAL_BINANCE_DATA_REUSE + STRESS_SEMANTIC_REPRODUCTION_GATE + EPISODE_BALANCED_FEATURE_ABLATION + POST_HOC_2026_REPLAY`

## Residual risks

- only four independently held-out development crises;
- all daily observations within an episode are highly correlated;
- direct horizon logistic classifiers are a diagnostic approximation, not a censor-aware trading engine;
- horizons 45d/60d are weakly identified in the available sample;
- 2026 is post-hoc and cannot select the model;
- the recovery definition still depends on breadth, so some predictive power is structurally related to the target definition;
- no future clean stress episode has validated BREADTH_PLUS_GAP.
