# Recovery Trajectory Model V1 — Diagnostic Result

Date: 2026-09-27
Workflow mode: DIAGNOSTIC_ONLY
Status: RESEARCH_ONLY / DIRECT_TRAJECTORY_FEATURE_REJECTED / NO TRADING

## Objective

Test the minimal trajectory model proposed after the false-vs-true recovery diagnostic.

Baseline:

`BREADTH + MEDIAN_SMA200_GAP`

Candidate:

`BREADTH + MEDIAN_SMA200_GAP + 7D_CHANGE_IN_MEDIAN_SMA200_GAP`

No other features were added.

No threshold grid, persistence search, feature search, regularization search, or trading mapping was performed.

## Predeclared acceptance gate

The trajectory candidate would be considered incrementally useful only if BOTH were true:

1. lower episode-balanced mean Brier across the 7d/14d horizons than the reproduced Breadth+Gap baseline;
2. lower mean Brier than baseline in at least 2 of 4 held-out development stress episodes.

The first condition is mandatory; per-episode wins alone cannot promote the candidate.

## Reproduction gate

The existing Breadth+Gap model was reproduced to machine precision using the same:

- prior-completed-episode walk-forward;
- confirmed recovery close excluded;
- episode-balanced training weights;
- training-only standardization;
- L2 logistic regression;
- fixed `C=1`.

Result:

`BASELINE_REPRODUCTION_GATE = PASS`

## Canonical data

Reused from existing successful Strategy Lab data run:

- GitHub Actions run: `36299600335`
- artifact: `10924993695`
- universe: ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK
- timeframe: 1D
- last fully closed candle: 2026-09-26

Stress reconstruction exactly reproduced the prior V1/V2 canonical episodes.

## Development walk-forward

Held-out episodes:

- STRESS_004 — 30d
- STRESS_005 — 11d
- STRESS_006 — 4d
- STRESS_007 — 141d

Scored horizons:

- 7d
- 14d

### Episode-balanced Brier

Lower is better.

| Model | 7d | 14d | Mean 7d/14d |
|---|---:|---:|---:|
| Breadth+Gap baseline | **0.2104** | **0.2066** | **0.2085** |
| + Gap Δ7d trajectory | 0.2231 | 0.2158 | **0.2194** |

Relative result:

Trajectory candidate worsened episode-balanced short-horizon Brier by approximately **5.2%**.

### State-weighted Brier

| Model | Mean Brier |
|---|---:|
| Breadth+Gap baseline | **0.1827** |
| + Gap Δ7d trajectory | 0.1894 |

The candidate is also worse under state weighting.

## Per-episode mean Brier across 7d/14d

| Episode | Breadth+Gap | + Gap Δ7d | Better |
|---|---:|---:|---|
| STRESS_004 | 0.1967 | **0.1807** | Trajectory |
| STRESS_005 | 0.2558 | **0.2545** | Trajectory |
| STRESS_006 | **0.2083** | 0.2583 | Baseline |
| STRESS_007 | **0.1733** | 0.1842 | Baseline |

The candidate wins 2/4 episodes, satisfying the secondary per-episode condition.

However the mandatory aggregate episode-balanced condition FAILS.

Formal verdict:

`DIRECT_TRAJECTORY_FEATURE_REJECTED_V1`

## Diagnostic 50% alert audit

The added trajectory feature did NOT change the first >=50% alert date in any held-out development episode.

The same critical false-early alerts remained:

### STRESS_007

7d:
- first alert: 2025-03-02
- actual remaining: 137d
- false early

14d:
- first alert: 2025-02-26
- actual remaining: 141d
- false early

The candidate therefore does not solve the failure mode that motivated it.

## Why the descriptive trajectory finding did not become a better logistic model

The previous false-vs-true diagnostic found that true recovery states often have a much stronger 7-day improvement in median SMA200 gap.

That finding remains valid descriptively.

But directly adding `gap_delta_7d` to the small logistic model does not improve out-of-episode probability calibration.

Plausible reasons consistent with the evidence:

1. `current median SMA200 gap` already contains much of the trajectory information accumulated over recent days;
2. the extra feature adds variance in a sample with only a few independent episodes;
3. the relationship between gap trajectory and recovery may be nonlinear / conditional rather than additive-linear;
4. the trajectory signal is strongest within selected false-vs-true alert states, not necessarily across every daily stress state used by the probability model.

These are interpretations, not separately proven mechanisms.

## Post-hoc 2026 replay

Label:

`POST_HOC_2026_DIAGNOSTIC_NOT_VALIDATION`

Period:

`2026-03-29 -> 2026-08-21`

### Short-horizon mean Brier

| Model | 7d | 14d | Mean |
|---|---:|---:|---:|
| Breadth+Gap baseline | **0.0324** | **0.1143** | **0.0734** |
| + Gap Δ7d trajectory | 0.0341 | 0.1212 | 0.0777 |

The candidate is again slightly worse.

### First >=50% alert dates

Unchanged versus baseline:

7d:
- 2026-08-19
- actual remaining: 3d

14d:
- 2026-08-13
- actual remaining: 9d

Therefore the trajectory feature provides no compensating post-hoc timing benefit.

## Coefficient sanity check on the 2026 training set

Standardized candidate coefficients remained directionally coherent but the trajectory coefficient was relatively small.

7d:
- breadth: ~+0.473
- current median gap: ~+0.427
- gap Δ7d: ~+0.123

14d:
- breadth: ~+0.262
- current median gap: ~+0.441
- gap Δ7d: ~+0.081

This is consistent with the possibility that current median gap already captures most of the useful continuous recovery state.

## Main conclusion

The trajectory hypothesis should be split into two statements:

SUPPORTED DESCRIPTIVELY:

`TRUE RECOVERY ALERTS OFTEN HAVE STRONGER RECENT SMA-GAP IMPROVEMENT THAN FALSE ALERTS`

NOT SUPPORTED AS A DIRECT MODEL EXTENSION:

`ADDING GAP_DELTA_7D AS A THIRD LINEAR LOGISTIC FEATURE IMPROVES RECOVERY FORECASTING`

Therefore:

`TRAJECTORY_MECHANISM_PRESENT / DIRECT_LINEAR_FEATURE_ADDITION_REJECTED`

Do not add the feature to the recovery model.

## What should NOT happen next

Do not immediately try:

- gap Δ3d vs Δ5d vs Δ10d;
- acceleration of acceleration;
- polynomial transforms;
- interaction grids;
- regularization tuning;
- trajectory thresholds selected from these episodes.

That would convert the same small sample into a feature-engineering optimizer.

## Research implication

The current supported recovery architecture remains:

- V2 survival / OOS for duration context;
- Breadth + current Median SMA200 Gap for 7d/14d recovery evidence;
- no confirm3;
- no direct gap-trajectory feature in the probability model.

The next justified move should focus on obtaining additional independent episodes / forward evidence, or on testing a qualitatively different confirmation mechanism with strong prior justification rather than further feature tuning.

## Runtime impact

Production code changed: NONE
Paper-live code changed: NONE
Strategy parameters changed: NONE
Schema changed: NONE
Migration required: NO

## Test level

`TEST_LEVEL: LOCAL_READ_ONLY_DIAGNOSTIC + CANONICAL_BINANCE_DATA_REUSE + BASELINE_REPRODUCTION_GATE + EPISODE_BALANCED_TRAJECTORY_MODEL_COMPARATOR + FALSE_ALERT_AUDIT + POST_HOC_2026_REPLAY`

## Residual risks

- only four independently held-out development crises;
- daily states are highly correlated inside each episode;
- the trajectory feature family was motivated after prior diagnostics, so multiple-hypothesis risk exists;
- the long STRESS_007 materially influences state-level results;
- 2026 is post-hoc;
- no future clean crisis has validated the current Breadth+Gap recovery architecture.
