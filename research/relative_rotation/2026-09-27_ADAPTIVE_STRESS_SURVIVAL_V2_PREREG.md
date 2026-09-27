# Adaptive Stress Survival V2 — Preregistered Diagnostic Plan

Date: 2026-09-27
Workflow mode: BUILD_NEW_APP
Status: RESEARCH_ONLY / SURVIVAL_DIAGNOSTIC / NO TRADING

## Objective

Replace V1's overconfident fixed remaining-days forecast with a censor-aware survival framework that estimates conditional recovery probabilities while explicitly refusing to trust extrapolation beyond historical duration support.

No capital-allocation or trading rule is authorized in this task.

## Background

V1 found weak but real state information in walk-forward history, yet failed badly on the 2025-11-01 -> 2026-08-22 stress episode:

- by 2026-03-29 the episode age was 148 days;
- the longest prior completed episode was 141 days;
- V1 still predicted a median of ~12 days remaining.

The primary V2 design requirement is therefore:

`OUT_OF_SUPPORT MUST BE EXPLICIT`

## OSS reuse

Adopt mature Python survival-analysis library:

- project: `CamDavidsonPilon/lifelines`
- release: `0.30.3`
- upstream tag commit: `a21e4328fa30bc107ae2a3e0276ec57e892e6504`
- license: MIT
- local dependency pin: `lifelines==0.30.3`

Adoption scope is limited to:
- `KaplanMeierFitter`
- `WeibullFitter`

No upstream source code is copied into this repository.

## Stress episode definition

Unchanged from V1:

- universe: ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK;
- breadth = count above causal SMA200;
- stress entry = 3 consecutive closes with breadth <= 3;
- confirmed recovery = 3 consecutive closes with breadth >= 5;
- episode starts on the third low-breadth close;
- episode ends on the third recovery close;
- unfinished episode at cutoff is right-censored.

## Forecast question

At each stress-state close with current episode age `a`:

`P(recovery within h days | episode has survived in stress through age a)`

for horizons:

`7, 14, 30, 45, 60` days.

## Training data at a test state

Causal expanding walk-forward:

1. all earlier completed stress episodes available before the current test episode began;
2. the current test episode itself may contribute ONE right-censored observation at its current age, because survival through today is known information;
3. its future end time is never exposed;
4. no future episode is used.

This is the specific censor-aware mechanism V2 adds.

## Models

### 1. KM_CENSORED_V2

Fit Kaplan-Meier to:

- durations of all prior completed episodes with `event_observed=1`;
- current episode age as one right-censored observation with `event_observed=0`.

Conditional probability:

`P(T <= a+h | T > a) = 1 - S(a+h) / S(a)`

when estimable.

If the current age exceeds the largest prior completed duration:

`duration_support = OUT_OF_SUPPORT`

The KM probabilities may still be emitted diagnostically, but the trusted forecast status is UNKNOWN.

### 2. WEIBULL_CENSORED_V2

Fit a 2-parameter Weibull model to the same completed + current-censored sample.

This model can extrapolate beyond historical duration support.

Its probabilities are ALWAYS labeled:

`PARAMETRIC_EXTRAPOLATION_DIAGNOSTIC`

when `duration_support = OUT_OF_SUPPORT`.

It is never allowed to override the support warning.

## Reference comparators

Reuse from V1:

- `V1_EPISODE_BALANCED_ANALOG`
- `AGE_ONLY`

The V2 diagnostic compares conditional recovery probabilities and remaining-time error where a median is defined.

## Out-of-support rule

Primary rule:

`current_episode_age > max(prior_completed_episode_duration)`

=> `OUT_OF_DURATION_SUPPORT`

Otherwise:

`IN_DURATION_SUPPORT`

No threshold tuning is allowed.

## Walk-forward minimum evidence

A test state is scored only when at least 3 prior completed episodes exist.

A formal aggregate verdict requires:

- at least 3 held-out episodes;
- at least 30 scored states.

## Primary metrics

Across scored states:

- Brier score at 7/14/30/45/60d;
- mean Brier across all horizons;
- median remaining-days MAE where forecast median is finite;
- percentage of states flagged OUT_OF_DURATION_SUPPORT;
- per-episode Brier;
- per-episode MAE;
- calibration by forecast-probability bucket where sample size permits.

## V2 decision rules

`CENSOR_AWARE_SURVIVAL_SIGNAL_V2` only if ALL are true:

1. Weibull mean Brier < V1 analog mean Brier on the same scored states;
2. KM mean Brier <= AGE_ONLY mean Brier;
3. Weibull median remaining-days MAE <= V1 analog median remaining-days MAE;
4. V2 correctly flags every state whose age exceeds prior completed duration support.

Otherwise:

`SURVIVAL_V2_NOT_YET_BETTER`

Even a PASS remains diagnostic only.

## Development cutoff

Primary walk-forward diagnostic:

`data <= 2026-03-28`

## Already-opened 2026 replay

After development metrics are frozen, replay:

`2026-03-29 -> 2026-09-26`

Label:

`POST_HOC_2026_SURVIVAL_REPLAY_NOT_VALIDATION`

Required report:

- support status through the long 2025-2026 episode;
- KM conditional probabilities;
- Weibull conditional probabilities;
- actual remaining days;
- whether V2 avoids V1's false-confidence behavior.

The replay cannot change the development verdict.

## Prohibited actions

- no trading;
- no capital weights;
- no Cox model in V2;
- no feature selection;
- no duration threshold optimization;
- no tuning Weibull parameters beyond maximum-likelihood fit;
- no choosing a model based on the 2026 replay;
- no calling the 2026 replay OOS/blind/untouched.

## Runtime impact

Production: NONE
Paper-live: NONE
Migration required: NO
