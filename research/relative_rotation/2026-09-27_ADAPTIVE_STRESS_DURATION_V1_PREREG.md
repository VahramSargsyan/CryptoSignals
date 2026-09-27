# Adaptive Stress Duration V1 — Preregistered Diagnostic Plan

Date: 2026-09-27
Workflow mode: DIAGNOSTIC_ONLY
Status: RESEARCH_ONLY / FORECAST_DIAGNOSTIC / NO TRADING

## Objective

Test whether the duration of a broad crypto stress regime can be estimated causally from historical market states.

This study does NOT choose a trading probation length.
It does NOT change production or paper-live behavior.

Primary target:

`REMAINING_STRESS_DAYS`

defined as the number of daily closes from the current stress-state observation until the stress episode reaches its confirmed recovery close.

## Stress regime definition

Use the existing frozen market-wide breadth definition:

- breadth = number of the 8 canonical assets above own causal SMA200;
- stress ENTRY confirms after 3 consecutive closes with breadth <= 3;
- stress RECOVERY confirms after 3 consecutive closes with breadth >= 5.

Episode start:

- the third qualifying low-breadth close.

Episode end:

- the third qualifying recovery close.

The episode end itself has `remaining_stress_days = 0`.

An episode without a confirmed recovery before the dataset cutoff is CENSORED.
Censored episodes are described but are not used as completed training labels.

The first eligible row must have valid SMA200 and VOL30 history for all 8 assets.

## Canonical universe

`ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK`

Daily Binance OHLCV only.

## V1 feature vector

All features are causal and use only information available at the current close:

1. `breadth_sma200`
2. `episode_age_days`
3. `breadth_delta_7d`
4. `median_sma200_gap`
   - median across the 8 assets of `close / SMA200 - 1`
5. `dispersion_sma200_gap`
   - cross-sectional standard deviation of those 8 SMA gaps
6. `median_vol30`
   - median trailing 30d close-to-close realized volatility across the 8 assets

No price data after the current close may enter these features.

V1 intentionally excludes router-specific features.
The first question is whether the broad market state itself contains duration information.

## Forecast model: episode-balanced historical analogs

V1 uses a simple analog model rather than a black-box ML model.

For each test-state observation:

1. training may use only COMPLETED stress episodes whose confirmed end is strictly before the current test episode began;
2. robust-standardize each feature using only training-state rows:
   - center = training median;
   - scale = training IQR;
   - zero IQR falls back to scale 1;
3. compute Euclidean distance in the standardized 6-feature space;
4. within each historical training episode, keep only that episode's single closest historical state;
5. every prior episode therefore contributes at most one analog;
6. use all available episode-level analogs; no optimized K is introduced;
7. predicted remaining days = median of analog remaining-day labels;
8. uncertainty interval = empirical 25th-75th percentile;
9. report empirical:
   - P(recovery <= 7d)
   - P(recovery <= 14d)
   - P(recovery <= 30d)
   - P(recovery <= 45d)
   - P(recovery <= 60d)

This episode-balanced rule prevents one long historical crisis from dominating the neighbor set with dozens of highly correlated daily rows.

## Baseline

Compare the state-aware analog model with a causal AGE-ONLY baseline.

For each prior completed training episode:

`baseline_remaining = max(episode_duration - current_episode_age, 0)`

Prediction = median across prior episodes.

The baseline therefore knows only:

- how long the current stress has already lasted;
- historical durations of prior completed stress episodes.

It does not use breadth trend, SMA gaps, dispersion or volatility.

## Walk-forward validation

Validation unit = whole stress episode.

For each completed test episode:

- all training episodes must have ended before the test episode start;
- no row from the test episode may enter training;
- score every eligible daily state in the test episode.

This is expanding walk-forward by episode, never random row splitting.

## Minimum evidence gate

If fewer than 3 completed test episodes have at least 2 prior completed training episodes, conclude:

`INSUFFICIENT_INDEPENDENT_EPISODES`

No predictive claim is allowed.

## Metrics

Across all scored walk-forward states:

- MAE of remaining-days prediction;
- median absolute error;
- signed mean error / bias;
- P25-P75 interval coverage;
- mean interval width;
- Brier score for recovery within 7/14/30/45/60 days.

Compute the same point/probability metrics for AGE-ONLY where applicable.

Also report metrics by held-out episode so one long episode cannot hide failures elsewhere.

## Diagnostic decision

If minimum evidence is available:

`PREDICTIVE_SIGNAL_PRESENT_V1` only if ALL are true:

1. analog MAE < age-only MAE;
2. analog median absolute error <= age-only median absolute error;
3. average analog Brier score across 7/14/30/45/60d < average age-only Brier score;
4. analog improves MAE versus age-only in at least half of scored test episodes.

Otherwise:

`WEAK_OR_NO_PREDICTIVE_SIGNAL_V1`

This is a diagnostic result only, not a trading promotion gate.

## Development data boundary

Primary walk-forward development diagnostic ends at:

`2026-03-28`

## Already-opened 2026 replay

After the development diagnostic is completed, the same frozen V1 model may be replayed on:

`2026-03-29 -> 2026-09-26`

This is explicitly:

`POST_HOC_2026_FORECAST_REPLAY_NOT_VALIDATION`

It is used only to visualize what the adaptive estimator would have predicted through the already-known 2026 stress episode.

The 2026 replay cannot change the V1 diagnostic verdict.

## Prohibited actions

- do not tune the feature set after seeing 2026;
- do not tune feature weights;
- do not search K;
- do not train on partial rows from the held-out episode;
- do not convert V1 forecasts into trades in this task;
- do not call the 2026 replay OOS, blind or untouched;
- do not infer a magical fixed duration such as 45d from the opened replay.

## Promotion path

Only if V1 demonstrates predictive signal:

1. preserve V1 unchanged;
2. collect future forward stress observations;
3. only in a separate preregistered study consider mapping forecast probabilities to capital exposure.

Migration required: NO
Production impact: NONE
