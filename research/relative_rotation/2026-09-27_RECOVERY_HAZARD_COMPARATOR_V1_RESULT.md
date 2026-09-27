# Recovery Hazard Comparator V1 — Read-Only Diagnostic Result

Date: 2026-09-27
Workflow mode: DIAGNOSTIC_ONLY
Status: RESEARCH_ONLY / NO TRADING / NO STRATEGY CODE CHANGED

## Purpose

Compare candidate mechanisms for detecting recovery from the existing broad crypto stress regime:

1. V2 Kaplan-Meier censor-aware survival reference;
2. V2 Weibull censor-aware survival reference;
3. V1 episode-balanced state analog reference;
4. AGE_ONLY reference;
5. penalized discrete-time logistic recovery hazard;
6. time-varying Cox-style hazard diagnostic;
7. statsmodels Markov TVTP diagnostic.

The objective is diagnostic only. No model is promoted and no capital-allocation rule is changed.

## Canonical data

Data was retrieved through the existing `Strategy Lab Regime Research` pipeline from clean `main`.

GitHub Actions data run:

`36299600335`

Data request branch:

`research-run/regime/recovery-hazard-comparator-data-v1-main`

Request commit:

`6c4ac54f6b19a6984e1c8ee72d028e05c4466b5e`

Artifact:

`10924993695 / cryptosignals-strategy-lab-regime`

Universe:

`ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK`

Timeframe:

`1D`

Requested range:

`2023-01-01 -> 2026-09-27 UTC`

Last fully closed daily candle in artifact:

`2026-09-26`

Seven assets had 1365 rows from 2023-01-01.
PEPE had 1241 rows from 2023-05-05.
No duplicate timestamps or candle-field NA values were observed.

## Stress semantic reproduction gate

The locally reconstructed stress definition exactly reproduced the V1/V2 development episodes:

| Episode | Start | End | Duration |
|---|---|---|---:|
| STRESS_001 | 2024-04-19 | 2024-05-19 | 30d |
| STRESS_002 | 2024-06-13 | 2024-07-16 | 33d |
| STRESS_003 | 2024-08-06 | 2024-08-25 | 19d |
| STRESS_004 | 2024-08-29 | 2024-09-28 | 30d |
| STRESS_005 | 2024-10-02 | 2024-10-13 | 11d |
| STRESS_006 | 2024-11-04 | 2024-11-08 | 4d |
| STRESS_007 | 2025-02-26 | 2025-07-17 | 141d |
| STRESS_008 | 2025-11-01 | censored at 2026-03-28 | >=148d |

Result:

`SEMANTIC_REPRODUCTION_GATE = PASS`

## OSS mechanisms reviewed

### lifelines

Repository: `CamDavidsonPilon/lifelines`
License: MIT

Relevant mechanism:

`CoxTimeVaryingFitter`

The official documentation supports long-format survival regression with covariates that change over time.

Important limitation from the upstream documentation:

future prediction with time-varying covariates is logically constrained because future covariate paths are unknown.

### statsmodels

Repository: `statsmodels/statsmodels`
License: BSD-3-Clause

Relevant mechanism:

`MarkovRegression / MarkovAutoregression + exog_tvtp`

The official Filardo example implements time-varying transition probabilities and changing expected regime duration.

Only FILTERED probabilities are causally relevant for live inference.
Smoothed probabilities use future observations and were not used.

## Local diagnostic formulations

No comparator code was committed because workflow mode was DIAGNOSTIC_ONLY.

### Discrete logistic hazard

Daily recovery hazard with fixed L2 regularization and no parameter search.

Features:

- breadth_sma200;
- episode_age_days;
- breadth_delta_7d;
- median_sma200_gap;
- dispersion_sma200_gap;
- median_vol30.

Prior completed episodes plus the currently survived part of the held-out episode were used causally.

### Cox-style time-varying hazard

A read-only local counting-process diagnostic was fitted with `statsmodels.PHReg`, fixed L2 regularization and Breslow baseline hazard.

This is a diagnostic approximation to the Cox time-varying idea.

It is NOT claimed to be an execution of `lifelines.CoxTimeVaryingFitter`.

### TVTP Markov

`statsmodels.MarkovRegression`, two latent regimes, switching variance and time-varying transition probabilities.

Transition regressors used current causal market-state features.

The lower conditional median-SMA-gap regime was mapped to STRESS.

Filtered, never smoothed, regime probabilities were used.

## Development comparison

Scored held-out episodes:

`STRESS_004 .. STRESS_007`

Scored daily states excluding the already-confirmed recovery close:

`186`

Horizons:

`7 / 14 / 30 / 45 / 60 days`

### State-weighted mean Brier

Lower is better.

| Model | Mean Brier |
|---|---:|
| Discrete logistic hazard | **0.2678** |
| Cox-style time-varying diagnostic | **0.2816** |
| V2 Kaplan-Meier | 0.3294 |
| TVTP Markov | 0.3634 |
| V2 Weibull | 0.3650 |
| V1 state analog | 0.5136 |
| AGE_ONLY | 0.5806 |

This table alone is misleading because STRESS_007 contributes 141 of the 186 daily states.

### Episode-balanced mean Brier

Each held-out crisis receives equal weight.

| Model | Episode-balanced mean Brier |
|---|---:|
| V2 Kaplan-Meier | **0.2595** |
| Cox-style time-varying diagnostic | 0.2620 |
| V1 state analog | 0.2739 |
| V2 Weibull | 0.2827 |
| AGE_ONLY | 0.3424 |
| Discrete logistic hazard | 0.3553 |
| TVTP Markov | 0.7221 |

This reverses the naive state-weighted conclusion.

The logistic hazard is NOT a universal development winner.

Its apparent aggregate advantage comes primarily from the long STRESS_007 episode.

## Per-episode mean Brier

| Episode | Duration | Logistic | Cox-style | TVTP | KM | Weibull | V1 | AGE |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| STRESS_004 | 30d | 0.4552 | 0.0557 | 0.7400 | **0.0228** | 0.0210 | 0.0348 | 0.0230 |
| STRESS_005 | 11d | 0.3568 | 0.3084 | 0.9273 | 0.2830 | 0.3110 | **0.2170** | 0.2830 |
| STRESS_006 | 4d | 0.3915 | 0.3587 | 1.0000 | 0.3340 | 0.3560 | **0.1960** | 0.3340 |
| STRESS_007 | 141d | **0.2174** | 0.3255 | 0.2213 | 0.3981 | 0.4427 | 0.6476 | 0.7295 |

Interpretation:

- KM remains strongest overall when independent episodes receive equal weight.
- V1 remains useful in very short episodes.
- logistic hazard is materially strongest in the long 141-day episode.
- no single model dominates all stress lengths.

## TVTP Markov failure mode

The current TVTP Markov formulation is not supported.

Across the development stress rows it collapsed into an almost absorbing stress regime:

`P(STRESS -> STRESS) ~= 1`

for the observed held-out stress periods.

Its apparently reasonable result on STRESS_007 is therefore largely produced by predicting no recovery throughout a very long crisis, not by a well-calibrated dynamic transition mechanism.

Decision:

`TVTP_MARKOV_V1_FORMULATION = REJECTED_DIAGNOSTICALLY`

Do not tune it on the same episodes.

## Post-hoc 2026 replay

Label:

`POST_HOC_2026_DIAGNOSTIC_NOT_VALIDATION`

Target episode:

`2025-11-01 -> 2026-08-22`

Total duration:

`294 days`

Replay:

`2026-03-29 -> 2026-08-21`

States:

`146`

All replay states were beyond the previous completed duration support of 141 days.

### Mean Brier on this single post-hoc episode

| Model | Mean Brier |
|---|---:|
| V2 Weibull | **0.1756** |
| Discrete logistic hazard | 0.1878 |
| TVTP Markov | 0.2068 |
| Cox-style diagnostic | 0.2137 |
| V2 Kaplan-Meier | 0.2137 |
| V1 analog | 0.5537 |

Weibull remains the best overall probability scorer on this one extreme episode.

That does not mean it detects recovery earliest.

## Recovery timing behavior

First day each model assigned probability >= 0.50:

### Discrete logistic hazard

- recovery <= 7d: 2026-08-20, actual remaining = 2d;
- recovery <= 14d: 2026-08-19, actual remaining = 3d;
- recovery <= 30d: 2026-08-19, actual remaining = 3d;
- recovery <= 45d: 2026-08-19, actual remaining = 3d;
- recovery <= 60d: 2026-08-19, actual remaining = 3d.

### Cox-style / KM

No >=0.50 signal while out of duration support.

Their historical event-time support ends before the live episode age, so they conservatively emit no new recovery hazard.

### TVTP Markov

The stress state remains nearly absorbing until 2026-08-21, one day before confirmed recovery, when it flips almost discontinuously.

### Weibull V2

No >=0.50 signal for 7/14/30/45d.
The 60d probability was already above 0.50 on 2026-03-29 and therefore was not a timely recovery detector.

### V1 analog

Produced false-early confidence:
14/30/45/60d probabilities were already >=0.50 on 2026-03-29.

## What caused the logistic jump

Key causal state change:

### 2026-08-18

- breadth = 2/8
- breadth delta 7d = +1
- median SMA200 gap = -7.24%
- logistic P(recovery <= 14d) = 2.32%

### 2026-08-19

- breadth = 4/8
- breadth delta 7d = +3
- median SMA200 gap = +0.45%
- logistic P(recovery <= 14d) = **70.11%**

### 2026-08-20

- breadth = 5/8
- breadth delta 7d = +3
- median SMA200 gap = +4.39%
- logistic P(recovery <= 14d) = **95.67%**

### 2026-08-21

- breadth = 6/8
- breadth delta 7d = +4
- median SMA200 gap = +13.49%
- logistic P(recovery <= 7d) = **99.97%**

Standardized logistic coefficients around the transition were dominated by:

- breadth: ~+1.8
- median SMA200 gap: ~+1.47
- breadth_delta_7d: ~+0.78
- episode age: negative
- median VOL30: mildly negative

This matters because the official recovery definition is itself based on breadth >= 5 for three closes.

Therefore the logistic hazard may primarily be learning a probabilistic / softened version of the existing breadth-recovery rule rather than discovering independent recovery alpha.

This is causal, not look-ahead, but it limits novelty.

## Main diagnostic conclusion

There is no justified universal replacement for V2.

The evidence supports a narrower architecture:

1. Preserve V2 censor-aware survival and explicit duration-support guard.
2. Preserve KM/Weibull as duration / uncertainty context.
3. Treat state-conditioned logistic hazard as a promising RECOVERY-EVIDENCE layer specifically for long stress regimes.
4. Do not use raw logistic probability as a trading trigger yet.
5. Reject the current TVTP Markov formulation.
6. Cox-style time-varying hazard is competitive episode-balanced but cannot extrapolate recovery hazard beyond observed event-time support in the current formulation.

Candidate future architecture:

`SURVIVAL / OOS GUARD + STATE-CONDITIONED RECOVERY EVIDENCE`

not:

`ONE MODEL REPLACES EVERYTHING`

## Critical limitations

- only four independently held-out development episodes;
- daily rows are strongly correlated within an episode;
- the 141-day episode dominates state-weighted metrics;
- logistic performance is poor on the three short held-out episodes;
- 2026 is post-hoc and cannot select the model;
- logistic features overlap strongly with the frozen breadth recovery definition;
- the Cox diagnostic used `statsmodels.PHReg`, not the exact upstream `lifelines.CoxTimeVaryingFitter`;
- no comparator thresholds were optimized;
- no trading or allocation mapping was tested.

## Runtime impact

Production code changed: NONE
Paper-live code changed: NONE
Strategy parameters changed: NONE
Schema changed: NONE
Migration required: NO

Only a research request JSON was committed to obtain canonical data through the existing pipeline.

## Test level

`TEST_LEVEL: LOCAL_READ_ONLY_DIAGNOSTIC + CANONICAL_BINANCE_DATA_ACTION_RUN + STRESS_SEMANTIC_REPRODUCTION_GATE + EPISODE_WALK_FORWARD_COMPARATOR + POST_HOC_2026_REPLAY`
