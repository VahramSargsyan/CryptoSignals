# Adaptive Stress Duration V1 — Diagnostic Result

Date: 2026-09-27
Branch: `research/adaptive-stress-duration-v1`
Final GitHub Actions run: `36297693425`
Source SHA: `00b55013a2b3a73423f2925d5830cbc28a0393c4`
Artifact ID: `10924238088`

Workflow mode: DIAGNOSTIC_ONLY
Status: RESEARCH_ONLY / PREDICTIVE_SIGNAL_PRESENT_V1 / NOT_CALIBRATED / NOT_TRADING_READY

## Objective

Test whether a broad crypto stress regime contains causal information about its own remaining duration.

No trading rule was changed or tested.

Target:

`REMAINING_STRESS_DAYS`

Stress definition remained frozen:

- entry: SMA200 breadth <= 3 for 3 consecutive closes;
- recovery: SMA200 breadth >= 5 for 3 consecutive closes.

## V1 model

Episode-balanced historical analogs.

Causal state features:

1. breadth SMA200;
2. current stress episode age;
3. 7-day breadth change;
4. median cross-asset SMA200 gap;
5. dispersion of SMA200 gaps;
6. median 30-day realized volatility.

For each prior completed stress episode, only its single closest historical state may contribute one analog.

This prevents long episodes from dominating the analog pool with many correlated daily rows.

Baseline:

`AGE_ONLY`

It knows only current episode age and durations of prior completed episodes.

## Development episode catalog

Development cutoff:

`2026-03-28`

Completed episodes available:

| Episode | Start | End | Duration |
|---|---|---|---:|
| STRESS_001 | 2024-04-19 | 2024-05-19 | 30d |
| STRESS_002 | 2024-06-13 | 2024-07-16 | 33d |
| STRESS_003 | 2024-08-06 | 2024-08-25 | 19d |
| STRESS_004 | 2024-08-29 | 2024-09-28 | 30d |
| STRESS_005 | 2024-10-02 | 2024-10-13 | 11d |
| STRESS_006 | 2024-11-04 | 2024-11-08 | 4d |
| STRESS_007 | 2025-02-26 | 2025-07-17 | 141d |

Censored at the development cutoff:

- STRESS_008 began 2025-11-01;
- no confirmed recovery was available by 2026-03-28;
- 148 observed stress-state rows existed by the cutoff;
- V1 correctly did NOT use a future completion label for this censored episode.

## Walk-forward validation

The first two completed episodes seed the historical analog pool.

Five later completed episodes were scored with only earlier completed episodes available for training.

Total scored states:

`210`

Scored held-out episodes:

`5`

### Aggregate results

| Metric | State-aware analog | AGE_ONLY |
|---|---:|---:|
| MAE remaining days | **45.73d** | 49.01d |
| Median absolute error | **33.5d** | 36.5d |
| Signed mean error | -41.34d | -43.22d |
| Mean Brier score | **0.4656** | 0.5365 |
| P25-P75 coverage | 14.29% | 15.24% |
| Mean interval width | 5.82d | 3.23d |

Analog MAE improved versus baseline in:

`4 / 5` held-out episodes.

Predeclared diagnostic checks:

- analog MAE better: PASS;
- analog median absolute error not worse: PASS;
- analog Brier better: PASS;
- analog MAE better in at least half of held-out episodes: PASS.

Formal preregistered verdict:

`PREDICTIVE_SIGNAL_PRESENT_V1`

## Per-episode MAE

| Held-out episode | Duration | Prior training episodes | Analog MAE | AGE_ONLY MAE | Better |
|---|---:|---:|---:|---:|---|
| STRESS_003 | 19d | 2 | 10.05d | 12.50d | Analog |
| STRESS_004 | 30d | 3 | 3.48d | **0.00d** | Baseline |
| STRESS_005 | 11d | 4 | 12.54d | 19.00d | Analog |
| STRESS_006 | 4d | 5 | 7.40d | 26.00d | Analog |
| STRESS_007 | 141d | 6 | 64.13d | 68.19d | Analog |

The state-aware features therefore add some historical information beyond episode age alone.

However, absolute error remains large.

## Critical calibration problem

The nominal P25-P75 interval should cover the realized remaining duration much more often than 14.29%.

Observed analog coverage:

`14.29%`

This is severe undercoverage.

The model is overconfident.

Its signed error is also strongly negative:

`-41.34 days`

Meaning:

the model systematically predicts recovery too soon.

Therefore the formal diagnostic PASS must NOT be interpreted as a usable timing model.

## Post-hoc 2026 replay

Label:

`POST_HOC_2026_FORECAST_REPLAY_NOT_VALIDATION`

The stress episode that crossed 2026 was:

- start: 2025-11-01;
- confirmed recovery: 2026-08-22;
- total duration: **294 days**.

Training set before that episode:

`7 completed historical episodes`

Longest historical completed episode:

`141 days`

Therefore by 2026-03-29:

- current episode age = **148 days**;
- the live stress episode was already older than every completed training episode.

This is an explicit out-of-historical-support condition.

### Forecast checkpoints

| Date | Episode age | Actual days remaining | V1 median prediction | P(recovery <=14d) | P(recovery <=30d) |
|---|---:|---:|---:|---:|---:|
| 2026-03-29 | 148d | **146d** | **12d** | 57.1% | **100%** |
| 2026-05-04 | 184d | **110d** | **8d** | 71.4% | **100%** |
| 2026-06-10 | 221d | **73d** | **11d** | 71.4% | **100%** |
| 2026-07-17 | 258d | **36d** | **10d** | 85.7% | **100%** |
| 2026-08-22 | 294d | 0d | 0d | 100% | 100% |

Post-hoc 2026 MAE:

`64.01 days`

Median absolute error:

`63 days`

This replay is a major extrapolation failure.

## Why V1 failed on the extreme regime

V1 is an analog interpolator.

It can ask:

> Which states in prior completed crises looked like today?

But once the current crisis becomes longer than every prior completed crisis, there is no historical analog for:

> a 148-day-old crisis that still has another 146 days remaining.

The prior completed episodes tell the model that crises of that age should already be near recovery.

Therefore the model repeatedly maps the current state to late-stage states from shorter historical episodes and predicts only ~8-12 days remaining.

This is not a small parameter problem.

It is an out-of-distribution / censoring problem.

## Important research implication

The adaptive-duration hypothesis is NOT rejected.

V1 supports a narrower statement:

`MARKET_STATE_CONTAINS_SOME_DURATION_INFORMATION_WITHIN_HISTORICAL_SUPPORT`

But V1 does NOT support:

`WE_CAN_RELIABLY_PREDICT_CRISIS_END_DATE`

The next technically justified model family would need to address two things before any trading use:

1. **Out-of-support detection**
   - explicitly refuse / widen uncertainty when episode age or state is outside historical support.

2. **Censor-aware survival / hazard modeling**
   - use ongoing censored crises as information;
   - estimate probability of recovery conditional on having survived in stress until today;
   - avoid pretending every regime must resemble a previously completed short episode.

No V2 model is implemented in this result.

## Decision

Preserve V1 as a positive-but-limited diagnostic.

Do NOT map V1 output to capital allocation.

Do NOT use its raw 8/12/30/45-day probabilities for real defensive exits.

Status:

`PREDICTIVE_SIGNAL_PRESENT_V1 / EXTRAPOLATION_FAILURE / NOT_TRADING_READY`

## Runtime impact

Production behavior: NONE
Paper-live behavior: NONE
Migration required: NO

## Test level

`GITHUB_ACTIONS_DIAGNOSTIC_EXECUTED + EPISODE_WALK_FORWARD_TESTED + POST_HOC_2026_FORECAST_REPLAY`

## Residual risks

- only 7 completed historical stress episodes before the long 2025-2026 regime;
- only 5 independently scored walk-forward test episodes;
- daily states inside each episode are strongly correlated;
- one 141-day episode contributes many aggregate scoring rows;
- P25-P75 uncertainty is badly undercalibrated;
- strong systematic early-recovery bias remains;
- post-hoc 2026 is not validation evidence;
- V1 cannot extrapolate to crisis durations beyond historical completed support.
