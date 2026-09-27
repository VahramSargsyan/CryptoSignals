# False Recovery vs True Recovery Trajectory Diagnostic V1

Date: 2026-09-27
Workflow mode: DIAGNOSTIC_ONLY
Status: RESEARCH_ONLY / TRAJECTORY_SEPARATION_PRESENT / NOT_TRIGGER_READY

## Objective

Determine whether false-early Breadth+Gap recovery alerts are measurably different from genuinely timely recovery alerts BEFORE inventing another confirmation rule.

No threshold optimization, trading rule, or strategy code change is authorized.

## Data

Canonical Binance D1 data reused from:

- Strategy Lab run: `36299600335`
- artifact: `10924993695`
- universe: ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK

Breadth+Gap recovery probabilities are the same reproduced probabilities used in the prior feature-ablation / horizon-separated diagnostics.

Diagnostic alert boundary remains descriptive only:

`P(recovery <= horizon) >= 0.50`

Horizons:

- 7d
- 14d

Alert labels:

- TRUE_TIMELY: actual remaining stress duration <= horizon
- FALSE_EARLY: actual remaining stress duration > horizon

## Frozen trajectory features

Only the following predeclared causal state/trajectory features were inspected:

- current breadth;
- current median SMA200 gap;
- 3d breadth change;
- 7d breadth change;
- 3d median-gap change;
- 7d median-gap change;
- cross-sectional SMA-gap dispersion;
- 7d dispersion change;
- median VOL30;
- 7d VOL30 change.

No post-signal future data was used as a candidate predictor.

## Development daily alert medians

### 7-day horizon

| Feature | TRUE_TIMELY | FALSE_EARLY |
|---|---:|---:|
| breadth | 4.0 | 4.0 |
| median SMA200 gap | **+3.38%** | +0.11% |
| breadth Δ3d | **+1** | 0 |
| breadth Δ7d | +1 | +1 |
| median gap Δ3d | **+5.22pp** | +2.13pp |
| median gap Δ7d | **+12.51pp** | +3.85pp |
| median VOL30 | 3.42% | 3.70% |

Raw breadth level does not distinguish true from false alerts here.

Trajectory / distance information does.

### 14-day horizon

| Feature | TRUE_TIMELY | FALSE_EARLY |
|---|---:|---:|
| breadth | 3.0 | 3.0 |
| median SMA200 gap | **-4.62%** | -10.59% |
| breadth Δ7d | **+1** | 0 |
| median gap Δ3d | **+1.98pp** | +0.69pp |
| median gap Δ7d | **+4.20pp** | +0.05pp |
| median VOL30 | 3.53% | 3.71% |

Again, current breadth is approximately identical while the 7-day recovery trajectory differs materially.

## Within-episode control: STRESS_007

This comparison is especially important because true and false alerts are observed inside the SAME 141-day stress episode.

### 7d alerts inside STRESS_007

| Feature | TRUE_TIMELY | FALSE_EARLY |
|---|---:|---:|
| breadth | 4.0 | 4.0 |
| median gap | **+3.38%** | +0.11% |
| breadth Δ3d | **+1** | 0 |
| median gap Δ3d | **+5.04pp** | +2.13pp |
| median gap Δ7d | **+13.22pp** | +3.85pp |
| median VOL30 | **3.39%** | 3.70% |

Nonparametric rank discrimination inside this episode:

- current median gap AUC: **0.975**
- gap Δ7d AUC: **0.824**
- gap dispersion AUC: 0.899
- median VOL30 AUC: 0.891 in the lower-vol=true direction
- current breadth AUC: 0.592

This strongly argues that the useful information is not merely the raw breadth count.

### 14d alerts inside STRESS_007

| Feature | TRUE_TIMELY | FALSE_EARLY |
|---|---:|---:|
| breadth | 3.5 | 3.0 |
| median gap | **-0.08%** | -9.26% |
| breadth Δ7d | **+1** | 0 |
| median gap Δ3d | **+2.62pp** | +0.84pp |
| median gap Δ7d | **+10.75pp** | +1.08pp |
| median VOL30 | **3.52%** | 3.70% |

Rank discrimination:

- gap Δ7d AUC: **0.791**
- current median gap AUC: **0.726**
- breadth AUC: 0.716
- median VOL30 AUC: 0.692 in the lower-vol=true direction

## Cross-episode repetition

The key 14d trajectory pattern repeats in both development episodes that contain BOTH false and true alert states.

### STRESS_004

FALSE_EARLY median:
- median gap: -15.24%
- gap Δ7d: **-7.21pp**

TRUE_TIMELY median:
- median gap: -9.98%
- gap Δ7d: **+4.95pp**

Within-episode AUC:
- breadth Δ7d: 0.877
- gap Δ7d: **0.871**
- current median gap: 0.786

### STRESS_007

FALSE_EARLY median:
- median gap: -9.26%
- gap Δ7d: **+1.08pp**

TRUE_TIMELY median:
- median gap: -0.08%
- gap Δ7d: **+10.75pp**

Within-episode AUC:
- gap Δ7d: **0.791**
- current median gap: 0.726

Therefore the same qualitative separator appears in two structurally different episodes:

`TRUE RECOVERY -> STRONGER 7-DAY IMPROVEMENT IN CROSS-ASSET SMA200 DISTANCE`

## Important negative findings

### Raw breadth is insufficient

At the daily-alert median:

- 7d true breadth = 4
- 7d false breadth = 4
- 14d true breadth = 3
- 14d false breadth = 3

Therefore the distinction is not simply:

`more assets above SMA200 = true recovery`

### 3-day persistence was the wrong abstraction

The prior Confirm3 diagnostic failed because false recovery states can persist.

The current result suggests that the useful dimension is not persistence of a high probability, but the MARKET'S DIRECTION OF TRAVEL.

### Simple monotonic daily counts are not obviously sufficient

The number of up-days over the previous three closes did not cleanly distinguish true from false states.

The magnitude of the 7-day cross-asset gap move is more informative than merely counting positive closes.

## Post-hoc 2026 consistency check

Label:

`POST_HOC_2026_DIAGNOSTIC_NOT_VALIDATION`

### 14d first timely alert — 2026-08-13

- actual remaining: 9d
- breadth: 2
- breadth Δ7d: +1
- median gap: -8.53%
- median gap Δ7d: **+3.63pp**

### 7d first timely alert — 2026-08-19

- actual remaining: 3d
- breadth: 4
- breadth Δ7d: +3
- median gap: +0.45%
- median gap Δ7d: **+9.46pp**

The opened 2026 replay is directionally consistent with the development finding:

near actual recovery, the cross-asset median distance to SMA200 improves materially over the preceding week.

This is descriptive only and cannot select thresholds.

## Main conclusion

`TRAJECTORY_SEPARATION_PRESENT_V1`

The strongest supported distinction between false and true Breadth+Gap recovery alerts is:

1. true alerts occur with a materially healthier CURRENT median SMA200 gap;
2. true alerts show a materially stronger 7-day improvement in median SMA200 gap;
3. breadth level alone is much less discriminating;
4. lower / stabilizing volatility may help, but evidence is weaker;
5. simple persistence / monotonic-count rules are not supported as the primary discriminator.

The result supports the mechanism:

`RECOVERY EVIDENCE SHOULD INCLUDE DISTANCE-TO-SMA TRAJECTORY, NOT ONLY STATE LEVEL OR PERSISTENCE`

It does NOT yet support a threshold such as:

- gap Δ7d > X;
- median gap > Y;
- weighted trajectory score > Z.

Selecting such values from these same four episodes would be parameter fitting.

## Next justified research step

A preregistered, low-parameter trajectory model should be considered, but only as RESEARCH.

The cleanest candidate family is a continuous trajectory-quality model using:

- current median SMA200 gap;
- 7-day change in median SMA200 gap;
- optionally breadth 7-day change as a secondary feature.

Before implementation, avoid threshold grids.

A future model should be evaluated episode-balanced and must preserve:

- V2 duration / OOS context;
- 7d / 14d recovery-evidence separation;
- no override of the OOS warning.

No trajectory trigger is implemented in this result.

## Runtime impact

Production code changed: NONE
Paper-live code changed: NONE
Strategy parameters changed: NONE
Schema changed: NONE
Migration required: NO

## Test level

`TEST_LEVEL: LOCAL_READ_ONLY_DIAGNOSTIC + CANONICAL_BINANCE_DATA_REUSE + FALSE_VS_TRUE_ALERT_TRAJECTORY_ANALYSIS + WITHIN_EPISODE_RANK_DIAGNOSTICS + POST_HOC_2026_CONSISTENCY_CHECK`

## Residual risks

- only four independently held-out development episodes;
- only two development episodes contain both false and true 14d alert states;
- 7d within-episode false-vs-true comparison is dominated by STRESS_007;
- daily alert rows are highly correlated;
- AUC values are descriptive, not independent validation;
- the feature family was investigated after earlier recovery diagnostics, so multiple-hypothesis risk is real;
- 2026 remains post-hoc;
- no future clean stress episode has tested the trajectory hypothesis.
