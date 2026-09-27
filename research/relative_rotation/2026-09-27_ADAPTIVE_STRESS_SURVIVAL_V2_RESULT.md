# Adaptive Stress Survival V2 — Diagnostic Result

Date: 2026-09-27
Branch: `research/adaptive-stress-survival-v2`
GitHub Actions run: `36298322173`
Source SHA: `123db1bbf94860e1c85d08abac094b8760d9124b`
Artifact ID: `10925156042`

Workflow mode: BUILD_NEW_APP
Status: RESEARCH_ONLY / CENSOR_AWARE_SURVIVAL_SIGNAL_V2 / NOT_TRADING_READY

## Objective

Test a right-censor-aware survival framework for broad crypto stress duration.

V2 does not trade.

It estimates:

`P(recovery within H days | stress has already survived to current age)`

for H = 7 / 14 / 30 / 45 / 60 days.

## OSS reuse

Survival fitting uses:

- `CamDavidsonPilon/lifelines`
- pinned version: `0.30.3`
- upstream tag commit: `a21e4328fa30bc107ae2a3e0276ec57e892e6504`
- license: MIT
- adopted components only: `KaplanMeierFitter`, `WeibullFitter`

No upstream source code was copied.

## Frozen episode definition

Unchanged:

- breadth = number of canonical 8 assets above own causal SMA200;
- stress begins on the 3rd consecutive close with breadth <= 3;
- recovery confirms on the 3rd consecutive close with breadth >= 5.

At every test state:

- all earlier completed episodes are event observations;
- the current test episode contributes one right-censored observation at its currently survived age;
- no future test-episode end is exposed.

## Development walk-forward

Development cutoff:

`2026-03-28`

A state was scored only with at least 3 prior completed episodes.

Scored:

- 4 held-out stress episodes;
- 190 daily stress states.

### Aggregate probability quality

| Model | Mean Brier score |
|---|---:|
| **KM censor-aware** | **0.3307** |
| Weibull censor-aware | **0.3629** |
| V1 state analog | 0.5030 |
| AGE_ONLY | 0.5714 |

Lower is better.

Both censor-aware survival models materially improved probability scoring relative to V1 and AGE_ONLY.

### Remaining-days median error

| Model | Median absolute error |
|---|---:|
| KM, where finite | **19.0d** |
| Weibull | **32.20d** |
| V1 state analog | 43.0d |
| AGE_ONLY | 46.5d |

KM had a finite conditional median on only 81 of 190 states.

That is intentional: nonparametric KM refuses to invent an event-time median when historical events do not support one.

### Duration support

Out-of-duration-support states:

`108 / 190 = 56.84%`

Support flag accuracy against the frozen rule:

`100%`

The high OOS fraction is itself an important finding: even on development history, long stress episodes often exceeded the maximum duration previously observed at that historical point.

## Formal preregistered gate

Checks:

- Weibull Brier < V1 analog Brier: PASS
- KM Brier <= AGE_ONLY Brier: PASS
- Weibull median error <= V1 median error: PASS
- support flag exact: PASS

Formal verdict:

`CENSOR_AWARE_SURVIVAL_SIGNAL_V2`

This remains diagnostic, not trading approval.

## Per-episode behavior

| Episode | Duration | OOS states | KM Brier | Weibull Brier | V1 Brier |
|---|---:|---:|---:|---:|---:|
| STRESS_004 | 30d | 0 | 0.0220 | **0.0204** | 0.0337 |
| STRESS_005 | 11d | 0 | 0.2854 | 0.3111 | **0.2010** |
| STRESS_006 | 4d | 0 | 0.3184 | 0.3484 | **0.1600** |
| STRESS_007 | 141d | 108 | **0.4024** | 0.4426 | 0.6431 |

Interpretation:

Survival V2 gains most of its aggregate advantage on the long 141-day regime.

It does not dominate V1 on every short episode.

Therefore V2 is not a universal replacement for state information; it primarily fixes duration conditioning and extrapolation discipline.

## Post-hoc 2026 replay

Label:

`POST_HOC_2026_SURVIVAL_REPLAY_NOT_VALIDATION`

Stress episode:

`2025-11-01 -> 2026-08-22`

Total duration:

`294 days`

Prior completed training episodes:

`7`

Maximum prior completed duration:

`141 days`

Replay states from 2026-03-29 onward:

`147`

OOS states:

`147 / 147 = 100%`

So V2 would have declared the entire observed March-August segment:

`OUT_OF_DURATION_SUPPORT`

### Checkpoints

| Date | Age | Actual remaining | Support | KM median | Weibull median | V1 median |
|---|---:|---:|---|---:|---:|---:|
| 2026-03-29 | 148d | 146d | OUT | undefined | 54.6d | 12d |
| 2026-05-04 | 184d | 110d | OUT | undefined | 67.0d | 8d |
| 2026-06-10 | 221d | 73d | OUT | undefined | 80.9d | 11d |
| 2026-07-17 | 258d | 36d | OUT | undefined | 95.8d | 10d |
| 2026-08-22 | 294d | 0d | OUT | undefined | 110.9d | 0d |

### 30-day recovery probability

On 2026-03-29:

- V1 analog: **100%**
- Weibull extrapolation: **31.97%**
- KM historical evidence: **0%**
- support status: **OUT_OF_DURATION_SUPPORT**

This is a major improvement in epistemic behavior.

V1 falsely expressed certainty that recovery would occur within 30 days.

V2 instead says:

- historical nonparametric data provide no observed hazard beyond the prior duration support;
- parametric Weibull can extrapolate, but only as explicitly marked diagnostic extrapolation;
- trusted duration forecast is UNKNOWN.

## 2026 error

Post-hoc median absolute error:

- Weibull V2: **51.16d**
- V1 analog: **63.0d**

Weibull improved the numerical error but remained too inaccurate for trading use.

Near the eventual recovery, the Weibull model became too pessimistic because it only models duration survival and does not yet use recovery-state market features.

This creates the next research boundary:

`DURATION SURVIVAL != RECOVERY STATE`

## Main conclusion

V2 solves one important problem from V1:

`FALSE CONFIDENCE OUTSIDE HISTORICAL DURATION SUPPORT`

It does NOT solve:

`TIMELY DETECTION OF RECOVERY WHILE OUT OF SUPPORT`

Supported findings:

1. censor-aware survival probabilities improve historical probability calibration versus V1;
2. explicit OOS detection is necessary;
3. KM appropriately refuses to produce unsupported medians;
4. Weibull tail extrapolation is numerically better than V1 in the long 2026 replay but still unreliable;
5. duration-only survival becomes increasingly pessimistic near actual recovery because it does not observe improving market state.

## Research implication

The technically justified next family would combine:

- survival / censoring discipline;
- explicit OOS status;
- causal market-state recovery features.

Potential future label:

`STATE_CONDITIONED_HAZARD_V3`

No V3 is implemented in this result.

A future V3 must not erase the OOS warning merely because state features look positive.

## Decision

Preserve V2.

Do not connect it to capital allocation.

Do not use Weibull median remaining days as an exit timer.

Status:

`CENSOR_AWARE_SURVIVAL_SIGNAL_V2 / OOS_GUARD_VALIDATED_DIAGNOSTICALLY / NOT_TRADING_READY`

## Runtime impact

Production behavior: NONE
Paper-live behavior: NONE
Migration required: NO

## Test level

`GITHUB_ACTIONS_DIAGNOSTIC_EXECUTED + EPISODE_WALK_FORWARD_SURVIVAL_TESTED + POST_HOC_2026_SURVIVAL_REPLAY`

## Residual risks

- only 4 independently scored V2 test episodes;
- only 7 completed episodes before the extreme 2025-2026 episode;
- 56.8% of development states were already out of prior duration support;
- Weibull tail depends on parametric shape assumptions;
- KM is conservative and cannot estimate unseen tail hazard;
- state features are not yet included in V2 survival hazard;
- post-hoc 2026 replay is not validation evidence;
- no trading or allocation mapping has been tested.
