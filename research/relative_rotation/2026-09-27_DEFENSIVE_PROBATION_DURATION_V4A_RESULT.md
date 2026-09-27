# Defensive Probation Duration Robustness V4A — Result

Date: 2026-09-27
Branch: `research/defensive-probation-duration-robustness-v4a`
GitHub Actions run: `36296959095`
Source SHA: `dab9b414a5e71e8f81b1497873b6f6d49894fdef`
Artifact ID: `10923973073`

Workflow mode: STRESS_TEST_ONLY
Status: RESEARCH_ONLY / DURATION_ALONE_DOES_NOT_FIX_V4

## Question

Was V4's user-proposed 14-day full-capital probation simply a poor duration?

Predeclared grid:

`3, 5, 7, 10, 14, 21, 30, 45, 60` days.

No neighboring values were added after results were visible.

A robust duration required a plateau of at least two adjacent predeclared values that both passed all five development gates.

## Development result

Selection/development data ended at:

`2026-03-28`

No duration passed all gates.

No robust plateau existed.

Final development conclusion:

`DURATION_ALONE_DOES_NOT_FIX_V4`

### Development one-year summary

Period:

`2025-03-29 -> 2026-03-28`

| Probation | Median return | Median max DD | Defensive-token exposure | Probation exposure | Defensive transitions |
|---:|---:|---:|---:|---:|---:|
| 3d | +16.41% | -56.57% | 66.16% | 4.11% | 11.5 |
| 5d | +9.53% | -58.29% | 64.79% | 6.16% | 10.5 |
| 7d | -12.73% | -67.16% | 62.33% | 8.63% | 10.5 |
| 10d | -0.30% | -64.31% | 58.63% | 12.33% | 10.5 |
| 14d | +6.22% | -63.90% | 53.70% | 17.26% | 10.5 |
| 21d | -23.08% | -70.29% | 47.40% | 23.01% | 10.5 |
| 30d | -2.96% | -60.98% | 45.75% | 24.66% | 8.5 |
| 45d | +35.58% | -56.08% | 33.42% | 36.99% | 8.5 |
| 60d | +13.86% | -56.08% | 23.84% | 47.12% | 7.0 |

Reference ORIGINAL breadth 3/5 defense for the same historical year:

- median return: +49.05%;
- median max DD: -47.13%;
- defensive exposure: ~69.86%;
- median defensive transitions: 3.

No duration beat the ORIGINAL architecture on the required multi-window robustness gates.

### Gate pattern

Across all 9 durations:

- opportunity-cost improvement gate: **0 wins out of required 2** for every tested duration;
- defensive-occupancy gate: generally improved, including 2/3 non-weak windows for each tested duration;
- protection retention: failed;
- churn control: failed for the tested family under the preregistered <=6 transition requirement;
- catastrophic-regression gate: passed.

Interpretation:

Changing only the length moves capital between the defensive sleeve and stressed-market exposure, but does not make the full-capital probe mechanism robust.

Short durations:
- reduce time outside defense;
- still create repeated transitions;
- still fail protection / opportunity gates.

Long durations:
- reduce defensive-token occupancy substantially;
- expose capital for much longer while broad stress remains unresolved;
- can occasionally create high returns in a particular recovery path;
- remain historically unstable.

## Post-hoc 2026 diagnostic replay

Period:

`2026-03-29 -> 2026-09-26`

This table is explicitly:

`POST_HOC_DIAGNOSTIC_REPLAY_NOT_SELECTION_DATA`

| Probation | Median return | Median max DD | Defensive exposure | Probation exposure | Actual transitions |
|---:|---:|---:|---:|---:|---:|
| 3d | +42.58% | **-18.34%** | 72.53% | 6.59% | 10 |
| 5d | +28.05% | **-18.34%** | 68.13% | 10.99% | 10 |
| 7d | +25.65% | -21.02% | 63.74% | 15.38% | 10 |
| 10d | +29.64% | -23.20% | 57.14% | 21.98% | 10 |
| 14d | +33.93% | -21.66% | 48.35% | 30.77% | 10 |
| 21d | +14.78% | -34.85% | 33.52% | 45.60% | 8 |
| 30d | +56.09% | -31.95% | 18.68% | 60.44% | 8 |
| 45d | **+109.97%** | -33.97% | 18.68% | 60.44% | 7 |
| 60d | +68.22% | -27.02% | 16.48% | 62.64% | 6 |

Reference 2026 replay:

- BASE router: +103.36%, DD -36.68%;
- ORIGINAL defense: +32.92%, DD -18.34%.

The 2026-only surface is highly non-monotonic:

- 3d looks attractive as a protection-preserving variant;
- 45d produces the highest visible return and even exceeds the base router;
- 60d gives another very different compromise;
- neighboring durations can differ sharply.

This is exactly why the 2026 replay was prohibited from selecting the parameter.

The apparently attractive 45d result is NOT supported by development robustness and must not be promoted.

## Main conclusion

The failed V4 result is not explained by choosing 14 instead of another fixed probation duration.

The deeper issue is:

`NEW RELATIVE SIGNAL -> 100% CAPITAL EXPOSED FOR A FIXED TIME`

A fixed timer cannot distinguish:

- a useful early recovery opportunity;
- a relative rotation occurring inside a still-severe market-wide stress regime.

The useful architectural finding remains:

`SHADOW_TARGET != ACTUAL_HOLDING`

Router memory should stay separate from capital custody.

The unsupported mechanism remains:

`FULL_CAPITAL_FIXED_DURATION_PROBE`

## Decision

- Do not select 3d, 45d, 60d, or any other duration from this sweep.
- Do not refine around the visible 45d peak (e.g. 40/42/48/50d).
- Preserve V4A as evidence that duration tuning alone does not solve the mechanism.
- A future research family should alter capital exposure architecture, not merely timer length.

Potential future research direction, not yet parameterized:

`PARTIAL / GRADUATED PROBE`

No allocation weights are selected here.

## Runtime impact

Production / paper-live changes: NONE
Migration required: NO

TEST_LEVEL:

`GITHUB_ACTIONS_BACKTEST_EXECUTED + PREREGISTERED_DURATION_ROBUSTNESS_SWEEP + POST_HOC_2026_DIAGNOSTIC_REPLAY`

Residual risks:

- V4A remains post-OOS research;
- multiple-hypothesis risk is substantial;
- the 2026 surface contains seductive but non-independent peaks;
- future parameter searches on this same family would further increase overfitting risk;
- any new capital-allocation architecture requires separate preregistration and future forward evidence.
