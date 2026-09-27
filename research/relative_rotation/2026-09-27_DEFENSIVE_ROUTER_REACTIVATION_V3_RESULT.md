# Defensive Router Reactivation V3 — Development Result

Date: 2026-09-27
Branch: `research/defensive-router-reactivation-v3`
GitHub Actions run: `36295670563`
Source SHA: `31f41095281c494d2b9adbaed775ef9a344f2e03`
Artifact ID: `10923817751`

Workflow mode: PATCH_FIX
Status: RESEARCH_ONLY / DEVELOPMENT_FAIL_DO_NOT_PROMOTE

## Candidate

`DEFENSIVE_LOW_VOL_CRYPTO_ROUTER_REACTIVATION_V3`

Entry remained identical to the frozen low-vol defense.

Exit rule:
- while defensive, wait for the first NEW relative-router transition generated after defensive entry;
- execute the shadow transition and exit defense into that new shadow target at the next daily open;
- a transition already pending before defensive entry does not count;
- no extra exit threshold or moving average was introduced.

Data boundary:

`END = 2026-03-28`

No post-2026-03-28 data was used.

## Main historical-year result

Period: 2025-03-29 -> 2026-03-28

| Variant | Median return | Worst start | Median max DD | Defensive exposure | Median defensive transitions |
|---|---:|---:|---:|---:|---:|
| Base router | +43.82% | +5.03% | -61.57% | 0% | 0 |
| Original breadth 3/5 defense | +49.05% | +41.84% | -47.13% | 69.86% | 3 |
| V3 router reactivation | **-2.78%** | -9.69% | **-56.57%** | 64.38% | **11.5** |

V3 sharply increased enter/exit cycling and destroyed most of the original defensive benefit.

## 180-day windows

| Window | Original return | V3 return | Original DD | V3 DD | Original exposure | V3 exposure |
|---|---:|---:|---:|---:|---:|---:|
| 2023-10-31 -> 2024-04-27 | +368.28% | +286.12% | -40.66% | -40.66% | 15.56% | 16.94% |
| 2024-04-28 -> 2024-10-24 | +4.71% | **+41.39%** | -33.09% | **-17.90%** | 51.67% | 73.89% |
| 2024-10-25 -> 2025-04-22 | +67.61% | **-2.80%** | -27.09% | -36.60% | 32.78% | 40.56% |
| 2025-04-23 -> 2025-10-19 | +39.73% | +30.24% | -43.20% | -37.51% | 46.11% | **81.11%** |
| 2025-10-20 -> 2026-03-28 | -0.54% | **-21.59%** | -16.78% | **-33.26%** | 91.88% | 88.13% |

## Predeclared gates

Protection retention: FAIL

- 2024 weak window improved strongly;
- late-2025/early-2026 weak window DD worsened by ~16.48 percentage points versus ORIGINAL, exceeding the allowed +10pp deterioration.

Opportunity-cost improvement: FAIL

- required: V3 beats ORIGINAL in at least 2 of 3 non-weak 180d windows;
- observed: 0 of 3.

Exposure improvement: FAIL

- required: V3 has lower defensive exposure in at least 2 of 3 non-weak windows;
- observed: 0 of 3.

Churn control on 180d windows: PASS

No catastrophic regression gate: PASS under the preregistered -10% threshold, although the 2024-10-25 -> 2025-04-22 return deterioration from +67.61% to -2.80% is still economically material.

Overall:

`ALL_GATES_PASS = FALSE`

`DEVELOPMENT_FAIL_DO_NOT_PROMOTE`

## Failure mode

Router activity is not equivalent to market recovery.

The relative router can generate new pairwise transitions while the broad crypto market remains deeply stressed.

Example from the historical one-year window, ATOM start:

- 2025-04-01 ENTER TRX defense, breadth = 1
- 2025-04-02 EXIT on router transition into PEPE, breadth still = 1
- 2025-04-05 RE-ENTER TRX defense
- later repeated exit/re-entry cycles continue

Late weak regime example:

- 2025-11-02 ENTER TRX, breadth = 3
- 2025-11-25 EXIT into PEPE, breadth = 2
- 2025-11-28 RE-ENTER TRX
- 2026-01-06 EXIT into ATOM, breadth = 1
- 2026-01-09 RE-ENTER TRX
- 2026-02-05 EXIT into TWT, breadth = 0
- 2026-02-08 RE-ENTER TRX

Interpretation:

A relative transition is evidence that the router is ACTIVE, but not evidence that the market is SAFE enough to abandon the defensive asset.

This distinction is now directly supported by historical evidence.

## Post-OOS redesign record

1. V2 shadow-target SMA200 confirm3 — FAILED.
2. V3 router reactivation — FAILED.

Do not continue by tuning neighboring numerical thresholds in these same mechanisms.

The next research family, if pursued, should change the defensive architecture rather than keep searching for one binary EXIT trigger.

Candidate direction for a future separately preregistered study:

`GRADUATED / PARTIAL RE-ENTRY`

Concept only:
- preserve market-wide defense;
- allow partial participation in recovering shadow-router opportunities;
- keep a defensive sleeve until broad recovery is confirmed.

No weights or thresholds are selected in this result document.

## Runtime impact

Production / paper-live behavior changed: NONE
Migration required: NO

TEST_LEVEL:
`GITHUB_ACTIONS_BACKTEST_EXECUTED + PREREGISTERED_DEVELOPMENT_GATES`

Residual risks:
- all V3 evidence is development/history;
- V3 was designed after the 2026 untouched diagnosis;
- two post-OOS redesign hypotheses have now been tested;
- multiple-hypothesis risk is increasing;
- future work needs a new architecture plus forward evidence, not repeated threshold search.
