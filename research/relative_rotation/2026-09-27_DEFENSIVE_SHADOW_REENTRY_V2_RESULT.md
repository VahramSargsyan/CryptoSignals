# Defensive Shadow-Target Re-entry V2 — Development Result

Date: 2026-09-27
Branch: `research/defensive-shadow-reentry-v2`
GitHub Actions successful run: `36295479776`
Source SHA: `df142589d5adf50192863cd1b9922f0eb9c6ce2a`
Artifact ID: `10922904604`

Workflow mode: PATCH_FIX
Status: RESEARCH_ONLY / DEVELOPMENT_FAIL_DO_NOT_PROMOTE

## Candidate

`DEFENSIVE_LOW_VOL_CRYPTO_SHADOW_SMA200_CONFIRM3_V2`

Entry remained identical to the frozen low-vol defensive candidate.

Only exit changed:
- track the current shadow-router target;
- reset confirmation when the shadow target changes;
- exit defense only after that same shadow target closes above its own causal SMA200 for 3 consecutive days;
- execute into the shadow target at the next daily open.

No data after 2026-03-28 was used in this run.

## Main historical-year comparison

Period: 2025-03-29 -> 2026-03-28

| Variant | Median return | Median max DD | Defensive exposure | Defensive transitions |
|---|---:|---:|---:|---:|
| Base router | +43.82% | -61.57% | 0% | 0 |
| Original breadth 3/5 defense | +49.05% | -47.13% | 69.86% | 3 |
| V2 shadow-target SMA200 | **+57.03%** | -47.13% | 68.77% | 4 |

The one-year aggregate improved, but the preregistered decision was based on sequential-window gates, not this headline row.

## 180-day windows

| Window | Original return | V2 return | Original DD | V2 DD | Original exposure | V2 exposure |
|---|---:|---:|---:|---:|---:|---:|
| 2023-10-31 -> 2024-04-27 | +368.28% | +360.97% | -40.66% | -40.66% | 15.56% | 19.44% |
| 2024-04-28 -> 2024-10-24 | +4.71% | **+41.39%** | -33.09% | **-17.90%** | 51.67% | 73.89% |
| 2024-10-25 -> 2025-04-22 | +67.61% | +54.34% | -27.09% | -27.09% | 32.78% | 39.44% |
| 2025-04-23 -> 2025-10-19 | +39.73% | **+54.63%** | -43.20% | -43.20% | 46.11% | 47.22% |
| 2025-10-20 -> 2026-03-28 | -0.54% | -0.54% | -16.78% | -16.78% | 91.88% | 91.88% |

## Predeclared gates

Protection retention: PASS

- weak 2024 window DD improved from -33.09% to -17.90%;
- late-2025/early-2026 DD remained unchanged at -16.78%.

Opportunity-cost improvement: FAIL

- required V2 to beat ORIGINAL in at least 2 of 3 non-weak 180d windows;
- observed: 1 of 3.

Exposure improvement: FAIL

- required lower defensive exposure in at least 2 of 3 non-weak windows;
- observed: 0 of 3.

Churn control: PASS

No catastrophic regression: PASS

Overall:

`ALL_GATES_PASS = FALSE`

`DEVELOPMENT_FAIL_DO_NOT_PROMOTE`

## Interpretation

The candidate did not solve the intended problem.

The target-specific SMA200 condition often occurred later than the original broad breadth recovery condition. As a result, V2 could remain defensive even longer.

It was especially protective in the 2024 weak regime, but that does not compensate for failing the opportunity-cost and exposure gates.

Important:
- do not tune SMA200 to SMA100/SMA150 on the same history;
- do not change confirm3 to confirm1/2 based on these results;
- preserve V2 as a rejected mechanism.

## Runtime impact

Production / paper-live changes: NONE
Migration required: NO

TEST_LEVEL:
`GITHUB_ACTIONS_BACKTEST_EXECUTED + PREREGISTERED_DEVELOPMENT_GATES`

Residual risks:
- all evidence is development/history;
- candidate was designed after the 2026 untouched diagnosis;
- repeated post-OOS redesign increases multiple-hypothesis risk;
- future candidates must be qualitatively distinct rather than neighboring parameter tweaks.
