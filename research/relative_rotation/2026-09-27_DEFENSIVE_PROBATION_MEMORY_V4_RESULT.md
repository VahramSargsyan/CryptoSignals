# Defensive Probation Memory Router V4 — Stress-Test Result

Date: 2026-09-27
Branch: `research/defensive-probation-memory-v4`
GitHub Actions run: `36296519093`
Source SHA: `e964a97cb1b8be6fa5c3d7b9a8de5b199845c11a`
Artifact ID: `10923828767`

Workflow mode: STRESS_TEST_ONLY
Status: RESEARCH_ONLY / DEVELOPMENT_FAIL_DO_NOT_PROMOTE

## Candidate

`DEFENSIVE_LOW_VOL_CRYPTO_PROBATION14_MEMORY_ROUTER_V4`

User-proposed structure:

- defensive entry unchanged: SMA200 breadth <= 3 for 3 closes;
- lowest trailing VOL30 token becomes defensive asset;
- shadow router keeps its logical position and continues to evolve;
- a NEW shadow transition while defensive launches a 14-day real-capital probation in the new shadow target;
- router continues to control actual capital during probation;
- probation timer does not reset on further router transitions;
- broad recovery = breadth >= 5 for 3 closes;
- if broad recovery confirms during probation, remain in router;
- if 14 days expire without broad recovery, actual capital returns to the same defensive token;
- shadow router memory is preserved;
- original broad-recovery exit remains available while defensive;
- 0.1% cost per actual transition;
- no neighboring probation lengths were tested.

## Development result

Development data ended at:

`2026-03-28`

No later data was used for the preregistered development gates.

### Historical one-year window

Period:

`2025-03-29 -> 2026-03-28`

| Variant | Median return | Median max DD | Defensive-token exposure | Probation exposure | Defensive transitions |
|---|---:|---:|---:|---:|---:|
| Base router | +43.82% | -61.57% | 0% | 0% | 0 |
| Original breadth 3/5 defense | +49.05% | -47.13% | 69.86% | 0% | 3 |
| V4 probation14 memory | **+6.22%** | **-63.90%** | 53.70% | 17.26% | 10.5 |

Interpretation:

V4 reduced time physically forced into the defensive token, but the 14-day probes exposed capital during stressed conditions often enough to destroy most of the original defensive advantage.

### Sequential 180-day windows

| Window | Original return | V4 return | Original DD | V4 DD | Original defensive exposure | V4 defensive exposure |
|---|---:|---:|---:|---:|---:|---:|
| 2023-10-31 -> 2024-04-27 | +368.28% | +319.26% | -40.66% | -40.66% | 15.56% | 7.78% |
| 2024-04-28 -> 2024-10-24 | +4.71% | +4.71% | -33.09% | -33.09% | 51.67% | 51.67% |
| 2024-10-25 -> 2025-04-22 | +67.61% | +43.65% | -27.09% | -41.16% | 32.78% | 25.00% |
| 2025-04-23 -> 2025-10-19 | +39.73% | +27.54% | -43.20% | -43.20% | 46.11% | 46.11% |
| 2025-10-20 -> 2026-03-28 | -0.54% | **-24.86%** | -16.78% | **-44.38%** | 91.88% | 74.38% |

Key failure:

In the late-2025 / early-2026 weak regime, V4 reduced defensive occupancy by ~17.5 percentage points but paid for it with a much worse return and drawdown.

Original:

- return ~-0.54%;
- DD ~-16.78%.

V4:

- return ~-24.86%;
- DD ~-44.38%.

### Predeclared gates

Protection retention: FAIL

- 2024 weak window: unchanged;
- late-2025/early-2026 weak window DD deterioration: ~+27.60 percentage points, above the allowed +10pp.

Opportunity-cost improvement: FAIL

- required V4 > ORIGINAL in at least 2 of 3 non-weak 180d windows;
- observed: 0 of 3.

Defensive occupancy improvement: PASS

- required lower defensive-token exposure in at least 2 of 3 non-weak windows;
- observed: 2 of 3.

Churn control: FAIL

- required median defensive transitions <= 6 per 180d window;
- V4 exceeded the limit in at least one window.

No catastrophic regression: PASS under the preregistered threshold.

Overall:

`DEVELOPMENT_FAIL_DO_NOT_PROMOTE`

## Opened-2026 diagnostic replay

This replay was run only after the development result, with the exact frozen V4 rule.

Period:

`2026-03-29 -> 2026-09-26`

Label:

`POST_HOC_DIAGNOSTIC_REPLAY_NOT_OOS`

| Variant | Median return | Median max DD | Defensive-token exposure | Probation exposure | Median actual transitions |
|---|---:|---:|---:|---:|---:|
| Base router | +103.36% | -36.68% | 0% | 0% | 4 |
| Original defense | +32.92% | **-18.34%** | 79.12% | 0% | 2 |
| V4 probation14 memory | **+33.93%** | -21.66% | **48.35%** | 30.77% | **10** |

V4 vs ORIGINAL in the already-opened 2026 replay:

- return: +32.92% -> +33.93% (~+1.01pp);
- max DD: -18.34% -> -21.66% (~3.32pp worse);
- defensive-token exposure: 79.12% -> 48.35%;
- probation exposure: 0% -> 30.77%;
- actual transitions: 2 -> 10;
- median probes started: 4;
- median probes succeeded by broad-recovery confirmation: 0;
- median probes failed / returned to defense: 4.

All 8 starting assets still finished positive.

## 2026 probe sequence

Representative ATOM-start path:

1. 2026-04-01: ENTER DEFENSE -> TRX, breadth 1.
2. 2026-04-02: PROBE -> TWT.
3. 2026-04-16: probation fails -> back to TRX, shadow remains TWT.
4. 2026-05-19: PROBE -> AAVE.
5. 2026-06-02: probation fails -> back to TRX, shadow remains AAVE.
6. 2026-06-30: PROBE -> TWT.
7. 2026-07-14: probation fails -> back to TRX, shadow remains TWT.
8. 2026-08-03: PROBE -> ATOM.
9. 2026-08-17: probation fails -> back to TRX, shadow remains ATOM.
10. 2026-08-23: ORIGINAL broad recovery finally confirms; exit TRX -> ATOM, breadth 7.

Important observation:

Every 14-day probe in this 2026 replay failed the broad-recovery test.

The market-wide stress condition remained severe during the probe starts:

- April probe: breadth ~1;
- May probe: breadth ~1;
- June probe: breadth ~1;
- August probe: breadth ~1, later ~2 at fallback.

The original broad-recovery rule did not confirm until 2026-08-23, when breadth reached 7 after the required confirmation sequence.

## Interpretation

The V4 test separates two findings.

### Supported architecture idea

Keeping:

- `shadow_target` = logical router state;
- `actual_holding` = defensive / executable capital state;

is coherent and worked technically.

Returning actual capital to TRX did not erase the router's logical memory.

This separation should be retained as an architectural concept.

### Rejected trading rule

The rule:

`NEW ROUTER SIGNAL -> FULL 14-DAY REAL-CAPITAL PROBE`

is not supported.

A new relative signal can occur while broad market stress is still extreme.

The 2026 replay reduced TRX occupancy but gained only ~1pp return while worsening drawdown and multiplying transitions.

Historical development evidence was materially worse.

Therefore:

`V4_PROBATION14_FULL_CAPITAL = REJECTED`

Do not optimize 7/10/21/30-day probation lengths on the same history.

## Research implication

The evidence suggests that the useful part is MEMORY, not full-capital probing.

A future architecture, if studied separately, should consider whether a probe must risk less than 100% of capital rather than changing the 14-day duration.

No allocation weights are selected here.

No V5 is authorized by this stress-test result.

## Runtime impact

Production / paper-live changes: NONE
Migration required: NO

TEST_LEVEL:
`GITHUB_ACTIONS_BACKTEST_EXECUTED + PREREGISTERED_DEVELOPMENT_GATES + POST_HOC_2026_DIAGNOSTIC_REPLAY`

Residual risks:

- V4 is post-OOS redesign attempt #3;
- the 2026 replay is not OOS evidence;
- multiple-hypothesis risk is increasing;
- strongest-extreme conflict semantics remain the reproducible runner baseline;
- the memory architecture is not yet independently validated as a production component.
