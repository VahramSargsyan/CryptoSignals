# CASH CRISIS QUARANTINE V1 — Evidence

Date: 2026-09-27  
Branch: `research/cash-crisis-quarantine-v1`  
Mode: STRESS_TEST_ONLY  
Status: EXECUTED / RESEARCH_ONLY / NO PRODUCTION CHANGE

## Run identity

- GitHub Actions run: `36300926430`
- Source commit: `85274059a44b473ff9cabe530f8a9a5ef312e9bd`
- Artifact: `cash-crisis-quarantine-v1`
- Artifact ID: `10924524875`
- Workflow conclusion: SUCCESS
- Reproduction gate: PASS

TEST_LEVEL: GITHUB_ACTIONS_EXECUTED + UNIT_TESTS + FROZEN_REPRODUCTION_GATE + REAL_BINANCE_1D + FULL_HISTORY + NON_OVERLAPPING_180D_120D_ROBUSTNESS

## Frozen design

Crisis entry remained frozen:
- breadth <=3 for 3 consecutive closes;
- next-open cash execution;
- shadow router continues;
- 0.1% each actual transition.

Cash durations were preregistered before execution:
- 14d
- 21d
- 30d
- 45d
- 60d

During cash, crisis detection was disabled.
After fixed cash exit, crisis streak reset to zero and immediately became eligible to start again from fresh closes.

## Reproduction period 2025-03-29 -> 2026-03-28

Frozen LOW_VOL reference:
- return: +49.05%
- max DD: -47.13%

Fixed quarantine:
- 14d: -36.57% / -55.38%
- 21d: -0.20% / -55.28%
- 30d: +28.16% / -43.20%
- 45d: -16.73% / -52.56%
- 60d: +65.43% / -43.20%

The apparent 60d advantage is not stable out of this period.

## Already-open 2026 period 2026-03-29 -> 2026-09-26

Frozen LOW_VOL reference:
- return: +32.92%
- max DD: -18.34%

Fixed quarantine:
- 14d: +55.53% / -18.34%
- 21d: +35.69% / -18.34%
- 30d: -15.32% / -33.76%
- 45d: +20.21% / -18.34%
- 60d: -1.27% / -11.31%

The ranking almost reverses versus the reproduction period:
- 60d was strongest historically but weak in opened 2026;
- 14d was very weak historically but strongest in opened 2026.

This is direct evidence of duration sensitivity.

## Full eligible history 2023-11-20 -> 2026-09-26

Frozen comparators:
- BASELINE: +646.96% / -71.23%
- LOW_VOL: +887.02% / -47.13%
- frozen-timing CASH: +258.06% / -67.56%

Fixed quarantine:
- 14d: +111.20% / -74.83%
- 21d: +162.60% / -64.29%
- 30d: +156.56% / -72.94%
- 45d: +279.76% / -55.42%
- 60d: +246.62% / -68.31%

No tested fixed duration beat frozen LOW_VOL on full-history return or drawdown.

## Re-trigger churn

Because V1 re-armed crisis detection immediately after each cash exit, a persistent low-breadth regime could generate repeated cash quarantines inside the same underlying crisis.

Full-history median cash entries:
- 14d: 33
- 21d: 25
- 30d: 19
- 45d: 14
- 60d: 11

Median actual transitions:
- 14d: 77
- 21d: 62
- 30d: 48
- 45d: 39
- 60d: 31

This is materially different from the eight frozen breadth-defined crisis episodes and likely overstates the intended number of independent crisis reactions.

## Robustness

13 preregistered windows:
- 5 x 180d
- 8 x 120d

Against frozen LOW_VOL, number of windows improving BOTH return and drawdown:
- 14d: 2/13
- 21d: 1/13
- 30d: 1/13
- 45d: 2/13
- 60d: 2/13

No duration improved both dimensions consistently.

On 180d windows specifically, no duration beat LOW_VOL on both return and drawdown in any of the five windows.

## Verdict

`CASH_CRISIS_QUARANTINE_V1_VERDICT = DURATION_SENSITIVE / RETRIGGER_CHURN / DO_NOT_PROMOTE`

Supported:
- finite cash quarantine can outperform LOW_VOL in individual regimes;
- cash duration materially changes both return and drawdown;
- returning to the active router can recover much more upside than frozen-timing cash.

Not supported:
- choosing 14/21/30/45/60d from this history and calling it robust;
- immediate crisis re-arming after fixed cash exit;
- promotion to production.

## Next semantic correction

The more faithful interpretation of “a new crisis” is one cash reaction per crisis episode.

Next preregistered variant:
- fixed cash duration still 14/21/30/45/60d;
- after cash exit, actual capital resumes the shadow router;
- crisis detector remains DISARMED;
- it re-arms only after the already-frozen recovery condition is observed: breadth >=5 for 3 consecutive closes;
- only after re-arm can a fresh breadth<=3 x3 sequence trigger another cash quarantine.

The recovery condition is therefore used only to identify that the old crisis episode ended, NOT to keep capital in cash.

## Runtime impact

Production changed: NONE  
Paper-live changed: NONE  
Automatic execution authorized: NO  
Migration required: NO
