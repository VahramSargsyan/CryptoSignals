# DEFENSIVE_LOW_VOL_CRYPTO — Untouched Validation 2026

Date: 2026-09-27  
Branch: `research/defensive-low-vol-untouched-v1`  
GitHub Actions run: `36294922355`  
Source SHA: `5fe61d384d8ee1ea3748e63b15aba4239be492df`  
Artifact ID: `10923587272`

Workflow mode: PATCH_FIX + STRESS_TEST_ONLY boundary  
Status: RESEARCH_ONLY / NO PRODUCTION CHANGE

## Frozen rule

Strategy label:

`DEFENSIVE_LOW_VOL_CRYPTO_SMA200_BREADTH_3_5_CONFIRM3_VOL30`

Parameters were fixed before opening post-2026-03-28 data:

- universe: ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK;
- base relative router unchanged;
- market breadth = count of the 8 tokens above own causal SMA200;
- enter defense after 3 consecutive closes with breadth <= 3;
- defensive token = lowest trailing 30d close-to-close realized volatility;
- hold that defensive token until exit;
- relative router continues in shadow;
- exit after 3 consecutive closes with breadth >= 5;
- next-open execution;
- 0.1% cost per actual transition;
- no USDT;
- no parameter changes after observing the untouched result.

## Reproduction gate

Before retrieving any post-2026-03-28 data, the new reproducible runner had to reproduce the already-seen historical candidate.

Historical period:

`2025-03-29 -> 2026-03-28`

Expected from prior research:

- baseline median return: +41.6%;
- baseline max DD: -62.2%;
- defensive median return: +49.4%;
- defensive max DD: -47.1%.

Fresh reproduction:

- baseline median return: **+43.82%**;
- baseline median max DD: **-61.57%**;
- defensive median return: **+49.05%**;
- defensive median max DD: **-47.13%**;
- defensive positive starts: **8/8**;
- median defensive transitions: **3**.

Predeclared tolerance:

`+/- 5 percentage points`

Result:

`REPRODUCTION_GATE = PASS`

Only after this PASS did the runner retrieve/open the untouched period.

## Untouched validation

Period:

`2026-03-29 -> 2026-09-26`

182 daily candles.

### Main result

| Variant | Median return | Worst start | Median max DD | Positive starts |
|---|---:|---:|---:|---:|
| Base 8-node rotation | **+103.36%** | +80.00% | -36.68% | 8/8 |
| + DEFENSIVE_LOW_VOL_CRYPTO | **+32.92%** | +9.49% | **-18.34%** | 8/8 |

Effect:

- max drawdown improved by about **18.34 percentage points**;
- defensive max drawdown was roughly half the baseline drawdown;
- median return was reduced by about **70.44 percentage points**;
- all 8 starts still finished positive;
- churn remained very low.

### By starting asset

Baseline returns:

- ATOM: +102.44%
- TWT: +80.00%
- PEPE: +106.58%
- BNB: +100.25%
- SOL: +98.09%
- TRX: +104.36%
- AAVE: +104.28%
- LINK: +110.97%

Defensive returns:

- ATOM: +34.45%
- TWT: +9.49%
- PEPE: +34.78%
- BNB: +32.06%
- SOL: +32.43%
- TRX: +30.94%
- AAVE: +33.42%
- LINK: +35.18%

## Defensive episode

All starts generated the same market-wide defensive episode.

Execution into defense:

`2026-04-01`

Selected token:

`TRX`

Execution out of defense:

`2026-08-23`

Return target on exit:

the current shadow router target, which had converged to `ATOM`.

Defensive occupancy:

- 144 / 182 days;
- approximately **79.1%** of the untouched period.

The shadow router continued to evolve while actual capital remained in TRX. Example shadow path from an ATOM start:

`ATOM -> TWT -> AAVE -> TWT -> ATOM`

Actual defensive holding during most of that sequence remained:

`TRX`

Interpretation:

The rule correctly identified a low-volatility defensive token and materially reduced drawdown, but the breadth exit condition kept the system defensive through most of a strong recovery regime.

## Monthly defensive path

Median defensive monthly returns:

- Mar 2026 partial: +2.53%
- Apr: +3.49%
- May: +7.46%
- Jun: -8.24%
- Jul: +3.16%
- Aug: -2.37%
- Sep through 26th: +27.61%

The April-August defensive block compounded to only roughly +2.8%, explaining much of the opportunity cost versus the active router.

## Predeclared interpretation

The untouched result matches the previously defined `MIXED` category:

- drawdown improved materially: YES;
- positive across most/all starts: YES, 8/8;
- excessive churn: NO;
- causal mechanism behaved as designed: YES;
- return loss too large: YES.

Therefore:

`UNTOUCHED_VERDICT = MIXED / REAL_RISK_CONTROL_BUT_TOO_MUCH_UPSIDE_SACRIFICE`

## Decision

Do not promote this exact rule as the default defensive layer.

Do not retune 3/5, SMA200, VOL30 or confirmation length on the now-opened 2026-03-29 -> 2026-09-26 period.

The period is no longer untouched and must never again be called clean OOS for this candidate.

Preserve the candidate as:

- evidence that internal-crypto low-vol defense can materially reduce drawdown;
- evidence that the current breadth exit is too persistent in at least one independent recovery regime;
- a benchmark for any future preregistered defensive mechanism.

## Runtime impact

Production/paper-live behavior changed:

`NONE`

Migration required:

`NO`

## Test level

`GITHUB_ACTIONS_BACKTEST_EXECUTED + HISTORICAL_REPRODUCTION_GATE + TRUE_UNTOUCHED_2026_VALIDATION`

Residual risks:

- only one independent ~6-month validation period;
- evaluation resets state at the window start for all eight starting assets;
- strongest-extreme conflict semantics are used by the reproducible runner;
- the rule was originally designed after earlier weak regimes;
- the untouched period happened to contain a strong recovery where conservative breadth exit was costly;
- future modifications must use a new preregistered validation boundary rather than tuning against this now-opened period.
