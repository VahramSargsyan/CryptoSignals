# U10 Monthly Surge 3-Bar Fractal Buy-Stop v1 — Evidence

Date: 2026-09-28
Mode: STRESS_TEST_ONLY
Validated branch: research/u10-monthly-surge-fractal3-buystop-v1

Primary GitHub Actions run:
36376455828

Primary source commit:
70a1db37911655e75a8ae56bf30853333c4f8393

Primary artifact:
10951141998

Artifact digest:
sha256:e34c320da326c431a486af8ba13485a0022f597e22dbb3d3607c054fb06ad504

Secondary reused-history run:
36376692360

Secondary artifact:
10950709940

Secondary digest:
sha256:42ed886cf2b00ce2bd5479cf61f48ff742c2d76416d084bbb47c2df93a25859c

Result:
PASS

TEST_LEVEL:
GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Objective

Test one and only one change to the prior structural buy-stop rule:

- 5-bar pivot: 2 left / pivot / 2 right
- 3-bar pivot: 1 left / pivot / 1 right

Everything else remained frozen:
- +100% surge arm
- P1 cash-out after 5% pullback
- P2 cash-out after 10% pullback
- 30% protected cash sleeve
- search activation after -15% U10 reference drawdown
- actual held-token HIGH prices
- lower-only stop ratchet
- reset on U10 asset rotation
- next-session activation after pivot confirmation
- gap-open protection
- 0.1% cash-out / re-entry cost

No parameter sweep was performed.

## Canonical U10

Baseline:

- final equity: 219,485.20 USDT
- max DD: -71.04%

### P1

| Re-entry | Final equity | Delta vs baseline | Max DD | Cash days |
|---|---:|---:|---:|---:|
| Fixed old-peak -25% | 270,653.52 | +23.31% | -66.36% | 66 |
| Moving-peak -25% | 252,642.73 | +15.11% | -68.60% | 41 |
| 5-bar fractal | 258,557.42 | +17.80% | -68.36% | 145 |
| 3-bar fractal | 238,737.35 | +8.77% | -69.03% | 62 |

3-bar versus 5-bar:

- terminal delta: -19,820.07 USDT
- relative delta: -7.67%
- max DD worsened from -68.36% to -69.03%
- total cash days fell from 145 to 62

Thus the 3-bar rule solved confirmation latency but gave back too much of the protective timing advantage.

### P2

| Re-entry | Final equity | Delta vs baseline | Max DD | Cash days |
|---|---:|---:|---:|---:|
| Fixed old-peak -25% | 265,751.95 | +21.08% | -66.36% | 63 |
| Moving-peak -25% | 248,067.33 | +13.02% | -68.60% | 38 |
| 5-bar fractal | 253,741.70 | +15.61% | -68.36% | 142 |
| 3-bar fractal | 234,910.56 | +7.03% | -69.03% | 59 |

3-bar versus 5-bar:

- -18,831.14 USDT
- -7.42%

Again confirmation was faster but economically worse on this path.

## Canonical 3-bar cycles

### 2024-02 surge — P1/P2

Cash-out execution:
2024-03-01

Search activation:
2024-03-16

5-bar re-entry:
2024-05-05
- asset: ATOM
- reference drawdown at re-entry close: -26.37%
- cash duration: 65 days

3-bar re-entry:
2024-04-08
- asset: TWT
- stop: 1.283
- pivot center: 2024-04-06
- confirmation: 2024-04-07
- reference drawdown at re-entry close: -21.54%
- cash duration: 38 days

3-bar entered 27 days earlier.

### 2025-11 surge

5-bar:
- re-entry 2025-12-04
- cash duration 25 days
- reference DD about -31.79%

3-bar:
- re-entry 2025-11-26
- cash duration 17 days
- reference DD about -32.79%

This episode did not clearly demonstrate premature entry despite faster confirmation.

### 2026-01 surge

5-bar:
- re-entry 2026-03-12
- TWT
- reference DD about -29.61%
- P1 cash duration 55 days
- P2 52 days

3-bar:
- re-entry 2026-01-23
- HBAR
- reference DD only about -13.16%
- P1 cash duration 7 days
- P2 4 days

This is the clearest 3-bar failure:
the faster local-high break treated an early bounce as sufficient reversal evidence and returned cash long before the later, deeper correction.

## 791 alternative U10s

### P1 3-bar

- final > ordinary baseline: 791/791 = 100.00%
- max DD improved: 90.14%
- both final and DD improved: 90.14%
- unfinished cycles: 0%

Median terminal delta vs baseline:
+17.34%

q25/q75:
+12.93% / +21.01%

Compared with 5-bar:
- 3-bar better: 112/791 = 14.16%
- 3-bar worse: 679/791 = 85.84%
- equal: 0
- median delta vs 5-bar: -3.49%
- q25/q75: -5.66% / -1.40%

Compared with moving-peak -25%:
- 3-bar better: 53.60%

Compared with fixed old-peak -25%:
- 3-bar better: 19.85%

Median total cash days per universe:
- 3-bar: 69
- 5-bar: 122

The faster rule roughly halved time out of market, but this was usually economically premature.

### P2 3-bar

- final > baseline: 785/791 = 99.24%
- 6 alternatives underperformed their own baseline
- max DD improved: 72.95%
- both improved: 72.19%
- unfinished cycles: 0%

Median terminal delta:
+15.20%

Compared with 5-bar:
- better: 112/791 = 14.16%
- worse: 679/791 = 85.84%
- median effect: -3.41%

Median total cash days:
- 3-bar: 59
- 5-bar: 117

P2 lost more robustness than P1.

## Actual 3-bar re-entry depth

Across 2,227 completed alternative P1 cycles:

Reference drawdown from latest peak at re-entry close:

- deepest: about -42.26%
- 10th percentile: about -35.34%
- 25th percentile: about -35.34%
- median: about -21.54%
- 75th percentile: about -17.79%
- shallowest: about -9.20%

5-bar median had been about -29.61%.

Thus the 3-bar rule materially shifted re-entry toward shallower / earlier reversals.

Cycle cash duration:

- min: 7 days
- q25: 17
- median: 24
- q75: 31
- max: 74

5-bar median cycle duration had been about 47 days.

## Main 2023-2026 conclusion

The 3-bar rule successfully addresses **speed**, but overshoots.

It is not a better replacement for 5-bar on the primary development period.

The evidence indicates:

> One right-side confirmation bar is often insufficient to distinguish a durable reversal from an early bounce.

P1 remains stronger than P2.

The 3-bar version should not replace 5-bar in the current research baseline.

## Secondary reused-history check: 2020-2022 OLD10

History status:
REUSED_HISTORY_SECONDARY_EVIDENCE

This period is already consumed and is NOT untouched OOS validation.

OLD10 baseline:

- final: 56,902.97
- max DD: -81.32%

### 5-bar prior result

P1:
- 50,295.00
- -11.61% vs baseline
- cash days: 85

P2:
- 49,569.15
- -12.89%
- cash days: 83

### 3-bar secondary result

P1:
- 56,083.05
- -1.44% vs baseline
- max DD -80.81%
- cash days: 46
- 3/3 cycles completed

P2:
- 55,329.41
- -2.77%
- max DD -80.81%
- cash days: 44
- 3/3 cycles completed

Thus 3-bar dramatically reduced the specific late-confirmation failure seen in old V-shaped recoveries.

### Old P1 cycles

January 2021:
- cash-out 2021-02-02
- search activated near -24.59%
- 3-bar re-entry 2021-02-05
- reference DD at re-entry: -17.61%
- only 3 cash days

April 2021:
- re-entry 2021-04-30
- reference DD: -14.01%
- 22 cash days

August 2021:
- re-entry 2021-09-15
- reference DD: -19.56%
- 21 cash days

The faster rule therefore solved much of the OLD10 latency problem.

## Combined interpretation

The two periods point in opposite directions:

2020-2022 reused history:
- 3-bar was much better than 5-bar because 5-bar missed fast V-shaped recoveries.

2023-2026 primary topology stress:
- 5-bar was better in 85.84% of alternative U10s because 3-bar often treated temporary bounces as durable reversals.

Therefore the research problem is now clearer:

> The correct confirmation speed appears regime-dependent.

This is not evidence to average the two periods or choose an intermediate value after the fact.

It is evidence that a future rule, if pursued, should identify **reversal quality / regime state causally**, rather than choose a single universal confirmation delay by backtest.

## Guardrail

Do not tune a hybrid using 2020-2022 and then call it out-of-sample.

No live/paper change.

No promotion of 3-bar.

## Residual risks

- 792 topologies share the same market regimes
- 3-bar was proposed after observing the 5-bar OLD10 latency failure
- 2020-2022 is reused historical evidence
- 15% search activation remains development-selected
- OHLC daily bars cannot resolve all intraday path ordering
- transaction friction model is simplified
- historical performance does not establish future performance
