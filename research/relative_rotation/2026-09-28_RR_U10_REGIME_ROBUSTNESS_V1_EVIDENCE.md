# RR U10 REGIME ROBUSTNESS V1 — EVIDENCE

Date: 2026-09-28
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Runtime identity

- Frozen universe: `RR_TARGET_U10_CANDIDATE_HBAR_V1`
- GitHub Actions run: `36427874155`
- Source commit: `a3a24eecc176ddc5454298679ea2366cfd0c9693`
- Artifact ID: `10971777711`
- Artifact: `rr-u10-regime-robustness-v1-1`
- Artifact SHA256: `6efd885db940ebcda828beec6f34a1e05cec1c325aafe6d431d8054c311560e4`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Question

Does the frozen single-book U10 relative-rotation strategy earn through both bull and bear markets, or is its historical return mainly bull-regime dependent?

## Strategy frozen before inspection

Universe:

`TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR`

Mechanics:
- Binance Spot D1
- 180d rolling median
- ARM 15%
- reversal 3%
- confirmed T -> next-open execution
- strongest max-dislocation routing
- 0.1% modeled transition cost
- all 10 possible starting assets
- MATURE: 2023-10-31 -> 2026-09-26

## Regime definitions

Existing H8 rules were reused without threshold tuning.

### BTC trend
- BTC_BULL: close > SMA200 and SMA50 > SMA200
- BTC_BEAR: close < SMA200 and SMA50 < SMA200
- BTC_TRANSITION: enough history but mixed condition

### Broad U10 trend
Breadth = fraction of frozen U10 above 50D SMA.

- BROAD_BULL: breadth >= 0.60
- BROAD_BEAR: breadth <= 0.40
- BROAD_SIDEWAYS: otherwise

## Classification

`BULL_DEPENDENT`

The base concentrated U10 is **not historically all-weather in absolute-return terms**.

It earns very strongly during bull regimes and loses during bear regimes.

## BTC-trend results

| Regime | Days | Episodes | Strategy median conditioned return | Worst start | Positive starts | Conditioned DD | Median episode | Positive episodes | BTC same days | U10 market same days |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| BTC_BULL | 589 | 12 | +1257.2% | +964.3% | 100% | -47.1% | +11.9% | 91.7% | +199.3% | -68.7% |
| BTC_BEAR | 348 | 6 | **-15.4%** | **-15.4%** | **0%** | -62.4% | -3.7% | 50.0% | -30.4% | -61.9% |
| BTC_TRANSITION | 125 | 17 | +184.6% | +184.6% | 100% | -40.8% | +0.1% | 52.9% | +17.5% | -30.0% |

Interpretation:
- U10 loses money during BTC_BEAR on every tested starting asset;
- however, the -15.4% conditioned loss is substantially smaller than BTC (-30.4%) and the median U10 market proxy (-61.9%) on those same days;
- this is relative resilience, not positive bear-market profitability.

## Broad-U10-trend results

| Regime | Days | Episodes | Strategy median conditioned return | Worst start | Positive starts | Conditioned DD | Median episode | Positive episodes | BTC same days | U10 market same days |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| BROAD_BULL | 420 | 39 | **+30366.8%** | +23791.5% | 100% | -19.9% | +3.4% | 89.7% | +752.2% | +351.0% |
| BROAD_BEAR | 590 | 41 | **-88.3%** | **-88.3%** | **0%** | -91.9% | -2.9% | 17.1% | -77.2% | -98.0% |
| BROAD_SIDEWAYS | 52 | 35 | -8.7% | -8.7% | 0% | -15.5% | +0.2% | 51.4% | +26.0% | -5.7% |

This is the strongest evidence of bull dependence:
- almost all compounded historical upside is earned on BROAD_BULL days;
- BROAD_BEAR days destroy most of the accumulated capital if compounded by themselves;
- broad sideways is slightly negative overall.

The very large BROAD_BULL conditioned return is mathematically consistent with the full-period return because bear-day losses offset a large part of bull-day gains.

## Bear performance by calendar year

### BTC_BEAR
- 2024: -15.3%
- 2025: -1.4%
- 2026 through cutoff: +1.3%

BTC_BEAR is therefore not uniformly losing every calendar year, but the full conditioned history remains negative.

### BROAD_BEAR
- 2024: -54.0%
- 2025: -62.5%
- 2026 through cutoff: -31.9%

BROAD_BEAR is negative in every tested calendar year.

## Episode behavior

### BTC_BEAR
Across 6 contiguous bear episodes:
- median episode return: -3.7%
- worst episode: -18.2%
- best episode: +22.1%
- positive episode rate: 50%
- longest episode: 276 days

### BROAD_BEAR
Across 41 contiguous bear episodes:
- median episode return: -2.9%
- worst episode: -31.1%
- best episode: +22.9%
- positive episode rate: about 17%
- longest episode: 93 days

So the bear weakness is not caused by one single crash episode.

## Starting-state robustness

For both BTC_BEAR and BROAD_BEAR:
- positive start rate = 0%;
- all 10 starting assets eventually converge to essentially the same bear-regime conditioned result.

This matches the previously documented strong convergence property of the relative-rotation graph.

## Relationship to previous bull-neutral stress

Previous exhaustive evidence showed positive bull-neutralized diagnostics after removing positive strategy returns on a narrower definition of extreme broad-bull days.

That test asked:

`Does all performance disappear if unusually strong broad-bull positive days are neutralized?`

This regime test asks a different and stronger question:

`Is the strategy profitable when market conditions are explicitly classified as bear?`

Answer: **no**.

The two findings are compatible:
- the strategy is not dependent on only the most extreme bull days;
- but it remains strongly dependent on generally bullish market regimes for absolute profitability.

## Research implication

The next distinct research problem is not another U10 token search.

It is a **defensive regime overlay** problem.

The repository already contains a research candidate:

`DEFENSIVE_LOW_VOL_CRYPTO_SMA200_BREADTH_3_5_CONFIRM3_VOL30`

Its purpose is conceptually aligned with this finding:
- detect broad weakness;
- move physical capital toward a low-volatility crypto defensive asset;
- preserve shadow/core routing;
- return when breadth recovers.

This evidence does not automatically approve that overlay. It motivates testing it specifically against the newly documented bear-regime failure.

## No-repeat rule

Do not rerun this exact regime attribution on the same U10/history merely to reproduce the result.

A repeat requires:
- materially new unseen data;
- changed regime semantics;
- changed strategy semantics/costs;
- changed universe;
- or an explicitly separate defensive-overlay / forward-validation test.

## Production boundary

- current paper/live U9 unchanged;
- frozen U10 candidate unchanged;
- defensive overlay remains disabled in production;
- Telegram unchanged;
- no exchange execution.

## Residual risks

- historical/post-selection evidence;
- common U10 panel begins in 2023;
- regime labels are daily and trailing, not macroeconomic cycle labels;
- conditioned return isolates returns on labeled days and is not a standalone strategy that enters/exits exactly at every regime boundary;
- no spread/liquidity stress beyond 0.1% transition cost.
