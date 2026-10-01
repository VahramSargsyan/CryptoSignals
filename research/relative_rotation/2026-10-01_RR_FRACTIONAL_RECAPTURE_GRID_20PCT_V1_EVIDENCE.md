# RR FRACTIONAL RECAPTURE GRID + 20% ROBUSTNESS V1 — EVIDENCE

Date: 2026-10-01
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Research question

Can Relative Rotation execution be improved without changing its daily path by targeting only a fraction of the price distance from current execution price toward the exact-3% ideal recapture price?

User example:
- current destination price = 14.50
- exact ideal = 14.00
- 60% fractional target = 14.50 - 0.60*(14.50-14.00) = 14.20

Frozen grid:
- fraction: 20%, 40%, 60%, 80%, 100%
- total hard fallback: 3h, 6h, 12h, 24h
- SELL must execute first
- destination BUY target is calculated only after SELL, using the next eligible 1H candle
- BUY is not credited in the same hour as SELL
- if unfinished at timeout, force the remaining transfer at hourly-open market proxy
- 0.1% SELL + 0.1% BUY fee
- DDG/non-applicable transitions remain immediate next-open

Formula:

`target = current + fraction * (ideal - current)`

The target is only allowed to seek favorable price improvement. If the exact ideal is already no better than current market, the leg uses a marketable limit at current.

## Runtime identity

### Grid run

- Branch: `research-run/sequential-limit-recapture-v1`
- GitHub Actions run: `36865207170`
- Source commit: `e6aaf65253cbfd91b5a911a743d8625b2ac528bb`
- Artifact ID: `11163527006`
- Artifact SHA256: `6f5c8419923748a7b37e0ba38e5faa5f0bd8aba6375a46af3765cc15c39cfcc1`

### 20% post-selection robustness run

- GitHub Actions run: `36866188244`
- Source commit: `2cbe09bacfd6a41bb4b4c627fd3fd0c100c7450b`
- Artifact ID: `11163822298`
- Artifact SHA256: `8b600aa72a086d0e3c14a791c04b5f64e1f455e0c43749c44f50676ba8e25003`

TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Controls

Current canonical U10+DDG:

- Validation 1Y: +188.7%
- Last 2Y: +2102.3%
- MATURE: +3588.4%
- MATURE DD: -62.4%

Two-fee immediate next-open control:

- MATURE: +3502.7%
- MATURE DD: -62.5%

## Grid result

The originally proposed 60% target did not improve the strategy.

### 60% fraction

| Timeout | Validation 1Y | Last 2Y | MATURE | MATURE DD | MATURE full-limit rate |
|---:|---:|---:|---:|---:|---:|
| 3h | +169.1% | +1914.7% | +3229.1% | -62.9% | 17.9% |
| 6h | +157.6% | +1721.5% | +2997.8% | -64.2% | 32.1% |
| 12h | +178.1% | +1886.4% | +3307.9% | -62.1% | 53.6% |
| 24h | +152.9% | +1803.4% | +3320.2% | -62.4% | 57.1% |

Thus 60% is still too ambitious / too far from market under the tested causal execution rules.

## Main grid finding: 20% family

### Final windows

| Fraction / timeout | Validation 1Y | Last 2Y | MATURE | MATURE DD |
|---|---:|---:|---:|---:|
| 20% / 3h | +194.6% | +2219.0% | +3804.6% | -61.7% |
| **20% / 6h** | **+198.9%** | **+2246.1%** | **+3848.9%** | **-61.5%** |
| 20% / 12h | +193.5% | +2184.6% | +3736.0% | -61.8% |
| 20% / 24h | +178.8% | +2090.5% | +3612.8% | -62.2% |
| Canonical | +188.7% | +2102.3% | +3588.4% | -62.4% |

20% / 6h beats canonical in all three displayed endpoint windows.

MATURE final-capital factor:

- vs canonical: **1.0706x**
- vs same two-fee next-open: **1.0961x**

This means approximately 7.1% more final capital than the canonical historical result, not +260 percentage points of independent alpha.

## Why short deadlines matter

For 3h / 6h / 12h:
- median pending days = 0
- median RR opportunities while pending = 0

Therefore these candidates normally finish before the next daily RR decision.

This is materially different from the rejected 7-day policies:
the experiment is now much closer to pure execution optimization rather than route modification.

## 20% execution mechanics — MATURE direct routes

| Timeout | Full limit rate | Median completion | Median exchange improvement | 25th percentile | 75th percentile |
|---:|---:|---:|---:|---:|---:|
| 3h | 67.9% | 2.5h | +0.45% | -0.24% | +1.58% |
| **6h** | **82.1%** | **2.5h** | **+0.50%** | **-0.16%** | **+1.58%** |
| 12h | 89.3% | 2.5h | +0.56% | -0.07% | +1.58% |
| 24h | 89.3% | 2.5h | +0.56% | -0.07% | +1.58% |

The nominal target is 20% of the leg-price distance to the ideal.
The realized exchange-rate improvement is lower because:
- SELL and BUY are sequential;
- BUY fraction is recalculated from the destination price after SELL;
- fallback cases can be worse than immediate next-open;
- source and destination co-move.

## Post-selection rolling robustness

Because 20% emerged from the 20-cell grid, a separate robustness pass was run on the fixed 20% family with 3h / 6h / 12h.

This is still not clean OOS.

### Rolling 12-month windows

23 monthly-stepped windows:

| Timeout | Median capital factor vs canonical | Worst factor | Windows beating canonical | Median DD delta |
|---:|---:|---:|---:|---:|
| 3h | 1.040x | 0.950x | 73.9% | +0.86pp |
| **6h** | **1.048x** | **0.963x** | **73.9%** | **+1.14pp** |
| 12h | 1.045x | 0.932x | 60.9% | +1.14pp |

Positive DD delta means less-negative / improved drawdown.

### Rolling 24-month windows

11 windows:

| Timeout | Median capital factor vs canonical | Worst factor | Windows beating canonical |
|---:|---:|---:|---:|
| 3h | 1.079x | 1.042x | **100%** |
| **6h** | **1.077x** | **1.051x** | **100%** |
| 12h | 1.053x | 1.022x | **100%** |

The 20% family therefore shows a broad 3-12h plateau rather than a single isolated 6h spike.

## Start-asset/window robustness

### 12-month observations

230 start-asset/window observations per timeout:

- 3h: 74.3% beat canonical; worst factor 0.932x
- 6h: 72.6% beat canonical; worst factor 0.936x
- 12h: 64.8% beat canonical; worst factor 0.904x

### 24-month observations

110 observations per timeout:

- 3h: 100% beat canonical; worst factor 1.021x
- **6h: 100% beat canonical; worst factor 1.026x**
- 12h: 99.1% beat canonical; worst factor 0.991x

## Event-level bootstrap

MATURE unique direct route episodes: 28.

Median execution-improvement bootstrap:

- 3h: +0.45%; 95% interval **-0.11% to +1.12%**
- **6h: +0.50%; 95% interval +0.07% to +1.21%**
- 12h: +0.56%; 95% interval **+0.21% to +1.34%**

The 6h and 12h event-level median intervals exclude zero in this descriptive bootstrap.

This bootstrap does not remove market-date / asset dependence.

## Interpretation

This is the first tested execution modification that:

1. preserves daily RR timing in the tested windows;
2. uses a two-fee model;
3. beats canonical endpoint returns at 1Y, 2Y and MATURE for a neighborhood of parameters;
4. improves median historical DD slightly;
5. shows positive rolling 24m results across all windows for the 20% family;
6. does not depend on waiting days for ideal prices.

However:

`20_PERCENT_6H = POST_SELECTION_RESEARCH_CANDIDATE`

not:

`PRODUCTION_APPROVED`.

20% was identified after observing the preregistered 20-cell grid.
The robustness pass reuses the same historical period.

## Classification

`60_PERCENT_FRACTIONAL_RECAPTURE = REJECTED`

`20_PERCENT_SHORT_TIMEOUT_FAMILY = VIABLE_RESEARCH_CANDIDATE`

`20_PERCENT_6H = FORWARD_SHADOW_CANDIDATE`

`LIVE_EXECUTION_CHANGE = NOT_APPROVED`

## Next admissible step

Preferred next step:
- freeze 20% / 6h without further parameter tuning;
- forward-shadow every new RR transition;
- record:
  - source sell target and fill time;
  - destination buy target and fill time;
  - fallback price;
  - realized exchange improvement vs immediate execution;
  - fees;
  - whether route timing remained identical;
- compare against live/paper canonical execution without actually changing capital routing.

Optional historical checks:
- transport to an older compatible pre-PEPE universe as a separate stratum;
- exchange-specific maker/taker fee and minimum-notional simulation;
- minute-level verification of wick-touch fills.

Do not tune 15/20/25% or 4/6/8h on the same history before a forward benchmark is frozen.

## Production boundary

No production/live/paper/Telegram/exchange order behavior changed.

## Residual risks

- 20% was selected post-grid;
- 1H wick touch is optimistic for full fill;
- order-book queue, partial fill, spread, slippage and exchange outages are not modeled;
- only 28 unique MATURE direct route episodes drive event-level execution statistics;
- DDG routes remain immediate next-open and are not optimized by this mechanism;
- rolling windows overlap and are not independent.
