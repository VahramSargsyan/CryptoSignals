# LINK Grid Volatility / Multi-Indicator Exit Regime Research V1

Date: **2026-09-26**  
Strategy: **VAHRAM_LINK_LEVEL_GRID_V1**  
Decision status: **TESTED / NOT PROMOTED / RISK-MANAGEMENT CANDIDATE ONLY**  
Runtime impact: **NONE**  
Migration required: **NO**

## Purpose

This document records exploratory research on using volatility and multi-indicator regime logic to choose between three exit grids:

- NARROW: Micro +3 / Mid +12
- WIDE: Micro +6 / Mid +18
- EXTRA_WIDE: Micro +9 / Mid +24

Grid entries were not changed.

## Shared methodology

Universe:

- LINKUSDT
- ETHUSDT
- SOLUSDT
- BNBUSDT
- BTCUSDT

Method:

- Binance Spot, 1D;
- 6 years total candles;
- first 1095 candles prehistory only;
- final 1096 candles evaluation;
- same causal H/L schedule as canonical grid research;
- 10 bps fees;
- 5 bps slippage;
- linear-depth allocation p=1;
- 100% positive-profit reinvestment;
- runner 0%;
- indicator state for candle T used only information available through T-1.

Reference:

| Profile | Five-asset geometric return | Median max drawdown | Closed exits |
| --- | ---: | ---: | ---: |
| BASE +1/+10 | +226.41% | 35.98% | 4,670 |
| WIDE +6/+18 | **+253.03%** | 38.67% | 1,595 |

## Volatility indicators

Exploratory calculations used:

- ATR% (14-day Wilder-style ATR divided by close);
- Bollinger bandwidth (20-day, 2 standard deviations);
- 20-day realized volatility of log returns;
- 252-day causal percentile ranks for volatility state classification.

The repository already contains Bollinger Bands. ATR/ADX calculations used in this exploratory test were local research calculations and were not added to production/shared indicator code.

## Family A — higher volatility -> wider exit

Mapping:

- low volatility -> +3/+12
- medium volatility -> +6/+18
- high volatility -> +9/+24

Results:

| Selector | Geometric return | Median DD |
| --- | ---: | ---: |
| ATR percentile | +239.99% | 39.40% |
| Bollinger bandwidth percentile | +241.23% | 38.78% |
| ATR + BBW consensus | +245.66% | 38.71% |
| ATR + BBW + realized-vol median consensus | +241.97% | 38.21% |

**Decision:** not promoted. Constant WIDE remained stronger.

## Family B — higher volatility -> faster exit

Symmetric inverse mapping:

- low volatility -> +9/+24
- medium volatility -> +6/+18
- high volatility -> +3/+12

Results:

| Selector | Geometric return | Median DD |
| --- | ---: | ---: |
| ATR percentile inverse | **+251.54%** | 39.54% |
| BBW percentile inverse | +247.73% | 39.68% |
| ATR + BBW consensus inverse | +244.47% | 39.78% |
| three-volatility consensus inverse | +244.32% | 39.71% |

ATR inverse was the closest volatility-only candidate to WIDE.

ATR inverse asset returns:

- ETH: +134.13%
- BTC: +85.00%
- SOL: +785.36%
- LINK: +513.33%
- BNB: +128.26%

Compared with WIDE, ATR inverse improved BTC and BNB but remained lower on ETH, SOL and LINK.

### 15 asset-year stability check

ATR inverse versus WIDE:

- wins: **11/15** asset-year windows;
- mean annual difference: about **+0.45 percentage points**;
- median annual difference: about **+0.96 percentage points**.

However, full-period compounded geometric return remained lower than WIDE, and median drawdown was slightly higher. The largest negative yearly difference was concentrated in a strong SOL period, indicating that the volatility brake can miss part of large trend tails.

**Decision:** interesting robustness signal, but not promoted.

## Family C — MA chooses +3/+12 vs +9/+24

Rule:

- bullish MA pair -> +9/+24
- bearish MA pair -> +3/+12

Results:

| MA pair | Five-asset geometric return |
| --- | ---: |
| 20/50 | +239.99% |
| 25/50 | +236.96% |
| 45/90 | +237.98% |
| 40/90 | +235.04% |
| 50/100 | +234.76% |
| 100/200 | +231.86% |

This is materially different from the earlier BASE/WIDE selector experiment, but the conclusion is similar: MA alone did not improve the shared WIDE result.

**Decision:** not promoted.

## Family D — multiple indicators

Tested simple combinations included:

- volatility consensus + MA50/100;
- inverse-volatility consensus + MA50/100;
- three-volatility consensus + MA50/100;
- volatility + ADX14 + MA50/100;
- volatility + ADX14 + directional movement (+DI/-DI).

Selected results:

| Multi-indicator rule | Geometric return | Median DD |
| --- | ---: | ---: |
| volatility consensus + MA | +235.58% | 39.59% |
| inverse volatility + MA | +251.53% | 39.75% |
| 3-vol consensus + MA | +235.07% | 39.53% |
| inverse 3-vol consensus + MA | +247.99% | 39.72% |
| volatility + ADX + MA | +235.54% | 39.87% |
| volatility + ADX + DI | +233.75% | 39.97% |

Adding more indicators did **not** improve the result. More features increased rule complexity without producing a better shared historical outcome.

## Family E — volatility as a brake, not a full regime engine

Because constant WIDE is already strong, a more conservative idea was tested: keep +6/+18 normally and switch to +3/+12 only in the top volatility tercile.

Results:

| Brake | Geometric return | Median DD |
| --- | ---: | ---: |
| ATR high-vol brake | +246.42% | 37.74% |
| BBW high-vol brake | **+249.84%** | **36.82%** |
| three-volatility consensus brake | +248.88% | 37.57% |

The BBW brake produced lower return than WIDE but also materially lower median drawdown:

- WIDE: +253.03%, median DD 38.67%
- BBW brake: +249.84%, median DD 36.82%

Across 15 asset-year windows, BBW brake beat WIDE in 9/15, with approximately flat average/median return difference.

Interpretation: volatility may be more useful as a **risk brake** than as a return-maximizing exit selector.

**Decision:** risk-management candidate only; not promoted.

## Current decision

1. Keep grid entries unchanged.
2. Keep constant WIDE +6/+18 as the strongest shared return reference from this research set.
3. Do not promote the tested +3/+12 / +6/+18 / +9/+24 volatility engine.
4. Do not assume multiple indicators improve the strategy merely because they add information; the tested MA/volatility/ADX combinations did not.
5. Preserve ATR-inverse as an interesting robustness observation.
6. Preserve BBW high-volatility brake as an interesting drawdown-reduction candidate.
7. Any future volatility/regime proposal must state how it differs materially from these tested families.

## Evidence provenance boundary

These results are exploratory local backtests using the same canonical five-asset candle files and mechanics used by repository-backed grid research.

The exact volatility/ADX experiment harness is not yet committed as a repository-owned reproducible script/artifact.

Therefore this file is:

- durable research memory;
- a duplicate-work guard;
- not sufficient evidence for strategy promotion.

Before promotion, encode the chosen hypothesis in repository-owned code, freeze its rules, and produce reproducible GitHub evidence on appropriately separated validation data.

## Paper-live boundary

No paper-live profile was changed.

Current forward profiles remain:

- CONTROL_BASE: +1/+10
- CANDIDATE_WIDE: +6/+18

No result in this document authorizes live-money execution.
