# LINK Grid Capital Distribution / H-L Lookback Research V1

Date: **2026-09-26**  
Strategy: **VAHRAM_LINK_LEVEL_GRID_V1**  
Workflow mode: **STRESS_TEST_ONLY**  
Decision status: **TESTED / NOT PROMOTED / RESEARCH CANDIDATES RECORDED**  
Runtime impact: **NONE**  
Migration required: **NO**

## Research question

Explore two structural parameters of the existing WIDE grid without changing entry logic or paper-live behavior:

1. capital distribution by grid depth;
2. trailing H/L lookback used to draw the 16x4 grid.

WIDE exits were kept fixed:

- Micro: +6 sublevels
- Mid recovery: +18 sublevels
- existing Mid percentage targets unchanged

## Baseline reproduction

Canonical five-asset datasets:

- LINKUSDT
- ETHUSDT
- SOLUSDT
- BNBUSDT
- BTCUSDT

Evaluation:

- 2023-09-25 through 2026-09-24
- 10 bps fee
- 5 bps slippage
- 30-candle range refresh
- no runner
- 100% positive-profit reinvestment

Current WIDE research baseline:

- H/L lookback: 1095 candles (~3y)
- allocation power: p=1 for Micro and Mid
- five-asset geometric return: **+253.03%**
- median max drawdown: **38.67%**
- closed exits: **1,595**

Local research engine reproduced the WIDE baseline exactly.

## Capital-depth distribution

Allocation weights are proportional to:

`level ^ p`

where p=0 means equal capital by main level and p=1 is the current linear-depth research baseline.

Same-power results, 3y H/L:

| p | Geometric return | Median DD |
| ---: | ---: | ---: |
| 0.0 | **+281.59%** | 46.31% |
| 0.5 | +270.56% | 42.43% |
| 1.0 | +253.03% | 38.67% |
| 1.5 | +233.42% | 35.17% |
| 2.0 | +214.08% | 32.31% |
| 2.5 | +196.04% | 30.25% |
| 3.0 | +179.66% | **28.41%** |

Interpretation:

- shallower/equal allocation increases historical return;
- deeper concentration reduces drawdown;
- current p=1 is a middle point rather than the return-maximizer.

The p=0 result is not promoted because it materially increases drawdown and later-window fragility.

### Annual reset stress check

Equal allocation p=0, 3y H/L versus current p=1/3y across 15 asset-year comparisons:

- wins: 9/15
- mean return difference: +13.39 pp
- median difference: +11.57 pp
- but final year five-asset geometric return:
  - current p=1: **+10.97%**
  - p=0: **-0.71%**
- final-year median DD:
  - current: **29.64%**
  - p=0: **41.05%**

This confirms that equal allocation can produce higher long-horizon compound while becoming much less resilient in a difficult recent window.

## Micro / Mid total capital split

With p=1 and 3y H/L:

| Micro / Mid | Geometric return | Median DD |
| --- | ---: | ---: |
| 20 / 80 | **+257.23%** | 39.90% |
| 35 / 65 | +255.13% | 39.63% |
| 50 / 50 | +253.03% | 38.67% |
| 65 / 35 | +250.92% | 37.70% |
| 80 / 20 | +248.80% | **36.73%** |

A Mid-heavier split increases return slightly, but also increases drawdown. This is a modest effect compared with depth allocation and is not promoted.

With p=0, Micro/Mid split had almost no impact on geometric return.

## H/L lookback — same 3-year evaluation

Fair comparison on the same 2023-09-25 through 2026-09-24 trading window:

| H/L lookback | Geometric return | Median DD | Closed exits |
| --- | ---: | ---: | ---: |
| 0.5y | +88.74% | 44.54% | 3,808 |
| 1y | +81.00% | 44.05% | 2,958 |
| 1.5y | +149.51% | 38.00% | 2,382 |
| 2y | +234.05% | 38.43% | 1,756 |
| 2.5y | **+258.58%** | 39.23% | 1,615 |
| 3y | +253.03% | **38.67%** | 1,595 |

Short lookbacks caused much higher turnover and materially lower returns.

The 2.5y region slightly exceeded the current 3y baseline.

## Refined H/L scan around 2.5–3.0 years

With current p=1:

| H/L | Geometric return | Median DD |
| --- | ---: | ---: |
| 2.25y | +261.30% | 38.77% |
| 2.40y | +261.58% | 39.29% |
| 2.60y | +259.59% | 39.40% |
| 2.75y | **+265.94%** | **38.58%** |
| 2.90y | +260.08% | 38.67% |
| 3.00y | +253.03% | 38.67% |

The result is not a single one-day spike: the broad 2.25–2.9y region is competitive with or above 3y on the full period.

### Asset results for p=1 / 2.75y

- LINK: +539.85%
- ETH: +146.08%
- SOL: +816.78%
- BNB: +139.18%
- BTC: +90.05%

Versus p=1 / 3y, 2.75y improved 4/5 assets and was slightly lower on SOL.

### Annual reset stress check for p=1 / 2.75y

Versus current p=1 / 3y across 15 asset-year comparisons:

- wins: 7/15
- mean difference: -0.71 pp
- median difference: 0.00 pp

This means the attractive full-period compound is not yet a robust year-by-year advantage.

Other nearby p=1 windows:

- 2.25y: 9/15 wins; mean +2.66 pp; median +4.79 pp
- 2.40y: 9/15 wins; mean +0.48 pp; median +3.35 pp
- 2.60y: 9/15 wins; mean -0.54 pp; median +0.26 pp
- 2.90y: 7/15 wins; mean +0.92 pp; median 0.00 pp

Interpretation: **2.25–2.4y appears more stable across annual windows, while 2.75y has the best full-period compound.**

## 4-year and 5-year H/L

The canonical saved datasets contain six total years. Therefore:

- a 4y H/L cannot be compared on the same full 3y trading window without at least 7 total years;
- a 5y H/L would require at least 8 total years for the same 3y evaluation.

To avoid fake apples-to-apples claims, longer H/L windows were tested only on shorter common evaluation windows.

### Common final 2-year evaluation — supports up to 4y H/L

| Lookback | Geometric return | Median DD |
| --- | ---: | ---: |
| 2.5y | +40.35% | 34.07% |
| 3y | +44.06% | 34.45% |
| 4y | **+44.95%** | **29.05%** |

On this 2-year window, 4y H/L was attractive, but the result is driven heavily by LINK and is not directly comparable with the full 3-year study.

### Common final 1-year evaluation — supports up to 5y H/L

| Lookback | Geometric return | Median DD |
| --- | ---: | ---: |
| 2.5y | **+14.83%** | 30.44% |
| 3y | +10.97% | 29.64% |
| 4y | +6.35% | 23.93% |
| 5y | +2.62% | **23.46%** |

Longer H/L increasingly reduced drawdown but also reduced return in the latest year.

## Combined capital + H/L observations

The historical return maximum among tested combinations was found toward:

- flatter capital allocation (p closer to 0)
- H/L around 2.75 years

Examples:

| p | H/L | Geometric return | Median DD |
| ---: | ---: | ---: | ---: |
| 0.0 | 2.75y | **+288.44%** | 45.65% |
| 0.25 | 2.75y | +285.66% | 43.64% |
| 0.5 | 2.75y | +280.55% | 41.66% |
| 0.75 | 2.75y | +273.78% | 40.05% |
| 1.0 | 2.75y | +265.94% | **38.58%** |
| 1.0 | 3.0y | +253.03% | 38.67% |

However, the flatter-capital variants deteriorated materially in the latest one-year window.

For 2.75y H/L, final-year five-asset geometric return / median DD:

- p=0.25: +3.24% / 38.35%
- p=0.50: +6.49% / 35.35%
- p=0.75: +9.26% / 32.62%
- p=1.00: **+11.58% / 30.43%**

Thus the current linear-depth allocation remains substantially more defensive in the recent difficult period.

## Current research decision

1. Do **not** promote equal capital p=0 despite its high full-period return.
2. Do **not** change the paper-live capital allocation.
3. The H/L lookback is a more promising structural parameter than previously expected.
4. Preserve **2.25y–2.4y** as the more stable shorter-lookback research band.
5. Preserve **2.75y** as the best full-period p=1 compound candidate.
6. 4y H/L is interesting for drawdown reduction, but a proper 3-year comparison requires at least 7 years of raw candles.
7. 5y H/L is not attractive on the available final-year comparison.
8. Before promotion, obtain longer raw history and preregister a small H/L candidate set rather than tuning dozens of day counts.

## Duplicate-work guard

Future chats should not rediscover as new:

- p=0 equal capital;
- p=0.25 / 0.5 / 0.75 compromise curves;
- Micro/Mid 20/80 through 80/20 split;
- H/L 0.5y, 1y, 1.5y, 2y, 2.5y, 3y;
- refined p=1 H/L 2.25y, 2.4y, 2.6y, 2.75y, 2.9y;
- diagnostic 4y and 5y shorter-window tests.

Any new test must state what data horizon or mechanism is materially different.

## Paper-live boundary

No paper-live profile changed.

Still frozen:

- CONTROL_BASE: +1/+10
- CANDIDATE_WIDE: +6/+18
- linear-depth p=1
- 1095-candle H/L until separately promoted

No result here authorizes live-money execution.
