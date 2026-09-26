# Grid Volatile-Asset Stress Test V1

Date: **2026-09-26**  
Strategy: **VAHRAM_LINK_LEVEL_GRID_V1**  
Workflow mode: **STRESS_TEST_ONLY**  
Decision status: **HYPOTHESIS REFINED / NO PROMOTION**  
Runtime impact: **NONE**  
Migration required: **NO**

## Research question

Test the hypothesis:

> The grid performs especially well on highly volatile assets.

Requested examples included TWT and PEPE. PEPE could not be tested under the canonical 1095-day H/L warmup using the available pinned daily snapshot because that snapshot contained only 1059 daily rows through 2026-03-28. No shorter-lookback substitute was silently used.

To keep the canonical grid mechanics intact, the executed comparison used four volatile assets with enough prehistory:

- TWT
- DOGE
- AVAX
- SHIB

## Common evaluation window

To compare the four assets on the same market window:

- evaluation start: **2024-05-10**
- evaluation end: **2026-03-28**
- evaluation days: **688**
- H/L lookback: **1095 prior daily candles**
- range refresh: **30 candles**
- fee: **10 bps**
- slippage: **5 bps**
- linear-depth allocation p=1
- full positive-profit reinvestment
- no runner

## Strategies

### Grid WIDE

- Micro +6
- Mid +18 recovery plus existing percentage target logic

### Grid BASE

- Micro +1
- Mid +10 recovery

### Buy & Hold

Full capital deployed at the first evaluation open and held to the end.

### SMA200 long-only

Hold while the previous close is above the 200-day SMA; otherwise cash. Execution occurs at the next open.

### Donchian 20/10 long-only

- enter after a previous close breaks above the preceding 20-day high;
- exit after a previous close breaks below the preceding 10-day low;
- execution at the next open.

## Volatility characteristics

| Asset | Annualized daily vol | Avg daily high-low range | Avg lower wick / open |
| --- | ---: | ---: | ---: |
| TWT | 76.4% | 6.20% | 1.81% |
| DOGE | 92.2% | 7.45% | 2.04% |
| AVAX | 90.9% | 7.20% | 1.90% |
| SHIB | 82.3% | 6.86% | 1.94% |

All four assets were highly volatile by daily realized-volatility and intraday-range measures.

## Results

| Asset | WIDE | WIDE DD | BASE | Buy & Hold | SMA200 | Donchian 20/10 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| TWT | **-24.03%** | 59.83% | -21.81% | -61.47% | -52.34% | -39.83% |
| DOGE | +31.61% | 49.45% | +34.61% | -39.30% | -17.04% | **+87.71%** |
| AVAX | **-40.18%** | 61.87% | -17.34% | -74.77% | -41.53% | -70.41% |
| SHIB | **-47.75%** | 66.22% | -39.27% | -74.80% | -41.11% | -67.09% |

## Key findings

### 1. Volatility alone is not sufficient

TWT, AVAX and SHIB were all highly volatile, yet the grid lost money over the common window.

The grid generally lost **less** than Buy & Hold during persistent declines, but that is risk mitigation, not positive edge.

### 2. DOGE supports the volatility-harvesting idea, but not universal superiority

DOGE:

- WIDE: +31.61%
- BASE: +34.61%
- Buy & Hold: -39.30%
- SMA200: -17.04%
- Donchian 20/10: +87.71%

The grid clearly harvested DOGE's oscillation better than passive holding, but a simple breakout strategy was stronger on this particular window.

### 3. WIDE is not always better than BASE on volatile assets

On all four assets in this common window, BASE was equal or better than WIDE in total return:

- TWT: BASE -21.81% vs WIDE -24.03%
- DOGE: BASE +34.61% vs WIDE +31.61%
- AVAX: BASE -17.34% vs WIDE -40.18%
- SHIB: BASE -39.27% vs WIDE -47.75%

This is important evidence against treating +6/+18 as universally best for every volatile asset.

### 4. Path structure matters more than raw volatility

The current evidence supports a refined hypothesis:

> The grid benefits from high volatility **when that volatility repeatedly mean-reverts / rebounds through its levels**.

It performs much worse when volatility is dominated by a persistent directional decline.

Useful future explanatory variables therefore include:

- number and depth of lower wicks;
- recovery frequency after large down days;
- time-to-recovery from local drawdowns;
- directional persistence / trend strength;
- fraction of daily ranges that retrace within N days;
- ratio of realized oscillation to net directional move.

This is more specific than simply ranking assets by annualized volatility.

## PEPE status

Binance listed PEPE/USDT on 2023-05-05.

The pinned daily research snapshot available in this pass ended on 2026-03-28 and contained **1059 daily rows**, fewer than the canonical **1095-day** H/L warmup.

Therefore:

**PEPE = CANONICAL TEST BLOCKED BY SNAPSHOT COVERAGE**

No 2-year or shortened H/L result is reported as if it were the canonical strategy.

## Research decision

1. Reject the simplistic rule **"more volatility = better grid result."**
2. Preserve the stronger hypothesis **"volatility + repeated recovery / mean reversion = favorable grid environment."**
3. Do not change WIDE or BASE from this test.
4. Do not tune asset-specific exits after seeing these results.
5. Future asset-selection research should classify **path structure**, not just realized volatility.
6. PEPE remains pending until a pinned dataset with >=1095 prehistory plus evaluation candles is materialized.

## Duplicate-work guard

Future chats should not claim TWT, DOGE, AVAX or SHIB are untested for the volatility hypothesis on the 2024-05-10 through 2026-03-28 window.

Future PEPE testing must preserve the canonical 1095-day H/L rule unless explicitly labeled a different strategy variant.

## Paper-live boundary

No paper-live profile changed.

No result here authorizes live-money execution.
