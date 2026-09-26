# LINK Grid vs Known Strategy Benchmarks V1

Date: **2026-09-26**  
Strategy: **VAHRAM_LINK_LEVEL_GRID_V1**  
Workflow mode: **STRESS_TEST_ONLY**  
Decision status: **BENCHMARK EXECUTED / NO STRATEGY PROMOTION**  
Runtime impact: **NONE**  
Migration required: **NO**

## Purpose

Compare the current LINK-style grid architecture against common external benchmark approaches, excluding the owner's previously rejected strategies.

This is a research comparison, not a claim that any benchmark implementation is the unique or canonical implementation of the named method.

## Shared data and execution

Universe:

- LINKUSDT
- ETHUSDT
- SOLUSDT
- BNBUSDT
- BTCUSDT

Canonical candle artifacts:

- the same six-year Binance Spot daily datasets already used by corrected grid research;
- first three years available as indicator / grid prehistory;
- evaluation window: 2023-09-25 through 2026-09-24.

Costs:

- 10 bps fee;
- 5 bps execution slippage.

Benchmark signal convention:

- signal state is computed using information available through the previous daily close;
- position change executes at the next daily open;
- long-only;
- idle capital receives zero yield;
- no leverage;
- full-position entries/exits for indicator strategies.

Grid convention:

- WIDE uses the existing grid simulator and therefore can execute pre-existing limit entries/exits against the daily candle high/low;
- this is intentional and represents the grid's actual mechanism, not an execution privilege granted to the other strategies.

## Benchmark definitions

### Buy & Hold

Deploy all capital at the first evaluation open and hold through the evaluation end.

### DCA monthly

Split starting capital equally across the 37 calendar months touched by the evaluation window. Deploy one installment at the first available daily open of each month.

### SMA200 trend

Hold the asset when the previous close is above its 200-day simple moving average. Otherwise hold cash.

### SMA50/200 trend

Hold when SMA50 > SMA200 and hold cash otherwise. This is a simple long-only golden-cross / death-cross benchmark.

### 12-month momentum

Hold when the previous close is above the close 365 calendar-trading days earlier. Otherwise hold cash.

### Donchian 20/10

Simple long-only breakout benchmark:

- enter after a close above the prior 20-day high;
- exit after a close below the prior 10-day low.

This is intentionally a stripped-down Donchian/Turtle-style benchmark and does not reproduce the full Turtle position-sizing system.

### Bollinger mean reversion 20,2

Simple benchmark:

- enter after a close below the lower 20-period, 2-standard-deviation band;
- exit after a close back at/above the 20-day middle average.

This is not presented as John Bollinger's official trading system.

### RSI14 30/70 mean reversion

- enter when previous RSI14 < 30;
- exit when previous RSI14 > 70.

### VAHRAM Grid WIDE

- linear-depth allocation p=1;
- 3-year H/L;
- 30-day range refresh;
- Micro +6;
- Mid +18 recovery plus existing percent targets;
- full positive-profit reinvestment;
- no permanent runner.

## Aggregate five-asset results

| Strategy | Geometric total return | Approx 3y CAGR | Median max DD | Worst asset return | Positive assets |
| --- | ---: | ---: | ---: | ---: | ---: |
| **VAHRAM Grid WIDE +6/+18** | **+253.03%** | **+52.27%/yr** | **38.67%** | **+82.26%** | **5/5** |
| Buy & Hold | +196.94% | +43.73%/yr | 67.55% | +69.63% | 5/5 |
| SMA200 trend | +147.05% | +35.18%/yr | 53.60% | +53.39% | 5/5 |
| 12m momentum | +103.70% | +26.77%/yr | 56.27% | +39.62% | 5/5 |
| SMA50/200 trend | +74.78% | +20.46%/yr | 61.48% | -3.27% | 4/5 |
| Donchian 20/10 | +69.87% | +19.32%/yr | 43.90% | -30.17% | 4/5 |
| DCA monthly | +23.50% | +7.29%/yr | 59.75% | +5.58% | 5/5 |
| RSI14 30/70 MR | +14.70% | +4.68%/yr | 45.90% | -23.77% | 3/5 |
| Bollinger MR 20,2 | +11.51% | +3.70%/yr | 44.05% | -38.18% | 4/5 |

For reference, the current BASE grid (+1/+10) on the same corrected datasets previously produced:

- five-asset geometric return: **+226.41%**
- median max drawdown: **35.98%**

Thus the untuned BASE control also exceeded the same-window Buy & Hold geometric return in this development sample, while WIDE increased return at some drawdown cost.

## Per-asset total returns

| Strategy | LINK | ETH | SOL | BNB | BTC |
| --- | ---: | ---: | ---: | ---: | ---: |
| **VAHRAM Grid WIDE** | **+516.80%** | **+137.02%** | **+818.79%** | +123.98% | +82.26% |
| Buy & Hold | +89.16% | +69.63% | +503.04% | **+271.94%** | +220.78% |
| SMA200 | +89.13% | +115.87% | +449.13% | +53.39% | +167.58% |
| 12m momentum | +39.62% | +60.21% | +215.41% | +43.11% | **+247.41%** |
| SMA50/200 | -3.27% | +8.88% | +165.01% | +225.57% | +79.48% |
| Donchian 20/10 | +22.87% | -30.17% | +298.29% | +133.00% | +77.66% |
| DCA monthly | +5.58% | +7.99% | +26.21% | +52.60% | +30.83% |
| RSI14 30/70 | +90.99% | -23.77% | -17.74% | +28.53% | +28.95% |
| Bollinger MR | +60.71% | -38.18% | +26.75% | +32.05% | +3.68% |

Key cross-asset observation:

- WIDE beat Buy & Hold on LINK, ETH and SOL;
- Buy & Hold beat WIDE on BNB and BTC;
- WIDE remained positive on all five assets and had the highest worst-asset return of the tested set.

## Per-asset max drawdown

| Strategy | LINK | ETH | SOL | BNB | BTC |
| --- | ---: | ---: | ---: | ---: | ---: |
| **VAHRAM Grid WIDE** | 38.67% | 39.85% | 52.60% | 27.48% | **17.92%** |
| Buy & Hold | 75.39% | 67.55% | 76.26% | 58.20% | 52.97% |
| SMA200 | 67.97% | 38.41% | 58.75% | 53.60% | 31.91% |
| 12m momentum | 56.27% | 46.69% | 65.60% | 60.07% | 28.10% |
| SMA50/200 | 73.00% | 61.48% | 68.43% | 36.77% | 39.68% |
| Donchian 20/10 | 63.58% | 62.41% | **43.90%** | 38.50% | 38.33% |
| DCA monthly | 63.73% | 59.75% | 68.52% | 53.20% | 47.21% |
| RSI14 30/70 | 45.90% | 58.65% | 53.90% | 38.48% | 28.69% |
| Bollinger MR | 44.05% | 59.31% | 52.26% | **28.80%** | 38.45% |

## Interpretation

On this historical development sample, the grid's main advantage is not simply high return.

The combination observed is:

- high cross-asset geometric return;
- substantially lower drawdown than passive holding;
- no negative asset result;
- particularly strong performance in LINK and SOL;
- much lower BTC drawdown than passive holding.

However, the result must not be interpreted as clean proof of superiority.

### Critical selection-bias caveat

WIDE (+6/+18), the capital curve, and H/L assumptions have already been studied on these same five assets and this same historical period.

Therefore:

- this is **development-sample benchmarking**, not fresh OOS evidence;
- known benchmark parameters were fixed before this comparison, but the grid itself has benefited from prior research on the sample;
- the current paper-live period starting 2026-09-26 remains the clean forward evidence boundary.

The comparison is useful for understanding the character of the grid, not for claiming future dominance.

## Research decision

1. Keep these external strategies as standing benchmarks for future grid research.
2. Do not optimize the benchmark strategies against the same sample merely to create a closer race.
3. Future candidate grid changes should be compared against at least:
   - WIDE;
   - BASE;
   - Buy & Hold;
   - SMA200 trend;
   - Donchian 20/10.
4. Repeat the benchmark on genuinely unseen forward data once enough paper-live history exists.
5. Do not promote WIDE to live-money status from this comparison.

## Duplicate-work guard

Future chats should not rediscover this comparison as new unless:

- the evaluation horizon changes;
- genuinely unseen forward data is used;
- benchmark mechanics are materially changed and clearly preregistered;
- an implementation error is found.

## Paper-live boundary

No paper-live profile changed.

Still frozen:

- CONTROL_BASE: +1/+10
- CANDIDATE_WIDE: +6/+18

This benchmark does not authorize live-money execution.
