# RR U10 TOTAL SMA25/50/100 CROSS EVENT STUDY V1 — EVIDENCE

Date: 2026-09-28  
Workflow mode: STRESS_TEST_ONLY  
Production/live changes: NONE

## Runtime identity

- Frozen universe: `RR_TARGET_U10_CANDIDATE_HBAR_V1`
- Market-cap cache: `data/market/cryptocap_total_d1.csv`
- Cache SHA256: `d0c9b3bac915e484069c6302ddce1a0b050abbb8de155e4106e39cf6e519d3ee`
- GitHub Actions run: `36441429077`
- Source commit: `14cd7d23c918e23c61ed2ae245904ab2f21bf596`
- Artifact ID: `10978980735`
- Artifact SHA256: `83d1ce89492a5ce557039a6fcc471cd09ef195d3a425126b539aa763dc7c2eea`
- TEST_LEVEL: `GITHUB_ACTIONS_REPOSITORY_CACHE_STRESS_TEST`

Market-cap history was read only from the repository cache. No TradingView refresh was used.

## Question

Does a fast market-cap moving-average crossover act as a useful early warning for U10?

Tested:
- SMA25 vs SMA50
- SMA25 vs SMA100
- SMA50 vs SMA100
- bullish/bearish sequence A: SMA25 crosses SMA50, then SMA25 crosses SMA100
- bullish/bearish sequence B: SMA25 crosses SMA50, then SMA50 crosses SMA100

All U10 event returns were normalized to 100 USDT on event date and aggregated across all 10 frozen starting states.

## Coverage

- MATURE: 2023-10-31 -> 2026-09-26
- 43 raw crossover events
- 20 completed preregistered sequences
- horizons: 7 / 14 / 30 / 60 / 90 calendar days

## Main bullish finding

### SMA25 crosses above SMA50 — earliest alert

This was **not** a reliable positive trading signal by itself.

| Horizon | Events | Median U10 | 100 USDT -> | Positive events |
|---|---:|---:|---:|---:|
| 7d | 11 | -5.9% | 94.12 | 18.2% |
| 14d | 11 | -6.9% | 93.05 | 36.4% |
| 30d | 11 | -6.3% | 93.68 | 45.5% |
| 60d | 11 | +7.5% | 107.53 | 54.5% |
| 90d | 10 | -4.9% | 95.11 | 40.0% |

Interpretation:

`SMA25 > SMA50` is too noisy to be a standalone buy/entry signal.

It may be useful as an **attention trigger** because it arrives early, but historical U10 performance immediately after the cross was often negative.

## Bullish sequence A — SMA25 > SMA50, then SMA25 > SMA100

Completed events: 5.  
Median confirmation lag: about 9 days.

| Horizon | Events | Median U10 | 100 USDT -> | Positive events |
|---|---:|---:|---:|---:|
| 7d | 5 | -1.4% | 98.56 | 20.0% |
| 14d | 5 | +7.8% | 107.76 | 60.0% |
| 30d | 5 | -4.0% | 95.97 | 40.0% |
| 60d | 4 | +12.2% | 112.17 | 75.0% |
| 90d | 4 | +25.7% | 125.66 | 75.0% |

Interpretation:

This is more informative than the raw 25/50 cross, but still noisy on 7-30 day horizons.

## Bullish sequence B — SMA25 > SMA50, then SMA50 > SMA100

This was the strongest tested bullish confirmation structure.

Completed events: 5.  
Median confirmation lag: about 29 days.

| Horizon | Events | Median U10 | 100 USDT -> | Positive events |
|---|---:|---:|---:|---:|
| 7d | 5 | +6.4% | 106.45 | 60.0% |
| 14d | 5 | +11.7% | 111.68 | 60.0% |
| 30d | 5 | +3.6% | 103.61 | 80.0% |
| 60d | 4 | **+22.7%** | **122.65** | **100.0%** |
| 90d | 4 | +21.8% | 121.80 | 75.0% |

The corresponding raw `SMA50_CROSS_ABOVE_100` events showed:
- 7d +8.2%
- 14d +13.7%
- 30d +9.4%
- 60d +27.9%
- 90d +25.5%

This supports the interpretation that the 50/100 cross is a stronger confirmation layer than 25/50, but it arrives later.

## Current 2026 recovery example

The current recovery produced the complete bullish cascade:

1. 2026-07-22: SMA25 crossed above SMA50
2. 2026-08-23: SMA25 crossed above SMA100
   - 32 days after initial alert
3. 2026-08-27: SMA50 crossed above SMA100
   - 36 days after initial alert
   - cached market state: `BULL_BUILDING`

For the 2026-08-27 sequence-B confirmation, actual historical U10 forward result through the cutoff was:

| Horizon | U10 median return | 100 USDT -> | TOTAL |
|---|---:|---:|---:|
| 7d | +17.6% | 117.57 | +1.0% |
| 14d | +15.7% | 115.74 | -3.4% |
| 30d | **+92.7%** | **192.69** | +7.1% |

No 60/90d observation exists yet for that event at the 2026-09-26 cutoff.

This single recent event is one reason the small historical sample must not be overinterpreted.

## Bearish crossover finding

Bearish crosses were **not reliable U10 exit signals**.

### SMA25 crosses below SMA50

Median U10:
- 7d +3.1%
- 14d +1.8%
- 30d +14.6%
- 60d +2.8%
- 90d +32.6%

### Bearish sequence A — 25 below 50, then 25 below 100

Median U10:
- 7d +0.8%
- 14d +4.9%
- 30d +11.5%
- 60d +1.9%
- 90d +43.3%

### Bearish sequence B — 25 below 50, then 50 below 100

Median U10:
- 7d +7.5%
- 14d +2.9%
- 30d **-7.4%**
- 60d +6.8%
- 90d +57.1%

At 30d, sequence-B bearish events coincided with a median TOTAL decline of -10.1%, so the market deterioration signal was real. But U10's relative-rotation mechanism often recovered or profited despite broad-market weakness.

Therefore:

`BEARISH_MA_CROSS != AUTOMATIC_U10_EXIT`

This is consistent with earlier regime evidence:
- U10 is bull-dependent over broad bear regimes in aggregate;
- but short/medium individual bear-crossover windows can still be profitable because the strategy rotates between assets.

## Frozen interpretation

### Attention hierarchy

1. **SMA25 / SMA50 cross**
   - early
   - noisy
   - suitable as `WATCH / PAY_ATTENTION`
   - not supported as standalone buy/sell rule

2. **SMA25 / SMA100 cross**
   - intermediate confirmation
   - still noisy on short horizons

3. **SMA50 / SMA100 cross after SMA25/50**
   - later
   - historically the strongest bullish confirmation in this sample
   - small sample: only 5 completed bull sequences

Frozen descriptive label:

`SMA25_50 = EARLY_ATTENTION; SMA50_100 = STRONGER_CONFIRMATION`

Not production-approved.

## Research implication

A more promising next hypothesis is not "trade every crossover."

It is a layered regime/attention model combining:
- SMA25/50 = early alert
- SMA25/100 and/or SMA50/100 = confirmation
- TOTAL vs SMA200/SMA300 = long-trend filter
- existing FULL_BULL / BULL_BUILDING / BEAR_BUILDING / FULL_BEAR states

That combined model requires a separate preregistered stress test.

## No-repeat / production boundary

Do not rerun this exact event study on the same cached history merely to reproduce it.

A new run requires:
- new unseen dates,
- changed crossover semantics,
- changed U10 mechanics/universe,
- or a separately preregistered combined-filter/forward-validation test.

No live/paper, Telegram, exchange, allocation, or universe behavior changed.
