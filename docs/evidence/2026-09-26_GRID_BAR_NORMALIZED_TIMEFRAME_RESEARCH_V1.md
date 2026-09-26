# Grid Bar-Normalized Timeframe Research V1

Date: **2026-09-26**  
Canonical strategy reference: **VAHRAM_LINK_LEVEL_GRID_V1**  
Research hypothesis: **VAHRAM_GRID_BAR_NORMALIZED_V1**  
Workflow mode: **STRESS_TEST_ONLY**  
Decision status: **PARTIAL BACKTEST / SCALE-FREE HYPOTHESIS NOT CONFIRMED / NO PROMOTION**  
Runtime impact: **NONE**  
Migration required: **NO**

## Research question

Can the Grid become timeframe-universal if its memory is defined as a fixed number of candles rather than a fixed calendar duration?

Frozen bar-normalized hypothesis:

- H/L lookback = **1095 bars**
- H/L refresh = **30 bars**
- 16 main levels / 64 sublevels
- p=1 linear-depth allocation
- WIDE Micro = **+6 sublevels**
- WIDE Mid recovery = **+18 sublevels**
- existing Mid percentage targets unchanged
- BASE control = +1 / +10
- 100% positive-profit reinvestment
- 10 bps fee
- 5 bps slippage
- no permanent runner

Only the candle duration changes.

Approximate real-time memory represented by 1095 bars:

| Timeframe | 1095 bars |
| --- | ---: |
| 1D | ~3.0 years |
| 4H | ~182.5 days |
| 1H | ~45.6 days |
| 15m | ~11.4 days |
| 5m | ~3.8 days |
| 1m | ~18.25 hours |

This is a materially different hypothesis from the canonical 3-year-memory strategy and is **not** a replacement for it.

## Test limitations

A single pinned multi-timeframe dataset spanning all five canonical assets and all requested timeframes was not available in the current runtime.

Therefore the pass has two evidence classes:

1. **same-calendar D1 vs 4H comparison** on BTC/ETH;
2. **mechanics / cost smoke tests** on H1, 15m, 5m and 1m using public Binance OHLCV snapshots with different historical windows.

Returns from different smoke-test windows must **not** be ranked against each other.

No raw third-party dataset was copied into CryptoSignals.

## Same-calendar D1 vs 4H test

Common evaluation:

- 2024-07-01 through 2026-03-31
- BTC and ETH
- same frozen Grid rules
- D1 uses 1095 daily bars (~3 years memory)
- 4H uses 1095 four-hour bars (~182 days memory)

### Results

| Asset | TF | WIDE return | WIDE DD | BASE return | Buy & Hold |
| --- | --- | ---: | ---: | ---: | ---: |
| BTC | **1D** | **+4.08%** | 10.96% | +3.64% | +8.45% |
| BTC | **4H** | **-11.72%** | 40.60% | -16.22% | +8.70% |
| ETH | **1D** | **+21.56%** | 26.51% | +19.49% | -38.92% |
| ETH | **4H** | **+6.20%** | 44.56% | +0.35% | -39.29% |

Interpretation:

- WIDE remained stronger than BASE on both timeframes and both assets.
- However, the bar-normalized 4H variant was materially worse than D1 on the same calendar period.
- BTC 4H became negative while D1 remained positive.
- ETH 4H remained positive and beat Hold, but with much worse drawdown than D1.

This is direct evidence **against** the simple claim that “1095 candles” is fully scale-invariant.

## H1 preliminary test

Public Binance hourly dataset:

- BTC / ETH / BNB
- data start: 2020-08-01 23:00
- 1095-hour warm-up
- evaluation: 2020-09-16 14:00 through 2020-12-05 00:00
- 1906 evaluation bars (~79 days)

| Asset | WIDE | WIDE DD | BASE | Buy & Hold | Closed exits |
| --- | ---: | ---: | ---: | ---: | ---: |
| BTC | **+11.42%** | 3.95% | +9.37% | +70.49% | 317 |
| ETH | **+18.82%** | 10.84% | +15.16% | +53.83% | 240 |
| BNB | **+14.98%** | 7.32% | +11.72% | +2.90% | 363 |

WIDE was profitable and beat BASE on all three assets.

This establishes that the Grid mechanics can function with ~45.6 days of H/L memory, but the sample is too short and old to prove H1 universality.

## Lower-timeframe BTC smoke tests

These use different public snapshots and are **not** cross-timeframe performance rankings.

| TF | Evaluation after 1095-bar warm-up | WIDE | BASE | B&H | Avg sublevel | Approx WIDE +6 target | Target / ~0.30% round-trip cost |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 15m | 3905 bars (~40.7d) | **+8.59%** | +5.31% | +32.95% | 0.192% | 1.151% | **3.84x** |
| 5m | 1962 bars (~6.8d) | **+1.31%** | +0.21% | +4.59% | 0.0978% | 0.587% | **1.96x** |
| 1m | 346 bars (~5.8h) | **+0.039%** | -0.094% | +0.346% | 0.0276% | 0.165% | **0.55x** |

The cost denominator is an intentionally rough round-trip estimate:

- buy fee 0.10%
- buy slippage 0.05%
- sell fee 0.10%
- sell slippage 0.05%
- total ~0.30%

Actual realized execution cost can differ.

## Transaction-cost boundary

The clearest structural finding is the compression of grid spacing as timeframe falls.

### H1

Observed average sublevel:

- BTC: ~0.472%
- ETH: ~0.632%
- BNB: ~0.663%

Approximate WIDE Micro +6 target:

- BTC: ~2.83%
- ETH: ~3.79%
- BNB: ~3.98%

Gross target / rough cost ratio:

- ~9.4x to ~13.3x

Costs are not the dominant structural problem at H1 in this preliminary sample.

### 15m

Average sublevel ~0.192%.

WIDE +6 target ~1.15%, about **3.84x** the rough round-trip cost.

Still plausible.

### 5m

Average sublevel ~0.0978%.

WIDE +6 target ~0.587%, only **1.96x** the rough round-trip cost.

This becomes cost-sensitive.

### 1m

Average sublevel ~0.0276%.

WIDE +6 target ~0.165%, only **0.55x** the rough round-trip cost.

Under the current fee/slippage assumption the canonical WIDE Micro geometry is structurally too small on this snapshot. The tiny positive smoke-test return is not meaningful performance evidence because only 346 post-warmup minutes were available.

## Additional 4H bear-window diagnostics

A separate recent 4H snapshot (2025-09-23 through 2026-03-24 after warm-up) produced:

| Asset | WIDE | BASE | Buy & Hold |
| --- | ---: | ---: | ---: |
| ETH | -28.25% | -27.69% | -48.90% |
| SOL | -40.05% | -36.98% | -58.55% |
| LINK | -37.84% | -34.05% | -57.51% |

Here Grid reduced the damage relative to Hold but did not convert a persistent decline into positive returns. BASE was better than WIDE on all three.

This is consistent with the prior volatile-asset finding: oscillation/recovery matters; raw bar frequency does not create edge by itself.

## Main findings

1. **Grid mechanics survive below D1.**
   - H1, 15m and 5m all produced valid Grid activity and positive preliminary WIDE results in their tested snapshots.
   - WIDE beat BASE in the BTC H1/15m/5m smoke tests and in BTC/ETH/BNB H1.

2. **The simple scale-free hypothesis is not confirmed.**
   - On the same 2024-07 to 2026-03 period, D1 materially beat 4H on BTC and ETH.
   - Therefore “1095 bars” is not sufficient by itself to preserve the canonical behavior.

3. **Calendar memory appears to matter.**
   - D1 1095 bars encodes ~3 years of market structure.
   - 4H 1095 bars encodes only ~182 days.
   - The worse same-window 4H result suggests that the long memory may be part of the edge, not merely an arbitrary candle count.

4. **There is a natural lower-timeframe transaction-cost boundary.**
   - H1 comfortably clears current costs.
   - 15m still has room.
   - 5m is borderline/cost-sensitive.
   - 1m is structurally compressed below the assumed round-trip cost with unchanged +6/+18 geometry.

5. **WIDE is not universally optimal across timeframe/regime.**
   - In the recent 4H bear diagnostic, BASE beat WIDE on ETH/SOL/LINK.

## Research decision

Status: **BAR-NORMALIZED HYPOTHESIS PARTIALLY SUPPORTED MECHANICALLY, NOT VALIDATED AS UNIVERSAL**

Do not change the canonical Grid.

Do not change paper-live.

Do not promote a 1m or 5m version from this pass.

The next clean experiment should use one pinned 1m Binance Spot dataset and aggregate it deterministically into:

- 1m
- 5m
- 15m
- 1h
- 4h
- 1d

on the exact same assets and exact same calendar window.

Then run two preregistered families:

### A. BAR_NORMALIZED

- H/L = 1095 bars
- refresh = 30 bars

### B. TIME_NORMALIZED

Hold real-time memory approximately constant while bar count changes.

This directly tests whether the key invariant is:

- number of observations;
- calendar memory;
- or some interaction between memory, volatility and transaction costs.

## Provenance / reproducibility boundary

This first pass used multiple public GitHub-hosted Binance OHLCV snapshots because a single complete multi-timeframe archive was not materialized in the runtime.

Examples used:

- hourly BTC/ETH/BNB from `priyanshux/cryptopy`;
- 4H BTC/ETH from `ElvisTV/backtestingbots_s1`;
- 15m BTC from `anothermario/bota`;
- 5m BTC from `sergio12S/RL-trader`;
- 1m BTC from `GwakJunwoo/binance_trader`;
- D1 BTC/ETH from the pinned `olaxbt/ai-market-maker` snapshot.

No third-party raw data was copied into CryptoSignals.

The exact temporary multi-source harness is not yet repository-owned; therefore this document is decision memory / hypothesis evidence rather than promotion-grade reproducibility.

## Duplicate-work guard

Future chats should not present as new:

- 1095-bar H1 smoke test;
- BTC 15m / 5m / 1m cost-compression test;
- same-window D1 vs 4H BTC/ETH comparison;
- the finding that 1m +6 target can fall below current round-trip costs;
- the finding that D1 outperformed 4H on the tested same calendar window.

A future test must materially improve the data boundary, ideally via one pinned 1m source deterministically aggregated to every timeframe.

## Paper-live boundary

No current paper-live profile or symbol universe changed.

Still frozen:

- CONTROL_BASE +1/+10
- CANDIDATE_WIDE +6/+18
- p=1
- 1095 **daily** H/L bars

No result here authorizes live-money execution.
