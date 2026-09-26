# Multi-Asset Relative Rotation Graph — Stress Test

Date: 2026-09-26  
Status: RESEARCH_ONLY / NOT_ACCEPTED_FOR_LIVE_EXECUTION  
Workflow mode: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING  
Manual execution required: YES

## 1. Research question

Test whether capital can rotate dynamically across a network of approved crypto assets rather than remain constrained to one fixed pair.

Universe used in the first full graph:

- ATOM
- TWT
- PEPE
- BNB
- SOL
- TRX
- AAVE
- LINK

This creates 28 unique undirected pair relationships.

The route is not predetermined. The engine starts from the currently held asset and follows only a confirmed relative-rotation transition.

## 2. Reference signal engine

Common reference parameters used for the initial graph stress test:

- timeframe: 1D
- relative ratio: TARGET / CURRENT according to pair orientation
- rolling median lookback: 180 days
- ARM threshold: 15%
- post-ARM extreme tracking: yes
- reversal confirmation: 3%
- execution: next daily open
- swap cost assumption: 0.1%
- closed candles only

State concept:

NO_SIGNAL -> PREWATCH -> ARMED -> EXTREME_TRACKING -> REVERSAL_CONFIRMING -> ROTATION_CONFIRMED

These are research defaults, not production-approved universal parameters.

## 3. Data

Primary graph stress-test dataset:

- public GitHub research dataset containing Binance-style 1D OHLCV files for all eight assets
- common history begins with PEPE listing: 2023-05-05
- common end used: 2026-03-28
- no duplicate dates found
- no invalid OHLC rows found

Cross-check against CryptoSignals:

- ATOM external vs repository canonical: 499/500 common OHLC rows exact
- TWT external vs repository canonical: 499/500 common OHLC rows exact
- only mismatch was 2025-06-11, where the older repository file appears to contain an unfinished daily candle
- LINK sampled values matched the Binance GitHub Actions historical artifact used earlier

Interpretation:

The external dataset is acceptable for independent stress testing, but it is not promoted to repository canonical market data.

## 4. Pair screen — 28 pairs

Reference rolling-window screen:

- 1-year evaluation windows
- windows shifted by approximately 60 days
- both starting assets tested
- same 180d / 15% / 3% engine
- 0.1% swap cost

Selected strongest results by median excess vs 50/50:

| Pair | Median excess vs 50/50 | Windows beating 50/50 | Median excess vs best HODL | Median rotations/year |
|---|---:|---:|---:|---:|
| PEPE/BNB | +124.5% | 83.3% | +53.5% | 5 |
| PEPE/TRX | +86.2% | 72.2% | -21.2% | 6 |
| PEPE/AAVE | +40.1% | 66.7% | +25.2% | 4 |
| PEPE/SOL | +35.5% | 77.8% | +17.3% | 4 |
| TRX/LINK | +32.0% | 66.7% | -12.9% | 4 |
| SOL/AAVE | +26.9% | 66.7% | +2.9% | 4 |
| PEPE/LINK | +24.6% | 66.7% | +10.4% | 2.5 |
| TWT/PEPE | +24.2% | 61.1% | +6.4% | 3 |
| TRX/AAVE | +23.8% | 72.2% | +3.3% | 3 |
| ATOM/TWT | +23.5% | 83.3% | +6.4% | 3 |

Important:

This table is a research screen only. A pair that looks weak alone can still be useful as a transition edge inside a larger graph.

## 5. First full network OOS test

Out-of-sample test period:

2025-03-29 -> 2026-03-28

All 28 edges were kept available.

Conflict rule for baseline TOP-1:

When multiple confirmed outbound transitions existed from the currently held asset, choose the candidate with the strongest confirmed relative extreme. A causal pair-specific percentile rank was also tested and selected the same route in this OOS sample.

### OOS results by starting asset

| Start asset | Network TOP-1 return | HODL return |
|---|---:|---:|
| ATOM | +41.3% | -60.3% |
| TWT | +42.9% | -50.0% |
| PEPE | +46.7% | -53.6% |
| BNB | +47.2% | +2.2% |
| SOL | +3.4% | -33.2% |
| TRX | +40.6% | +37.0% |
| AAVE | +39.3% | -41.6% |
| LINK | +42.0% | -36.5% |

Summary:

- median network TOP-1 return: +41.6%
- equal-weight 8-asset benchmark: -29.5%
- best single HODL in the period: TRX +37.0%
- median rotations: about 8
- network max drawdown: approximately -62.2%

This is the strongest current evidence for the multi-asset rotation concept, but one OOS year is not enough for acceptance.

## 6. Example observed route

Starting from TWT during the OOS year:

TWT -> PEPE -> TRX -> LINK -> TWT -> ATOM -> PEPE -> ATOM -> TWT

This produced a genuine historical cycle:

TWT -> PEPE -> TRX -> LINK -> TWT

The cycle was not specified in advance.

## 7. Router sensitivity

The graph result is highly dependent on what happens when several transitions are simultaneously confirmed.

| Conflict rule | Median OOS return |
|---|---:|
| strongest relative extreme | +41.6% |
| strongest confirmation dislocation | +41.6% |
| causal pair-specific percentile | +41.6% |
| weakest relative extreme | +2.4% |
| skip conflict | -30.5% |
| first available pair | -28.0% |

Interpretation:

A large part of the observed alpha appears to come from routing quality, not merely pair signal generation.

This is a major research risk and must be walk-forward tested.

## 8. Parameter sensitivity

Selected perturbations of the common engine:

| Parameters | Median OOS return |
|---|---:|
| 120d / 15% ARM / 3% reversal | -14.3% |
| 180d / 12.5% / 3% | +32.4% |
| 180d / 15% / 2% | +23.0% |
| 180d / 15% / 3% | +41.6% |
| 180d / 15% / 5% | -21.9% |
| 180d / 20% / 3% | +53.7% |
| 240d / 15% / 3% | +63.6% |

Do not optimize to the best visible row. The table shows that the mechanism is materially parameter-sensitive.

## 9. Pair filtering warning

A strict train-only edge filter was tested before the OOS year.

Only BNB/SOL and TRX/AAVE passed the strict train filter.

The resulting OOS network failed badly:

- median OOS return: approximately -34.8%

Interpretation:

Permanent deletion of apparently weak pair edges may remove transitions that become useful in a later regime.

The research direction should focus on dynamic routing quality rather than a simple GOOD_PAIR / BAD_PAIR binary classification.

## 10. Split-router test

Capital splitting was tested only when a real routing conflict existed.

| Conflict behavior | Median OOS return | Approx max DD |
|---|---:|---:|
| TOP-1 100% | +41.6% | -62.2% |
| equal split TOP-2 | +31.7% | -65.2% |
| equal split TOP-3 | +6.0% | -65.3% |
| 50% HOLD + 50% TOP-1 on conflict | +17.9% | -61.5% |
| 1/3 HOLD + 1/3 + 1/3 TOP-2 | +22.2% | -64.6% |

Interpretation:

- TOP-2 splitting remained profitable in this OOS year but gave up about 10 percentage points versus TOP-1.
- TOP-3 diversification nearly destroyed the observed edge.
- simple diversification did not materially improve drawdown.
- next test should examine weighted TOP-1/TOP-2 mixes such as 80/20, 70/30 and 60/40.

## 11. HODL role

HODL remains necessary in three distinct roles.

### Benchmark

Every rotation result must be compared with:

- HODL of the starting asset
- best HODL among the available assets
- equal-weight multi-asset benchmark

### System state

If no confirmed outbound transition exists:

HOLD_CURRENT_ASSET

No forced trade should occur.

### Possible independent CORE sleeve

Future research may compare:

- 100% rotation
- CORE HODL + ROTATION
- staking-aware ATOM CORE + ROTATION

The CORE sleeve must be independent. It must not leave tiny residual fragments after every trade.

## 12. Same-window GRID vs ROTATION comparison

User requirement:

Compare the existing grid research strategy and the new rotation network on the exact same one-year period instead of comparing one year of rotation against three years of grid evidence.

Common period:

2025-03-29 -> 2026-03-28

Grid implementation:

VAHRAM_LINK_LEVEL_GRID_V1 research code from corrected-grid branch, using:

- 3-year causal prehistory requirement
- 1095-day H/L range
- refresh every 30 daily candles
- linear_depth_reserved allocation
- 10 bps fee
- 5 bps slippage
- Micro + Mid capital pools
- same strategy mechanics documented in the corrected grid research implementation

Independent same-window reproduction on available historical data:

| Strategy / asset | 1Y return | Max DD |
|---|---:|---:|
| Rotation network TOP-1 median | +41.6% | -62.2% |
| LINK grid | +20.9% | -31.6% |
| ETH grid | +12.5% | -26.8% |
| SOL grid | -4.1% | -29.0% |
| BNB grid | -4.3% | -14.0% |
| BTC grid | -3.0% | -8.6% |

Interpretation:

On this exact one-year period, the new rotation network produced the stronger return result than each reproduced grid case.

However:

- grid drawdowns were materially smaller
- grid and rotation solve different problems
- rotation has only one full OOS year so far
- the same-window grid reproduction is an independent local reconstruction of the documented research implementation, not a new official GitHub Actions evidence run
- the result must not be treated as proof that rotation dominates grid generally

## 13. Hybrid research hypothesis

Potential architecture:

RISK_OFF
  -> RELATIVE ROUTER
      -> selected asset
          -> GRID or HOLD inside selected asset

Interpretation:

- rotation chooses WHERE capital should reside
- grid may determine HOW capital works while residing in that asset
- risk-off decides WHEN capital should leave crypto entirely

No hybrid backtest has been accepted yet.

## 14. Next tests

Priority order:

1. weighted conflict routing:
   - TOP-1 100%
   - TOP-1/TOP-2 80/20
   - 70/30
   - 60/40
   - 50/50
2. rolling and walk-forward OOS router validation
3. per-conflict attribution for every routing fork
4. CORE HODL + ROTATION sleeve tests
5. staking-aware ATOM accounting
6. independent RISK_OFF layer
7. GRID + ROTATION hybrid using the same market window
8. consistent live-data extension through 2026-09-26 before any current signal use

## 15. Safety / execution boundary

No automatic live conversion is authorized.

Any future real trade remains manually approved by Vahram.

## 16. Current verdict

STATUS: PROMISING_RESEARCH / NOT_PRODUCTION_APPROVED

The multi-asset graph passed an important first OOS stress test and showed a real dynamically generated token cycle. It also showed severe router sensitivity, parameter sensitivity and a large drawdown.

TEST_LEVEL: HISTORICAL_GRAPH_STRESS_TEST + 1Y_OOS + SAME_WINDOW_GRID_REPRODUCTION
