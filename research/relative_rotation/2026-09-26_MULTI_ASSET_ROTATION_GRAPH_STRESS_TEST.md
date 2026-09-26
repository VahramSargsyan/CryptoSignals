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


## 17. Weighted TOP-1 / TOP-2 conflict split

Follow-up stress test completed after the first graph report.

Rule:
- if only one outbound transition is confirmed, rotate 100% to it;
- only at a true multi-signal conflict, split between the best two candidates ranked by the causal pair-specific percentile / relative extreme ordering.

Same OOS period: 2025-03-29 -> 2026-03-28.

| TOP-1 / TOP-2 conflict weight | Median OOS return | Worst start return | Approx median max DD |
|---|---:|---:|---:|
| 100 / 0 | +41.6% | +3.4% | -62.2% |
| 80 / 20 | +37.9% | +0.3% | -63.3% |
| 70 / 30 | +35.9% | -1.3% | -63.9% |
| 60 / 40 | +33.8% | -2.9% | -64.5% |
| 50 / 50 | +31.7% | -4.6% | -65.2% |

Interpretation:
- every additional allocation to TOP-2 reduced the median return in this OOS year;
- diversification toward TOP-2 did not improve drawdown and instead made it slightly worse;
- 80/20 preserved much of the return but still failed to provide a risk benefit in this sample;
- therefore TOP-2 splitting is not justified as a drawdown-control mechanism by this test alone;
- keep weighted splits as robustness evidence, not as a production rule.


## 18. Walk-forward / sequential-window stability test

Follow-up test completed on 2026-09-26.

Goal:
Check whether the TOP-1 router remains useful across sequential, non-overlapping market regimes rather than only in the previously highlighted 2025-03-29 -> 2026-03-28 OOS year.

Method:
- same 8 assets / 28 pairs
- same 180d rolling median / 15% ARM / 3% reversal
- no retuning between windows
- causal pair-specific percentile used only as the conflict tie/ranking layer
- next-open execution
- 0.1% cost
- each window starts from each of the eight possible starting assets

### 180-day sequential windows

| Window | Median network return | Equal-weight benchmark | Best HODL | Positive starts | Median trades |
|---|---:|---:|---:|---:|---:|
| 2023-10-31 -> 2024-04-27 | +374.2% | +128.8% | +532.5% | 8/8 | 7 |
| 2024-04-28 -> 2024-10-24 | -29.2% | +13.2% | +63.3% | 0/8 | 1.5 |
| 2024-10-25 -> 2025-04-22 | +138.3% | +9.3% | +52.7% | 8/8 | 6.5 |
| 2025-04-23 -> 2025-10-19 | +20.1% | +25.1% | +83.2% | 5/8 | 1.5 |
| 2025-10-20 -> 2026-03-28 | -44.9% | -47.7% | -1.5% | 0/8 | 3.5 |

Interpretation:
- the router is not regime-independent;
- 3 of 5 sequential 180-day windows were positive;
- 2 of 5 were strongly negative;
- the network beat the equal-weight benchmark in 3 of 5 windows;
- the strongest early window did not beat the best single HODL because PEPE itself rose about +532% over that window;
- the strategy therefore must not be described as an always-positive bear-market engine.

### 120-day sequential windows

Median network returns by non-overlapping 120-day window:

- 2023-10-31 -> 2024-02-27: +405.9%
- 2024-02-28 -> 2024-06-26: -32.1%
- 2024-06-27 -> 2024-10-24: -30.4%
- 2024-10-25 -> 2025-02-21: +132.7%
- 2025-02-22 -> 2025-06-21: +9.5%
- 2025-06-22 -> 2025-10-19: +54.1%
- 2025-10-20 -> 2026-02-16: -30.8%

Summary:
- 4 of 7 windows were positive;
- 5 of 7 beat the equal-weight benchmark;
- two consecutive weak windows in 2024 show a sustained regime problem, not one isolated bad month.

### Failure-mode attribution

Bad window 1: 2024-04-28 -> 2024-10-24

Common behavior:
- most starting assets eventually converged into ATOM;
- after the final transition into ATOM, the strategy often remained there for about 150 days;
- ATOM then lost approximately 44% from the final transition date to the end of the window;
- example: TWT -> ATOM executed 2024-06-03, then no further confirmed outbound transition occurred before 2024-10-24.

Bad window 2: 2025-10-20 -> 2026-03-28

Common behavior:
- routes eventually converged into TWT;
- the final ATOM -> TWT transition executed 2026-02-05;
- the network then remained in TWT for 52 days;
- TWT lost approximately 39% from that execution date to the end of the window.

Key finding:

The dominant failure mode is not only a wrong branch choice at a conflict. It is **post-transition stale holding**: once the network converges into an asset, relative pair logic may remain silent while the held asset falls sharply in absolute terms.

Research implication:

A separate absolute-risk / stale-hold layer is justified for testing. It must be independent from pair routing and must not be fitted to these two failures after the fact.

Suggested next stress-test layer:
- baseline market-wide RISK_OFF candidate, defined before testing;
- compare no-risk-off vs simple causal absolute-trend filters;
- do not optimize the threshold/window to the best historical row;
- preserve manual approval for all real execution.
