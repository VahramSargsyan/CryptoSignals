# WEIGHTED_MULTI_SIGNAL_V1 — Historical Diagnostic R1

WORKFLOW_MODE: **DIAGNOSTIC_ONLY**  
RISK_CLASS: **L2 — research diagnostics only**  
SOURCE_STRATEGY: `WEIGHTED_MULTI_SIGNAL_V1/1.0.0`  
SOURCE_COMMIT: `9c402f8363e1151a8062be24931741460ce2ea3f`  
SOURCE_RUN: `WEIGHTED_MULTI_SIGNAL_V1_2021_2025_R1`  
WORKFLOW_RUN_ID: `36174420370`  
ARTIFACT_DIGEST: `sha256:44e9a9724009740ad2c32c0d86455f68d82a7918912bd1cb7cc6beb25463c5ce`  
DIAGNOSTIC_DATE: **2026-09-25**

## Scope

This report diagnoses the already-frozen historical run. It does **not** change:

- strategy formulas;
- five 20-point weights;
- 60/100 threshold;
- 3-block minimum;
- LONG_ONLY_SIGNAL_FLIP_V1 execution;
- V1/V2/LINK candidates;
- paper-live or live behavior.

No post-result filter in this report is validated as a new strategy rule.

## Evidence integrity

The original runtime completed:

- 13/13 Binance Spot symbols;
- 1D candles;
- 2021-01-01 inclusive through 2026-01-01 exclusive;
- fee 10 bps;
- slippage 5 bps;
- next-candle-open execution;
- 88/88 unit/regression tests PASS.

For this diagnostic pass, signal-block membership was reconstructed from the saved canonical candles using the exact frozen v1 formulas. Reconstructed actionable signal counts matched the original runtime exactly for every symbol:

- ATOM 350
- BNB 321
- BTC 369
- DOGE 332
- ETH 374
- OSMO 236
- SAGA 127
- SOL 358
- TIA 195
- TON 98
- TWT 304
- W 88
- XRP 273

This exact count match is the main cross-check that the diagnostic signal reconstruction is aligned with the tested strategy.

## 1. Global baseline

391 closed trades:

- win rate: 39.13%
- pooled average net trade return: +7.74%
- pooled median net trade return: -3.69%
- pooled profit factor: 2.013
- positive total-return symbols: 6/13
- benchmark beaten: 4/13
- median symbol max drawdown: 80.20%

These pooled statistics are descriptive cross-symbol aggregates, not one multi-asset portfolio equity curve.

## 2. Year stability

| Entry year | Trades | Win rate | Avg trade | Median trade | PF | Sum of trade returns* |
|---|---:|---:|---:|---:|---:|---:|
| 2021 | 42 | 54.76% | +63.76% | +2.98% | 7.777 | +26.779 |
| 2022 | 76 | 34.21% | -2.47% | -5.93% | 0.663 | -1.875 |
| 2023 | 75 | 46.67% | +8.79% | -1.48% | 3.160 | +6.594 |
| 2024 | 74 | 48.65% | +8.03% | -0.34% | 2.243 | +5.943 |
| 2025 | 124 | 26.61% | -5.80% | -7.78% | 0.425 | -7.187 |

*Sum of individual trade returns is diagnostic only and is not portfolio return.

2021 alone contributes 26.779 of the total 30.254 pooled net-return sum, about **88.5%**.

Stability check:

- 2022–2025: PF 1.134, avg +1.00%, median -4.18%;
- 2023–2025: PF 1.263, avg +1.96%, median -3.56%;
- 2024–2025: PF 0.928, avg -0.63%, median -5.24%.

Therefore the headline PF 2.013 is not stable across calendar periods.

## 3. Tail concentration

Total positive-return mass across winning trades: 60.106.  
Total loss mass: 29.851.

Concentration of positive-return mass:

- largest 1 winner: 13.5%;
- largest 3 winners: 30.9%;
- largest 5 winners: 43.4%;
- largest 10 winners: 56.5%;
- largest 20 winners: 67.9%.

Sensitivity when the largest winning trades are removed:

| Removed largest winners | Remaining PF | Avg trade | Median trade | Sum of trade returns |
|---|---:|---:|---:|---:|
| 1 | 1.741 | +5.67% | -3.69% | +22.131 |
| 3 | 1.392 | +3.02% | -3.85% | +11.704 |
| 5 | 1.140 | +1.09% | -4.03% | +4.191 |
| 10 | 0.877 | -0.97% | -4.20% | -3.682 |
| 20 | 0.646 | -2.85% | -4.68% | -10.571 |

The strategy is therefore materially **right-tail dependent**.

The largest individual historical winner was BNB, entered on 2021-02-04 signal and exited 2021-05-18, with approximately +812% net trade return. Large SOL and DOGE 2021 trades also contribute strongly.

## 4. The five-block strategy collapses into one dominant three-block pattern

Entry combination distribution:

| Entry block combination | Trades | Win rate | Avg trade | Median trade | PF | Sum returns |
|---|---:|---:|---:|---:|---:|---:|
| MACD + StochRSI + Volume/Candle | 292 | 42.47% | +11.36% | -3.06% | 2.470 | +33.173 |
| MA + StochRSI + Volume/Candle | 46 | 23.91% | -3.86% | -6.69% | 0.507 | -1.774 |
| MA + MACD + StochRSI | 22 | 31.82% | -5.85% | -6.17% | 0.377 | -1.287 |
| MA + MACD + StochRSI + Volume/Candle | 12 | 25.00% | +2.40% | -2.68% | 1.319 | +0.289 |
| MA + StochRSI + Bollinger | 10 | 40.00% | -2.70% | -4.23% | 0.503 | -0.270 |
| Other sparse combinations | 9 | — | — | — | — | approximately +0.123 |

The dominant `MACD + StochRSI + Volume/Candle` entry accounts for **292/391 = 74.7%** of trades.

Its pooled PF is 2.470 and pooled trade-return sum is +33.173.

All other entry combinations together:

- 99 trades;
- win rate 29.29%;
- average -2.95%;
- median -4.69%;
- PF 0.599;
- sum -2.919.

So the first historical edge attributed to the five-block strategy is actually concentrated almost entirely in one three-block sub-pattern.

## 5. Block-presence association

This is descriptive association, not causal proof.

| Entry block present | Trades | Avg trade | Median | PF | Sum returns |
|---|---:|---:|---:|---:|---:|
| MA trend | 91 | -3.38% | -6.12% | 0.569 | -3.078 |
| MACD | 330 | +9.75% | -3.59% | 2.255 | +32.164 |
| StochRSI | 390 | +7.77% | -3.69% | 2.016 | +30.290 |
| Bollinger reversion | 18 | -0.62% | -1.65% | 0.837 | -0.111 |
| Volume/Candle | 356 | +8.93% | -3.55% | 2.169 | +31.786 |

Absence associations:

- no MA trend: 300 trades, PF 2.468;
- no MACD: 61 trades, PF 0.548;
- no Volume/Candle: 35 trades, PF 0.424.

StochRSI appears in 390/391 entries, so under the current 60-point / 3-of-5 rule it behaves almost like a de facto required block rather than one equal optional voter.

## 6. Score distribution

- score 60: 379/391 trades, PF 2.035;
- score 80: 12/391 trades, PF 1.319;
- score 100: 0 trades.

The intended 0–100 strength model is therefore not yet producing a broad strength distribution. In practice almost every trade is a three-block 60-point decision.

## 7. Market-regime diagnostics

Regime tags use the existing H8 trailing-only regime engine at the entry signal candle.

### BTC trend

Excluding early UNKNOWN history:

| BTC regime | Trades | Avg | Median | PF |
|---|---:|---:|---:|---:|
| BTC_BULL | 199 | -0.56% | -5.92% | 0.936 |
| BTC_BEAR | 117 | +6.63% | -2.91% | 2.075 |
| BTC_TRANSITION | 58 | +3.38% | -1.89% | 1.549 |

The early UNKNOWN bucket has only 17 trades and contains several exceptional 2021 winners; it must not be interpreted as an actionable regime.

### Broad crypto trend

| Broad regime | Trades | Avg | Median | PF |
|---|---:|---:|---:|---:|
| BROAD_BULL | 150 | +4.08% | -3.57% | 1.521 |
| BROAD_BEAR | 186 | +0.01% | -4.60% | 1.002 |
| BROAD_SIDEWAYS | 50 | +12.89% | -1.39% | 3.110 |

### BTC volatility

| Volatility | Trades | Avg | Median | PF |
|---|---:|---:|---:|---:|
| VOL_HIGH | 83 | -3.89% | -7.08% | 0.608 |
| VOL_NORMAL | 201 | +1.49% | -3.56% | 1.207 |
| VOL_LOW | 83 | +2.68% | -3.70% | 1.418 |

Again, UNKNOWN contains early-history 2021 tail winners and is not a usable regime.

### Market-wide volume participation

| Volume regime | Trades | Avg | Median | PF |
|---|---:|---:|---:|---:|
| VOLUME_HIGH | 219 | +10.61% | -2.62% | 2.537 |
| VOLUME_LOW | 127 | +5.44% | -5.69% | 1.627 |
| VOLUME_NORMAL | 45 | +0.23% | -7.27% | 1.028 |

Among already-defined H8 dimensions, market-wide volume participation is one of the clearest descriptive separators in this historical run.

## 8. Dominant three-block pattern by regime

For `MACD + StochRSI + Volume/Candle` entries only:

- BTC_BULL: PF 1.097;
- BTC_BEAR: PF 2.284;
- BTC_TRANSITION: PF 1.719;
- BROAD_BULL: PF 1.695;
- BROAD_BEAR: PF 1.173;
- BROAD_SIDEWAYS: PF 4.762;
- VOL_HIGH: PF 0.746;
- VOL_NORMAL: PF 1.472;
- VOL_LOW: PF 1.528;
- VOLUME_HIGH: PF 3.092;
- VOLUME_LOW: PF 2.017;
- VOLUME_NORMAL: PF 1.097.

The pattern is therefore historically weakest in high BTC volatility and strongest during broad-sideways / high-participation conditions. This is a post-hoc observation only.

## 9. Directional event-study instability

### BUY signal, +14 candles

| Year | Observations | Avg directional return | Median |
|---|---:|---:|---:|
| 2021 | 234 | +12.96% | +0.95% |
| 2022 | 174 | -4.04% | -5.51% |
| 2023 | 342 | +3.73% | +1.04% |
| 2024 | 461 | +6.51% | +2.13% |
| 2025 | 434 | -5.22% | -5.56% |

### SELL signal, +14 candles

Directional return for SELL is inverted, so positive means price subsequently moved down as the SELL direction predicted.

| Year | Observations | Avg directional return | Median |
|---|---:|---:|---:|
| 2021 | 81 | -3.17% | -1.97% |
| 2022 | 413 | +0.59% | +1.18% |
| 2023 | 299 | -2.21% | +1.46% |
| 2024 | 336 | -3.38% | -2.04% |
| 2025 | 629 | +2.38% | +2.50% |

The strategy's directional edge changes materially by year. In 2025 BUY events are strongly weak while SELL events are directionally positive, which is particularly important because the current trading engine is long-only: SELL closes longs but does not monetize short exposure.

## 10. Exit-pattern association

A major diagnostic association appears in exits:

- entry `MACD + StochRSI + Volume/Candle` -> exit same combination:
  - 116 trades;
  - win rate 51.72%;
  - median +2.06%;
  - PF 4.110;
  - sum +32.976.

- entry dominant combination -> exit `MA + MACD + StochRSI`:
  - 50 trades;
  - win rate 16.00%;
  - median -7.56%;
  - PF 0.154.

- entry dominant combination -> exit `MA + MACD + StochRSI + Volume/Candle`:
  - 26 trades;
  - win rate 0%;
  - median -13.60%;
  - PF 0.000.

These groups are associations with the state at exit, not proof that a specific exit block causes the loss. A trade may already be deeply negative before the exit state appears.

## Diagnostic conclusions

1. **The headline PF 2.013 overstates stability.** It is heavily influenced by 2021 and a small right tail of exceptional winners.
2. **The five-block design is not behaving as five equally useful voters.** Roughly 75% of entries are one three-block pattern.
3. **MACD + StochRSI + Volume/Candle is the only high-sample entry combination with strong pooled historical economics.**
4. **MA participation is associated with weaker entries in v1.** This does not prove moving averages are useless; it shows the current binary MA-vote formulation does not add historical robustness.
5. **Bollinger contributes very few entries** and therefore is not functioning as a meaningful equal fifth block in v1.
6. **The score is effectively binary around 60.** 379/391 trades have exactly 60 strength.
7. **Year/regime dependence is material.** 2022 and 2025 fail, while 2021/2023/2024 are profitable in pooled terms.
8. **Market-wide volume participation appears useful descriptively.**
9. **High-volatility entry conditions are weak in the tested history.**
10. **The SELL side contains information in some years, especially 2025, but the current long-only engine can use it only as an exit signal.**

## Candidate hypotheses for a future version

These are **research hypotheses only**, not authorized fixes:

- treat the dominant MACD + StochRSI + Volume/Candle pattern as a separately versioned hypothesis rather than hiding it inside a nominal five-block vote;
- test whether MA should be a regime/filter/context feature instead of a +20 vote;
- test whether Bollinger belongs in a separate mean-reversion strategy family;
- test regime-aware entry eligibility using predeclared H8 dimensions;
- test BUY and SELL directions independently;
- evaluate a separately frozen short-capable research engine if SELL directional evidence remains persistent;
- use walk-forward / future holdout validation before any acceptance claim.

No V2 should be created by simply selecting the best post-hoc filters from this report and retesting them on the same 2021–2025 sample.
