# Pre-OOS V1 vs V2 baseline — 2026-09-25 R2

Status: BACKTEST_EXECUTED_PRE_OOS_RESEARCH  
Acceptance decision: NOT ALLOWED  
Protected blind period: 2026 (not opened)  
Source commit: `7b17f2544005e2da02e672c9ae86edc057a3bf35`  
GitHub Actions run: `36113077578`  
Artifact digest: `sha256:dbee81e66b3ac71bfdee8851ab462f1419908c188563f36f5173b52cd61300e7`

## Verification

- 45 / 45 unit and regression tests passed.
- 13 / 13 tracked symbols completed.
- 0 runtime error files.
- 26 strategy evidence JSON files were produced.
- All datasets passed the current duplicate/order/gap/grid/OHLC/null/negative-volume checks.
- Listing-aware starts were preserved instead of inventing pre-listing history.

## Fixed comparison boundary

Period: 2021-01-01 through the closed 2025-12-31 daily candle.  
Execution engine: `LONG_ONLY_SIGNAL_FLIP_V1`.  
Signal availability: candle T close.  
Entry/exit execution: next candle open.  
Fee: 10 bps.  
Slippage: 5 bps.  
Event-study horizons: +1 / +3 / +7 / +14 candles.

## Descriptive trading results

Across all 13 symbol datasets:

| Metric | VAHRAM_ORIGINAL_V1 | VAHRAM_TRUE_STOCHRSI_V2 |
|---|---:|---:|
| Signals | 1296 | 1161 |
| Trades | 149 | 146 |
| Pooled trade win rate | 56.38% | 54.79% |
| Pooled average trade return | -0.71% | -1.13% |
| Pooled median trade return | 4.13% | 2.20% |
| Pooled profit factor | 0.933 | 0.893 |
| Symbols with positive strategy total return | 4 / 13 | 3 / 13 |
| Symbols beating same-symbol benchmark | 3 / 13 | 3 / 13 |
| Median symbol max drawdown | 78.51% | 81.17% |

These aggregate return statistics are descriptive only. Symbols have different listing histories and this is not a multi-asset portfolio backtest.

## Signal-level finding worth preserving

The pooled event study separates BUY from SELL rather than hiding them inside one average.

For BUY signals, both variants had positive average directional returns at +1, +3, +7 and +14 candles. The TRUE StochRSI variant was numerically higher at each of those horizons in this pre-OOS sample.

For SELL signals, average directional return was negative at every tested horizon for both variants. Under the event-study definition, that means price tended to move against the SELL direction after those signals on average.

This does **not** prove that BUY should be accepted or SELL should be removed. It identifies a concrete hypothesis for H8 regime analysis and later exit-rule research.

## No strategy verdict yet

Neither strategy is marked ACCEPTED or REJECTED from this run.

Reasons:

1. 2026 is still blind.
2. acceptance thresholds were intentionally not chosen after looking at the results;
3. regime analysis is not yet applied;
4. the current trading engine is a named research interpretation and does not reproduce legacy 25/50/100 partial sizing;
5. several newer assets have shorter histories.

Next research boundary: H8 market-regime classification, then freeze acceptance gates before opening the 2026 OOS period.
