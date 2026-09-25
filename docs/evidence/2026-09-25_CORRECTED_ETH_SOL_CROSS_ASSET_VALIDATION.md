# Corrected cross-asset validation — ETHUSDT and SOLUSDT

Date: 2026-09-25  
Strategy: `VAHRAM_LINK_LEVEL_GRID_V1`  
Methodology: **6 years total / 3 years prehistory / final 3 years trading**  
Status: **CROSS_ASSET RESEARCH / NOT FORMAL OOS**

## Shared methodology

For each asset:

```text
dataset: 2191 daily candles
prehistory: 1095 daily candles
trading/evaluation: 1096 daily candles
trading start: 2023-09-25
allocation preset: linear_depth_reserved
fees: 10 bps
slippage: 5 bps
range refresh: every 30 daily candles (research assumption)
```

The first H/L grid is calculated at the trading start using only the preceding three years. No trades are allowed during the prehistory window.

## ETHUSDT

GitHub Actions run: `36136235756`  
Artifact ID: `10865300723`  
Run ID: `RUN-47390258b7de2f2509c5`  
Dataset: `BINANCE:ETHUSDT:1D:62c8d7d5b63d300a`

```text
total strategy return: +119.8053%
Micro return: +121.4294%
Mid return: +118.1812%
buy-and-hold: +70.0533%
max drawdown: 38.9967%
closed trades: 1081
open Micro lots: 42
open Mid lots: 12
```

## SOLUSDT

GitHub Actions run: `36136246985`  
Artifact ID: `10865075929`  
Run ID: `RUN-648214d6dbb89478db0b`  
Dataset: `BINANCE:SOLUSDT:1D:60fe3a76bb1d67b3`

```text
total strategy return: +736.0574%
Micro return: +696.1725%
Mid return: +775.9423%
buy-and-hold: +504.5455%
max drawdown: 51.5712%
closed trades: 1041
open Micro lots: 41
open Mid lots: 12
```

## Corrected comparison

| Metric | LINKUSDT | ETHUSDT | SOLUSDT |
|---|---:|---:|---:|
| Strategy total return | +482.2161% | +119.8053% | +736.0574% |
| Micro return | +471.1002% | +121.4294% | +696.1725% |
| Mid return | +493.3320% | +118.1812% | +775.9423% |
| Buy & hold | +89.6299% | +70.0533% | +504.5455% |
| Max drawdown | 35.9760% | 38.9967% | 51.5712% |
| Closed trades | 625 | 1081 | 1041 |

## Interpretation boundary

All three corrected runs are profitable and all three exceed their same-period buy-and-hold benchmark in this current research implementation.

This is materially stronger cross-asset evidence than the superseded 3-year-only runs.

It is still **not an acceptance claim** because several rules remain research assumptions, especially:

- H/L refresh cadence after trading starts;
- canonical Micro/Mid capital allocation;
- canonical Mid entry placement;
- exact percentage-target derivation.

The correct next step is preregistered multi-asset validation under frozen rules, not parameter tuning after seeing these results.
