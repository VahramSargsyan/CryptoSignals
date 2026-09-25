# Corrected cross-asset validation — BNBUSDT and BTCUSDT

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

No trading occurs during prehistory. The first H/L grid at the evaluation start is based only on the preceding three years of daily candles.

## BNBUSDT

GitHub Actions run: `36140374482`  
Artifact ID: `10866761636`  
Run ID: `RUN-764773cb6222272429ba`  
Dataset: `BINANCE:BNBUSDT:1D:a07a7f7566e5243b`

```text
total strategy return: +104.1704%
Micro return: +93.3718%
Mid return: +114.9690%
buy-and-hold: +272.8709%
max drawdown: 26.3147%
closed trades: 997
open Micro lots: 33
open Mid lots: 10
```

## BTCUSDT

GitHub Actions run: `36140388792`  
Artifact ID: `10866426854`  
Run ID: `RUN-2ab3460b6a87f04de04f`  
Dataset: `BINANCE:BTCUSDT:1D:52681302302c81b5`

```text
total strategy return: +69.6046%
Micro return: +60.8840%
Mid return: +78.3252%
buy-and-hold: +221.5825%
max drawdown: 16.2595%
closed trades: 926
open Micro lots: 27
open Mid lots: 8
```

## Five-asset corrected comparison

| Metric | LINK | ETH | SOL | BNB | BTC |
|---|---:|---:|---:|---:|---:|
| Strategy return | +482.2161% | +119.8053% | +736.0574% | +104.1704% | +69.6046% |
| Buy & hold | +89.6299% | +70.0533% | +504.5455% | +272.8709% | +221.5825% |
| Max drawdown | 35.9760% | 38.9967% | 51.5712% | 26.3147% | 16.2595% |
| Closed trades | 625 | 1081 | 1041 | 997 | 926 |

## Interpretation

The current implementation is profitable on all five tested large-cap assets.

It outperformed buy-and-hold on LINK, ETH and SOL, but underperformed buy-and-hold on BNB and BTC over this specific evaluation period.

That pattern is consistent with a grid/mean-reversion system that can monetize repeated volatility but can leave upside on the table during sustained strong trends.

The BNB result does **not** confirm an expectation of exceptional relative performance under the current research assumptions. That may mean either:

- the remembered/manual BNB implementation differed from the current code;
- unresolved rules such as H/L refresh, capital allocation or Mid entry materially change BNB behavior;
- or the historical period/methodology being remembered was different.

No parameters were changed after seeing BNB or BTC results.

## Status impact

Strategy remains `BACKTESTED`.

This evidence broadens the cross-asset sample and is specifically useful because it includes two cases where the strategy remains profitable but fails to beat passive holding.
