# Backtest methodology correction — full 3-year prehistory

Date: 2026-09-25  
Status: **CORRECTED METHODOLOGY**

## Owner clarification

For a strategy test that evaluates a three-year trading period, the backtester must already know the preceding market history on the first trading day.

Therefore the minimum default dataset is:

```text
6 years total history
├── first 3 years: PREHISTORY ONLY
│   └── used to determine H/L and the initial 16 x 4 grid
└── final 3 years: TRADING / EVALUATION
```

No trading is allowed during the prehistory window.

At the first evaluation candle, the active H/L range is calculated only from the preceding three years of daily candles.

## Why the previous runs were not canonical

The first LINK/SOL/ETH research runs downloaded only the three-year evaluation period and allowed the range to become active after a short 90-candle warmup.

That means the early part of those runs did not have the three years of information that a real trader would have had when starting the strategy.

Those runs remain useful implementation smoke tests but are **superseded as strategy-performance evidence**.

## Code correction

The corrected engine now:

- downloads 6 years by default;
- uses the final 3 years as the trading/evaluation window;
- requires at least 1095 prior daily candles before the first trade;
- calculates the first H/L exactly at the evaluation start from prior candles only;
- forbids all entries before the evaluation start;
- calculates buy-and-hold benchmark from the same evaluation start;
- reports dataset candles, prehistory candles, and trading candles separately.

The exact H/L refresh trigger after trading begins remains a research assumption (currently every 30 daily candles) until owner-frozen.

## Corrected LINK run

GitHub Actions run: `36135794525`  
Run ID: `RUN-93c77fa26c49e02ceb21`  
Dataset: `BINANCE:LINKUSDT:1D:1262fdbb884059ba`

```text
dataset candles: 2191
prehistory candles: 1095
trading candles: 1096
trading start: 2023-09-25

strategy total return: +482.2161%
Micro return: +471.1002%
Mid return: +493.3320%
buy-and-hold: +89.6299%
max drawdown: 35.9760%
closed trades: 625
open Micro lots at end: 45
open Mid lots at end: 13
```

## Interpretation boundary

The corrected LINK result is materially different from the earlier three-year-only run.

It should not yet be treated as accepted profitability evidence because:

- the 30-candle H/L refresh cadence is still not owner-frozen;
- the capital allocation preset is still research-only;
- Mid entry placement is still research-only;
- more cross-asset and sensitivity validation is required.

However, this is now the correct default **time-window structure** for future tests of this strategy.
