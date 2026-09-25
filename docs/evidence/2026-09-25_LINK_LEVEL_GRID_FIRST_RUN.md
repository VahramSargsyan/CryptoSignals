# LINK Level Grid — first executable research run

Date: 2026-09-25  
Status: **RUNTIME PASS / RESEARCH ONLY**  
Strategy: `VAHRAM_LINK_LEVEL_GRID_V1`  
Source commit: `f33dde450eb33edb9ebc1b56b76445fa7245c020`  
Dataset: `BINANCE:LINKUSDT:1D:07c433b5760a267a`

## Runtime evidence

GitHub Actions run:

`36132903127`

Artifact:

`link-level-grid-LINKUSDT-linear_depth_reserved`  
Artifact ID: `10862951975`

The artifact contains:

- canonical candles;
- trade ledger;
- equity curve;
- range refresh history;
- allocation table;
- summary JSON;
- download metadata.

## Test evidence

Full repository suite:

```text
Ran 60 tests
OK
```

The run also executed the real Binance-backed LINK research backtest successfully.

## Input

```text
symbol: LINKUSDT
timeframe: 1D
candles: 1096
historical period: 3 years
allocation preset: linear_depth_reserved
range lookback: up to 1095 prior candles
minimum range history: 90 candles
range refresh cadence: 30 candles
Micro capital: 1000 normalized units
Mid capital: 1000 normalized units
```

## First result

```text
run_id: RUN-ec8cfd26fda605c20008
total_return: +140.4020%
micro_total_return: +140.4247%
mid_total_return: +140.3793%
benchmark_buy_hold_return: +89.6299%
max_drawdown: 41.1522%
closed_trade_count: 1495
open_micro_lots_end: 47
open_mid_lots_end: 13
```

## Interpretation boundary

These numbers are **not a profitability claim and not an accepted strategy result**.

The implementation deliberately labels several still-unfrozen product choices as research assumptions:

- 30-candle range refresh;
- 90-candle minimum warmup;
- `linear_depth_reserved` capital allocation;
- Mid entry at each main level's A boundary;
- Micro one-sublevel exit.

The result proves that the strategy can now be executed deterministically against real LINK data without look-ahead in the H/L range construction.

The next research step is to freeze or sensitivity-test the unresolved assumptions before treating performance as evidence about the owner's intended strategy.
