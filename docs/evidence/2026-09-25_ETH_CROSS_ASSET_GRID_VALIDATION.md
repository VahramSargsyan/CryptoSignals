> **SUPERSEDED PERFORMANCE EVIDENCE:** this run used insufficient prehistory before the trading window. Keep it only as an implementation/runtime smoke test. See `docs/evidence/2026-09-25_GRID_PREHISTORY_METHOD_CORRECTION.md` for the corrected methodology.\n\n# Cross-asset validation — ETHUSDT

Date: 2026-09-25  
Strategy: `VAHRAM_LINK_LEVEL_GRID_V1`  
Validation type: **CROSS_ASSET_RESEARCH / NOT FORMAL OOS**  
Symbol: `ETHUSDT`  
Timeframe: `1D`

## Runtime evidence

GitHub Actions run: `36134629875`  
Artifact: `link-level-grid-ETHUSDT-linear_depth_reserved`  
Artifact ID: `10862039461`  
Source commit: `f9658613e3c7e01f4e205c7c948e6c52c68bd7a1`  
Dataset: `BINANCE:ETHUSDT:1D:a4ee296f034719d5`  
Candles: `1096`

## Test evidence

```text
Ran 60 tests
OK
```

The real Binance-backed ETH backtest completed successfully.

## Parameters

```text
allocation_preset: linear_depth_reserved
range_refresh_candles: 30
range_min_history_candles: 90
range_lookback_candles: 1095
Micro capital: 1000 normalized units
Mid capital: 1000 normalized units
fees: 10 bps
slippage: 5 bps
```

No ETH-specific parameter tuning was performed.

## Result

```text
run_id: RUN-d2e7ae9c84b3c219ee36

total_return: +63.9134%
micro_total_return: +56.5784%
mid_total_return: +71.2484%

benchmark_buy_hold_return: +70.0533%
max_drawdown: 32.3323%

closed_trade_count: 1433
open_micro_lots_end: 42
open_mid_lots_end: 12
```

## Interpretation

ETH is a much closer comparison than SOL.

The strategy remained profitable on ETH without asset-specific tuning and produced a return close to passive ETH holding over the same period.

The Mid layer slightly exceeded buy-and-hold on its own normalized capital pool, while the combined strategy finished below buy-and-hold because the Micro layer returned less.

This is useful cross-asset evidence, but not formal out-of-sample validation because the protocol and acceptance thresholds were not preregistered.

## Comparison

| Metric | LINKUSDT | SOLUSDT | ETHUSDT |
|---|---:|---:|---:|
| Strategy total return | +140.4020% | +41.9064% | +63.9134% |
| Micro return | +140.4247% | +40.2921% | +56.5784% |
| Mid return | +140.3793% | +43.5208% | +71.2484% |
| Buy & hold | +89.6299% | +504.5455% | +70.0533% |
| Max drawdown | 41.1522% | 44.8148% | 32.3323% |
| Closed trades | 1495 | 1155 | 1433 |

## Status impact

Strategy status remains:

```text
BACKTESTED
```

with additional cross-asset evidence.
