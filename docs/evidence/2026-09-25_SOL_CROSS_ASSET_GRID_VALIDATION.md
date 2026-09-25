> **SUPERSEDED PERFORMANCE EVIDENCE:** this run used insufficient prehistory before the trading window. Keep it only as an implementation/runtime smoke test. See `docs/evidence/2026-09-25_GRID_PREHISTORY_METHOD_CORRECTION.md` for the corrected methodology.\n\n# Cross-asset validation — SOLUSDT

Date: 2026-09-25  
Strategy: `VAHRAM_LINK_LEVEL_GRID_V1`  
Validation type: **CROSS_ASSET_RESEARCH / NOT FORMAL OOS**  
Symbol: `SOLUSDT`  
Timeframe: `1D`

## Why SOL

SOL was selected as a large-cap external asset to test whether the level-grid mechanics produce a qualitatively similar result outside LINK.

The strategy code and default research parameters were reused without symbol-specific tuning.

## Runtime evidence

GitHub Actions run:

`36133871517`

Artifact:

`link-level-grid-SOLUSDT-linear_depth_reserved`

Artifact ID:

`10862838818`

Source commit used by the run:

`ca641c481b29bdac154dc0a01b1e0f9654d63581`

Dataset:

`BINANCE:SOLUSDT:1D:2e54d39d19fd7152`

Candles:

`1096`

## Test evidence

Full repository suite:

```text
Ran 60 tests
OK
```

Real Binance-backed SOL backtest completed successfully.

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

No SOL-specific parameter optimization was performed.

## Result

```text
run_id: RUN-fc372692d9a8fa379eb2

total_return: +41.9064%
micro_total_return: +40.2921%
mid_total_return: +43.5208%

benchmark_buy_hold_return: +504.5455%
max_drawdown: 44.8148%

closed_trade_count: 1155
open_micro_lots_end: 42
open_mid_lots_end: 12
```

## Interpretation

The experiment confirms an important distinction:

- the strategy was profitable in this SOL historical sample;
- both Micro and Mid were profitable;
- the behavior generalized mechanically to another asset without symbol-specific tuning;
- however, the strategy **did not outperform buy-and-hold SOL** over this particular three-year period.

Therefore this result supports:

```text
cross-asset mechanical viability
+ positive-return evidence on SOL
```

but it does **not** support a claim that the strategy universally outperforms passive holding.

## Comparison with the first LINK run

| Metric | LINKUSDT | SOLUSDT |
|---|---:|---:|
| Strategy total return | +140.4020% | +41.9064% |
| Micro return | +140.4247% | +40.2921% |
| Mid return | +140.3793% | +43.5208% |
| Buy & hold | +89.6299% | +504.5455% |
| Max drawdown | 41.1522% | 44.8148% |
| Closed trades | 1495 | 1155 |

The important research signal is that positive strategy returns appeared on both assets, while relative performance against buy-and-hold was strongly asset/path dependent.

## Status impact

No promotion to `ACCEPTED`.

The strategy remains:

```text
BACKTESTED
```

with broader cross-asset evidence.

This SOL run should be treated as a reproducible external-symbol validation point, not as formal out-of-sample proof because the validation protocol and acceptance thresholds were not preregistered before the experiment.
