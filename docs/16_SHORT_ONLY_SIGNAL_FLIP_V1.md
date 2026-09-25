# SHORT_ONLY_SIGNAL_FLIP_V1 — Research Execution Engine

WORKFLOW_MODE: **BUILD_NEW_APP**  
RISK_CLASS: **L3 — shared research execution semantics**  
MIGRATION_REQUIRED: **NO**  
LIVE/PAPER-LIVE IMPACT: **NONE**

## Purpose

Evaluate bearish strategy signals without changing the existing long-only engine.

The engine is research-only. It does not authorize margin trading, exchange execution, leverage, borrowing, paper-live shorting, or live shorting.

## Semantics

- entry signal: SELL;
- entry execution: next candle open;
- exit signal: BUY;
- exit execution: next candle open;
- short notional: 1.0 × current account equity;
- no leverage above 1× notional;
- entry fee and cover fee are charged;
- SELL entry slippage lowers the execution price;
- BUY cover slippage raises the execution price;
- position is marked to market against candle close;
- intraday candle high is used for adverse zero-equity detection.

## Zero-equity rule

Crypto can rise by multiples, so a short engine cannot pretend losses are naturally capped at -100% while continuing to trade.

Policy:

`ZERO_EQUITY_LIQUIDATION_AT_INTRADAY_HIGH`

If the daily high, including adverse BUY slippage and estimated cover fee, would reduce collateral equity to zero or below:

1. trade is terminated on that candle;
2. net trade return is fixed at -100%;
3. account equity becomes zero;
4. no further trades execute for that symbol/run.

This is a simplified conservative collateral model, not an exchange-specific liquidation-price simulator.

## Short benchmark

`benchmark_return` in this engine means a 1× collateralized short benchmark:

- short at first evaluation open;
- cover at final evaluation close;
- same fees/slippage;
- same zero-equity liquidation rule.

It is not the ordinary long buy-and-hold benchmark.

## IMPACT_MAP

DIRECTLY_AFFECTED:

- `core/backtest/trading_short.py`
- research comparison engine selector
- research runner position mode
- short-engine unit tests

TRANSITIVE_DEPENDENCIES:

- BacktestRunManifest engine_name / engine_config
- common evidence serializer
- comparison summary metrics

UNAFFECTED:

- `LONG_ONLY_SIGNAL_FLIP_V1`
- all strategy signal formulas
- V1 / V2 / LINK strategy identities
- Binance canonical candle schema
- live / paper-live
- Telegram notifications
- Google Sheets / APP schemas

## Rollback

Revert the short-engine PR. Existing long-only research remains unchanged because LONG_ONLY stays the default position mode.

## Test gate

Before merge:

- next-open SELL entry / BUY cover;
- adverse slippage direction;
- fee reduction;
- mark-to-market drawdown;
- intraday-high zero-equity liquidation;
- no trades after bankruptcy;
- short benchmark semantics;
- manifest/policy exact match;
- all legacy tests PASS.
