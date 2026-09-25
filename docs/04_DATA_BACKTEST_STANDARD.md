# Data & Backtest Standard v1

Status: DRAFT

## 1. Primary objective

Backtests must answer whether a strategy has repeatable evidence, not produce the prettiest possible historical result.

## 2. Market data

Initial market-data source:

- Binance OHLCV / kline history.

Initial frequency:

- daily candles (1D).

Initial tracked universe is the current repository token list, subject to later registry cleanup.

## 3. Data identity

Each backtest run should record a dataset identity containing at least:

- source;
- symbol list;
- timeframe;
- start timestamp;
- end timestamp;
- data retrieval timestamp/version;
- missing-candle count;
- duplicate-candle count.

## 4. Storage policy

### GitHub

Store:

- code;
- configs;
- fixtures;
- small reproducible test datasets.

Avoid committing large continuously growing raw market-history datasets by default.

### Google Sheets / Drive

Use for:

- operational daily data where bounded;
- experiment catalog;
- results;
- human review;
- charts/dashboards;
- live/paper signals.

### Re-downloadable history

Historical market data should be reproducibly downloadable from its source.

## 5. Backtest correctness rules

### No look-ahead

A signal at candle T may only use information available at or before the allowed execution point.

Future high/low/close values must never leak into the signal.

### Explicit execution convention

Every test must state whether execution is assumed at:

- signal candle close;
- next candle open;
- other explicitly defined price.

Do not mix conventions between strategies.

### Fees

Fees must be configurable and included in strategy comparison.

### Slippage

Slippage assumptions must be configurable.

A zero-slippage test may exist as a diagnostic baseline but should not be the only acceptance evidence.

### One engine

All strategies compared in the same experiment should use the same execution/backtest engine.

## 6. Required output per trade

At minimum:

- run_id;
- strategy_id/version;
- symbol;
- timeframe;
- entry timestamp;
- entry price;
- exit timestamp;
- exit price;
- side;
- fees;
- slippage;
- gross return;
- net return;
- holding period;
- regime at entry;
- signal strength;
- signal reasons.

## 7. Required summary metrics

At minimum:

- total return;
- benchmark return;
- trade count;
- wins/losses;
- win rate;
- average return/trade;
- median return/trade;
- profit factor;
- expectancy;
- max drawdown;
- average holding duration;
- exposure;
- results by symbol;
- results by year;
- results by market regime.

## 8. Validation split

Do not tune parameters on the same period used for final acceptance.

A proposed initial split:

```text
2021–2024  research/development
2025       validation/tuning checkpoint
2026       out-of-sample / blind validation
```

This split is a starting proposal, not yet frozen.

Walk-forward validation may replace or supplement it.

## 9. Parameter optimization

Optimization is allowed only when:

- objective function is declared before seeing final validation results;
- parameter ranges are documented;
- validation remains held out;
- stability around the optimum is inspected;
- number of trials is recorded.

A single sharp optimum is an overfitting warning.

## 10. Market-regime evidence

Every accepted strategy should eventually be evaluated by regime.

Initial dimensions:

- bull/bear/sideways;
- volatility;
- volume/liquidity;
- broad-market trend.

This enables statuses such as `REGIME_SPECIFIC`.

## 11. Current StochRSI naming issue

The existing CryptoSignals implementation named `calculate_stoch_rsi` computes a stochastic position from rolling Close min/max.

It is not canonical Stochastic RSI.

Testing standard:

- preserve it unchanged as part of `VAHRAM_ORIGINAL_V1`;
- implement canonical StochRSI separately;
- compare both;
- never mix historical results between the two definitions.

## 12. Acceptance evidence

Before a strategy becomes `ACCEPTED`, evidence should include:

- reproducible backtest manifest;
- held-out evaluation;
- data-quality report;
- fees/slippage assumptions;
- benchmark comparison;
- regime breakdown;
- known weaknesses;
- residual risks.

## 13. Paper-live validation

Backtest acceptance does not immediately imply live trust.

Next state:

`PAPER_LIVE`

Compare:

- expected signal timing;
- actual scheduled signal timing;
- missing candles;
- runtime failures;
- duplicate signals;
- data-provider differences;
- real-world latency.

Only then consider `LIVE_SIGNALS`.
