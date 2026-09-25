# CryptoSignals Strategy Laboratory — Product Blueprint v1

Status: DRAFT / PLANNING  
Workflow mode: ECOSYSTEM_PLANNING  
Runtime changes: NONE

## 1. Product purpose

Build a research laboratory where crypto strategies are treated as hypotheses that must be tested.

The system must be able to produce evidence for both:

- strategy fitness;
- strategy failure.

A rejected strategy is still useful research output and must remain traceable.

## 2. System layers

```text
                    DATA LAYER
             Binance + market context
                        |
                        v
                  FEATURE ENGINE
                        |
                        v
                 MARKET REGIME
                        |
                        v
              STRATEGY LABORATORY
              /       |        \
            S1        S2        S3
              \       |        /
                        v
                  BACKTEST ENGINE
                        |
                        v
                  EVIDENCE STORE
                        |
                        v
                STRATEGY REGISTRY
                 /             \
          REJECTED            ACCEPTED
                                 |
                                 v
                            PAPER / LIVE
                                 |
                                 v
                              TELEGRAM
```

## 3. Source-of-truth boundaries

### GitHub

GitHub owns:

- strategy source code;
- strategy version history;
- shared backtest engine;
- data adapters;
- indicator implementations;
- tests;
- research documentation;
- small synthetic/fixture datasets;
- evidence manifests and reproducible configuration.

GitHub should not become the main long-term store for large market-history datasets.

### Google Sheets / Drive

Google owns the human-facing data/evidence layer:

- strategy registry;
- signal history;
- backtest summaries;
- market regime history;
- experiment results;
- acceptance/rejection reasons;
- Telegram delivery log;
- operational configuration where appropriate.

Historical raw files may be staged in Drive if useful, but large-scale market history should remain replaceable/re-downloadable.

### Binance / market-data providers

Primary market data should come from market-data sources, not GitHub copies.

Initial source:

- Binance OHLCV / kline data.

Additional market/macro sources are future work and require source/license/reliability review before adoption.

### Telegram

Telegram is a notification surface only.

Initial scope:

- paper/live signal alerts;
- strategy name/version;
- symbol/timeframe;
- signal strength;
- key reasons;
- market regime;
- signal timestamp;
- duplicate suppression.

No automatic real-money order execution is part of v1.

## 4. Strategy contract

Every strategy should eventually expose the same logical contract:

```text
INPUT
- OHLCV data
- indicator/features
- strategy config
- market regime context

OUTPUT
- BUY / SELL / HOLD
- strength / confidence score
- reasons
- strategy_id
- strategy_version
- timestamp
```

This allows all strategies to use the same backtest and evidence pipeline.

## 5. Preserve the original strategy

The current repository behavior must be frozen as:

`VAHRAM_ORIGINAL_V1`

Important: the current function named `calculate_stoch_rsi` does not implement the canonical Stochastic RSI formula. It calculates a stochastic position of Close inside a rolling Close range.

This must **not** be silently replaced.

Instead create a separate future variant:

`VAHRAM_TRUE_STOCHRSI_V2`

Then test both on identical data.

## 6. Strategy lifecycle

Allowed statuses:

```text
IDEA
EXPERIMENTAL
BACKTESTED
OUT_OF_SAMPLE_TESTED
WALK_FORWARD_TESTED
ACCEPTED
REJECTED
NEEDS_MORE_DATA
REGIME_SPECIFIC
OVERFIT_RISK
PAPER_LIVE
LIVE_SIGNALS
RETIRED
```

A strategy can move backward if new evidence invalidates earlier conclusions.

## 7. Rejected strategies

Rejected strategies must not be deleted.

They should retain:

- version;
- tested parameters;
- tested symbols/timeframes;
- test periods;
- metrics;
- rejection reason;
- market-regime observations;
- links to evidence.

This prevents repeating failed experiments and allows later re-testing under new regimes.

## 8. Market-regime layer

The laboratory should test not only:

> Does this strategy work?

but:

> Under what market conditions does this strategy work?

Initial regime dimensions:

- BTC trend;
- broad crypto trend;
- volatility regime;
- volume/liquidity regime;
- market breadth;
- risk-on / risk-off proxy;
- optional macro context after source review.

Example evidence:

```text
Strategy: VAHRAM_V3

ALL:
Win rate: ...

BTC_BULL:
Win rate: ...

BTC_BEAR:
Win rate: ...

HIGH_VOL:
Win rate: ...

SIDEWAYS:
Win rate: ...
```

## 9. Future meta-strategy

A later phase may select strategy behavior by regime:

```text
IF BULL_TREND
  -> Momentum strategy

IF SIDEWAYS
  -> Mean-reversion strategy

IF HIGH_VOLATILITY
  -> Breakout strategy

IF EXTREME_RISK_OFF
  -> NO_TRADE
```

This must only be attempted after regime labels and strategy evidence are trustworthy.

## 10. Research / Live repository split

Initial rule:

- keep research, accepted, rejected, and experimental strategies in this repository.

Possible later split:

```text
CryptoSignals-Lab
  - experiments
  - backtests
  - rejected strategies
  - optimization
  - evidence

CryptoSignals-Live
  - accepted strategy versions only
  - live ingestion
  - paper/live signal runtime
  - Telegram notifier
  - operational controls
```

Do not split early without operational need.

## 11. Proposed Google Sheets workbook

Possible logical sheets:

| Sheet | Purpose |
|---|---|
| CONFIG | symbols, timeframes, feature flags |
| STRATEGY_REGISTRY | strategy lifecycle and status |
| CANDLES_D1 | bounded daily OHLCV if Sheets storage is used |
| MARKET_REGIME | regime labels and context |
| SIGNALS | generated signals |
| BACKTEST_RUNS | run metadata |
| BACKTEST_TRADES | trade-level evidence |
| BACKTEST_METRICS | strategy/run summary |
| TELEGRAM_LOG | sent notifications / deduplication |
| RUN_LOG | ingestion and scheduled-job observability |

Schema is not frozen. Any future schema implementation requires a migration plan if an existing live workbook is changed.

## 12. Recommended execution split

### Google Apps Script

Good fit for:

- scheduled data refresh;
- small/bounded daily datasets;
- Google Sheets writes;
- Telegram messages;
- lightweight live signal calculation;
- operational logs.

### Python in GitHub

Good fit for:

- reusable strategy engine;
- large historical backtests;
- parameter sweeps;
- advanced metrics;
- data-science workflows;
- GitHub Actions scheduled tests;
- future ML/feature-engineering experiments.

A hybrid architecture is preferred over forcing every heavy backtest into Apps Script.

## 13. Core evidence principle

A strategy result is only valid when the run records:

- exact strategy version;
- exact parameters;
- exact dataset/time range;
- symbols/timeframes;
- fees;
- slippage assumption;
- entry rule;
- exit rule;
- data gaps;
- code commit SHA;
- result metrics.

No reproducible evidence = no acceptance.
