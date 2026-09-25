# CryptoSignals Strategy Laboratory — Roadmap v1

Status: DRAFT  
Mode: ECOSYSTEM_PLANNING

## Phase 0 — Freeze current baseline

Goal: preserve the existing project before changing behavior.

Deliverables:

- tag/document current strategy as `VAHRAM_ORIGINAL_V1`;
- document exact current formulas;
- document current tracked symbols;
- preserve current CSV signal output;
- add regression fixtures for known historical rows.

Exit criterion:

- current behavior can be reproduced after future refactors.

## Phase 1 — Shared data layer

Goal: separate market-data ingestion from strategy logic.

Build:

- Binance historical OHLCV downloader;
- incremental updater;
- normalized candle schema;
- symbol/timeframe registry;
- deterministic dataset IDs;
- data-quality checks;
- gap/duplicate detection.

Initial timeframe:

- 1D first.

Later:

- 4H;
- 1H;
- others only when scale/performance is justified.

## Phase 2 — Shared indicator / feature engine

Goal: avoid each strategy recalculating indicators differently.

Initial features:

- Bollinger Bands;
- current VAHRAM stochastic-close-range indicator;
- canonical RSI;
- canonical Stochastic RSI;
- volume moving average;
- SMA 50 / 100 / 200 trend block;
- candle body strength;
- trend filters;
- volatility features.

All formulas require tests.

## Phase 3 — Strategy interface

Create a common strategy contract and Strategy Registry.

Initial strategies:

1. `VAHRAM_ORIGINAL_V1`
2. `VAHRAM_TRUE_STOCHRSI_V2`
3. Bollinger + RSI variant
4. weighted multi-signal variant inspired by MoniGoMani concepts
5. momentum/trend-following experiment
6. mean-reversion experiment

Each strategy is versioned and independently configurable.

## Phase 4 — Backtest engine

One shared engine for every strategy.

Must support:

- identical historical data for all comparisons;
- deterministic entry/exit processing;
- fees;
- configurable slippage;
- no look-ahead;
- multiple symbols;
- multiple timeframes;
- run manifests;
- trade-level output;
- summary metrics.

Required metrics:

- total return;
- benchmark return;
- trade count;
- win rate;
- average/median trade return;
- profit factor;
- expectancy;
- max drawdown;
- average holding period;
- exposure;
- results by symbol;
- results by year;
- results by regime.

## Phase 5 — Validation discipline

Do not optimize and validate on the same data.

Proposed initial split (not frozen):

- 2021–2024: research/development;
- 2025: validation/tuning checkpoint;
- 2026: blind/out-of-sample validation.

Alternative walk-forward windows may be used after the engine exists.

Required controls:

- look-ahead bias test;
- overfitting warning;
- minimum trade-count threshold;
- parameter stability check;
- benchmark comparison;
- sensitivity analysis.

## Phase 6 — Market Regime Engine

Goal: explain when strategies work.

Initial regime labels:

- BULL;
- BEAR;
- SIDEWAYS;
- HIGH_VOLATILITY;
- LOW_VOLATILITY;
- HIGH_VOLUME;
- LOW_VOLUME;
- RISK_ON;
- RISK_OFF.

Start with market-derived features.

Macro data is a separate source-review task.

## Phase 7 — Google Sheets evidence workspace

Create a Google workbook for:

- strategy registry;
- backtest run catalog;
- metrics;
- regime analytics;
- signals;
- experiment decisions;
- Telegram log.

The workbook is a human-facing evidence/operations layer, not the only computation engine.

## Phase 8 — Telegram

After strategy acceptance:

- Telegram bot;
- deduplicated signals;
- strategy/version in every message;
- signal reason;
- strength score;
- regime context;
- paper/live mode marker.

No duplicate alert for the same strategy/symbol/timeframe/signal event.

## Phase 9 — Paper-live observation

Accepted backtest strategy becomes:

`PAPER_LIVE`

Measure:

- real scheduled-data behavior;
- missing-data events;
- signal drift;
- runtime stability;
- differences between backtest assumptions and live signal timing.

Only after a meaningful paper-live period may status become:

`LIVE_SIGNALS`

## Phase 10 — Optional Lab / Live split

Only when operational maturity justifies it:

- keep this repository as Lab;
- create a separate Live repo;
- promote exact accepted strategy versions from Lab to Live;
- never edit a live strategy silently.

## Phase 11 — Advanced research

Future candidates:

- weighted-signal optimization;
- walk-forward optimization;
- cross-asset context;
- BTC leadership filters;
- market breadth;
- feature importance;
- ML experiments;
- regime-aware strategy selection;
- portfolio-level risk controls.

These are future research, not initial requirements.
