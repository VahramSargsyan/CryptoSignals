# H7 — Reproducible V1 vs V2 Comparison Runner

WORKFLOW_MODE: BUILD_NEW_APP  
RISK_CLASS: L3 — external market-data + research evidence orchestration  
MIGRATION_REQUIRED: NO

## Definition of Ready

CAPABILITY_ID: CRYPTO_STRATEGY_LAB_H7  
USER_RESULT: both protected strategy variants can be run on the same fresh Binance Spot history with identical cost/execution semantics and artifact evidence.  
CANONICAL_SOURCE_OF_TRUTH: Binance Spot public klines + accepted GitHub source commit.  
SCOPE: public REST pagination, comparison orchestration, pre-OOS research workflow, artifact packaging.  
OUT_OF_SCOPE: acceptance decision, parameter tuning, 2026 OOS reveal, regimes, Sheets, Telegram, live trading.  
DO_NOT_TOUCH: VAHRAM_ORIGINAL_V1 formulas, legacy scripts, repository historical CSVs.  
DEPENDENCIES: Python 3.12; pandas 2.2.3 in workflow; official GitHub actions; Binance public Spot REST.  
SHARED_HOTSPOTS: canonical candle contract, strategy outputs, trading engine, evidence schema.  
SCHEMA / ID / RELATION IMPACT: none persisted.  
MIGRATION_PLAN: not required.  
ROLLBACK: revert H7 PR; H0-H6 remain functional.  
PROMOTION_PATH: local tests -> PR -> main -> short-lived research-run branch -> GitHub Actions -> artifact inspection.  
FRESHNESS_FINGERPRINT: main SHA + request JSON + dataset content hashes + run IDs.  
REVALIDATE_IF: Binance Spot kline API changes, source commit changes, execution policy changes, or evidence schema changes.  
UNRESOLVED_DECISIONS: NONE.

## Binance boundary

The public client uses:

GET /api/v3/klines

with UTC klines, startTime/endTime pagination and at most 1000 rows per request.

No API key or secret is required.

The client is bounded by max_pages and rejects overlapping/backwards pagination.

## Comparison

For each clean symbol dataset, H7 runs both:

- VAHRAM_ORIGINAL_V1
- VAHRAM_TRUE_STOCHRSI_V2

using:

- the exact same dataset_id;
- the exact same timeframe;
- the exact same LONG_ONLY_SIGNAL_FLIP_V1 engine;
- the exact same fee/slippage assumptions;
- the same event-study horizons.

Per strategy it stores:

- run manifest;
- event-study observations/summary;
- trading metrics;
- complete evidence JSON.

## Pre-OOS protection

The initial automated research request ends at:

2026-01-01T00:00:00Z

For 1D UTC candles this allows the candle opened 2025-12-31 to be used after it has closed, while preventing the 2026 daily history from entering the first comparison.

Therefore the first H7 run is descriptive pre-OOS research, not final acceptance evidence.

2026 OOS is intentionally left unopened until acceptance gates are frozen.

## Repository storage rule

Downloaded canonical candles and detailed evidence are uploaded as GitHub Actions artifacts with finite retention.

They are not committed as a continuously growing market-history store.

Small request manifests and code/tests remain in GitHub.
