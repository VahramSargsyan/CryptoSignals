# MACD_CROSS_V1

Status: **EXPERIMENTAL**  
WORKFLOW_MODE: **BUILD_NEW_APP**  
RISK_CLASS: **L2 — research-only strategy candidate**  
MIGRATION_REQUIRED: **NO**  
LIVE/PAPER-LIVE IMPACT: **NONE**

## Research objective

Create an independently testable MACD crossover benchmark from the existing OSS shortlist.

## Identity

- strategy_id: `MACD_CROSS_V1`
- strategy_version: `1.0.0`
- first research timeframe: `1D`
- execution engine: `LONG_ONLY_SIGNAL_FLIP_V1`

## OSS provenance

External source:

- repository: `w1ld3r/crypto-signal`
- source commit: `8ae473999f83d3025e6e9429c3734429fb0e180d`
- license: MIT
- reviewed source: `app/analyzers/indicators/macd_cross.py`
- source defaults: fast=12, slow=26, signal=9

Source crossover semantics:

- bullish event: previous MACD < previous signal AND current MACD > current signal;
- bearish event: previous MACD > previous signal AND current MACD < current signal.

## Local adaptation

CryptoSignals does not add TA-Lib.

The project-owned shared indicator engine implements:

- EMA seeded from a complete SMA window;
- MACD 12/26/9;
- strict two-candle crossover detection.

BUY maps to bullish cross and SELL maps to bearish cross. Signal strength is fixed at 100 because the source crossover is binary.

This is an independent implementation of the reviewed strategy idea. It is not claimed to be bit-for-bit identical to TA-Lib on every warm-up edge case.

## Data/schema/dependency impact

- no schema change;
- no Google Sheets change;
- no new Python dependency;
- no secrets;
- no live/paper-live modification;
- V1/V2 remain the default comparison set.

## First evidence plan

Historical research baseline:

- Binance Spot
- 13-symbol current universe
- 1D
- 2021-01-01 inclusive to 2026-01-01 exclusive
- fee = 10 bps
- slippage = 5 bps
- execution = next candle open
- validation type = `HISTORICAL_RESEARCH_2021_2025`

2026 is not treated as a new blind OOS period.

## Promotion

`EXPERIMENTAL -> BACKTESTED -> future held-out/walk-forward evidence -> acceptance decision`
