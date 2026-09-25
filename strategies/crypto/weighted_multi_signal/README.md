# WEIGHTED_MULTI_SIGNAL_V1

Status: **EXPERIMENTAL**  
WORKFLOW_MODE: **BUILD_NEW_APP**  
RISK_CLASS: **L2 — research-only strategy candidate**  
MIGRATION_REQUIRED: **NO**  
LIVE/PAPER-LIVE IMPACT: **NONE**

## Objective

Build the first genuinely multi-factor CryptoSignals candidate instead of treating a single indicator as a complete strategy.

## Concept provenance

MoniGoMani was reviewed as a **GPL-3.0 reference only** at commit:

`6572c167c2e8fab999ded4ce2b5c035868f89e95`

The architectural concepts retained are:

- multiple independent weighted signals;
- total signal strength;
- minimum number of triggered signals;
- trend-aware strategy design.

No MoniGoMani GPL source code is copied into this implementation.

## Frozen v1 scoring before first backtest

Five signal families. Each family is worth exactly 20 points.

### 1. MA trend — 20

Bullish:

- SMA50 > SMA100 > SMA200
- close > SMA200

Bearish:

- SMA50 < SMA100 < SMA200
- close < SMA200

### 2. MACD — 20

Bullish: MACD > signal line.  
Bearish: MACD < signal line.

### 3. StochRSI — 20

Bullish: K > D.  
Bearish: K < D.

### 4. Bollinger mean reversion — 20

Bullish: close <= lower band.  
Bearish: close >= upper band.

### 5. Volume + candle confirmation — 20

Requires:

- volume > 20-period volume MA;
- candle body strength >= 0.50.

Bullish if close > open.  
Bearish if close < open.

## Decision rule

A BUY or SELL requires:

- score >= 60 / 100;
- at least 3 of 5 signal families;
- winning side score strictly greater than the opposite side.

Otherwise there is no actionable signal.

The 60-point threshold, five equal weights, and 3-block minimum are frozen **before** the first historical runtime. No parameter search is part of v1.

## Why equal weights first

The purpose of v1 is to measure whether combining independent information improves over the single-indicator baselines without immediately introducing optimization bias.

Only after baseline evidence exists may a separately versioned candidate test unequal weights.

## Existing inputs reused

- Bollinger Bands;
- canonical StochRSI;
- MACD 12/26/9;
- SMA50 / SMA100 / SMA200;
- volume MA20;
- candle body strength.

## First research plan

- Binance Spot
- current 13-symbol universe
- 1D
- 2021-01-01 inclusive to 2026-01-01 exclusive
- fee 10 bps
- slippage 5 bps
- next-candle-open execution
- no 2026 blind-OOS claim

The first runtime is historical evidence only.

## Promotion

`EXPERIMENTAL -> BACKTESTED -> future held-out/walk-forward -> acceptance decision`
