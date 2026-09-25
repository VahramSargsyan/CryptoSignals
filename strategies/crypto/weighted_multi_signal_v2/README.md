# WEIGHTED_MULTI_SIGNAL_V2 — CORE-GATED FREEZE

Status: **EXPERIMENTAL / FROZEN BEFORE FIRST V2 RUNTIME**  
WORKFLOW_MODE: **BUILD_NEW_APP**  
RISK_CLASS: **L2 — research-only strategy candidate**  
MIGRATION_REQUIRED: **NO**  
LIVE/PAPER-LIVE IMPACT: **NONE**

## Why V2 exists

V2 is a new hypothesis derived from the completed V1 historical diagnostic.

The V1 diagnostic showed that most historical positive economics were concentrated in one entry pattern:

`MACD + StochRSI + Volume/Candle`

This observation is post-hoc. Therefore V2 must not reuse 2021–2025 as independent validation.

## Frozen strategy identity

- strategy_id: `WEIGHTED_MULTI_SIGNAL_V2`
- strategy_version: `2.0.0`
- timeframe: `1D`
- trading engine: existing `LONG_ONLY_SIGNAL_FLIP_V1`

## Frozen core gate

BUY requires **all three** conditions on the signal candle:

1. MACD > MACD signal line;
2. StochRSI K > D;
3. volume > 20D volume MA, candle body strength >= 0.50, and close > open.

SELL requires the exact bearish mirror:

1. MACD < MACD signal line;
2. StochRSI K < D;
3. volume > 20D volume MA, candle body strength >= 0.50, and close < open.

There is no 2-of-3 fallback and no alternate block combination.

## MA50 / MA100 / MA200 role

Moving averages are **context only**, not an entry permission.

Bullish MA confirmation:

- SMA50 > SMA100 > SMA200;
- close > SMA200.

Bearish MA confirmation:

- SMA50 < SMA100 < SMA200;
- close < SMA200.

MA confirmation changes reported strength but cannot create or block a trade in V2.

## Strength

- core consensus without aligned MA context: 75 / 100;
- core consensus with aligned MA context: 100 / 100.

Strength is an internal deterministic confidence label, not a probability forecast.

## Bollinger boundary

Bollinger is intentionally excluded from V2 trading decisions.

Reason: the V1 diagnostic showed sparse participation and V2 is intended to isolate the dominant trend/momentum/participation hypothesis. Bollinger remains available for a separately versioned mean-reversion strategy family.

## Historical research boundary

Allowed first research run:

- Binance Spot;
- current 13-symbol universe;
- 1D;
- 2021-01-01 inclusive;
- 2026-01-01 exclusive;
- fee 10 bps;
- slippage 5 bps;
- next-candle-open execution.

This is **training/research evidence only** because the V2 hypothesis was derived from that historical sample.

## Future holdout freeze

The following daily period is reserved and must not be used for V2 tuning:

`2026-09-26T00:00:00Z` onward.

Rules:

- do not inspect V2 performance on this holdout and then change V2 under the same version;
- any formula change becomes a new version;
- acceptance requires separately declared future evidence / paper-live protocol;
- 2026 data previously seen elsewhere in the laboratory must not be described as blind historical OOS for this newly designed candidate.

## No optimization in V2

V2 does not search:

- MACD parameters;
- StochRSI parameters;
- body threshold;
- volume window;
- MA windows;
- strength values.

The first run tests this one frozen formulation only.

## Rollback

Remove the V2 strategy registration/files. V1, V2 legacy strategies, LINK grid, StochRSI Cross and MACD Cross remain unchanged.
