# U8 Drawdown Entry Stress v1 — preregistration

Date: 2026-09-27
Mode: PATCH_FIX
Scope: research/backtest reporting only

## Objective

Measure how a fresh 10,000 USDT U8 portfolio behaves when started at different dates around the historically strongest ATOM-start peak-to-trough drawdown.

This is not a new strategy and does not change live U8 behavior.

## Frozen U8

Universe:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK

Mechanics:
- Binance Spot 1D closed candles
- rolling median: 180 days
- ARM threshold: 15%
- reversal confirmation: 3%
- strongest confirmed max-dislocation router
- next-open execution
- transition cost: 0.1% per executed transition

## Historical drawdown anchor

Previously observed continuation-path max drawdown:
- prior equity peak: 2025-09-20
- trough: 2026-06-06
- peak-to-trough drawdown: about -71.23%

The stress test must not assume that a fresh portfolio started near this period follows the same capital path as the continuation portfolio.

## Fresh-entry scenarios

Each scenario independently starts with 10,000 USDT and buys ATOM at that scenario's first daily open.

Scenarios:
1. ONE_YEAR_BEFORE_PEAK: 2024-09-20
2. SAME_YEAR_START: 2025-01-01
3. AT_PEAK_DATE: 2025-09-20
4. TROUGH_YEAR_START: 2026-01-01

The full historical panel before each start remains available only for indicator warm-up and event-state generation.

## Required output per scenario

- start date and end date
- initial ATOM open and quantity
- transition ledger
- route
- final equity and return
- minimum equity vs initial 10,000 USDT
- date of minimum equity
- days below initial capital
- maximum peak-to-trough drawdown
- peak and trough dates / dollar values
- worst loss versus initial capital
- whether equity ever recovered above 10,000 USDT after first falling below it
- first recovery date if applicable

## Interpretation rule

Always report separately:
- MAX DRAWDOWN FROM PRIOR PEAK
- MINIMUM EQUITY VS INITIAL CAPITAL

Never describe peak-to-trough drawdown as loss of original capital.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_BACKTEST
