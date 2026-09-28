# RR U10 PURE BTC SMA200 REGIME V1 — PREREG

Date: 2026-09-28
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Question
As a simpler alternative to the H8 BTC 50/200 regime, does the frozen single-book U10 earn when BTC is above its 200-day SMA, and what happens when BTC is below its 200-day SMA?

## Frozen strategy
Universe: `RR_TARGET_U10_CANDIDATE_HBAR_V1`
Assets: `TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR`

Mechanics unchanged:
- Binance Spot D1
- 180d rolling median
- ARM 15%
- reversal 3%
- confirmed T -> next-open execution
- strongest max-dislocation routing
- 0.1% transition cost
- all 10 possible starting assets
- MATURE: 2023-10-31 -> 2026-09-26

## Regime
Use BTCUSDT daily close and trailing 200-day SMA only.

- `BTC_ABOVE_SMA200`: BTC close >= trailing SMA200
- `BTC_BELOW_SMA200`: BTC close < trailing SMA200
- `UNKNOWN`: insufficient history

No SMA50 condition and no tuned thresholds.

## Metrics
For each start asset and regime:
- day count
- compounded strategy return on regime-labelled days
- conditioned drawdown
- positive-day rate
- contiguous episode count
- median/worst/best episode return
- positive episode rate
- BTC compounded return on same days

Aggregate across all 10 starting assets:
- median / worst / best conditioned return
- positive-start rate
- median conditioned DD

Also report per-calendar-year regime-conditioned returns.

## Production boundary
Research only. No live/paper/Telegram/exchange changes.

Target test level:
`TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`
