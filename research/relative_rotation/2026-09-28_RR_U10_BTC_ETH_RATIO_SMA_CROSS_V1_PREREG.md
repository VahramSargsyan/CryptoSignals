# RR U10 BTC/ETH RATIO SMA CROSS V1 — PREREG

Date: 2026-09-28
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Question

Does weakness in the BTC/ETH ratio — meaning ETH is outperforming BTC — coincide with stronger subsequent U10 performance, consistent with an alt-rotation / altseason hypothesis?

Mirror question:
Does strength in BTC/ETH coincide with weaker subsequent U10 performance?

## Ratio

Daily:
`BTC_ETH_RATIO = BTCUSDT close / ETHUSDT close`

Interpretation:
- ratio down = ETH outperforming BTC
- ratio up = BTC outperforming ETH

This is the inverse of ETH/BTC and is used because the user explicitly framed the hypothesis as BTC relative to ETH falling.

## Frozen strategy

Universe:
`RR_TARGET_U10_CANDIDATE_HBAR_V1`

Assets:
`TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR`

Core U10 mechanics unchanged:
- Binance Spot D1
- rolling median lookback 180d
- ARM 15%
- reversal 3%
- strongest confirmed max-dislocation router
- signal close T -> next available daily open
- 0.1% modeled transition cost
- all 10 possible starting assets
- evaluation starts 2023-10-31
- cutoff 2026-09-26

## Ratio moving averages

On daily BTC/ETH ratio:
- SMA25
- SMA50
- SMA100
- SMA200

For each SMA, detect raw end-of-day price/ratio crosses:

### ETH-strength / alt-rotation candidate
`BTC_ETH_CROSS_BELOW_SMA_N`

Previous day:
`ratio >= SMA_N`

Current day:
`ratio < SMA_N`

### BTC-strength mirror
`BTC_ETH_CROSS_ABOVE_SMA_N`

Previous day:
`ratio <= SMA_N`

Current day:
`ratio > SMA_N`

No buffer, hysteresis, or post-hoc tuning.

## 3-day persistence diagnostic

For each raw cross also record whether the ratio remains on the crossed side for the next 3 daily bars.

This does not redefine the raw event; it is only a robustness diagnostic.

## U10 capital test

At every crossover event and for each of the 10 frozen U10 start states:

- normalize event-date U10 equity to 100 USDT;
- evaluate after:
  - 7d
  - 14d
  - 30d
  - 60d
  - 90d

Aggregate each event first across 10 start states by median.

Then summarize by:
- direction
- SMA period
- horizon
- raw event vs 3-day-persistent subset

Report:
- event count
- median U10 return
- worst event
- best event
- positive-event rate
- median ending capital from 100 USDT

## Market comparison

At the same horizons also report:
- BTC forward return
- ETH forward return
- BTC/ETH ratio forward return

This lets us distinguish:
- ETH simply outperforming BTC;
- broad positive crypto conditions;
- genuine U10 benefit.

## Optional altseason context

This study does not redefine altseason.

External benchmark definition for context:
CoinMarketCap classifies Altcoin Season when 75% of the top 100 eligible coins outperform BTC over the prior 90 days.

Therefore BTC/ETH is tested only as a **possible early rotation indicator**, not as altseason ground truth.

## Output

Persist:
- `btc_eth_daily.csv`
- `cross_events.csv`
- `event_horizon_detail.csv`
- `event_summary.csv`
- `summary.json`
- `report.md`

## Interpretation

Potential descriptive labels:
- `USEFUL_ALT_ROTATION_ATTENTION_SIGNAL`
- `MIXED / NOISY`
- `NOT_USEFUL_FOR_U10`

No production promotion from this test alone.

## No-repeat / production boundary

Research only.

Do not alter:
- paper/live U9
- frozen U10 candidate
- defensive overlay
- Telegram
- exchange execution
- portfolio allocation

Target test level:

`TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`
