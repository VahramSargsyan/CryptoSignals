# RR U10 COMBINED TOTAL + BTC/ETH REGIME V1 — PREREG

Date: 2026-09-28
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Question

Does combining the broad market-cap regime (TradingView CRYPTOCAP:TOTAL)
with BTC/ETH relative-strength state produce a materially cleaner U10
attention/risk signal than either component alone?

## Frozen U10

Universe:
`RR_TARGET_U10_CANDIDATE_HBAR_V1`

Assets:
`TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR`

Core mechanics unchanged:
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

## TOTAL source

Use repository cache only:

`data/market/cryptocap_total_d1.csv`

Frozen identity:
- 1,398 daily rows
- 2022-11-29 -> 2026-09-26
- SHA256:
  `d0c9b3bac915e484069c6302ddce1a0b050abbb8de155e4106e39cf6e519d3ee`

No TradingView refresh is allowed for this test.

Recompute / verify:
- SMA25
- SMA50
- SMA100
- SMA200
- SMA300

TOTAL regime states remain frozen from prior work:

### TOTAL_BULL
`BULL_BUILDING` or `FULL_BULL_ALIGNMENT`

where:
- BULL_BUILDING = TOTAL > SMA50 > SMA100, without full bull
- FULL_BULL_ALIGNMENT =
  TOTAL > SMA50 > SMA100 > SMA200 > SMA300

### TOTAL_BEAR
`BEAR_BUILDING` or `FULL_BEAR_ALIGNMENT`

where:
- BEAR_BUILDING = TOTAL < SMA50 < SMA100, without full bear
- FULL_BEAR_ALIGNMENT =
  TOTAL < SMA50 < SMA100 < SMA200 < SMA300

MIXED is neither bull nor bear for the combined signal.

## BTC/ETH ratio

Daily:

`BTC_ETH_RATIO = BTCUSDT close / ETHUSDT close`

Interpretation:
- ratio down = ETH outperforming BTC
- ratio up = BTC outperforming ETH

Moving averages:
- SMA25
- SMA50
- SMA200

## Frozen ratio states

Use 3-day persistence for state construction.

### ETH_FAST_25
BTC/ETH is below SMA25 on the current day and the previous 2 daily bars.

### ETH_MEDIUM_50
BTC/ETH is below SMA50 on the current day and the previous 2 daily bars.

### BTC_LONG_200
BTC/ETH is above SMA200 on the current day and the previous 2 daily bars.

These are states, not one-day crossover events.

## Combined signals

### COMBO_BULL_25
`TOTAL_BULL AND ETH_FAST_25`

### COMBO_BULL_50
`TOTAL_BULL AND ETH_MEDIUM_50`

### COMBO_RISK_200
`TOTAL_BEAR AND BTC_LONG_200`

An event occurs only when the combined condition changes from FALSE to TRUE.
Continuous days inside the same state do not create duplicate events.

## Component-only comparison signals

To measure whether combination adds information, also create entry events for:

- `TOTAL_BULL_ONLY`
- `TOTAL_BEAR_ONLY`
- `ETH_FAST_25_ONLY`
- `ETH_MEDIUM_50_ONLY`
- `BTC_LONG_200_ONLY`

Each component event is also FALSE -> TRUE state entry.

No component threshold is changed after runtime inspection.

## U10 capital test

At every signal entry and for each of the 10 frozen U10 start states:

- normalize event-date U10 equity to 100 USDT;
- evaluate after:
  - 7d
  - 14d
  - 30d
  - 60d
  - 90d

For each event:
- first aggregate the 10 start states by median forward U10 return.

For each signal / horizon report:
- event count
- median U10 forward return
- worst event
- best event
- positive-event rate
- median ending capital from 100 USDT
- median TOTAL forward return
- median BTC forward return
- median ETH forward return
- median BTC/ETH forward return

## Combination lift

For each combined signal compare against its relevant component-only signals.

Bull:
- COMBO_BULL_25 vs TOTAL_BULL_ONLY and ETH_FAST_25_ONLY
- COMBO_BULL_50 vs TOTAL_BULL_ONLY and ETH_MEDIUM_50_ONLY

Risk:
- COMBO_RISK_200 vs TOTAL_BEAR_ONLY and BTC_LONG_200_ONLY

Report descriptive lift:
- median U10 return difference
- positive-event-rate difference
- event-count reduction

Do not optimize signal definition based on lift.

## Current-state diagnostic

At cutoff 2026-09-26 persist:
- TOTAL state
- BTC/ETH ratio
- BTC/ETH SMA25 / SMA50 / SMA200
- ETH_FAST_25 true/false
- ETH_MEDIUM_50 true/false
- BTC_LONG_200 true/false
- COMBO_BULL_25 true/false
- COMBO_BULL_50 true/false
- COMBO_RISK_200 true/false

## Interpretation

Possible descriptive outcomes:

- `COMBINATION_ADDS_SIGNAL_QUALITY`
- `COMBINATION_REDUCES_NOISE_BUT_SAMPLE_TOO_SMALL`
- `NO_CLEAR_COMBINATION_BENEFIT`

No production promotion from this study alone.

## Output

Persist:
- `daily_joint_state.csv`
- `signal_events.csv`
- `event_horizon_detail.csv`
- `event_summary.csv`
- `combination_lift.csv`
- `summary.json`
- `report.md`

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

`TEST_LEVEL: GITHUB_ACTIONS_REPOSITORY_CACHE_PLUS_LIVE_PUBLIC_DATA_STRESS_TEST`
