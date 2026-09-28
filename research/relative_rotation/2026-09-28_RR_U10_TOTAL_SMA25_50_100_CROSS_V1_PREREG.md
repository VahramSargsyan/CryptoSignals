# RR U10 TOTAL SMA25/50/100 CROSS EVENT STUDY V1 — PREREG

Date: 2026-09-28
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Question

Can moving-average crossovers on the cached broad crypto market-cap series be used as an early **attention signal** for the frozen U10 strategy?

Specifically test:

1. SMA25 crossing SMA50;
2. SMA25 then crossing SMA100;
3. alternative cascade: SMA25 crossing SMA50, then SMA50 crossing SMA100.

Test bullish and bearish directions symmetrically.

## Frozen data

Market-cap source:

`data/market/cryptocap_total_d1.csv`

Canonical identity:
- symbol: `CRYPTOCAP:TOTAL`
- rows: 1,398
- range: 2022-11-29 through 2026-09-26
- SHA256: `d0c9b3bac915e484069c6302ddce1a0b050abbb8de155e4106e39cf6e519d3ee`

CACHE_FIRST only. This test must not access TradingView or other market-cap sources.

## Frozen U10

Universe:

`RR_TARGET_U10_CANDIDATE_HBAR_V1`

Assets:

`TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR`

Core mechanics unchanged:
- Binance Spot D1
- 180d rolling median
- ARM 15%
- reversal 3%
- strongest confirmed max-dislocation router
- signal close T -> next available daily open
- 0.1% modeled transition cost
- all 10 possible starting assets
- evaluation starts at MATURE: 2023-10-31

## SMA construction

From cached daily TOTAL close:

- SMA25
- SMA50
- SMA100

Cross definition uses end-of-day values:

### Bullish
- `SMA25_CROSS_ABOVE_50`: previous SMA25 <= SMA50 and current SMA25 > SMA50
- `SMA25_CROSS_ABOVE_100`: previous SMA25 <= SMA100 and current SMA25 > SMA100
- `SMA50_CROSS_ABOVE_100`: previous SMA50 <= SMA100 and current SMA50 > SMA100

### Bearish
- symmetric crosses below.

No threshold buffer is tuned.

## Two sequence interpretations

### Sequence A — SMA25 leads both

Bull:
1. SMA25 crosses above SMA50;
2. within 90 calendar days SMA25 crosses above SMA100;
3. no SMA25 cross back below SMA50 before step 2.

Bear:
1. SMA25 crosses below SMA50;
2. within 90 days SMA25 crosses below SMA100;
3. no SMA25 cross back above SMA50 before step 2.

Sequence confirmation date = step-2 date.

### Sequence B — classic stack progression

Bull:
1. SMA25 crosses above SMA50;
2. within 90 days SMA50 crosses above SMA100;
3. no SMA25 cross back below SMA50 before step 2.

Bear:
1. SMA25 crosses below SMA50;
2. within 90 days SMA50 crosses below SMA100;
3. no SMA25 cross back above SMA50 before step 2.

Sequence confirmation date = step-2 date.

The initial SMA25/50 cross is also evaluated separately as the earliest alert.

## U10 capital in USDT

For each event and each of the 10 frozen U10 starting states:

- normalize event-date U10 equity to `100 USDT`;
- measure value after:
  - 7 calendar days
  - 14 days
  - 30 days
  - 60 days
  - 90 days

Use last available strategy daily observation on or before each target date.

For each event type / horizon report:
- event count with full horizon coverage;
- median forward U10 return across events, where each event return is first the median across the 10 starting states;
- worst event return;
- best event return;
- positive-event rate;
- median ending capital from 100 USDT.

Also report TOTAL forward return over the same horizons.

## Event-detail output

For every crossover / completed sequence persist:
- date
- event/sequence type
- direction
- TOTAL
- SMA25 / SMA50 / SMA100
- U10 100-USDT capital after each horizon
- TOTAL forward return
- existing cached market state when available

## "Attention signal" interpretation

This test does **not** promote a trading rule.

A crossover can be called a useful **attention signal** only descriptively if:
- bullish events are followed by mostly positive U10 outcomes over one or more useful horizons; and/or
- bearish events are followed by mostly negative U10 outcomes;
- the effect is not driven by only one event.

Because the available U10 MATURE window is only 2023-10-31 through 2026-09-26, small event counts must be called out explicitly.

## Output

Persist:
- `cross_events.csv`
- `sequence_events.csv`
- `event_horizon_detail.csv`
- `event_summary.csv`
- `summary.json`
- `report.md`

## No-repeat / production boundary

Research only.

Do not change:
- paper/live U9
- frozen U10 candidate
- defensive overlay
- Telegram
- exchange execution

Target test level:

`TEST_LEVEL: GITHUB_ACTIONS_REPOSITORY_CACHE_STRESS_TEST`
