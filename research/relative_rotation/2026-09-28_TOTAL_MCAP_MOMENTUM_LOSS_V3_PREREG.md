# TOTAL MARKET CAP MOMENTUM-LOSS V3 — PREREG

Date: 2026-09-28  
Workflow mode: STRESS_TEST_ONLY  
Production/live changes: NONE

## Purpose

Test whether **loss of speed** in TOTAL and the fast SMA25 helps distinguish an upcoming normalization from continued extension when TradingView `CRYPTOCAP:TOTAL` is already stretched above SMA100.

This follows V1 (distance) and V2 (fan shape). V1 outcome barriers and episode de-duplication are reused unchanged.

## Frozen source

Repository cache only:

`data/market/cryptocap_total_d1.csv`

Canonical identity:
- symbol: `CRYPTOCAP:TOTAL`
- provider: TradingView
- timeframe: 1D
- rows: 1,398
- range: 2022-11-29 through 2026-09-26
- SHA256: `d0c9b3bac915e484069c6302ddce1a0b050abbb8de155e4106e39cf6e519d3ee`

No external market data may be used.

## Reused event semantics

Only **upside SMA100 episodes** are evaluated.

Thresholds:
- +10%
- +15%
- +20%
- +25%
- +30%
- +40%

Episode de-duplication:
- start on first close crossing the threshold from inside;
- no repeat at the same threshold while active;
- re-arm only after the directional gap compresses to <= 50% of threshold.

Outcome barriers:
- RETURN50: gap <= 50% of trigger gap
- BREAKOUT25: gap >= 125% of trigger gap
- horizon: 90 calendar days

## Causal momentum features at trigger

### TOTAL 5-day momentum

`mom5 = close_t / close_(t-5) - 1`

Previous 5-day momentum:

`prev_mom5 = close_(t-5) / close_(t-10) - 1`

Price momentum change:

`price_accel5 = mom5 - prev_mom5`

### SMA25 speed

Current SMA25 5-day slope:

`sma25_slope5 = SMA25_t / SMA25_(t-5) - 1`

Previous SMA25 5-day slope:

`prev_sma25_slope5 = SMA25_(t-5) / SMA25_(t-10) - 1`

SMA25 speed change:

`sma25_accel5 = sma25_slope5 - prev_sma25_slope5`

## Frozen labels

### DUAL_ACCELERATION

- `price_accel5 > 0`
- `sma25_accel5 > 0`

Both TOTAL momentum and fast-average speed are increasing.

### DUAL_DECELERATION

- `price_accel5 < 0`
- `sma25_accel5 < 0`

Both TOTAL momentum and fast-average speed are decreasing.

### MIXED_MOMENTUM

All other combinations.

No numeric acceleration threshold is tuned in V3; only the sign is used.

## Summary outputs

For each SMA100 trigger threshold and momentum label:
- N
- RETURN_FIRST %
- BREAKOUT_FIRST %
- unresolved/censored %
- median trigger distance
- median TOTAL 5d momentum
- median price acceleration
- median SMA25 speed acceleration
- median additional extension
- median days to maximum extension
- median days to RETURN50
- median days to SMA100 touch

Also report a descriptive aggregate across +10% through +30% threshold observations.

Because multiple threshold observations can belong to one broad market move, the aggregate is not treated as an independent-event estimate.

## Practical usefulness criterion

V3 is considered descriptively useful only if DUAL_DECELERATION and DUAL_ACCELERATION show a materially different historical continuation/return mix **and** the difference is not based on only one or two observations.

No production threshold or trading action is created.

## Current-state classification

Classify latest cached date 2026-09-26 using the same definitions.

## Boundary

Research only. Do not change:
- paper/live logic
- Telegram
- allocations
- exchange execution
- frozen U9/U10 universe decisions

Target test level:

`TEST_LEVEL: GITHUB_ACTIONS_REPOSITORY_CACHE_STRESS_TEST`
