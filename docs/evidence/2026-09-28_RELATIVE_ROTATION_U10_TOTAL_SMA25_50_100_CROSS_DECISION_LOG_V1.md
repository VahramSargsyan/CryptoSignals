# Relative Rotation U10 TOTAL SMA25/50/100 Cross Decision Log V1

Date: 2026-09-28  
Status: ACTIVE RESEARCH MEMORY  
Workflow mode: STRESS_TEST_ONLY  
Production change: NONE

## Canonical subject

Frozen universe:
`RR_TARGET_U10_CANDIDATE_HBAR_V1`

Canonical evidence:
`research/relative_rotation/2026-09-28_RR_U10_TOTAL_SMA25_50_100_CROSS_V1_EVIDENCE.md`

Runtime:
- run: `36441429077`
- source SHA: `14cd7d23c918e23c61ed2ae245904ab2f21bf596`
- artifact: `10978980735`
- artifact SHA256: `83d1ce89492a5ce557039a6fcc471cd09ef195d3a425126b539aa763dc7c2eea`
- TEST_LEVEL: `GITHUB_ACTIONS_REPOSITORY_CACHE_STRESS_TEST`

Market-cap source:
`data/market/cryptocap_total_d1.csv`

No TradingView refresh was used.

## Frozen conclusion

`SMA25_50 = EARLY_ATTENTION; SMA50_100 = STRONGER_CONFIRMATION`

### SMA25 crosses SMA50 upward

Do not treat as standalone buy signal.

Median U10:
- 7d: -5.9%
- 14d: -6.9%
- 30d: -6.3%
- 60d: +7.5%
- 90d: -4.9%

Interpretation:
`WATCH / PAY_ATTENTION`, not entry.

### SMA25 then crosses SMA100 upward

Intermediate confirmation, but noisy:
- 14d +7.8%
- 30d -4.0%
- 60d +12.2%
- 90d +25.7%

### SMA25 > SMA50, then SMA50 > SMA100

Strongest tested bullish confirmation:
- median confirmation lag ~29d
- 7d +6.4%
- 14d +11.7%
- 30d +3.6%
- 60d +22.7%, 100% positive among 4 full-horizon events
- 90d +21.8%, 75% positive among 4 full-horizon events

Sample is too small for production approval.

## Current 2026 sequence

- 2026-07-22: SMA25 > SMA50
- 2026-08-23: SMA25 > SMA100
- 2026-08-27: SMA50 > SMA100; state BULL_BUILDING

From 2026-08-27:
- 7d U10: +17.6% -> 100 USDT = 117.57
- 14d: +15.7% -> 115.74
- 30d: +92.7% -> 192.69

The current episode is part of the historical sample and must not be treated as independent validation.

## Bearish-cross conclusion

Do not use SMA25/50 or SMA25/100 bearish crosses as automatic U10 exits.

The U10 rotation often stayed profitable after broad-market bearish crosses.

The clearest bearish sequence, SMA25<50 then SMA50<100, had:
- 30d U10 median -7.4%
- 30d TOTAL median -10.1%
but U10 medians returned positive at 60d and 90d.

Frozen statement:

`BEARISH_MA_CROSS = RISK_ATTENTION, NOT AUTOMATIC_EXIT`

## Next distinct hypothesis

A combined regime filter may be worth testing separately:

- SMA25/50 early alert
- SMA25/100 and/or SMA50/100 confirmation
- TOTAL vs SMA200/SMA300 long-trend filter
- FULL_BULL / BULL_BUILDING / BEAR_BUILDING / FULL_BEAR state

Do not promote or implement this without a separate preregistered stress test.

## No-repeat

Do not rerun the identical crossover event study on the same cached history.

A new run requires:
- unseen dates,
- changed crossover semantics,
- changed U10 mechanics/universe,
- or a separate combined-filter / forward-validation protocol.

## Production boundary

No live/paper, Telegram, exchange, universe, allocation, or execution behavior changed.
