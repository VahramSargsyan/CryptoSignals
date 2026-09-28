# Relative Rotation U10 Combined TOTAL + BTC/ETH Decision Log V1

Date: 2026-09-28
Status: ACTIVE RESEARCH MEMORY
Workflow mode: STRESS_TEST_ONLY
Production change: NONE

## Canonical evidence

`research/relative_rotation/2026-09-28_RR_U10_COMBINED_TOTAL_BTCETH_REGIME_V1_EVIDENCE.md`

Runtime:
- run: `36460817442`
- source SHA: `fdeb2aa4074ffe387c4a6419b122347a850c9f45`
- artifact: `10987291497`
- artifact SHA256: `160aa2d22cbdff2a9342670c40811c03e5250ab9d9590055c1fb6df66839eb41`
- TEST_LEVEL: `GITHUB_ACTIONS_REPOSITORY_CACHE_PLUS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Frozen classification

`PARTIAL_COMBINATION_BENEFIT / BULLISH_LONG_HORIZON_ONLY`

## Bullish side

`COMBO_BULL_25 = TOTAL bullish + BTC/ETH persistently below SMA25`

- 60d median U10: +23.0%, positive 80.0%
- 90d median U10: +18.1%, positive 73.3%
- improves 60/90d results versus both TOTAL_BULL_ONLY and ETH_FAST_25_ONLY
- not useful as a short-term entry signal

`COMBO_BULL_50 = TOTAL bullish + BTC/ETH persistently below SMA50`

- 30d +13.8%
- 60d +13.7%
- 90d +13.9%
- materially improves the ratio-only SMA50 state
- does not consistently beat TOTAL_BULL_ONLY

Frozen interpretation:

`COMBINED_BULL = BULLISH_ATTENTION / ALLOW_CAPITAL_TO_KEEP_WORKING`

Not a precise entry command.

## Risk side

`COMBO_RISK_200 = TOTAL bearish + BTC/ETH persistently above SMA200`

- 7d -2.2%
- 14d -8.3%
- 30d -1.9%
- 60d -3.1%
- 90d +38.6%

This combination sharpens short-term risk but fails as a medium/long exit signal.

Standalone `BTC_LONG_200_ONLY` remained cleaner for medium/long risk:
- 30d -3.8%
- 60d -12.2%
- 90d -10.9%

Frozen interpretation:

`COMBO_RISK_200 = SHORT_TERM_RISK_ATTENTION, NOT LONG_TERM_EXIT`

Do not replace the standalone BTC/ETH>SMA200 warning with the combined bearish signal.

## Current state at cutoff

2026-09-26:
- TOTAL: BULL_BUILDING
- BTC/ETH = 31.314082
- SMA25 = 31.434973
- SMA50 = 32.018621
- SMA200 = 34.082697
- ETH_FAST_25 = TRUE
- ETH_MEDIUM_50 = TRUE
- BTC_LONG_200 = FALSE
- COMBO_BULL_25 = TRUE
- COMBO_BULL_50 = TRUE
- COMBO_RISK_200 = FALSE

Frozen descriptive label:

`BULLISH_ROTATION_COMBO_ACTIVE`

No production meaning is implied.

## No-repeat / production boundary

Do not rerun unchanged on the same history.

A new study requires unseen data, changed joint-state semantics, changed U10 strategy/universe, or a separate forward/cash-withdrawal protocol.

No live/paper, Telegram, exchange, allocation, or execution behavior changed.
