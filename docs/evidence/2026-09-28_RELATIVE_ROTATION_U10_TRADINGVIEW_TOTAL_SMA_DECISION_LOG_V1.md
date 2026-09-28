# Relative Rotation U10 TradingView TOTAL + SMA Decision Log V1

Date: 2026-09-28
Status: ACTIVE RESEARCH MEMORY
Workflow mode: STRESS_TEST_ONLY
Production change: NONE

## Canonical subject

Frozen universe:

`RR_TARGET_U10_CANDIDATE_HBAR_V1`

Canonical evidence:

`research/relative_rotation/2026-09-28_RR_U10_MONTHLY_TRADINGVIEW_TOTAL_SMA_V1_EVIDENCE.md`

Runtime:

- GitHub Actions run: `36436522995`
- Source commit: `c0b3aa4e180e46e2b973756311189803997a8a66`
- Artifact ID: `10975563877`
- Artifact SHA256: `b8d5ebfd7397bccd9024eb1e37aef243904dd429ff680b70e41bb8c45b5e69e0`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Market-cap source

Use `TradingView CRYPTOCAP:TOTAL` for this research family.

Definition: TradingView accumulated market capitalization of the top-125 cryptocurrencies.

Daily history was retrieved with pinned:

`rongardF/tvdatafeed@e6f6aaa7de439ac6e454d9b26d2760ded8dc4923`

## Invalid-source memory

Never use run `36435470008` as market-cap evidence.

It is frozen as:

`INVALID_SOURCE_SEMANTICS`

Reason: CoinMarketCap historical-page `pageProps.globalMetrics.marketCap` was verified to be current-site metadata and produced a false near-flat historical series.

## Exact market-cap MA framework

Daily:
- SMA50
- SMA100
- SMA200
- SMA300

Frozen states:

- FULL_BULL_ALIGNMENT:
  `TOTAL > SMA50 > SMA100 > SMA200 > SMA300`
- BULL_BUILDING:
  `TOTAL > SMA50 > SMA100`, without full bull
- FULL_BEAR_ALIGNMENT:
  `TOTAL < SMA50 < SMA100 < SMA200 < SMA300`
- BEAR_BUILDING:
  `TOTAL < SMA50 < SMA100`, without full bear
- MIXED otherwise.

## Frozen current interpretation — 2026-09-26

State:

`BULL_BUILDING`

Values:
- TOTAL: ~$2.87T
- SMA50: ~$2.55T
- SMA100: ~$2.35T
- SMA200: ~$2.39T
- SMA300: ~$2.51T

Therefore:
- TOTAL is above all four moving averages;
- fast structure is bullish: TOTAL > SMA50 > SMA100;
- long moving averages have not completed bullish ordering because SMA100 < SMA200 < SMA300.

Frozen description:

`RECOVERY / BULL_BUILDING, NOT FULL_BULL_CONFIRMED`

## Current bear-to-bull transition sequence

- 2026-06 month-end: FULL_BEAR
- 2026-07 month-end: FULL_BEAR
- 2026-08-14: 3-day-confirmed entry into FULL_BEAR
- 2026-08-17: 3-day-confirmed exit from FULL_BEAR
- 2026-08-19: TOTAL crossed above SMA200
- 2026-08-21: TOTAL crossed above SMA300
- 2026-08 month-end: BULL_BUILDING
- 2026-09-26: BULL_BUILDING

This is the documented recovery sequence.

## Prior bull-run transition example

Late 2024:
- 2024-08 month-end: BEAR_BUILDING
- 2024-09: MIXED
- 2024-10: BULL_BUILDING
- 2024-11-24: 3-day-confirmed FULL_BULL
- 2024-11 month:
  - TOTAL +44.5%
  - BTC +37.2%
  - U10 +252.7%

Important implication:

FULL_BULL is confirmation, not necessarily the earliest entry signal.

## Deterioration example

2025/2026:
- 2025-10-14: confirmed exit from FULL_BULL
- 2025-11-03: TOTAL crossed below SMA200
- 2025-11-13: after SMA300 whipsaws, TOTAL was again below SMA300
- 2025-11 through 2026-01: BEAR_BUILDING at month-end
- 2026-02-19: 3-day-confirmed FULL_BEAR

Important implication:

FULL_BEAR is late confirmation. Earlier deterioration is visible from loss of FULL_BULL, price below fast averages, and then SMA200 loss.

## Relationship summary

Monthly:
- TOTAL vs BTC same-month Pearson: 0.980
- TOTAL vs BTC direction agreement: 97.1%
- TOTAL vs U10 same-month Pearson: 0.609
- TOTAL vs U10 direction agreement: 74.3%
- TOTAL current month vs next-month BTC Pearson: 0.032
- TOTAL current month vs next-month U10 Pearson: 0.116

Interpretation:
- TOTAL is highly contemporaneous with BTC;
- raw market-cap monthly change alone is not a strong one-month leading signal in this sample;
- MA structure is the more useful regime descriptor.

## MA-state monthly summary

- FULL_BULL_ALIGNMENT:
  - 7 months
  - median TOTAL +15.4%
  - median BTC +12.1%
  - median U10 +16.6%
  - BTC positive 100%
  - U10 positive 85.7%

- BULL_BUILDING:
  - 4 months
  - median TOTAL +10.4%
  - median BTC +11.0%
  - median U10 +37.3%
  - BTC positive 100%
  - U10 positive 75%

- MIXED:
  - 14 months
  - median TOTAL +0.2%
  - median BTC +1.6%
  - median U10 +2.4%

- BEAR_BUILDING:
  - 6 months
  - median TOTAL -10.6%
  - median BTC -9.5%
  - median U10 -11.0%
  - BTC positive 0%
  - U10 positive 33.3%

- FULL_BEAR_ALIGNMENT:
  - 4 months
  - median TOTAL -5.8%
  - median BTC -6.5%
  - median U10 -4.3%

## Research implication

Use a layered regime model rather than one binary switch:

Early recovery:
- exit FULL_BEAR
- regain SMA200/SMA300
- TOTAL > SMA50 > SMA100
- fast MAs rising

Confirmed bull:
- TOTAL > SMA50 > SMA100 > SMA200 > SMA300

Early deterioration:
- exit FULL_BULL
- TOTAL under SMA50/SMA100
- then under SMA200
- BEAR_BUILDING

Confirmed bear:
- TOTAL < SMA50 < SMA100 < SMA200 < SMA300

This is descriptive research only, not yet a production timing rule.

## No-repeat / production boundary

Do not rerun the same corrected TOTAL/SMA study on identical history without materially new data or a changed hypothesis.

No paper/live, Telegram, exchange, universe, or allocation behavior changed.
