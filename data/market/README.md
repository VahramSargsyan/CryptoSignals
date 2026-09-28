# CRYPTOCAP TOTAL Daily Cache

Canonical repository cache for the broad crypto market-cap series used by the
relative-rotation research.

## Files

- `cryptocap_total_d1.csv`
- `cryptocap_total_d1.meta.json`

## Dataset

- Symbol: `CRYPTOCAP:TOTAL`
- Provider: TradingView
- Definition: accumulated market capitalization of the top-125 cryptocurrencies
  from TradingView's Crypto Coins Screener
- Timeframe: daily
- Rows: 1,398
- Range: 2022-11-29 through 2026-09-26
- Dataset SHA256:
  `d0c9b3bac915e484069c6302ddce1a0b050abbb8de155e4106e39cf6e519d3ee`

The cached CSV is the exact `total_daily.csv` from successful research run
`36436522995`, artifact `10975563877`.

## Cache policy

Use this repository cache first.

Do not fetch TradingView again when the requested analysis end date is on or
before `2026-09-26`.

External refresh is justified only when:

1. the requested analysis needs dates after the cache end date;
2. a documented source/semantic correction is required; or
3. the dataset fails hash/schema validation.

When refreshing, preserve the existing historical rows and append/replace only
verified daily observations. Recompute SMA/state-derived columns after the
combined daily series is assembled.

## Columns

The cache contains:

- timestamp
- open / high / low / close
- volume
- SMA50 / SMA100 / SMA200 / SMA300
- 5-day slope for each SMA
- `ma_state`

## Validation history

Bootstrap import:
- run `36439066214`: PASS

Cache-first validation without `tvdatafeed` installed:
- run `36439261228`: PASS

## Scope

This is research/reference data. It does not change live/paper execution,
Telegram behavior, the frozen U10 candidate, or exchange connectivity.
