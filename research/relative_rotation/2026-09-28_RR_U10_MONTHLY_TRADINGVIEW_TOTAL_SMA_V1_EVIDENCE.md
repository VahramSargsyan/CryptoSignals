# RR U10 MONTHLY TRADINGVIEW TOTAL + SMA V1 — EVIDENCE

Date: 2026-09-28  
Workflow mode: STRESS_TEST_ONLY  
Production/live changes: NONE

## Runtime identity

- Frozen universe: `RR_TARGET_U10_CANDIDATE_HBAR_V1`
- GitHub Actions run: `36436522995`
- Source commit: `c0b3aa4e180e46e2b973756311189803997a8a66`
- Artifact ID: `10975563877`
- Artifact: `rr-u10-monthly-total-marketcap-ma-v1-4`
- Artifact SHA256: `b8d5ebfd7397bccd9024eb1e37aef243904dd429ff680b70e41bb8c45b5e69e0`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Source / validation

Market-cap series:

`TradingView CRYPTOCAP:TOTAL`

Definition: accumulated market capitalization of the top-125 cryptocurrencies
in TradingView's Crypto Coins Screener.

Transport:
- anonymous TradingView historical data;
- open-source `tvdatafeed`;
- dependency pinned to
  `rongardF/tvdatafeed@e6f6aaa7de439ac6e454d9b26d2760ded8dc4923`.

Retrieved daily range:
- 2022-11-29 through 2026-09-26 after cutoff;
- enough warmup for exact SMA300 before MATURE.

### Rejected exploratory source

Run `36435470008` used
`CoinMarketCap historical page -> pageProps.globalMetrics.marketCap`.

Validation proved that field was current-site metadata, not the historical
snapshot total. That output is:

`INVALID_SOURCE_SEMANTICS`

It must not be cited as evidence.

## Exact daily moving averages

Calculated from daily TradingView TOTAL closes:

- SMA50
- SMA100
- SMA200
- SMA300

States were preregistered before the corrected runtime:

- `FULL_BULL_ALIGNMENT`:
  `TOTAL > SMA50 > SMA100 > SMA200 > SMA300`
- `BULL_BUILDING`:
  not full bull, but `TOTAL > SMA50 > SMA100`
- `FULL_BEAR_ALIGNMENT`:
  `TOTAL < SMA50 < SMA100 < SMA200 < SMA300`
- `BEAR_BUILDING`:
  not full bear, but `TOTAL < SMA50 < SMA100`
- otherwise `MIXED`.

## Unified monthly table

U10 monthly return is the median monthly return across all 10 frozen possible
starting assets.

| Month | TOTAL | TOTAL MoM | BTC MoM | U10 MoM | SMA50 | SMA100 | SMA200 | SMA300 | State |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 2023-10 | $1.26T | — | — | — | $1.08T | $1.08T | $1.10T | $1.08T | BULL_BUILDING |
| 2023-11 | $1.39T | +10.6% | +8.9% | +15.2% | $1.27T | $1.16T | $1.13T | $1.12T | FULL_BULL_ALIGNMENT |
| 2023-12 | $1.61T | +15.4% | +12.1% | +40.3% | $1.50T | $1.32T | $1.21T | $1.18T | FULL_BULL_ALIGNMENT |
| 2024-01 | $1.58T | -1.3% | +0.7% | -9.9% | $1.61T | $1.49T | $1.28T | $1.23T | MIXED |
| 2024-02 | $2.20T | +38.9% | +43.6% | +16.6% | $1.73T | $1.64T | $1.38T | $1.30T | FULL_BULL_ALIGNMENT |
| 2024-03 | $2.62T | +19.0% | +16.6% | +7.2% | $2.27T | $1.95T | $1.61T | $1.44T | FULL_BULL_ALIGNMENT |
| 2024-04 | $2.18T | -16.8% | -14.9% | -20.4% | $2.42T | $2.17T | $1.81T | $1.56T | MIXED |
| 2024-05 | $2.45T | +12.7% | +11.3% | +3.4% | $2.32T | $2.37T | $1.98T | $1.69T | MIXED |
| 2024-06 | $2.25T | -8.1% | -7.1% | -0.8% | $2.38T | $2.37T | $2.12T | $1.82T | MIXED |
| 2024-07 | $2.28T | +1.1% | +3.0% | +1.4% | $2.26T | $2.31T | $2.21T | $1.94T | MIXED |
| 2024-08 | $2.03T | -11.0% | -8.7% | -15.2% | $2.17T | $2.24T | $2.28T | $2.04T | BEAR_BUILDING |
| 2024-09 | $2.18T | +7.5% | +7.4% | +6.8% | $2.07T | $2.13T | $2.26T | $2.10T | MIXED |
| 2024-10 | $2.31T | +5.8% | +11.0% | -7.3% | $2.18T | $2.13T | $2.22T | $2.17T | BULL_BUILDING |
| 2024-11 | $3.33T | +44.5% | +37.2% | +252.7% | $2.63T | $2.36T | $2.31T | $2.29T | FULL_BULL_ALIGNMENT |
| 2024-12 | $3.18T | -4.5% | -2.9% | +17.6% | $3.29T | $2.78T | $2.46T | $2.44T | MIXED |
| 2025-01 | $3.45T | +8.4% | +9.5% | -3.5% | $3.38T | $3.15T | $2.65T | $2.54T | FULL_BULL_ALIGNMENT |
| 2025-02 | $2.74T | -20.5% | -17.7% | -15.2% | $3.24T | $3.30T | $2.77T | $2.61T | BEAR_BUILDING |
| 2025-03 | $2.63T | -4.2% | -2.1% | -10.7% | $2.86T | $3.09T | $2.88T | $2.64T | BEAR_BUILDING |
| 2025-04 | $2.90T | +10.3% | +14.1% | +23.5% | $2.70T | $2.90T | $2.96T | $2.68T | MIXED |
| 2025-05 | $3.23T | +11.4% | +11.1% | +19.2% | $3.05T | $2.89T | $3.09T | $2.78T | BULL_BUILDING |
| 2025-06 | $3.27T | +1.2% | +2.4% | -10.4% | $3.27T | $3.02T | $3.09T | $2.90T | MIXED |
| 2025-07 | $3.73T | +14.2% | +8.0% | +49.4% | $3.48T | $3.33T | $3.14T | $3.06T | FULL_BULL_ALIGNMENT |
| 2025-08 | $3.71T | -0.6% | -6.5% | -1.4% | $3.81T | $3.55T | $3.21T | $3.23T | MIXED |
| 2025-09 | $3.85T | +3.8% | +5.4% | +51.4% | $3.86T | $3.73T | $3.36T | $3.31T | MIXED |
| 2025-10 | $3.65T | -5.2% | -3.9% | -30.8% | $3.85T | $3.84T | $3.54T | $3.36T | MIXED |
| 2025-11 | $3.04T | -16.8% | -17.6% | +45.4% | $3.41T | $3.65T | $3.58T | $3.34T | BEAR_BUILDING |
| 2025-12 | $2.93T | -3.4% | -3.0% | -11.2% | $3.02T | $3.38T | $3.53T | $3.33T | BEAR_BUILDING |
| 2026-01 | $2.63T | -10.2% | -10.2% | +21.6% | $3.00T | $3.13T | $3.49T | $3.37T | BEAR_BUILDING |
| 2026-02 | $2.28T | -13.4% | -14.9% | -16.7% | $2.64T | $2.82T | $3.28T | $3.33T | FULL_BEAR_ALIGNMENT |
| 2026-03 | $2.32T | +1.9% | +2.0% | -26.9% | $2.33T | $2.62T | $3.05T | $3.24T | FULL_BEAR_ALIGNMENT |
| 2026-04 | $2.52T | +8.5% | +11.8% | +16.6% | $2.43T | $2.44T | $2.83T | $3.16T | MIXED |
| 2026-05 | $2.47T | -2.2% | -3.5% | +4.5% | $2.56T | $2.45T | $2.67T | $3.05T | MIXED |
| 2026-06 | $2.02T | -18.3% | -20.4% | +10.6% | $2.30T | $2.39T | $2.53T | $2.88T | FULL_BEAR_ALIGNMENT |
| 2026-07 | $2.14T | +6.2% | +7.3% | +8.2% | $2.16T | $2.32T | $2.41T | $2.70T | FULL_BEAR_ALIGNMENT |
| 2026-08 | $2.62T | +22.5% | +25.0% | +55.4% | $2.28T | $2.24T | $2.34T | $2.55T | BULL_BUILDING |
| 2026-09 | $2.87T | +9.4% | +7.4% | +73.6% | $2.55T | $2.35T | $2.39T | $2.51T | BULL_BUILDING |

## Relationship

Across valid monthly rows:

- Same-month TOTAL MoM vs BTC MoM Pearson: `0.980`
- Same-month direction agreement TOTAL vs BTC: `97.1%`
- Same-month TOTAL MoM vs U10 MoM Pearson: `0.609`
- Same-month direction agreement TOTAL vs U10: `74.3%`
- TOTAL MoM vs next-month BTC return Pearson: `0.032`
- TOTAL MoM vs next-month U10 return Pearson: `0.116`

Interpretation:

TOTAL moves very closely with BTC contemporaneously. It is not a strong
one-month leading predictor in this sample. The useful information is the
trend / MA regime, not the raw monthly TOTAL return alone.

This does not imply causality: market capitalization is mechanically derived
from asset prices and circulating supply.

## MA-state summary

| State | Months | Median TOTAL MoM | Median BTC | Median U10 | BTC positive months | U10 positive months |
|---|---:|---:|---:|---:|---:|---:|
| FULL_BULL_ALIGNMENT | 7 | +15.4% | +12.1% | +16.6% | 100.0% | 85.7% |
| BULL_BUILDING | 4 | +10.4% | +11.0% | +37.3% | 100.0% | 75.0% |
| MIXED | 14 | +0.2% | +1.6% | +2.4% | 57.1% | 57.1% |
| BEAR_BUILDING | 6 | -10.6% | -9.5% | -11.0% | 0.0% | 33.3% |
| FULL_BEAR_ALIGNMENT | 4 | -5.8% | -6.5% | -4.3% | 50.0% | 50.0% |

## Major cycle transition findings

### Bull phase beginning in late 2023

- 2023-10 month-end: BULL_BUILDING
- 2023-11-18: first 3-day-confirmed FULL_BULL_ALIGNMENT
- November 2023 month-end remained FULL_BULL
- November BTC: +8.9%
- November U10: +15.2%
- December U10: +40.3%

### 2024 correction and renewed acceleration

The market never reached a durable full-bear alignment in the 2024 correction.

- 2024-08 month-end: BEAR_BUILDING
- September: MIXED
- October: BULL_BUILDING
- 2024-10-14: TOTAL crossed above SMA200
- 2024-10-26: crossed back above SMA200 after a brief whipsaw
- 2024-11-05: crossed above SMA200 again after one-day dip
- 2024-11-24: 3-day-confirmed FULL_BULL_ALIGNMENT
- November month-end TOTAL: +44.5%
- BTC: +37.2%
- U10: +252.7%

This shows that FULL_BULL is a confirmation signal and can arrive after part of
the move has already begun.

### Market deterioration into the 2025/2026 bear phase

Layered deterioration:

1. 2025-10-14: confirmed exit from FULL_BULL_ALIGNMENT
2. 2025-11-03: TOTAL crossed below SMA200
3. 2025-11-04/05/06/07/13: SMA300 area showed repeated whipsaws; by
   2025-11-13 TOTAL was again below SMA300
4. November 2025 month-end: BEAR_BUILDING
5. December 2025: BEAR_BUILDING
6. January 2026: BEAR_BUILDING
7. 2026-02-19: first 3-day-confirmed FULL_BEAR_ALIGNMENT

This means waiting for FULL_BEAR is substantially slower than recognizing the
earlier loss of bull structure.

### Current bear-to-bull recovery in 2026

The important sequence is:

1. 2026-06 and 2026-07 month-end: FULL_BEAR_ALIGNMENT
2. 2026-08-14: another 3-day-confirmed entry into FULL_BEAR
3. 2026-08-17: 3-day-confirmed exit from FULL_BEAR
4. 2026-08-19: TOTAL crossed above SMA200
5. 2026-08-21: TOTAL crossed above SMA300
6. 2026-08 month-end: BULL_BUILDING
7. 2026-09-26: still BULL_BUILDING

At 2026-09-26:

- TOTAL: ~$2.87T
- SMA50: ~$2.55T
- SMA100: ~$2.35T
- SMA200: ~$2.39T
- SMA300: ~$2.51T

So:

`TOTAL > SMA50 > SMA100`

but:

`SMA100 < SMA200 < SMA300`

Therefore short/medium trend has turned bullish, while the full long-term bull
stack has **not yet** re-formed.

Frozen descriptive label:

`RECOVERY / BULL_BUILDING, NOT FULL_BULL_CONFIRMED`

## Practical regime interpretation for future research

The evidence suggests a layered model is more informative than a single
bull/bear switch:

### Early recovery / "market begins to live"

Evidence pattern:

- exit from FULL_BEAR,
- TOTAL regains SMA200/SMA300,
- TOTAL > SMA50 > SMA100,
- SMA50 / SMA100 start rising.

### Confirmed bull

`TOTAL > SMA50 > SMA100 > SMA200 > SMA300`

Historically, FULL_BULL monthly rows had:
- median BTC +12.1%
- median U10 +16.6%

But FULL_BULL can be late.

### Early deterioration / "market begins to die"

Evidence pattern:

- exit from FULL_BULL,
- TOTAL falls under SMA50/SMA100,
- then below SMA200,
- BEAR_BUILDING appears.

### Confirmed bear

`TOTAL < SMA50 < SMA100 < SMA200 < SMA300`

This is a slower confirmation layer and should not be treated as the earliest
warning.

These are research observations, not yet a production timing rule.

## No-repeat / production boundary

Do not rerun the identical corrected TOTAL/SMA analysis on the same history
merely to reproduce it.

A new run requires new unseen data, changed MA semantics, changed universe /
strategy semantics, or a separately preregistered defensive-overlay timing
test.

No paper/live, Telegram, exchange, or portfolio behavior changed.
