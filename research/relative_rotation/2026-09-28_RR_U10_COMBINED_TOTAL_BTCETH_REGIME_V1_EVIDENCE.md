# RR U10 COMBINED TOTAL + BTC/ETH REGIME V1 — EVIDENCE

Date: 2026-09-28
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Runtime identity

- Frozen universe: `RR_TARGET_U10_CANDIDATE_HBAR_V1`
- GitHub Actions run: `36460817442`
- Source commit: `fdeb2aa4074ffe387c4a6419b122347a850c9f45`
- Artifact ID: `10987291497`
- Artifact: `rr-u10-combined-total-btceth-regime-v1-1`
- Artifact SHA256: `160aa2d22cbdff2a9342670c40811c03e5250ab9d9590055c1fb6df66839eb41`
- TEST_LEVEL: `GITHUB_ACTIONS_REPOSITORY_CACHE_PLUS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Frozen inputs

### TOTAL

Repository cache only:

`data/market/cryptocap_total_d1.csv`

SHA256:

`d0c9b3bac915e484069c6302ddce1a0b050abbb8de155e4106e39cf6e519d3ee`

No TradingView refresh.

TOTAL bullish state:
- BULL_BUILDING
- FULL_BULL_ALIGNMENT

TOTAL bearish state:
- BEAR_BUILDING
- FULL_BEAR_ALIGNMENT

### BTC/ETH

`BTC_ETH_RATIO = BTCUSDT / ETHUSDT`

3-day persistent ratio states:
- ETH_FAST_25: ratio below SMA25 for current + prior 2 bars
- ETH_MEDIUM_50: ratio below SMA50 for current + prior 2 bars
- BTC_LONG_200: ratio above SMA200 for current + prior 2 bars

### Combined signals

- COMBO_BULL_25 = TOTAL_BULL AND ETH_FAST_25
- COMBO_BULL_50 = TOTAL_BULL AND ETH_MEDIUM_50
- COMBO_RISK_200 = TOTAL_BEAR AND BTC_LONG_200

Events are FALSE -> TRUE entries only.

## Frozen conclusion

`PARTIAL_COMBINATION_BENEFIT / BULLISH_LONG_HORIZON_ONLY`

The combination adds useful information on the bullish side, especially over
60-90 day horizons, but it does **not** produce a cleaner long-horizon bearish
exit/risk signal.

## Bullish combined signals

### COMBO_BULL_25

| Horizon | Events | Median U10 | 100 USDT -> | Positive |
|---|---:|---:|---:|---:|
| 7d | 17 | -1.3% | 98.74 | 41.2% |
| 14d | 17 | +1.1% | 101.06 | 52.9% |
| 30d | 16 | +4.9% | 104.86 | 56.2% |
| 60d | 15 | **+23.0%** | **123.03** | **80.0%** |
| 90d | 15 | **+18.1%** | **118.07** | **73.3%** |

Compared with TOTAL_BULL_ONLY:

- 60d return lift: +6.5 pp
- 60d positive-rate lift: +2.2 pp
- 90d return lift: +8.0 pp
- 90d positive-rate lift: +12.2 pp

Compared with ETH_FAST_25_ONLY:

- 60d return lift: +4.6 pp
- 60d positive-rate lift: +13.3 pp
- 90d return lift: +7.4 pp
- 90d positive-rate lift: +11.8 pp

Interpretation:

The combination is not useful as a short-term entry trigger.
It becomes more informative over 60-90 days.

### COMBO_BULL_50

| Horizon | Events | Median U10 | 100 USDT -> | Positive |
|---|---:|---:|---:|---:|
| 7d | 14 | +1.8% | 101.84 | 50.0% |
| 14d | 14 | +2.4% | 102.40 | 64.3% |
| 30d | 14 | **+13.8%** | **113.79** | 57.1% |
| 60d | 13 | +13.7% | 113.69 | **76.9%** |
| 90d | 13 | +13.9% | 113.87 | 61.5% |

Compared with ETH_MEDIUM_50_ONLY:

- 30d return lift: +7.2 pp
- 60d return lift: +7.7 pp
- 90d return lift: +16.5 pp
- 60d positive-rate lift: +18.1 pp
- 90d positive-rate lift: +17.8 pp

Compared with TOTAL_BULL_ONLY the advantage is mixed:
- 14d: +2.1 pp return lift
- 30d: -1.4 pp
- 60d: -2.9 pp
- 90d: +3.8 pp

Interpretation:

The BTC/ETH SMA50 layer clearly improves the ratio-only signal, but it does not
consistently dominate TOTAL_BULL alone.

## Bearish / risk combined signal

### COMBO_RISK_200

| Horizon | Events | Median U10 | 100 USDT -> | Positive |
|---|---:|---:|---:|---:|
| 7d | 11 | -2.2% | 97.83 | 36.4% |
| 14d | 11 | **-8.3%** | **91.68** | 45.5% |
| 30d | 11 | -1.9% | 98.08 | 45.5% |
| 60d | 11 | -3.1% | 96.91 | 45.5% |
| 90d | 11 | **+38.6%** | **138.56** | 54.5% |

This combination creates a stronger **near-term warning**:
- versus TOTAL_BEAR_ONLY:
  - 7d: -4.3 pp return lift (more negative)
  - 14d: -11.2 pp
- versus BTC_LONG_200_ONLY:
  - 7d: -1.8 pp
  - 14d: -9.1 pp

But it fails as a persistent exit signal:
- 90d U10 median rebounds to +38.6%
- it is much less negative than BTC_LONG_200_ONLY at 60/90d

Standalone BTC_LONG_200_ONLY remained cleaner for long-horizon U10 risk:
- 30d: -3.8%
- 60d: -12.2%
- 90d: -10.9%

Frozen interpretation:

`TOTAL_BEAR + BTC_ETH_ABOVE_SMA200 = SHORT_TERM_RISK_ALERT, NOT LONG_TERM_EXIT`

## Current state at 2026-09-26

- TOTAL state: `BULL_BUILDING`
- BTC/ETH: `31.314082`
- BTC/ETH SMA25: `31.434973`
- BTC/ETH SMA50: `32.018621`
- BTC/ETH SMA200: `34.082697`

States:
- ETH_FAST_25: TRUE
- ETH_MEDIUM_50: TRUE
- BTC_LONG_200: FALSE
- COMBO_BULL_25: TRUE
- COMBO_BULL_50: TRUE
- COMBO_RISK_200: FALSE

Descriptive current label:

`BULLISH_ROTATION_COMBO_ACTIVE`

This is not a future-return guarantee and not a production signal.

## Practical interpretation

### Best use of combined bull signal

The combination is better suited to:

`ALLOW_CAPITAL_TO_KEEP_WORKING / BULLISH_ATTENTION`

than to a precise entry date.

It is most useful when:
- TOTAL is already BULL_BUILDING or FULL_BULL;
- BTC/ETH is persistently below fast/medium moving averages.

### Best use of combined risk signal

The bearish combination is useful for:

`SHORT_TERM_RISK_ATTENTION`

especially on 7-14 day horizons.

It should not be used as:

`SELL_AND_STAY_OUT_FOR_90_DAYS`

because U10 historically often recovered strongly after those events.

## Relationship to withdrawal planning

For a future cash-withdrawal indicator, this evidence suggests:

- bullish combo active:
  no historical evidence that it is an especially strong reason to rush out of U10;
- bearish combo active:
  stronger reason to pay attention to near-term risk;
- standalone BTC/ETH above SMA200 remains more useful than the bearish combo for
  medium/long risk monitoring.

A cash-withdrawal model still requires separate preregistration because the
objective is different from maximizing strategy return.

## No-repeat

Do not rerun this exact combined study unchanged on the same historical window.

A new run requires:
- unseen dates;
- changed combined-state semantics;
- changed U10 mechanics/universe;
- or separate forward / cash-withdrawal validation.

## Production boundary

No live/paper, Telegram, exchange, universe, allocation, or execution behavior changed.
