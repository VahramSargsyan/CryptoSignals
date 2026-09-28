# RR U10 MONTHLY TRADINGVIEW TOTAL + SMA V1 — PREREG

Date: 2026-09-28
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Source correction

A first exploratory run attempted to read CoinMarketCap historical-page
`pageProps.globalMetrics.marketCap`. Validation showed that field is a
current-site global metric even when the page URL is historical. The resulting
near-flat ~$2.86T series was therefore invalid for historical analysis.

That exploratory output is classified:

`INVALID_SOURCE_SEMANTICS`

It must not be used as evidence and will not be merged as a result.

## Goal

Build one unified monthly view connecting:

- broad crypto market capitalization,
- BTC monthly return,
- frozen U10 strategy monthly return,
- exact daily market-cap SMA structure.

Then inspect which pre-frozen moving-average configurations coincide with
market expansion ("market starts to live") and deterioration ("market starts
to die").

## Frozen strategy

Universe:

`RR_TARGET_U10_CANDIDATE_HBAR_V1`

Assets:

`TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR`

Core semantics unchanged:

- Binance Spot D1
- rolling median lookback 180d
- ARM 15%
- reversal 3%
- confirmed close T -> next available daily open
- strongest max-dislocation router
- 0.1% modeled transition cost
- all 10 starting assets

## Analysis window

Monthly table:

- baseline 2023-10-31
- then every month-end through 2026-08-31
- final partial month at 2026-09-26

This matches the existing MATURE research window.

## Market-cap source

Use TradingView symbol:

`CRYPTOCAP:TOTAL`

Definition: TradingView's accumulated market capitalization of the top-125
cryptocurrencies from its Crypto Coins Screener.

Retrieve daily bars through the open-source `tvdatafeed` client in anonymous
mode, pinned to source commit:

`rongardF/tvdatafeed@e6f6aaa7de439ac6e454d9b26d2760ded8dc4923`

Require at least 1,300 daily bars and coverage beginning no later than
2022-12-01 so SMA300 is fully warmed before the main analysis.

Persist the raw daily TOTAL series used by the run.

Important naming:

- call this `TradingView TOTAL` or `CRYPTOCAP:TOTAL`;
- do not call it the literal capitalization of every cryptocurrency in existence.

## Monthly TOTAL

For each table row:

- sample the last available TOTAL daily close on or before the row date;
- `TOTAL MoM = current / previous - 1`.

## BTC monthly return

Binance BTCUSDT D1.

- sample last available close on or before each table date;
- `BTC MoM = current / previous - 1`.

## U10 monthly return

For each of the 10 frozen possible starting assets:

- simulate the full MATURE U10 path once;
- sample strategy equity at each table date;
- calculate monthly return between adjacent samples.

Reported U10 monthly return:

`median monthly return across the 10 frozen starting states`

Also persist min/max start-state monthly return.

Do not select a favorable start.

## Exact TOTAL moving averages

Calculate on DAILY `CRYPTOCAP:TOTAL` closes:

- SMA50
- SMA100
- SMA200
- SMA300

These are exact daily simple moving averages on the retrieved TradingView TOTAL
series, not monthly approximations.

At each monthly row persist:

- TOTAL close
- SMA50 / SMA100 / SMA200 / SMA300
- percentage distance of TOTAL from each SMA
- slope sign of each SMA vs 5 trading days earlier

## Frozen daily MA states

No period or threshold optimization after inspection.

### FULL_BULL_ALIGNMENT

`TOTAL > SMA50 > SMA100 > SMA200 > SMA300`

### BULL_BUILDING

Not full bull, but:

`TOTAL > SMA50 > SMA100`

### FULL_BEAR_ALIGNMENT

`TOTAL < SMA50 < SMA100 < SMA200 < SMA300`

### BEAR_BUILDING

Not full bear, but:

`TOTAL < SMA50 < SMA100`

### MIXED

Anything else with full SMA history.

## Daily transition events

Identify exact dates for:

- TOTAL crossing above/below SMA50
- TOTAL crossing above/below SMA100
- TOTAL crossing above/below SMA200
- TOTAL crossing above/below SMA300
- first entry into FULL_BULL_ALIGNMENT
- first exit from FULL_BULL_ALIGNMENT
- first entry into FULL_BEAR_ALIGNMENT
- first exit from FULL_BEAR_ALIGNMENT

For MA-state entries/exits, record the raw first day and whether the state
persisted for at least 3 consecutive daily bars. The 3-day flag is diagnostic;
it does not redefine the raw event.

For each major state event, attach:

- TOTAL value
- BTC close
- next 30d BTC return
- next 90d BTC return
- next 30d U10 return if available from the strategy equity curve
- next 90d U10 return if available

This is descriptive only.

## Relationship statistics

Monthly:

- Pearson TOTAL MoM vs BTC MoM
- Pearson TOTAL MoM vs U10 monthly return
- directional agreement TOTAL vs BTC
- directional agreement TOTAL vs U10
- lag-1 correlation TOTAL MoM vs next-month BTC return
- lag-1 correlation TOTAL MoM vs next-month U10 return

Interpretation rule:

Market capitalization is mechanically linked to asset prices
(price × circulating supply). Same-period correlation is expected and does not
prove market cap causes price changes. Lagged relationships are diagnostic and
must not be called causal or predictive without separate out-of-sample evidence.

## Output

Persist:

- `total_daily.csv`
- `monthly_table.csv`
- `daily_transition_events.csv`
- `ma_state_summary.csv`
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

Target test level:

`TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`
