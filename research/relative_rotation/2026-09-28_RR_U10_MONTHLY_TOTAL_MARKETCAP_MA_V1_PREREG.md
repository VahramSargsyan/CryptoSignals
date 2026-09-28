# RR U10 MONTHLY TOTAL MARKET CAP + MA V1 — PREREG

Date: 2026-09-28
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Goal

Build one unified monthly view connecting:
- total crypto market capitalization,
- BTC monthly return,
- frozen U10 strategy monthly return,
- broad market-cap trend / moving-average state.

The purpose is descriptive regime research: understand when the market appears to transition from deterioration to expansion and vice versa.

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

Baseline snapshot:
- 2023-10-31

Monthly rows:
- 2023-10 through 2026-09
- final row uses 2026-09-26 cutoff rather than a future month-end

This matches the existing MATURE research period.

## Total market-cap source

Use CoinMarketCap public historical snapshot pages:

`https://coinmarketcap.com/historical/YYYYMMDD/`

Extract the page's embedded `__NEXT_DATA__` JSON:

`props.pageProps.globalMetrics.marketCap`

This is the historical global market-cap snapshot value reported by CoinMarketCap for that date.

For each requested month-end:
- request the exact calendar month-end;
- if unavailable, retry backward up to 3 calendar days;
- persist the actual snapshot date used;
- fail the test if no value is found.

No summing of individual tokens and no synthetic U10 proxy is allowed for this column.

## BTC monthly return

Binance BTCUSDT D1.

For each monthly row:
- take the last available BTC close on or before the snapshot/cutoff date;
- monthly return = current row close / previous row close - 1.

## U10 monthly return

For each of the 10 possible starting assets:
- simulate the frozen U10 once across the full MATURE period;
- sample strategy equity at each month-end row;
- compute each start-state monthly return from consecutive sampled equity values.

Monthly U10 column:
- median monthly return across the 10 frozen starting states.

Also persist:
- minimum start-state monthly return;
- maximum start-state monthly return.

Do not choose a favorable start asset.

## Market-cap monthly change

`market_cap_mom = current_month_end_market_cap / previous_month_end_market_cap - 1`

## Moving averages

Because this source is sampled monthly rather than daily, do NOT label the following as exact daily SMA50/100/200/300.

Use fixed monthly equivalents:

- `MC_MA2` ≈ 60 calendar days, nearest coarse analogue of SMA50
- `MC_MA3` ≈ 90 calendar days, nearest coarse analogue of SMA100
- `MC_MA7` ≈ 210 calendar days, nearest coarse analogue of SMA200
- `MC_MA10` ≈ 300 calendar days, nearest coarse analogue of SMA300

These are simple moving averages of monthly market-cap snapshots.

The previously completed exact BTC SMA200 test remains the exact daily-SMA reference:
- BTC above SMA200 -> U10 strongly positive historically
- BTC below SMA200 -> U10 negative historically

## MA state descriptors

For every row with enough history, calculate:

1. Price-to-MA flags:
   - CAP > MA2
   - CAP > MA3
   - CAP > MA7
   - CAP > MA10

2. Alignment:
   - `FULL_BULL_ALIGNMENT`: CAP > MA2 > MA3 > MA7 > MA10
   - `BULL_BUILDING`: CAP > MA2 and MA2 > MA3, but full bull alignment is absent
   - `MIXED`: neither bullish nor bearish definition
   - `BEAR_BUILDING`: CAP < MA2 and MA2 < MA3, but full bear alignment is absent
   - `FULL_BEAR_ALIGNMENT`: CAP < MA2 < MA3 < MA7 < MA10

3. Slopes:
   - each MA rising / falling versus previous monthly row.

No thresholds or MA periods are optimized after seeing U10 returns.

## Transition analysis

Descriptively identify:
- first transition into BULL_BUILDING;
- first transition into FULL_BULL_ALIGNMENT;
- first loss of full bull alignment;
- first transition into BEAR_BUILDING;
- first transition into FULL_BEAR_ALIGNMENT;
- reversals back toward bullish alignment.

For each transition, show:
- market cap level and MoM change,
- BTC monthly return,
- U10 monthly return,
- next 1-month and next 3-month cumulative BTC/U10 return where enough future data exists.

This is descriptive, not a production timing rule.

## Relationship statistics

Across monthly rows with valid returns, report:
- Pearson correlation: market-cap MoM vs BTC monthly return;
- Pearson correlation: market-cap MoM vs U10 median monthly return;
- same-month directional agreement rates.

Important interpretation:
Market capitalization is mechanically linked to crypto prices (price × circulating supply), so correlation is expected and does not prove market-cap change causes asset-price change.

## Output

Persist:
- `monthly_table.csv`
- `transition_events.csv`
- `summary.json`
- `report.md`

## No-repeat / production boundary

Research only.

Do not alter:
- paper/live U9,
- frozen U10 candidate,
- defensive overlay,
- Telegram,
- exchange execution.

Target test level:
`TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`
