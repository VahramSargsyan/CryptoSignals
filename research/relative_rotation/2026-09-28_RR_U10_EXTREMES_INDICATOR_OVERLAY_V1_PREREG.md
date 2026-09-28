# RR U10 EXTREMES + INDICATOR OVERLAY V1 — PREREG

Date: 2026-09-28
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Question

What were TOTAL-market and ETH/BTC conditions during:
1. explosive U10 months;
2. severe U10 monthly drawdowns;
3. previously documented major bull-exit / bear / recovery milestones?

## Frozen thresholds

- EXPLOSIVE_MONTH: U10 monthly median return >= +40%
- SEVERE_DIP_MONTH: U10 monthly median return <= -15%

Thresholds are frozen before runtime inspection.

## Frozen U10

Use the existing `RR_TARGET_U10_CANDIDATE_HBAR_V1` mechanics unchanged,
all 10 frozen starting assets, MATURE 2023-10-31 through 2026-09-26.

## TOTAL source

Use repository cache only:
`data/market/cryptocap_total_d1.csv`

SHA256:
`d0c9b3bac915e484069c6302ddce1a0b050abbb8de155e4106e39cf6e519d3ee`

Use exact daily TOTAL SMA25/50/100/200/300 and the already frozen TOTAL state
classification.

## ETH/BTC

Use Binance BTCUSDT / ETHUSDT D1 closes from the already tested source family.

Compute:
`ETH_BTC = ETHUSDT / BTCUSDT`

Then exact daily:
- SMA25
- SMA50
- SMA100
- SMA200

Important: do not invert BTC/ETH moving averages. Compute ETH/BTC moving averages directly from ETH/BTC daily values.

For each monthly extreme report:
- U10 monthly return
- TOTAL monthly change
- TOTAL state at month-end
- TOTAL distance from SMA50/100/200/300 at month-end
- ETH/BTC monthly change
- ETH/BTC distance from SMA25/50/100/200 at month-end

## Frozen cycle milestones

Use these dates already documented in prior canonical evidence:

### 2024 correction / recovery
- 2024-04-12: loss of bullish structure into MIXED
- 2024-10-26: return to BULL_BUILDING
- 2024-11-24: FULL_BULL confirmation

### 2025/2026 deterioration / recovery
- 2025-10-14: confirmed exit from FULL_BULL
- 2025-11-03: TOTAL below SMA200
- 2026-02-19: FULL_BEAR confirmation
- 2026-05-02: temporary BULL_BUILDING recovery attempt
- 2026-05-22: return to MIXED
- 2026-08-17: confirmed exit from FULL_BEAR
- 2026-08-19: TOTAL regains SMA200
- 2026-08-21: TOTAL regains SMA300
- 2026-08-27: BULL_BUILDING
- 2026-09-26: current cutoff state

For every milestone report TOTAL and ETH/BTC distances from all relevant SMAs.

## Aggregate pattern checks

For explosive months and severe-dip months separately report:
- fraction with positive TOTAL MoM;
- fraction whose TOTAL state is bullish;
- fraction with ETH/BTC positive MoM;
- fraction with ETH/BTC above SMA25 / 50 / 100 / 200.

No causal claim is allowed.

## Output

Persist:
- `extreme_months.csv`
- `cycle_milestones.csv`
- `pattern_summary.csv`
- `summary.json`
- `report.md`

## Production boundary

No live/paper, Telegram, exchange, allocation, or execution changes.

Target test level:
`TEST_LEVEL: GITHUB_ACTIONS_REPOSITORY_CACHE_PLUS_LIVE_PUBLIC_DATA_STRESS_TEST`
