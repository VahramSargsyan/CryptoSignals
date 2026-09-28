# DAILY CONFIRMATION REVERSAL LEDGER V1

Date: 2026-09-28  
Workflow mode: STRESS_TEST_ONLY  
Production/live changes: NONE

## Purpose

Record the actual daily reversal percentage at every unique sequential Relative Rotation transition used by the current TARGET U9 path set.

This answers: when the frozen engine requires reversal >=3%, how large was the reversal by the daily candle that actually confirmed the rotation?

## Dataset

Frozen GitHub dataset cache:

- release tag: `rr-dataset-cache-v1-36405537505`
- source: Binance Spot
- timeframe used for this ledger: 1D closed candles
- history start: 2023-05-05
- evaluation path window: 2023-10-31 through 2026-09-26
- TARGET U9: TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP
- lookback: 180 daily closes
- ARM threshold: 15%
- reversal threshold: 3%

The nine possible TARGET-U9 start-asset paths were replayed with the existing strongest-max-dislocation router. Their selected transitions were deduplicated by confirmation date + from_asset + to_asset, producing 34 unique sequential transitions.

## Field meanings

- `arm_pct`: absolute deviation from rolling median on the daily close that first armed that pair cycle.
- `max_dislocation_pct`: largest absolute daily-close dislocation reached after ARM and before confirmation.
- `daily_reversal_pct`: actual reversal from the post-ARM extreme on the daily close that triggered CONFIRMED.
- `confirm_deviation_pct`: absolute deviation from rolling median remaining on the confirmation candle.

## Main result

Unique sequential transitions: **34**

Actual daily confirmation reversal:

- minimum: **3.01%**
- median: **5.00%**
- mean: **6.11%**
- maximum: **15.46%**

Count at or above selected reversal levels:

- >=3%: 34 / 34
- >=4%: 23 / 34
- >=5%: 17 / 34
- >=6%: 13 / 34
- >=8%: 6 / 34
- >=10%: 4 / 34
- >=12%: 3 / 34
- >=14%: 2 / 34

The two largest daily confirmation reversals were:

1. 2025-09-21 — TWT -> FIL — **15.46%**
2. 2026-08-25 — PEPE -> TWT — **14.86%**

The largest post-ARM dislocation in the selected transition set was:

- 2025-11-08 — FIL -> PEPE — ARM **51.57%**, max dislocation **142.68%**, daily confirmation reversal **11.22%**

## Interpretation boundary

This ledger is descriptive historical evidence only.

It does not approve changing the 3% threshold, changing the daily confirmation rule, or promoting an intraday exit rule.

The result does show that the daily close frequently confirms substantially later than the nominal 3% reversal threshold: half of the selected transitions confirmed at approximately 5% or more, and 4 of 34 confirmed at 10% or more.

## Machine-readable evidence

See:

`research/relative_rotation/2026-09-28_DAILY_CONFIRMATION_REVERSAL_LEDGER_V1.csv`

TEST_LEVEL: LOCAL_BACKTEST_EXECUTED / FROZEN_DATASET
