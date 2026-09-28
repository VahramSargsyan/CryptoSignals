# RR U10 DAY-0 PRICE RESET COUNTERFACTUAL V1 — EVIDENCE

Date: 2026-09-28
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Runtime identity

- Run: `36475082924`
- Source commit: `360330965f3e7dca8c65b7f2758db69a8b736047`
- Artifact ID: `10993326443`
- Artifact SHA256: `3d1d0fc3f572e7437059cdbde2a4c63b302eb5b82fee915ab07864790250a286`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_COUNTERFACTUAL_STRESS_TEST`

## Counterfactual semantics

Snapshot date:

`2023-10-31`

Freeze each U10 token's day-0 open price.

At every later canonical rotation:
1. current token is valued at its real historical next-open price;
2. 0.1% transition cost is charged;
3. destination token is bought at its frozen 2023-10-31 price, not its real
   price on the rotation date;
4. after entry, that token follows its real historical price path until the
   next canonical rotation;
5. the same rule applies to the last rotation before cutoff.

This is intentionally impossible stale-price execution and is not realizable
P&L.

## Frozen day-0 prices

| Asset | Day-0 open USDT |
|---|---:|
| TWT | 1.0736 |
| PEPE | 0.00000117 |
| BNB | 228 |
| TRX | 0.09558 |
| AAVE | 83.68 |
| AVAX | 11.46 |
| FIL | 3.857 |
| ALGO | 0.1121 |
| XRP | 0.578 |
| HBAR | 0.0537 |

## Route equivalence

All 10 starting states passed:
- identical canonical route: PASS
- identical transition count: PASS
- baseline execution capital matches the same real-price route: PASS

Only destination purchase price differs in DAY0_RESET.

## Final normalized capital

| Start | Transitions | Baseline final | DAY0_RESET final | Reset / baseline | Reset path monotonic |
|---|---:|---:|---:|---:|---|
| TWT | 26 | 3177.07 | 1,431,140 | 450.5x | NO |
| PEPE | 25 | 3191.20 | 1,282,190 | 401.8x | NO |
| BNB | 24 | 2541.91 | 1,014,640 | 399.2x | NO |
| TRX | 26 | 3312.90 | 1,492,330 | 450.5x | NO |
| AAVE | 26 | 3436.30 | 1,533,990 | 446.4x | NO |
| AVAX | 25 | 3377.88 | 1,425,800 | 422.1x | NO |
| FIL | 26 | 2913.89 | 1,433,030 | 491.8x | NO |
| ALGO | 25 | 2855.54 | 1,205,320 | 422.1x | NO |
| XRP | 27 | 3932.38 | 1,798,250 | 457.3x | NO |
| HBAR | 26 | 3737.86 | 1,514,670 | 405.2x | NO |

Aggregate:
- median baseline final capital: `3252.05 USDT`
- median DAY0_RESET final capital: `1,432,085 USDT`
- median DAY0_RESET / baseline multiple: `434.3x`
- all-start reset paths monotonic: NO

## Interpretation

The synthetic result explodes because later rotations can buy destination
tokens at stale 2023-10-31 prices even after those tokens have appreciated
substantially in the real market.

Therefore the test isolates a mathematical amplification effect:

`REAL_SOURCE_MARK_TO_MARKET + FROZEN_DESTINATION_PURCHASE_PRICE`

It does not prove achievable trading profit.

The fact that all reset paths remain non-monotonic is also important:
even with the artificial stale-price advantage, some holding periods still
reduce capital before the next rotation.

Frozen classification:

`DAY0_RESET_CAUSES_EXTREME_SYNTHETIC_AMPLIFICATION_BUT_NOT_MONOTONIC_GROWTH`

## Production boundary

No live/paper, Telegram, exchange, allocation, universe or execution behavior
changed.
