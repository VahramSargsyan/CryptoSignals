# Relative Rotation U10 Day-0 Price Reset Counterfactual Decision Log V1

Date: 2026-09-28
Status: ACTIVE RESEARCH MEMORY
Workflow mode: STRESS_TEST_ONLY
Production change: NONE

## Canonical evidence

`research/relative_rotation/2026-09-28_RR_U10_DAY0_PRICE_RESET_COUNTERFACTUAL_V1_EVIDENCE.md`

Runtime:
- run: `36475082924`
- source SHA: `360330965f3e7dca8c65b7f2758db69a8b736047`
- artifact: `10993326443`
- artifact SHA256: `3d1d0fc3f572e7437059cdbde2a4c63b302eb5b82fee915ab07864790250a286`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_COUNTERFACTUAL_STRESS_TEST`

## Frozen semantics

At 2023-10-31 freeze every U10 token open price.

At every later canonical rotation:
- value the source holding at its real execution-date open;
- charge 0.1%;
- buy the destination token at its frozen 2023-10-31 open;
- then let that token follow real historical prices until the next canonical
  rotation.

The canonical route/dates remain unchanged.

## Frozen result

Median from 100 USDT:

- real-price baseline: `3252.05 USDT`
- DAY0_RESET: `1,432,085 USDT`
- median amplification vs baseline: `434.3x`

All 10 start paths were non-monotonic even under this synthetic rule.

Frozen classification:

`DAY0_RESET_CAUSES_EXTREME_SYNTHETIC_AMPLIFICATION_BUT_NOT_MONOTONIC_GROWTH`

## Interpretation boundary

This is not executable P&L.

The huge result is created by deliberately allowing stale historical prices to
be reused as future destination purchase prices.

Do not cite this result as:
- a live return expectation;
- arbitrage;
- evidence that historical prices can be re-used;
- a production execution model.

It is only a mathematical counterfactual requested by the user.

## No-repeat / production boundary

Do not rerun unchanged on the same history.

A repeat requires changed counterfactual semantics, unseen data, or a documented
bug correction.

No live/paper, Telegram, exchange, allocation, universe or execution behavior
changed.
