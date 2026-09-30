# U8 Transition Ledger v1 — preregistration

Date: 2026-09-27
Mode: PATCH_FIX
Scope: research/backtest reporting only

## Objective

Add exact per-transition accounting to the frozen canonical U8 relative-rotation backtest so every executed rotation records token quantities and USDT values.

This patch does not change signal logic, router logic, universe, live configuration, or execution convention.

## Frozen U8

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK

Parameters:
- Binance Spot 1D closed candles
- rolling median: 180 days
- ARM threshold: 15%
- reversal confirmation: 3%
- strongest confirmed max-dislocation router
- next-open execution
- transition cost: 0.1% per executed transition

## Evaluation period

Use the maximum mature common-data span available through the 2026-09-26 closed candle.

Common raw panel begins when all eight assets have data.
The evaluation begins only after the 180-row warm-up is complete.

## Accounting normalization

Initial notional: 10,000 USDT at the first evaluation-day open.

The absolute quantities are normalized examples and scale linearly with starting capital.
Strategy percentage returns are unchanged by the chosen notional.

## Required ledger row

For every executed transition record:
- transition number
- signal date
- execution date
- pair
- from asset
- from quantity before swap
- from-asset execution open
- gross USDT value before transition cost
- transition-cost rate
- transition-cost USDT
- net USDT value after cost
- to asset
- to-asset execution open
- to quantity after swap
- shadow no-cost quantity
- cumulative fee drag in USDT
- cumulative fee drag percent versus no-cost shadow
- max dislocation that selected the route

## Validation

Run a parallel no-cost shadow portfolio through the exact same route.

Invariant:
actual_value / no_cost_value after N transitions = (1 - 0.001)^N

The runner must fail if this accounting identity is violated beyond floating-point tolerance.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_BACKTEST
