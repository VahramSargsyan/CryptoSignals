# U10 Trailing Profit Lock v1 — preregistration

Date: 2026-09-27
Mode: PATCH_FIX
Scope: research-only capital-management overlay

## Question

Instead of selling 20% immediately when U10 reaches 10x starting capital, what happens if 10x only activates a trailing-profit monitor?

Example intended behavior:

10,000 -> 100,000 -> 150,000 -> 130,000

If the running peak is 150,000 and the portfolio falls by at least 2x original capital (20,000), sell 20% into cash.

Re-enter that cash only after the frozen U10 reference equity falls to 50% below the locked peak.

For the example above:
- locked peak = 150,000
- cash-out trigger = at or below 130,000
- re-entry trigger = at or below 75,000

## Frozen U10

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

Frozen strategy mechanics:
- Binance Spot 1D closed candles
- rolling median 180 days
- ARM 15%
- reversal confirmation 3%
- strongest confirmed max-dislocation router
- next-open strategy execution
- 0.1% modeled transition cost per U10 rotation

## Capital overlay

Starting capital:

10,000 USDT

Activation threshold:

10x initial capital = 100,000 USDT

Trailing cash-out threshold:

2x initial capital = 20,000 USDT absolute decline from the running post-activation U10 reference-equity peak.

Cash-out fraction:

20% of the actually invested sleeve.

Cash-out execution:

On the next daily open after a closed-candle trigger, after any already-pending U10 strategy transition.

Cash-out cost:

0.1% of the sold sleeve.

## Reference equity

A parallel no-overlay U10 reference portfolio follows the exact same U10 route and normal 0.1% strategy-transition costs.

Reference equity is used only to determine:
- first 10x activation;
- running peak before cash-out;
- 20,000 USDT decline trigger;
- 50% peak drawdown re-entry trigger.

This prevents the parked cash sleeve from mechanically changing the trigger thresholds.

## Locked peak

Before cash-out:
- update running peak whenever the reference U10 close makes a new high.

When the 20,000-USDT decline trigger fires:
- lock the current running peak;
- do not move that locked peak afterward.

## Re-entry

After cash-out execution:
- wait for a later daily close where reference U10 equity <= 50% of the locked peak;
- on the next daily open, after any pending U10 strategy rotation, re-enter the entire cash sleeve into the asset currently held by U10;
- apply 0.1% modeled re-entry cost.

The trigger-day close that caused the cash-out cannot simultaneously cause re-entry. Re-entry monitoring begins after cash-out execution.

## Comparison set

1. NO_OVERLAY — frozen U10 baseline.
2. IMMEDIATE_20PCT_AT_10X — prior experiment: sell 20% immediately after first 10x close and keep it in cash through end.
3. TRAILING_2X_THEN_REENTER_50PCT — this experiment.

Also report the prior 6-month and 12-month re-entry results from existing Strategy Lab evidence only as contextual comparison, not as recomputed outputs unless needed.

## Required output

- 10x activation date;
- running peak date/value;
- 2x-decline trigger date;
- cash-out execution date;
- locked peak;
- trigger decline in USDT and percent;
- cash parked after cost;
- 50%-from-peak re-entry trigger date;
- re-entry execution date / asset / cost;
- minimum total overlay equity after cash-out;
- overlay max drawdown after cash-out;
- terminal equity;
- terminal delta versus frozen U10 baseline;
- terminal delta versus immediate-20%-at-10x cash-forever variant.

## Interpretation boundary

This is a historical capital-management overlay, not a change to U10 signal logic.

Do not treat one observed peak/re-entry path as proof of an optimal rule.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_BACKTEST
