# U10 Monthly Surge 3-Bar Fractal Buy-Stop v1 — preregistration

Date: 2026-09-28
Mode: STRESS_TEST_ONLY
Scope: research-only comparison of faster structural re-entry confirmation; no live/paper changes

## Objective

Test whether the main weakness observed in the 5-bar fractal re-entry — late confirmation after V-shaped recoveries — can be reduced by using a faster 3-bar pivot while leaving every other rule unchanged.

This study changes exactly one structural parameter:

- prior fractal: 2 bars left / pivot / 2 bars right
- candidate fractal: 1 bar left / pivot / 1 bar right

No other threshold or portfolio rule changes.

## Frozen P1 / P2 cash-out

P1:
- arm after +100% monthly U10 reference-equity surge versus prior completed selected-month extreme
- track post-arm running peak
- cash-out after >=5% daily-close pullback
- sell 30% next open
- 0.1% modeled cash-out cost

P2:
- identical except cash-out requires >=10% pullback

## Frozen search activation

After cash-out:
- continue tracking latest U10 reference running peak
- activate structural re-entry search only after reference daily-close equity is >=15% below that latest peak
- once activated, search remains active until re-entry

The 15% activation threshold is unchanged.

## Candidate 3-bar swing high

Use actual daily HIGH prices of the asset currently held by frozen U10.

A 3-bar confirmed swing high exists at bar t when:

- high[t] > high[t-1]
- high[t] > high[t+1]

The pivot becomes known only at the CLOSE of t+1.

Therefore its buy-stop can become active no earlier than the next daily session.

No same-bar fill is allowed on the confirmation candle.

## Buy-stop mechanics

Unchanged from 5-bar test:

- set buy-stop at the confirmed pivot HIGH
- later LOWER confirmed pivot highs ratchet the stop downward
- later higher pivots do not raise an already-lower stop
- if U10 rotates to another asset while cash is parked:
  - cancel old asset stop
  - reset the pivot sequence for the new current asset
  - do not reuse pivots formed before that asset became current
- at each eligible session:
  - frozen U10 rotation executes first at open
  - if asset changed, reset stop before fill check
  - if open >= stop, fill at open
  - else if session high >= stop, fill at stop
  - else keep order
- on fill:
  - reinvest all parked cash
  - apply 0.1% modeled re-entry cost

No fixed -25% re-entry.
No timeout.
No peak-reclaim.
No moving average.
No stop buffer.

## Comparison controls

For canonical U10 and all 791 alternative U10 topologies compare:

1. baseline U10
2. fixed old-peak -25% re-entry
3. moving-reference-peak -25% re-entry
4. 5-bar fractal buy-stop
5. 3-bar fractal buy-stop

## Required metrics

Per universe / variant:
- final equity
- delta vs baseline
- delta vs fixed -25%
- delta vs moving-peak -25%
- delta vs 5-bar fractal
- max drawdown
- cash-outs
- re-entries
- unfinished cycles
- days in cash
- confirmed pivots
- stop replacements
- gap-open fills
- stop-price fills

Per re-entry cycle:
- cash-out date
- search activation date
- current asset
- pivot center date
- pivot confirmation date
- stop price
- fill date / price / type
- reference drawdown from latest peak at re-entry
- cash-cycle duration
- asset resets

Cross-topology:
- final > baseline rate
- DD better than baseline rate
- BOTH improve rate
- 3-bar > 5-bar rate
- 3-bar > moving-peak -25% rate
- 3-bar > fixed -25% rate
- unfinished-cycle rate
- median/q25/q75 terminal delta vs baseline
- median delta vs 5-bar
- distribution of actual re-entry depth
- median cash duration

## Current-data boundary

Primary stress period:
2023-10-31 -> 2026-09-26

Use the same exhaustive 792 U10 topologies:
- mandatory ATOM/TWT/PEPE
- choose 7 of the fixed remaining 12-asset pool

These share the same market dates and are topology robustness, not independent future histories.

## Old-history guardrail

Do not tune this rule using 2020-2022.

The old period has already been consumed by prior research and can only be used later as REUSED_HISTORY_SECONDARY evidence.

## Anti-overfit guardrail

Do not test:
- 0-left / 0-right pivots
- 1-left / 2-right
- 2-left / 1-right
- different search activation thresholds
- stop buffers
- timeouts
- moving averages

This study is a direct 5-bar versus 3-bar confirmation-speed test.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
