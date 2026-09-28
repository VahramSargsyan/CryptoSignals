# U10 Monthly Surge Fractal Buy-Stop Re-entry v1 — preregistration

Date: 2026-09-28
Mode: STRESS_TEST_ONLY
Scope: research-only re-entry experiment; no live/paper strategy changes

## Objective

Test whether the already-observed post-surge pullback effect can be monetized more robustly by replacing a fixed -25% re-entry level with a causal market-structure buy-stop.

The re-entry order is based on confirmed local highs of the currently held U10 asset, not on a fixed portfolio drawdown target.

## Frozen P1 / P2 cash-out

P1:
- arm after +100% monthly surge versus prior completed selected-month extreme
- track post-arm U10 reference running peak
- cash-out signal after >=5% daily-close pullback from that peak
- sell 30% of the invested sleeve next open
- 0.1% modeled cash-out cost

P2:
- identical except cash-out signal is >=10%

No other cash-out parameter changes.

## Fractal search activation

After actual cash-out execution, do not search for a re-entry immediately.

Activate fractal search only after U10 reference daily-close equity is at least 15% below the latest post-surge reference running peak.

The 15% activation threshold is fixed before viewing this test result.

While cash remains parked:
- the reference running peak may continue to update on new U10 daily-close highs;
- search activation is evaluated against that latest reference peak.

Once activated for a cash cycle, the search remains active until re-entry.

## Confirmed swing-high definition

Use the actual daily HIGH prices of the asset currently held by frozen U10.

A 5-bar confirmed swing high exists at bar t when:

- high[t] > high[t-1]
- high[t] > high[t-2]
- high[t] > high[t+1]
- high[t] > high[t+2]

The pivot becomes known only at the CLOSE of t+2.

Therefore a buy-stop based on that pivot can become active no earlier than the next daily session.

This is equivalent to the user's described structure:
two bars on the left, one local high, two bars on the right.

No future bars are visible before confirmation.

## Asset-change reset

The buy-stop belongs to the currently held U10 asset.

If frozen U10 rotates to another asset while cash is parked:
- cancel the old asset's buy-stop;
- reset the fractal sequence for the new current asset;
- require a newly confirmed 5-bar swing high after the later of:
  - fractal-search activation;
  - the date the new asset became the held U10 asset.

Do not reuse a pivot formed before the reset boundary.

## Trailing the buy-stop downward

After a confirmed swing high becomes available:
- set buy-stop at that pivot HIGH.

If a later confirmed swing high is LOWER:
- cancel the old stop;
- replace it with the lower pivot-high stop.

If a later confirmed swing high is higher:
- keep the lower existing stop.

Thus the order can only ratchet downward while the decline continues.

## Buy-stop execution

At each daily session after the order became active:

1. Frozen U10 performs any scheduled asset rotation at the open first.
2. If the current asset changed, cancel/reset the old order before evaluating a fill.
3. If the current asset open >= stop:
   - fill at that open price.
4. Else if the current asset daily high >= stop:
   - fill at the stop price.
5. Else:
   - order remains active.

On fill:
- reinvest the entire parked cash sleeve;
- apply 0.1% modeled re-entry cost;
- buy the current asset;
- resume normal U10 rotation with the enlarged invested sleeve.

No fixed -25% re-entry is used in this fractal variant.
No timeout is used.
No peak-reclaim trigger is used.

## Primary variants

P1-FRACTAL:
- +100% surge
- sell 30% after 5% pullback
- begin fractal search after 15% U10 reference drawdown from latest post-surge peak
- 5-bar swing-high buy-stop re-entry

P2-FRACTAL:
- same, but sell after 10% pullback

## Comparison controls

For canonical U10 and all 791 alternative U10 topologies compare:

1. BASELINE U10
2. P1/P2 original frozen -25% re-entry
3. P1/P2 trailing-reference-peak -25% re-entry
4. P1/P2 FRACTAL buy-stop re-entry

## Required metrics

Per universe / variant:
- final equity
- delta vs baseline
- delta vs fixed -25% control
- delta vs trailing-peak -25% control
- max drawdown
- minimum equity vs initial
- cash-outs
- fractal search activations
- confirmed swing highs considered
- stop replacements
- stop fills
- unfinished cash cycles
- days in cash
- cash-cycle durations
- fill asset
- fill price
- stop price
- whether fill occurred at open gap or at stop
- U10 reference drawdown from latest peak at actual re-entry

Cross-topology:
- final > baseline rate
- DD better than baseline rate
- BOTH improve rate
- fractal > fixed -25% rate
- fractal > trailing-peak -25% rate
- unfinished-cycle rate
- median terminal delta
- q25/q75
- median re-entry reference drawdown
- distribution of actual re-entry depths

## Current-data validation boundary

Primary development/stress period:
2023-10-31 -> 2026-09-26

Use all 792 previously frozen U10 topologies:
- mandatory ATOM/TWT/PEPE
- choose 7 of the remaining fixed 12-asset pool

The same market dates mean these are topology-robustness tests, not independent future histories.

## Older-data rule

Do not use 2020-2022 to select or change any parameter in this experiment.

If the fractal rule is coherent on 2023-2026, 2020-2022 may be run only as already-consumed secondary historical evidence, not as untouched out-of-sample validation.

## Guardrails

- no live/paper promotion
- no parameter sweep
- no changing 15%
- no changing 2-left / 2-right pivot definition
- no buffer above pivot
- no alternative timeout
- no moving averages

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
