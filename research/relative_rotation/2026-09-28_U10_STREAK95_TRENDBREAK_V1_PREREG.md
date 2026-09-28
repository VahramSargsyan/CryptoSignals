# U10 STREAK95 Trend-Break Exit v1 — preregistration

Date: 2026-09-28
Mode: STRESS_TEST_ONLY
Scope: research-only exit-timing test; no live/paper changes

## Objective

Test whether the weak multimonth STREAK95 implementation can be improved by waiting for a causal trend-break structure before selling 30%.

This study does NOT change the STREAK95 definition and does NOT change re-entry mechanics.

## Frozen STREAK95 trigger

A qualifying overheat streak requires:

- at least 2 consecutive positive selected calendar months;
- cumulative selected-equity gain >=95%;
- any zero/negative selected month resets the streak;
- a single +95% month does not qualify.

The selected-month construction is unchanged from prior research.

## Trend-break filter

A STREAK95 event only enters WATCH state.

No 30% cash-out is allowed while the consecutive-positive monthly streak remains intact.

The monthly streak is considered broken only after the first later completed selected calendar month whose selected change is <=0.

That month is known only after its final daily close.

After the monthly streak break, require a confirmed LOWER HIGH on the currently held frozen-U10 asset.

## 5-bar lower-high definition

Use actual daily HIGH prices of the asset currently held by frozen U10.

A confirmed 5-bar swing high at center bar t requires:

- high[t] > high[t-1]
- high[t] > high[t-2]
- high[t] > high[t+1]
- high[t] > high[t+2]

The pivot becomes known only after t+2 closes.

A LOWER HIGH is confirmed when:

- a prior confirmed 5-bar swing high exists on the same uninterrupted current-asset segment after the STREAK95 event began; and
- a later confirmed 5-bar swing high is strictly lower than that prior confirmed swing high; and
- the later pivot is confirmed after the completed monthly streak-break date.

If frozen U10 rotates to another asset:
- discard the old asset's pivot anchor;
- require a new pair of confirmed 5-bar swing highs on the new asset segment.

No pivot from a previous asset may be reused.

## Sale execution

The lower-high filter is an additional permission condition.

P1-TRENDBREAK:
- after STREAK95,
- wait for monthly streak break,
- wait for confirmed lower high,
- then require U10 reference close to be at least 5% below the latest running reference peak since STREAK95 trigger,
- sell 30% next open.

P2-TRENDBREAK:
- identical, except require 10% reference pullback.

If the 5% / 10% reference pullback has already been satisfied when the lower high is confirmed, sell next open.

Continue updating the reference running peak until cash-out.

Apply 0.1% modeled cash-out cost.

## Re-entry mechanics

Keep the prior 5-bar fractal buy-stop re-entry unchanged:

- after cash-out, update the latest U10 reference running peak;
- activate re-entry search only after U10 reference close is >=15% below that latest peak;
- search actual HIGH prices of the currently held U10 asset for confirmed 5-bar swing highs;
- buy-stop becomes active only from the session after pivot confirmation;
- lower confirmed pivot highs ratchet the stop downward;
- higher pivots do not raise an existing lower stop;
- U10 asset rotation cancels/reset the old stop;
- gap above stop fills at open;
- otherwise session high touching stop fills at stop;
- reinvest all parked cash;
- apply 0.1% modeled re-entry cost.

No timeout.
No fixed -25% re-entry.
No 3-bar pivot.

## Controls

Compare:

1. baseline U10;
2. original P1/P2-STREAK95 implementation:
   - sell after 5% / 10% reference pullback immediately after STREAK95;
3. P1/P2-STREAK95-TRENDBREAK:
   - monthly streak break + 5-bar lower high + same 5% / 10% pullback;
4. report the prior single-month +100% strategy separately only as context, not as a control to optimize against.

## Required event / timing metrics

For every STREAK95 event:

- STREAK95 trigger date;
- qualifying streak start/end;
- cumulative gain;
- first non-positive month and completion date;
- delay from STREAK95 trigger to monthly streak break;
- asset at lower-high confirmation;
- prior pivot high;
- lower pivot high;
- lower-high confirmation date;
- reference drawdown from running peak at confirmation;
- sale signal/execution date;
- reference drawdown at sale;
- delay from STREAK95 trigger to sale.

## Required portfolio metrics

For canonical U10 and every alternative topology:

- baseline final equity;
- original STREAK95 P1/P2 final equity;
- trend-break P1/P2 final equity;
- delta vs baseline;
- delta vs original STREAK95 implementation;
- max drawdown;
- max-DD change vs baseline;
- cash-outs;
- re-entries;
- unfinished watch states;
- unfinished cash cycles;
- days in cash;
- days spent armed/watch before sale.

## Topology robustness

Use the same exhaustive 792 frozen U10 topologies:

- mandatory ATOM/TWT/PEPE;
- choose 7 of the fixed remaining 12-asset pool.

Across the 791 non-canonical alternatives report:

- trend-break final > baseline rate;
- trend-break final > original STREAK95 rate;
- DD improved vs baseline rate;
- BOTH final and DD improved rate;
- unfinished-watch rate;
- unfinished-cash rate;
- median/q25/q75 terminal delta vs baseline;
- median terminal delta vs original STREAK95;
- median delay to sale.

## Guardrails

- no threshold sweep;
- 95% remains unchanged;
- minimum streak remains 2 positive months;
- lower-high uses 5-bar only;
- no 3-bar comparison in this study;
- no alternate monthly-break definition;
- no moving averages;
- no 2020-2022 tuning;
- no live/paper promotion.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
