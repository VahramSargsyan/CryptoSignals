# RR POST-SIGNAL ACCELERATION V1 — PREREGISTRATION

Date: 2026-10-03
Mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Research question

After canonical Relative Rotation produces a primary CONFIRMED route `SOURCE -> B`
and B is entered at the next daily open, can a different TARGET token `C`
that suddenly accelerates relative to B during the first three completed daily
bars identify a small, repeatable continuation regime?

This is intentionally different from the rejected naive M7/M14 strongest-third-token
override. The candidate is not selected at the original RR signal close. It is
selected only if new post-signal price information creates a large relative impulse.

## Frozen RR signal semantics

- Binance Spot D1 closed candles;
- pair ratio = right / left;
- rolling median = 180 daily observations;
- ARM = 15%;
- post-ARM extreme tracking;
- reversal confirmation = 3%;
- primary route = strongest CONFIRMED destination by max dislocation;
- TARGET universe:
  `TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR`;
- legacy/sunset sources may be observed:
  `ATOM, SOL, LINK`.

This experiment does not change the canonical signal.

## Causal timing

For a primary CONFIRMED signal at close T:

1. baseline destination B is entered at open T+1;
2. after each of the first 1, 2, and 3 completed D1 bars, inspect all TARGET
   tokens C excluding SOURCE and B;
3. compute C's cumulative relative return versus B from the common T+1 open
   to the observation close;
4. if the primary trigger is met, detection occurs at that close;
5. any hypothetical switch B -> C occurs only at the next daily open.

No future price may be used to select C or to timestamp the trigger.

## Relative impulse

For observation day k in {1,2,3}:

`REL_IMPULSE_k(C,B) =
   (C_close_k / C_entry_open) / (B_close_k / B_entry_open) - 1`

At each observation close, the candidate is the TARGET token with the largest
positive relative impulse.

## Primary trigger

`FIRST_3D_REL_IMPULSE_10PCT`

Trigger at the first observation close within days 1-3 where the strongest
candidate has:

`REL_IMPULSE >= +10%`

If no candidate reaches +10% by the third close, there is no trigger.

## Fixed robustness thresholds

Run the same logic with:
- +5%;
- +15%.

These are robustness surfaces only. V1 must not choose a "best" threshold after
seeing results.

## Outcome measurement

Hypothetical switch execution = open immediately after detection close.

Compare candidate C with staying in baseline B from that same switch open.

For each trigger compute relative excess:

`REL_EXCESS_h =
   (1 + return_C_h) / (1 + return_B_h) - 1`

at fixed horizons:
- +3 D1 bars;
- +7 D1 bars;
- +14 D1 bars;
- +30 D1 bars.

Primary evaluation horizon: **14 bars**.

## Primary metrics

For the +10% trigger report:

- number of eligible RR opportunities;
- trigger count and trigger rate;
- detection-day distribution (day 1 / 2 / 3);
- candidate beats baseline rate at 3/7/14/30 bars;
- median and mean relative excess;
- 25th / 75th percentile relative excess;
- false-positive rate at 14 bars: REL_EXCESS <= 0;
- strong-continuation rate at 14 bars: REL_EXCESS >= +20%;
- median remaining candidate absolute return after hypothetical switch.

The same summary is produced for +5% and +15% only as robustness.

## Current AAVE case audit

For the canonical `LINK -> ALGO` signal at 2026-09-28 close:

- compute AAVE relative impulse versus ALGO after day 1, day 2, day 3;
- record whether/when AAVE crosses +5%, +10%, +15%;
- record the strongest third-token candidate at each observation close;
- if AAVE triggers, compute the next-open hypothetical switch date;
- measure remaining AAVE return and AAVE-vs-ALGO relative excess to the latest
  closed D1 candle available under the fixed as-of;
- separately show whether the routing rule would actually have chosen AAVE or
  another stronger acceleration candidate at that detection close.

This distinguishes:
- "AAVE later became strong enough to notice"
from
- "A causal rule would actually have selected AAVE."

## Fixed as-of

`2026-10-03T16:30:00Z`

No candle not closed by this timestamp may enter V1.

## Decision boundary

This is a diagnostic continuation test only.

Even a positive result does not authorize:
- changing RR routing;
- automatic B -> C switching;
- position sizing changes;
- Telegram trading commands;
- universe changes.

A production candidate requires a separate full-path counterfactual with
transaction costs and path dependence after this diagnostic stage.

## No-retune rule

Do not add more observation windows, thresholds, or momentum lookbacks after
seeing V1 results.

Any new feature or threshold family requires V2.

TEST_LEVEL planned:
`GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_STRESS_TEST`
