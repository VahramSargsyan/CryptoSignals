# RR THIRD-TOKEN MOMENTUM V1 — PREREGISTRATION

Date: 2026-10-03
Mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Question

When canonical Relative Rotation produces a CONFIRMED route `SOURCE -> B`, does a different TARGET token `C` that already has stronger short-horizon momentum at the signal close systematically outperform B afterwards?

This experiment was requested after the live `LINK -> ALGO` episode, where AAVE subsequently appeared much stronger than ALGO/TRX.

The experiment must distinguish:
1. what was knowable at the signal close;
2. what happened only afterwards;
3. whether an alternative token would already have satisfied the accepted Destination Dominance 1.5x topology rule.

## Frozen strategy semantics used for signal generation

- Binance Spot D1 closed candles;
- pair ratio = right / left;
- rolling median = 180 daily observations;
- ARM = 15%;
- post-ARM extreme tracking;
- reversal confirmation = 3%;
- TARGET universe:
  `TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR`;
- legacy/sunset sources may still be observed:
  `ATOM, SOL, LINK`;
- no exchange orders;
- no production configuration changes.

## Causal momentum definitions

At the close of signal day T only:

- M7(asset) = close(T) / close(T-7) - 1;
- M14(asset) = close(T) / close(T-14) - 1.

For every canonical CONFIRMED event `SOURCE -> B`:

- C7 = TARGET token, excluding SOURCE and B, with highest M7;
- C14 = TARGET token, excluding SOURCE and B, with highest M14.

No future price is used to select C7 or C14.

Primary candidate: **C14**.
C7 is a predeclared robustness comparison, not a tuning search.

## Forward comparison

Execution reference = next daily open after signal close.

For B, C7 and C14 compute tradable open-to-open returns at:
- +3 days;
- +7 days;
- +14 days;
- +30 days.

Relative excess of candidate C versus baseline B:

`(1 + return_C) / (1 + return_B) - 1`.

Primary descriptive horizon: **14 days**.
Other horizons are diagnostics.

No horizon is used to rewrite the historical route.

## Subgroups fixed before results

Report:
1. every CONFIRMED route;
2. events where C14 had higher M14 than baseline B;
3. events where C14 exceeded B's M14 by at least 10 percentage points;
4. same summaries for C7 as robustness.

## Destination Dominance audit

For the specific `LINK -> ALGO` event around 2026-09-28, reconstruct the D1 pair state at the signal close and record:

- SOURCE -> AAVE relation/state;
- ALGO -> AAVE relation/state;
- SOURCE -> AAVE max dislocation;
- baseline LINK -> ALGO max dislocation;
- whether AAVE would have met the accepted 1.5x Destination Dominance override rule.

This answers why AAVE was or was not a valid destination under the strategy *at that time*, independently of its later performance.

## Current-case table

For signal date 2026-09-28, persist for every TARGET token:

- M7;
- M14;
- momentum ranks;
- next-open forward returns available by the fixed as-of time;
- pair direction from LINK;
- ARM/CONFIRMED state if present.

Explicitly highlight ALGO, TRX and AAVE.

## Fixed as-of

`2026-10-03T15:55:00Z`

No candle that was not closed by this timestamp may enter the analysis.

## Interpretation gates

This is a diagnostic stress-test.

A finding that C14 often outperforms B does **not** authorize replacing Relative Rotation with momentum chasing.

A useful result would justify a separately preregistered shadow overlay or route rule. It must not silently change:
- paper-live routing;
- Telegram trading commands;
- position sizing;
- universe;
- exchange execution.

## No-retune rule

Do not add more lookbacks or choose a "best" horizon after seeing results in this version.

Any new momentum definition requires V2.

TEST_LEVEL planned:
`GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_STRESS_TEST`
