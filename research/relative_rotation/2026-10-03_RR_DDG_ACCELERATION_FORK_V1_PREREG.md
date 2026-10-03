# RR DDG + ACCELERATION FORK V1 — PREREGISTRATION

Date: 2026-10-03
Mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Purpose

Re-evaluate the last month of Relative Rotation using the currently accepted
Destination Dominance 1.5x router, then test a separate causal hypothesis:

> after the corrected RR+DDG route selects destination B, can a different
> TARGET token C become detectably stronger soon enough that switching from B
> to C would have been preferable?

The same fork hypothesis is also evaluated over the full compatible history,
not only the current AAVE example.

## Frozen current-core semantics

- Binance Spot D1 closed candles;
- pair ratio = right / left;
- rolling median = 180 daily observations;
- ARM = 15%;
- post-ARM extreme tracking;
- reversal confirmation = 3%;
- baseline route: strongest same-day CONFIRMED event from the held source;
- accepted DDG override:
  1. SOURCE -> B is ARMED or CONFIRMED;
  2. max_dislocation(SOURCE -> B) >= 1.50 x baseline max_dislocation;
  3. baseline-destination -> B is ARMED or CONFIRMED toward B;
  4. choose strongest qualifying B;
- TARGET universe:
  TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR;
- sunset/legacy sources may still exit into TARGET:
  ATOM, SOL, LINK.

No route threshold is retuned in this experiment.

## Fixed dates

Recent-month audit:
- start: 2026-09-03T00:00:00Z
- end: latest closed common D1 candle under fixed as-of

Fixed as-of:
- 2026-10-03T16:30:00Z

No unclosed candle may enter the test.

## Test A — corrected RR+DDG route ledger

For every source/date with a baseline CONFIRMED event:

1. reconstruct the exact same-day pair states causally;
2. apply the accepted DDG 1.5x rule;
3. persist:
   - baseline destination;
   - effective destination;
   - whether an override occurred;
   - baseline max dislocation;
   - competing max dislocation;
   - strength ratio;
   - destination relation state.

The recent-month output must show all corrected effective routes.

## Test B — post-route acceleration against corrected destination

For every corrected effective route SOURCE -> B:

1. hypothetical execution at next daily open;
2. during the first 1-3 completed daily bars, compare every other TARGET token C
   with B from that common entry open;
3. define:

   REL_IMPULSE(C,B) =
   (C_close / C_entry_open) / (B_close / B_entry_open) - 1

4. trigger at the first close where the strongest C has REL_IMPULSE >= +10%;
5. hypothetical switch B -> C occurs at next daily open.

Primary outcome:
- C versus staying in B for +14 bars after the hypothetical switch.

Diagnostics:
- +3, +7, +30 bars;
- trigger day 1/2/3;
- candidate identity;
- median/mean relative excess;
- win rate;
- strong continuation rate >= +20%.

This test answers the user's current practical question:
if corrected RR+DDG would have selected TRX instead of ALGO, would AAVE have
become detectably superior to TRX soon enough to justify a later switch?

## Test C — NEXT RR SIGNAL vs ACCELERATION FORK

This is the separately preregistered hypothesis proposed in chat.

For every corrected route SOURCE -> A:

1. enter A next open;
2. search the next 1-3 signal closes for the first corrected RR+DDG route A -> B;
3. at that exact signal close compute the strongest alternative acceleration
   candidate C relative to A since A entry;
4. if C != B and REL_IMPULSE(C,A) >= +10%, record a conflict;
5. compare:
   - RR branch: enter B next open;
   - acceleration branch: enter C next open.

Primary comparison horizon:
- 14 bars.

Diagnostics:
- 3 / 7 / 30 bars;
- conflict count;
- C beats B rate;
- median/mean relative excess;
- strong continuation >= +20%.

No future price is used in candidate selection.

## Specific LINK case

At the 2026-09-28 close reconstruct:

- baseline primary route from LINK;
- effective current DDG route;
- whether current logic would skip ALGO and go directly to TRX;
- after the corrected next-open entry, AAVE relative impulse versus the actual
  corrected destination after day 1 / 2 / 3;
- first causal +10% AAVE trigger if any;
- hypothetical switch date;
- realized relative excess to the latest available closed candle.

Also report the old ALGO branch only as a historical comparison, not as the
current-core decision.

## Evidence boundaries

This is diagnostic research.

Even if AAVE is caught in the current episode, promotion requires that the
full-history or at least recent-month pattern survives beyond a single example.

No production/live/Telegram/exchange/universe/sizing behavior may change.

## No-retune rule

Do not change:
- +10% acceleration threshold;
- 1-3 day observation window;
- DDG 1.5x threshold;
- 14-day primary outcome

after seeing V1 results.

Any altered rule requires V2.

TEST_LEVEL planned:
GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_STRESS_TEST
