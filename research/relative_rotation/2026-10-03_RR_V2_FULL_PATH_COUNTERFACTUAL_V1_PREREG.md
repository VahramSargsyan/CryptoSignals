# RR V2 FULL-PATH COUNTERFACTUAL V1 — PREREGISTRATION

Date: 2026-10-03
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Objective

Test whether the already-frozen V2 continuation classifier improves the
**entire path-dependent Relative Rotation strategy**, not just isolated
post-trigger trades.

Compare:

A. CORE
   - current RR + Destination Dominance 1.5x router only.

B. CORE_PLUS_V2
   - same CORE;
   - after a CORE transition, arm the frozen V2 acceleration detector;
   - if the frozen `PERSIST2_REEXPAND` classifier passes, actually switch the
     held asset at the next daily open;
   - from that point onward, all later RR+DDG decisions use the **new actual
     held asset**.

No V2 threshold or condition may change in this test.

## Frozen CORE semantics

- Binance Spot D1 closed candles;
- pair ratio = right / left;
- rolling median = 180 daily observations;
- ARM = 15%;
- reversal confirmation = 3%;
- baseline route = strongest same-day CONFIRMED destination;
- Destination Dominance override threshold = 1.50x;
- accepted DDG topology requirements unchanged;
- execution = next daily open;
- TARGET universe:
  TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR;
- legacy/sunset start assets may exit into TARGET:
  ATOM, SOL, LINK.

## Frozen V2 overlay

After a CORE transition into held asset B:

1. during the first 1-3 completed D1 bars, find the strongest alternative
   TARGET token C relative to B since the common entry open;
2. base trigger if C reaches >= +10% relative impulse;
3. do not switch yet;
4. on the next close C must still be acceleration rank #1;
5. on the following close C must again be rank #1;
6. on that second confirmation close C's relative impulse must be strictly
   greater than its original trigger impulse;
7. if all conditions pass, switch B -> C at the next daily open.

This is exactly the frozen `PERSIST2_REEXPAND` classifier from V2.

## Path interaction / cancellation rule

CORE remains active while V2 is waiting for confirmation.

If CORE emits a valid route from the currently held asset **before V2 has fully
confirmed**, the CORE move executes next open and the unfinished V2 watch is
cancelled.

This prevents the overlay from freezing the base strategy while waiting.

After every executed CORE transition, a fresh V2 watch begins.

After an executed V2 transition:
- the held asset becomes the V2 candidate;
- no recursive V2 watch is armed from that V2 transition itself;
- the next V2 watch begins only after the next executed CORE transition.

## Same-close priority

If V2 becomes fully confirmed on the same close that CORE also emits a route
from the current held asset:

PRIMARY semantics:
- V2 overlay takes priority for the next open.

Reason:
- the experiment is specifically testing whether the confirmed V2 overlay
  should override the core destination at the moment it becomes actionable.

ROBUSTNESS semantics:
- also run CORE_PRIORITY, where the CORE route wins on that same close.

No priority rule will be selected after seeing results.

## Starting capital and execution accounting

Each run starts with:
- capital = 100.0 normalized USDT-equivalent;
- fully invested in the chosen start asset at the window start open.

At every transition:
- value current holding at execution-day open;
- deduct fixed effective transition cost;
- convert remaining value into destination at destination execution-day open.

No intra-day fills or lookahead.

## Fixed transaction-cost surface

Primary:
- 0.10% effective cost per transition.

Robustness:
- 0.50%
- 1.00%
- 3.00%

These are fixed before seeing results and reflect the known uncertainty between
idealized backtest friction and worse real effective conversion losses.

No cost level is optimized.

## Primary evaluation windows

1. MATURE:
   - 2023-10-31 through latest closed common D1 candle.

2. LAST_2Y:
   - 2024-10-03 through latest closed common D1 candle.

3. LAST_1Y:
   - 2025-10-03 through latest closed common D1 candle.

Start-state coverage:
- all 13 supported source/start assets:
  TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR, ATOM, SOL, LINK.

Each start state is tested independently.

## Rolling robustness

At primary 0.10% transition cost and primary V2-priority semantics:

- rolling 365-calendar-day windows;
- evaluate every available daily start where a full 365-day window exists;
- all 10 TARGET start assets are tested independently.

Report per rolling window:
- median return across starts;
- median max drawdown;
- median transitions;
- CORE_PLUS_V2 minus CORE return delta.

Aggregate:
- better / equal / worse window counts;
- fraction of windows where CORE_PLUS_V2 median return is higher;
- median return delta;
- p25 / p75 return delta;
- worst / best return delta;
- drawdown change distribution.

## Regime diagnostic

Download causal Binance BTCUSDT D1 and compute SMA200 using only information
available at each rolling-window start.

Classify each 365-day window start:

- BTC_BULL_START: BTC close >= causal SMA200
- BTC_BEAR_START: BTC close < causal SMA200

For each start regime report:
- number of windows;
- better / equal / worse counts;
- median OVERLAY-minus-CORE return delta;
- median max-drawdown delta.

This regime split is descriptive only and cannot alter the overlay.

## Primary metrics

For every window / start asset / variant / cost:

- final capital;
- total return;
- maximum drawdown;
- transition count;
- total modeled transition cost paid;
- number of executed CORE moves;
- number of executed V2 moves;
- number of V2 base triggers;
- number of completed V2 confirmations;
- number of cancelled V2 watches due to intervening CORE moves;
- final held asset.

Aggregate by window:
- median return across starts;
- worst / best return;
- positive-start rate;
- median / worst max drawdown;
- median transitions;
- median extra transitions caused by V2;
- median modeled cost.

## Decision comparison

At PRIMARY cost (0.10%) and PRIMARY V2 priority, compare CORE_PLUS_V2 to CORE.

A useful full-path overlay should improve more than isolated trade metrics.

Report:
- number/share of start states with higher final capital;
- median final-capital ratio;
- median return delta;
- median max-drawdown delta;
- transition inflation;
- cost inflation.

No production approval follows automatically.

## Current AAVE trace

Persist a detailed event trace for a LINK start over the recent period around
2026-09-28, showing:

- corrected CORE LINK -> TRX execution;
- AAVE +10% trigger;
- persistence confirmations;
- V2 switch if reached in the path simulation;
- subsequent CORE route decisions from the new held asset;
- equity and cost after each executed move.

This verifies that the full simulator reproduces the intended current case.

## Fixed as-of

2026-10-03T16:30:00Z

No unclosed D1 candle may enter the experiment.

## Promotion gate

Possible research classifications:

- FULL_PATH_REJECTED
- FULL_PATH_MIXED
- FULL_PATH_PROMISING_NOT_PRODUCTION_READY

Frozen classification gate:

`FULL_PATH_PROMISING_NOT_PRODUCTION_READY` requires ALL:
- MATURE median return delta > 0 at 0.10% cost;
- at least 50% of MATURE start states finish with higher capital;
- median MATURE max-drawdown delta >= -10 percentage points;
- at least 40% of rolling 365d windows have higher median return;
- MATURE median return delta remains > 0 at 0.50% cost.

`FULL_PATH_REJECTED` if BOTH:
- MATURE median return delta <= 0 at 0.10% cost;
- fewer than 50% of MATURE start states improve.

Otherwise:
- `FULL_PATH_MIXED`.

These gates are fixed before runtime.

Even then:
- production/live remains unchanged;
- unseen forward observation remains mandatory.

## No-retune rule

Do not alter after results:
- V2 trigger threshold;
- confirmation structure;
- priority semantics;
- evaluation windows;
- cost surface;
- rolling-window definition.

Any changed overlay is a new version.

TEST_LEVEL planned:
GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_PATH_DEPENDENT_STRESS_TEST
