# RR D1-ANCHOR + H1 CONFIRMATION V1 — PREREGISTRATION

Date: 2026-10-04
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Objective

Test whether Relative Rotation can execute materially earlier by preserving the
canonical 180-day D1 anchor while moving confirmation timing to closed H1 bars.

This is NOT the existing informational H1 snapshot. V1 implements full causal
state machines for research only.

## Frozen benchmark

### D1_CORE
Canonical current mechanics:
- Binance Spot D1 closed candles;
- pair ratio = RIGHT / LEFT;
- rolling median = 180 closed D1 observations;
- ARM = +/-15%;
- post-ARM extreme tracking on D1 closes;
- reversal confirmation = 3%;
- current Destination Dominance 1.50x router;
- execution = next D1 open;
- TARGET / sunset semantics unchanged.

## Primary hybrid

### D1_ARM_H1_CONFIRM

Purpose: accelerate confirmation without changing when an anomaly is accepted
as sufficiently large.

Rules:
1. MEDIAN180 is calculated only from closed D1 candles.
2. A pair may enter ARM only at a closed D1 candle, using the canonical
   +/-15% rule.
3. Once armed, every subsequent closed H1 candle updates:
   - pair ratio;
   - post-ARM extreme;
   - max dislocation relative to the latest available closed-D1 median anchor;
   - reversal from extreme.
4. When reversal reaches 3% on a closed H1 candle, CONFIRMED occurs.
5. DDG 1.50x is evaluated causally at that same H1 close using current pair
   states and same-close events.
6. Execution is at the next H1 open, which is the same timestamp as the just
   closed H1 candle end.
7. After H1 confirmation the pair resets to NONE.
8. It may only ARM again at a later D1 close.
9. If a confirmation happens exactly at a D1 close, the pair may not re-arm on
   that same close.

This is the PRIMARY experiment.

## Secondary aggressive hybrid

### D1_ANCHOR_FULL_H1

Same D1 MEDIAN180 anchor, but:
- ARM is allowed on any closed H1 candle;
- extreme and reversal are also H1;
- confirmation and execution remain H1;
- D1 median anchor updates only when a new D1 candle closes.

This tests the cost of making the whole state machine intraday.

## Causal anchor mapping

For an H1 close at time T:
- use only D1 candles whose close time is <= T;
- D1 candle timestamp D represents the Binance daily candle [D, D+24h);
- its close becomes available at D+24h;
- execution never uses an unclosed daily candle.

Validate that each D1 close matches the corresponding final H1 close within a
small numerical tolerance before trusting the run.

## Data

Source:
- Binance Spot public klines.

Timeframes:
- D1;
- H1.

Assets:
- TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR;
- sunset/exit-only starts: ATOM, SOL, LINK.

Fixed as-of:
- 2026-10-04T00:30:00+04:00
- equivalent 2026-10-03T20:30:00Z.

Only fully closed D1/H1 candles may enter the test.

## Full-path simulation

For each engine independently:
- actual held state changes after every executed route;
- later signals are evaluated from the actual held asset;
- normalized starting capital = 100;
- transition costs deducted on every executed move;
- no look-ahead.

Windows:
- MATURE: 2023-10-31 through latest common closed H1;
- LAST_2Y: 2024-10-04 through latest;
- LAST_1Y: 2025-10-04 through latest.

Start assets:
- all 13 supported assets.

## Cost surface

Primary:
- 0.10% effective transition cost.

Robustness:
- 0.50%;
- 1.00%;
- 3.00%.

No cost level will be selected after results.

## Primary metrics

Per engine/window/cost:
- median final capital across starts;
- median return;
- worst / best return;
- median / worst max drawdown;
- median transition count;
- median holding duration;
- positive-start rate;
- final-asset concentration.

Head-to-head versus D1_CORE:
- improved-start share;
- median final-capital ratio;
- median return delta;
- median DD delta;
- median extra transitions.

## Timing metrics

### Pair-confirmation lead

For each D1 pair CONFIRMED event:
- find the same pair / from / to hybrid confirmation occurring before the D1
  execution time and no more than 24h earlier;
- report matched count;
- earlier count;
- median / p25 / p75 lead hours.

### Route-level lead

For each D1 effective RR+DDG route:
- find same source -> effective destination hybrid route in the prior 24h;
- report match rate;
- median lead hours.

### Execution-price impact

For matched same-route cases compare target units per source unit:

UNITS_PER_SOURCE = source_open / target_open

at:
- hybrid execution H1 open;
- canonical D1 execution open.

Report median relative improvement:
(H1_units_per_source / D1_units_per_source) - 1.

Positive means H1 execution bought more destination units.

## Intraday false-confirm audit

For every hybrid pair CONFIRMED:
- PRESERVED_NEXT_D1 = same pair/direction is canonical D1 CONFIRMED on the
  first daily close after the H1 confirmation;
- PRESERVED_WITHIN_3_D1 = same pair/direction becomes canonical D1 CONFIRMED
  within the next three daily closes;
- otherwise classify as H1_ONLY_WITHIN_3D.

Report rates for PRIMARY and aggressive secondary separately.

This does not declare D1 truth; it measures how often an intraday reversal would
have disappeared under canonical daily confirmation.

## Current-case audit

Persist detailed traces for:
- LINK -> TRX around 2026-09-28/29;
- ALGO -> TRX around 2026-09-29/30;
- AAVE/TRX pair state from 2026-09-28 through latest closed H1.

Report:
- exact H1 arm/confirm times;
- D1 signal/execution times;
- lead or lag in hours;
- whether H1 would have changed the destination;
- whether AAVE/TRX ever produced a genuine H1 RR confirmation toward AAVE.

## Frozen decision gate

### H1_CONFIRM_PROMISING
requires PRIMARY D1_ARM_H1_CONFIRM at 0.10% cost:
- MATURE median final capital > D1_CORE;
- >=50% MATURE start states improve;
- median max-DD delta >= -10 percentage points;
- median transition count <= 1.5x D1_CORE;
- matched same-route median lead > 0 hours;
- H1_ONLY_WITHIN_3D pair-confirm rate < 50%.

### H1_CONFIRM_REJECTED
if BOTH:
- MATURE median final capital <= D1_CORE;
- fewer than 50% of MATURE start states improve.

Otherwise:
- H1_CONFIRM_MIXED.

No production promotion follows automatically.

## No-retune rule

After V1 results do not change:
- ARM 15%;
- reversal 3%;
- MEDIAN180;
- 24h matching window;
- 3-D1 preservation window;
- cost surface;
- primary/secondary semantics.

Any changed confirmation rule requires V2.

## TEST_LEVEL planned

GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_H1_PATH_DEPENDENT_STRESS_TEST
