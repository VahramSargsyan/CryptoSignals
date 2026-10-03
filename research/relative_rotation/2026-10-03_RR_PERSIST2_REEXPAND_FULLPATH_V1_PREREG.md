# RR PERSIST2_REEXPAND FULL-PATH V1 — PREREGISTRATION

Date: 2026-10-03
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Objective

Take the already frozen V2 classifier `PERSIST2_REEXPAND` exactly as tested and
measure whether adding it to the current RR + Destination Dominance 1.5x core
improves the **whole path-dependent strategy**, not isolated trades.

No classifier threshold or confirmation condition may change in this test.

## Frozen core

Current core routing:
- Binance Spot D1 closed candles;
- pair ratio = right / left;
- rolling median = 180 daily observations;
- ARM = 15%;
- post-ARM extreme tracking;
- reversal confirmation = 3%;
- primary route = strongest same-day CONFIRMED event from held source;
- Destination Dominance override threshold = 1.50x;
- TARGET universe:
  TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR;
- legacy/sunset assets may only exit into TARGET:
  ATOM, SOL, LINK.

## Frozen acceleration overlay

The overlay is armed only after a **core RR+DDG transition** into a new held asset B.

Base trigger:
- during the first 1-3 completed D1 bars after the core entry;
- strongest alternative TARGET C relative to B reaches >= +10%.

Primary confirmation:
1. C remains #1 at the next completed D1 close;
2. C remains #1 again at the following completed D1 close;
3. at that second confirmation close, C's relative impulse versus B is strictly
   greater than at the original +10% trigger.

If all pass:
- hypothetical acceleration switch B -> C occurs at the next daily open;
- pay one transition cost;
- after an acceleration switch the overlay is disarmed;
- it becomes eligible again only after the next ordinary RR+DDG transition.

No re-trigger is allowed for the same core holding after a failed first +10% candidate.

## Event precedence

This is frozen before results:

- an ordinary RR+DDG signal may interrupt an unconfirmed acceleration setup;
- if an RR+DDG signal occurs before the second confirmation close, the core route
  executes next open and the pending acceleration setup is cancelled;
- if the second acceleration confirmation and an RR+DDG signal occur on the same
  close:
  - if both select the same destination, execute one transition and label it
    CORE_ACCEL_AGREE;
  - if they select different destinations, the fully confirmed acceleration
    destination takes precedence for this research overlay.
- every transition is executed at next daily open.

This makes the comparison genuinely path-dependent and causal.

## Start / end

Primary mature full-path evaluation:
- start: 2023-10-31T00:00:00Z
- end: latest common closed D1 candle under fixed as-of.

Fixed as-of:
- 2026-10-03T16:30:00Z

Primary start assets:
- all 10 frozen TARGET assets.

Secondary robustness starts:
- ATOM, SOL, LINK.

Artificial initial positions do NOT arm the acceleration overlay.
The overlay first becomes active only after the first real RR+DDG transition.

## Costs

Primary cost:
- 0.10% capital haircut per transition, matching the canonical historical RR model.

Predeclared cost sensitivity:
- 0.50%
- 1.00%
- 2.00%
- 3.00%

These are not optimized thresholds. They test whether extra acceleration switches
remain economically meaningful under materially higher effective execution friction.

## Primary metrics

For each start asset and each cost level compare:

Baseline:
- RR+DDG 1.5x

Overlay:
- RR+DDG 1.5x + frozen PERSIST2_REEXPAND

Report:
- final capital from normalized 100;
- total return;
- overlay minus baseline return;
- transition count;
- acceleration switch count;
- RR transition count;
- max drawdown from daily close equity;
- time in each final/held asset if available.

Primary aggregate across 10 TARGET starts:
- median final capital;
- median total return;
- median max drawdown;
- overlay better / equal / worse start count.

## Rolling 365-day robustness

Use monthly-spaced window starts (30-day spacing) where a full 365-day outcome is
available.

For each window:
- start each of the 10 TARGET assets;
- run baseline and overlay from the same start open to the same end close;
- overlay remains disarmed until first core transition.

Report:
- number of paired start-window observations;
- overlay better / equal / worse;
- median return delta;
- median max-drawdown delta.

## Regime split

Download Binance BTCUSDT D1 under the same as-of.

At each rolling-window start:
- BTC close > causal SMA200 = BTC_BULL_START;
- otherwise = BTC_BEAR_START.

Report rolling-window overlay-vs-baseline metrics separately for BULL and BEAR
starts.

This is a descriptive robustness split, not a trading gate.

## Current live-shape reconstruction

Also trace a path beginning from LINK on 2026-09-28 close / 2026-09-29 open under:
- baseline corrected RR+DDG;
- overlay.

Verify whether:
- baseline holds TRX;
- overlay detects AAVE;
- frozen PERSIST2_REEXPAND confirms at Oct-01 close;
- overlay switches TRX -> AAVE at Oct-02 open.

This micro-trace is a semantic check, not the main performance sample.

## Acceptance interpretation

Possible classifications:

- FULL_PATH_WORSE
- FULL_PATH_MIXED
- FULL_PATH_IMPROVEMENT_RESEARCH_ONLY

A positive isolated-trade V2 result is not enough.

For `FULL_PATH_IMPROVEMENT_RESEARCH_ONLY`, the primary 0.10% mature comparison
must satisfy all:
- overlay median final capital > baseline median final capital;
- overlay better on more than half of 10 TARGET start paths;
- overlay median max drawdown is not worse by more than 5 percentage points;
- rolling paired median return delta > 0.

Even if this gate passes, production is NOT authorized.

Promotion still requires unseen forward evidence and realistic manual execution
cost observation.

## No-retune rule

Do not alter:
- +10% trigger;
- first 1-3 bar trigger window;
- two confirmation closes;
- #1 persistence;
- re-expansion condition;
- 1.50x DDG;
- cost sensitivity grid;
- rolling spacing;
- 365-day window length

after seeing results.

Any change requires a new version.

TEST_LEVEL planned:
GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_PATH_DEPENDENT_STRESS_TEST
