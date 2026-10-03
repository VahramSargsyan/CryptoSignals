# RR EXTREME CONTINUATION FULL-PATH V3 — PREREGISTRATION

Date: 2026-10-03
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Objective

Test whether the frozen V2 classifier `PERSIST2_REEXPAND` improves the entire
path-dependent RR+DDG strategy after accounting for:

- changed holdings;
- changed future RR source state;
- extra transitions;
- transition costs;
- drawdown;
- rolling-window stability.

The V2 classifier itself is frozen and must not be retuned in V3.

## Baseline strategy

`RR_DDG_1P5X_BASELINE`

- Binance Spot D1 closed candles;
- U10 TARGET:
  TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR;
- RR median lookback: 180 D1 bars;
- ARM: 15%;
- reversal confirmation: 3%;
- signal close T -> execution at next D1 open;
- accepted Destination Dominance minimum: 1.50x;
- strongest qualifying DDG destination;
- no leverage;
- one fully invested asset at a time.

## Overlay strategy

`RR_DDG_1P5X_PLUS_PERSIST2_REEXPAND_V2`

After every ordinary RR+DDG execution into held asset B:

1. inspect first 1-3 completed D1 bars after entry;
2. candidate C is the strongest TARGET relative to B from the common entry open;
3. base trigger occurs when C reaches >= +10% relative impulse;
4. do NOT switch yet;
5. C must remain acceleration rank #1 on the next close;
6. C must remain rank #1 on the following close;
7. on that second confirmation close, C's relative impulse must be strictly
   greater than at the original +10% trigger;
8. if all pass, switch B -> C at the next D1 open.

After an overlay switch:
- no new acceleration monitor starts from that overlay entry;
- the next overlay monitor begins only after a subsequent ordinary RR+DDG
  transition.

This prevents acceleration-on-acceleration cascades.

## Event precedence

Canonical RR+DDG has priority over an unconfirmed overlay.

If an ordinary RR+DDG signal from the currently held asset appears before the
overlay classifier completes:
- execute the RR+DDG route next open;
- cancel/reset the pending overlay observation;
- start a fresh overlay observation from the new ordinary RR+DDG entry.

If RR+DDG and a completed PERSIST2_REEXPAND confirmation occur on the same
signal close:
- RR+DDG wins;
- the overlay switch is not executed.

This conservative precedence preserves the core strategy.

## Path semantics

For each evaluation start:
- start fully invested in one U10 asset;
- quantity = capital / start-day open;
- mark equity daily at D1 close;
- every transition converts the full position at next-day open;
- transition cost is deducted before buying the new asset;
- future signals are generated from the asset actually held after each action.

Thus an overlay switch can materially alter every later route.

## Primary cost assumption

Per-transition all-in cost:
- **0.10%**

This matches the historical RR research baseline.

## Fixed execution-cost stress

Run the same frozen paths at:
- 0.10%
- 0.50%
- 1.00%
- 3.00%

No cost level is selected after seeing results.

## Evaluation windows

Using latest common closed D1 under fixed as-of:

- LAST_1Y: final 365 calendar days;
- LAST_2Y: final 730 calendar days;
- MATURE: 2023-10-31 through latest common closed D1.

For each window run all 10 U10 starting assets.

## Primary portfolio metrics

For BASELINE and OVERLAY:

- median total return across 10 starts;
- worst-start return;
- best-start return;
- positive-start rate;
- median max drawdown;
- worst max drawdown;
- median transition count;
- overlay transition count;
- final-asset distribution.

Primary decision comparison:
- MATURE at 0.10% cost.

## Rolling stability

At 0.10% transition cost:

- daily rolling 365-day windows;
- daily rolling 730-day windows when enough history exists.

For each window compute median return across all 10 start assets.

Report overlay minus baseline:
- better window count;
- equal window count;
- worse window count;
- median delta;
- worst delta;
- best delta.

## Regime diagnostic

Download BTCUSDT D1 through the same public Binance adapter.

Define causal regime at an overlay confirmation close:

- BTC_BULL: BTC close >= causal SMA200;
- BTC_BEAR: BTC close < causal SMA200.

For executed overlay switches report:
- count by regime;
- local candidate-vs-held relative return over next 14 D1 opens when available;
- win rate;
- median relative excess;
- >=20% continuation rate.

This is diagnostic only and does not alter V3 routing.

## Specific current AAVE path

Also persist a trace for the start/path containing corrected:

`LINK -> TRX -> [V2 overlay candidate]`

around 2026-09-28.

Because LINK is sunset/exit-only and not a U10 start, this is a separate
diagnostic trace, not part of the 10-start aggregate.

Report:
- corrected RR+DDG transition;
- V2 confirmation dates;
- actual V3 precedence outcome;
- hypothetical overlay execution date if not preempted;
- subsequent held path through latest closed D1.

## Fixed as-of

`2026-10-03T16:30:00Z`

No unclosed candle may enter the experiment.

## Decision gate

Primary overlay classification:

- `FULLPATH_REJECTED` if MATURE median return is worse and no meaningful
  drawdown improvement compensates;
- `FULLPATH_TRADEOFF_ONLY` if return/drawdown move in opposite directions;
- `FULLPATH_PROMISING_RESEARCH_SIGNAL` if at 0.10%:
  - MATURE median return improves;
  - MATURE median max drawdown is not worse by more than 2 percentage points;
  - rolling 365d better windows exceed worse windows;
  - result remains directionally positive at 0.50% cost.

No production promotion is authorized by V3 alone.

## No-retune rule

Do not change after results:
- +10% base trigger;
- first 1-3 bar trigger window;
- two confirmation closes;
- rank #1 persistence;
- re-expansion condition;
- RR priority;
- cost grid;
- evaluation windows.

Any altered semantics require V4.

TEST_LEVEL planned:
`GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_FULL_PATH_STRESS_TEST`
