# U10 Monthly Surge -> Pullback Overlay v1 — preregistration

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Scope: research-only hypothesis test; no live/paper strategy changes

## Motivation

A diagnostic of the canonical mature U10 path showed three monthly selected-extreme increases above +100%:

- 2024-02: +265.56%
- 2025-11: +142.28%
- 2026-01: +109.38%

Two were followed by >=25% selected-extreme declines in the immediately following month.
The 2024-02 surge continued higher through March and the large decline arrived in April.

Therefore the hypothesis is NOT preregistered as "the next month must fall".

The research hypothesis is:

> After an unusually large monthly U10 equity surge, a substantial pullback may occur within roughly the next one to two months often enough to justify testing a partial profit-lock / lower re-entry overlay.

## Canonical U10

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

Frozen strategy mechanics:
- Binance Spot 1D closed candles
- rolling median 180 days
- ARM 15%
- reversal confirmation 3%
- strongest-confirmed max-dislocation router
- next-open execution
- 0.1% modeled U10 transition cost
- fresh portfolio starts in ATOM

## Monthly selected-extreme state

This test reuses the previously defined monthly diagnostic semantics.

Starting anchor:
10,000 USDT.

For each completed calendar month:
- find the monthly maximum daily-close reference equity;
- find the monthly minimum daily-close reference equity;
- compare each with the previously selected monthly extreme;
- select whichever has the larger absolute percentage displacement from the previous selected extreme.

That completed-month selected extreme becomes the known anchor for the next calendar month.

No future month information is used to trade the overlay.

## Causal surge arming rule

For calendar month M, the previous completed month's selected extreme E(M-1) is already known.

During month M, after each daily close, define current reference-equity gain:

current U10 reference close / E(M-1) - 1.

A surge is armed the first time this gain reaches the configured surge threshold.

Primary surge threshold:
+100%.

Sensitivity thresholds:
+75%, +100%, +125%.

After arming:
- track the highest frozen-U10 reference daily-close equity reached since arming;
- do not sell immediately.

## Partial profit-lock

Fixed protected fraction:
30% of the actually invested U10 sleeve.

Cash-out signal:
daily-close reference equity pulls back from the post-arm running peak by at least the configured pullback threshold.

Cash-out pullback sensitivity:
- 5%
- 10%
- 15%

Execution:
- next daily open;
- first execute any already-pending frozen U10 rotation;
- then sell 30% of the active invested sleeve into USDT;
- apply 0.1% modeled cash-out cost.

At execution:
- lock the running reference-equity peak that caused the cash-out.

## Re-entry

After cash-out:
wait until frozen-U10 reference daily-close equity is at least the configured drawdown below the locked peak.

Re-entry thresholds:
- 20%
- 25%
- 30%
- 35%

Execution:
- next daily open;
- after any already-pending U10 rotation;
- reinvest the full parked cash sleeve into the asset currently held by frozen U10;
- apply 0.1% modeled re-entry cost.

One surge can create at most one cash-out/re-entry cycle.

After re-entry:
- the same surge event cannot re-arm;
- a new cycle requires a later calendar-month surge event.

Cash modeled as non-yielding USDT.

## Primary user-proposed variants

P1:
- surge +100%
- sell after 5% pullback from post-surge peak
- cash fraction 30%
- re-enter at -25% from locked peak

P2:
- surge +100%
- sell after 10% pullback
- cash fraction 30%
- re-enter at -25% from locked peak

These two variants are primary and must be reported even if another sensitivity combination has a better historical result.

## Sensitivity grid

Exploratory robustness grid:

3 surge thresholds
x 3 pullback thresholds
x 4 re-entry thresholds
= 36 fixed parameter combinations.

Cash fraction remains 30% throughout this study.

Do not add more thresholds after seeing results.

## Direct event-study question

Before judging overlay performance, measure whether the underlying effect exists.

For every +100% monthly surge event:
- record the post-surge peak;
- measure worst reference-equity decline from that peak within 31 calendar days;
- measure worst decline within 62 calendar days;
- flag whether decline reached at least -25%.

Report:
- event count;
- next-31-day hit rate for -25%;
- next-62-day hit rate for -25%;
- median / quartiles of worst 31d and 62d pullback.

## Cross-topology robustness / anti-snooping set

Do not validate only on canonical U10.

Use the already frozen 15-asset candidate pool:

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, AVAX, FIL, ETH, ALGO, ADA, XRP, HBAR

Mandatory in every alternative U10:
- ATOM
- TWT
- PEPE

Choose 7 of the remaining 12.

This creates exactly:

C(12,7) = 792 U10 universes.

Canonical U10 is one member of this exhaustive set.

Every universe uses:
- the same strategy mechanics;
- the same common data panel;
- ATOM start;
- the same mature start;
- the same overlay parameter grid.

The 791 non-canonical universes are the primary topology-robustness set.

This is not a clean future OOS test because the market dates are shared.
It is a topology robustness test designed to reduce dependence on the specifically chosen FIL/HBAR canonical U10.

## Metrics per universe / variant

Baseline:
- final equity;
- total return;
- max drawdown;
- minimum equity vs initial;
- transition count.

Overlay:
- final equity;
- delta versus baseline in USDT and percent;
- max drawdown;
- max-DD change in percentage points;
- minimum equity vs initial;
- number of surge arms;
- number of cash-outs;
- number of completed re-entries;
- days in cash;
- terminal cash if any.

Cross-universe summary:
- fraction of 791 alternatives where overlay terminal equity > baseline;
- fraction where max DD improves;
- fraction where BOTH terminal equity and max DD improve;
- median terminal delta;
- q25/q75 terminal delta;
- median max-DD change;
- event-count distribution.

## Selection guardrail

Do not declare a parameter combination "optimal".

The sensitivity grid may identify historically stronger regions, but:
- primary P1/P2 remain the preregistered user variants;
- same-date / same-market topology tests do not prove future generalization;
- any future promotion would require forward/paper evidence or a genuinely held-out time period.

## Live guardrail

No live or paper-live behavior changes.
No automatic promotion.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
