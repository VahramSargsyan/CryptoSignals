# U10-Style Monthly Surge Pullback — 2020-2022 Temporal Holdout v1

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Scope: validation-only older-history test; no parameter tuning and no live/paper changes

## Objective

Validate the already-frozen monthly-surge capital-management rule on an older market period that was not used to choose the current return-cycle logic.

Evaluation period:

2020-01-01 -> 2022-12-31

This study is validation-only.

Do not modify:
- +100% surge threshold;
- P1 5% cash-out pullback;
- P2 10% cash-out pullback;
- 30% protected fraction;
- 25% trailing re-entry depth;
- moving re-entry peak logic;
- 0.1% modeled U10 transition / cash-out / re-entry costs.

## OLD10-2020 fixed universe

ATOM, BTC, ETH, BNB, XRP, TRX, ADA, LINK, XLM, LTC

Selection rule:

- assets are long-lived liquid crypto assets that existed before 2020 and still exist today;
- ATOM is retained as the starting asset to preserve the current U10 capital anchor;
- assets are chosen for historical availability / survival, not because of 2020-2022 strategy performance.

Hard eligibility gate:

Every selected asset must have at least 180 common daily Binance Spot candles before 2020-01-01 after panel intersection.

If any selected asset fails this gate, the workflow must fail.
Do not replace an ineligible asset after viewing performance.

## Frozen U10-style rotation mechanics

Use the same graph mechanics as current U10 research:

- Binance Spot 1D closed candles
- pair ratio = right / left
- rolling median = 180 days
- ARM threshold = 15%
- reversal confirmation = 3%
- strongest-confirmed max-dislocation conflict router
- next-open execution
- transition cost = 0.1%
- start with 10,000 USDT in ATOM

The universe differs because TWT/PEPE and several current U10 assets did not have suitable pre-2020 history.

This is therefore a **U10-style temporal holdout**, not the literal current U10 universe.

## Frozen monthly surge overlays

### P1-TRAILING

- monthly surge arm: +100% relative to prior completed selected-month extreme
- after arming, track post-arm running peak
- cash-out signal: >=5% pullback from that running peak
- sell 30% next open
- apply 0.1% modeled cash-out cost
- while cash is parked, keep updating the reference running peak on every new daily-close high
- re-enter full cash after a >=25% drawdown from the latest running peak
- execute next open
- apply 0.1% modeled re-entry cost

### P2-TRAILING

Same, except cash-out signal is >=10%.

## Direct effect study

Independently of overlay P/L, identify every completed selected-month surge >= +100%.

For each event measure:
- worst running-peak drawdown within 31 calendar days;
- worst running-peak drawdown within 62 calendar days;
- whether -25% was reached in each horizon.

Report:
- event count;
- hit rate;
- individual event dates;
- individual 31d / 62d worst drawdowns.

## Required portfolio metrics

Baseline OLD10:
- final equity
- total return
- minimum equity vs initial
- max drawdown
- transitions
- route

P1/P2 trailing:
- final equity
- delta vs baseline
- max drawdown
- max-DD change vs baseline
- cash-outs
- re-entries
- unfinished cash cycles
- cash days
- cycle durations
- whether re-entry peak changed while cash

## Validation interpretation

This test must answer only:

1. Did +100% surge events also tend to experience >=25% later pullbacks in 2020-2022?
2. Did the frozen P1/P2 trailing overlays help or hurt this OLD10 path?
3. Did the moving re-entry peak complete cycles without parameter tuning?

Do not tune thresholds from this old period.

Do not declare a rule optimal from one OLD10 history.

## Bias disclosure

This OLD10 universe has survivorship bias because the user explicitly requested assets that existed then and still exist today.

It also differs from the present U10 universe.

The value of this test is **temporal separation of the overlay logic**, not a claim that the asset universe is historically unbiased.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_TEMPORAL_HOLDOUT
