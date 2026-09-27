# U10 Monthly Surge -> Pullback Overlay v1 — Evidence

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Validated branch: research/u10-monthly-surge-pullback-v1
GitHub Actions run: 36343997067
Source commit: d539cf552f7a1a8f3b9c584cfd73674f4f041c35
Artifact ID: 10939368539
Artifact digest: sha256:a3c7352d542c91db2b1de98b102b6c0a57146f6631a8e1d9665215118001d5cd
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Research question

After a very large monthly U10 capital surge, does a meaningful pullback occur often enough to justify a partial profit-lock and lower re-entry overlay?

The user-proposed primary mechanics were preregistered before reviewing the exhaustive-universe result:

P1:
- arm after +100% monthly reference-equity surge versus the prior completed selected-month extreme;
- wait for a 5% pullback from the post-arm running peak;
- sell 30% of the invested sleeve into USDT on the next open;
- re-enter all parked cash after a 25% reference-equity drawdown from the locked peak;
- 0.1% modeled cash-out cost;
- 0.1% modeled re-entry cost.

P2:
- same mechanics, but wait for a 10% pullback before selling 30%.

No live/paper U10 behavior changed.

## Important correction to the original observation

The monthly selected-extreme diagnostic had three canonical U10 monthly increases above +100%:

- 2024-02: +265.56%
- 2025-11: +142.28%
- 2026-01: +109.38%

At the coarse one-selected-point-per-month level:
- 2025-11 was followed by 2025-12 -46.80%;
- 2026-01 was followed by 2026-02 -38.03%;
- 2024-02 was followed by another positive selected point in March, with the large selected-month decline occurring in April.

Therefore the preregistered claim was deliberately weakened from:
"the next month falls at least 25%"

to:

"after an unusually large monthly surge, a substantial pullback may occur within roughly the next one to two months."

The daily-equity event study below then tested that claim directly.

## Canonical U10 direct event study

For each completed selected-month surge >= +100%:
- start from the selected surge date/value;
- allow the reference equity to make new highs;
- measure the worst running-peak drawdown within 31 and 62 calendar days.

| Surge month | Selected surge | Surge equity | Peak seen in 31d | Worst running DD 31d | Worst running DD 62d |
|---|---:|---:|---:|---:|---:|
| 2024-02 | +265.56% | 56,284.29 | 66,881.36 | -26.25% | -40.66% |
| 2025-11 | +142.28% | 175,691.95 | 175,691.95 | -42.08% | -46.80% |
| 2026-01 | +109.38% | 195,693.36 | 195,693.36 | -38.03% | -38.03% |

Canonical event count:

3

Canonical -25% hit rate:
- within 31 days: 3/3 = 100%
- within 62 days: 3/3 = 100%

Median worst running pullback:
- 31 days: -38.03%
- 62 days: -40.66%

This strengthens the user's original observation when daily running-peak drawdown is used rather than only one selected monthly point.

## Canonical baseline

Mature window:

2023-10-31 -> 2026-09-26

Initial:

10,000 USDT

Baseline final equity:

219,485.20 USDT

Baseline return:

+2,094.85%

Baseline max drawdown:

-71.04%

## Canonical primary overlay results

| Variant | Surge | Sell pullback | Cash fraction | Re-entry | Final equity | Delta vs baseline | Max DD | DD improvement | Cash-outs | Re-entries | Days in cash |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| P1 | +100% | -5% | 30% | -25% | 270,653.52 | +23.31% | -66.36% | +4.68 pp | 3 | 3 | 66 |
| P2 | +100% | -10% | 30% | -25% | 265,751.95 | +21.08% | -66.36% | +4.68 pp | 3 | 3 | 63 |

### P1 actual cycles

Cycle 1:
- arm month: 2024-02
- arm date: 2024-02-26
- locked peak: 56,284.29
- cash-out signal: 2024-02-29 at reference equity 48,496.53 / -13.84% from peak
- cash-out execution: 2024-03-01
- re-entry signal: 2024-04-13 at 39,684.39 / -29.49%
- re-entry execution: 2024-04-14
- time in cash by execution-date difference: 44 days

Cycle 2:
- arm month: 2025-11
- arm date: 2025-11-07
- locked peak: 175,691.95
- cash-out signal: 2025-11-08 at 153,212.49 / -12.79%
- execution: 2025-11-09
- re-entry signal: 2025-11-14 at 123,109.51 / -29.93%
- execution: 2025-11-15
- time in cash: 6 days

Cycle 3:
- arm month: 2026-01
- arm date: 2026-01-06
- locked peak: 195,693.36
- cash-out signal: 2026-01-15 at 182,905.22 / -6.53%
- execution: 2026-01-16
- re-entry signal: 2026-01-31 at 144,268.96 / -26.28%
- execution: 2026-02-01
- time in cash: 16 days

Total P1 daily closes with parked cash:

66

### P2 actual cycles

The first two cycles used the same actual cash-out dates as P1 because the daily close crossed both 5% and 10% thresholds in one move.

The third cycle waited longer:

- locked peak: 195,693.36
- cash-out signal: 2026-01-18 at 173,482.88 / -11.35%
- execution: 2026-01-19
- re-entry execution: 2026-02-01

Total P2 daily closes with parked cash:

63

## Exhaustive alternative-U10 topology test

Candidate pool:

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, AVAX, FIL, ETH, ALGO, ADA, XRP, HBAR

Mandatory in every U10:
- ATOM
- TWT
- PEPE

Choose 7 of the other 12.

Total U10 universes:

792

Canonical U10 is one member.

Alternative topology set:

791 universes

Every universe used:
- the same market dates;
- the same 180d / 15% / 3% / max-dislocation / next-open U10 mechanics;
- ATOM start;
- the same 10,000 USDT mature start;
- the same overlay rules.

## Does the +100% -> large pullback effect generalize across U10 topologies?

All 791 alternative universes had at least one +100% selected-month surge event.

Total alternative +100% surge events:

2,227

Events per alternative universe:
- 2 events: 216 universes
- 3 events: 505 universes
- 4 events: 70 universes

Pooled -25% running-drawdown hit rate:
- within 31 days: 88.86%
- within 62 days: 88.95%

Pooled median worst running pullback:
- 31 days: -38.03%
- 62 days: -40.66%

Universe-level hit-rate distribution for 31d:
- median universe hit rate: 100%
- 69.91% of alternative universes had every +100% event reach -25% within 31d
- 95.20% had at least two-thirds of their events reach -25%
- every alternative universe had at least 50% of its +100% events reach -25%

The 62d distribution was nearly identical.

## Market-regime clustering

The topology result is not equivalent to 2,227 independent market episodes.

The events cluster in the same broad crypto periods:

| Surge month | Alternative events | -25% hit in 31d | -25% hit in 62d | Median worst 31d DD |
|---|---:|---:|---:|---:|
| 2026-01 | 791 | 100.00% | 100.00% | -41.89% |
| 2025-11 | 461 | 100.00% | 100.00% | -42.08% |
| 2025-05 | 330 | 88.79% | 89.39% | -26.09% |
| 2024-02 | 329 | 100.00% | 100.00% | -26.25% |
| 2024-11 | 196 | 53.57% | 53.57% | -26.00% |
| 2025-07 | 120 | 0.00% | 0.00% | -15.98% |

This is crucial.

The effect is broad across topologies, but it is **not universal across every surge regime**.

July 2025 is a direct counterexample: 120 alternative U10s produced a +100% selected-month surge, yet none reached a -25% running pullback in the following 31 or 62 days.

Therefore the evidence supports a conditional historical tendency, not a deterministic rule.

## P1 robustness across 791 alternative U10s

P1:

+100% surge / 5% pullback / sell 30% / re-enter -25%.

Results:

- terminal equity improved versus its own baseline in 761/791 = 96.21% of alternatives
- max drawdown improved in 715/791 = 90.39%
- BOTH terminal equity and max drawdown improved in 685/791 = 86.60%

Terminal delta distribution:
- median: +23.44%
- q25: +18.97%
- q75: +25.81%
- minimum: -14.60%
- maximum: +38.12%

Max-DD improvement:
- median: +4.68 percentage points
- observed range: 0.00 to +10.90 pp

Typical activity:
- median cash-outs: 3
- median re-entries: 3
- median daily closes with cash parked: 80
- q25/q75 cash days: 66 / 120

Thus P1 was not merely a canonical-U10 artifact in this topology stress.

It still failed to improve terminal equity in 30 alternative universes and therefore is not a universal dominance rule.

## P2 robustness across 791 alternative U10s

P2:

+100% surge / 10% pullback / sell 30% / re-enter -25%.

Results:

- terminal equity improved in 761/791 = 96.21%
- max drawdown improved in 713/791 = 90.14%
- BOTH improved in 683/791 = 86.35%

Terminal delta distribution:
- median: +20.10%
- q25: +15.45%
- q75: +22.70%
- minimum: -16.76%
- maximum: +30.34%

Median max-DD improvement:

+3.41 percentage points

Median daily closes with cash parked:

63

P2 was also broadly positive historically, but P1 produced the stronger median terminal uplift in this frozen test.

This does NOT prove 5% is generally superior.

## Sensitivity grid

Preregistered sensitivity:

- surge: +75%, +100%, +125%
- sell pullback: 5%, 10%, 15%
- re-entry: -20%, -25%, -30%, -35%
- cash fraction fixed at 30%

36 combinations.

Important descriptive regions:

### +100 / 5 / re-entry -30

Across 791 alternatives:
- terminal improve rate: 96.21%
- DD improve rate: 90.52%
- both improve: 86.73%
- median terminal delta: +28.68%
- median DD improvement: +4.81 pp
- median cash days: 129

This is an interesting robustness region for follow-up.

### +100 / 5 / re-entry -35

Across 791 alternatives:
- terminal improve rate: 80.03%
- DD improve rate: 90.90%
- both improve: 71.05%
- median terminal delta: +41.86%
- median DD improvement: +6.12 pp
- median cash days: 143

The larger median gain comes with materially lower consistency across universes.

It should NOT be called the best or optimal rule.

### +125 surge threshold

The stricter +125% threshold usually produced fewer cycles:
median one cash-out/re-entry.

Its max-DD benefit was far less consistent across topologies than the +100% region.

### +75 surge threshold

The lower +75% threshold produced more events/cycles and several strong historical cells, but also more sensitivity to re-entry depth and more cases with long parked-cash periods.

## Interpretation

This experiment changes the status of the hypothesis.

Before the test:
- the idea was a pattern noticed on one canonical U10 path;
- data-snooping risk was high.

After the test:
- the direct +100%-surge / >=25%-pullback effect appeared in 88.86% of 2,227 alternative-topology events within 31 days;
- the exact P1/P2 overlay improved terminal equity in 96.21% of 791 alternative U10s;
- P1/P2 improved BOTH terminal equity and max drawdown in about 86% of alternatives.

Therefore the effect is strong enough to deserve continued research.

However it is not yet promotion-grade evidence because:
- all universes share the same historical market dates;
- topology variations are highly correlated observations, not 791 independent market histories;
- the event table shows clear regime dependence;
- some universes lose from the overlay;
- this is still retrospective historical data.

## Next validation required

Do not immediately add this rule to live U10.

The highest-value next validation is temporal rather than another topology sweep.

Recommended next steps:

1. Freeze P1 and P2 exactly as tested.
2. Test them on rolling / earlier held-out capital-start windows where the monitor has valid prior state.
3. Separate discovery period from a genuinely later evaluation period where feasible.
4. Add forward paper observation of future +100% surge events.
5. Only after temporal evidence decide whether the overlay deserves promotion.

## Guardrail

No live/paper U10 behavior changed by this research.

No parameter combination is promoted.

TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Residual risks

- shared market dates create strong dependence across the 792 universes;
- only a small number of distinct macro surge regimes exist in the mature history;
- daily-close triggers can gap across 5%, 10%, 25%, etc.;
- cash is modeled as non-yielding USDT;
- stablecoin/counterparty risk is not modeled;
- additional real spread/slippage may exceed 0.1%;
- historical performance does not establish future performance.
