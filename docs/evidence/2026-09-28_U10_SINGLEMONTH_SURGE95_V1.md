# U10 Single-Month Surge 95% v1 — Evidence

Date: 2026-09-28
Mode: STRESS_TEST_ONLY
Validated branch: research/u10-singlemonth-surge95-v1

Primary GitHub Actions run:
36379375833

Primary source commit:
8b06df43b2a56ad541fe1beb0c8e3caba06d2ed6

Primary artifact:
10951894492

Artifact digest:
sha256:1f72b2d0ccab2af044fd78672808c18c527d3674fc984891145e67cf9e541250

Independent canonical verification run:
36379478637

Canonical verification artifact:
10952510166

Canonical verification digest:
sha256:70abb251951c2363e86f5237dcde3e6a3b4a8c24c3a76e8c80d26b4b26a8fe69

Result:
PASS

TEST_LEVEL:
GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Objective

Test exactly one change to the established explosive single-month surge rule:

- control: selected monthly change >= +100%
- candidate: selected monthly change >= +95%

This is NOT the multimonth STREAK95 rule.

All trading mechanics remained frozen:
- P1 cash-out after 5% post-surge pullback
- P2 cash-out after 10%
- sell 30%
- 0.1% modeled cash-out cost
- re-entry search after reference equity is >=15% below latest running peak
- confirmed 5-bar swing-high buy-stop on currently held U10 asset
- lower pivot highs ratchet stop downward
- asset rotation resets the stop
- 0.1% re-entry cost

No other parameter changed.

## Canonical U10 event study

With >=100%:
- 3 events

With >=95%:
- 4 events

New incremental event:

September 2025:
- selected monthly change: +98.98%
- selected equity: 127,668.80 USDT
- worst running drawdown within 31d: -43.20%
- within 62d: -44.54%
- within 93d: -46.80%
- -25% hit: YES in all horizons

Canonical event summary:

| Group | Events | -25% hit 31d | 62d | 93d | Median worst DD 31d |
|---|---:|---:|---:|---:|---:|
| >=100% | 3 | 100% | 100% | 100% | -38.03% |
| >=95% | 4 | 100% | 100% | 100% | -40.05% |
| incremental 95-100% | 1 | 100% | 100% | 100% | -43.20% |

On the canonical path, lowering the threshold captured a real large-pullback event rather than noise.

## Canonical portfolio

Baseline U10:
- final: 219,485.20 USDT
- max DD: -71.04%

### P1 5-bar fractal

100% trigger:
- final: 258,557.42
- +17.80% vs baseline
- max DD: -68.36%
- 3 cash-outs / 3 re-entries
- 145 cash days

95% trigger:
- final: 262,340.46
- +19.53% vs baseline
- +3,783.04 USDT / +1.46% vs 100%-trigger variant
- max DD: -68.36%
- 4 cash-outs / 4 re-entries
- 154 cash days
- unfinished cycles: 0

### P2 5-bar fractal

100% trigger:
- final: 253,741.70

95% trigger:
- final: 257,454.27
- +17.30% vs baseline
- +3,712.57 USDT / +1.46% vs 100%-trigger variant
- max DD unchanged at -68.36%
- 4/4 cycles completed
- 151 cash days

## 791 alternative U10 topology stress

### Event counts

>=100%:
- 2,227 events

>=95%:
- 2,760 events

Incremental 95-100%:
- 533 events
- spread across 516 / 791 alternative U10s
- 499 universes gained one new event
- 17 universes gained two new events

Incremental-event -25% hit rate:
- within 31d: 96.06%
- within 62d: 96.06%
- within 93d: 96.06%

Median incremental-event worst running drawdown:
- 31d: -43.20%
- 62d: -51.68%
- 93d: -60.70%

At face value this is stronger than the >=100% pooled event set.

## Critical regime clustering

The 533 incremental 95-100% events are NOT 533 independent market episodes.

They cluster almost entirely in two calendar regimes:

### September 2025
- 512 incremental events
- -25% hit within 31d: 100%
- within 62d: 100%
- within 93d: 100%
- median worst DD 31d: -43.20%
- median worst DD 62d: -51.68%
- median worst DD 93d: -60.70%

### July 2026
- 21 incremental events
- -25% hit within 31d: 0%
- within 62d: 0%
- within 93d: 0%
- median worst DD: about -19.04%

Therefore the pooled 96.06% rate is dominated by the shared September-2025 regime.

The threshold reduction is topology-robust in that regime, but the evidence still contains only a very small number of distinct calendar regimes.

## P1 95% versus 100% across 791 alternatives

95%-trigger P1:
- final > ordinary U10 baseline: 100.00%
- max DD improved vs baseline: 91.78%
- BOTH final and DD improved: 91.78%
- unfinished cycles: 0%

Terminal delta vs baseline:
- q25: +17.80%
- median: +23.17%
- q75: +25.39%

Direct 95% vs 100%:
- 95% better: 516 / 791 = 65.23%
- equal: 275 / 791 = 34.77%
- worse: 0 / 791 = 0%

Among the 516 universes that actually gained at least one 95-100% event:
- 95% was better in 516/516
- equal: 0
- worse: 0
- median improvement vs 100%: +1.46%

### Regime detail

Universes with September-2025 incremental event:
- 512
- P1 95% better than 100% in 512/512

Universes with July-2026 incremental event:
- 21
- P1 95% better in 21/21
- despite those July events not reaching -25% within 93 days

Thus P1 did not require a full -25% correction to benefit from the extra event under the frozen 5-bar implementation.

## P2 95% versus 100% across 791 alternatives

95%-trigger P2:
- final > baseline: 100.00%
- max DD improved: 80.91%
- BOTH improved: 80.91%
- unfinished cycles: 0%

Median terminal delta vs baseline:
+19.26%

Direct 95% vs 100%:
- better: 512 / 791 = 64.73%
- equal: 275
- worse: 4

Among universes with incremental events:
- better: 512
- worse: 4

The four failures belonged to the small July-2026 incremental regime.

P1 therefore remained the more robust variant.

## Comparison with 100% threshold

Prior 100%-trigger P1 fractal topology result:
- final > baseline: 100%
- both final and DD improved: 90.14%
- median terminal delta vs baseline: +21.39%

95%-trigger P1:
- final > baseline: 100%
- both improved: 91.78%
- median terminal delta: +23.17%

On this frozen 2023-2026 topology dataset, lowering the threshold from 100% to 95% improved both terminal distribution and drawdown-improvement coverage.

## Interpretation

The 95% threshold performed better than 100% in the current topology stress.

Most importantly:
- canonical U10 gained one real event at +98.98%;
- that event led to a large subsequent correction;
- 516 alternative U10s gained incremental events;
- P1 improved in all 516 of those universes and worsened in none.

However this does NOT provide 516 independent confirmations.

The incremental signal is highly regime-clustered:
- one very strong September-2025 regime;
- one small July-2026 counter-regime.

Therefore the correct conclusion is:

> +95% is a stronger candidate threshold than +100% on the current historical/topology evidence, especially for P1, but the improvement is largely supported by one shared historical regime and still requires future temporal confirmation.

## Selection-bias warning

The user proposed 95% after noticing that the canonical September-2025 monthly increase was approximately +98.98%.

Therefore 95% is explicitly data-informed.

It must not be treated as a clean out-of-sample discovery.

No further threshold sweep should be performed from this result.

## Decision

For research baseline:
- prefer tracking P1 at 95% alongside the frozen 100% control;
- do not yet promote 95% to live/paper execution;
- do not search 96/97/98/99%;
- future new surge events are the cleanest validation.

P2 remains secondary.

## Live guardrail

No live/paper strategy behavior changed.

## Residual risks

- 95% was selected after observing the +98.98% canonical event;
- alternative U10s share the same calendar market regimes;
- most incremental evidence comes from September 2025;
- only one small distinct counter-regime appears in July 2026;
- 5-bar fractal mechanics remain development-selected;
- daily OHLC execution has simplified friction;
- historical performance does not establish future performance.

TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
