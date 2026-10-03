# RR POST-SIGNAL ACCELERATION V1 — EVIDENCE

Date: 2026-10-03
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Runtime identity

- Research branch: `research/rr-post-signal-acceleration-v1`
- Draft PR: #113
- GitHub Actions run: `37137736593`
- Job: `111245593011`
- Artifact ID: `11279865413`
- Artifact: `rr-post-signal-acceleration-v1-2`
- Artifact SHA256: `2aecaf01f3946f695eb5c6527d2ef4a497cab50e3356deccc65ebb9d4a2169a2`
- Fixed as-of: `2026-10-03T16:30:00Z`
- Latest common closed D1 candle: `2026-10-02T00:00:00Z`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_STRESS_TEST`

## Question

After a canonical RR CONFIRMED route `SOURCE -> B` is entered at the next
daily open, does a different TARGET token C that accelerates sharply relative
to B during the first 1-3 completed daily bars continue to outperform?

Primary trigger:
- strongest third-token relative impulse reaches **+10%** within first 3 bars;
- detection uses only closed data;
- hypothetical switch occurs at the next daily open;
- primary evaluation horizon = 14 bars.

Robustness thresholds:
- +5%;
- +15%.

## Dataset

Eligible primary RR opportunities:
- **3420**

## Aggregate results

### +5% trigger

Trigger count:
- 2441 / 3420 = **71.4%**

| Horizon | N | Candidate beats baseline | Median relative excess | Mean relative excess | Strong continuation >=20% |
|---:|---:|---:|---:|---:|---:|
| 3d | 2431 | 47.2% | -0.43% | +0.79% | 4.4% |
| 7d | 2425 | 46.1% | -1.05% | +1.97% | 8.5% |
| 14d | 2395 | 49.3% | -0.41% | +2.61% | 12.2% |
| 30d | 2356 | 44.7% | -3.51% | +3.60% | 18.7% |

### +10% primary trigger

Trigger count:
- 1444 / 3420 = **42.2%**

| Horizon | N | Candidate beats baseline | Median relative excess | Mean relative excess | Strong continuation >=20% |
|---:|---:|---:|---:|---:|---:|
| 3d | 1438 | 45.3% | -0.69% | +0.60% | 6.7% |
| 7d | 1423 | 45.3% | -1.86% | +2.93% | 11.2% |
| 14d | 1407 | 47.5% | -1.35% | +3.81% | 15.9% |
| 30d | 1382 | 46.2% | -3.59% | +5.56% | 19.5% |

### +15% robustness trigger

Trigger count:
- 793 / 3420 = **23.2%**

| Horizon | N | Candidate beats baseline | Median relative excess | Mean relative excess | Strong continuation >=20% |
|---:|---:|---:|---:|---:|---:|
| 3d | 792 | 41.8% | -1.44% | +0.59% | 10.1% |
| 7d | 777 | 47.1% | -1.57% | +5.01% | 13.6% |
| 14d | 770 | 46.9% | -2.07% | +4.81% | 18.1% |
| 30d | 754 | 40.6% | -7.49% | +5.78% | 21.5% |

## Detection timing

### +10% primary trigger
- day 1: 424 = 29.4%
- day 2: 606 = 42.0%
- day 3: 414 = 28.7%

The signal is not merely a day-1 gap detector; most +10% triggers appear on
days 2-3.

## Core finding

The post-signal acceleration idea **does catch real explosive continuations**,
but as a generic switching rule it does not generalize.

At the primary +10% / 14d setting:

- candidate beats baseline: **47.5%**
- median relative excess: **-1.35%**
- mean relative excess: **+3.81%**
- strong-continuation >= +20%: **15.9%**
- false-positive / non-outperformance rate: **52.5%**

The positive mean with a negative median is the same right-tail structure seen
in the earlier third-token momentum test:

`MOST_CASES_NOT_BETTER + SMALL_SUBSET_OF_LARGE_WINNERS`

Raising the threshold from 10% to 15% does not fix the typical result:
- median 14d relative excess worsens to -2.07%;
- candidate beats baseline remains below 50%.

Therefore:

`NAIVE_POST_SIGNAL_ACCELERATION_SWITCH = NOT_SUPPORTED`

## LINK -> ALGO / AAVE live-case

Canonical signal:
- close: 2026-09-28
- route: `LINK -> ALGO`
- baseline entry: 2026-09-29 open

AAVE acceleration versus ALGO:

| Observation | Top candidate | Top relative impulse | AAVE relative impulse | AAVE rank |
|---:|---|---:|---:|---:|
| day 1 / Sep-29 close | AAVE | +19.73% | +19.73% | 1 |
| day 2 / Sep-30 close | AAVE | +17.12% | +17.12% | 1 |
| day 3 / Oct-01 close | AAVE | +27.23% | +27.23% | 1 |

AAVE crossed all three frozen thresholds on **day 1**:
- +5%;
- +10%;
- +15%.

Under the causal primary +10% rule:
- detection: Sep-29 close;
- hypothetical switch: Sep-30 open;
- selected candidate: **AAVE**;
- AAVE was the strongest candidate, not merely one of many qualifying tokens.

From the hypothetical Sep-30 switch open through the latest closed candle
available in V1:
- AAVE relative excess versus staying in ALGO: **+7.28%**.

Thus the live AAVE episode is a genuine example that the acceleration detector
would have caught causally.

## Interpretation

The current AAVE case is **not hindsight under this V1 detector**:
after one completed day following the LINK -> ALGO entry, AAVE had already
become the strongest relative accelerator by +19.73% versus ALGO.

However, historical evidence says that doing this systematically would be
wrong without another filter:
- more than half of +10% triggers fail to outperform by 14 days;
- the median outcome is negative;
- stronger +15% impulses still do not solve the problem.

The remaining research problem is therefore narrower:

> Can we distinguish the ~16% of +10% acceleration triggers that become
> >= +20% relative continuations from the majority that mean-revert?

That is a classification / second-stage confirmation problem, not a reason to
replace the RR core.

## Decision

No production change.

Frozen classifications:

`AAVE_2026_09_29_ACCELERATION = CAUSALLY_DETECTABLE`

`NAIVE_POST_SIGNAL_ACCELERATION_SWITCH = NOT_SUPPORTED`

`EXTREME_CONTINUATION_SUBSET = RESEARCH_CANDIDATE`

## Next admissible research

A new preregistered V2 may study features available **at acceleration detection**
that separate extreme continuations from false positives, for example:

- candidate's own RR topology/state at detection;
- candidate breadth/rank across all source relationships;
- acceleration persistence across two consecutive closes;
- volume/liquidity expansion if point-in-time data are available;
- cross-sectional breadth of simultaneous acceleration.

Do not retune the +5/+10/+15 thresholds on the same history.

## Artifact files

- `eligible_rr_opportunities.csv`
- `trigger_ledger.csv`
- `aggregate_summary.csv`
- `detection_day_distribution.csv`
- `link_algo_aave_case.csv`
- `link_algo_aave_case.json`
- `summary.json`
- `report.md`

## No-repeat rule

Do not rerun the same first-3-bar relative-impulse threshold family on the same
history merely to choose a nicer threshold.

A repeat requires:
- new forward evidence;
- a separately preregistered second-stage feature family;
- a material bug fix;
- or a different causal research question.
