# U10 Multimonth STREAK95 + Recovery15 v1 — Evidence

Date: 2026-09-28
Mode: STRESS_TEST_ONLY
Validated branch: research/u10-streak95-recovery15-v1

GitHub Actions run:
36381980047

Source commit:
9b73a2fb3bf8b5226c1deb604cc84dc74a4e8a4d

Artifact:
10952572573

Artifact digest:
sha256:f27bd5fe692235fe575a008bf4c8ebbf4a7559cfe38f2a509a366f42236da2c8

Result:
PASS

TEST_LEVEL:
GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Objective

Apply the exact same 15-calendar-day / 98%-of-cashout-reference recovery fallback to the multimonth STREAK95 strategy.

CONTROL:
- at least 2 consecutive positive selected calendar months;
- cumulative selected-equity gain >=95%;
- P1 sells 30% after >=5% reference pullback;
- P2 sells 30% after >=10%;
- normal 5-bar fractal buy-stop re-entry.

CANDIDATE:
- identical CONTROL;
- if cash remains parked 15 calendar days after actual cash-out:
  - recovery level = 98% of frozen-U10 reference OPEN equity at cash-out;
  - if reference close at first eligibility is already >= recovery level, schedule next-open re-entry;
  - otherwise wait for a later close crossing upward through that level.

No parameter sweep.

## Canonical U10

Baseline:
- final equity: 219,485.20 USDT
- max DD: -71.04%

### P1

CONTROL multimonth STREAK95:
- final: 219,322.44
- -0.07% vs baseline
- max DD: -69.54%
- cash days: 136

CANDIDATE + Recovery15:
- final: 218,712.79
- -0.35% vs baseline
- -609.65 USDT / -0.28% vs CONTROL
- max DD unchanged: -69.54%
- cash days: 74
- fractal re-entries: 2
- recovery re-entries: 2
- unfinished cycles: 0

Recovery15 cut 62 cash days but slightly reduced terminal equity.

### P2

CONTROL:
- final: 227,103.08
- +3.47% vs baseline
- max DD: -69.54%
- cash days: 106

CANDIDATE + Recovery15:
- final: 214,773.24
- -2.15% vs baseline
- -12,329.84 USDT / -5.43% vs CONTROL
- max DD unchanged: -69.54%
- cash days: 72
- fractal re-entries: 3
- recovery re-entries: 1
- unfinished cycles: 0

P2 lost its prior positive terminal advantage and fell below ordinary U10.

## Canonical cycle detail

### P1 2024-02 -> 2024-03

Cash-out:
2024-04-01

Recovery level:
55,310.16

At 15-day eligibility:
2024-04-16 reference close = 40,360.69

Reference was far below the recovery level:
ratio = 0.7297

Fallback therefore did NOT execute.

Normal fractal re-entry:
2024-05-05

Cash duration:
34 days

### P1 2024-10 -> 2024-11

Cash-out:
2024-12-01

Recovery level:
42,020.02

Eligibility:
2024-12-16 reference close = 47,283.18

Ratio:
1.1253

Fallback was already well above the 98% recovery level and therefore executed next open:

2024-12-17

Route:
RECOVERY_IMMEDIATE

Cash duration:
16 days

### P1 2025-04 -> 2025-05

Cash-out:
2025-06-01

Normal fractal re-entry:
2025-06-09

Cash duration:
8 days

Fractal completed before fallback eligibility.

### P1 2026-07 -> 2026-08

Cash-out:
2026-09-01

Recovery level:
169,357.46

Eligibility:
2026-09-16 reference close = 178,809.31

Ratio:
1.0558

Fallback executed:
2026-09-17

Cash duration:
16 days

Thus P1 used fallback in 2 of 4 canonical cycles.

### P2

P2 used fallback only in the 2024-10 -> 2024-11 cycle:

- cash-out: 2024-12-20
- recovery level: 42,802.62
- eligibility: 2025-01-04
- reference close: 46,812.70
- ratio: 1.0937
- fallback execution: 2025-01-05
- 16 cash days

Other P2 cycles:
- 2024 spring: fractal after 34 days while recovery level remained un-reclaimed at day 15;
- 2025 spring: fractal after 8 days;
- 2026 late cycle: fractal after 14 days, before fallback eligibility.

## 791 alternative U10s

### P1

Candidate vs CONTROL:

- better: 24.15%
- equal: 14.41%
- worse: 61.44%

Candidate vs ordinary U10:
- final > baseline: 35.52%
- both final and DD better: 34.01%

Fallback used in at least one cycle:
- 85.59% of alternative U10s

Unfinished cash:
- 0%

Median candidate delta vs CONTROL:
-0.28%

q25/q75:
-5.63% / 0.00%

Median cash-day change:
-49 days

The fallback substantially reduced time in cash but usually weakened the multimonth strategy.

### P2

Candidate vs CONTROL:

- better: 0.00%
- equal: 34.89%
- worse: 65.11%

Candidate vs ordinary U10:
- final > baseline: 14.92%
- both final and DD better: 14.29%

Fallback used:
- 65.11%

Unfinished cash:
- 0%

Median delta vs CONTROL:
-5.43%

q25/q75:
-5.43% / 0.00%

Median cash-day change:
-34 days

The result is particularly clear for P2:
Recovery15 never improved the original P2 multimonth strategy in any alternative U10 topology.

## Cycle-level behavior

Across 791 alternative U10s:

### P1

Completed candidate cycles:
1,951

- FRACTAL: 959
- RECOVERY_IMMEDIATE: 992
- RECOVERY_CROSS: 0

Fallback share:
50.85%

Median candidate cash-cycle duration:
16 days

Among fallback-used cycles, reference-close / recovery-level ratio at eligibility:

- minimum: 1.0024
- median: 1.0558
- maximum: 1.1452

Every fallback execution was already ABOVE the recovery level at the first 15-day eligibility point.

### P2

Completed candidate cycles:
1,951

- FRACTAL: 1,420
- RECOVERY_IMMEDIATE: 531
- RECOVERY_CROSS: 0

Fallback share:
27.22%

Median cash-cycle duration:
14 days

Fallback eligibility ratio:

- minimum: 1.0024
- median: 1.0937
- maximum: 1.1452

Again:
RECOVERY_CROSS = 0.

## Critical finding

The same structural problem appeared as in the single-month SURGE95 fallback test.

Whenever the 15-day fallback actually executed, the U10 reference was already above the 98% recovery level at first eligibility.

Therefore the fallback did not function as:

> wait for a later recovery through the old level.

It functioned as:

> if fractal has not finished by day 15 and reference is already near/above the sale zone, force re-entry around day 16.

This often cut off the benefit of waiting for the structural fractal entry.

## Comparison with single-month SURGE95 fallback

Single-month SURGE95:
- fallback used in about 69% of alternative universes;
- candidate worse than control in 55.75%;
- median effect -3.53%.

Multimonth STREAK95:

P1:
- fallback used in 85.59%;
- worse in 61.44%;
- median effect -0.28%.

P2:
- fallback used in 65.11%;
- worse in 65.11%;
- never better;
- median effect -5.43%.

Thus the exact Recovery15 rule is not rescued by applying it to the slower multimonth regime.

## Decision

Reject the exact 15d / 98%-reference fallback for multimonth STREAK95 as well.

Do not promote it.

Retain the original multimonth STREAK95 research controls unchanged for evidence:
- P1 original;
- P2 original.

But note that the multimonth strategy itself remains weakly robust compared with the much stronger single-month SURGE95 P1 research candidate.

## Research implication

The fallback question is now tested in two structurally different overheat regimes:

1. explosive single-month SURGE95;
2. persistent multimonth STREAK95.

In both:
- no unfinished historical cash cycles existed;
- Recovery15 reduced cash duration;
- Recovery15 usually reduced terminal performance;
- RECOVERY_CROSS never occurred.

This is strong evidence against adding an arbitrary fixed-time recovery fallback to the current research baseline.

## Live guardrail

No live/paper strategy change.

## Residual risks

- 15d / 98% are development-selected;
- topology tests share market regimes;
- absence of unfinished historical cycles does not prove future cash cannot remain stranded;
- daily synthetic reference close may miss intraday recovery paths;
- historical performance does not establish future performance.

TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
