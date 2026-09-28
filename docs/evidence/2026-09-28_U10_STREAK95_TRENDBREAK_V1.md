# U10 STREAK95 Trend-Break Exit v1 — Evidence

Date: 2026-09-28
Mode: STRESS_TEST_ONLY
Validated branch: research/u10-streak95-trendbreak-v1

Primary GitHub Actions run:
36378225818

Primary source commit:
8908631ea56174d0e11e5a28aff0d237a9cbfac8

Primary artifact:
10951773117

Artifact digest:
sha256:f279cd6f1531efe070d633afbb602e2a369c15302b36d2b5669c1ef67018a876

Independent canonical verification run:
36378296336

Canonical verification artifact:
10951701875

Canonical artifact digest:
sha256:c83732de3f6770b79af260e83b8971694f850f0779ccc5464c1356933ce0b3a3

Result:
PASS

TEST_LEVEL:
GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Objective

Test whether the weak STREAK95 sale rule improves if 30% cash-out is delayed until a causal trend-break is visible.

The STREAK95 trigger and re-entry mechanics were frozen.

## Frozen STREAK95

A qualifying event requires:

- at least 2 consecutive positive selected calendar months;
- cumulative selected-equity gain >=95%;
- any zero/negative selected month resets the streak;
- a single +95% month does not qualify.

## Trend-break filter

After STREAK95:

1. remain fully invested while the positive monthly streak remains intact;
2. wait for the first completed selected calendar month with selected change <=0;
3. on the currently held U10 token require a confirmed 5-bar LOWER HIGH;
4. lower high means a later confirmed 5-bar swing high strictly below a prior confirmed 5-bar swing high on the same uninterrupted asset segment;
5. if U10 rotates assets, the pivot comparison resets;
6. once lower high is confirmed:
   - P1 requires the same >=5% reference pullback from latest running peak;
   - P2 requires >=10%;
7. sell 30% next open.

Because the lower-high confirmation frequently happened after a much larger decline, the 5% / 10% condition was usually already satisfied.

## Re-entry

Unchanged from prior 5-bar fractal research:

- while cash is parked, update latest U10 reference running peak;
- activate search after reference equity is >=15% below latest peak;
- confirmed 5-bar swing-high buy-stop;
- lower pivots ratchet stop downward;
- U10 asset rotation resets the order;
- next-session stop execution;
- 0.1% modeled re-entry cost.

## Canonical U10

Baseline:

- final equity: 219,485.20 USDT
- max DD: -71.04%

Original STREAK95:

P1:
- 219,322.44
- -0.07% vs baseline
- max DD -69.54%

P2:
- 227,103.08
- +3.47%
- max DD -69.54%

### P1-TRENDBREAK

- final: 198,096.80
- delta vs baseline: -21,388.40 / -9.74%
- delta vs original P1-STREAK95: -9.68%
- max DD: -71.17%
- max-DD change vs baseline: -0.12 pp
- cash-outs: 3
- re-entries: 3
- unfinished cash cycles: 0
- unfinished WATCH states: 1
- total WATCH days: 141
- cash days: 60

### P2-TRENDBREAK

- final: 196,582.17
- delta vs baseline: -22,903.03 / -10.43%
- delta vs original P2-STREAK95: -13.44%
- max DD: -71.17%
- cash-outs: 3
- re-entries: 3
- unfinished WATCH: 1
- cash days: 53

The independent canonical verification reproduced these results exactly.

## Canonical cycle diagnostics

### 2024-02 -> 2024-03 streak

STREAK95 signal:
2024-03-31

Monthly streak break:
2024-04-30

Lower-high structure:
- prior ATOM pivot: 9.243 on 2024-04-23
- lower pivot: 9.10 on 2024-05-02
- lower-high confirmation: 2024-05-04

Reference drawdown at lower-high confirmation:
-31.43%

Cash-out:
- signal 2024-05-04
- execution 2024-05-05

Thus the trend-break filter waited until the large correction had already occurred.

Re-entry:
2024-05-20

### 2024-10 -> 2024-11 streak

STREAK95:
2024-11-30

Monthly break:
2024-12-31

Lower-high confirmation:
2025-01-19 on BNB

Reference drawdown at lower high:
-8.87%

P1:
- cash-out 2025-01-20

P2:
- 10% threshold was not yet met at lower-high confirmation;
- cash-out signal 2025-01-26
- execution 2025-01-27
- actual pullback -10.98%

This is one of the few cases where the filter behaved close to the intended idea.

### 2025-04 -> 2025-05 streak

STREAK95:
2025-05-31

Monthly break:
2025-06-30

Lower-high confirmation:
2025-07-01 on HBAR

Reference drawdown:
-33.13%

Cash-out:
2025-07-02

Again the filter sold only after the large decline was already established.

### 2026-07 -> 2026-08 streak

No later non-positive selected month existed before test end.

Therefore the strategy remained fully invested and finished in WATCH state.

This is not stranded cash; it is an unresolved diagnostic state.

## 791 alternative U10 topology stress

### P1-TRENDBREAK

Across 791 alternatives:

- final > ordinary U10 baseline: 47.79%
- final > original P1-STREAK95: 59.42%
- max DD improved vs baseline: 3.92%
- BOTH final and DD improved: 2.91%
- unfinished cash cycles: 0%
- unfinished WATCH state: 58.28%

Terminal delta vs baseline:
- median: -0.83%
- q25: -1.42%
- q75: +4.44%

Median delta vs original P1-STREAK95:
+1.46%

Median total WATCH days:
98

Median cash days:
26

P1 trend-break slightly improved the original P1 implementation in a majority of topologies, but still failed to produce a robust advantage over ordinary U10 and almost eliminated drawdown protection.

### P2-TRENDBREAK

- final > baseline: 47.03%
- final > original P2-STREAK95: 44.50%
- max DD improved: 3.41%
- BOTH improved: 2.53%
- unfinished WATCH: 58.28%
- unfinished cash: 0%

Median terminal delta vs baseline:
-0.83%

Median delta vs original P2-STREAK95:
-0.75%

Median WATCH days:
98

Median cash days:
26

P2 was clearly worse than the original P2-STREAK95 implementation.

## Sale-timing distribution

Across 1,490 completed alternative P1 trend-break cash cycles:

Delay from STREAK95 trigger to sale signal:

- minimum: 31 days
- q25: 34 days
- median: 50 days
- q75: 60 days
- maximum: 67 days

Reference drawdown at lower-high / P1 sale:

- least-deep: about -8.87%
- q25: about -33.13%
- median: about -31.43%
- q75: about -9.59%
- deepest: about -44.52%

Because the distribution clusters by shared market regimes, quartile ordering reflects discrete regime concentrations rather than a smooth statistical distribution.

More directly:

- 71.81% of completed cycles sold after reference decline >=15%
- 53.09% after decline >=25%
- 52.62% after decline >=30%
- 22.15% after decline >=35%

P2 had essentially the same lower-high timing; its 10% pullback gate occasionally delayed sale further.

## Why the filter failed

The hypothesis was:

> A completed negative month plus lower high may distinguish a real trend break from an ordinary pullback.

The structural idea is coherent, but the chosen confirmation stack is too slow:

1. STREAK95 is only known at month completion.
2. The strategy then waits until an entire later month finishes non-positive.
3. It then waits for a 5-bar lower-high confirmation.
4. In strong corrections, the market can already be 25-35% below the running peak before all these conditions become known.

Therefore the filter frequently identifies a trend break **after the protective sale opportunity has already passed**.

The drawdown result confirms this:
only about 3-4% of alternative U10s improved max DD versus baseline.

## Important nuance

The 58.28% unfinished WATCH rate is not the same as stranded cash.

Those universes remained fully invested because the trend-break conditions were not completed before dataset end.

This is safer than an unresolved cash position, but it also shows how conservative/slow the filter is.

## Decision

Reject this exact implementation:

STREAK95
-> wait for completed non-positive month
-> wait for 5-bar lower high
-> sell 30%

It is too slow for protective capital management.

Do not promote it.

The negative result does NOT invalidate the broader idea that persistent-trend overheat needs a causal trend-break filter.

It only rejects this specific confirmation stack.

## Research implication

The next useful question, if pursued, is not another sale percentage.

The evidence suggests the monthly-break requirement is the dominant source of delay.

A future causal trend-break rule would need to react **inside the month** while still avoiding the premature-entry problems seen with overly fast 3-bar structures.

That would be a new preregistered hypothesis.

## Live guardrail

No live/paper change.

## Residual risks

- STREAK95 and 95% were development-selected;
- 792 topologies share the same market dates;
- lower-high structure is one specific trend-break definition;
- daily OHLC cannot reveal all intraday path details;
- no untouched future temporal validation;
- historical performance does not establish future performance.

TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
