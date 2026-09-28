# U10 Consecutive Positive-Month Streak >=95% v1 — Evidence

Date: 2026-09-28
Mode: STRESS_TEST_ONLY
Validated branch: research/u10-multimonth-streak95-v1

GitHub Actions run:
36377218013

Source commit:
da177d378842ccabcd7e2d9604f26de3176f4cd3

Artifact:
10951108176

Artifact digest:
sha256:509204c7b824814c5711e594ba96800d634157d4dd666f121ce466ebec724fc8

Result:
PASS

TEST_LEVEL:
GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Hypothesis

Test a distinct overheating trigger:

> At least two consecutive positive selected calendar months, with no negative/zero month inside the streak, and cumulative selected-equity gain >=95%.

Any non-positive selected month resets the streak.

A single +95% month does not qualify.

This study changes only the trigger.

The P1/P2 30% cash-out and 5-bar structural buy-stop re-entry mechanics remain frozen.

## Causal timing

The selected monthly extreme is known only when the calendar month completes.

Therefore:
- streak qualification is evaluated after the final daily close of the qualifying month;
- the highest U10 reference daily-close equity inside the streak is retained as the known streak peak;
- any P1/P2 cash-out can occur no earlier than the following session.

## Canonical U10 streak events

Four qualifying events occurred.

| Streak | Positive months | Cumulative gain | Overlap with single +100% month | Worst DD 31d | Worst DD 62d | Worst DD 93d |
|---|---:|---:|---:|---:|---:|---:|
| 2024-02 -> 2024-03 | 2 | +334.39% | YES | -40.66% | -40.66% | -47.89% |
| 2024-10 -> 2024-11 | 2 | +138.55% | NO | -13.33% | -13.33% | -31.34% |
| 2025-04 -> 2025-05 | 2 | +140.35% | NO | -38.25% | -38.25% | -38.25% |
| 2026-07 -> 2026-08 | 2 | +108.12% | NO | -18.34% | -18.34% | -18.34% |

Canonical -25% hit rate:
- 31 days: 2/4 = 50%
- 62 days: 2/4 = 50%
- 93 days: 3/4 = 75%

The one event overlapping the prior single-month +100% signal was also the strongest event.

Among the three genuinely distinct non-overlap canonical streaks:
- -25% within 31d: 1/3
- within 62d: 1/3
- within 93d: 2/3

## Canonical portfolio

Baseline:
- final equity: 219,485.20 USDT
- max DD: -71.04%

### P1-STREAK95

- final: 219,322.44
- delta vs baseline: -162.76 / -0.07%
- max DD: -69.54%
- DD improvement: +1.51 pp
- streak events: 4
- cash-outs: 4
- re-entries: 4
- unfinished cycles: 0
- cash days: 136

P1 was essentially terminal-neutral while modestly reducing max DD.

### P2-STREAK95

- final: 227,103.08
- delta vs baseline: +7,617.88 / +3.47%
- max DD: -69.54%
- DD improvement: +1.51 pp
- streak events: 4
- cash-outs: 4
- re-entries: 4
- unfinished: 0
- cash days: 106

P2 was better than P1 on the canonical path, but the exhaustive topology test below shows the effect is weakly robust.

## Canonical cash cycles

### 2024-02 -> 2024-03

Cumulative streak:
+334.39%

P1/P2 cash-out:
2024-04-01

5-bar re-entry:
2024-05-05

Reference drawdown at re-entry:
about -26.37%

This event strongly overlaps the already-known single-month +100% surge phenomenon.

### 2024-10 -> 2024-11

Cumulative streak:
+138.55%

No single-month +100% overlap.

P1:
- cash-out 2024-12-01
- re-entry 2025-02-08
- reference DD at re-entry about -19.45%
- 69 cash days

P2:
- cash-out 2024-12-20
- same re-entry 2025-02-08
- 50 cash days

The first 62 days after streak completion did not reach -25%.

### 2025-04 -> 2025-05

Cumulative streak:
+140.35%

No single-month +100% overlap.

Cash-out:
2025-06-01

Re-entry:
2025-06-09

Reference DD at re-entry:
about -17.79%

This distinct multimonth event did produce a strong later correction.

### 2026-07 -> 2026-08

Cumulative streak:
+108.12%

No single-month +100% overlap.

P1 cash-out:
2026-09-01

P2 cash-out:
2026-09-12

Re-entry:
2026-09-26

Reference equity at the re-entry close was approximately flat/slightly above the reference peak metric used in the cycle.

The subsequent -25% correction did not occur before dataset end.

This is a direct counterexample to treating streak95 as a deterministic overheat rule.

## Exhaustive 791-alternative topology event study

All 791 alternative U10s produced at least one streak95 event.

Total alternative streak events:

1,951

Overall -25% hit rate:
- within 31d: 57.41%
- within 62d: 57.41%
- within 93d: 76.37%

Median worst drawdown:
- 31d: -26.00%
- 62d: -33.84%
- 93d: -33.84%

Overlap with prior single-month +100% signal:

434 / 1,951 = 22.25%

Therefore most streak95 events are genuinely distinct from the single-month surge rule.

## Critical non-overlap analysis

Alternative events with NO single-month +100% inside the streak:

1,517 events

Their -25% hit rate:
- 31d: 45.22%
- 62d: 45.22%
- 93d: 69.61%

Median worst drawdown:
- 31d: -18.34%
- 62d: -18.34%
- 93d: -31.34%

-35% hit rate:
- 31d: 8.24%
- 62d: 23.07%
- 93d: 23.07%

These figures are materially weaker than the prior single-month +100% study.

## Overlap events

Alternative streak95 events that DID include a single-month +100% move:

434 events

-25% hit:
- 31d: 100%
- 62d: 100%
- 93d: 100%

Median worst drawdown:
- 31d: -40.66%
- 62d: -40.66%
- 93d: -47.89%

This indicates that a substantial part of the strongest aggregate streak95 effect is inherited from the already-known single-month +100% phenomenon.

## Event clustering

Most streak95 events clustered in a small number of market regimes:

- 2024-10 -> 2024-11: 595 alternative events
- 2025-04 -> 2025-05: 461
- 2026-07 -> 2026-08: 461
- 2024-02 -> 2024-03: 301
- 2024-11 -> 2024-12: 105
- 2023-11 -> 2024-02: 28

Thus 1,951 events are not independent market episodes.

## 791-alternative portfolio robustness

### P1-STREAK95

- final > baseline: 49.68%
- max DD improved: 49.43%
- BOTH final and DD improved: 34.39%
- unfinished cycles: 0%
- median terminal delta: -0.02%
- q25/q75: -3.92% / +2.94%
- median cash days: 72

This is effectively neutral and not robust.

### P2-STREAK95

- final > baseline: 53.48%
- max DD improved: 49.81%
- BOTH final and DD improved: 37.93%
- unfinished cycles: 0%
- median terminal delta: +1.83%
- q25/q75: -2.15% / +4.97%
- median cash days: 61

P2 is mildly positive in the median, but only slightly more than half of alternative U10s beat baseline.

This is not strong enough to support promotion.

## Comparison with the prior single-month +100% effect

Prior single-month +100% event study across alternative U10 topologies:

- -25% hit within 31d: about 88.86%
- within 62d: about 88.95%

Current distinct multimonth streak95 events excluding single-month overlap:

- 31d: 45.22%
- 62d: 45.22%
- 93d: 69.61%

Therefore:

> Consecutive multimonth +95% appreciation is a real and distinct state, but it is not nearly as strong a short-horizon reversal signal as a single explosive +100% month.

## Interpretation

The user's observation is valid enough to define a separate regime:

- several positive months can compound to >95% without any one month being +100%;
- these episodes do show elevated later correction risk;
- the correction is often slower and less reliable than after an explosive single-month surge.

The evidence suggests two different overheat types:

### Explosive overheat
Single selected month +100%+
- high probability of large pullback within roughly 31 days
- previously observed about 89% across alternative topologies

### Persistent trend overheat
At least two consecutive positive selected months, cumulative >=95%
- distinct events often do NOT reverse immediately
- only about 45% of non-overlap events hit -25% inside 31/62 days
- about 70% hit -25% by 93 days

This persistent-trend state should not simply inherit the same sale rule without further evidence.

## Decision

Do not combine STREAK95 with the current single-month +100% live/paper research rule.

Do not promote P1/P2-STREAK95.

The trigger is worth retaining as a diagnostic regime label.

If researched further, the next question should be whether persistent-trend overheat needs a different exit condition than explosive overheat rather than merely changing the threshold.

## Live guardrail

No live/paper change.

## Residual risks

- monthly selected-extreme construction is development-defined
- 95% threshold was user-selected after inspecting current history
- 792 topologies share the same market dates
- events cluster in a few crypto regimes
- 5-bar re-entry mechanics may not be appropriate for this slower trend regime
- no untouched future temporal validation
- historical performance does not establish future performance

TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
