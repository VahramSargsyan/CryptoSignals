# U10 Monthly Surge Pullback + Trailing Re-entry Peak v1 — Evidence

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Validated branch: research/u10-monthly-surge-trailing-reentry-peak-v1
GitHub Actions run: 36346616490
Source commit: 4ae202559c5caedf6a6ca0be78cdf280c7c933f4
Artifact ID: 10940727477
Artifact digest: sha256:3eada8fce27f2a7883401aba1976609fdb86c9493bd6e6c14d3045a00cf59dff
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Frozen rule

P1/P2 cash-out logic is unchanged.

P1:
- +100% monthly surge arm
- sell 30% after >=5% pullback from post-arm running peak

P2:
- +100% monthly surge arm
- sell 30% after >=10% pullback

Re-entry:
- while cash is parked, continue updating the frozen-U10 reference running peak whenever a new daily-close high occurs;
- re-enter the full cash sleeve only after reference equity closes >=25% below the latest running peak;
- execute next open;
- 0.1% modeled re-entry cost.

No timeout.
No peak-reclaim trigger.
No new percentage threshold.

## Canonical U10

| Variant | Baseline | Original frozen-peak P1/P2 | 65d | Peak reclaim | Trailing peak | Trailing vs original | Max DD trailing | Cash days |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| P1 | 219,485.20 | 270,653.52 | 270,653.52 | 238,128.62 | 252,642.73 | -6.65% | -68.60% | 41 |
| P2 | 219,485.20 | 265,751.95 | 265,751.95 | 233,816.07 | 248,067.33 | -6.65% | -68.60% | 38 |

Trailing peak gives back some of the hindsight advantage of the original frozen-peak re-entry, but it still materially exceeds the ordinary U10 baseline.

Canonical delta versus baseline:
- P1 trailing: +15.11%
- P2 trailing: +13.02%

### Canonical cycle behavior

2024-02 cycle:

Initial locked peak:
56,284.29 USDT

While cash was parked, U10 made a new reference peak:
66,881.36 USDT on 2024-03-11.

Trailing -25% threshold therefore moved upward.

P1/P2 trailing re-entry:
- signal: 2024-03-19
- execution: 2024-03-20
- cash duration: 19 days
- drawdown from latest peak: -26.26%

Original frozen-peak rule:
- waited until 2024-04-14
- 44 days in cash

Peak-reclaim rule:
- re-entered too early on 2024-03-07

Trailing therefore sat between the two failure modes:
- it did not chase the old peak reclaim;
- it did not remain anchored to the obsolete lower peak until April.

2025-11:
- no higher peak formed while cash was parked;
- trailing and original rule matched;
- re-entry after 6 days.

2026-01:
- no higher peak formed;
- trailing and original matched;
- P1 re-entry after 16 days;
- P2 after 13 days.

## 791 alternative U10s

### P1 trailing

- final equity above ordinary-U10 baseline: 100.00%
- max DD improved vs baseline: 90.39%
- BOTH final and DD improved: 90.39%
- no unfinished cash cycles: 100.00%
- at least one cash-cycle peak update occurred in 80.66% of alternatives

Terminal improvement versus baseline:
- minimum: +5.43%
- q25: +15.11%
- median: +17.41%
- q75: +18.05%
- maximum: +23.21%

Max-DD improvement:
- median: +2.45 percentage points
- observed maximum improvement: +8.66 pp

Compared with original frozen-peak P1:
- trailing better: 30 / 791 = 3.79%
- equal: 153 / 791
- worse: 608 / 791
- median terminal effect: -6.65%

This is expected:
the original rule can benefit from waiting longer with hindsight when a deeper correction eventually arrives.

Compared with 65d:
- trailing better in 28.95% of alternatives.

Compared with exact old-peak reclaim:
- trailing better in 80.66%.

Median cash days:
55.

No alternative finished with cash stranded.

### P2 trailing

- final above baseline: 100.00%
- max DD improved: 90.52%
- BOTH improved: 90.52%
- no unfinished cycles: 100.00%
- peak updated while cash in 64.22%

Terminal improvement versus baseline:
- minimum: +1.63%
- q25: +12.22%
- median: +14.66%
- q75: +15.91%
- maximum: +20.80%

Median max-DD improvement:
+2.45 pp

Compared with original frozen-peak P2:
- trailing better: 30 / 791 = 3.79%
- equal: 283
- worse: 478
- median terminal effect: -6.65%

Compared with peak reclaim:
- trailing better in 64.22%.

Median cash days:
38.

No alternative finished with cash stranded.

## Regime behavior

Trailing peak mainly changes cycles where the market resumes making new highs after cash-out.

P1 cycle summary:

| Arm month | Cycles | Peak changed while cash | Median cash days |
|---|---:|---:|---:|
| 2024-02 | 330 | 330 | 19 |
| 2024-11 | 196 | 196 | 34 |
| 2025-05 | 330 | 91 | 20 |
| 2025-07 | 120 | 120 | 79 |
| 2025-11 | 462 | 0 | 6 |
| 2026-01 | 792 | 0 | 16 |

The July-2025 failure regime is the most important:

- original frozen peak could wait 123-131 days or never re-enter before dataset end;
- moving peak follows the resumed market upward;
- the later 25% correction from that newer high produced a completed re-entry;
- median/actual cycle duration in the July regime: roughly 71-79 days depending on P1/P2.

Thus the rule solves the stranded-cash problem without introducing a fixed timeout.

## Interpretation

The trailing re-entry peak is not historically the highest-return version on 2023-2026.

The original frozen-peak rule often earns more because it can wait for a much lower eventual entry.

But that advantage comes with a structural weakness:
its re-entry level can become obsolete if the market makes substantially higher highs.

The moving-peak version trades some terminal upside for:

- no arbitrary time constant;
- no premature old-peak reclaim;
- no unfinished cash cycles in all 791 alternative U10s;
- 100% of alternative U10s still above their own ordinary baseline;
- about 90% improving both terminal equity and max drawdown.

This makes it a coherent candidate for temporal holdout validation.

## Freeze decision for historical validation

For the next 2020-2022 study, freeze and test:

### P1-TRAILING
- +100% surge
- 5% pullback cash-out
- 30% cash
- moving reference peak while cash is parked
- re-enter at -25% from latest peak

### P2-TRAILING
- same, but 10% cash-out pullback

Do not tune these rules on 2020-2022.

The older period is validation-only.

## Residual risks

- all 2023-2026 topology tests share the same market regimes;
- moving-peak logic was developed after observing 2023-2026 failure modes;
- old-period universe will differ because TWT/PEPE and other newer assets did not exist;
- old-survivor universe will have survivorship bias by construction;
- daily-close triggers can gap across thresholds;
- historical performance does not establish future performance.

## Live guardrail

No live/paper behavior changed.

TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
