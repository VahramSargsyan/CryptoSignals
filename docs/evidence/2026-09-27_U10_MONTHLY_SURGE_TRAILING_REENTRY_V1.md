# U10 Monthly Surge Pullback + Trailing Re-entry Peak v1 — Evidence

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Validated branch: research/u10-monthly-surge-trailing-reentry-v1
GitHub Actions run: 36346146398
Source commit: 370b53d73275d9f8db0a8c2288965d5629aead3f
Artifact ID: 10940417793
Artifact digest: sha256:0b5993ab7925df2cd2eb4fd2e5331447daa35f8c79df62eb060b5f7241fb999b
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Objective

Complete P1/P2 with no new timeout or re-entry percentage.

After cash-out:
- keep the existing -25% re-entry depth;
- update the re-entry reference peak upward whenever frozen-U10 daily-close equity makes a new high;
- re-enter all parked cash at the next open after a close <= 75% of that latest running peak.

## Canonical U10

| Variant | No fallback | 65d | Old-peak reclaim | Trailing re-entry | Trail vs no fallback | Max DD | Cash days | Unfinished |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| P1 | 270,653.52 | 270,653.52 | 238,128.62 | 252,642.73 | -6.65% | -68.60% | 41 | 0 |
| P2 | 265,751.95 | 265,751.95 | 233,816.07 | 248,067.33 | -6.65% | -68.60% | 38 | 0 |

Compared with baseline U10 219,485.20:
- P1 trailing remains +15.11% above baseline
- P2 trailing remains +13.02% above baseline

The trailing rule closes every canonical cycle.

## 791 alternative U10s

### P1 trailing

- final > baseline: 100.00%
- max DD better than baseline: 90.39%
- both better: 90.39%
- trailing > no-fallback P1: 3.79%
- trailing > 65d: 28.95%
- trailing > old-peak reclaim: 80.66%
- median terminal delta vs no-fallback P1: -6.65%
- q25 / q75 delta vs no-fallback: -8.59% / -1.09%
- unfinished cycles: 0.00%
- median final uplift vs baseline: +17.41%

### P2 trailing

- final > baseline: 100.00%
- max DD better than baseline: 90.52%
- both better: 90.52%
- trailing > no-fallback P2: 3.79%
- trailing > 65d: 26.30%
- trailing > old-peak reclaim: 64.22%
- median terminal delta vs no-fallback P2: -6.65%
- unfinished cycles: 0.00%
- median final uplift vs baseline: +14.66%

## Interpretation

The trailing re-entry rule solves the mechanical completion problem:
- no arbitrary elapsed-day constant;
- no immediate old-peak reclaim;
- no parked-cash cycles remain unfinished across the 792 tested U10 topologies.

It is not a free improvement.

Relative to the original frozen-peak P1/P2:
- it re-enters sooner after a continuing uptrend creates a higher peak;
- therefore it usually buys higher than waiting for a -25% decline from the original lower locked peak;
- median terminal equity is lower than the original no-fallback overlay.

The trade-off is:

ORIGINAL FROZEN PEAK:
- historically higher terminal equity;
- can leave cash parked for very long periods when the old peak becomes obsolete.

TRAILING RE-ENTRY PEAK:
- historically lower terminal equity than no-fallback P1/P2;
- guarantees a completed re-entry cycle on every tested topology in the available history;
- still improves baseline terminal equity in 100% of alternative topologies;
- still improves both terminal equity and max DD in about 90% of alternatives.

## Freeze decision for historical validation

No additional return-rule tuning should be performed on 2023-2026.

Freeze these candidates before opening older data:

1. P1_ORIGINAL
   +100% surge / -5% cash-out / 30% cash / -25% from frozen locked peak.

2. P1_TRAILING_REENTRY
   same P1 cash-out, but -25% re-entry follows the latest running peak while cash is parked.

P2 equivalents remain secondary controls.

Older 2020-2022 validation must not modify these rules.

## Guardrail

No live/paper behavior changed.

TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
