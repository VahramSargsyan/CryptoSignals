# U10 Monthly Surge Pullback + Peak-Reclaim Fallback v1 — Evidence

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Validated branch: research/u10-monthly-surge-peak-reclaim-v1
GitHub Actions run: 36345775861
Source commit: 255a0f3421c65e0a2a9c9167366c1fdea3560cd2
Artifact ID: 10940790526
Artifact digest: sha256:4cf795413d986a5b9d1a602838172c713c81e850d38c0b06d2e16d5168df4716
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Objective

Test a non-time-based invalidation fallback for frozen P1/P2:

- natural re-entry remains -25% from the locked peak;
- if reference equity instead closes back at or above the locked peak first, re-enter next open;
- reason = PEAK_RECLAIM.

No time threshold, MA, reclaim buffer, or other parameter was introduced.

## Canonical U10

| Variant | No fallback | 65d fallback | Peak reclaim | Reclaim vs no fallback | Max DD |
|---|---:|---:|---:|---:|---:|
| P1 | 270,653.52 | 270,653.52 | 238,128.62 | -12.02% | -71.04% |
| P2 | 265,751.95 | 265,751.95 | 233,816.07 | -12.02% | -71.04% |

Peak reclaim was used once in both P1 and P2.

Canonical false-reclaim cycle:

- arm: 2024-02-26
- cash-out execution: 2024-03-01
- original locked peak: 56,284.29
- reference equity reclaimed that peak on 2024-03-06
- fallback re-entry: 2024-03-07
- cash duration: 6 days
- reference equity then made a still higher peak: 66,881.36 on 2024-03-11
- the original no-fallback -25% condition eventually re-entered on 2024-04-14
- no-fallback cash duration: 44 days

Thus reclaiming the old peak did not invalidate the later correction.

It merely showed that the old peak was no longer the relevant peak.

## 791 alternative U10s

### P1

Peak reclaim used in:

80.66% of alternatives.

Compared with baseline:
- final equity > baseline: 99.12%
- max DD improved: 64.22%
- both improved: 63.84%

Compared with no-fallback P1:
- reclaim better: 3.79%
- reclaim worse: the large majority of cases where it changed the path
- median terminal effect: -10.35%

Compared with 65d timeout:
- reclaim better: 8.85%

### P2

Peak reclaim used in:

64.22%.

Compared with baseline:
- final > baseline: 95.83%
- DD improved: 69.66%
- both improved: 67.76%

Compared with no-fallback P2:
- reclaim better: 3.79%
- median terminal effect: -10.11%

Compared with 65d:
- reclaim better: 9.73%

## Main finding

Exact locked-peak reclaim is too eager.

A recovery above the original locked peak often occurs before the surge cycle has actually completed.

The canonical February/March 2024 path demonstrates the failure cleanly:

1. surge;
2. partial pullback and cash-out;
3. old peak reclaimed;
4. new higher peak formed;
5. large correction arrived afterward.

Therefore old locked peak reclaimed does not imply deep-pullback thesis invalidated.

It often means only that the relevant peak moved higher.

## Next logically minimal return rule

The data suggest a cleaner rule that introduces no new parameter:

### TRAILING RE-ENTRY PEAK

After cash-out:
- keep the existing -25% re-entry depth;
- while cash is parked, continue updating the reference running peak whenever U10 makes a new daily-close high;
- re-entry threshold becomes 25% below that latest running peak;
- re-enter next open when reference equity closes <= latest_running_peak * 0.75.

This differs from the original no-fallback rule only in one respect:

the peak is no longer frozen at cash-out.

It differs from peak reclaim because a new high does not force immediate re-entry.

It differs from 65d because there is no time constant.

This directly addresses both observed failure modes:
- February 2024: old peak reclaim should not prematurely re-enter; new peak should replace old peak.
- July 2025: continued upside raises the eventual -25% re-entry floor, preventing cash from remaining anchored to an obsolete lower peak indefinitely.

This rule should be preregistered once and tested before opening 2020-2022 history.

## Guardrail

Do not optimize reclaim buffers or timeouts from current data.

No live/paper behavior changed.

TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
